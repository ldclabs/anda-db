//! # `SEARCH`, `VALIDATE`, `PREVIEW`, `VERIFY`
//!
//! Four different questions, deliberately not merged:
//!
//! ```text
//! SEARCH     what might this be?      grounding, and only grounding
//! VALIDATE   would this be accepted?  legality, without touching state
//! PREVIEW    what would it do?        effect, without committing
//! VERIFY     is this artifact real?   integrity
//! ```
//!
//! ## What a SEARCH result is not
//!
//! A relevance score is not a confidence and not a belief, and a miss is not an
//! absence (§77 of the Core model, §109 of KQL). The result therefore carries
//! its score semantics and its index freshness, and never a `confidence` field
//! — a caller that copied a score into an Assertion would be inventing an
//! epistemic commitment out of a text match.

use anda_kip::{
    ElementKind, Json, KipError, KipErrorCode, PreviewCommand, SearchCommand, SearchTarget,
    ValidateCommand, ValidateTarget, VerifyTarget,
};

use super::Answer;
use super::describe::{scalar_json, scalar_str, scalar_usize};
use crate::id::ElementId;
use crate::kql::Context;
use crate::view;

/// `SEARCH <KIND> :term` — grounding.
pub async fn search(cx: &mut Context<'_>, command: &SearchCommand) -> Result<Answer, KipError> {
    let term = scalar_str(cx, &command.term, "SEARCH")?;
    if let Some(mode) = &command.mode {
        let mode = scalar_str(cx, mode, "MODE")?;
        if mode != "keyword" {
            return Err(KipError::new(
                KipErrorCode::SearchModeUnsupported,
                format!(
                    "this engine has no embedding model, so {mode:?} search is unavailable; \
                     \"keyword\" is the only mode"
                ),
            ));
        }
    }
    if command.as_of_seq.is_some() {
        return Err(KipError::new(
            KipErrorCode::HistoricalSearchUnavailable,
            "this engine keeps no historical index, so AS OF SEQ search is unavailable",
        ));
    }
    let threshold = match &command.threshold {
        Some(scalar) => match scalar_json(cx, scalar)? {
            Json::Number(n) => n.as_f64().unwrap_or(0.0),
            other => {
                return Err(KipError::type_mismatch(format!(
                    "THRESHOLD takes a number, got {other}"
                )));
            }
        },
        None => 0.0,
    };
    let limit = match &command.limit {
        Some(scalar) => scalar_usize(cx, scalar, "LIMIT")?.min(100),
        None => 10,
    };
    let offset = match &command.cursor {
        Some(scalar) => scalar_usize(cx, scalar, "CURSOR")?,
        None => 0,
    };
    let with_type = match &command.with_type {
        Some(scalar) => Some(
            cx.env
                .resolve_symbol(
                    crate::schema::SymbolKind::ConceptType,
                    &scalar_str(cx, scalar, "WITH TYPE")?,
                    crate::schema::Intent::Read,
                )?
                .to_string(),
        ),
        None => None,
    };
    let with_predicate = match &command.with_predicate {
        Some(scalar) => Some(
            cx.env
                .resolve_symbol(
                    crate::schema::SymbolKind::PredicateType,
                    &scalar_str(cx, scalar, "WITH PREDICATE")?,
                    crate::schema::Intent::Read,
                )?
                .to_string(),
        ),
        None => None,
    };

    let kinds: Vec<(ElementKind, &[&str])> = match command.target {
        SearchTarget::Concept => vec![(
            ElementKind::Concept,
            &["name", "aliases", "attributes"] as &[&str],
        )],
        SearchTarget::Proposition => {
            vec![(ElementKind::Proposition, &["predicate_ref", "attributes"])]
        }
        SearchTarget::Evidence => vec![(ElementKind::Evidence, &["payload_inline"])],
        SearchTarget::Cognition => vec![
            (ElementKind::Concept, &["name", "aliases", "attributes"]),
            (ElementKind::Proposition, &["predicate_ref", "attributes"]),
            (ElementKind::Evidence, &["payload_inline"]),
        ],
        // An Assertion's content is a stance and a number, and an Activity's is
        // a class and two timestamps. Neither carries text worth indexing, and
        // returning nothing would read as "no such claim exists".
        SearchTarget::Assertion | SearchTarget::Activity => {
            return Err(KipError::new(
                KipErrorCode::SearchIndexUnavailable,
                "Assertions and Activities carry no free text, so this engine builds no \
                 full-text index over them; reach them through the Proposition or Evidence they \
                 are about",
            ));
        }
    };

    let mut hits: Vec<(f32, Json)> = Vec::new();
    for (kind, fields) in kinds {
        let collection = cx.store.elements(kind);
        let index = collection.get_bm25_index(fields).map_err(|_| {
            KipError::new(
                KipErrorCode::SearchIndexUnavailable,
                format!("no full-text index exists over {kind}"),
            )
        })?;
        // Over-fetch: the filters below are applied after scoring, so the
        // window has to be wide enough to survive them.
        for (seq, score) in index.search_advanced(&term, (limit + offset).saturating_mul(4), None) {
            if score < threshold as f32 {
                continue;
            }
            let id = ElementId::new(kind, seq);
            let Some(element) = cx.load(id).await? else {
                continue;
            };
            if element.space() != cx.space || !element.is_active() {
                continue;
            }
            let rendered = view::render(&element);
            if let Some(expected) = &with_type
                && rendered["schema_ref"].as_str() != Some(expected.as_str())
            {
                continue;
            }
            if let Some(expected) = &with_predicate
                && rendered["predicate_ref"].as_str() != Some(expected.as_str())
            {
                continue;
            }
            hits.push((
                score,
                serde_json::json!({
                    "id": id.to_string(),
                    "kind": kind.to_string(),
                    // Named `score`, never `confidence`: copying this into an
                    // Assertion would invent an epistemic commitment out of a
                    // text match.
                    "score": score,
                    "element": rendered,
                }),
            ));
        }
    }
    hits.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let total = hits.len();
    let page: Vec<Json> = hits
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(_, hit)| hit)
        .collect();
    let consumed = offset + page.len();

    let space = cx.store.get_space(&cx.space).await?;
    Ok(Answer {
        result: serde_json::json!({
            "hits": page,
            "search_context": {
                "mode": "keyword",
                "score_semantics": "bm25_relevance_not_confidence",
                // §77: the index may lag the committed state, and a caller
                // deciding whether a miss means anything needs to know that.
                "index_seq": space.seq,
                "current_space_seq": space.seq,
                "consistency": "index is maintained synchronously with commits",
            },
            "caveat": "a SEARCH score is not a confidence and a miss is not an absence; \
                       ground with SEARCH, then read with FIND or BELIEF",
        }),
        next_cursor: (consumed < total).then(|| consumed.to_string()),
    })
}

/// `VALIDATE` — legality, without touching state.
pub fn validate(cx: &mut Context<'_>, command: &ValidateCommand) -> Result<Answer, KipError> {
    let input = scalar_str(cx, &command.value, "VALIDATE")?;
    let report = |valid: bool, violations: Vec<Json>| {
        Answer::whole(serde_json::json!({
            "valid": valid,
            "violations": violations,
            "warnings": [],
            // Spec's five-layer discipline: legality is not a promise of
            // commit. Preconditions, conflicts and Governance all decide later.
            "note": "VALIDATE reports legality only; a valid command may still fail to commit",
        }))
    };

    Ok(match command.target {
        ValidateTarget::Kql | ValidateTarget::Kml => {
            let parsed = anda_kip::parse_kip(&input);
            match parsed {
                Ok(parsed) => {
                    let expected_mutation = command.target == ValidateTarget::Kml;
                    if parsed.is_mutation() != expected_mutation {
                        report(
                            false,
                            vec![serde_json::json!({
                                "code": "LanguageMismatch",
                                "message": format!(
                                    "this parses as {}, not as the requested language",
                                    anda_kip::CommandType::from(&parsed)
                                ),
                            })],
                        )
                    } else {
                        report(true, vec![])
                    }
                }
                Err(err) => report(
                    false,
                    vec![serde_json::json!({
                        "code": err.name(),
                        "message": err.message,
                    })],
                ),
            }
        }
        ValidateTarget::SchemaPackage => match crate::schema::SchemaPackage::parse(&input) {
            Ok(_) => report(true, vec![]),
            Err(err) => report(
                false,
                vec![serde_json::json!({"code": err.name(), "message": err.message})],
            ),
        },
        ValidateTarget::Capsule | ValidateTarget::ImportPlan => {
            return Err(KipError::unsupported_capability(
                "this engine has no Capsule reader, so it cannot validate one",
            ));
        }
    })
}

/// `PREVIEW KML` — effect, without committing.
pub async fn preview(cx: &mut Context<'_>, command: &PreviewCommand) -> Result<Answer, KipError> {
    let PreviewCommand::Kml(scalar) = command else {
        return Err(KipError::unsupported_capability(
            "this engine has no Capsule importer, so it cannot preview one",
        ));
    };
    let source = scalar_str(cx, scalar, "PREVIEW KML")?;
    let statement = match anda_kip::parse_kip(&source)? {
        anda_kip::Command::Kml(statement) => statement,
        other => {
            return Err(KipError::language_mismatch(format!(
                "PREVIEW KML takes a mutation, and this is {}",
                anda_kip::CommandType::from(&other)
            )));
        }
    };

    // A preview is a dry run, which is the same code path a committing run
    // takes right up to the commit. Simulating it separately would let the two
    // drift, and the drift would only show up as a preview that lied.
    let mut request = anda_kip::Request::single(&source);
    request.options = Some(anda_kip::RequestOptions {
        dry_run: Some(true),
        ..Default::default()
    });
    let operation = request.operations[0].clone();
    let response = crate::kml::execute(cx.store, &cx.space, &statement, &request, &operation).await;

    if let Some(error) = response.error {
        return Ok(Answer::whole(serde_json::json!({
            "would_commit": false,
            "error": error,
        })));
    }
    Ok(Answer::whole(serde_json::json!({
        "would_commit": true,
        "effect": response.first_result(),
        "receipt": response.receipt,
        "note": "a preview reserves no identity and establishes no durable state",
    })))
}

/// `VERIFY` — integrity.
pub fn verify(
    _cx: &mut Context<'_>,
    target: VerifyTarget,
    _value: &anda_kip::Scalar,
) -> Result<Answer, KipError> {
    // Answering `{"valid": true}` without checking a digest or a signature
    // would be the worst possible failure here: integrity is exactly the layer
    // a caller trusts to be paranoid on its behalf.
    Err(KipError::unsupported_capability(format!(
        "this engine cannot verify a {target:?}: it implements no digest profile and no \
         signature checking, and reporting an unverified artifact as valid would defeat the \
         purpose of asking"
    )))
}
