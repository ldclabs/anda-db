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
//! absence (§2.10, §66.6). The result therefore carries
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
use crate::store::history::CursorFamily;

/// `SEARCH <KIND> :term` — grounding.
/// How many index hits are scored per page requested.
const SEARCH_OVERFETCH: usize = 4;

/// The smallest candidate window a search considers, whatever the page size.
///
/// A page of ten in a database whose index spans several Spaces would
/// otherwise be decided by forty hits that may all belong to somebody else.
const SEARCH_MIN_WINDOW: usize = 512;

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
        Some(scalar) => super::read_cursor(cx, scalar, CursorFamily::Search)?.offset,
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
            vec![(ElementKind::Proposition, &["predicate_ref"] as &[&str])]
        }
        SearchTarget::Evidence => vec![(ElementKind::Evidence, &["payload_inline"])],
        SearchTarget::Cognition => vec![
            (ElementKind::Concept, &["name", "aliases", "attributes"]),
            (ElementKind::Proposition, &["predicate_ref"]),
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

    // Over-fetch, because the filters below run after scoring: Space,
    // lifecycle state, declared type and — most importantly — Governance
    // visibility. The index is database-wide while a search is Space-scoped,
    // so a narrow Space in a busy database can have its whole page crowded out
    // by hits it may not see. The window is therefore wide in absolute terms
    // rather than a small multiple of the page, and what still falls off the
    // end is disclosed as a caveat rather than reported as an empty Space
    // (§66.6).
    let window = (limit + offset)
        .saturating_mul(SEARCH_OVERFETCH)
        .max(SEARCH_MIN_WINDOW);
    let mut hits: Vec<(f32, Json)> = Vec::new();
    let mut scanned = 0usize;
    for (kind, fields) in kinds {
        let collection = cx.store.elements(kind);
        let index = collection.get_bm25_index(fields).map_err(|_| {
            KipError::new(
                KipErrorCode::SearchIndexUnavailable,
                format!("no full-text index exists over {kind}"),
            )
        })?;
        let candidates = index.search_advanced(&term, window, None);
        scanned = scanned.max(candidates.len());
        for (seq, score) in candidates {
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
            // The redacted view `load` cached, not a fresh render: a search
            // snippet is a read, and a mask that hid a field from FIND must
            // hide it from SEARCH too (§88.5).
            let rendered = cx.view_of(id);
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
                    "element": rendered.as_ref(),
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
            // §66.6 in the one place a caller can act on it: this page was cut
            // from a bounded candidate window, so an exhaustive question needs
            // FIND, which has no such window. The comparison is against the
            // window that actually ran, not against its floor — a wide page
            // raises the window, and measuring it against the floor would
            // report a search that saw everything as one that did not.
            "exhaustive": scanned < window,
        }),
        next_cursor: super::next_cursor(cx, CursorFamily::Search, consumed, total),
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
            match crate::capsule::parse(&input) {
                Ok(capsule) => report(true, vec![])
                    .with_detail("records", Json::from(capsule.payload.records.len())),
                Err(err) => report(
                    false,
                    vec![serde_json::json!({"code": err.name(), "message": err.message})],
                ),
            }
        }
    })
}

/// `EXPORT CAPSULE` — the portable form of a subgraph.
pub async fn export_capsule(
    cx: &mut Context<'_>,
    command: &anda_kip::ExportCapsuleCommand,
) -> Result<Answer, KipError> {
    // An export is snapshot-consistent (§41.1): binding the read coordinate
    // before the roots are selected is what makes the closure it walks one
    // coherent state rather than several.
    if let Some(as_of) = &command.as_of {
        let seq = cx.resolve_as_of(as_of).await?;
        cx.as_of = Some(seq);
        let version = cx.store.schema_version_at(&cx.space, seq).await?;
        cx.env = cx.store.schema_environment_at(&cx.space, version).await?;
    }
    let mut options = anda_kip::Map::new();
    if let Some(block) = &command.options {
        options = crate::projection::settings_of(block, |name| cx.param_ref(name))?;
    }

    // The roots come from the selection block, exactly as a KQL read would
    // find them — an export selects with the same solver a query uses, so the
    // two cannot disagree about what a pattern matches.
    let solutions = cx.solve(&command.where_clauses).await?;
    let roots: Vec<crate::id::ElementId> = match &command.target {
        anda_kip::ElementRef::Handle(name) => solutions.elements_of(name),
        anda_kip::ElementRef::Id(id) => vec![id.parse()?],
        anda_kip::ElementRef::Param(name) => match cx.param_ref(name)? {
            Json::String(id) => vec![id.parse()?],
            other => {
                return Err(KipError::type_mismatch(format!(
                    "the parameter :{name} must carry an element id, got {other}"
                )));
            }
        },
    };
    if roots.is_empty() {
        return Err(KipError::projection_target_unbound(
            "the selection block bound no root elements, so there is nothing to export",
        ));
    }

    let capsule = crate::capsule::export(cx, roots, &options).await?;
    Ok(Answer::whole(serde_json::to_value(&capsule).map_err(
        |err| KipError::internal_error(format!("a Capsule failed to encode: {err}")),
    )?))
}

/// `PREVIEW KML` and `PREVIEW IMPORT CAPSULE` — effect, without committing.
pub async fn preview(cx: &mut Context<'_>, command: &PreviewCommand) -> Result<Answer, KipError> {
    let PreviewCommand::Kml(scalar) = command else {
        let PreviewCommand::ImportCapsule { capsule, into } = command else {
            unreachable!("the two preview forms are exhaustive");
        };
        let source = scalar_str(cx, capsule, "PREVIEW IMPORT CAPSULE")?;
        let into = scalar_str(cx, into, "INTO")?;
        let parsed = crate::capsule::parse(&source)?;
        // Validation runs against the destination Space, because that is where
        // the schema has to resolve — an artifact that is fine here may be
        // unreadable there.
        let nexus = crate::CognitiveNexus::attach(cx.store.clone());
        let report =
            crate::capsule::import(&nexus, &parsed, &into, true, cx.auth.clone(), false).await?;
        return Ok(Answer::whole(report.to_json(true)));
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
    let response = crate::kml::execute(
        cx.store,
        &cx.space,
        &statement,
        &request,
        &operation,
        cx.authority,
        cx.auth,
    )
    .await;

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
    cx: &mut Context<'_>,
    target: VerifyTarget,
    value: &anda_kip::Scalar,
) -> Result<Answer, KipError> {
    match target {
        VerifyTarget::Capsule => {
            let source = scalar_str(cx, value, "VERIFY CAPSULE")?;
            let capsule = crate::capsule::parse(&source)?;
            Ok(Answer::whole(crate::capsule::verify(&capsule)?))
        }
        // Answering `{"valid": true}` without checking anything would be the
        // worst possible failure here: integrity is exactly the layer a caller
        // trusts to be paranoid on its behalf.
        other => Err(KipError::unsupported_capability(format!(
            "this engine cannot verify a {other:?}: it implements no signature checking for one, \
             and reporting an unverified artifact as valid would defeat the purpose of asking"
        ))),
    }
}
