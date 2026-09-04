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
    ElementKind, Json, KipError, KipErrorCode, PreviewCommand, Scalar, SearchCommand, SearchTarget,
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
        //
        // `UnsupportedCapability`, not `SearchIndexUnavailable`: the second
        // carries the `safe_same_request` retry class, which would send an
        // Agent back to re-run a search that can never work. A permanent
        // absence reported as a transient one is a retry loop.
        SearchTarget::Assertion | SearchTarget::Activity => {
            return Err(KipError::new(
                KipErrorCode::UnsupportedCapability,
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
                    // §66.4: a safe snippet — the indexed text of the redacted
                    // view, windowed around the term — beside the element.
                    "snippet": snippet_of(kind, rendered.as_ref(), &term),
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
        warnings: Vec::new(),
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
    // §75 puts a single operation's Receipt on its own result; the top-level
    // slot belongs to an `atomic` transaction, which this engine does not run.
    // Reading the wrong one made every preview report a null Receipt.
    let receipt = response
        .results
        .first()
        .and_then(|result| result.receipt.as_ref());
    Ok(Answer::whole(serde_json::json!({
        "would_commit": true,
        "effect": response.first_result(),
        "receipt": receipt,
        "note": "a preview reserves no identity and establishes no durable state",
    })))
}

/// `VERIFY` — integrity.
pub async fn verify(
    cx: &mut Context<'_>,
    target: VerifyTarget,
    value: &Scalar,
) -> Result<Answer, KipError> {
    match target {
        VerifyTarget::Capsule => {
            let source = scalar_str(cx, value, "VERIFY CAPSULE")?;
            let capsule = crate::capsule::parse(&source)?;
            Ok(Answer::whole(crate::capsule::verify(&capsule)?))
        }
        VerifyTarget::Receipt => verify_receipt(cx, value).await,
        VerifyTarget::SchemaPackage => verify_package(cx, value).await,
    }
}

/// The artifact a `VERIFY` operand names: JSON text, or the object itself
/// when it arrives bound as a parameter.
fn artifact_json(cx: &Context<'_>, scalar: &Scalar, what: &str) -> Result<Json, KipError> {
    let value = match scalar {
        Scalar::Literal(literal) => Json::from(literal.clone()),
        Scalar::Param(name) => cx.param_ref(name)?,
    };
    match value {
        Json::String(text) => serde_json::from_str(&text).map_err(|err| {
            KipError::new(
                KipErrorCode::ArtifactParseError,
                format!(
                    "{what} takes the artifact as JSON text or as an object, and this text does \
                     not parse: {err}"
                ),
            )
        }),
        Json::Object(_) => Ok(value),
        other => Err(KipError::new(
            KipErrorCode::ArtifactParseError,
            format!("{what} takes the artifact as JSON text or as an object, got {other}"),
        )),
    }
}

/// `VERIFY RECEIPT` (§69.1): the digest §33.2 seals a Receipt with, recomputed,
/// and — where the Receipt names a transaction this Space committed — the
/// Commit Record it describes, compared field by field.
async fn verify_receipt(cx: &mut Context<'_>, value: &Scalar) -> Result<Answer, KipError> {
    let artifact = artifact_json(cx, value, "VERIFY RECEIPT")?;
    let receipt: anda_kip::Receipt = serde_json::from_value(artifact).map_err(|err| {
        KipError::new(
            KipErrorCode::ArtifactParseError,
            format!("this is not a readable Receipt: {err}"),
        )
    })?;
    let Some(declared) = receipt.receipt_digest.clone() else {
        return Err(KipError::new(
            KipErrorCode::ArtifactParseError,
            "a Receipt carries `receipt_digest` (§33.2), and one without it cannot be checked",
        ));
    };
    let recomputed = crate::tx::receipt_digest(&receipt);
    if declared != recomputed {
        return Err(KipError::new(
            KipErrorCode::DigestMismatch,
            format!(
                "this Receipt declares the digest {declared} and its content digests to \
                 {recomputed}; it was modified after it was issued"
            ),
        ));
    }
    let attestation = attest_receipt(cx, &receipt).await?;
    let valid = attestation
        .get("matches")
        .and_then(Json::as_bool)
        .unwrap_or(true);
    Ok(Answer::whole(serde_json::json!({
        "valid": valid,
        "receipt_digest": recomputed,
        "signed": !receipt.proofs.is_empty(),
        "signature": {
            "checked": false,
            "reason": "signed_receipts is not advertised (§33.3); a proof on this Receipt is \
                       carried, not checked",
        },
        "attestation": attestation,
        "note": "a matching digest means the Receipt is intact; attestation says whether this \
                 runtime committed what it describes",
    })))
}

/// Whether the journal holds the transaction a Receipt describes, and whether
/// it says the same thing. Reading the journal takes `read_history`; a caller
/// without it gets the digest check alone, and no hint about which
/// transactions exist (§30.4).
async fn attest_receipt(
    cx: &mut Context<'_>,
    receipt: &anda_kip::Receipt,
) -> Result<Json, KipError> {
    let allowed = cx
        .authority
        .authorize(
            crate::governance::Permission::ReadHistory,
            &crate::governance::ResourceContext::default(),
            cx.auth,
        )
        .into_result()
        .is_ok();
    if !allowed {
        return Ok(serde_json::json!({
            "checked": false,
            "reason": "attestation reads the transaction journal, which takes read_history",
        }));
    }
    let unknown = serde_json::json!({"checked": true, "known": false});
    let Some(tx_id) = receipt.tx_id.as_deref() else {
        return Ok(unknown);
    };
    let Some(row) = cx.store.find_transaction(tx_id).await? else {
        return Ok(unknown);
    };
    if row.space != cx.space {
        return Ok(unknown);
    }
    let mut mismatched: Vec<&str> = Vec::new();
    let status = serde_json::to_value(receipt.status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default();
    if status != row.status {
        mismatched.push("status");
    }
    if receipt
        .space_id
        .as_deref()
        .is_some_and(|id| id != row.space)
    {
        mismatched.push("space_id");
    }
    if receipt.space_seq.is_some_and(|seq| seq != row.seq) {
        mismatched.push("space_seq");
    }
    if receipt
        .snapshot_seq
        .is_some_and(|seq| seq != row.snapshot_seq)
    {
        mismatched.push("snapshot_seq");
    }
    if receipt
        .committed_at
        .as_deref()
        .is_some_and(|at| at != row.committed_at)
    {
        mismatched.push("committed_at");
    }
    if receipt
        .transaction_class
        .as_deref()
        .is_some_and(|class| class != row.transaction_class)
    {
        mismatched.push("transaction_class");
    }
    if receipt
        .request_digest
        .as_deref()
        .is_some_and(|digest| !row.request_digest.is_empty() && digest != row.request_digest)
    {
        mismatched.push("request_digest");
    }
    if receipt
        .schema_environment_version
        .is_some_and(|version| version != row.schema_environment_version)
    {
        mismatched.push("schema_environment_version");
    }
    Ok(serde_json::json!({
        "checked": true,
        "known": true,
        "tx_id": tx_id,
        "matches": mismatched.is_empty(),
        "mismatched": mismatched,
    }))
}

/// `VERIFY SCHEMA PACKAGE` (§69.1): the artifact's own declared digest
/// (§20.11, sha256 over every top-level field except `integrity`), and —
/// where a package is installed under the same reference — whether it is the
/// same content.
async fn verify_package(cx: &mut Context<'_>, value: &Scalar) -> Result<Answer, KipError> {
    let artifact = artifact_json(cx, value, "VERIFY SCHEMA PACKAGE")?;
    let text = serde_json::to_string(&artifact).map_err(|err| {
        KipError::internal_error(format!("a JSON value failed to re-encode: {err}"))
    })?;
    let package = crate::schema::SchemaPackage::parse(&text)?;
    let package_ref = package.package_ref()?.to_string();
    let integrity = artifact.get("integrity");
    let declared = integrity
        .and_then(|integrity| integrity.get("content_digest"))
        .and_then(Json::as_str)
        .filter(|digest| !digest.is_empty());
    let declared = match declared {
        Some(declared) => {
            let mut covered = artifact.clone();
            if let Some(object) = covered.as_object_mut() {
                object.remove("integrity");
            }
            let recomputed = declared_package_digest(&covered);
            if recomputed != declared {
                return Err(KipError::new(
                    KipErrorCode::DigestMismatch,
                    format!(
                        "this package declares the digest {declared} and its content digests to \
                         {recomputed}; it was modified after it was published"
                    ),
                ));
            }
            serde_json::json!({
                "checked": true,
                "content_digest": declared,
                "covers": "all top-level fields except integrity",
            })
        }
        None => serde_json::json!({
            "checked": false,
            "reason": "the artifact declares no integrity.content_digest",
        }),
    };
    let engine_digest =
        crate::store::schema::content_digest(&serde_json::to_value(&package).unwrap_or(Json::Null));
    let installed = match cx.store.installed_packages().await?.get(&package_ref) {
        Some(stored) => {
            let stored_digest = crate::store::schema::content_digest(
                &serde_json::to_value(stored.as_ref()).unwrap_or(Json::Null),
            );
            serde_json::json!({"known": true, "matches": stored_digest == engine_digest})
        }
        None => serde_json::json!({"known": false}),
    };
    let valid = installed
        .get("matches")
        .and_then(Json::as_bool)
        .unwrap_or(true);
    let signatures = integrity
        .and_then(|integrity| integrity.get("signatures"))
        .and_then(Json::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    Ok(Answer::whole(serde_json::json!({
        "valid": valid,
        "package_ref": package_ref,
        "content_digest": engine_digest,
        "declared": declared,
        "signed": signatures > 0,
        "signature": {
            "checked": false,
            "reason": "capsule_signatures is not advertised (§37.8); a signature on this package \
                       is carried, not checked",
        },
        "installed": installed,
        "note": "a matching digest means the artifact is intact, not that its definitions are \
                 wanted; VALIDATE SCHEMA PACKAGE and DESCRIBE PACKAGE answer that",
    })))
}

/// The digest a published package declares (§20.11): `sha256:` over the RFC
/// 8785 canonical JSON of the artifact without its `integrity` block, which
/// is how the Cognitive Memory Profile's own artifact was sealed.
fn declared_package_digest(covered: &Json) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "sha256:{}",
        hex::encode(Sha256::digest(anda_kip::canonical_json(covered).as_bytes()))
    )
}

/// The characters a search snippet shows.
const SNIPPET_WIDTH: usize = 200;

/// A safe snippet for a search hit (§66.4): the text the index matched on,
/// read from the **redacted** view so a masked field stays masked (§88.5),
/// windowed around the first occurrence of the term.
fn snippet_of(kind: ElementKind, view: &Json, term: &str) -> String {
    let fields: &[&str] = match kind {
        ElementKind::Concept => &["name", "aliases", "attributes"],
        ElementKind::Proposition => &["predicate_ref"],
        ElementKind::Evidence => &["payload"],
        _ => &[],
    };
    let mut text = String::new();
    for field in fields {
        collect_text(&view[*field], &mut text);
    }
    window(&text, term, SNIPPET_WIDTH)
}

fn collect_text(value: &Json, out: &mut String) {
    match value {
        Json::String(text) => {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(text);
        }
        Json::Array(items) => items.iter().for_each(|item| collect_text(item, out)),
        Json::Object(map) => map.values().for_each(|item| collect_text(item, out)),
        _ => {}
    }
}

/// `width` characters of `text` around the first case-insensitive occurrence
/// of `term`, or from the start when it does not occur as a phrase. Character
/// based on both engines, so the two produce the same snippet.
fn window(text: &str, term: &str, width: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    let fold = |c: char| c.to_lowercase().next().unwrap_or(c);
    let lower: Vec<char> = chars.iter().map(|c| fold(*c)).collect();
    let needle: Vec<char> = term.chars().map(fold).collect();
    let at = (!needle.is_empty())
        .then(|| {
            lower
                .windows(needle.len())
                .position(|w| w == needle.as_slice())
        })
        .flatten();
    let start = at.map(|i| i.saturating_sub(width / 4)).unwrap_or(0);
    let end = (start + width).min(chars.len());
    chars[start..end]
        .iter()
        .collect::<String>()
        .trim()
        .to_string()
}
