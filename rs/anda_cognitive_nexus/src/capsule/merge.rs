//! # The semantic merge
//!
//! Turning a Capsule's records into destination elements. The hard part is not
//! writing rows — it is that **an element's id is Nexus-local** (§7.1), so every
//! reference in the artifact points at an identity that does not exist here.
//! Import therefore resolves an identity for each record, rewrites every
//! reference onto the destination's ids, and only then writes.
//!
//! ## Identity resolution (§38.2)
//!
//! ```text
//! 1  a prior import of this element from this Capsule   the client_key
//! 2  a trusted canonical_id already in this Space       cross-system identity
//! 3  the structural identity of a Proposition           one tuple, one Proposition
//! 4  otherwise, a new element
//! ```
//!
//! Step 1 is what makes an import idempotent across restarts, and it is stored
//! *on the element* as its `client_key` rather than in a side table: a mapping
//! written separately from the elements it maps can survive a crash the
//! elements did not, and then a re-import points at things that are not there.
//!
//! The source's Space-local `key` is deliberately **not** imported. A key is
//! Space-local (§5.3), so two Spaces' `person:alice` may be two different
//! people; carrying it across would make the destination's own
//! `UPSERT ... MATCH {key: …}` resolve ambiguously. Name is not identity either
//! (§38.3), so it resolves nothing — it is imported as the grounding state it
//! is.
//!
//! ## What arrives unchanged
//!
//! The epistemic payload. An imported Assertion keeps the stance, mode and
//! confidence its assertor gave it, because those describe how *that actor*
//! arrived at the claim and no transport changes that. What this runtime
//! observed — an import, of this artifact, from that source — is engine origin,
//! and it goes where engine origin goes (§27): `_system.origin`, stamped by the
//! transaction, never inherited from the source.
//!
//! ## What is refused
//!
//! A reference the Capsule neither carries nor resolves in this Space. The
//! alternative — dropping the edge and importing the rest — produces a graph
//! that looks whole and is not: an Assertion with its Evidence silently
//! removed reads as an unsupported claim rather than as a broken import.

use anda_kip::{Capsule, ElementKind, Json, KipError, KipErrorCode, Map};
use std::collections::BTreeMap;

use super::ImportReport;
use crate::governance::{AuthContext, EffectiveAuthority};
use crate::id::ElementId;
use crate::store::rows::*;
use crate::store::{Element, Store, eq_field};
use crate::term::{Endpoint, tuple_key};
use crate::tx::Transaction;

/// The `client_key` an imported element carries.
///
/// It is the artifact's digest plus the id the element had inside it, so the
/// same Capsule imported twice resolves to the same destination elements and
/// two different Capsules that happen to share an internal id do not collide.
fn import_key(digest: &str, source_id: &str) -> String {
    format!("kip:import:{digest}:{source_id}")
}

/// One record on its way in.
struct Record {
    kind: ElementKind,
    source_id: String,
    view: Json,
}

/// Writes a verified Capsule's records into a Space.
pub async fn merge(
    store: &Store,
    capsule: &Capsule,
    space_id: &str,
    digest: &str,
    mut report: ImportReport,
    auth: AuthContext,
) -> Result<ImportReport, KipError> {
    let records = collect(capsule)?;

    // Phase 1: resolve an identity for every record before anything is
    // written. A half-resolved import would write some elements against
    // destination ids and others against ids that never existed.
    let mut mapping: BTreeMap<String, ElementId> = BTreeMap::new();
    let mut fresh: Vec<&Record> = Vec::new();
    let mut reused = 0usize;
    for record in &records {
        if let Some(existing) = resolve_existing(store, space_id, digest, record).await? {
            mapping.insert(record.source_id.clone(), existing);
            reused += 1;
        } else {
            fresh.push(record);
        }
    }

    let origin = serde_json::json!({
        "import": {
            "capsule_digest": digest,
            "source_nexus": capsule.payload.source.nexus_id,
            "source_space": capsule.payload.source.space_ref,
            "source_snapshot_seq": capsule.payload.source.snapshot_seq,
        }
    });
    // Import runs on the host's own authority: it is a host API, and the host
    // already decided that this Space accepts another Brain's cognition.
    let authority = EffectiveAuthority::resolve(store, space_id, &auth).await?;
    let mut tx = Transaction::begin(store, space_id, origin, false, authority, auth).await?;

    // Phase 2: mint the ids the new records will wear, so a reference to a
    // record that appears later in the artifact still resolves.
    for record in &fresh {
        let id = tx.mint(record.kind).await?;
        mapping.insert(record.source_id.clone(), id);
    }

    // Phase 3: rewrite and stage. A Proposition resolves against the tuple it
    // becomes *after* rewriting, because one Space keeps one Proposition per
    // semantic tuple (§12.4) — importing a tuple the destination already has
    // must bind it, not collide with its unique index.
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for record in &fresh {
        let id = mapping[&record.source_id];
        if record.kind == ElementKind::Proposition
            && let Some(existing) = resolve_tuple(store, space_id, &record.view, &mapping).await?
        {
            // The minted shell is left unstaged; commit discards it.
            let _ = id;
            mapping.insert(record.source_id.clone(), existing);
            reused += 1;
            continue;
        }
        let element = build(record, id, space_id, digest, &mapping)?;
        tx.stage_new(id, element, "import");
        *counts.entry(record.kind.to_string()).or_default() += 1;
    }

    let entry = crate::store::space::JournalEntry {
        idempotency_key: format!("kip:import:{digest}"),
        ..Default::default()
    };
    tx.commit(entry).await?;

    report.counts = counts;
    report.mapping = mapping
        .into_iter()
        .map(|(source, id)| (source, id.to_string()))
        .collect();
    if reused > 0 {
        report.warnings.push(format!(
            "{reused} record(s) resolved to elements this Space already had, and were not written \
             again"
        ));
    }
    Ok(report)
}

/// What an import would do, without doing it.
///
/// The preview resolves identity for real — a re-import previews as "resolves
/// to C-7", not as "would create something" — because a preview that cannot
/// tell those apart is not a preview of this import.
pub async fn preview(
    store: &Store,
    capsule: &Capsule,
    space_id: &str,
    digest: &str,
    mut report: ImportReport,
) -> Result<ImportReport, KipError> {
    let records = collect(capsule)?;
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut reused = 0usize;
    let mut mapping: BTreeMap<String, ElementId> = BTreeMap::new();

    // Same two phases as the real import: identity first, then the tuples,
    // which can only be resolved once their endpoints are known.
    let mut unresolved: Vec<&Record> = Vec::new();
    for record in &records {
        match resolve_existing(store, space_id, digest, record).await? {
            Some(existing) => {
                mapping.insert(record.source_id.clone(), existing);
                reused += 1;
            }
            None => unresolved.push(record),
        }
    }
    for record in &unresolved {
        if record.kind == ElementKind::Proposition
            && let Some(existing) = resolve_tuple(store, space_id, &record.view, &mapping).await?
        {
            mapping.insert(record.source_id.clone(), existing);
            reused += 1;
            continue;
        }
        *counts.entry(record.kind.to_string()).or_default() += 1;
    }

    for record in &records {
        let resolved = mapping
            .get(&record.source_id)
            .map(ElementId::to_string)
            .unwrap_or_else(|| "<new element>".to_string());
        report.mapping.insert(record.source_id.clone(), resolved);
    }
    report.counts = counts;
    if reused > 0 {
        report.warnings.push(format!(
            "{reused} record(s) already resolve to elements in this Space and would not be \
             written again"
        ));
    }
    report.warnings.push(
        "preview only: no identity was reserved and no durable state was established".to_string(),
    );
    Ok(report)
}

/// Every record the Capsule carries, with the id it had at the source.
fn collect(capsule: &Capsule) -> Result<Vec<Record>, KipError> {
    let payload = &capsule.payload;
    let mut records = Vec::new();
    for (kind, views) in [
        (ElementKind::Concept, &payload.records.concepts),
        (ElementKind::Proposition, &payload.records.propositions),
        (ElementKind::Evidence, &payload.records.evidence),
        (ElementKind::Activity, &payload.records.activities),
        (ElementKind::Assertion, &payload.records.assertions),
    ] {
        for view in views {
            let Some(source_id) = view.get("id").and_then(Json::as_str) else {
                return Err(KipError::new(
                    KipErrorCode::CapsuleValidationFailed,
                    "a Capsule record carries no `id`, so nothing else in the artifact can refer \
                     to it",
                ));
            };
            records.push(Record {
                kind,
                source_id: source_id.to_string(),
                view: view.clone(),
            });
        }
    }
    Ok(records)
}

/// Steps 1 and 2 of the resolution order.
async fn resolve_existing(
    store: &Store,
    space_id: &str,
    digest: &str,
    record: &Record,
) -> Result<Option<ElementId>, KipError> {
    let key = import_key(digest, &record.source_id);
    if let Some(id) = find_by_client_key(store, space_id, record.kind, &key).await? {
        return Ok(Some(id));
    }
    if record.kind == ElementKind::Concept
        && let Some(canonical) = record.view.get("canonical_id").and_then(Json::as_str)
        && !canonical.is_empty()
        && let Some(id) = find_concept_by_canonical_id(store, space_id, canonical).await?
    {
        return Ok(Some(id));
    }
    Ok(None)
}

async fn find_by_client_key(
    store: &Store,
    space_id: &str,
    kind: ElementKind,
    key: &str,
) -> Result<Option<ElementId>, KipError> {
    if kind == ElementKind::Proposition {
        // A Proposition has no client key: its identity is its tuple.
        return Ok(None);
    }
    let ids = store
        .elements(kind)
        .query_all_ids(anda_db::query::Filter::And(vec![
            Box::new(eq_field("space", anda_db_schema::Fv::Text(space_id.into()))),
            Box::new(eq_field(
                "client_key",
                anda_db_schema::Fv::Text(key.to_string()),
            )),
        ]))
        .await
        .map_err(crate::error::db_error)?;
    Ok(ids.first().map(|seq| ElementId::new(kind, *seq)))
}

async fn find_concept_by_canonical_id(
    store: &Store,
    space_id: &str,
    canonical: &str,
) -> Result<Option<ElementId>, KipError> {
    let ids = store
        .concepts()
        .query_all_ids(anda_db::query::Filter::And(vec![
            Box::new(eq_field("space", anda_db_schema::Fv::Text(space_id.into()))),
            Box::new(eq_field(
                "canonical_id",
                anda_db_schema::Fv::Text(canonical.to_string()),
            )),
        ]))
        .await
        .map_err(crate::error::db_error)?;
    Ok(ids
        .first()
        .map(|seq| ElementId::new(ElementKind::Concept, *seq)))
}

/// Step 3: the Proposition this tuple already is, once its endpoints are
/// rewritten.
async fn resolve_tuple(
    store: &Store,
    space_id: &str,
    view: &Json,
    mapping: &BTreeMap<String, ElementId>,
) -> Result<Option<ElementId>, KipError> {
    // An endpoint that has no destination identity yet cannot be part of a
    // tuple the destination already holds, so this is "not here", not an
    // error. The real import resolves endpoints first and hits the strict path.
    let (Ok(subject), Ok(object)) = (
        endpoint(view.get("subject"), mapping, "subject"),
        endpoint(view.get("object"), mapping, "object"),
    ) else {
        return Ok(None);
    };
    let predicate = text(view, "predicate_ref");
    let key = tuple_key(space_id, &subject, &predicate, &object);
    Ok(store
        .find_proposition(&key)
        .await?
        .map(|row| ElementId::new(ElementKind::Proposition, row._id)))
}

/// Builds the destination row for one record.
fn build(
    record: &Record,
    id: ElementId,
    space_id: &str,
    digest: &str,
    mapping: &BTreeMap<String, ElementId>,
) -> Result<Element, KipError> {
    let view = &record.view;
    let client_key = import_key(digest, &record.source_id);
    let facets = map_of(view, "facets");
    let structural = rewrite_structural(map_of(view, "structural"), mapping)?;
    let retention = view
        .get("retention")
        .cloned()
        .filter(|value| !value.is_null())
        .unwrap_or(Json::Null);
    let expires_at = retention
        .get("expires_at")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string();

    Ok(match record.kind {
        ElementKind::Concept => Element::Concept(Box::new(ConceptRow {
            _id: id.seq,
            schema_ref: text(view, "schema_ref"),
            // The source's Space-local key stays at the source (§5.3).
            key: String::new(),
            name: text(view, "name"),
            canonical_id: text(view, "canonical_id"),
            aliases: strings(view, "aliases"),
            attributes: map_of(view, "attributes"),
            facets,
            structural,
            client_key,
            expires_at,
            retention,
            ..Default::default()
        })),
        ElementKind::Proposition => {
            let subject = endpoint(view.get("subject"), mapping, "subject")?;
            let object = endpoint(view.get("object"), mapping, "object")?;
            let predicate_ref = text(view, "predicate_ref");
            Element::Proposition(Box::new(PropositionRow {
                _id: id.seq,
                subject: subject.to_json(),
                subject_key: subject.key(),
                predicate_ref: predicate_ref.clone(),
                object: object.to_json(),
                object_key: object.key(),
                tuple_key: tuple_key(space_id, &subject, &predicate_ref, &object),
                attributes: map_of(view, "attributes"),
                facets,
                structural,
                expires_at,
                retention,
                ..Default::default()
            }))
        }
        ElementKind::Assertion => {
            let proposition = reference_id(view.get("proposition_id"), mapping, "proposition")?;
            let asserted_by = endpoint(view.get("asserted_by"), mapping, "asserted_by")?;
            let evidence_refs = rewrite_refs(view.get("evidence_refs"), mapping, "evidence")?;
            let evidence_ids = evidence_refs.iter().filter_map(reference_target).collect();
            Element::Assertion(Box::new(AssertionRow {
                _id: id.seq,
                proposition_id: proposition.to_string(),
                asserted_by: asserted_by.to_json(),
                asserted_by_key: asserted_by.key(),
                stance: text(view, "stance"),
                mode: text(view, "mode"),
                // A missing confidence stays missing: the sentinel is what
                // keeps "the actor stated none" from reading as a number.
                confidence: view
                    .get("confidence")
                    .and_then(Json::as_f64)
                    .unwrap_or(-1.0),
                asserted_at: text(view, "asserted_at"),
                valid_from: nested_text(view, "valid_time", "from"),
                valid_until: nested_text(view, "valid_time", "until"),
                evidence_refs,
                evidence_ids,
                context_refs: rewrite_refs(view.get("context_refs"), mapping, "context")?,
                status: nested_text(view, "lifecycle", "status"),
                supersedes: rewrite_ids(view, "lifecycle", "supersedes", mapping)?,
                superseded_by: rewrite_ids(view, "lifecycle", "superseded_by", mapping)?,
                retracted_at: nested_text(view, "lifecycle", "retracted_at"),
                client_key,
                facets,
                structural,
                expires_at,
                retention,
                ..Default::default()
            }))
        }
        ElementKind::Evidence => Element::Evidence(Box::new(EvidenceRow {
            _id: id.seq,
            evidence_class: text(view, "evidence_class"),
            payload_mode: nested_text(view, "payload", "mode"),
            payload_inline: view
                .get("payload")
                .and_then(|payload| payload.get("inline"))
                .cloned()
                .unwrap_or(Json::Null),
            content_ref: nested_text(view, "payload", "content_ref"),
            content_digest: text(view, "content_digest"),
            media_type: text(view, "media_type"),
            observed_at: text(view, "observed_at"),
            source_refs: rewrite_refs(view.get("source_refs"), mapping, "source")?,
            generated_by: view
                .get("generated_by")
                .and_then(|value| value.get("id"))
                .and_then(Json::as_str)
                .map(|source| resolve(mapping, source, "generated_by").map(|id| id.to_string()))
                .transpose()?
                .unwrap_or_default(),
            status: nested_text(view, "lifecycle", "status"),
            corrects: rewrite_ids(view, "lifecycle", "corrects", mapping)?,
            corrected_by: rewrite_ids(view, "lifecycle", "corrected_by", mapping)?,
            client_key,
            facets,
            structural,
            expires_at,
            retention,
            ..Default::default()
        })),
        ElementKind::Activity => Element::Activity(Box::new(ActivityRow {
            _id: id.seq,
            activity_class: text(view, "activity_class"),
            status: text(view, "status"),
            started_at: text(view, "started_at"),
            ended_at: text(view, "ended_at"),
            inputs: rewrite_refs(view.get("inputs"), mapping, "inputs")?,
            outputs: rewrite_refs(view.get("outputs"), mapping, "outputs")?,
            associated_actors: rewrite_refs(
                view.get("associated_actors"),
                mapping,
                "associated_actors",
            )?,
            parameters_digest: text(view, "parameters_digest"),
            client_key,
            facets,
            structural,
            expires_at,
            retention,
            ..Default::default()
        })),
    })
}

// ---------------------------------------------------------------------------
// Rewriting
// ---------------------------------------------------------------------------

/// Maps one source id onto the destination element it became.
///
/// A reference the Capsule neither carries nor resolves here fails the import.
/// The closure exists so this does not happen; when it does, the artifact is
/// incomplete, and importing its readable half would leave a graph whose gaps
/// are invisible.
fn resolve(
    mapping: &BTreeMap<String, ElementId>,
    source: &str,
    what: &str,
) -> Result<ElementId, KipError> {
    mapping.get(source).copied().ok_or_else(|| {
        KipError::new(
            KipErrorCode::CapsuleValidationFailed,
            format!(
                "a record's {what} references {source}, which this Capsule does not carry; export \
                 it with a referential closure, or the destination would hold an edge to nothing"
            ),
        )
    })
}

/// The members a reference can be spelled under.
///
/// `{"id": …}` is the general form, and `evidence_id` is the one a citation
/// uses because an `EvidenceRef` carries a role alongside it. Missing either
/// would leave the import pointing at the source's ids — an edge that resolves
/// in the destination to some unrelated element, or to nothing.
const REFERENCE_KEYS: &[&str] = &["id", "evidence_id"];

/// Rewrites a reference value — `{"id": …}`, `{"evidence_id": …}`, or a bare id
/// string.
fn rewrite_reference(
    value: &Json,
    mapping: &BTreeMap<String, ElementId>,
    what: &str,
) -> Result<Json, KipError> {
    match value {
        Json::String(text) if text.parse::<ElementId>().is_ok() => {
            Ok(Json::String(resolve(mapping, text, what)?.to_string()))
        }
        Json::Object(object) => {
            let Some(key) = REFERENCE_KEYS
                .iter()
                .find(|key| object.get(**key).and_then(Json::as_str).is_some())
            else {
                // A canonical or foreign reference names something outside
                // this Space's id space, so there is nothing to rewrite.
                return Ok(value.clone());
            };
            let source = object[*key].as_str().unwrap_or_default().to_string();
            let mut rewritten = object.clone();
            rewritten.insert(
                (*key).to_string(),
                Json::String(resolve(mapping, &source, what)?.to_string()),
            );
            Ok(Json::Object(rewritten))
        }
        other => Ok(other.clone()),
    }
}

/// The element a rewritten reference points at.
fn reference_target(value: &Json) -> Option<String> {
    match value {
        Json::String(text) => Some(text.clone()),
        Json::Object(object) => REFERENCE_KEYS
            .iter()
            .find_map(|key| object.get(*key).and_then(Json::as_str))
            .map(str::to_string),
        _ => None,
    }
}

fn rewrite_refs(
    value: Option<&Json>,
    mapping: &BTreeMap<String, ElementId>,
    what: &str,
) -> Result<Vec<Json>, KipError> {
    let Some(Json::Array(items)) = value else {
        return Ok(vec![]);
    };
    items
        .iter()
        .map(|item| rewrite_reference(item, mapping, what))
        .collect()
}

fn rewrite_ids(
    view: &Json,
    container: &str,
    field: &str,
    mapping: &BTreeMap<String, ElementId>,
) -> Result<Vec<String>, KipError> {
    let Some(Json::Array(items)) = view.get(container).and_then(|block| block.get(field)) else {
        return Ok(vec![]);
    };
    items
        .iter()
        .filter_map(Json::as_str)
        .map(|source| resolve(mapping, source, field).map(|id| id.to_string()))
        .collect()
}

fn rewrite_structural(
    structural: Map<String, Json>,
    mapping: &BTreeMap<String, ElementId>,
) -> Result<Map<String, Json>, KipError> {
    let mut out = Map::new();
    for (field, value) in structural {
        let Json::Array(items) = value else {
            continue;
        };
        let rewritten: Result<Vec<Json>, KipError> = items
            .iter()
            .map(|item| rewrite_reference(item, mapping, &field))
            .collect();
        out.insert(field, Json::Array(rewritten?));
    }
    Ok(out)
}

/// Reads a tuple endpoint and rewrites it if it names a local element.
fn endpoint(
    value: Option<&Json>,
    mapping: &BTreeMap<String, ElementId>,
    what: &str,
) -> Result<Endpoint, KipError> {
    let value = value.ok_or_else(|| {
        KipError::new(
            KipErrorCode::CapsuleValidationFailed,
            format!("a record is missing its {what}"),
        )
    })?;
    let rewritten = rewrite_reference(value, mapping, what)?;
    Endpoint::from_json(&rewritten)
}

fn reference_id(
    value: Option<&Json>,
    mapping: &BTreeMap<String, ElementId>,
    what: &str,
) -> Result<ElementId, KipError> {
    let source = value.and_then(Json::as_str).ok_or_else(|| {
        KipError::new(
            KipErrorCode::CapsuleValidationFailed,
            format!("a record is missing its {what}"),
        )
    })?;
    resolve(mapping, source, what)
}

// ---------------------------------------------------------------------------
// View readers
// ---------------------------------------------------------------------------

fn text(view: &Json, field: &str) -> String {
    view.get(field)
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string()
}

fn nested_text(view: &Json, container: &str, field: &str) -> String {
    view.get(container)
        .and_then(|block| block.get(field))
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string()
}

fn strings(view: &Json, field: &str) -> Vec<String> {
    match view.get(field) {
        Some(Json::Array(items)) => items
            .iter()
            .filter_map(Json::as_str)
            .map(str::to_string)
            .collect(),
        _ => vec![],
    }
}

fn map_of(view: &Json, field: &str) -> Map<String, Json> {
    match view.get(field) {
        Some(Json::Object(map)) => map.clone(),
        _ => Map::new(),
    }
}
