//! # Recording repair (Spec §57.8)
//!
//! Extraction or attribution can be wrong while the captured source is right:
//! the Brain recorded that Alice said something she never said. That is not
//! Alice's retraction and not a correction of sound Evidence, and writing it as
//! either would forge a history. Recording repair is the third route:
//!
//! ```text
//! the actor's claim was wrong     supersession by the same actor   (§14.2)
//! the world moved on              one new Assertion; succession    (§25.4)
//! the Brain recorded it wrongly   recording repair                 (here)
//! ```
//!
//! One transaction verifies the source's identity, digest and locator, the
//! recorder's origin, the expected versions and every replacement, then
//! appends a terminal `recording_repair` Activity and invalidates the wrong
//! extraction. The invalidation is protected Governance state on the element —
//! a new version, the payload untouched — so history reads the repair state at
//! its own snapshot, and `_system.recording_validity` says which extraction no
//! longer stands and which repair invalidated it. Current projection excludes
//! it; a derived artifact that depended on it needs review.
//!
//! The repair Activity names the source and the invalidated extraction as its
//! inputs. The replacements are the recorder's own ordinary Assertions and are
//! named only in the `RecordingRepair` Facet: listing them as outputs would
//! make an extraction look like a derivation, which it is not.

use crate::{
    governance::{AuthContext, EffectiveAuthority, Permission, ResourceContext},
    id::ElementId,
    nexus::Session,
    store::{
        Element, Store,
        rows::{ControlRecordRow, EvidenceRow, state},
        space::JournalEntry,
    },
};
use anda_kip::{
    Json, KipError, KipErrorCode, Map,
    cognitive::{RecordingRepair, RecordingValidity, RepairReason},
};
use serde_json::json;
use std::collections::BTreeSet;

/// The `governance` member naming the repair that invalidated an extraction.
pub const REPAIR_KEY: &str = "recording_repair";
/// The Activity class of a repair (Profile §6.5).
pub const REPAIR_CLASS: &str = "recording_repair";
/// The Facet a repair Activity carries (Profile §6.5).
pub const REPAIR_FACET: &str = "RecordingRepair";

/// The repair that invalidated an element, if one did.
pub fn repair_ref(governance: &Json) -> Option<&str> {
    governance
        .get(REPAIR_KEY)
        .and_then(Json::as_str)
        .filter(|reference| !reference.is_empty())
}

/// Whether an element is an extraction a recording repair invalidated.
pub fn is_invalidated(element: &Element) -> bool {
    repair_ref(element.governance()).is_some()
}

/// `_system.recording_validity` for one governance block.
pub fn validity(governance: &Json) -> RecordingValidity {
    match repair_ref(governance) {
        Some(reference) => RecordingValidity {
            status: "invalidated".into(),
            repair_ref: Some(reference.to_string()),
        },
        None => RecordingValidity {
            status: "valid".into(),
            repair_ref: None,
        },
    }
}

/// Discloses the repair only after the read path has checked discovery of
/// the Activity. Both wire spellings must obey the same decision.
pub(crate) fn set_visible_reference(view: &mut Json, reference: Option<&str>) {
    let value = reference.map_or(Json::Null, Json::from);
    if let Some(slot) = view
        .get_mut("_system")
        .and_then(|system| system.get_mut("recording_validity"))
        .and_then(|validity| validity.get_mut("repair_ref"))
    {
        *slot = value.clone();
    }
    if let Some(slot) = view
        .get_mut("governance")
        .and_then(|governance| governance.get_mut(REPAIR_KEY))
    {
        *slot = value;
    }
}

/// Compares recorded actors by their current identity, without rewriting the
/// old Assertion or requiring a new attribution after an identity merge.
async fn actor_key(store: &Store, space: &str, actor: &Json) -> Result<String, KipError> {
    let Some(mut id) = crate::term::element_reference(actor) else {
        return Ok(crate::kml::clauses::endpoint_key(actor));
    };
    for _ in 0..64 {
        if id.kind != anda_kip::ElementKind::Concept {
            return Ok(crate::term::Endpoint::Local(id).key());
        }
        let element = store.get_element(id).await?;
        if element.space() != space {
            return Err(KipError::not_found_or_not_visible("actor is unavailable"));
        }
        let Element::Concept(row) = element else {
            unreachable!()
        };
        if row.merged_into.is_empty() {
            return Ok(crate::term::Endpoint::Local(id).key());
        }
        id = row.merged_into.parse()?;
    }
    Err(KipError::internal_error(
        "actor merge chain exceeds 64 hops",
    ))
}

impl Session {
    /// Repairs an extraction the recorder got wrong (§57.8).
    ///
    /// Requires `repair_recording` on every invalidated element, and limits a
    /// repair to the caller's own source-backed outputs. Replacements must
    /// already exist as the caller's own Assertions citing the same source,
    /// with `asserted_at` recovered from that source — never the repair time.
    /// A retry of the same repair returns the recorded one without writing.
    pub async fn repair_recording(
        &self,
        space: &str,
        repair: RecordingRepair,
    ) -> Result<Json, KipError> {
        check_shape(&repair)?;
        self.with_authority(space, async |authority| {
            let store = &self.nexus.store;
            let key = format!(
                "{REPAIR_CLASS}:{}",
                crate::schema::contracts::digest(&serde_json::to_value(&repair).unwrap())?
            );
            // The same repair, retried, is answered from the recorded one under
            // current authorization; nothing is checked or written again.
            if let Some(activity) =
                recorded(store, space, &key, &repair, &authority, &self.auth).await?
            {
                return Ok(json!({
                    "repair_ref": activity,
                    "invalidated_refs": repair.invalidated_refs,
                    "replacement_refs": repair.replacement_refs,
                    "replayed": true,
                }));
            }
            check(store, space, &repair, &authority, &self.auth).await?;
            // The engine's record of who wrote this and through what, as an
            // ordinary write records it (§26).
            let mut origin = json!({"principal_id": self.auth.principal_id});
            if !self.auth.client.is_empty() {
                origin["channel"] = json!(self.auth.client);
            }
            let mut tx = crate::tx::Transaction::begin(
                store,
                space,
                origin,
                false,
                authority.clone(),
                (*self.auth).clone(),
            )
            .await?;
            match stage(store, &mut tx, &repair, &key).await {
                Ok(Staged::Fresh(activity)) => {
                    let outcome = tx.commit(JournalEntry::default()).await?;
                    Ok(json!({
                        "repair_ref": activity,
                        "invalidated_refs": repair.invalidated_refs,
                        "replacement_refs": repair.replacement_refs,
                        "receipt": outcome.receipt,
                    }))
                }
                Ok(Staged::Replayed(activity)) => {
                    tx.abort().await;
                    Ok(json!({
                        "repair_ref": activity,
                        "invalidated_refs": repair.invalidated_refs,
                        "replacement_refs": repair.replacement_refs,
                        "replayed": true,
                    }))
                }
                Err(error) => {
                    tx.abort().await;
                    Err(error)
                }
            }
        })
        .await
    }
}

enum Staged {
    Fresh(String),
    Replayed(String),
}

/// The request's own rules: named refs, no overlap, every invalidated
/// element guarded, and no guard on an element the repair does not touch.
fn check_shape(repair: &RecordingRepair) -> Result<(), KipError> {
    let constraint = |message: &str| Err(KipError::constraint_violation(message.to_string()));
    if repair.invalidated_refs.is_empty() {
        return constraint("a recording repair invalidates at least one extraction");
    }
    if repair.source_ref.is_empty() || repair.source_locator.is_empty() {
        return constraint("a recording repair names its source and a locator inside it");
    }
    let invalidated: BTreeSet<&str> = repair.invalidated_refs.iter().map(String::as_str).collect();
    let replacements: BTreeSet<&str> = repair.replacement_refs.iter().map(String::as_str).collect();
    if invalidated.len() != repair.invalidated_refs.len()
        || replacements.len() != repair.replacement_refs.len()
    {
        return constraint("a recording repair names each element once");
    }
    if !invalidated.is_disjoint(&replacements) || invalidated.contains(repair.source_ref.as_str()) {
        return constraint(
            "an invalidated extraction is neither its own replacement nor the source",
        );
    }
    for reference in &repair.invalidated_refs {
        if !repair.expected_versions.contains_key(reference) {
            return Err(KipError::constraint_violation(format!(
                "expected_versions must guard the invalidated extraction {reference}"
            )));
        }
    }
    for reference in repair.expected_versions.keys() {
        if !invalidated.contains(reference.as_str()) && !replacements.contains(reference.as_str()) {
            return Err(KipError::constraint_violation(format!(
                "expected_versions names {reference}, which this repair does not touch"
            )));
        }
    }
    Ok(())
}

/// The repair recorded under this key, if the caller may still make it.
async fn recorded(
    store: &Store,
    space: &str,
    key: &str,
    repair: &RecordingRepair,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<Option<String>, KipError> {
    let ids = store
        .activities()
        .query_all_ids(crate::store::eq_fields(&[
            ("space", anda_db_schema::Fv::Text(space.into())),
            ("client_key", anda_db_schema::Fv::Text(key.into())),
        ]))
        .await
        .map_err(crate::error::db_error)?;
    let Some(id) = ids.first() else {
        return Ok(None);
    };
    let activity = ElementId::new(anda_kip::ElementKind::Activity, *id).to_string();
    visible(store, space, &activity, authority, auth).await?;
    for reference in &repair.invalidated_refs {
        let element = visible(store, space, reference, authority, auth).await?;
        authority
            .authorize(
                Permission::RepairRecording,
                &ResourceContext::of_element(&element),
                auth,
            )
            .into_result()?;
    }
    Ok(Some(activity))
}

/// Resolves an element the caller may read in full, existence-neutrally.
async fn visible(
    store: &Store,
    space: &str,
    reference: &str,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<Element, KipError> {
    let unavailable = || KipError::not_found_or_not_visible(format!("{reference} is unavailable"));
    let id: ElementId = reference.parse().map_err(|_| unavailable())?;
    let element = store.get_element(id).await.map_err(|_| unavailable())?;
    if element.space() != space
        || matches!(element.state(), state::PURGED | state::PENDING)
        || !authority
            .may_read(&element, auth)
            .is_some_and(|v| v.content && v.constraints.fields.is_empty())
    {
        return Err(unavailable());
    }
    Ok(element)
}

/// Everything §57.8 asks the engine to verify, against current state.
async fn check(
    store: &Store,
    space: &str,
    repair: &RecordingRepair,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<(), KipError> {
    let source = visible(store, space, &repair.source_ref, authority, auth).await?;
    let Element::Evidence(evidence) = &source else {
        return Err(KipError::constraint_violation(
            "a recording repair names captured Evidence as its source",
        ));
    };
    verify_source(evidence, repair)?;
    let source_time = timestamp_at_locator(&evidence.payload_inline, &repair.source_locator)?;

    let principal = auth.principal_id.as_str();
    let mut invalidated_actors = BTreeSet::new();
    let mut invalidated_times = BTreeSet::new();
    for reference in &repair.invalidated_refs {
        let element = visible(store, space, reference, authority, auth).await?;
        authority
            .authorize(
                Permission::RepairRecording,
                &ResourceContext::of_element(&element),
                auth,
            )
            .into_result()?;
        let Element::Assertion(row) = &element else {
            return Err(KipError::constraint_violation(format!(
                "{reference} is not an Assertion; a recording repair invalidates an extracted claim"
            )));
        };
        if let Some(earlier) = repair_ref(&row.governance) {
            return Err(KipError::constraint_violation(format!(
                "{reference} was already invalidated by {earlier}"
            )));
        }
        if row.state == state::TOMBSTONED {
            return Err(KipError::not_found_or_not_visible(format!(
                "{reference} is unavailable"
            )));
        }
        if !cites(&row.evidence_ids, &repair.source_ref) {
            return Err(KipError::constraint_violation(format!(
                "{reference} does not cite {}; a repair covers outputs of that source",
                repair.source_ref
            )));
        }
        // §57.8: by default a repair reaches only the recorder's own
        // source-backed outputs. Recording attribution is not authority.
        if row.origin.get("import").is_some()
            || row.origin["principal_id"].as_str() != Some(principal)
        {
            return Err(KipError::not_authorized(format!(
                "{reference} was not recorded by this Principal; a recording repair reaches \
                 only the recorder's own source-backed outputs (§57.8)"
            )));
        }
        expect_version(repair, reference, row.version)?;
        if repair.reason == RepairReason::ExtractionError && !repair.replacement_refs.is_empty() {
            invalidated_actors.insert(actor_key(store, space, &row.asserted_by).await?);
        }
        invalidated_times.insert(row.asserted_at.clone());
    }

    for reference in &repair.replacement_refs {
        let element = visible(store, space, reference, authority, auth).await?;
        let Element::Assertion(row) = &element else {
            return Err(KipError::constraint_violation(format!(
                "replacement {reference} is not an Assertion"
            )));
        };
        if row.state != state::ACTIVE
            || row.status != "active"
            || repair_ref(&row.governance).is_some()
        {
            return Err(KipError::constraint_violation(format!(
                "replacement {reference} is not an active, standing Assertion"
            )));
        }
        if row.origin.get("import").is_some()
            || row.origin["principal_id"].as_str() != Some(principal)
        {
            return Err(KipError::not_authorized(format!(
                "replacement {reference} was not recorded by this Principal"
            )));
        }
        // The ordinary permission for the replacement, as of now: the one a
        // new Assertion by this Principal for that actor would need.
        let permission = if authority.is_bound_to_actor(&row.asserted_by_key)
            || row.asserted_by_key.is_empty()
        {
            Permission::Assert
        } else {
            Permission::RecordAttributedAssertion
        };
        authority
            .authorize(permission, &ResourceContext::of_element(&element), auth)
            .into_result()?;
        if !cites(&row.evidence_ids, &repair.source_ref) {
            return Err(KipError::constraint_violation(format!(
                "replacement {reference} does not cite {}",
                repair.source_ref
            )));
        }
        if repair.reason == RepairReason::ExtractionError
            && !invalidated_actors.contains(&actor_key(store, space, &row.asserted_by).await?)
        {
            return Err(KipError::constraint_violation(format!(
                "replacement {reference} names another actor; an extraction error keeps the \
                 actor, and a wrong actor is an attribution_error"
            )));
        }
        // §57.8: a replacement describes the original claim, so its claim time
        // is recovered from the original source — never the repair's time.
        // A captured historical message may already have supplied its own
        // claim time, earlier than observed_at (§13.2). Preserve that time;
        // the capture timestamp is an alternative, not an override.
        let preserved = invalidated_times.contains(&row.asserted_at)
            && (evidence.observed_at.is_empty() || row.asserted_at <= evidence.observed_at);
        let claimed = preserved
            || row.asserted_at == evidence.observed_at
            || source_time.as_deref() == Some(row.asserted_at.as_str());
        if !claimed {
            return Err(KipError::constraint_violation(format!(
                "replacement {reference} is asserted at {}, not at the original source's time; \
                 a repair recovers asserted_at from the source it repairs (§57.8)",
                row.asserted_at
            )));
        }
        // The reference closure: what the replacement stands on is still there.
        visible(store, space, &row.proposition_id, authority, auth).await?;
        for evidence_ref in &row.evidence_ids {
            visible(store, space, evidence_ref, authority, auth).await?;
        }
        if let Some(expected) = repair.expected_versions.get(reference)
            && *expected != row.version
        {
            return Err(stale(reference, row.version, *expected));
        }
    }
    Ok(())
}

/// The digest a source is verified against: the one it was captured with,
/// or — for bytes held inline without one, as ingestion mints them (§71.1) —
/// the canonical digest of those bytes.
pub fn source_digest(evidence: &EvidenceRow) -> Option<String> {
    if !evidence.content_digest.is_empty() {
        return Some(evidence.content_digest.clone());
    }
    if evidence.payload_mode == "inline" && !evidence.payload_inline.is_null() {
        return crate::schema::contracts::digest(&evidence.payload_inline).ok();
    }
    None
}

/// The source is the one the extraction was made from: same digest, and the
/// locator resolves inside bytes the engine still holds.
fn verify_source(evidence: &EvidenceRow, repair: &RecordingRepair) -> Result<(), KipError> {
    let Some(digest) = source_digest(evidence) else {
        return Err(KipError::constraint_violation(
            "the source carries no digest a repair could be verified against",
        ));
    };
    if digest != repair.source_digest {
        return Err(KipError::new(
            KipErrorCode::DigestMismatch,
            "the source digest does not match the captured source",
        ));
    }
    if evidence.payload_mode != "inline" || evidence.payload_inline.is_null() {
        return Err(KipError::constraint_violation(
            "the source bytes are not held inline, so the locator cannot be verified",
        ));
    }
    verify_locator(&evidence.payload_inline, &repair.source_locator)
}

/// A JSON Pointer into the payload, or `bytes=<start>-<end>` (inclusive) over
/// its text — the string itself, or the canonical JSON of anything else.
fn verify_locator(payload: &Json, locator: &str) -> Result<(), KipError> {
    let unresolved = || {
        KipError::constraint_violation(format!(
            "source locator {locator:?} does not resolve inside the source"
        ))
    };
    if locator.starts_with('/') {
        return payload.pointer(locator).map(|_| ()).ok_or_else(unresolved);
    }
    if let Some(range) = locator.strip_prefix("bytes=") {
        let (start, end) = range.split_once('-').ok_or_else(unresolved)?;
        let (start, end): (usize, usize) = (
            start.parse().map_err(|_| unresolved())?,
            end.parse().map_err(|_| unresolved())?,
        );
        let length = match payload {
            Json::String(text) => text.len(),
            other => anda_kip::try_canonical_json(other)?.len(),
        };
        return if start <= end && end < length {
            Ok(())
        } else {
            Err(unresolved())
        };
    }
    Err(KipError::constraint_violation(format!(
        "unsupported source locator {locator:?}: use a JSON Pointer or bytes=<start>-<end>"
    )))
}

/// A host can recover a misrecorded claim time by pointing at its exact
/// canonical timestamp in the source. No field names or dates are guessed.
fn timestamp_at_locator(payload: &Json, locator: &str) -> Result<Option<String>, KipError> {
    let text = if locator.starts_with('/') {
        payload
            .pointer(locator)
            .and_then(Json::as_str)
            .map(str::to_string)
    } else if let Some(range) = locator.strip_prefix("bytes=") {
        let text = match payload {
            Json::String(text) => text.clone(),
            other => anda_kip::try_canonical_json(other)?,
        };
        range.split_once('-').and_then(|(start, end)| {
            let start = start.parse::<usize>().ok()?;
            let end = end.parse::<usize>().ok()?;
            text.get(start..=end).map(str::to_string)
        })
    } else {
        None
    };
    Ok(text.filter(|text| crate::time::parse(text).is_ok()))
}

fn cites(evidence_ids: &[String], source: &str) -> bool {
    evidence_ids.iter().any(|id| id == source)
}

fn expect_version(repair: &RecordingRepair, reference: &str, actual: u64) -> Result<(), KipError> {
    match repair.expected_versions.get(reference) {
        Some(expected) if *expected == actual => Ok(()),
        Some(expected) => Err(stale(reference, actual, *expected)),
        None => Err(KipError::constraint_violation(format!(
            "expected_versions must guard {reference}"
        ))),
    }
}

fn stale(reference: &str, actual: u64, expected: u64) -> KipError {
    KipError::version_conflict(format!(
        "{reference} is at version {actual}, not the expected {expected}"
    ))
}

/// Plans the repair Activity, then — for a fresh repair — the invalidations
/// and the `recording` control coordinate (§36.1).
async fn stage(
    store: &Store,
    tx: &mut crate::tx::Transaction,
    repair: &RecordingRepair,
    key: &str,
) -> Result<Staged, KipError> {
    let inputs: Vec<&String> = std::iter::once(&repair.source_ref)
        .chain(&repair.invalidated_refs)
        .collect();
    let tuples: Vec<String> = (0..inputs.len())
        .map(|i| format!("(\"inputs\", :input{i})"))
        .collect();
    let command = format!(
        r#"CREATE ACTIVITY ?repair {{
            CLIENT KEY :key
            SET FIELDS {{activity_class: "{REPAIR_CLASS}", status: "completed", started_at: :now, ended_at: :now}}
            SET FACET "{REPAIR_FACET}" {{
                source_ref: :source_ref, source_digest: :source_digest,
                source_locator: :source_locator, invalidated_refs: :invalidated_refs,
                replacement_refs: :replacement_refs, reason: :reason,
                expected_versions: :expected_versions
            }}
            SET STRUCTURAL {{ {} }}
        }}"#,
        tuples.join(" ")
    );
    let mut parameters = Map::from_iter([
        ("key".to_string(), json!(key)),
        ("now".to_string(), json!(tx.cx.at)),
        ("source_ref".to_string(), json!(repair.source_ref)),
        ("source_digest".to_string(), json!(repair.source_digest)),
        ("source_locator".to_string(), json!(repair.source_locator)),
        (
            "invalidated_refs".to_string(),
            json!(repair.invalidated_refs),
        ),
        (
            "replacement_refs".to_string(),
            json!(repair.replacement_refs),
        ),
        (
            "reason".to_string(),
            serde_json::to_value(repair.reason).unwrap(),
        ),
        (
            "expected_versions".to_string(),
            json!(repair.expected_versions),
        ),
    ]);
    for (i, reference) in inputs.iter().enumerate() {
        parameters.insert(format!("input{i}"), json!(reference));
    }
    let anda_kip::Command::Kml(statement) = anda_kip::parse_kip(&command)? else {
        return Err(KipError::internal_error("repair command is not KML"));
    };
    crate::kml::plan(
        store,
        tx,
        &statement,
        Some(&parameters),
        &anda_kip::Operation::new(command.as_str()),
    )
    .await?;
    let activity_id = tx.handles()["repair"];
    let activity = activity_id.to_string();
    if !tx.is_new_element(activity_id) {
        // The same repair, retried: its digest is the key. Nothing is written
        // again, and the recorded repair is what the caller learns.
        return Ok(Staged::Replayed(activity));
    }
    tx.authorized_recording_repairs.insert(activity_id);

    for reference in &repair.invalidated_refs {
        let id: ElementId = reference.parse()?;
        if let Element::Assertion(row) = tx.load(id).await? {
            if !row.governance.is_object() {
                row.governance = json!({});
            }
            row.governance[REPAIR_KEY] = json!(activity);
        }
        tx.mark_changed(id, anda_kip::ChangeOp::Update);
    }

    let control_key = format!("recording/{activity}");
    tx.control_effects.push(ControlRecordRow {
        _id: 0,
        record_id: format!("{}:{control_key}", tx.cx.tx_id),
        space: tx.cx.space.clone(),
        key: control_key,
        seq: tx.cx.seq,
        version: 1,
        kind: "recording".into(),
        value: json!({
            "repair_ref": activity,
            "source_ref": repair.source_ref,
            "invalidated_refs": repair.invalidated_refs,
            "replacement_refs": repair.replacement_refs,
            "reason": repair.reason,
        }),
        origin: tx.cx.origin.clone(),
    });
    Ok(Staged::Fresh(activity))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locators_resolve_inside_the_held_bytes() {
        let message = json!({"role": "user", "content": "I am not vegetarian."});
        verify_locator(&message, "/content").unwrap();
        assert!(verify_locator(&message, "/missing").is_err());
        let text = json!("I am not vegetarian.");
        verify_locator(&text, "bytes=0-19").unwrap();
        assert!(verify_locator(&text, "bytes=0-20").is_err());
        assert!(verify_locator(&text, "bytes=5-2").is_err());
        assert!(verify_locator(&text, "line 1").is_err());
        let at = "2026-01-01T00:00:00.000Z";
        assert_eq!(
            timestamp_at_locator(&json!({"time": at}), "/time")
                .unwrap()
                .as_deref(),
            Some(at)
        );
        assert_eq!(
            timestamp_at_locator(&json!(format!("记录:{at}")), "bytes=7-30")
                .unwrap()
                .as_deref(),
            Some(at)
        );
        assert!(
            timestamp_at_locator(&json!({"time": "2026-01-01T00:00:00Z"}), "/time")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_repair_guards_what_it_touches_and_nothing_else() {
        let mut repair = RecordingRepair {
            source_ref: "E-1".into(),
            source_digest: "sha256:00".into(),
            source_locator: "/content".into(),
            invalidated_refs: vec!["A-1".into()],
            replacement_refs: vec!["A-2".into()],
            reason: RepairReason::ExtractionError,
            expected_versions: [("A-1".to_string(), 1)].into(),
        };
        check_shape(&repair).unwrap();
        repair.expected_versions.insert("A-9".into(), 1);
        assert!(check_shape(&repair).is_err());
        repair.expected_versions.remove("A-9");
        repair.replacement_refs.push("A-1".into());
        assert!(check_shape(&repair).is_err());
        repair.replacement_refs = vec![];
        repair.expected_versions.clear();
        assert!(check_shape(&repair).is_err());
    }
}
