//! # Changing an element's own Governance members
//!
//! An element's `governance` block — its classification, its influence-authority
//! ceiling, its policy reference — is not an author-writable field. `anda_kip`'s
//! parser refuses it in every assignment, on the text path and on the pre-parsed
//! AST path alike, so no KML statement can reach it (§50, §51). This module is
//! the other half: the authorized ways it *does* change.
//!
//! ```text
//! classify              raise a label            update
//!                       lower one                declassify
//! elevate_authority     raise a ceiling          elevate_authority + approvals
//!                       lower one                elevate_authority
//! quarantine / release  hold out of ordinary use quarantine
//! ```
//!
//! ## Why raising is ordinary and lowering is not
//!
//! Classification and authority move in opposite directions, and each has one
//! privileged direction: the one that *reveals* or *empowers*. An agent that
//! notices it has written something sensitive should be able to say so without a
//! Governance ticket, and an agent that has decided a Skill is dangerous should
//! be able to demote it immediately (§132). Making the cautious direction
//! privileged would make caution rare.
//!
//! ## Why non-amplification is checked at elevation, not at derivation
//!
//! Everything is created `descriptive`, the bottom of the ladder, so a derived
//! artifact cannot exceed its inputs by default — the rule holds without doing
//! anything. It becomes load-bearing only when somebody asks to *raise* one, and
//! that is where the lineage recorded at commit is read: a summary of a
//! descriptive Skill cannot become behavioral, however locally it was written
//! (§127, §128, and the §243 fixture).
//!
//! ## Why these commit as transactions
//!
//! Each writes a new element version and takes a Space sequence, exactly as a
//! cognitive write does. That is what keeps §177 answerable — *what
//! classification did this element have then* — and what puts the change in the
//! authorized change stream (§186). Each is recorded in the Governance audit as
//! well, because the two logs answer different questions: the version log says
//! what the element looked like, the audit says who decided that and why.

use anda_kip::{Json, KipError};

use super::auth::AuthContext;
use super::decision::{EffectiveAuthority, ResourceContext};
use super::store::MutationEntry;
use super::{Permission, authority, classification};
use crate::id::ElementId;
use crate::store::rows::state;
use crate::store::{Element, Store};

/// The `governance` member holding an element's influence-authority ceiling.
pub const AUTHORITY_KEY: &str = "max_influence_authority";
/// The `governance` member recording what a derived element was derived from.
pub const LINEAGE_KEY: &str = "authority_lineage";
/// The `governance` member recording why an element is held out of use.
pub const QUARANTINE_KEY: &str = "quarantine_reason";

/// The influence-authority ceiling an element carries (§124).
pub fn ceiling_of(element: &Element) -> &str {
    let stated = element
        .governance()
        .get(AUTHORITY_KEY)
        .and_then(Json::as_str)
        .unwrap_or_default();
    if stated.is_empty() {
        authority::DEFAULT
    } else {
        stated
    }
}

/// The elements a derived artifact inherits its ceiling from (§128).
pub fn lineage_of(element: &Element) -> Vec<String> {
    element
        .governance()
        .get(LINEAGE_KEY)
        .and_then(Json::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Sets one element's classification label (§93, §100).
///
/// Returns the label that was there before, so a caller can report the
/// transition rather than only the destination.
pub async fn classify(
    store: &Store,
    space_id: &str,
    id: ElementId,
    label: &str,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<String, KipError> {
    let element = readable(store, space_id, id, authority, auth).await?;
    let resource = ResourceContext::of_element(&element);

    let current = element.classification().to_string();
    let effective = if current.is_empty() {
        authority.default_classification()
    } else {
        current.as_str()
    };
    let lowering = classification::rank(label) < classification::rank(effective);
    let permission = if lowering {
        Permission::Declassify
    } else {
        Permission::Update
    };
    let decision = super::approval::resolve(
        store,
        space_id,
        &resource,
        authority.authorize(permission, &resource, auth),
        auth,
    )
    .await?
    .into_result()?;

    let op = if lowering { "declassify" } else { "classify" };
    let patch = |governance: &Json| set_member(governance, "classification", Json::from(label));
    let version = commit(store, space_id, element, op, None, patch, auth).await?;
    audit(
        store,
        space_id,
        id,
        op,
        serde_json::json!({"from": current, "to": label, "version": version}),
        auth,
    )
    .await?;
    super::approval::consume(store, &decision).await?;
    Ok(current)
}

/// Raises or lowers how strongly one element may influence action (§129, §132).
///
/// Raising is checked against the element's authority lineage: a derived
/// artifact cannot be elevated past the lowest ceiling it was derived from, so
/// no chain of summarizing turns a descriptive note into an executable one.
///
/// Returns the ceiling the element carried before.
pub async fn elevate_authority(
    store: &Store,
    space_id: &str,
    id: ElementId,
    class: &str,
    authority_state: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<String, KipError> {
    if authority::rank(class) == 0 && class != authority::DESCRIPTIVE && !class.is_empty() {
        return Err(KipError::constraint_violation(format!(
            "{class:?} is not an influence-authority class this engine implements"
        )));
    }
    let element = readable(store, space_id, id, authority_state, auth).await?;
    let resource = ResourceContext::of_element(&element);
    // §129: elevation is exactly the operation a policy asks for independent
    // approval on, and §246 requires that one approval of two is not partial
    // activation. That is decided here rather than by the caller.
    let decision = super::approval::resolve(
        store,
        space_id,
        &resource,
        authority_state.authorize(Permission::ElevateAuthority, &resource, auth),
        auth,
    )
    .await?
    .into_result()?;

    let current = ceiling_of(&element).to_string();
    let raising = authority::rank(class) > authority::rank(&current);
    if raising {
        let granted_ceiling = super::decision::authority_ceiling(&decision.constraints);
        if authority::rank(class) > authority::rank(granted_ceiling) {
            return Err(KipError::not_authorized(format!(
                "{class:?} exceeds this Principal's influence-authority ceiling {granted_ceiling:?}"
            )));
        }
        let bound = inherited_ceiling(store, &element).await?;
        if authority::rank(class) > authority::rank(&bound) {
            return Err(KipError::not_authorized(format!(
                "{id} was derived from material capped at {bound:?}, so it cannot be raised to \
                 {class:?}. Transformation does not raise authority — elevate what it was \
                 derived from, or record an independent artifact"
            )));
        }
    }

    let op = if raising { "elevate" } else { "downgrade" };
    let patch = |governance: &Json| set_member(governance, AUTHORITY_KEY, Json::from(class));
    let version = commit(store, space_id, element, op, None, patch, auth).await?;
    audit(
        store,
        space_id,
        id,
        if raising {
            "elevate_authority"
        } else {
            "downgrade_authority"
        },
        // §130: an elevation record names the artifact, both ceilings, who
        // decided, and when. The transaction and the audit entry supply the
        // rest between them.
        serde_json::json!({"from": current, "to": class, "version": version}),
        auth,
    )
    .await?;
    super::approval::consume(store, &decision).await?;
    Ok(current)
}

/// Holds an element out of ordinary use, pending review (§133).
///
/// Not a retraction and not an archive: it says *local Governance does not
/// currently allow ordinary use of this*, which is a statement about this Brain
/// and not about the source (§134).
pub async fn quarantine(
    store: &Store,
    space_id: &str,
    id: ElementId,
    reason: &str,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<(), KipError> {
    let element = readable(store, space_id, id, authority, auth).await?;
    let resource = ResourceContext::of_element(&element);
    let decision = super::approval::resolve(
        store,
        space_id,
        &resource,
        authority.authorize(Permission::Quarantine, &resource, auth),
        auth,
    )
    .await?
    .into_result()?;
    let reason = reason.to_string();
    let patch =
        |governance: &Json| set_member(governance, QUARANTINE_KEY, Json::from(reason.as_str()));
    let version = commit(
        store,
        space_id,
        element,
        "quarantine",
        Some(state::QUARANTINED),
        patch,
        auth,
    )
    .await?;
    audit(
        store,
        space_id,
        id,
        "quarantine",
        serde_json::json!({"reason": reason, "version": version}),
        auth,
    )
    .await?;
    super::approval::consume(store, &decision).await?;
    Ok(())
}

/// Returns a quarantined element to ordinary use.
pub async fn release(
    store: &Store,
    space_id: &str,
    id: ElementId,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<(), KipError> {
    let element = readable(store, space_id, id, authority, auth).await?;
    if element.state() != state::QUARANTINED {
        return Err(KipError::invalid_lifecycle_transition(format!(
            "{id} is {:?}, not quarantined; releasing it would silently revive an element that \
             was archived or tombstoned for a different reason",
            element.state()
        )));
    }
    let resource = ResourceContext::of_element(&element);
    let decision = super::approval::resolve(
        store,
        space_id,
        &resource,
        authority.authorize(Permission::Quarantine, &resource, auth),
        auth,
    )
    .await?
    .into_result()?;
    let patch = |governance: &Json| set_member(governance, QUARANTINE_KEY, Json::Null);
    let version = commit(
        store,
        space_id,
        element,
        "release",
        Some(state::ACTIVE),
        patch,
        auth,
    )
    .await?;
    audit(
        store,
        space_id,
        id,
        "release_quarantine",
        serde_json::json!({"version": version}),
        auth,
    )
    .await?;
    super::approval::consume(store, &decision).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

/// Loads an element the caller is entitled to see.
///
/// Reading it is the floor for every operation here: a caller who may not see
/// an element must not be able to learn what it is classified as by trying to
/// change it.
async fn readable(
    store: &Store,
    space_id: &str,
    id: ElementId,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<Element, KipError> {
    let element = store.get_element(id).await?;
    if element.space() != space_id {
        return Err(KipError::not_found_or_not_visible(format!(
            "{id} does not live in {space_id}"
        )));
    }
    authority
        .authorize(
            Permission::Read,
            &ResourceContext::of_element(&element),
            auth,
        )
        .into_result()?;
    Ok(element)
}

/// The lowest ceiling among the material this element was derived from.
///
/// An element with no recorded lineage is not derived from anything this engine
/// knows about, so nothing bounds it beyond policy. An input that has since been
/// erased bounds it at the bottom: authority that cannot be verified is
/// authority that is not held.
async fn inherited_ceiling(store: &Store, element: &Element) -> Result<String, KipError> {
    let lineage = lineage_of(element);
    if lineage.is_empty() {
        return Ok(authority::EXECUTABLE.to_string());
    }
    let mut bound = authority::EXECUTABLE.to_string();
    for reference in lineage {
        let Ok(id) = reference.parse::<ElementId>() else {
            continue;
        };
        let input = match store.get_element(id).await {
            Ok(input) => input,
            Err(_) => return Ok(authority::DESCRIPTIVE.to_string()),
        };
        bound = authority::meet(&bound, ceiling_of(&input)).to_string();
    }
    Ok(bound)
}

/// Writes a Governance patch onto an element as its own transaction.
async fn commit<F>(
    store: &Store,
    space_id: &str,
    element: Element,
    op: &'static str,
    new_state: Option<&str>,
    patch: F,
    auth: &AuthContext,
) -> Result<u64, KipError>
where
    F: Fn(&Json) -> Json,
{
    let cx = store
        .begin_transaction(space_id, engine_origin(auth))
        .await?;
    macro_rules! write {
        ($row:expr) => {
            put(store, &cx, *$row, op, new_state, patch).await
        };
    }
    match element {
        Element::Concept(row) => write!(row),
        Element::Proposition(row) => write!(row),
        Element::Assertion(row) => write!(row),
        Element::Evidence(row) => write!(row),
        Element::Activity(row) => write!(row),
    }
}

/// Writes one row's new Governance block and records the version.
///
/// The version log entry is appended in the same commit as the row, for the
/// same reason every cognitive write does it: a history written afterwards can
/// be missing exactly the change a crash interrupted, and a history with a hole
/// answers `AS OF` wrongly instead of refusing.
async fn put<R, F>(
    store: &Store,
    cx: &crate::store::write::WriteContext,
    mut row: R,
    op: &'static str,
    new_state: Option<&str>,
    patch: F,
) -> Result<u64, KipError>
where
    R: crate::store::write::Row,
    F: Fn(&Json) -> Json,
{
    {
        let envelope = row.envelope_mut();
        let updated = patch(envelope.governance);
        *envelope.governance = updated;
        if let Some(state) = new_state {
            *envelope.state = state.to_string();
        }
    }
    let version = store.update(cx, &mut row).await?;
    let id = ElementId::new(R::KIND, row.id());
    store.record_version(cx, id, version, op, &row).await?;
    let schema_environment_version = store.get_space(&cx.space).await?.schema_environment_version;
    store
        .journal(
            cx,
            crate::store::space::JournalEntry {
                status: "committed".to_string(),
                transaction_class: "governance".to_string(),
                schema_environment_version,
                result: serde_json::json!({"element": id.to_string(), "op": op}),
                changes: vec![serde_json::json!({
                    "id": id.to_string(),
                    "kind": id.kind.to_string(),
                    "op": op,
                    "version": version,
                })],
                ..Default::default()
            },
        )
        .await?;
    Ok(version)
}

/// Merges one member into an element's Governance block.
///
/// A merge rather than a replacement, and `null` removes rather than stores:
/// classification, authority ceiling and policy reference are separate
/// decisions under separate permissions, and changing one must not silently
/// drop another.
fn set_member(governance: &Json, key: &str, value: Json) -> Json {
    let mut object = governance
        .as_object()
        .cloned()
        .unwrap_or_else(serde_json::Map::new);
    if value.is_null() {
        object.remove(key);
    } else {
        object.insert(key.to_string(), value);
    }
    Json::Object(object)
}

async fn audit(
    store: &Store,
    space_id: &str,
    id: ElementId,
    operation: &'static str,
    mut record: Json,
    auth: &AuthContext,
) -> Result<(), KipError> {
    if let Some(object) = record.as_object_mut() {
        object.insert("element".to_string(), Json::from(id.to_string()));
    }
    store
        .governance
        .record_mutation(MutationEntry {
            operation,
            at: crate::time::now(),
            space_id: space_id.to_string(),
            resource: id.to_string(),
            principal_id: auth.principal_id.clone(),
            record,
        })
        .await
        .map(|_| ())
}

/// The engine origin a Governance write stamps.
fn engine_origin(auth: &AuthContext) -> Json {
    serde_json::json!({
        "principal_id": auth.principal_id,
        "channel": "governance",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setting_one_member_keeps_the_others() {
        let before = serde_json::json!({"policy_ref": "policy-7"});
        let after = set_member(
            &before,
            "classification",
            Json::from(classification::SECRET),
        );
        assert_eq!(after["policy_ref"], "policy-7");
        assert_eq!(after["classification"], classification::SECRET);
    }

    #[test]
    fn a_null_value_removes_the_member() {
        let before = serde_json::json!({"quarantine_reason": "under review"});
        let after = set_member(&before, QUARANTINE_KEY, Json::Null);
        assert!(after.get(QUARANTINE_KEY).is_none());
    }

    #[test]
    fn a_block_that_was_absent_becomes_one() {
        let after = set_member(&Json::Null, "classification", Json::from("public"));
        assert_eq!(after["classification"], "public");
    }
}
