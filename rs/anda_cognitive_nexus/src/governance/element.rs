//! # Changing an element's own Governance members
//!
//! An element's `governance` block — its classification and policy reference —
//! is not an author-writable field. `anda_kip`'s parser refuses it in every
//! assignment, on the text path and on the pre-parsed AST path alike, so no KML
//! statement can reach it (§50, §51). This module is the other half: the
//! authorized way it *does* change.
//!
//! ## Which permission, and why it depends on the direction
//!
//! ```text
//! raising a classification    update      more restrictive: safe
//! lowering a classification   declassify  a disclosure decision (§100)
//! setting a policy_ref        manage_policy
//! ```
//!
//! Raising is deliberately ordinary. An agent that notices it has written
//! something sensitive should be able to say so without a Governance ticket;
//! it is the direction that *reveals* things that needs authority. Making both
//! directions privileged would make labelling rare, and unlabelled content
//! defaults to the Space's classification rather than to the truth.
//!
//! ## Why it commits as a transaction
//!
//! A classification change writes a new element version and takes a Space
//! sequence, exactly as a cognitive write does. That is what keeps §177
//! answerable — *what classification did this element have then* — and what
//! puts the change in the authorized change stream (§186). It is recorded in
//! the Governance audit as well, because the two logs answer different
//! questions: the version log says what the element looked like, the audit says
//! who decided that and under which policy.

use anda_kip::{Json, KipError};

use super::auth::AuthContext;
use super::decision::{EffectiveAuthority, ResourceContext};
use super::store::MutationEntry;
use super::{Permission, classification};
use crate::id::ElementId;
use crate::store::{Element, Store};

/// Sets one element's classification label.
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
    let element = store.get_element(id).await?;
    if element.space() != space_id {
        return Err(KipError::not_found_or_not_visible(format!(
            "{id} does not live in {space_id}"
        )));
    }
    // Reading it is the floor: a caller who may not see the element may not
    // learn what it is classified as by trying to change it.
    let resource = ResourceContext::of_element(&element);
    authority
        .authorize(Permission::Read, &resource, auth)
        .into_result()?;

    let current = element.classification().to_string();
    let effective = if current.is_empty() {
        authority.default_classification()
    } else {
        current.as_str()
    };
    let permission = if classification::rank(label) < classification::rank(effective) {
        Permission::Declassify
    } else {
        Permission::Update
    };
    authority
        .authorize(permission, &resource, auth)
        .into_result()?;

    let cx = store
        .begin_transaction(space_id, engine_origin(auth))
        .await?;
    let op = if permission == Permission::Declassify {
        "declassify"
    } else {
        "classify"
    };
    let version = write_classification(store, &cx, element, label, op).await?;

    store
        .governance
        .record_mutation(MutationEntry {
            operation: if op == "declassify" {
                "declassify"
            } else {
                "classify"
            },
            space_id: space_id.to_string(),
            resource: id.to_string(),
            principal_id: auth.principal_id.clone(),
            record: serde_json::json!({
                "element": id.to_string(),
                "from": current,
                "to": label,
                "version": version,
                "tx_id": cx.tx_id,
            }),
        })
        .await?;
    Ok(current)
}

/// Writes the new label onto whichever row this is, versioning it.
async fn write_classification(
    store: &Store,
    cx: &crate::store::write::WriteContext,
    element: Element,
    label: &str,
    op: &str,
) -> Result<u64, KipError> {
    match element {
        Element::Concept(row) => put(store, cx, *row, label, op).await,
        Element::Proposition(row) => put(store, cx, *row, label, op).await,
        Element::Assertion(row) => put(store, cx, *row, label, op).await,
        Element::Evidence(row) => put(store, cx, *row, label, op).await,
        Element::Activity(row) => put(store, cx, *row, label, op).await,
    }
}

/// Writes one row's new Governance block and records the version.
///
/// The version log entry is appended in the same commit as the row, for the
/// same reason every cognitive write does it: a history written afterwards can
/// be missing exactly the change a crash interrupted, and a history with a hole
/// answers `AS OF` wrongly instead of refusing.
async fn put<R: crate::store::write::Row>(
    store: &Store,
    cx: &crate::store::write::WriteContext,
    mut row: R,
    label: &str,
    op: &str,
) -> Result<u64, KipError> {
    {
        let envelope = row.envelope_mut();
        let updated = with_classification(envelope.governance, label);
        *envelope.governance = updated;
    }
    let version = store.update(cx, &mut row).await?;
    let id = ElementId::new(R::KIND, row.id());
    store.record_version(cx, id, version, op, &row).await?;
    Ok(version)
}

/// Merges a classification into an element's Governance block.
///
/// A merge rather than a replacement: `policy_ref` is a separate decision under
/// a separate permission, and a classification change must not silently drop it.
fn with_classification(governance: &Json, label: &str) -> Json {
    let mut object = governance
        .as_object()
        .cloned()
        .unwrap_or_else(serde_json::Map::new);
    object.insert(
        "classification".to_string(),
        Json::String(label.to_string()),
    );
    Json::Object(object)
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
    fn setting_a_classification_keeps_the_policy_reference() {
        let before = serde_json::json!({"policy_ref": "policy-7"});
        let after = with_classification(&before, classification::SECRET);
        assert_eq!(after["policy_ref"], "policy-7");
        assert_eq!(after["classification"], classification::SECRET);
    }

    #[test]
    fn a_block_that_was_absent_becomes_one() {
        let after = with_classification(&Json::Null, classification::PUBLIC);
        assert_eq!(after["classification"], classification::PUBLIC);
    }
}
