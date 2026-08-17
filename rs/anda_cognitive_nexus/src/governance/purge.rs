//! # Physical erasure
//!
//! `PURGE` is the one operation here that destroys something. Everything else
//! in KIP 2.0 is additive or lifecycle: archiving removes from recall,
//! tombstoning removes logically, superseding replaces, and all three leave the
//! record of what happened intact. Purge exists because privacy and legal
//! obligations sometimes outrank that (§164), and it is deliberately the most
//! guarded operation in the engine.
//!
//! ## What is erased, and what survives
//!
//! ```text
//! erased    the element's content columns
//!           every historical version of it in the version log
//!
//! kept      the element's identity, kind and Space
//!           a digest of what was there
//!           the fact that it was purged, by whom, when
//! ```
//!
//! The stub is what §19.3 asks for: *purge SHOULD leave a digest stub so audit
//! and provenance-root identity survive byte destruction*. Deleting the row
//! outright would break every reference to it — and a dangling reference does
//! not say "this was erased", it says nothing at all, which is worse for an
//! auditor and worse for a reader.
//!
//! **The version log is the part that is easy to forget.** Every commit appends
//! the whole row it wrote, so an element purged from its current row would still
//! be fully readable through `AS OF`. A purge that left that behind would be a
//! purge in name only.
//!
//! ## Why the default refuses
//!
//! `REFERENCE POLICY` defaults to `deny_if_referenced` (§173) because in a
//! cognitive history an Assertion, an Activity or an Experience may point at the
//! target, and erasing the whole dependency chain falsifies history (§175). KIP
//! 1.x made destructive cascade ordinary; 2.0 deliberately does not.

use anda_kip::{Json, KipError};
use std::collections::BTreeSet;

use super::Permission;
use super::decision::ResourceContext;
use super::store::MutationEntry;
use crate::id::ElementId;
use crate::store::{Element, Store};
use crate::tx::Transaction;

/// How a purge treats elements that still point at the target (§173).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferencePolicy {
    /// Refuse while anything references the target. The default.
    DenyIfReferenced,
    /// Erase the target and leave its stub for references to resolve to.
    TombstoneReference,
    /// Erase the target and its dependents. Requires approval.
    AuthorizedCascade,
}

impl ReferencePolicy {
    /// Reads the `REFERENCE POLICY` clause; absent means the conservative one.
    pub fn parse(name: Option<&str>) -> Result<Self, KipError> {
        match name {
            None | Some("deny_if_referenced") => Ok(Self::DenyIfReferenced),
            Some("tombstone_reference") => Ok(Self::TombstoneReference),
            Some("authorized_cascade") => Ok(Self::AuthorizedCascade),
            Some(other) => Err(KipError::constraint_violation(format!(
                "{other:?} is not a reference policy; this engine implements \
                 deny_if_referenced, tombstone_reference and authorized_cascade"
            ))),
        }
    }

    /// The wire name, for receipts and audit.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DenyIfReferenced => "deny_if_referenced",
            Self::TombstoneReference => "tombstone_reference",
            Self::AuthorizedCascade => "authorized_cascade",
        }
    }
}

/// What one purge did.
#[derive(Clone, Debug, Default)]
pub struct PurgeReport {
    /// The elements whose content was erased.
    pub purged: Vec<String>,
    /// How many historical versions were destroyed with them.
    pub versions_destroyed: usize,
}

/// Stages erasure inside an existing KML transaction.
pub async fn stage(
    store: &Store,
    tx: &mut Transaction,
    id: ElementId,
    policy: ReferencePolicy,
) -> Result<PurgeReport, KipError> {
    let space_id = tx.cx.space.clone();
    // `Transaction::load` is the one reader that also enforces the Space, so
    // going through it here replaces a separate fetch and its own check.
    let element = tx.load(id).await?.clone();
    let resource = ResourceContext::of_element(&element);
    tx.authorize_element(id, Permission::Read).await?;
    // §167 lists purging critical Evidence among the operations a policy may
    // require independent approval for, and this is where such an approval is
    // consumed — bound to this element, and spent by using it.
    let decision = super::approval::resolve(
        store,
        &space_id,
        &resource,
        tx.authority
            .authorize(Permission::Purge, &resource, &tx.auth),
        &tx.auth,
    )
    .await?
    .into_result()?;

    // §163: a legal hold is exactly the thing purge must not walk past, and it
    // is checked before anything else destructive is decided.
    if has_legal_hold(&element) {
        return Err(KipError::legal_hold_conflict(format!(
            "{id} is under a legal hold; lifting the hold is a separate Governance decision \
             under its own permission"
        )));
    }

    let referrers = referrers_of(store, &space_id, id).await?;
    let mut targets = vec![id];
    match policy {
        ReferencePolicy::DenyIfReferenced if !referrers.is_empty() => {
            // Names how many, not which: the referring elements may be ones
            // this caller cannot read, and a purge refusal must not become a
            // way to enumerate them (§103).
            return Err(KipError::purge_denied(format!(
                "{} element(s) still reference {id}. Erasing a referenced element leaves a \
                 history that points at nothing; choose REFERENCE POLICY \
                 \"tombstone_reference\" to keep the identity stub, or \"authorized_cascade\" to \
                 erase the dependents too",
                referrers.len()
            )));
        }
        ReferencePolicy::AuthorizedCascade => {
            for referrer in &referrers {
                let element = store.get_element(*referrer).await?;
                if has_legal_hold(&element) {
                    return Err(KipError::legal_hold_conflict(format!(
                        "{referrer} depends on {id} and is under a legal hold, so this cascade \
                         cannot complete"
                    )));
                }
                tx.authorize_element(*referrer, Permission::Purge).await?;
            }
            targets.extend(referrers.iter().copied());
        }
        _ => {}
    }

    tx.defer_approval(decision);
    let mut report = PurgeReport::default();
    for target in targets {
        let element = tx.load(target).await?.clone();
        let digest = crate::store::schema::content_digest(&crate::view::render(&element));
        report.versions_destroyed += tx.stage_purge(target, stub(element, &digest)).await?;
        report.purged.push(target.to_string());

        tx.defer_governance_audit(MutationEntry {
            operation: "purge",
            at: tx.cx.at.clone(),
            space_id: space_id.to_string(),
            resource: target.to_string(),
            principal_id: tx.auth.principal_id.clone(),
            // The receipt §164 permits: enough to audit the erasure, and
            // nothing of what was erased.
            record: serde_json::json!({
                "element": target.to_string(),
                "content_digest": digest,
                "reference_policy": policy.as_str(),
                "tx_id": tx.cx.tx_id,
            }),
        });
    }
    Ok(report)
}

/// Whether an element is held against erasure (§82, §163).
pub fn has_legal_hold(element: &Element) -> bool {
    element
        .retention()
        .get("legal_hold")
        .and_then(Json::as_bool)
        .unwrap_or(false)
}

/// Every element in the Space that points at this one.
///
/// Index lookups rather than a scan: each reference an element can hold has a
/// key column beside it, which is what makes a provenance-aware purge planner
/// possible at all (§166).
async fn referrers_of(
    store: &Store,
    space_id: &str,
    id: ElementId,
) -> Result<Vec<ElementId>, KipError> {
    let mut found: BTreeSet<ElementId> = BTreeSet::new();
    for referrer in store.referrers(space_id, id).await? {
        if referrer != id {
            found.insert(referrer);
        }
    }
    Ok(found.into_iter().collect())
}

/// Replaces a row with its identity stub.
///
/// A fresh default row with the envelope carried across, rather than the old
/// row with its fields cleared: a default has every column empty by
/// construction, so a column added later cannot be forgotten here and quietly
/// survive erasure.
///
/// `origin` is kept — it names the Principal that wrote the element, which is
/// audit information about the deployment rather than the content being erased,
/// and losing it would make the stub unattributable.
fn stub(element: Element, digest: &str) -> Element {
    macro_rules! erase {
        ($variant:ident, $ty:ident, $previous:expr $(, $extra:ident = $value:expr)?) => {{
            let previous = *$previous;
            let row = $ty {
                _id: previous._id,
                space: previous.space,
                state: state::PURGED.to_string(),
                version: previous.version,
                seq: previous.seq,
                created_at: previous.created_at,
                updated_at: previous.updated_at,
                created_tx: previous.created_tx,
                updated_tx: previous.updated_tx,
                origin: previous.origin,
                governance: purge_marker(digest),
                $($extra: $value,)?
                ..Default::default()
            };
            Element::$variant(Box::new(row))
        }};
    }
    use crate::store::rows::*;
    match element {
        Element::Concept(row) => erase!(Concept, ConceptRow, row),
        // `tuple_key` is unique-indexed, so two purged Propositions would
        // collide on the empty string. The stub keeps a per-element placeholder
        // that carries none of the tuple it replaced.
        Element::Proposition(row) => {
            let placeholder = format!("purged:{}", row._id);
            erase!(Proposition, PropositionRow, row, tuple_key = placeholder)
        }
        Element::Assertion(row) => erase!(Assertion, AssertionRow, row),
        Element::Evidence(row) => erase!(Evidence, EvidenceRow, row),
        Element::Activity(row) => erase!(Activity, ActivityRow, row),
    }
}

/// The Governance block a purged stub carries.
fn purge_marker(digest: &str) -> Json {
    serde_json::json!({
        "purged": true,
        "content_digest": digest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_reference_policy_refuses() {
        // §173, §174: KIP 1.x made destructive cascade ordinary. 2.0 does not.
        assert_eq!(
            ReferencePolicy::parse(None).unwrap(),
            ReferencePolicy::DenyIfReferenced
        );
    }

    #[test]
    fn an_unknown_reference_policy_is_refused_not_defaulted() {
        // Defaulting would silently run a destructive operation under a policy
        // the caller did not ask for.
        assert!(ReferencePolicy::parse(Some("delete_everything")).is_err());
    }

    #[test]
    fn a_purge_marker_carries_a_digest_and_no_content() {
        let marker = purge_marker("abc123");
        assert_eq!(marker["content_digest"], "abc123");
        assert_eq!(marker["purged"], true);
        assert_eq!(marker.as_object().unwrap().len(), 2);
    }
}
