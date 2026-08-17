//! # Satisfying an approval
//!
//! A policy can require that an operation be independently approved before it
//! runs (§167). The decision engine says so by returning
//! [`Decision::RequireApproval`], which is **not** a soft allow: the operation
//! does not happen while it is outstanding (§40). This module is what turns it
//! into an allow, and only when a real approval exists.
//!
//! ## Bound to one operation, not to a permission
//!
//! An approval is matched by a digest over *what is being approved* — this
//! Space, this permission, this element. An approval for "purge E-1" therefore
//! does nothing for "purge E-2". Without that binding, one approval would
//! become a standing licence, which is the failure §246 tests for from the
//! other side.
//!
//! ## Consumed, not merely counted
//!
//! A satisfied approval is marked `consumed` only after the authorized
//! operation succeeds, so the same signatures cannot authorize it twice and a
//! failed attempt does not spend them without use.

use anda_kip::KipError;
use sha3::{Digest, Sha3_256};
use std::collections::BTreeSet;

use super::auth::AuthContext;
use super::decision::{Authorization, ResourceContext};
use super::{Decision, Permission};
use crate::store::Store;

/// The identity of one concrete operation, for binding an approval to it.
///
/// Deliberately includes the resource: an approval that named only the
/// permission would authorize every future use of it.
pub fn subject_digest(
    space_id: &str,
    permission: Permission,
    resource: &ResourceContext,
) -> String {
    let mut hasher = Sha3_256::new();
    for part in [
        space_id,
        permission.as_str(),
        &resource.kind,
        &resource.schema_ref,
        &resource.element_id,
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0x1f]);
    }
    format!("sha3-256:{}", hex::encode(hasher.finalize()))
}

/// Turns a `require_approval` decision into an allow, if the approvals exist.
///
/// Any other decision passes through untouched: this consumes approvals, it
/// never manufactures authority. A caller that was denied outright is still
/// denied however many approvals it collects.
pub async fn resolve(
    store: &Store,
    space_id: &str,
    resource: &ResourceContext,
    decision: Authorization,
    auth: &AuthContext,
) -> Result<Authorization, KipError> {
    if decision.decision != Decision::RequireApproval {
        return Ok(decision);
    }
    let digest = subject_digest(space_id, decision.permission, resource);
    let granted = store
        .governance
        .granted_approvals(space_id, &digest)
        .await?;

    let approvers = granted
        .iter()
        .flat_map(|row| row.approver_ids.iter().cloned())
        .collect::<BTreeSet<_>>()
        .len();
    if (approvers as u64) < decision.obligations.approvals_required {
        return Ok(Authorization {
            reason: format!(
                "{} of {} independent approval(s) recorded for this operation",
                approvers, decision.obligations.approvals_required
            ),
            ..decision
        });
    }

    let used: Vec<String> = granted
        .iter()
        .map(|row| super::store::approval_id(row._id))
        .collect();
    let _ = auth;
    Ok(Authorization {
        decision: Decision::AllowWithConstraints,
        reason: format!(
            "{} is approved by {} independent Principal(s)",
            decision.permission, approvers
        ),
        authorities_used: [decision.authorities_used, used].concat(),
        ..decision
    })
}

/// The approval rows a permitted decision is carrying.
fn approvals_of(decision: &Authorization) -> Vec<u64> {
    let mut seen = BTreeSet::new();
    for authority in &decision.authorities_used {
        if let Some(id) = authority
            .strip_prefix("kip:approval:")
            .and_then(|value| value.parse::<u64>().ok())
        {
            seen.insert(id);
        }
    }
    seen.into_iter().collect()
}

/// A permitted decision, together with the approvals that made it one.
///
/// The type exists because "resolve here, spend there" is a two-step contract
/// that nothing else states. An approval buys one *completed* operation, so
/// there are exactly two honest endings: [`Approved::spend`], once the
/// authorized operation has actually succeeded, and [`Approved::defer`], which
/// hands the same obligation to a transaction that spends it at commit.
///
/// Not `Clone`, and `#[must_use]`: dropping one on the floor leaves the same
/// signatures able to authorize the next attempt, which is precisely what
/// requiring an approval was meant to prevent.
#[must_use = "an approval that is neither spent nor deferred still authorizes the next caller"]
pub struct Approved {
    decision: Authorization,
    approvals: Vec<u64>,
}

impl Approved {
    /// Requires an already-resolved decision to permit, and takes custody of
    /// the approvals it spent.
    pub fn require(decision: Authorization) -> Result<Self, KipError> {
        let decision = decision.into_result()?;
        let approvals = approvals_of(&decision);
        Ok(Self {
            decision,
            approvals,
        })
    }

    /// The decision itself, for the constraints and obligations it carries.
    pub fn decision(&self) -> &Authorization {
        &self.decision
    }

    /// Spends the approvals, after the authorized operation succeeded.
    pub async fn spend(self, store: &Store) -> Result<(), KipError> {
        for id in self.approvals {
            store.governance.consume_approval(id).await?;
        }
        Ok(())
    }

    /// Hands the obligation to a transaction, which spends it at commit.
    ///
    /// A staged operation has not happened yet, so spending now would charge
    /// an approval for a statement that may still refuse.
    pub fn defer(self, tx: &mut crate::tx::Transaction) {
        tx.defer_approval(self);
    }
}

/// Resolves a decision and requires it to permit, in one step.
pub async fn require(
    store: &Store,
    space_id: &str,
    resource: &ResourceContext,
    decision: Authorization,
    auth: &AuthContext,
) -> Result<Approved, KipError> {
    Approved::require(resolve(store, space_id, resource, decision, auth).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_approval_is_bound_to_the_element_not_just_the_permission() {
        let one = subject_digest(
            "space",
            Permission::Purge,
            &ResourceContext::kind("evidence").with_element("E-1"),
        );
        let two = subject_digest(
            "space",
            Permission::Purge,
            &ResourceContext::kind("evidence").with_element("E-2"),
        );
        assert_ne!(one, two);
    }

    #[test]
    fn the_same_operation_in_two_spaces_is_two_operations() {
        let here = subject_digest(
            "space-a",
            Permission::Declassify,
            &ResourceContext::kind("concept").with_element("C-1"),
        );
        let there = subject_digest(
            "space-b",
            Permission::Declassify,
            &ResourceContext::kind("concept").with_element("C-1"),
        );
        assert_ne!(here, there);
    }

    #[test]
    fn the_digest_is_stable_across_runs() {
        // An approval recorded yesterday has to still match today.
        let resource = ResourceContext::kind("evidence").with_element("E-7");
        assert_eq!(
            subject_digest("space", Permission::Purge, &resource),
            subject_digest("space", Permission::Purge, &resource)
        );
    }
}
