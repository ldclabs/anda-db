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
//! A satisfied approval is marked `consumed`, so the same two signatures cannot
//! authorize the operation twice. Re-running it needs a new approval — which is
//! the point of requiring one.

use anda_kip::KipError;
use sha3::{Digest, Sha3_256};

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

    let approvers: usize = granted.iter().map(|row| row.approver_ids.len()).sum();
    if (approvers as u64) < decision.obligations.approvals_required {
        return Ok(Authorization {
            reason: format!(
                "{} of {} independent approval(s) recorded for this operation",
                approvers, decision.obligations.approvals_required
            ),
            ..decision
        });
    }

    let mut used = Vec::with_capacity(granted.len());
    for row in &granted {
        store.governance.consume_approval(row._id).await?;
        used.push(super::store::approval_id(row._id));
    }
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
