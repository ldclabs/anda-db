/**
 * # Satisfying an approval
 *
 * A policy can require that an operation be independently approved before it
 * runs (§167). The decision engine says so by returning `require_approval`,
 * which is **not** a soft allow: the operation does not happen while it is
 * outstanding (§40). This module is what turns it into an allow, and only when a
 * real approval exists.
 *
 * ## Bound to one operation, not to a permission
 *
 * An approval is matched by a digest over *what is being approved* — this Space,
 * this permission, this element. An approval for "purge E-1" therefore does
 * nothing for "purge E-2". Without that binding, one approval would become a
 * standing licence, which is the failure §246 tests for from the other side.
 *
 * ## Consumed, not merely counted
 *
 * A satisfied approval is marked `consumed`, so the same two signatures cannot
 * authorize the operation twice. Re-running it needs a new approval — which is
 * the point of requiring one.
 *
 * @see rs/anda_cognitive_nexus/src/governance/approval.rs
 */

import { digestParts } from '../digest.js'
import type { Store } from '../store/index.js'
import type { AuthContext } from './auth.js'
import type { Authorization, ResourceContext } from './decision.js'
import type { Permission } from './permission.js'
import { approvalId } from './rows.js'

/**
 * The identity of one concrete operation, for binding an approval to it.
 *
 * Deliberately includes the resource: an approval that named only the permission
 * would authorize every future use of it.
 *
 * The hash is this engine's own — the reference engine spells the same digest
 * with SHA3-256 — because nothing compares these across engines. An approval is
 * matched against rows in the same database that produced it, and a shared
 * canonicalization would be a cost paid for an interop that does not exist here.
 */
export function subjectDigest(
  spaceId: string,
  permission: Permission,
  resource: ResourceContext,
): string {
  // Length-prefixed rather than concatenated, so no two different operations can
  // be spelled into each other by one field's tail running into the next.
  return `sha256:${digestParts([
    spaceId,
    permission,
    resource.kind,
    resource.schema_ref,
    resource.element_id,
  ])}`
}

/**
 * Turns a `require_approval` decision into an allow, if the approvals exist.
 *
 * Any other decision passes through untouched: this consumes approvals, it never
 * manufactures authority. A caller that was denied outright is still denied
 * however many approvals it collects.
 */
export function resolveApproval(
  store: Store,
  spaceId: string,
  resource: ResourceContext,
  decision: Authorization,
  auth: AuthContext,
): Authorization {
  if (decision.decision !== 'require_approval') return decision

  const digest = subjectDigest(spaceId, decision.permission, resource)
  const granted = store.governance.grantedApprovals(spaceId, digest)

  const approvers = granted.reduce((total, row) => total + row.approver_ids.length, 0)
  if (approvers < decision.obligations.approvals_required) {
    // §246: one approval where two are required is not partial activation. The
    // decision stays `require_approval`, and the reason says how far along it is.
    return {
      ...decision,
      reason:
        `${approvers} of ${decision.obligations.approvals_required} independent ` +
        `approval(s) recorded for this operation`,
    }
  }

  const used: string[] = []
  for (const row of granted) {
    store.governance.consumeApproval(row.id)
    used.push(approvalId(row.id))
  }
  void auth
  return {
    ...decision,
    decision: 'allow_with_constraints',
    authorities_used: [...decision.authorities_used, ...used],
    reason: `${decision.permission} is approved by ${approvers} independent Principal(s)`,
  }
}
