/**
 * The Governance Control Plane.
 *
 * One import for the vocabularies a decision ranks against, the permission
 * registry it resolves on, and the protected row shapes it reads. The storage
 * that holds those rows lives in `store/governance.ts`, beside the cognitive
 * store it shares a database and a transaction with.
 *
 * @see ./lattice.ts for what this plane is and why it is separate
 */

export {
  ANONYMOUS_PRINCIPAL,
  ANY_SPACE,
  SYSTEM_PRINCIPAL,
  assurance,
  authStrength,
  authority,
  bindingClass,
  classification,
  govStatus,
  isPermitted,
  principalClass,
  purposeAssurance,
  type Decision,
} from './lattice.js'

export {
  anonymousAuth,
  effectivePurpose,
  isAuthenticated,
  mergeRequestContext,
  principalAuth,
  systemAuth,
  type AuthContext,
  type RequestContext,
} from './auth.js'

export {
  EffectiveAuthority,
  authorityCeiling,
  isSpaceScope,
  requirePermitted,
  resourceOf,
  resourceOfElement,
  spaceResource,
  type Authorization,
  type ResourceContext,
} from './decision.js'

export { resolveApproval, subjectDigest } from './approval.js'

export { redactView } from './redact.js'

export {
  AUTHORITY_KEY,
  LINEAGE_KEY,
  QUARANTINE_KEY,
  ceilingOf,
  classify,
  elevateAuthority,
  lineageOf,
  quarantine,
  release,
  type ElementGovernanceContext,
} from './element.js'

export {
  clausePermissions,
  kmlPermissions,
  kqlPermissions,
  metaPermissions,
} from './gate.js'

export {
  ALL_PERMISSIONS,
  PERMISSIONS,
  describePermission,
  familyOf,
  isAlwaysAudited,
  isPermission,
  parsePermission,
  type Family,
  type Permission,
} from './permission.js'

export {
  approvalId,
  asConditions,
  asConstraints,
  asObligations,
  asScope,
  asStatement,
  auditId,
  bindingId,
  conditionsContain,
  delegationId,
  emptyConditions,
  emptyConstraints,
  emptyObligations,
  emptyScope,
  grantId,
  inForceAt,
  mergeObligations,
  rowIdOf,
  scopeContains,
  scopeIntersect,
  scopeIsEmpty,
  tightenConstraints,
  type ActorBindingRow,
  type ApprovalRow,
  type AuthorityConditions,
  type AuthorityConstraints,
  type AuthorityScope,
  type DelegationRow,
  type GovernanceAuditRow,
  type GovernancePolicyRow,
  type GrantRow,
  type PolicyObligations,
  type PolicyStatement,
  type PrincipalGroupRow,
  type PrincipalRow,
} from './rows.js'
