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
