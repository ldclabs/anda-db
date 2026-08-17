/**
 * # Deciding whether an operation may happen
 *
 * The resolution order is §42's, and the order is the security property:
 *
 * ```text
 * protocol invariant
 *     ↓
 * matching explicit deny
 *     ↓
 * matching allow: owner, Grant, Delegation, or Policy statement
 *     ↓
 * default deny
 * ```
 *
 * Nothing about cognitive content appears anywhere in it. A Concept's
 * attributes, an Assertion's confidence and a Proposition's subject are not
 * inputs to authorization, which is the whole of "cognitive content may describe
 * authority but only Governance can grant it" (§48).
 *
 * ## Resolved once per request, not once per element
 *
 * {@link EffectiveAuthority.resolve} does every read the control plane needs —
 * Principal, groups, Grants, Delegations, Policy — and then
 * {@link EffectiveAuthority.authorize} is pure. A read that filters ten thousand
 * candidate elements therefore costs one control-plane load, not ten thousand.
 *
 * It also means authority is re-resolved on every request, which is what makes
 * revocation take effect for a session that started before it (§188, §245).
 *
 * ## Why one allow is chosen rather than all of them merged
 *
 * Several authorities may permit the same operation. Each is independently
 * sufficient, so intersecting their constraints would let an unrelated narrow
 * Grant shrink what a broad one already allows. The least restrictive matching
 * allow is chosen, and its constraints are the decision's.
 *
 * Obligations go the other way and accumulate across every matching Policy
 * statement, because an obligation is what the deployment requires of the
 * operation rather than a limit on one authority (§184).
 *
 * @see rs/anda_cognitive_nexus/src/governance/decision.rs
 */

import { errors } from '../errors.js'
import { formatElementId } from '../id.js'
import {
  classificationOf,
  schemaRefOf,
  type Element,
  type SpaceRow,
  type Store,
} from '../store/index.js'
import { nowTime } from '../time.js'
import {
  ALL_PERMISSIONS,
  isAlwaysAudited,
  isPermission,
  type Permission,
} from './permission.js'
import type { AuthContext } from './auth.js'
import {
  authStrength,
  classification,
  govStatus,
  isPermitted,
  purposeAssurance,
  type Decision,
} from './lattice.js'
import {
  asConditions,
  asConstraints,
  asScope,
  asStatement,
  conditionsContain,
  delegationId,
  emptyConditions,
  emptyConstraints,
  emptyObligations,
  emptyScope,
  grantId,
  mergeObligations,
  rowIdOf,
  scopeContains,
  scopeIsEmpty,
  type ActorBindingRow,
  type AuthorityConditions,
  type AuthorityConstraints,
  type AuthorityScope,
  type DelegationRow,
  type GovernancePolicyRow,
  type GrantRow,
  type PolicyObligations,
  type PolicyStatement,
  type PrincipalRow,
} from './rows.js'

/**
 * How deep a Delegation chain may be walked.
 *
 * A bound rather than a cycle check, because both failure modes end the same
 * way: authority that cannot be resolved is authority that is not held.
 */
const MAX_DELEGATION_DEPTH = 8

/**
 * What an operation is being performed on (§90).
 *
 * Every field is "unset means unconstrained", so a Space-wide operation with no
 * particular target — running a query at all, publishing a policy — is expressed
 * by leaving them empty rather than by a second code path.
 */
export interface ResourceContext {
  /** The Core element kind, e.g. `concept`. */
  kind: string
  /** The exact Schema symbol reference. */
  schema_ref: string
  /** The element's classification label. */
  classification: string
  /** The element id. */
  element_id: string
}

/** A resource that names nothing in particular — the Space as a whole. */
export function spaceResource(): ResourceContext {
  return { kind: '', schema_ref: '', classification: '', element_id: '' }
}

/** A resource of one Core kind, optionally narrowed further. */
export function resourceOf(
  kind: string,
  extra: Partial<ResourceContext> = {},
): ResourceContext {
  return { ...spaceResource(), kind, ...extra }
}

/**
 * The resource one Cognitive Element presents to an authorization.
 *
 * Note what is *not* here: the element's name, attributes, confidence or
 * subject. Authorization reads the element's kind, type and classification and
 * nothing a cognitive writer controls the meaning of — which is the storage-level
 * shape of "cognitive content cannot grant authority" (§48).
 */
export function resourceOfElement(element: Element): ResourceContext {
  return {
    kind: element.kind.toLowerCase(),
    schema_ref: schemaRefOf(element),
    classification: classificationOf(element),
    element_id: formatElementId({ kind: element.kind, seq: element.row.id }),
  }
}

/**
 * Whether this names nothing in particular — the Space as a whole.
 *
 * The two authorization layers ask different questions, and this is what tells
 * them apart. A command gate asks *may this Principal do this here at all*, so a
 * Grant narrowed to one classification still lets the query run; the narrowing is
 * then applied element by element, where there is an element to apply it to.
 * Judging the command against a null resource would deny every scoped Grant its
 * own commands.
 */
export function isSpaceScope(resource: ResourceContext): boolean {
  return (
    resource.kind === '' &&
    resource.schema_ref === '' &&
    resource.classification === '' &&
    resource.element_id === ''
  )
}

/** A one-line description, for audit and for denial messages. */
function label(resource: ResourceContext): string {
  if (resource.element_id !== '') return resource.element_id
  if (resource.kind !== '') return resource.kind
  return 'the Space'
}

/** One authorization decision (§39). */
export interface Authorization {
  /** What it evaluated to. */
  decision: Decision
  /** The permission that was asked for. */
  permission: Permission
  /** What the operation may do, if it may happen. */
  constraints: AuthorityConstraints
  /** What the runtime must also do. */
  obligations: PolicyObligations
  /** The Policy that decided it, when one did. */
  policy_id: string
  /** That Policy's version. */
  policy_version: number
  /** The Grants and Delegations that matched. */
  authorities_used: string[]
  /**
   * Whether the authority that permitted this reaches everything.
   *
   * What a Space-wide answer — a count, a total — may be built from. A permitted
   * decision under a narrowed authority is still permitted; it just cannot be
   * the basis for a number that speaks for the whole Space (§106).
   */
  unrestricted: boolean
  /** Why, in one line. Safe to return to the caller (§267). */
  reason: string
}

/**
 * Turns a refusal into the error a caller sees.
 *
 * The message names the permission and nothing else. It does not say whether the
 * target exists, which policy statement matched, or who else holds the
 * permission — a denial that explained itself fully would be a disclosure channel
 * for the state it was protecting (§107, §267).
 */
export function requirePermitted(decision: Authorization): Authorization {
  if (isPermitted(decision.decision)) return decision
  if (decision.decision === 'require_approval') {
    throw errors.requiresApproval(
      `${decision.permission} requires an independent approval that has not ` +
        `been recorded`,
    )
  }
  throw errors.notAuthorized(
    `this operation requires the ${decision.permission} permission`,
  )
}

/** One authority that could permit an operation. */
interface Candidate {
  id: string
  actions: string[]
  scope: AuthorityScope
  conditions: AuthorityConditions
  constraints: AuthorityConstraints
}

/** Whether this authority narrows nothing at all. */
function isUnrestricted(candidate: Candidate): boolean {
  return (
    scopeIsEmpty(candidate.scope) &&
    candidate.constraints.fields.length === 0 &&
    candidate.constraints.max_classification === '' &&
    candidate.constraints.max_results === null
  )
}

/**
 * How much this authority restricts, for choosing between two allows.
 *
 * Only a tie-breaker: any candidate that reaches this point independently
 * permits the operation, so the score decides which one's constraints the
 * decision carries, never whether it is permitted.
 */
function restrictiveness(candidate: Candidate): number {
  const { scope, constraints } = candidate
  return (
    scope.kinds.length +
    scope.schema_refs.length +
    scope.classifications.length +
    scope.elements.length +
    constraints.fields.length +
    (constraints.export ? 0 : 1) +
    (constraints.max_results === null ? 0 : 1) +
    (constraints.max_classification === '' ? 0 : 1)
  )
}

/** Everything the control plane says about one Principal in one Space. */
export class EffectiveAuthority {
  /** The Space this authority is in. */
  readonly space: SpaceRow
  /** The acting Principal's record. */
  readonly principal: PrincipalRow
  /** The groups it belongs to. */
  readonly groups: string[]
  /** Whether it owns the Space. */
  readonly isOwner: boolean
  /** The Policy version in force, when the Space binds one. */
  readonly policy: GovernancePolicyRow | null
  /** The Space's ActorBindings for this Principal. */
  readonly bindings: ActorBindingRow[]
  private readonly candidates: Candidate[]

  private constructor(parts: {
    space: SpaceRow
    principal: PrincipalRow
    groups: string[]
    isOwner: boolean
    policy: GovernancePolicyRow | null
    bindings: ActorBindingRow[]
    candidates: Candidate[]
  }) {
    this.space = parts.space
    this.principal = parts.principal
    this.groups = parts.groups
    this.isOwner = parts.isOwner
    this.policy = parts.policy
    this.bindings = parts.bindings
    this.candidates = parts.candidates
  }

  /**
   * Reads the control plane for one Principal in one Space.
   *
   * Fails when the asserted Principal has no record. A host that names an
   * identity the control plane has never heard of has a configuration bug, and
   * resolving it to "some caller with no Grants" would hide that bug behind a
   * denial that looks like policy.
   */
  static resolve(store: Store, spaceId: string, auth: AuthContext): EffectiveAuthority {
    return EffectiveAuthority.#resolveAtDepth(
      store,
      spaceId,
      auth.principal_id,
      auth.delegation_chain,
      0,
    )
  }

  /**
   * Reads the control plane as it stood at a past instant (§176, §177).
   *
   * Answers *who had access at time T*, which is a different question from *who
   * has access now* and must never be mistaken for it: an auditor who observes
   * that a Principal could read something in January learns nothing about today
   * (§179). Nothing here is cached and nothing here authorizes — this is a
   * reconstruction for an authorized reader, not a decision path.
   *
   * Delegations are resolved against the *present* authority of their delegator,
   * because reconstructing a whole historical chain would need the delegator's
   * historical Grants recursively; the report says so rather than implying a
   * precision it does not have.
   */
  static resolveAt(
    store: Store,
    spaceId: string,
    auth: AuthContext,
    at: string,
  ): EffectiveAuthority {
    const gov = store.governance
    const space = requireSpace(store, spaceId)
    const principal = requirePrincipal(store, auth.principal_id)
    const groups = gov.groupsOfAt(auth.principal_id, at)
    const isOwner = ownsSpace(space, auth.principal_id)

    const candidates: Candidate[] = []
    for (const grant of gov.grantsAt(spaceId, auth.principal_id, groups, at)) {
      candidates.push(candidateOfGrant(grant))
    }
    for (const delegation of gov.delegationsAt(spaceId, auth.principal_id, at)) {
      const candidate = EffectiveAuthority.#resolveDelegation(
        store,
        spaceId,
        delegation,
        0,
      )
      if (candidate !== null) candidates.push(candidate)
    }

    return new EffectiveAuthority({
      space,
      principal,
      groups,
      isOwner,
      policy:
        space.default_policy_id === ''
          ? null
          : gov.policyAt(space.default_policy_id, at),
      bindings: gov.bindingsAt(auth.principal_id, spaceId, at),
      candidates,
    })
  }

  /**
   * Decides whether `permission` may be exercised on `resource`.
   *
   * Pure: everything it reads was loaded by {@link EffectiveAuthority.resolve}.
   */
  authorize(
    permission: Permission,
    resource: ResourceContext,
    auth: AuthContext,
  ): Authorization {
    const now = nowTime()
    const policyId = this.policy?.policy_id ?? ''
    const policyVersion = this.policy?.version ?? 0
    const deny = (reason: string): Authorization => ({
      decision: 'deny',
      permission,
      constraints: emptyConstraints(),
      obligations: this.baselineObligations(permission),
      policy_id: policyId,
      policy_version: policyVersion,
      authorities_used: [],
      unrestricted: false,
      reason,
    })

    if (this.principal.status !== govStatus.ACTIVE) {
      return deny('the acting Principal is not active')
    }
    if (this.space.status === 'suspended') {
      return deny('the MemorySpace is suspended')
    }

    // The classification a resource carries when it names none is the Space
    // default, never `public` (§95). Only for a resource that names something: a
    // Space-scope check has no element to classify, and giving it the default
    // would make every classification-narrowed Grant fail its own commands.
    const target: ResourceContext =
      isSpaceScope(resource) || resource.classification !== ''
        ? resource
        : { ...resource, classification: this.defaultClassification() }

    const statements = this.statements()

    // §42: an explicit deny wins over every allow, including the owner's. The
    // owner is not locked out by it — a host holds the control plane directly and
    // can publish a new policy version — but nothing that arrives through a
    // request can talk past a deny.
    for (const statement of statements) {
      if (
        statement.effect === 'deny' &&
        this.statementMatches(statement, permission, target, auth, now)
      ) {
        return deny('an explicit policy statement denies this operation')
      }
    }

    const allows: Candidate[] = []
    if (this.isOwner) {
      allows.push({
        id: `owner:${this.principal.principal_id}`,
        actions: [],
        scope: emptyScope(),
        conditions: emptyConditions(),
        constraints: { ...emptyConstraints(), export: true },
      })
    }
    for (const candidate of this.candidates) {
      if (candidateMatches(candidate, permission, target, auth, now)) {
        allows.push(candidate)
      }
    }
    let obligations = this.baselineObligations(permission)
    for (const statement of statements) {
      if (
        statement.effect !== 'allow' ||
        !this.statementMatches(statement, permission, target, auth, now)
      ) {
        continue
      }
      obligations = mergeObligations(obligations, statement.obligations)
      allows.push({
        id: `policy:${policyId}@${policyVersion}`,
        actions: statement.actions,
        scope: statement.resource,
        conditions: statement.conditions,
        constraints: statement.constraints,
      })
    }

    let chosen: Candidate | null = null
    for (const candidate of allows) {
      if (chosen === null || restrictiveness(candidate) < restrictiveness(chosen)) {
        chosen = candidate
      }
    }
    if (chosen === null) {
      return deny(`nothing grants ${permission} over ${label(target)}`)
    }

    // §40: an unmet approval blocks. It is not a soft allow, and the operation
    // does not run while it is outstanding.
    if (obligations.approvals_required > 0) {
      return {
        decision: 'require_approval',
        permission,
        constraints: chosen.constraints,
        obligations,
        policy_id: policyId,
        policy_version: policyVersion,
        authorities_used: [chosen.id],
        unrestricted: false,
        reason: `${permission} needs ${obligations.approvals_required} independent approval(s)`,
      }
    }

    // Whether the decision carries anything beyond a bare allow. `export: true`
    // counts, which reads oddly — it widens rather than narrows — but the flag
    // lives in the constraint block and the two engines must agree on what a
    // decision records. `unrestricted` is computed separately and ignores it, so
    // nothing that matters resolves on this distinction.
    const base = emptyConstraints()
    const constrained =
      chosen.constraints.fields.length > 0 ||
      chosen.constraints.max_results !== base.max_results ||
      chosen.constraints.max_influence_authority !== base.max_influence_authority ||
      chosen.constraints.max_classification !== base.max_classification ||
      chosen.constraints.export !== base.export
    return {
      decision: constrained ? 'allow_with_constraints' : 'allow',
      permission,
      constraints: chosen.constraints,
      obligations,
      policy_id: policyId,
      policy_version: policyVersion,
      authorities_used: [chosen.id],
      unrestricted: isUnrestricted(chosen),
      reason: `${permission} is granted over ${label(target)}`,
    }
  }

  /**
   * Whether this caller may read one element, and under what narrowing.
   *
   * `null` means the element is outside this Principal's query universe (§104):
   * it does not appear in results, is not counted, does not affect ranking, and
   * asking for it by id answers the same as asking for one that was never
   * written. That last part is deliberate — a distinguishable "exists but hidden"
   * is the existence leak §103 is about.
   */
  mayRead(element: Element, auth: AuthContext): AuthorityConstraints | null {
    const decision = this.authorize('read', resourceOfElement(element), auth)
    return isPermitted(decision.decision) ? decision.constraints : null
  }

  /**
   * Whether this caller's authority reaches every element in the Space.
   *
   * A Space-wide count is only honest when it is: a caller whose Grant is
   * narrowed to one classification must not be told how many elements exist
   * outside it (§106). Answered from the authority rather than by scanning,
   * because the point is to avoid producing the number at all.
   */
  readsWholeSpace(auth: AuthContext): boolean {
    if (this.isOwner) return true
    const decision = this.authorize('read', spaceResource(), auth)
    return isPermitted(decision.decision) && decision.unrestricted
  }

  /** Whether this Principal may speak as a semantic actor here (§14, §66). */
  isBoundToActor(actorKey: string): boolean {
    return this.bindings.some((binding) => binding.actor_key === actorKey)
  }

  /** The class of binding this Principal holds to an actor, if any. */
  bindingClassOf(actorKey: string): string | null {
    const found = this.bindings.find((binding) => binding.actor_key === actorKey)
    return found?.binding_class ?? null
  }

  /** How well a claim attributed to this actor is attributable (§16). */
  attributionAssurance(actorKey: string): string {
    const found = this.bindings.find((binding) => binding.actor_key === actorKey)
    if (found?.assurance === 'verified') return 'verified'
    if (found?.assurance === 'strongly_inferred') return 'strongly_inferred'
    return 'unverified'
  }

  /** The Space's default classification. */
  defaultClassification(): string {
    return this.space.default_classification === ''
      ? classification.DEFAULT
      : this.space.default_classification
  }

  /**
   * When the first of this Principal's authorities lapses, if any does.
   *
   * §266 lists this among the things an Agent must be able to learn about
   * itself: autonomous planning that does not know when its Delegation expires
   * plans work it will not be allowed to finish.
   */
  earliestExpiry(): string | null {
    const bounds = this.candidates
      .map((candidate) => candidate.conditions.valid_until)
      .filter((until) => until !== '')
      .sort()
    return bounds[0] ?? null
  }

  /**
   * The permission names this Principal holds somewhere in this Space.
   *
   * For `DESCRIBE ACCESS`. Deliberately coarse: it answers "could this ever be
   * allowed" rather than "is this allowed on that element", because the second
   * question's answer depends on an element whose existence the caller may not be
   * entitled to learn.
   */
  permissionNames(auth: AuthContext): Permission[] {
    const resource = spaceResource()
    return ALL_PERMISSIONS.filter((permission) =>
      isPermitted(this.authorize(permission, resource, auth).decision),
    )
  }

  /**
   * Whether this Principal holds an action broadly enough to delegate it (§31).
   *
   * A question rather than a getter, for the same reason `Targets` keeps its ids
   * private in the write path: what a Principal holds is answered by asking, and
   * handing the candidate list out as data would make the next caller's
   * comparison its own business. Called only from
   * {@link resolveDelegation} — the attenuation check is the only place a
   * *delegator's* standing is examined instead of a caller's.
   */
  confersForDelegation(
    action: string,
    scope: AuthorityScope,
    conditions: AuthorityConditions,
  ): boolean {
    return this.candidates.some(
      (candidate) =>
        candidate.actions.includes(action) &&
        scopeContains(candidate.scope, scope) &&
        conditionsContain(candidate.conditions, conditions),
    )
  }

  /**
   * The resolution itself, at one depth of the Delegation walk.
   *
   * Inside the class because it constructs one, and constructing an
   * `EffectiveAuthority` is the one thing nothing outside this file may do:
   * authority that can be assembled by a caller is not authority, it is a
   * struct.
   */
  static #resolveAtDepth(
    store: Store,
    spaceId: string,
    principalId: string,
    delegationChain: readonly string[],
    depth: number,
  ): EffectiveAuthority {
    const gov = store.governance
    const space = requireSpace(store, spaceId)
    const principal = requirePrincipal(store, principalId)

    // A suspended or revoked Principal keeps its record and loses its authority.
    // Returning an empty candidate set rather than an error means the refusal
    // reads as "not permitted", which is what it is.
    const live = principal.status === govStatus.ACTIVE
    const groups = live ? gov.groupsOf(principalId) : []
    const isOwner = live && ownsSpace(space, principalId)

    const candidates: Candidate[] = []
    if (live) {
      if (delegationChain.length === 0) {
        for (const grant of gov.grantsFor(spaceId, principalId, groups)) {
          candidates.push(candidateOfGrant(grant))
        }
        for (const delegation of gov.delegationsTo(spaceId, principalId)) {
          const candidate = EffectiveAuthority.#resolveDelegation(
            store,
            spaceId,
            delegation,
            depth,
          )
          if (candidate !== null) candidates.push(candidate)
        }
      } else {
        candidates.push(
          ...EffectiveAuthority.#resolveNamedChain(
            store,
            spaceId,
            principalId,
            delegationChain,
            depth,
          ),
        )
      }
    }

    return new EffectiveAuthority({
      space,
      principal,
      groups,
      isOwner,
      policy:
        space.default_policy_id === ''
          ? null
          : gov.activePolicy(space.default_policy_id),
      bindings: live ? gov.bindingsOf(principalId, spaceId) : [],
      candidates,
    })
  }

  /**
   * Resolves one Delegation against its delegator's *current* authority.
   *
   * This is why Delegation is not stored as a kind of Grant. A Grant is checked
   * against its own record; a Delegation is checked against a record plus a live
   * question — does the delegator still hold this? — and the answer can change
   * without the Delegation's own row changing at all (§35).
   */
  static #resolveDelegation(
    store: Store,
    spaceId: string,
    delegation: DelegationRow,
    depth: number,
  ): Candidate | null {
    if (depth >= MAX_DELEGATION_DEPTH) return null
    const parent = EffectiveAuthority.#resolveAtDepth(
      store,
      spaceId,
      delegation.delegator_principal,
      [],
      depth + 1,
    )

    const scope = asScope(delegation.scope)
    const conditions = asConditions(delegation.conditions)
    const constraints = asConstraints(delegation.constraints)

    // §31: the delegated actions are what the delegator can actually confer
    // right now, not what the record says it once could.
    const actions = delegation.actions.filter((action) => {
      if (!isPermission(action)) return false
      if (parent.isOwner) return true
      return parent.confersForDelegation(action, scope, conditions)
    })
    if (actions.length === 0) return null
    return { id: delegationId(delegation.id), actions, scope, conditions, constraints }
  }

  /**
   * Resolves a Delegation chain the caller named explicitly.
   *
   * Each link must name the previous as its parent and the last must name the
   * caller as its delegate. A chain that does not link is not a narrower
   * authority — it is two unrelated Delegations presented as one, which is how
   * §238's amplification would be spelled if the linkage went unchecked.
   */
  static #resolveNamedChain(
    store: Store,
    spaceId: string,
    principalId: string,
    chain: readonly string[],
    depth: number,
  ): Candidate[] {
    let previous: DelegationRow | null = null
    let last: DelegationRow | null = null
    for (const id of chain) {
      const rowId = rowIdOf(id)
      if (rowId === null) {
        throw errors.notAuthorized(
          `${JSON.stringify(id)} is not a Delegation identifier`,
        )
      }
      const row = store.governance.delegation(rowId)
      if (row === null) {
        throw errors.notAuthorized(`no Delegation ${JSON.stringify(id)}`)
      }
      if (row.status !== govStatus.ACTIVE || row.space_id !== spaceId) {
        throw errors.notAuthorized(
          `Delegation ${JSON.stringify(id)} is not in force in this MemorySpace`,
        )
      }
      if (previous !== null) {
        if (row.parent_delegation !== delegationId(previous.id)) {
          throw errors.notAuthorized(
            `Delegation ${JSON.stringify(id)} does not descend from the one before it`,
          )
        }
        if (previous.may_redelegate !== 1) {
          throw errors.notAuthorized(
            `Delegation ${delegationId(previous.id)} does not permit re-delegation`,
          )
        }
      }
      previous = row
      last = row
    }
    if (last === null) return []
    if (last.delegate_principal !== principalId) {
      throw errors.notAuthorized(
        'the named Delegation chain does not end at the acting Principal',
      )
    }
    const candidate = EffectiveAuthority.#resolveDelegation(store, spaceId, last, depth)
    return candidate === null ? [] : [candidate]
  }

  private statements(): PolicyStatement[] {
    return (this.policy?.statements ?? []).map(asStatement)
  }

  /**
   * The obligations that hold before any policy is consulted.
   *
   * §172 lists operations whose absence from an audit log is itself the
   * incident. A deployment may audit more than this; it cannot audit less.
   */
  private baselineObligations(permission: Permission): PolicyObligations {
    return {
      ...emptyObligations(),
      audit: isAlwaysAudited(permission) || this.space.audit_mode === 'verbose',
    }
  }

  private statementMatches(
    statement: PolicyStatement,
    permission: Permission,
    resource: ResourceContext,
    auth: AuthContext,
    now: string,
  ): boolean {
    const principal = this.principal.principal_id
    if (statement.principals.length > 0 && !statement.principals.includes(principal)) {
      return false
    }
    if (
      statement.groups.length > 0 &&
      !statement.groups.some((group) => this.groups.includes(group))
    ) {
      return false
    }
    if (statement.actions.length > 0 && !statement.actions.includes(permission)) {
      return false
    }
    return (
      (isSpaceScope(resource) || scopeMatches(statement.resource, resource)) &&
      conditionsHold(statement.conditions, auth, now)
    )
  }
}

function candidateMatches(
  candidate: Candidate,
  permission: Permission,
  resource: ResourceContext,
  auth: AuthContext,
  now: string,
): boolean {
  return (
    candidate.actions.includes(permission) &&
    (isSpaceScope(resource) ||
      (scopeMatches(candidate.scope, resource) &&
        reachesClassification(candidate.constraints, resource))) &&
    conditionsHold(candidate.conditions, auth, now)
  )
}

/** Whether an authority's bounds cover this resource. */
function scopeMatches(scope: AuthorityScope, resource: ResourceContext): boolean {
  return (
    covers(scope.kinds, resource.kind) &&
    covers(scope.schema_refs, resource.schema_ref) &&
    covers(scope.classifications, resource.classification) &&
    covers(scope.elements, resource.element_id)
  )
}

/** Whether an authority's classification ceiling reaches this resource. */
function reachesClassification(
  constraints: AuthorityConstraints,
  resource: ResourceContext,
): boolean {
  return (
    constraints.max_classification === '' ||
    classification.rank(resource.classification) <=
      classification.rank(constraints.max_classification)
  )
}

/** Whether the runtime context satisfies an authority's conditions. */
function conditionsHold(
  conditions: AuthorityConditions,
  auth: AuthContext,
  now: string,
): boolean {
  if (conditions.valid_from !== '' && now < conditions.valid_from) return false
  if (conditions.valid_until !== '' && now >= conditions.valid_until) return false
  if (authStrength.rank(auth.auth_strength) < authStrength.rank(conditions.min_auth_strength)) {
    return false
  }
  if (
    purposeAssurance.rank(auth.purpose_assurance) <
    purposeAssurance.rank(conditions.min_purpose_assurance)
  ) {
    return false
  }
  if (conditions.purpose.length > 0 && !conditions.purpose.includes(auth.purpose)) {
    return false
  }
  return true
}

/**
 * Whether a bound list covers a value, where empty means "every value".
 *
 * An empty *value* against a bounded list does not match: a query with no
 * particular element in view must not be judged against a Grant that was narrowed
 * to one element.
 */
function covers(bound: readonly string[], value: string): boolean {
  return bound.length === 0 || (value !== '' && bound.includes(value))
}

function candidateOfGrant(grant: GrantRow): Candidate {
  return {
    id: grantId(grant.id),
    actions: [...grant.actions],
    scope: asScope(grant.scope),
    conditions: asConditions(grant.conditions),
    constraints: asConstraints(grant.constraints),
  }
}

function requireSpace(store: Store, spaceId: string): SpaceRow {
  const space = store.space(spaceId)
  if (space === null) {
    throw errors.notFoundOrNotVisible(`no MemorySpace ${spaceId}`)
  }
  return space
}

function requirePrincipal(store: Store, principalId: string): PrincipalRow {
  const principal = store.governance.findPrincipal(principalId)
  if (principal === null) {
    throw errors.unauthenticated(
      `no Principal ${JSON.stringify(principalId)} is registered in this Nexus`,
    )
  }
  return principal
}

function ownsSpace(space: SpaceRow, principalId: string): boolean {
  return space.owner_principal === principalId || space.owners.includes(principalId)
}

/** The influence-authority ceiling this decision imposes. */
export function authorityCeiling(constraints: AuthorityConstraints): string {
  return constraints.max_influence_authority === ''
    ? 'executable'
    : constraints.max_influence_authority
}
