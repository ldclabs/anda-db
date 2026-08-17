/**
 * # The protected records of the Governance Control Plane
 *
 * These rows live in the same SQLite database as cognitive state and are
 * emphatically **not** cognitive state (Governance §2). A Concept saying "Alice
 * is an administrator" is a claim; a {@link GrantRow} naming Alice is authority.
 * The whole plane exists to keep those two things from being the same write.
 *
 * Consequences that shape every row here:
 *
 * - **No KML clause can reach them.** They are written through host APIs only,
 *   which is what stops a prompt injection into ordinary memory formation from
 *   having a path to privilege escalation (§264).
 * - **Revocation is a status change, never a delete.** A revoked Grant must stop
 *   authorizing future operations without rewriting the audit that says it
 *   authorized a past one (§36, §49).
 * - **Every mutation is mirrored into {@link GovernanceAuditRow} with the
 *   complete new record.** Whole records rather than diffs, for the same reason
 *   the element version log stores whole rows: a diff chain with one missing
 *   link answers a historical question wrongly instead of refusing (§175).
 *
 * Ids are minted from the SQLite row id rather than carried in a column, because
 * the row id is assigned at insert and a second write to store the derived
 * string would be a window where the record exists without its own name. Records
 * a caller names — Principals, groups, policies — keep the caller's id, because
 * those names appear in Grants and in audit records and must stay stable.
 *
 * The field names match `rs/anda_cognitive_nexus/src/governance/rows.rs` so the
 * two engines can be read side by side, except that `_id` is spelled `id` to
 * match the element rows of this engine.
 *
 * @see rs/anda_cognitive_nexus/src/governance/rows.rs
 */

import type { Json, JsonMap } from '../json.js'
import { authority, authStrength, classification, purposeAssurance } from './lattice.js'

// ---------------------------------------------------------------------------
// Minted identifiers
// ---------------------------------------------------------------------------

/** The id an ActorBinding row answers to. */
export const bindingId = (rowId: number): string => `kip:binding:${rowId}`
/** The id a Grant row answers to. */
export const grantId = (rowId: number): string => `kip:grant:${rowId}`
/** The id a Delegation row answers to. */
export const delegationId = (rowId: number): string => `kip:delegation:${rowId}`
/** The id an Approval row answers to. */
export const approvalId = (rowId: number): string => `kip:approval:${rowId}`
/** The id a Governance audit entry answers to. */
export const auditId = (rowId: number): string => `kip:audit:${rowId}`

/** Reads the row number back out of a minted Governance id. */
export function rowIdOf(id: string): number | null {
  const at = id.lastIndexOf(':')
  if (at < 0) return null
  const tail = id.slice(at + 1)
  if (!/^\d+$/.test(tail)) return null
  return Number(tail)
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/**
 * An authenticated execution identity (§7, §8).
 *
 * A Principal answers "who is making this protocol operation". It is not the
 * semantic Person or Agent Concept an Assertion is attributed to — that is what
 * {@link ActorBindingRow} bridges, under Governance authority rather than by
 * anyone writing a Proposition about it.
 *
 * KIP standardizes no authentication protocol. `auth_provider` and
 * `auth_subject` record *which* deployment subsystem vouched for this identity
 * and under what opaque subject; neither is a credential and neither is verified
 * here.
 */
export interface PrincipalRow {
  /** The row id. */
  id: number
  /** The stable Principal id, as it appears in `_system.origin.principal_id`. */
  principal_id: string
  /** One of `principalClass`. */
  principal_class: string
  /** One of `govStatus`. */
  status: string
  /** A human-readable label. Carries no authority (§203). */
  display_name: string
  /** Which deployment subsystem authenticated this identity. */
  auth_provider: string
  /** That subsystem's opaque subject reference. Never a credential. */
  auth_subject: string
  created_at: string
  updated_at: string
  /** When it was revoked; empty while it is not. */
  revoked_at: string
  /** Bumped on every change, so a cached decision can be invalidated (§187). */
  version: number
}

/**
 * A named set of Principals (§25).
 *
 * Membership controls authority, which is exactly why it cannot be derived from
 * ordinary cognitive Propositions: an Agent that could write "I am a maintainer"
 * would be granting itself maintenance.
 */
export interface PrincipalGroupRow {
  id: number
  /** The stable group id, as Grants name it. */
  group_id: string
  name: string
  description: string
  /** The member Principal ids. */
  members: string[]
  status: string
  created_at: string
  updated_at: string
  version: number
}

/**
 * The trusted link between a Principal and a semantic actor (§14).
 *
 * Without one, a Principal may still *record* what an actor said — recording a
 * claim is not impersonation (§17) — but it cannot exercise that actor's
 * authority. The two are separate permissions for a reason: a Formation Agent
 * that observed "Alice: I prefer dark mode" should be able to store it as
 * Alice's stated claim without thereby being able to retract Alice's other
 * Assertions.
 */
export interface ActorBindingRow {
  id: number
  principal_id: string
  /**
   * The semantic actor as the caller named it: an element id or a canonical
   * identity. Kept verbatim so a report can show what was configured.
   */
  actor_ref: string
  /**
   * The same actor as an endpoint key.
   *
   * Normalized on write, because this is what it is compared against:
   * `assertions.asserted_by_key` is an endpoint key, and a binding stored in any
   * other spelling would silently never match.
   */
  actor_key: string
  /** One of `bindingClass`. */
  binding_class: string
  /** One of `assurance`. */
  assurance: string
  /**
   * The Space this binding applies in, or `*` for every Space.
   *
   * Scoped because representation is not global: an agent that may speak for an
   * organization inside its project Space should not thereby speak for it in
   * someone's personal Brain.
   */
  scope: string
  status: string
  created_at: string
  updated_at: string
  revoked_at: string
  version: number
}

/**
 * Authority conferred on a Principal or group over one Space (§28, §29).
 *
 * A Grant is authority state the Nexus evaluates, not a credential: holding a
 * serialized copy proves nothing, because the grantee is identified by the
 * authenticated Principal and not by possession of the record (§30).
 *
 * An empty scope list means "every value", which reads as permissive and is: the
 * restriction that matters is `actions`, and a Grant with no actions confers
 * nothing at all.
 */
export interface GrantRow {
  id: number
  /**
   * The Space whose authority this confers. Exactly one, never a pattern: a
   * Space is the authorization boundary, and URI structure confers nothing
   * (§22).
   */
  space_id: string
  /** The grantee Principal; empty when the grantee is a group. */
  grantee_principal: string
  /** The grantee group; empty when the grantee is a Principal. */
  grantee_group: string
  /** The permission names conferred. */
  actions: string[]
  /** The {@link AuthorityScope} this Grant is bounded to. */
  scope: JsonMap
  /** The {@link AuthorityConditions} that must hold at decision time. */
  conditions: JsonMap
  /** The {@link AuthorityConstraints} every allowed operation carries. */
  constraints: JsonMap
  /**
   * Whether the grantee may delegate any part of this (§34: false by default,
   * because uncontrolled sub-agent spawning must not multiply privilege).
   */
  delegation_allowed: number
  status: string
  /** The Principal that created the Grant, for separation-of-duties checks. */
  granted_by: string
  created_at: string
  updated_at: string
  revoked_at: string
  version: number
}

/**
 * One Principal conferring part of *its own* authority on another (§32).
 *
 * Kept apart from {@link GrantRow} because the two are evaluated differently,
 * and collapsing them would lose the difference. A Grant stands on its own; a
 * Delegation is only ever as good as its delegator's authority *right now* — if
 * the parent is revoked, the child stops working even though its own record
 * still says `active` and its own expiry is still in the future (§35).
 */
export interface DelegationRow {
  id: number
  space_id: string
  /** The Principal conferring the authority. */
  delegator_principal: string
  /** The Principal receiving it. */
  delegate_principal: string
  /**
   * The permission names conferred. Attenuated against the delegator's
   * effective authority at decision time, not merely at creation time.
   */
  actions: string[]
  scope: JsonMap
  conditions: JsonMap
  constraints: JsonMap
  /** The delegation this one descends from, when it is a re-delegation. */
  parent_delegation: string
  /** Whether the delegate may re-delegate. Non-transitive by default (§34). */
  may_redelegate: number
  status: string
  created_at: string
  updated_at: string
  revoked_at: string
  version: number
}

/**
 * One immutable version of a Governance Policy (§44, §46).
 *
 * Appended, never updated, for the same reason the Schema Environment log is: an
 * audit has to be able to answer *which policy version authorized this
 * operation*, and rewriting a policy in place retroactively changes the answer.
 *
 * The version in force for a Space is the greatest version of the policy the
 * Space names; the version in force *at a past coordinate* is the greatest one
 * created at or before it.
 */
export interface GovernancePolicyRow {
  id: number
  /** `{policy_id}@{version}` — the exact identity of one version. */
  policy_ref: string
  /** The stable policy id a Space binds to. */
  policy_id: string
  /** The version; monotonic per policy id. */
  version: number
  /** The Space this policy governs, or `*` for a Nexus-wide policy. */
  space_id: string
  description: string
  /** The ordered {@link PolicyStatement} list. */
  statements: Json[]
  created_at: string
  created_by: string
}

/**
 * A pending or satisfied multi-party approval (§167–§170).
 *
 * Approval is control state, not a semantic statement: a Concept saying "Alice
 * approved this" satisfies nothing. What satisfies it is an approving Principal
 * recorded here by an authorized Governance operation.
 *
 * `subject_digest` binds the approval to one concrete operation. Without it an
 * approval for "purge this one Evidence record" would authorize purging
 * anything, which is the failure mode §246 tests for.
 */
export interface ApprovalRow {
  id: number
  space_id: string
  /** The permission being approved. */
  operation: string
  /** The resource it targets. */
  resource: string
  /** A digest binding this approval to one concrete request. */
  subject_digest: string
  /** How many independent approvals are required. */
  required: number
  /** One entry per approval: `{principal_id, at, note}`. */
  approvals: Json[]
  /**
   * The approving Principal ids, lifted out for the separation-of-duties check
   * and for indexed lookup.
   */
  approver_ids: string[]
  /**
   * Whether the requester may also approve. False by default: the same Principal
   * proposing and approving is the separation-of-duties failure §170 names
   * first.
   */
  allow_self_approval: number
  /** `pending`, `granted`, `denied`, `expired` or `consumed`. */
  status: string
  /** The Principal that asked for the approval. */
  requested_by: string
  created_at: string
  updated_at: string
  /** When it stops being usable; empty for no expiry. */
  expires_at: string
  version: number
}

/**
 * One append-preserved Governance audit entry (§172–§175).
 *
 * Two things are recorded here and deliberately tagged apart:
 *
 * ```text
 * mutation   the control plane changed, and this is the whole new record
 * decision   an authorization decision was made, and this is what decided it
 * ```
 *
 * Corrections append; nothing here is ever rewritten. An audit that could be
 * edited would answer §247 — *which policy version authorized this?* — with
 * today's answer rather than the one that was true.
 */
export interface GovernanceAuditRow {
  id: number
  /** `mutation` or `decision`. */
  entry_class: string
  at: string
  /** The Space it concerned, or `*` for a Nexus-wide record. */
  space_id: string
  principal_id: string
  /** The delegation chain the operation ran under, delegator-first. */
  delegation_chain: string[]
  /** The permission or Governance operation name. */
  operation: string
  resource: string
  /**
   * `allow`, `allow_with_constraints`, `deny`, `require_approval`, or the
   * mutation verb for a `mutation` entry.
   */
  decision: string
  /** Why, in one line. Safe to show a caller (§267). */
  reason: string
  policy_id: string
  policy_version: number
  /** The Grant and Delegation ids that matched. */
  authorities_used: string[]
  /** The Approval ids that were consumed. */
  approvals: string[]
  /** The obligations the decision carried. */
  obligations: JsonMap
  /** For a `mutation` entry: the complete new record, as stored. */
  record: Json
  request_id: string
  /** The cognitive transaction it authorized, when there was one. */
  tx_id: string
}

// ---------------------------------------------------------------------------
// The shared shapes stored in the JSON columns
// ---------------------------------------------------------------------------

/**
 * What a Grant or Delegation is bounded to (§90).
 *
 * Every list is "empty means every value". That is permissive by design: the
 * action list is what confers authority, and a scope is a *narrowing* of it.
 */
export interface AuthorityScope {
  /** Core element kinds in scope. */
  kinds: string[]
  /** Schema symbol references in scope. */
  schema_refs: string[]
  /** Classification labels in scope. */
  classifications: string[]
  /** Specific element ids in scope, when the authority is that narrow. */
  elements: string[]
}

/** What must hold at decision time for an authority to apply (§29, §38). */
export interface AuthorityConditions {
  /** The purposes this authority is limited to; empty means any purpose. */
  purpose: string[]
  /** The least purpose assurance accepted. */
  min_purpose_assurance: string
  /** The least authentication strength accepted. */
  min_auth_strength: string
  /** Not in force before this instant; empty for no lower bound. */
  valid_from: string
  /** Not in force at or after this instant; empty for no upper bound. */
  valid_until: string
}

/**
 * What every operation an authority allows must carry (§39).
 *
 * These are not permissions; they ride along with an allow. A Grant that allows
 * `read` with `fields: ["summary"]` allows the read *and* narrows what comes
 * back.
 */
export interface AuthorityConstraints {
  /** The element fields that may be returned; empty means every field. */
  fields: string[]
  /** The most rows a read may return; `null` means the engine's own limit. */
  max_results: number | null
  /** The highest influence authority reachable. */
  max_influence_authority: string
  /** The highest classification readable; empty means the Space default. */
  max_classification: string
  /** Whether the result may leave the Space at all (§78). */
  export: boolean
}

/**
 * What an allow requires the runtime to do as well (§184).
 *
 * An obligation that cannot be satisfied denies the operation. A policy that
 * requires an audit record, on a runtime whose audit is unavailable, must not
 * quietly proceed without one.
 */
export interface PolicyObligations {
  /** Whether the decision must be written to the audit log. */
  audit: boolean
  /** How many independent approvals the operation needs. */
  approvals_required: number
  /** A named redaction profile to apply to the result. */
  redaction_profile: string
}

/**
 * One ordered rule in a Policy (§45).
 *
 * `deny` wins over `allow` regardless of order (§42); the order matters only for
 * reading the policy, never for resolving it. Making order significant would
 * mean a policy's meaning depends on where somebody appended a rule.
 */
export interface PolicyStatement {
  /** `allow` or `deny`. */
  effect: string
  /** The Principal ids this statement matches; empty means every Principal. */
  principals: string[]
  /** The groups it matches; empty means every group. */
  groups: string[]
  /** The permission names it matches; empty means every permission. */
  actions: string[]
  /** The resources it matches. */
  resource: AuthorityScope
  /** The conditions under which it applies. */
  conditions: AuthorityConditions
  /** The constraints an `allow` carries. */
  constraints: AuthorityConstraints
  /** The obligations an `allow` carries. */
  obligations: PolicyObligations
}

// ---------------------------------------------------------------------------
// Reading the JSON columns
// ---------------------------------------------------------------------------

/**
 * The shapes above arrive from a JSON column, which is to say from something
 * this engine wrote *some* version of and cannot assume the shape of. Each
 * reader fills every member, so no downstream check has to ask whether a list
 * is there before asking what is in it — an `undefined` where an empty array
 * belongs is how "unrestricted" and "crashed" become the same code path.
 */

const strings = (value: unknown): string[] =>
  Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : []

const text = (value: unknown): string => (typeof value === 'string' ? value : '')

const flag = (value: unknown): boolean => value === true || value === 1

/** Reads an {@link AuthorityScope} from a stored value. */
export function asScope(value: unknown): AuthorityScope {
  const raw = (value ?? {}) as Record<string, unknown>
  return {
    kinds: strings(raw.kinds),
    schema_refs: strings(raw.schema_refs),
    classifications: strings(raw.classifications),
    elements: strings(raw.elements),
  }
}

/** Reads an {@link AuthorityConditions} from a stored value. */
export function asConditions(value: unknown): AuthorityConditions {
  const raw = (value ?? {}) as Record<string, unknown>
  return {
    purpose: strings(raw.purpose),
    min_purpose_assurance: text(raw.min_purpose_assurance),
    min_auth_strength: text(raw.min_auth_strength),
    valid_from: text(raw.valid_from),
    valid_until: text(raw.valid_until),
  }
}

/** Reads an {@link AuthorityConstraints} from a stored value. */
export function asConstraints(value: unknown): AuthorityConstraints {
  const raw = (value ?? {}) as Record<string, unknown>
  const maxResults = raw.max_results
  return {
    fields: strings(raw.fields),
    max_results:
      maxResults === undefined || maxResults === null
        ? null
        : typeof maxResults === 'number' &&
            Number.isSafeInteger(maxResults) &&
            maxResults >= 0
          ? maxResults
          : 0,
    max_influence_authority: text(raw.max_influence_authority),
    max_classification: text(raw.max_classification),
    export: flag(raw.export),
  }
}

/** Reads a {@link PolicyObligations} from a stored value. */
export function asObligations(value: unknown): PolicyObligations {
  const raw = (value ?? {}) as Record<string, unknown>
  return {
    audit: flag(raw.audit),
    approvals_required:
      typeof raw.approvals_required === 'number' ? raw.approvals_required : 0,
    redaction_profile: text(raw.redaction_profile),
  }
}

/** Reads a {@link PolicyStatement} from a stored value. */
export function asStatement(value: unknown): PolicyStatement {
  const raw = (value ?? {}) as Record<string, unknown>
  return {
    effect: text(raw.effect),
    principals: strings(raw.principals),
    groups: strings(raw.groups),
    actions: strings(raw.actions),
    resource: asScope(raw.resource),
    conditions: asConditions(raw.conditions),
    constraints: asConstraints(raw.constraints),
    obligations: asObligations(raw.obligations),
  }
}

/** An {@link AuthorityScope} that narrows nothing. */
export const emptyScope = (): AuthorityScope => asScope(null)
/** An {@link AuthorityConditions} that requires nothing. */
export const emptyConditions = (): AuthorityConditions => asConditions(null)
/** An {@link AuthorityConstraints} that narrows nothing and forbids export. */
export const emptyConstraints = (): AuthorityConstraints => asConstraints(null)
/** A {@link PolicyObligations} that requires nothing. */
export const emptyObligations = (): PolicyObligations => asObligations(null)

// ---------------------------------------------------------------------------
// Attenuation
// ---------------------------------------------------------------------------

/**
 * Whether `child` stays inside `parent`, where empty means "unrestricted".
 *
 * An empty child list against a non-empty parent list is *not* a subset — it
 * means "every value", which is exactly what attenuation must refuse (§31).
 */
function narrows(parent: readonly string[], child: readonly string[]): boolean {
  if (parent.length === 0) return true
  return child.length > 0 && child.every((value) => parent.includes(value))
}

/** The intersection of two "empty means unrestricted" lists. */
function intersect(a: readonly string[], b: readonly string[]): string[] {
  if (a.length === 0) return [...b]
  if (b.length === 0) return [...a]
  return a.filter((value) => b.includes(value))
}

/** Whether `other` stays inside `scope`. */
export function scopeContains(scope: AuthorityScope, other: AuthorityScope): boolean {
  return (
    narrows(scope.kinds, other.kinds) &&
    narrows(scope.schema_refs, other.schema_refs) &&
    narrows(scope.classifications, other.classifications) &&
    narrows(scope.elements, other.elements)
  )
}

/** The intersection of two scopes, as the effective bound of a chain. */
export function scopeIntersect(a: AuthorityScope, b: AuthorityScope): AuthorityScope {
  return {
    kinds: intersect(a.kinds, b.kinds),
    schema_refs: intersect(a.schema_refs, b.schema_refs),
    classifications: intersect(a.classifications, b.classifications),
    elements: intersect(a.elements, b.elements),
  }
}

/** Whether two scopes narrow exactly the same things. */
export function scopeIsEmpty(scope: AuthorityScope): boolean {
  return (
    scope.kinds.length === 0 &&
    scope.schema_refs.length === 0 &&
    scope.classifications.length === 0 &&
    scope.elements.length === 0
  )
}

/**
 * Whether `other` is at least as restrictive as `conditions`.
 *
 * Time is the subtle one: a child that outlives its parent is the classic
 * delegation amplification (§238), so an empty child `valid_until` against a
 * bounded parent fails.
 */
export function conditionsContain(
  conditions: AuthorityConditions,
  other: AuthorityConditions,
): boolean {
  return (
    narrows(conditions.purpose, other.purpose) &&
    purposeAssurance.rank(other.min_purpose_assurance) >=
      purposeAssurance.rank(conditions.min_purpose_assurance) &&
    authStrength.rank(other.min_auth_strength) >=
      authStrength.rank(conditions.min_auth_strength) &&
    atLeast(conditions.valid_from, other.valid_from) &&
    atMost(conditions.valid_until, other.valid_until)
  )
}

/**
 * Whether `child` starts no earlier than `parent`, with empty meaning "no lower
 * bound".
 */
function atLeast(parent: string, child: string): boolean {
  return parent === '' || (child !== '' && child >= parent)
}

/**
 * Whether `child` ends no later than `parent`, with empty meaning "no upper
 * bound".
 */
function atMost(parent: string, child: string): boolean {
  return parent === '' || (child !== '' && child <= parent)
}

/** The stricter of two constraint sets — what a chain actually allows. */
export function tightenConstraints(
  a: AuthorityConstraints,
  b: AuthorityConstraints,
): AuthorityConstraints {
  return {
    fields: intersect(a.fields, b.fields),
    max_results:
      a.max_results === null
        ? b.max_results
        : b.max_results === null
          ? a.max_results
          : Math.min(a.max_results, b.max_results),
    max_influence_authority: lowerNamed(
      a.max_influence_authority,
      b.max_influence_authority,
      authority.rank,
    ),
    max_classification: lowerNamed(
      a.max_classification,
      b.max_classification,
      classification.rank,
    ),
    export: a.export && b.export,
  }
}

/** Whether `child` stays inside every constraint imposed by `parent`. */
export function constraintsContain(
  parent: AuthorityConstraints,
  child: AuthorityConstraints,
): boolean {
  const boundedNumber =
    parent.max_results === null ||
    (child.max_results !== null && child.max_results <= parent.max_results)
  const boundedAuthority =
    parent.max_influence_authority === '' ||
    (child.max_influence_authority !== '' &&
      authority.rank(child.max_influence_authority) <=
        authority.rank(parent.max_influence_authority))
  const boundedClassification =
    parent.max_classification === '' ||
    (child.max_classification !== '' &&
      classification.rank(child.max_classification) <=
        classification.rank(parent.max_classification))
  return (
    narrows(parent.fields, child.fields) &&
    boundedNumber &&
    boundedAuthority &&
    boundedClassification &&
    (parent.export || !child.export)
  )
}

/** The lower of two ranked names, treating empty as "no ceiling stated". */
function lowerNamed(a: string, b: string, rank: (name: string) => number): string {
  if (a === '') return b
  if (b === '') return a
  return rank(a) <= rank(b) ? a : b
}

/**
 * The union of two obligation sets — obligations only ever accumulate.
 *
 * The opposite direction from constraints, and for a different reason: a
 * constraint is one authority's limit, so an independently sufficient authority
 * must not inherit another's. An obligation is what the *deployment* requires of
 * the operation, so every matching statement's obligation applies (§184).
 */
export function mergeObligations(
  a: PolicyObligations,
  b: PolicyObligations,
): PolicyObligations {
  return {
    audit: a.audit || b.audit,
    approvals_required: Math.max(a.approvals_required, b.approvals_required),
    redaction_profile: b.redaction_profile === '' ? a.redaction_profile : b.redaction_profile,
  }
}

/**
 * Whether a record with these timestamps was in force at an instant.
 *
 * A record created after the coordinate did not exist then, and one revoked at
 * or before it was already gone. Both bounds are checked against the record's
 * own timestamps rather than against its current status, which is the whole
 * reason revocation is a status change rather than a delete.
 */
export function inForceAt(createdAt: string, revokedAt: string, at: string): boolean {
  return createdAt <= at && (revokedAt === '' || revokedAt > at)
}
