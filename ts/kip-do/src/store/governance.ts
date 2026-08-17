/**
 * The persistent home of the Governance Control Plane.
 *
 * Storage only, exactly like {@link Store}: this layer knows about rows,
 * statuses and timestamps, and nothing about what a permission means or who may
 * exercise it. The decision engine sits above it and is pure.
 *
 * ## Two lookups per question, and the second is not the first with a filter
 *
 * Every record here answers two different questions:
 *
 * ```text
 * grantsFor(...)  what is in force now      reads `status`
 * grantsAt(..., t) what was in force then    reads `created_at` / `revoked_at`
 * ```
 *
 * The historical form deliberately ignores `status`, because that is the whole
 * reason revocation is a status change rather than a delete (§36, §177). An
 * auditor asking *who could read this in January* gets January's answer, and
 * gets it without that being a claim about today (§179).
 *
 * ## The historical view has millisecond resolution, and on this platform that
 * is coarser than it sounds
 *
 * A Workers isolate's clock does not advance during synchronous execution — it
 * moves at I/O boundaries. So every Governance mutation made inside one request
 * carries the *same* `created_at`, and a record created and revoked in that one
 * request was never in force at any observable instant. {@link inForceAt}
 * resolves that toward not-in-force, which is the refusing direction and the
 * safe one: a reconstruction that over-reports authority is worse than one that
 * under-reports it.
 *
 * Where a total order is needed rather than a bound — replaying which group a
 * Principal was in — the tiebreak is the audit row id, so two membership
 * changes in one request still resolve to the later one rather than to
 * whichever the scan reached last.
 *
 * ## Every mutation is mirrored into the audit
 *
 * Whole records, not diffs. A control-plane change that is not in the audit is
 * a change nothing can attribute, and §172 lists the operations whose absence
 * from a log is itself the incident.
 *
 * @see rs/anda_cognitive_nexus/src/governance/store.rs
 */

import { errors } from '../errors.js'
import {
  ANY_SPACE,
  assurance as assuranceLevels,
  govStatus,
  principalClass,
} from '../governance/lattice.js'
import {
  approvalId,
  bindingId,
  delegationId,
  grantId,
  inForceAt,
  type ActorBindingRow,
  type ApprovalRow,
  type AuthorityConditions,
  type AuthorityConstraints,
  type AuthorityScope,
  type DelegationRow,
  type GovernanceAuditRow,
  type GovernancePolicyRow,
  type GrantRow,
  type PolicyStatement,
  type PrincipalGroupRow,
  type PrincipalRow,
} from '../governance/rows.js'
import { tryParseElementId } from '../id.js'
import type { Json, JsonMap } from '../json.js'
import { endpointKey } from '../term.js'
import { nowTime } from '../time.js'
import { decodeRow, insertStatement, updateStatement, type SqlRow } from './codec.js'

// ---------------------------------------------------------------------------
// Drafts — what a caller supplies, as opposed to what is stored
// ---------------------------------------------------------------------------

/** What a caller supplies to create a Principal. */
export interface PrincipalDraft {
  principal_id: string
  principal_class?: string
  display_name?: string
  auth_provider?: string
  auth_subject?: string
}

/** What a caller supplies to create or replace a group. */
export interface GroupDraft {
  group_id: string
  name?: string
  description?: string
  members?: readonly string[]
}

/** What a caller supplies to bind a Principal to a semantic actor. */
export interface ActorBindingDraft {
  principal_id: string
  /** The semantic actor: an element id or a canonical identity. */
  actor_ref: string
  binding_class?: string
  assurance?: string
  /** The Space it applies in, or `*`. */
  scope?: string
}

/** What a caller supplies to create a Grant. */
export interface GrantDraft {
  space_id: string
  grantee_principal?: string
  grantee_group?: string
  actions: readonly string[]
  scope?: Partial<AuthorityScope>
  conditions?: Partial<AuthorityConditions>
  constraints?: Partial<AuthorityConstraints>
  delegation_allowed?: boolean
}

/** What a caller supplies to create a Delegation. */
export interface DelegationDraft {
  space_id: string
  delegator_principal: string
  delegate_principal: string
  actions: readonly string[]
  scope?: Partial<AuthorityScope>
  conditions?: Partial<AuthorityConditions>
  constraints?: Partial<AuthorityConstraints>
  parent_delegation?: string
  may_redelegate?: boolean
}

/** What a caller supplies to publish a Policy version. */
export interface PolicyDraft {
  policy_id: string
  space_id?: string
  description?: string
  statements: readonly Partial<PolicyStatement>[]
}

/** What a caller supplies to open an approval request. */
export interface ApprovalDraft {
  space_id: string
  operation: string
  resource: string
  subject_digest: string
  required?: number
  allow_self_approval?: boolean
  expires_at?: string
}

/** One control-plane mutation, as the audit log records it. */
export interface MutationEntry {
  /** The Governance verb, e.g. `create_grant`. */
  operation: string
  /** The Space it concerned; empty becomes `*`. */
  space_id?: string
  /** What it acted on. */
  resource: string
  /** The acting Principal. */
  principal_id?: string
  /** The complete new record. */
  record: Json
}

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

export class GovernanceStore {
  readonly sql: SqlStorage

  constructor(sql: SqlStorage) {
    this.sql = sql
  }

  // --- Principals --------------------------------------------------------

  /**
   * Creates a Principal if it is not already there.
   *
   * Idempotent because bootstrap runs on every construction: the system
   * Principal must survive a restart without being recreated, and recreating it
   * would reset a deployment's own edits to it.
   */
  ensurePrincipal(draft: PrincipalDraft): PrincipalRow {
    const existing = this.findPrincipal(draft.principal_id)
    if (existing !== null) return existing

    const at = nowTime()
    const row: Omit<PrincipalRow, 'id'> = {
      principal_id: draft.principal_id,
      principal_class: draft.principal_class ?? principalClass.AGENT,
      status: govStatus.ACTIVE,
      display_name: draft.display_name ?? '',
      auth_provider: draft.auth_provider ?? '',
      auth_subject: draft.auth_subject ?? '',
      created_at: at,
      updated_at: at,
      revoked_at: '',
      version: 1,
    }
    const id = this.insert('gov_principals', row)
    const stored = { ...row, id }
    this.recordMutation({
      operation: 'create_principal',
      resource: stored.principal_id,
      record: stored as unknown as Json,
    })
    return stored
  }

  /** Looks a Principal up by id. */
  findPrincipal(principalId: string): PrincipalRow | null {
    return this.one<PrincipalRow>(
      'gov_principals',
      'SELECT * FROM gov_principals WHERE principal_id = ?',
      principalId,
    )
  }

  /**
   * Moves a Principal to a new lifecycle status (§9).
   *
   * Never a delete: a historical write by a later-revoked Principal stays
   * attributable to it, and the origin stamp on that element is not rewritten.
   */
  setPrincipalStatus(principalId: string, status: string, actor: string): PrincipalRow {
    const row = this.findPrincipal(principalId)
    if (row === null) {
      throw errors.notFoundOrNotVisible(`no Principal ${JSON.stringify(principalId)}`)
    }
    row.status = status
    row.updated_at = nowTime()
    if (status === govStatus.REVOKED) row.revoked_at = row.updated_at
    row.version += 1
    this.update('gov_principals', row)
    this.recordMutation({
      operation: 'set_principal_status',
      resource: principalId,
      principal_id: actor,
      record: row as unknown as Json,
    })
    return row
  }

  // --- Principal groups --------------------------------------------------

  /** Creates a group or replaces its membership list. */
  putGroup(draft: GroupDraft, actor: string): PrincipalGroupRow {
    const at = nowTime()
    const existing = this.findGroup(draft.group_id)
    const row: PrincipalGroupRow =
      existing === null
        ? {
            id: 0,
            group_id: draft.group_id,
            name: draft.name ?? draft.group_id,
            description: draft.description ?? '',
            members: [...(draft.members ?? [])],
            status: govStatus.ACTIVE,
            created_at: at,
            updated_at: at,
            version: 1,
          }
        : {
            ...existing,
            name: draft.name ?? existing.name,
            description: draft.description ?? existing.description,
            members: [...(draft.members ?? existing.members)],
            updated_at: at,
            version: existing.version + 1,
          }
    if (existing === null) {
      const { id: _drop, ...insertable } = row
      row.id = this.insert('gov_principal_groups', insertable)
    } else {
      this.update('gov_principal_groups', row)
    }
    // The audit entry is what `groupsOfAt` replays, so it carries the whole
    // membership list rather than the delta: the row says who is in the group
    // now, and only this says who was in it then (§177).
    this.recordMutation({
      operation: 'put_group',
      resource: row.group_id,
      principal_id: actor,
      record: row as unknown as Json,
    })
    return row
  }

  /** Looks a group up by id. */
  findGroup(groupId: string): PrincipalGroupRow | null {
    return this.one<PrincipalGroupRow>(
      'gov_principal_groups',
      'SELECT * FROM gov_principal_groups WHERE group_id = ?',
      groupId,
    )
  }

  /** The active groups a Principal belongs to. */
  groupsOf(principalId: string): string[] {
    return this.sql
      .exec<{ group_id: string }>(
        `SELECT g.group_id FROM gov_principal_groups g, json_each(g.members) m
           WHERE m.value = ? AND g.status = ?
           ORDER BY g.group_id`,
        principalId,
        govStatus.ACTIVE,
      )
      .toArray()
      .map((row) => row.group_id)
  }

  /**
   * Which groups a Principal belonged to at a past instant (§177).
   *
   * Replayed from the audit rather than read off the group rows, because a
   * group's membership is stored as one current list: the row says who is in it
   * now and the audit says who was in it then. The audit carrying whole records
   * rather than diffs is what makes this a lookup instead of a reconstruction.
   */
  groupsOfAt(principalId: string, at: string): string[] {
    const entries = this.sql
      .exec<SqlRow>(
        `SELECT * FROM gov_audit WHERE operation = ? AND at <= ? ORDER BY at, id`,
        'put_group',
        at,
      )
      .toArray()
      .map((row) => decodeRow<GovernanceAuditRow>('gov_audit', row))

    // Last write wins per group. Ordered by `(at, id)` rather than by `at`
    // alone, because two membership changes inside one millisecond would
    // otherwise resolve by whichever the scan happened to reach last.
    const member = new Map<string, boolean>()
    for (const entry of entries) {
      const record = entry.record as { group_id?: unknown; members?: unknown } | null
      const group = typeof record?.group_id === 'string' ? record.group_id : null
      if (group === null) continue
      const members = Array.isArray(record?.members) ? record.members : []
      member.set(group, members.includes(principalId))
    }
    return [...member]
      .filter(([, held]) => held)
      .map(([group]) => group)
      .sort()
  }

  // --- ActorBindings -----------------------------------------------------

  /** Binds a Principal to a semantic actor. */
  createBinding(draft: ActorBindingDraft, actor: string): ActorBindingRow {
    const at = nowTime()
    const row: Omit<ActorBindingRow, 'id'> = {
      principal_id: draft.principal_id,
      actor_ref: draft.actor_ref,
      actor_key: actorKey(draft.actor_ref),
      binding_class: draft.binding_class ?? '',
      assurance: draft.assurance ?? assuranceLevels.UNVERIFIED,
      scope: draft.scope ?? ANY_SPACE,
      status: govStatus.ACTIVE,
      created_at: at,
      updated_at: at,
      revoked_at: '',
      version: 1,
    }
    const id = this.insert('gov_actor_bindings', row)
    const stored = { ...row, id }
    this.recordMutation({
      operation: 'create_actor_binding',
      resource: bindingId(id),
      principal_id: actor,
      record: stored as unknown as Json,
    })
    return stored
  }

  /** Revokes an ActorBinding. */
  revokeBinding(id: number, actor: string): void {
    const row = this.byId<ActorBindingRow>('gov_actor_bindings', id)
    if (row === null) {
      throw errors.notFoundOrNotVisible(`no ActorBinding ${bindingId(id)}`)
    }
    row.status = govStatus.REVOKED
    row.updated_at = nowTime()
    row.revoked_at = row.updated_at
    row.version += 1
    this.update('gov_actor_bindings', row)
    this.recordMutation({
      operation: 'revoke_actor_binding',
      resource: bindingId(id),
      principal_id: actor,
      record: row as unknown as Json,
    })
  }

  /**
   * The active bindings a Principal holds in a Space.
   *
   * Includes bindings scoped to every Space: representation that a deployment
   * declared globally still applies here, but a Space-scoped binding never leaks
   * into another Space.
   */
  bindingsOf(principalId: string, spaceId: string): ActorBindingRow[] {
    return this.many<ActorBindingRow>(
      'gov_actor_bindings',
      `SELECT * FROM gov_actor_bindings
         WHERE principal_id = ? AND status = ? AND (scope = ? OR scope = ?)
         ORDER BY id`,
      principalId,
      govStatus.ACTIVE,
      spaceId,
      ANY_SPACE,
    )
  }

  /** The ActorBindings that were in force at a past instant. */
  bindingsAt(principalId: string, spaceId: string, at: string): ActorBindingRow[] {
    return this.many<ActorBindingRow>(
      'gov_actor_bindings',
      `SELECT * FROM gov_actor_bindings
         WHERE principal_id = ? AND (scope = ? OR scope = ?) ORDER BY id`,
      principalId,
      spaceId,
      ANY_SPACE,
    ).filter((row) => inForceAt(row.created_at, row.revoked_at, at))
  }

  // --- Grants ------------------------------------------------------------

  /** Creates a Grant. */
  createGrant(draft: GrantDraft, actor: string): GrantRow {
    const at = nowTime()
    const row: Omit<GrantRow, 'id'> = {
      space_id: draft.space_id,
      grantee_principal: draft.grantee_principal ?? '',
      grantee_group: draft.grantee_group ?? '',
      actions: [...draft.actions],
      scope: (draft.scope ?? {}) as JsonMap,
      conditions: (draft.conditions ?? {}) as JsonMap,
      constraints: (draft.constraints ?? {}) as JsonMap,
      delegation_allowed: draft.delegation_allowed === true ? 1 : 0,
      status: govStatus.ACTIVE,
      granted_by: actor,
      created_at: at,
      updated_at: at,
      revoked_at: '',
      version: 1,
    }
    const id = this.insert('gov_grants', row)
    const stored = { ...row, id }
    this.recordMutation({
      operation: 'create_grant',
      space_id: stored.space_id,
      resource: grantId(id),
      principal_id: actor,
      record: stored as unknown as Json,
    })
    return stored
  }

  /** Revokes a Grant. Future operations lose it; past ones keep their audit. */
  revokeGrant(id: number, actor: string): void {
    const row = this.byId<GrantRow>('gov_grants', id)
    if (row === null) throw errors.notFoundOrNotVisible(`no Grant ${grantId(id)}`)
    row.status = govStatus.REVOKED
    row.updated_at = nowTime()
    row.revoked_at = row.updated_at
    row.version += 1
    this.update('gov_grants', row)
    this.recordMutation({
      operation: 'revoke_grant',
      space_id: row.space_id,
      resource: grantId(id),
      principal_id: actor,
      record: row as unknown as Json,
    })
  }

  /** Looks a Grant up by row id. */
  grant(id: number): GrantRow | null {
    return this.byId<GrantRow>('gov_grants', id)
  }

  /**
   * Every active Grant that could apply to a Principal in a Space.
   *
   * Direct Grants and group Grants together, because a decision is about the
   * Principal's whole standing and evaluating them separately would let a group
   * deny fail to see a direct allow.
   */
  grantsFor(spaceId: string, principalId: string, groups: readonly string[]): GrantRow[] {
    const rows = this.many<GrantRow>(
      'gov_grants',
      `SELECT * FROM gov_grants
         WHERE space_id = ? AND grantee_principal = ? AND status = ? ORDER BY id`,
      spaceId,
      principalId,
      govStatus.ACTIVE,
    )
    for (const group of groups) {
      rows.push(
        ...this.many<GrantRow>(
          'gov_grants',
          `SELECT * FROM gov_grants
             WHERE space_id = ? AND grantee_group = ? AND status = ? ORDER BY id`,
          spaceId,
          group,
          govStatus.ACTIVE,
        ),
      )
    }
    return rows
  }

  /**
   * The Grants that were in force at a past instant (§177).
   *
   * Reads the same rows as the live lookup and judges them by their own
   * timestamps instead of by their current status.
   */
  grantsAt(
    spaceId: string,
    principalId: string,
    groups: readonly string[],
    at: string,
  ): GrantRow[] {
    const rows = this.many<GrantRow>(
      'gov_grants',
      `SELECT * FROM gov_grants
         WHERE space_id = ? AND grantee_principal = ? ORDER BY id`,
      spaceId,
      principalId,
    )
    for (const group of groups) {
      rows.push(
        ...this.many<GrantRow>(
          'gov_grants',
          `SELECT * FROM gov_grants WHERE space_id = ? AND grantee_group = ? ORDER BY id`,
          spaceId,
          group,
        ),
      )
    }
    return rows.filter((row) => inForceAt(row.created_at, row.revoked_at, at))
  }

  // --- Delegations -------------------------------------------------------

  /** Creates a Delegation. */
  createDelegation(draft: DelegationDraft, actor: string): DelegationRow {
    const at = nowTime()
    const row: Omit<DelegationRow, 'id'> = {
      space_id: draft.space_id,
      delegator_principal: draft.delegator_principal,
      delegate_principal: draft.delegate_principal,
      actions: [...draft.actions],
      scope: (draft.scope ?? {}) as JsonMap,
      conditions: (draft.conditions ?? {}) as JsonMap,
      constraints: (draft.constraints ?? {}) as JsonMap,
      parent_delegation: draft.parent_delegation ?? '',
      may_redelegate: draft.may_redelegate === true ? 1 : 0,
      status: govStatus.ACTIVE,
      created_at: at,
      updated_at: at,
      revoked_at: '',
      version: 1,
    }
    const id = this.insert('gov_delegations', row)
    const stored = { ...row, id }
    this.recordMutation({
      operation: 'create_delegation',
      space_id: stored.space_id,
      resource: delegationId(id),
      principal_id: actor,
      record: stored as unknown as Json,
    })
    return stored
  }

  /** Revokes a Delegation. */
  revokeDelegation(id: number, actor: string): void {
    const row = this.byId<DelegationRow>('gov_delegations', id)
    if (row === null) {
      throw errors.notFoundOrNotVisible(`no Delegation ${delegationId(id)}`)
    }
    row.status = govStatus.REVOKED
    row.updated_at = nowTime()
    row.revoked_at = row.updated_at
    row.version += 1
    this.update('gov_delegations', row)
    this.recordMutation({
      operation: 'revoke_delegation',
      space_id: row.space_id,
      resource: delegationId(id),
      principal_id: actor,
      record: row as unknown as Json,
    })
  }

  /** Looks a Delegation up by row id. */
  delegation(id: number): DelegationRow | null {
    return this.byId<DelegationRow>('gov_delegations', id)
  }

  /** The active Delegations naming a Principal as delegate in a Space. */
  delegationsTo(spaceId: string, principalId: string): DelegationRow[] {
    return this.many<DelegationRow>(
      'gov_delegations',
      `SELECT * FROM gov_delegations
         WHERE space_id = ? AND delegate_principal = ? AND status = ? ORDER BY id`,
      spaceId,
      principalId,
      govStatus.ACTIVE,
    )
  }

  /** The Delegations that were in force at a past instant. */
  delegationsAt(spaceId: string, principalId: string, at: string): DelegationRow[] {
    return this.many<DelegationRow>(
      'gov_delegations',
      `SELECT * FROM gov_delegations
         WHERE space_id = ? AND delegate_principal = ? ORDER BY id`,
      spaceId,
      principalId,
    ).filter((row) => inForceAt(row.created_at, row.revoked_at, at))
  }

  // --- Policies ----------------------------------------------------------

  /**
   * Publishes the next version of a Policy (§46).
   *
   * Always a new row. A policy update that edited the previous version in place
   * would retroactively change what every audit record citing it means.
   */
  publishPolicy(draft: PolicyDraft, actor: string): GovernancePolicyRow {
    const version = (this.activePolicy(draft.policy_id)?.version ?? 0) + 1
    const row: Omit<GovernancePolicyRow, 'id'> = {
      policy_ref: `${draft.policy_id}@${version}`,
      policy_id: draft.policy_id,
      version,
      space_id: draft.space_id ?? ANY_SPACE,
      description: draft.description ?? '',
      statements: draft.statements.map((statement) => statement as Json),
      created_at: nowTime(),
      created_by: actor,
    }
    const id = this.insert('gov_policies', row)
    const stored = { ...row, id }
    this.recordMutation({
      operation: 'publish_policy',
      space_id: stored.space_id,
      resource: stored.policy_ref,
      principal_id: actor,
      record: stored as unknown as Json,
    })
    return stored
  }

  /** The greatest version of a Policy. */
  activePolicy(policyId: string): GovernancePolicyRow | null {
    return this.one<GovernancePolicyRow>(
      'gov_policies',
      `SELECT * FROM gov_policies WHERE policy_id = ?
         ORDER BY version DESC LIMIT 1`,
      policyId,
    )
  }

  /** The version of a Policy that was in force at an instant (§177). */
  policyAt(policyId: string, at: string): GovernancePolicyRow | null {
    return this.one<GovernancePolicyRow>(
      'gov_policies',
      `SELECT * FROM gov_policies WHERE policy_id = ? AND created_at <= ?
         ORDER BY version DESC LIMIT 1`,
      policyId,
      at,
    )
  }

  /** Every version of a Policy, oldest first. */
  policyVersions(policyId: string): GovernancePolicyRow[] {
    return this.many<GovernancePolicyRow>(
      'gov_policies',
      'SELECT * FROM gov_policies WHERE policy_id = ? ORDER BY version',
      policyId,
    )
  }

  // --- Approvals ---------------------------------------------------------

  /** Opens an approval request for one concrete operation. */
  requestApproval(draft: ApprovalDraft, actor: string): ApprovalRow {
    const at = nowTime()
    const row: Omit<ApprovalRow, 'id'> = {
      space_id: draft.space_id,
      operation: draft.operation,
      resource: draft.resource,
      subject_digest: draft.subject_digest,
      required: Math.max(1, draft.required ?? 1),
      approvals: [],
      approver_ids: [],
      allow_self_approval: draft.allow_self_approval === true ? 1 : 0,
      status: 'pending',
      requested_by: actor,
      created_at: at,
      updated_at: at,
      expires_at: draft.expires_at ?? '',
      version: 1,
    }
    const id = this.insert('gov_approvals', row)
    const stored = { ...row, id }
    this.recordMutation({
      operation: 'request_approval',
      space_id: stored.space_id,
      resource: approvalId(id),
      principal_id: actor,
      record: stored as unknown as Json,
    })
    return stored
  }

  /**
   * Adds one Principal's approval.
   *
   * Refuses a second approval from the same Principal, and — unless the request
   * opted out — refuses the requester's own (§170). Both are the same rule:
   * *independent* approvals, or the count means nothing.
   */
  approve(id: number, approver: string, note = ''): ApprovalRow {
    const row = this.byId<ApprovalRow>('gov_approvals', id)
    if (row === null) {
      throw errors.notFoundOrNotVisible(`no Approval ${approvalId(id)}`)
    }
    if (row.status !== 'pending') {
      throw errors.requiresApproval(
        `approval ${approvalId(id)} is ${row.status}, not pending`,
      )
    }
    if (row.approver_ids.includes(approver)) {
      throw errors.notAuthorized(
        'one Principal counts once: a second approval from the same identity ' +
          'would make a two-of-N requirement satisfiable by one actor',
      )
    }
    if (row.allow_self_approval !== 1 && row.requested_by === approver) {
      throw errors.notAuthorized(
        'separation of duties: the Principal that requested this operation ' +
          'may not also approve it',
      )
    }
    const at = nowTime()
    row.approvals.push({ principal_id: approver, at, note } as Json)
    row.approver_ids.push(approver)
    if (row.approver_ids.length >= row.required) row.status = 'granted'
    row.updated_at = at
    row.version += 1
    this.update('gov_approvals', row)
    this.recordMutation({
      operation: 'approve',
      space_id: row.space_id,
      resource: approvalId(id),
      principal_id: approver,
      record: row as unknown as Json,
    })
    return row
  }

  /**
   * Marks an approval as spent.
   *
   * An approval authorizes one operation, not a standing licence: the same two
   * signatures must not be usable twice. Re-running the operation needs a new
   * approval, which is the whole point of requiring one.
   */
  consumeApproval(id: number): void {
    const row = this.byId<ApprovalRow>('gov_approvals', id)
    if (row === null) return
    row.status = 'consumed'
    row.updated_at = nowTime()
    row.version += 1
    this.update('gov_approvals', row)
    this.recordMutation({
      operation: 'consume_approval',
      space_id: row.space_id,
      resource: approvalId(id),
      record: row as unknown as Json,
    })
  }

  /** Looks an Approval up by row id. */
  findApproval(id: number): ApprovalRow | null {
    return this.byId<ApprovalRow>('gov_approvals', id)
  }

  /** The granted, unexpired approvals bound to one operation subject. */
  grantedApprovals(spaceId: string, subjectDigest: string): ApprovalRow[] {
    const now = nowTime()
    return this.many<ApprovalRow>(
      'gov_approvals',
      `SELECT * FROM gov_approvals
         WHERE space_id = ? AND subject_digest = ? AND status = 'granted'
         ORDER BY id`,
      spaceId,
      subjectDigest,
    ).filter((row) => row.expires_at === '' || row.expires_at > now)
  }

  // --- Audit -------------------------------------------------------------

  /** Appends one control-plane mutation to the audit log. */
  recordMutation(entry: MutationEntry): number {
    return this.appendAudit({
      entry_class: 'mutation',
      at: nowTime(),
      space_id: entry.space_id === undefined || entry.space_id === '' ? ANY_SPACE : entry.space_id,
      principal_id: entry.principal_id ?? '',
      operation: entry.operation,
      resource: entry.resource,
      // A mutation's "decision" is the verb: there was no allow/deny to record,
      // and leaving the column empty would make a mutation entry look like a
      // decision whose outcome was lost.
      decision: entry.operation,
      record: entry.record,
    })
  }

  /** Appends one authorization decision to the audit log. */
  recordDecision(row: Partial<GovernanceAuditRow>): number {
    return this.appendAudit({ ...row, entry_class: 'decision' })
  }

  /** Reads audit entries for a Space, newest first. */
  readAudit(spaceId: string, limit: number): GovernanceAuditRow[] {
    return this.many<GovernanceAuditRow>(
      'gov_audit',
      'SELECT * FROM gov_audit WHERE space_id = ? ORDER BY id DESC LIMIT ?',
      spaceId,
      limit,
    )
  }

  private appendAudit(partial: Partial<GovernanceAuditRow>): number {
    const row: Omit<GovernanceAuditRow, 'id'> = {
      entry_class: partial.entry_class ?? '',
      at: partial.at ?? nowTime(),
      space_id: partial.space_id === undefined || partial.space_id === '' ? ANY_SPACE : partial.space_id,
      principal_id: partial.principal_id ?? '',
      delegation_chain: partial.delegation_chain ?? [],
      operation: partial.operation ?? '',
      resource: partial.resource ?? '',
      decision: partial.decision ?? '',
      reason: partial.reason ?? '',
      policy_id: partial.policy_id ?? '',
      policy_version: partial.policy_version ?? 0,
      authorities_used: partial.authorities_used ?? [],
      approvals: partial.approvals ?? [],
      obligations: partial.obligations ?? {},
      record: partial.record ?? null,
      request_id: partial.request_id ?? '',
      tx_id: partial.tx_id ?? '',
    }
    return this.insert('gov_audit', row)
  }

  // --- row plumbing ------------------------------------------------------

  private insert(table: string, row: object): number {
    const { sql, values } = insertStatement(table, row)
    this.sql.exec(sql, ...values)
    const found = this.sql
      .exec<{ id: number }>('SELECT last_insert_rowid() AS id')
      .toArray()[0]
    if (!found) throw errors.internalError('no row id after an insert')
    return found.id
  }

  private update(table: string, row: { id: number }): void {
    const { sql, values } = updateStatement(table, row, row.id)
    this.sql.exec(sql, ...values)
  }

  private byId<T>(table: string, id: number): T | null {
    return this.one<T>(table, `SELECT * FROM ${table} WHERE id = ?`, id)
  }

  private one<T>(table: string, sql: string, ...values: SqlStorageValue[]): T | null {
    const row = this.sql.exec<SqlRow>(sql, ...values).toArray()[0]
    return row ? decodeRow<T>(table, row) : null
  }

  private many<T>(table: string, sql: string, ...values: SqlStorageValue[]): T[] {
    return this.sql
      .exec<SqlRow>(sql, ...values)
      .toArray()
      .map((row) => decodeRow<T>(table, row))
  }
}

/**
 * Normalizes an actor reference into the endpoint key it is compared against.
 *
 * A local element id becomes the local endpoint key; anything else is treated as
 * a canonical identity. The alternative — storing what the caller typed — makes
 * a binding that looks right and matches nothing, which is the worst possible
 * failure for a record whose whole job is to be found.
 */
export function actorKey(reference: string): string {
  const local = tryParseElementId(reference)
  return local === null
    ? endpointKey({ kind: 'canonical', canonicalId: reference })
    : endpointKey({ kind: 'local', id: local })
}
