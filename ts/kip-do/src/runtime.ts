import { formatSymbolRef } from './schema/symbol.js'
import { elementReferences } from './store/references.js'
import { ELEMENT_KINDS } from './id.js'
import { TABLES } from './store/rows.js'
import type { ControlRecord } from './control.js'
import type { DispatchRequest } from './cognitive.js'
import { publishControl, projectionPolicyAt, artifactValue } from './control.js'
import { dependencyValidity } from './projection/dependency.js'
import { pinnedPlane } from './schema/contracts.js'
/** Durable state and change coverage; the host owns scheduling and external I/O. */
import { errors } from './errors.js'
import { canonicalJson, isJsonMap, type Json, type JsonMap } from './json.js'
import { digest, validateValue } from './schema/contracts.js'
import { parseElementId, formatElementId } from './id.js'
import { Transaction, diffPlanes } from './tx.js'
import {
  requirePermitted,
  resourceOfElement,
  spaceResource,
} from './governance/index.js'
import { Context } from './kql/context.js'
import { projectionBasis } from './projection/index.js'
import { normalizeTime, nowTime } from './time.js'
import { render } from './view.js'
import type { Element, TransactionRow } from './store/rows.js'
import type { Session } from './nexus.js'

import { PROFILE_PREFIX as PROFILE } from './schema/profile-ref.js'
const obj = (v: Json | undefined): JsonMap => (isJsonMap(v) ? v : {})
const facet = (e: Element, n: string): JsonMap | undefined =>
  e.row.facets[PROFILE + n] as JsonMap | undefined
const fail = (s: string): never => {
  throw errors.constraintViolation(s)
}

function erasureEdges(tx: Transaction): [string, string][] {
  const edges = new Map<string, [string, string]>()
  const add = (a: string, b: string): void => {
    edges.set(JSON.stringify([a, b]), [a, b])
  }
  const rows: Element[] = []
  for (const kind of ELEMENT_KINDS)
    for (const row of tx.store.all<Element['row']>(
      TABLES[kind],
      `SELECT * FROM ${TABLES[kind]} WHERE space = ?`,
      tx.cx.space,
    ))
      rows.push({ kind, row } as Element)
  rows.push(...[...tx.staged.values()].map((s) => s.element))
  if (rows.length > 100000)
    fail('erasure closure exceeds scan budget; completion cannot be claimed')
  for (const e of rows) {
    const id = formatElementId({ kind: e.kind, seq: e.row.id })
    if (facet(e, 'ErasurePlan')) continue
    if (e.kind === 'Activity') {
      for (const r of e.row.inputs)
        add(typeof r === 'string' ? r : String(obj(r as Json).id), id)
      for (const r of e.row.outputs)
        add(id, typeof r === 'string' ? r : String(obj(r as Json).id))
      const basis = facet(e, 'DependencyBasis')
      for (const g of (basis?.groups ?? []) as JsonMap[])
        for (const pin of g.pins as JsonMap[]) add(String(pin.id), id)
    } else
      for (const ref of elementReferences(e)) add(formatElementId(ref.to), id)
  }
  for (const row of tx.store.all<ControlRecord>(
    'kip_control_records',
    'SELECT * FROM kip_control_records WHERE space = ?',
    tx.cx.space,
  ))
    if (row.key.startsWith('erasure_edges/'))
      for (const edge of obj(row.value).edges as string[][])
        add(edge[0]!, edge[1]!)
  return [...edges.values()]
}

function verifyErasurePlan(tx: Transaction, plan: JsonMap): void {
  validateValue(
    { $ref: 'urn:kip:2.0:schema:cognitive-records#/$defs/ErasurePlan' },
    plan,
  )
  if (plan.status === 'completed' && plan.scope === 'semantic_forgetting') {
    const pending = [...(plan.source_event_refs as string[])],
      seen = new Set<string>(),
      edges = erasureEdges(tx)
    if (!pending.length)
      fail('semantic erasure completion needs explicit source roots')
    while (pending.length) {
      const ref = pending.pop()!
      if (seen.has(ref)) continue
      seen.add(ref)
      const e =
        tx.staged.get(ref)?.element ?? tx.store.load(parseElementId(ref))
      if (!e || e.row.state !== 'purged')
        fail(
          'semantic erasure cannot complete while an owned dependent remains',
        )
      requirePermitted(
        tx.authority.authorize('read', resourceOfElement(e!), tx.auth),
      )
      pending.push(...edges.filter(([a]) => a === ref).map(([, b]) => b))
    }
  }
  for (const target of plan.targets as JsonMap[]) {
    if (target.state !== 'erased') continue
    const ref = String(target.ref),
      local = ['element', 'summary', 'index', 'cache', 'payload'].includes(
        String(target.surface),
      )
        ? (tx.staged.get(ref)?.element ?? tx.store.load(parseElementId(ref)))
        : null
    if (local)
      requirePermitted(
        tx.authority.authorize('read', resourceOfElement(local), tx.auth),
      )
    const erased = ['replay', 'blob'].includes(String(target.surface))
      ? obj(tx.store.controlAt(tx.cx.space, `artifact/${ref}`)?.value).state ===
        'erased'
      : !!local &&
        (local.row.state === 'purged' ||
          (target.surface === 'payload' &&
            local.kind === 'Evidence' &&
            local.row.payload_mode === 'purged'))
    if (!erased)
      fail(
        'ErasurePlan cannot claim erased targets without verified storage coverage',
      )
  }
}

export function validateErasurePlan(
  session: Session,
  space: string,
  plan: JsonMap,
): void {
  const authority = session.effectiveAuthority(space)
  requirePermitted(authority.authorize('purge', spaceResource(), session.auth))
  const tx = new Transaction(
    session.nexus.store,
    space,
    session.nexus.environment(space),
    {},
    true,
    authority,
    session.auth,
  )
  verifyErasurePlan(tx, plan)
}

export function validateDurable(tx: Transaction): void {
  for (const [id, s] of tx.staged) {
    if (
      s.isNew &&
      s.element.kind === 'Activity' &&
      s.element.row.client_key.startsWith('watch_fire:') &&
      !tx.authorizedWatchFires.has(id)
    )
      throw errors.notAuthorized(
        'watch_fire client keys are reserved for protected Watch advancement',
      )
  }
  const roots = new Set(
    [...tx.staged]
      .filter(([, s]) => s.verb === 'purge' || s.verb === 'purge_payload')
      .map(([id]) => id),
  )
  if (roots.size) {
    const edges = erasureEdges(tx).filter(
        ([a, b]) => roots.has(a) || roots.has(b),
      ),
      key = `erasure_edges/${tx.cx.tx_id}`
    tx.controlEffects.push({
      record_id: key,
      space: tx.cx.space,
      key,
      seq: 0,
      version: 1,
      kind: 'erasure',
      value: { edges },
      origin: tx.cx.origin,
    })
  }
  for (const [id, s] of tx.staged) {
    const e = s.element
    const erasure = facet(e, 'ErasurePlan')
    if (s.changed && erasure) verifyErasurePlan(tx, erasure)
    if (
      !s.changed ||
      s.verb === 'purge' ||
      e.kind !== 'Concept' ||
      ![PROFILE + 'SleepTask', PROFILE + 'Watch'].includes(e.row.schema_ref)
    )
      continue
    if (
      s.baseRow &&
      !tx.guarded.get(id)?.has('version') &&
      [...diffPlanes(e, s.baseRow).planes].some(
        (p) => !tx.guarded.get(id)?.has(p),
      )
    )
      throw errors.versionConflict(
        'every changed task/watch plane requires a version guard',
      )
    const before = s.baseRow ? tx.store.load(parseElementId(id)) : null
    if (e.row.schema_ref === PROFILE + 'SleepTask') {
      const old = before ? facet(before, 'LeaseState') : undefined,
        lease = facet(e, 'LeaseState')
      const beforeStatus = String(
          obj(s.baseRow?.attributes).status ?? 'pending',
        ),
        status = String(e.row.attributes.status)
      if (!lease) {
        if (old || ['running', 'completed', 'failed'].includes(status))
          fail('running and terminal tasks require retained fenced lease')
        continue
      }
      const expiry = normalizeTime(lease.expires_at, 'lease expiry')
      lease.expires_at = expiry
      if (status === 'completed' && beforeStatus !== 'running')
        fail('only a leased running task can complete')
      const fence = Number(lease.fencing_token),
        attempts = Number(lease.attempt_count)
      if (old) {
        const oldExpiry = normalizeTime(old.expires_at, 'lease expiry'),
          expired = oldExpiry <= tx.cx.at
        if (status === 'running' && (expired || beforeStatus !== 'running')) {
          if (
            fence !== Number(old.fencing_token) + 1 ||
            attempts !== Number(old.attempt_count) + 1 ||
            lease.owner !== tx.auth.principal_id ||
            expiry <= tx.cx.at
          )
            fail(
              'takeover must advance fence and attempt count under authenticated owner',
            )
        } else if (beforeStatus === 'running') {
          if (
            expired ||
            old.owner !== tx.auth.principal_id ||
            lease.owner !== tx.auth.principal_id ||
            fence !== old.fencing_token ||
            attempts !== old.attempt_count ||
            expiry < oldExpiry
          )
            fail('stale or expired lease cannot renew, complete or dispatch')
        } else if (canonicalJson(old) !== canonicalJson(lease))
          fail('inactive task lease changes require acquisition')
      } else if (
        status !== 'running' ||
        !['pending', 'blocked', 'failed'].includes(beforeStatus) ||
        fence !== 1 ||
        attempts !== 1 ||
        lease.owner !== tx.auth.principal_id ||
        expiry <= tx.cx.at
      )
        fail('initial lease requires authenticated acquisition with fence 1')
    } else {
      const old = before ? facet(before, 'WatchState') : undefined,
        watch = facet(e, 'WatchState'),
        beforeStatus = before
          ? String(obj(render(before).attributes).status)
          : null,
        afterStatus = String(e.row.attributes.status)
      if (!before && (afterStatus !== 'disarmed' || watch))
        fail('a new Watch must be disarmed without WatchState')
      if (
        beforeStatus !== null &&
        beforeStatus !== afterStatus &&
        !tx.authorizedWatchUpdates.has(id)
      )
        throw errors.notAuthorized(
          'Watch status is updated by protected arm/advance binding',
        )
      if (!watch && (old || e.row.attributes.status === 'armed'))
        fail('armed Watch requires persisted WatchState')
      if (watch) {
        if (
          s.baseRow &&
          !tx.authorizedWatchUpdates.has(id) &&
          ['watch_class', 'due_at'].some(
            (k) =>
              canonicalJson(obj(s.baseRow?.attributes)[k] ?? null) !==
              canonicalJson(e.row.attributes[k] ?? null),
          )
        )
          throw errors.notAuthorized(
            'changing an armed Watch deadline/class requires a protected new generation',
          )
        if (watch.condition_digest !== digest(e.row.attributes.condition!))
          fail('Watch condition digest mismatch')
        if (
          canonicalJson(old ?? null) !== canonicalJson(watch) &&
          !tx.authorizedWatchUpdates.has(id)
        )
          throw errors.notAuthorized(
            'WatchState is updated by protected arm/advance binding',
          )
      }
    }
  }
}

export function changePage(
  session: Session,
  after: number,
  limit: number,
  space: string,
  target = session.nexus.store.currentSeq(space),
): JsonMap {
  const store = session.nexus.store,
    authority = session.effectiveAuthority(space),
    head = store.currentSeq(space)
  requirePermitted(
    authority.authorize('read_history', spaceResource(), session.auth),
  )
  if (
    !Number.isSafeInteger(after) ||
    after < 0 ||
    after > target ||
    !Number.isSafeInteger(target) ||
    target > head ||
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > 10000
  )
    fail('invalid change page bounds')
  const cx = new Context(
    store,
    session.nexus.environment(space),
    space,
    authority,
    session.auth,
  )
  const rows = store.all<TransactionRow>(
    'transactions',
    "SELECT * FROM transactions WHERE space = ? AND seq > ? AND seq <= ? AND status = 'committed' ORDER BY seq LIMIT ?",
    space,
    after,
    target,
    limit + 1,
  )
  const floor = Number(
    obj(store.controlAt(space, 'internal/governance')?.value).coverage_floor ??
      0,
  )
  const complete = rows.length <= limit && after >= floor
  rows.length = Math.min(rows.length, limit)
  const through = complete ? target : (rows.at(-1)?.seq ?? after)
  const coverage = {
    through_seq: through,
    complete,
    authorization_view: projectionBasis(cx, cx.projectionPolicy, cx.validAt)
      .authorization_view!,
  }
  const changes: Json[] = []
  for (const row of rows) {
    const visible = row.changes.flatMap((c) => {
      const e = store.load(parseElementId(c.id)),
        v = e && authority.mayRead(e, session.auth)
      if (!v?.content) return []
      if (!v.constraints.fields.length) return [c]
      const value = {
        ...c,
        touched: (c.touched ?? []).filter((p) =>
          v.constraints.fields.includes(
            p.replace(/^fields\./, '').split('.')[0]!,
          ),
        ),
      }
      delete value.refs
      delete value.planes
      delete value.state
      return [value]
    })
    const controls = (obj(row.result).control_changes ?? []) as Json[]
    if (
      row.transaction_class === 'governance' &&
      obj(row.result).schema_environment_version !== undefined
    )
      controls.push({
        kind: 'schema',
        version: String(row.schema_environment_version),
      })
    if (row.changes.some((c) => c.op === 'merge'))
      controls.push({ kind: 'identity', version: String(row.seq) })
    if (!visible.length && !controls.length) continue
    changes.push({
      kip: '2.0',
      space_id: space,
      space_seq: row.seq,
      tx_id: row.tx_id,
      committed_at: row.committed_at,
      transaction_class: row.transaction_class,
      schema_environment_version: row.schema_environment_version,
      changes: visible as unknown as Json,
      control_changes: controls,
      coverage,
    })
  }
  return {
    changes,
    coverage,
    through_time: nowTime(),
    next_cursor: String(through),
    resync_required: after < floor,
  }
}

export function structuredCondition(condition: Json): boolean {
  return ['element', 'slot', 'type'].some((k) => k in obj(condition))
}

export function bindWatchCondition(cx: Context, condition: Json): Json {
  if (new TextEncoder().encode(JSON.stringify(condition)).length > 65536)
    throw errors.resourceExhausted('Watch condition exceeds 64 KiB')
  if (typeof condition === 'string') {
    if (!condition.trim())
      throw errors.typeMismatch('Watch text cannot be empty')
    return condition
  }
  if (
    !isJsonMap(condition) ||
    (!structuredCondition(condition) && condition.text === undefined)
  )
    throw errors.typeMismatch('Watch condition requires selectors or text')
  if (
    condition.text !== undefined &&
    (typeof condition.text !== 'string' || !condition.text.trim())
  )
    throw errors.typeMismatch('Watch text must be a nonempty string')
  const c = structuredClone(obj(condition))
  for (const key of Object.keys(c))
    if (!['element', 'slot', 'type', 'ops', 'touched', 'text'].includes(key))
      fail('unknown structured Watch selector')
  if (c.type !== undefined) {
    if (typeof c.type !== 'string') fail('Watch type must be name or reference')
    c.type = formatSymbolRef(
      cx.env.resolveSymbol('ConceptType', String(c.type), 'read'),
    )
  }
  for (const key of ['ops', 'touched'])
    if (c[key] !== undefined) {
      if (!Array.isArray(c[key])) fail('Watch ops/touched must be arrays')
      for (const item of c[key] as Json[])
        if (
          typeof item !== 'string' ||
          !item ||
          (key === 'ops' &&
            ![
              'create',
              'update',
              'lifecycle',
              'merge',
              'purge',
              'retention',
              'payload_purge',
            ].includes(item))
        )
          fail('invalid Watch selector member')
    }
  const references: string[] = []
  if (c.element !== undefined) {
    if (typeof c.element !== 'string') fail('Watch element must be reference')
    references.push(String(c.element))
  }
  if (c.slot !== undefined) {
    const slot = obj(c.slot)
    if (typeof slot.subject !== 'string' || typeof slot.predicate !== 'string')
      fail('Watch slot needs subject and predicate')
    const id = parseElementId(String(slot.subject))
    if (id.kind !== 'Concept') fail('Watch slot subject must be Concept')
    const canonical = cx.canonicalEndpoint({
      id: String(slot.subject),
    }) as JsonMap
    references.push(String(canonical.id))
    c.slot = {
      subject: canonical.id!,
      predicate: formatSymbolRef(
        cx.env.resolveSymbol('PredicateType', String(slot.predicate), 'read'),
      ),
    }
  }
  for (const reference of references) {
    const row = cx.load(parseElementId(reference), false),
      v = row && cx.authority.mayRead(row, cx.auth)
    if (!v?.content)
      throw errors.notFoundOrNotVisible(
        'Watch selector outside observation scope',
      )
    if (
      v.constraints.fields.length &&
      Array.isArray(c.touched) &&
      c.touched.some(
        (p) =>
          !v.constraints.fields.includes(
            String(p)
              .replace(/^fields\./, '')
              .split('.')[0]!,
          ),
      )
    )
      throw errors.notAuthorized(
        'Watch field selector outside observation scope',
      )
  }
  return c
}

function transaction(
  session: Session,
  space: string,
  ref: string,
  expected: number,
): Transaction {
  const tx = new Transaction(
    session.nexus.store,
    space,
    session.nexus.environment(space),
    { principal_id: session.auth.principal_id },
    false,
    session.effectiveAuthority(space),
    session.auth,
  )
  const id = parseElementId(ref)
  requirePermitted(
    tx.authority.authorize(
      'update',
      resourceOfElement(tx.load(id)),
      session.auth,
    ),
  )
  tx.expectVersions(id, [{ version: expected, plane: null }])
  return tx
}

export function leaseTask(
  session: Session,
  space: string,
  ref: string,
  expected: number,
  expiresAt: string,
): JsonMap {
  return session.nexus.transact(() => {
    const tx = transaction(session, space, ref, expected),
      e = tx.load(parseElementId(ref))
    if (e.kind !== 'Concept' || e.row.schema_ref !== PROFILE + 'SleepTask')
      fail('task must be SleepTask')
    const row = (e as Extract<Element, { kind: 'Concept' }>).row,
      old = facet(e, 'LeaseState')
    const takeover =
      row.attributes.status !== 'running' ||
      (!!old &&
        normalizeTime(old.expires_at, 'lease expiry') <= tx.cx.at)
    const lease = {
      owner: session.auth.principal_id,
      fencing_token: Number(old?.fencing_token ?? 0) + Number(takeover || !old),
      expires_at: normalizeTime(expiresAt, 'lease expiry'),
      attempt_count: Number(old?.attempt_count ?? 0) + Number(takeover || !old),
    }
    row.attributes.status = 'running'
    row.facets[PROFILE + 'LeaseState'] = lease
    tx.markChanged(parseElementId(ref), 'update')
    return { lease, receipt: tx.commit('') as unknown as Json }
  })
}

function checkDispatch(
  session: Session,
  space: string,
  request: DispatchRequest,
): JsonMap {
  const store = session.nexus.store,
    authority = session.effectiveAuthority(space)
  const task = store.load(parseElementId(request.task_ref)),
    lease = task && facet(task, 'LeaseState')
  if (
    !task ||
    task.row.space !== space ||
    obj(render(task).attributes).status !== 'running' ||
    !lease ||
    lease.owner !== session.auth.principal_id ||
    lease.fencing_token !== request.fencing_token ||
    normalizeTime(lease.expires_at, 'lease expiry') <= nowTime()
  )
    throw errors.versionConflict(
      'dispatch requires current unexpired lease fence',
    )
  requirePermitted(
    authority.authorize('update', resourceOfElement(task), session.auth),
  )
  return checkAttentionAttempt(session, space, request.attempt_ref)
}

export function checkAttentionAttempt(
  session: Session,
  space: string,
  attemptRef: string,
): JsonMap {
  const store = session.nexus.store,
    authority = session.effectiveAuthority(space)
  const activity = store.load(parseElementId(attemptRef)),
    attempt = activity && facet(activity, 'AttemptRecord')
  if (
    !activity ||
    activity.row.space !== space ||
    activity.row.state !== 'active' ||
    !attempt ||
    activity.row.origin.import
  )
    throw errors.notFoundOrNotVisible('local committed attempt unavailable')
  requirePermitted(
    authority.authorize('read', resourceOfElement(activity), session.auth),
  )
  artifactValue(store, space, obj(attempt.selection_policy))
  if (typeof attempt.trial_ref === 'string') {
    const trialRow = store.load(parseElementId(attempt.trial_ref)),
      trial = trialRow && facet(trialRow, 'TrialRecord')
    if (!trial) fail('trial unavailable')
    const policy = obj(
      store.controlAt(
        space,
        `evaluation_policy/${obj(trial!.evaluation_policy).id}`,
      )?.value,
    )
    if (
      !Array.isArray(policy.allowed_rules) ||
      !policy.allowed_rules.includes(obj(trial!.rule).content_digest!) ||
      !Array.isArray(policy.allowed_parameters) ||
      !policy.allowed_parameters.includes(
        obj(trial!.parameters).content_digest!,
      ) ||
      policy.observer_control_digest !==
        obj(trial!.comparability).observer_control_digest ||
      Number(trial!.quota) < Number(policy.minimum_independent_attempts)
    )
      throw errors.notAuthorized(
        'current policy no longer authorizes trial dispatch',
      )
  }

  if (attempt.preconditions_satisfied !== 'yes')
    fail('attempt preconditions not satisfied')
  const decision = store.load(parseElementId(String(attempt.decision_ref))),
    record = decision && facet(decision, 'DecisionRecord')
  if (!decision || !record) fail('action decision unavailable')
  requirePermitted(
    authority.authorize('read', resourceOfElement(decision!), session.auth),
  )
  const oldBasis = obj(record!.basis),
    policy = projectionPolicyAt(store, space, Number.MAX_SAFE_INTEGER, {
      context_refs: oldBasis.context_refs!,
      purpose: oldBasis.purpose!,
      risk: oldBasis.risk!,
    })
  const cx = new Context(
      store,
      session.nexus.environment(space),
      space,
      authority,
      session.auth,
    ),
    basis = projectionBasis(cx, policy, cx.validAt)
  for (const key of [
    'schema_environment_version',
    'identity_version',
    'policy',
    'trust_version',
    'authorization_view',
    'context_refs',
    'purpose',
    'risk',
  ])
    if (canonicalJson(oldBasis[key]) !== canonicalJson(basis[key]))
      throw errors.versionConflict(
        'action decision basis changed; re-plan before dispatch',
      )
  for (const reference of attempt.applied_revisions as string[]) {
    const revision = store.load(parseElementId(reference))
    if (!revision) fail('revision unavailable')
    const permission = requirePermitted(
      authority.authorize('read', resourceOfElement(revision!), session.auth),
    )
    if (
      revision!.row.governance.authority_class !== 'executable' ||
      (permission.constraints.max_influence_authority &&
        permission.constraints.max_influence_authority !== 'executable')
    )
      throw errors.notAuthorized(
        'exact revision lacks executable authority in this scope',
      )
    const families = revision!.row.structural[
        PROFILE + 'revision_of'
      ] as Json[],
      familyRef = families?.[0],
      family =
        familyRef &&
        store.load(
          parseElementId(
            typeof familyRef === 'string'
              ? familyRef
              : String(obj(familyRef).id),
          ),
        )
    if (
      !family ||
      !(family.row.structural[PROFILE + 'current_revision'] as Json[]).some(
        (r) => (typeof r === 'string' ? r : obj(r).id) === reference,
      )
    )
      throw errors.versionConflict('selected revision no longer current')
    if (
      dependencyValidity(cx, revision!, policy, cx.validAt).action_eligible !==
      true
    )
      throw errors.versionConflict('revision dependency validity changed')
  }
  const contract = facet(decision!, 'DependencyBasis')
  if (!contract) fail('action gate requires DependencyBasis')
  for (const group of contract!.groups as JsonMap[]) {
    if (group.role === 'context') continue
    const members = (group.pins as JsonMap[]).map((pin) => {
      const source = store.load(parseElementId(String(pin.id)))
      if (!source) return false
      const planes = obj(pin.planes),
        valid = Object.keys(planes).length
          ? Object.entries(planes).every(
              ([name, version]) =>
                pinnedPlane(
                  source.row.plane_versions as unknown as JsonMap,
                  name,
                ) === version,
            )
          : pin.version === source.row.version
      return (
        valid &&
        dependencyValidity(cx, source, policy, cx.validAt).action_eligible ===
          true
      )
    })
    if (
      !members.length ||
      !(group.role === 'any_of'
        ? members.some(Boolean)
        : members.every(Boolean))
    )
      throw errors.versionConflict('action prerequisite changed')
  }
  return attempt
}

export function enqueueDispatch(
  session: Session,
  space: string,
  request: DispatchRequest,
): JsonMap {
  const attempt = checkDispatch(session, space, request),
    key = `dispatch/${attempt.attempt_id}`,
    old = session.nexus.store.controlAt(space, key)
  if (old) {
    if (
      canonicalJson(obj(old.value).request) !==
      canonicalJson(request as unknown as Json)
    )
      fail('attempt already has different dispatch intent')
    return { version: old.version, intent: old.value }
  }
  const row = publishControl(
    session.nexus.store,
    space,
    key,
    'dispatch',
    0,
    {
      state: 'ready',
      attempt_id: attempt.attempt_id!,
      request: request as unknown as Json,
    },
    { principal_id: session.auth.principal_id },
  )
  return { version: row.version, intent: row.value }
}

export function beginDispatch(
  session: Session,
  space: string,
  attemptId: string,
  expected: number,
  fencingToken: number,
): JsonMap {
  const key = `dispatch/${attemptId}`,
    row = session.nexus.store.controlAt(space, key)
  if (!row) throw errors.notFoundOrNotVisible('dispatch intent unavailable')
  if (row.version !== expected)
    throw errors.versionConflict('dispatch intent version changed')
  const value = obj(row.value),
    request = value.request as unknown as DispatchRequest
  const attempt = session.nexus.store.load(parseElementId(request.attempt_ref))
  if (!attempt) throw errors.notFoundOrNotVisible('attempt unavailable')
  requirePermitted(
    session
      .effectiveAuthority(space)
      .authorize('read', resourceOfElement(attempt), session.auth),
  )
  if (value.state === 'completed' || value.state === 'outcome_unknown')
    return {
      action: value.state === 'completed' ? 'done' : 'outcome_unknown',
      idempotency_key: attemptId,
      version: row.version,
      intent: value,
    }
  if (fencingToken < request.fencing_token)
    throw errors.versionConflict('stale dispatch fence')
  request.fencing_token = fencingToken
  checkDispatch(session, space, request)
  const action =
    value.state === 'ready' || request.supports_idempotency
      ? 'dispatch'
      : request.supports_outcome_lookup
        ? 'lookup'
        : 'outcome_unknown'
  value.state = action === 'outcome_unknown' ? 'outcome_unknown' : 'dispatching'
  const saved = publishControl(
    session.nexus.store,
    space,
    key,
    'dispatch',
    expected,
    value,
    { principal_id: session.auth.principal_id },
  )
  return {
    action,
    idempotency_key: attemptId,
    intent: saved.value,
    version: saved.version,
  }
}

export function reconcileDispatch(
  session: Session,
  space: string,
  attemptId: string,
  expected: number,
  outcomeRef: string,
): Json {
  const key = `dispatch/${attemptId}`,
    row = session.nexus.store.controlAt(space, key),
    outcome = session.nexus.store.load(parseElementId(outcomeRef))
  if (!row || !outcome)
    throw errors.notFoundOrNotVisible('dispatch or outcome unavailable')
  requirePermitted(
    session
      .effectiveAuthority(space)
      .authorize('read', resourceOfElement(outcome), session.auth),
  )
  const value = obj(row.value),
    record = facet(outcome, 'OutcomeRecord')
  if (
    outcome.row.space !== space ||
    !record ||
    record.attempt_ref !== obj(value.request).attempt_ref ||
    record.terminal !== true
  )
    fail('reconciliation must name attempt terminal observation')
  if (value.state === 'completed' && value.outcome_ref === outcomeRef)
    return value
  value.state =
    record!.outcome_status === 'unknown' ? 'outcome_unknown' : 'completed'
  value.outcome_ref = outcomeRef
  return publishControl(
    session.nexus.store,
    space,
    key,
    'dispatch',
    expected,
    value,
    { principal_id: session.auth.principal_id },
  ).value
}
