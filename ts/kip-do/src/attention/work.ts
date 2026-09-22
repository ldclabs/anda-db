import type { Session, CognitiveNexus } from '../nexus.js'
import type { Json, JsonMap } from '../json.js'
import type {
  WakeRecord,
  WakeRetry,
  WakeContinuation,
  RuntimePin,
  WatchCheckpoint,
  WakeResumeVerifier,
} from './types.js'
import { errors } from '../errors.js'
import { digest } from '../schema/contracts.js'
import { normalizeTime } from '../time.js'
import { parseKip } from '../kip/parser.js'
import { tryParseElementId } from '../id.js'
import { planKml } from '../kml/index.js'
import { kmlPermissions } from '../governance/gate.js'
import {
  PROFILE,
  FORMAT,
  CONTINUATION,
  CONFIG,
  obj,
  json,
  eq,
  bytes,
  bounded,
  safe,
  validDigest,
  validatePin,
  exact,
  invalid,
  conflict,
  next,
  fullRead,
  permit,
  context,
  basis,
  checkpointKey,
  transaction,
  stageControl,
  runtimeRef,
  replay,
  commit,
} from './common.js'

export function loadWake(s: Session, space: string, ref: string): WakeRecord {
  permit(s, space, 'maintain')
  const row = s.nexus.store.controlAt(space, ref)
  if (!row) throw errors.notFoundOrNotVisible('wake unavailable')
  if (row.kind !== 'wake') invalid('not a wake record')
  const wake = structuredClone(row.value) as unknown as WakeRecord
  if (
    wake.wake_ref !== ref ||
    wake.version !== row.version ||
    ![FORMAT, CONTINUATION].includes(wake.format) ||
    (wake.format === FORMAT &&
      (wake.parent_ref !== undefined || wake.continuation_key !== undefined)) ||
    (wake.format === CONTINUATION &&
      (!wake.parent_ref || !wake.continuation_key))
  )
    invalid('invalid wake identity/version')
  fullRead(s, space, wake.fire.watch_ref)
  fullRead(s, space, wake.fire_activity_ref)
  return wake
}
export function currentWake(s: Session, space: string, wake: WakeRecord): void {
  const cfg = s.nexus.store.controlAt(space, CONFIG)
  if (!cfg) conflict('binding_unavailable')
  if (!eq(cfg.value, { scope: wake.scope, pins: wake.pins }))
    conflict('basis_changed')
  const watch = fullRead(s, space, wake.fire.watch_ref)
  if (
    watch.kind !== 'Concept' ||
    watch.row.attributes.status !== 'fired' ||
    obj(watch.row.facets[PROFILE + 'WatchState']).arm_generation !==
      wake.fire.arm_generation
  )
    conflict('generation_conflict')
  const saved = s.nexus.store.controlAt(
    space,
    checkpointKey(wake.fire.watch_ref, wake.fire.arm_generation),
  )
  if (!saved) conflict('history_gap')
  const cp = saved.value as unknown as WatchCheckpoint
  if (
    cp.format !== 'nexus:watch-checkpoint-v1' ||
    cp.watch_ref !== wake.fire.watch_ref ||
    cp.arm_generation !== wake.fire.arm_generation
  )
    invalid('unsupported or mismatched Watch checkpoint')
  if (!eq(cp.basis, basis(context(s, space)))) conflict('basis_changed')
}
export function live(s: Session, wake: WakeRecord, now = Date.now()): void {
  if (
    wake.state.stage !== 'running' ||
    wake.state.lease.owner !== s.auth.principal_id ||
    wake.state.lease.expires_at_ms <= now
  )
    conflict('lease_lost')
}
function expiry(at: string, now: number): number {
  const value = Date.parse(normalizeTime(at, 'lease expiry'))
  if (!safe(value) || value <= now || value > now + 300000)
    invalid('wake lease must expire within five real-time minutes')
  return value
}
type Action =
  | { kind: 'claim' | 'renew'; expiresAt: string }
  | { kind: 'block'; retry: WakeRetry }
  | { kind: 'resume'; verified?: { condition: string; pin: RuntimePin } }
  | { kind: 'cancel'; reason: string }
  | {
      kind: 'finish'
      command: string
      parameters: JsonMap
      continuations: WakeContinuation[]
    }
export function updateWake(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  fence: number,
  action: Action,
): JsonMap {
  return s.nexus.transact(() => {
    const wake = loadWake(s, space, ref),
      operation = `${action.kind}_wake`
    const body =
      action.kind === 'claim' || action.kind === 'renew'
        ? { expires_at: normalizeTime(action.expiresAt, 'lease expiry') }
        : action.kind === 'block'
          ? action.retry
          : action.kind === 'cancel'
            ? { reason: action.reason }
            : action.kind === 'finish'
              ? {
                  command: action.command,
                  parameters: action.parameters,
                  continuations: action.continuations,
                }
              : null
    const operationKey = runtimeRef('operation', {
      domain: 'anda-brain:operation-v1',
      scope: wake.scope,
      wake_ref: ref,
      operation,
      step: expected,
    })
    const key = `attention\x1f${s.auth.principal_id}\x1f${operationKey}`,
      hash = digest(json({ request: { fence, body }, pins: wake.pins })),
      previous = replay(s, space, key, hash)
    if (previous) {
      for (const output of previous.outputs as string[])
        if (tryParseElementId(output)) fullRead(s, space, output, false)
      return previous
    }
    if (wake.version !== expected) conflict('version_conflict')
    if (wake.fence !== fence) conflict('lease_lost')
    if (['completed', 'cancelled'].includes(wake.state.stage))
      conflict('version_conflict')
    if (action.kind !== 'cancel' && action.kind !== 'block')
      currentWake(s, space, wake)
    const now = Date.now(),
      receiptRef = operationKey.replace('operation/', 'receipt/'),
      outputs: string[] = []
    switch (action.kind) {
      case 'claim':
        if (
          !(
            wake.state.stage === 'pending' && wake.state.not_before_ms <= now
          ) &&
          !(
            wake.state.stage === 'running' &&
            wake.state.lease.expires_at_ms <= now
          )
        )
          conflict('not_ready')
        wake.fence = next(wake.fence)
        wake.state = {
          stage: 'running',
          lease: {
            owner: s.auth.principal_id,
            expires_at_ms: expiry(action.expiresAt, now),
          },
        }
        break
      case 'renew': {
        live(s, wake, now)
        if (wake.state.stage !== 'running') conflict('lease_lost')
        const expires = expiry(action.expiresAt, now)
        if (expires < wake.state.lease.expires_at_ms)
          invalid('lease renewal cannot shorten expiry')
        wake.state.lease.expires_at_ms = expires
        break
      }
      case 'block': {
        live(s, wake, now)
        const r = action.retry
        exact(r, ['reason', 'resume'])
        if (
          ![
            'basis_changed',
            'history_gap',
            'budget_exhausted',
            'binding_unavailable',
            'semantic_unknown',
            'outcome_unknown',
          ].includes(r.reason)
        )
          invalid('invalid blocked reason')
        exact(
          r.resume,
          r.resume.kind === 'at'
            ? ['kind', 'not_before_ms']
            : ['kind', 'condition_digest'],
        )
        if (
          !(
            r.resume.kind === 'at' &&
            safe(r.resume.not_before_ms) &&
            r.resume.not_before_ms > now &&
            r.reason !== 'outcome_unknown'
          ) &&
          !(
            r.resume.kind === 'on_change' &&
            validDigest(r.resume.condition_digest)
          )
        )
          invalid('blocked work needs an explicit bounded resume condition')
        wake.state = { stage: 'blocked', retry: structuredClone(r) }
        break
      }
      case 'resume': {
        if (wake.state.stage !== 'blocked') conflict('not_ready')
        const r = wake.state.retry.resume
        if (r.kind === 'on_change') {
          if (action.verified?.condition !== r.condition_digest)
            throw errors.unsupportedCapability(
              'on_change wake recovery requires a registered condition verifier',
            )
        } else if (r.not_before_ms > now) conflict('not_ready')
        wake.state = { stage: 'pending', not_before_ms: now }
        break
      }
      case 'cancel':
        if (!bounded(action.reason))
          invalid('bounded cancellation reason required')
        wake.fence = next(wake.fence)
        wake.state = { stage: 'cancelled', receipt_ref: receiptRef }
        break
      case 'finish': {
        live(s, wake, now)
        if (bytes(action.command) > 65536 || action.continuations.length > 16)
          invalid('wake completion exceeds output budget')
        const index = s.nexus.store.controlAt(
          space,
          `attention/dispatches/${ref}`,
        )
        for (const key of (index?.value ?? []) as string[])
          if (
            obj(s.nexus.store.controlAt(space, key)?.value).state !==
            'completed'
          )
            conflict('outcome_unknown')
        wake.state = { stage: 'completed', receipt_ref: receiptRef }
        break
      }
    }
    wake.version = next(wake.version)
    const tx = transaction(s, space)
    if (['renew', 'block', 'finish'].includes(action.kind))
      tx.attentionLeases.push({ reference: ref, version: expected, fence })
    if (action.kind === 'finish') {
      if (action.command.trim()) {
        const parsed = parseKip(action.command)
        if (!('Kml' in parsed)) invalid('wake outputs must be one KML block')
        if (parsed.Kml.clauses.length > 128)
          invalid('wake completion exceeds clause budget')
        for (const p of kmlPermissions(parsed.Kml)) permit(s, space, p)
        planKml(tx, parsed.Kml, action.parameters)
        outputs.push(...Object.values(tx.handles()))
      }
      const keys = new Set<string>()
      for (const child of action.continuations) {
        exact(child, ['key', 'not_before_ms'])
        if (
          typeof child.key !== 'string' ||
          !child.key ||
          bytes(child.key) > 128 ||
          keys.has(child.key) ||
          !safe(child.not_before_ms)
        )
          invalid('invalid/duplicate continuation key')
        keys.add(child.key)
        const childRef = runtimeRef('wake', {
          domain: 'anda-brain:wake-continuation-v1',
          scope: wake.scope,
          parent_ref: ref,
          operation_key: operationKey,
          key: child.key,
        })
        const childWake: WakeRecord = {
          ...wake,
          format: CONTINUATION,
          wake_ref: childRef,
          parent_ref: ref,
          continuation_key: child.key,
          version: 1,
          fence: 0,
          state: { stage: 'pending', not_before_ms: child.not_before_ms },
        }
        stageControl(tx, childRef, 0, 'wake', childWake)
        outputs.push(childRef)
      }
      if (outputs.length > 128) invalid('wake completion exceeds output count')
      const before = loadWake(s, space, ref)
      live(s, before)
      currentWake(s, space, before)
    }
    stageControl(tx, ref, expected, 'wake', wake)
    stageControl(tx, receiptRef, 0, 'runtime', {
      format: FORMAT,
      identity: {
        scope: wake.scope,
        operation_key: operationKey,
        request_digest: hash,
      },
      pins: wake.pins,
      state: { status: 'committed', commit_seq: tx.snapshotSeq + 1, outputs },
    })
    return commit(s, tx, key, hash, {
      wake: json(wake),
      receipt_ref: receiptRef,
      outputs,
      resume_verification:
        action.kind === 'resume' && action.verified
          ? json({
              condition_digest: action.verified.condition,
              verifier: action.verified.pin,
            })
          : null,
    })
  })
}
interface Binding {
  condition: Json
  pin: RuntimePin
  verifier: WakeResumeVerifier
}
const verifiers = new WeakMap<CognitiveNexus, Map<string, Binding>>()
export function registerResumeVerifier(
  n: CognitiveNexus,
  condition: Json,
  pin: RuntimePin,
  verifier: WakeResumeVerifier,
): string {
  validatePin(pin)
  if (
    bytes(JSON.stringify(condition)) > 65536 ||
    typeof verifier !== 'function'
  )
    invalid('invalid bounded resume verifier')
  const key = digest(condition),
    bindings = verifiers.get(n) ?? new Map<string, Binding>()
  if (bindings.has(key)) invalid('resume verifier already registered')
  bindings.set(key, {
    condition: structuredClone(condition),
    pin: structuredClone(pin),
    verifier,
  })
  verifiers.set(n, bindings)
  return key
}
export async function resumeWake(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  fence: number,
): Promise<JsonMap> {
  const wake = loadWake(s, space, ref)
  if (
    wake.state.stage !== 'blocked' ||
    wake.state.retry.resume.kind !== 'on_change'
  )
    return updateWake(s, space, ref, expected, fence, { kind: 'resume' })
  if (wake.version !== expected || wake.fence !== fence)
    conflict('version_or_fence_conflict')
  const condition = wake.state.retry.resume.condition_digest,
    binding = verifiers.get(s.nexus)?.get(condition)
  if (!binding)
    throw errors.unsupportedCapability(
      'no registered verifier for this wake resume condition',
    )
  // No SQLite transaction spans host I/O. Recheck versions, grants and basis after awaiting.
  if (
    (await binding.verifier({
      wake: structuredClone(wake),
      condition: structuredClone(binding.condition),
    })) !== true
  )
    conflict('not_ready')
  return updateWake(s, space, ref, expected, fence, {
    kind: 'resume',
    verified: { condition, pin: binding.pin },
  })
}
