/** Native attention records share the graph's SQLite transaction and journal. */
import { errors, KipError } from '../errors.js'
import { canonicalJson, isJsonMap, type Json, type JsonMap } from '../json.js'
import { digest } from '../schema/contracts.js'
import { Context } from '../kql/context.js'
import { projectionBasis } from '../projection/index.js'
import { Transaction } from '../tx.js'
import { parseElementId, formatElementId } from '../id.js'
import { normalizeTime } from '../time.js'
import {
  requirePermitted,
  resourceOfElement,
  spaceResource,
  type Permission,
} from '../governance/index.js'
import type { Element } from '../store/rows.js'
import type { Session } from '../nexus.js'
import type {
  AttentionConfig,
  RuntimePin,
  WatchFire,
  WakeRecord,
} from './types.js'

export { PROFILE_PREFIX as PROFILE } from '../schema/profile-ref.js'
export const FORMAT = 'anda-brain:attention-v1'
export const CONTINUATION = 'anda-brain:attention-continuation-v1'
export const CONFIG = 'attention/config'
export const obj = (v: Json | undefined): JsonMap => (isJsonMap(v) ? v : {})
export const json = (v: unknown): Json => v as Json
export const eq = (a: unknown, b: unknown): boolean =>
  canonicalJson(json(a ?? null)) === canonicalJson(json(b ?? null))
export const bytes = (s: string): number => new TextEncoder().encode(s).length
export const bounded = (v: string, max = 256): boolean =>
  typeof v === 'string' && !!v && bytes(v) <= max && v.trim() === v
export const validDigest = (v: string): boolean =>
  typeof v === 'string' && /^sha256:[0-9a-f]{64}$/.test(v)
export const safe = (v: number): boolean => Number.isSafeInteger(v) && v >= 0
export function invalid(message: string): never {
  throw errors.constraintViolation(message)
}
export function conflict(reason: string): never {
  throw new KipError('VersionConflict', reason, { attention_reason: reason })
}
export function next(v: number): number {
  if (!safe(v) || v === Number.MAX_SAFE_INTEGER)
    throw errors.resourceExhausted('attention counter exhausted')
  return v + 1
}
export const runtimeRef = (prefix: string, input: unknown): string =>
  `${prefix}/v1/${digest(json(input)).slice(7)}`
export const requestKey = (s: Session, input: unknown): string =>
  `attention\x1f${s.auth.principal_id}\x1f${digest(json(input))}`
export const checkpointKey = (watch: string, generation: number): string =>
  `attention/watch/${watch}/${generation}`
export function exact(value: unknown, keys: string[]): void {
  if (
    !isJsonMap(json(value)) ||
    Object.keys(value as object).some((k) => !keys.includes(k))
  )
    invalid('invalid attention record fields')
}
export function validatePin(pin: RuntimePin): void {
  exact(pin, ['id', 'digest'])
  if (!bounded(pin.id) || !validDigest(pin.digest))
    invalid('invalid runtime configuration pin')
}
export function validateConfig(config: AttentionConfig): void {
  exact(config, ['scope', 'pins'])
  exact(config.scope, ['space_id', 'space_instance'])
  exact(config.pins, ['policy', 'evaluator', 'binding'])
  if (!bounded(config.scope.space_id) || !bounded(config.scope.space_instance))
    invalid('invalid runtime scope')
  validatePin(config.pins.policy)
  for (const pin of [config.pins.evaluator, config.pins.binding])
    if (pin !== null) validatePin(pin)
}
export function context(s: Session, space: string): Context {
  return new Context(
    s.nexus.store,
    s.nexus.environment(space),
    space,
    s.effectiveAuthority(space),
    s.auth,
  )
}
export function basis(cx: Context): JsonMap {
  const b = projectionBasis(cx, cx.projectionPolicy, cx.validAt)
  return {
    schema: b.schema_environment_version!,
    identity: b.identity_version!,
    policy: b.policy!,
    trust: b.trust_version!,
    authorization: b.authorization_view!,
  }
}
export function permit(
  s: Session,
  space: string,
  permission: Permission,
  e?: Element,
): void {
  requirePermitted(
    s
      .effectiveAuthority(space)
      .authorize(
        permission,
        e ? resourceOfElement(e) : spaceResource(),
        s.auth,
      ),
  )
}
export function fullRead(
  s: Session,
  space: string,
  ref: string,
  active = true,
): Element {
  const e = s.nexus.store.load(parseElementId(ref)),
    v = e && s.effectiveAuthority(space).mayRead(e, s.auth)
  if (
    !e ||
    e.row.space !== space ||
    (active && e.row.state !== 'active') ||
    !v?.content ||
    v.constraints.fields.length
  )
    throw errors.notFoundOrNotVisible('attention source is not fully visible')
  permit(s, space, 'read', e)
  return e
}
export function transaction(s: Session, space: string): Transaction {
  return new Transaction(
    s.nexus.store,
    space,
    s.nexus.environment(space),
    { principal_id: s.auth.principal_id },
    false,
    s.effectiveAuthority(space),
    s.auth,
  )
}
export function stageControl(
  tx: Transaction,
  key: string,
  expected: number,
  kind: string,
  value: unknown,
): void {
  if (
    (tx.store.controlAt(tx.cx.space, key)?.version ?? 0) !== expected ||
    tx.controlEffects.some((r) => r.key === key)
  )
    conflict('version_conflict')
  tx.controlEffects.push({
    record_id: `${tx.cx.tx_id}:${key}`,
    space: tx.cx.space,
    key,
    seq: tx.snapshotSeq + 1,
    version: next(expected),
    kind,
    value: json(value),
    origin: tx.cx.origin,
  })
}
export function configuration(s: Session, space: string): AttentionConfig {
  const saved = s.nexus.store.controlAt(space, CONFIG)
  if (saved) {
    const cfg = saved.value as unknown as AttentionConfig
    validateConfig(cfg)
    return cfg
  }
  const cfg: AttentionConfig = {
    scope: {
      space_id: space,
      space_instance: [...crypto.getRandomValues(new Uint8Array(32))]
        .map((b) => b.toString(16).padStart(2, '0'))
        .join(''),
    },
    pins: {
      policy: {
        id: 'nexus:structured-watch-v1',
        digest: digest({ engine: 'nexus:structured-watch-v1' }),
      },
      evaluator: null,
      binding: null,
    },
  }
  return cfg
}
export function replay(
  s: Session,
  space: string,
  key: string,
  requestDigest: string,
): JsonMap | null {
  const row = s.nexus.store.transactionByKey(space, key)
  if (!row) return null
  if (row.request_digest !== requestDigest) invalid('idempotency_conflict')
  const runtime = obj(row.result).runtime
  if (!isJsonMap(runtime))
    invalid('attention receipt lacks its retained result')
  return {
    ...runtime,
    receipt: {
      status: row.status,
      tx_id: row.tx_id,
      space_id: space,
      space_seq: row.seq,
      snapshot_seq: row.snapshot_seq,
      committed_at: row.committed_at,
      request_digest: row.request_digest,
      schema_environment_version: row.schema_environment_version,
      handles: obj(row.result).handles ?? {},
      changes: json(row.changes),
    },
  }
}
export function commit(
  s: Session,
  tx: Transaction,
  key: string,
  requestDigest: string,
  result: JsonMap,
): JsonMap {
  tx.runtimeResult = result
  tx.commit(key, requestDigest)
  return replay(s, tx.cx.space, key, requestDigest)!
}
export function validateCommitLeases(tx: Transaction): void {
  const now = Date.now()
  for (const guard of tx.attentionLeases) {
    const row = tx.store.controlAt(tx.cx.space, guard.reference),
      wake = row?.value as unknown as WakeRecord | undefined
    if (
      !wake ||
      row!.version !== guard.version ||
      wake.fence !== guard.fence ||
      wake.state.stage !== 'running' ||
      wake.state.lease.owner !== tx.auth.principal_id ||
      wake.state.lease.expires_at_ms <= now
    )
      conflict('lease_lost')
  }
}
export function fireKey(fire: WatchFire): string {
  const id = parseElementId(fire.watch_ref)
  if (
    id.kind !== 'Concept' ||
    !id.seq ||
    formatElementId(id) !== fire.watch_ref ||
    !safe(fire.arm_generation) ||
    !fire.arm_generation
  )
    invalid('invalid Watch fire identity')
  let suffix: string
  if (fire.trigger.kind === 'delta') {
    if (!safe(fire.trigger.matched_seq) || !fire.trigger.matched_seq)
      invalid('invalid matching sequence')
    suffix = String(fire.trigger.matched_seq)
  } else {
    if (!safe(fire.trigger.due_seq)) invalid('invalid matching sequence')
    suffix = `silence:${normalizeTime(fire.trigger.due_at, 'Watch deadline')}`
  }
  return `watch_fire:${fire.watch_ref}:${fire.arm_generation}:${suffix}`
}
