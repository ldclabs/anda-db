import {
  requirePermitted,
  spaceResource,
  type EffectiveAuthority,
  type AuthContext,
} from './governance/index.js'
import { parseElementId } from './id.js'
/** Protected Nexus contracts, callable by an authenticated Brain host. */
import { errors } from './errors.js'
import { digest } from './schema/contracts.js'
import {
  baseline,
  forecast,
  memoryDefault,
  MEMORY_DEFAULT_ID,
  policyFromSettings,
  type Policy,
} from './projection/policy.js'
import { nowTime } from './time.js'
import type { Json, JsonMap } from './json.js'
import type { Store } from './store/store.js'

export interface ControlRecord {
  id: number
  record_id: string
  space: string
  key: string
  seq: number
  version: number
  kind: string
  value: Json
  origin: JsonMap
}
export const initialProjection = (): JsonMap => ({
  baseline: baseline() as unknown as Json,
  forecast: forecast() as unknown as Json,
  'memory-default': memoryDefault() as unknown as Json,
})

export function publishControl(
  store: Store,
  space: string,
  key: string,
  kind: string,
  expected: number,
  value: Json,
  origin: JsonMap,
): ControlRecord {
  if ((store.controlAt(space, key)?.version ?? 0) !== expected)
    throw errors.versionConflict('protected control version changed')
  if (!Number.isSafeInteger(expected) || expected >= Number.MAX_SAFE_INTEGER)
    throw errors.constraintViolation('control version exhausted')
  const seq = store.nextSeq(space),
    txId = `${space}#${seq}`
  const row = {
    record_id: `${txId}:${key}`,
    space,
    key,
    seq,
    version: expected + 1,
    kind,
    value,
    origin,
  }
  store.putControl(row)
  store.putGovernanceTransaction({
    space,
    seq,
    tx_id: txId,
    snapshot_seq: seq - 1,
    committed_at: nowTime(),
    schema_environment_version: store.space(space)!.schema_environment_version,
    changes: [],
    result: {
      control_changes: [
        'policy',
        'trust',
        'identity',
        'authorization',
        'schema',
        'recording',
      ].includes(kind)
        ? [{ kind, version: String(seq) }]
        : [],
      key,
      version: row.version,
    },
  })
  return store.controlAt(space, key)!
}

export function projectionPolicyAt(
  store: Store,
  space: string,
  seq: number,
  settings: JsonMap,
): Policy {
  const explicit = typeof settings.policy === 'string'
  const coordinate = explicit ? Number.MAX_SAFE_INTEGER : seq
  const saved = store.controlAt(space, 'projection', coordinate)
  const trust = store.controlAt(space, 'trust', coordinate)
  if (!saved || !trust)
    throw errors.historicalSnapshotUnavailable(
      'projection or trust history unavailable',
    )
  const requested = policyFromSettings(settings)
  const name = requested.id.startsWith('kip:policy:forecast')
    ? 'forecast'
    : requested.id.startsWith(MEMORY_DEFAULT_ID)
      ? 'memory-default'
      : 'baseline'
  // The standard memory policy is fixed by its artifact (§21.13), so a Space
  // created before it was bundled still resolves it.
  const stored = (saved.value as JsonMap)[name]
  const policy = (
    stored === undefined && name === 'memory-default'
      ? memoryDefault()
      : structuredClone(stored)
  ) as unknown as Policy
  if ('accept' in settings) policy.accept = requested.accept
  if ('material' in settings) policy.material = requested.material
  if (
    ['modes', 'include_predicted', 'include_hypothetical'].some(
      (k) => k in settings,
    )
  )
    policy.modes = requested.modes
  if ('explanation' in settings) policy.explanation = requested.explanation
  if (policy.material > policy.accept)
    throw errors.constraintViolation('material exceeds accept')
  Object.assign(policy, {
    id: requested.id,
    explicit_selection: explicit,
    context_refs: requested.context_refs,
    purpose: requested.purpose,
    risk: requested.risk,
    trust_weights: (trust.value as JsonMap).weights,
    default_trust_weight: (trust.value as JsonMap).default_weight,
    contextual_trust_rules: (trust.value as JsonMap).rules ?? [],
    trust_version: digest(trust.value),
  })
  return policy
}

export function artifactValue(store: Store, space: string, pin: JsonMap): Json {
  const row = store.controlAt(space, `artifact/${pin.artifact_ref}`)
  const value = row?.value as JsonMap | undefined
  if (!value || value.state !== 'available')
    throw errors.notFoundOrNotVisible('artifact erased or unavailable')
  if (digest(value.content!) !== pin.content_digest)
    throw errors.digestMismatch('artifact bytes do not match pinned digest')
  return value.content!
}

export function requireArtifactMaterial(
  store: Store,
  space: string,
  pin: JsonMap,
  refs: string[],
): void {
  const row = store.controlAt(space, `artifact/${pin.artifact_ref}`),
    sources = (row?.value as JsonMap | undefined)?.source_refs
  if (!Array.isArray(sources) || refs.some((r) => !sources.includes(r)))
    throw errors.constraintViolation(
      'replay artifact must inherit governance and erasure from every material input',
    )
}

export function authorizedArtifact(
  store: Store,
  space: string,
  reference: string,
  authority: EffectiveAuthority,
  auth: AuthContext,
): { payload: Json; sources: string[] } {
  const row = store.controlAt(space, `artifact/${reference}`),
    value = row?.value as JsonMap | undefined
  if (!value || !Array.isArray(value.source_refs))
    throw errors.notFoundOrNotVisible('artifact unavailable')
  const sources = value.source_refs as string[]
  if (!sources.length)
    requirePermitted(
      authority.authorize('read_governance_history', spaceResource(), auth),
    )
  for (const reference of sources) {
    const row = store.load(parseElementId(reference)),
      visibility = row && authority.mayRead(row, auth)
    if (!visibility?.content || visibility.constraints.fields.length)
      throw errors.notFoundOrNotVisible('artifact unavailable')
  }
  return {
    payload: artifactValue(store, space, {
      artifact_ref: reference,
      content_digest: value.content_digest!,
    }),
    sources,
  }
}
