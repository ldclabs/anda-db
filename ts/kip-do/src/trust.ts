/** Explicit trust settings and governed calibration provenance; algorithms live in the host. */
import type { Session } from './nexus.js'
import type { Json, JsonMap } from './json.js'
import type { ArtifactPin } from './cognitive.js'
import { isJsonMap } from './json.js'
import { authorizedArtifact, publishControl } from './control.js'
import { parseElementId, formatElementId } from './id.js'
import { formatSymbolRef } from './schema/symbol.js'
import { digest } from './schema/contracts.js'
import { errors } from './errors.js'
import {
  bytes,
  json,
  exact,
  safe,
  invalid,
  permit,
  requestKey,
  transaction,
  stageControl,
  replay,
  commit,
} from './attention/common.js'

export interface ContextualTrustRule {
  id: string
  actor_ref: string
  predicate_ref: string | null
  context_ref: string | null
  weight: number
}
export interface TrustConfiguration {
  weights: Record<string, number>
  default_weight: number
  rules?: ContextualTrustRule[]
}
export interface TrustCalibrationProposal {
  format: string
  space_id: string
  expected_version: number
  configuration: TrustConfiguration
  method: ArtifactPin
  evidence_refs: string[]
  uncertainty: Json
}
export function validateTrust(configuration: TrustConfiguration): void {
  exact(configuration, ['weights', 'default_weight', 'rules'])
  if (
    !isJsonMap(json(configuration.weights)) ||
    (configuration.rules !== undefined && !Array.isArray(configuration.rules))
  )
    invalid('invalid trust configuration')
  const rules = configuration.rules ?? []
  if (
    Object.keys(configuration.weights).length > 1024 ||
    rules.length > 128 ||
    [
      configuration.default_weight,
      ...Object.values(configuration.weights),
      ...rules.map((r) => r.weight),
    ].some(
      (v) => typeof v !== 'number' || !Number.isFinite(v) || v < 0 || v > 1,
    )
  )
    invalid('trust weights must be bounded in [0,1]')
  const ids = new Set<string>(),
    selectors = new Set<string>()
  for (const rule of rules) {
    exact(rule, ['id', 'actor_ref', 'predicate_ref', 'context_ref', 'weight'])
    const selector = JSON.stringify([
      rule.actor_ref,
      rule.predicate_ref ?? null,
      rule.context_ref ?? null,
    ])
    if (
      typeof rule.id !== 'string' ||
      !rule.id ||
      bytes(rule.id) > 128 ||
      ids.has(rule.id) ||
      (!rule.predicate_ref && !rule.context_ref) ||
      selectors.has(selector)
    )
      invalid(
        'contextual trust needs unique IDs/selectors and an explicit context or predicate',
      )
    ids.add(rule.id)
    selectors.add(selector)
  }
}
export function trustWeight(
  rules: ContextualTrustRule[],
  fallback: number,
  actor: string,
  predicate: string,
  contexts: string[],
): number {
  let selected: { specificity: number; weight: number } | null = null,
    ambiguous = false
  for (const rule of rules) {
    if (
      rule.actor_ref !== actor ||
      (rule.predicate_ref != null && rule.predicate_ref !== predicate) ||
      (rule.context_ref != null && !contexts.includes(rule.context_ref))
    )
      continue
    const specificity =
      Number(rule.predicate_ref != null) + Number(rule.context_ref != null)
    if (selected && specificity === selected.specificity)
      ambiguous ||= selected.weight !== rule.weight
    else if (!selected || specificity > selected.specificity) {
      selected = { specificity, weight: rule.weight }
      ambiguous = false
    }
  }
  if (ambiguous)
    invalid('ambiguous contextual trust; use an explicit combined scope')
  return selected?.weight ?? fallback
}
function validateReferences(
  s: Session,
  space: string,
  config: TrustConfiguration,
): void {
  validateTrust(config)
  for (const rule of config.rules ?? []) {
    for (const ref of [
      rule.actor_ref,
      ...(rule.context_ref != null ? [rule.context_ref] : []),
    ]) {
      const id = parseElementId(ref),
        row = s.nexus.store.load(id)
      if (
        id.kind !== 'Concept' ||
        formatElementId(id) !== ref ||
        !row ||
        row.row.space !== space
      )
        throw errors.notFoundOrNotVisible('trust context unavailable')
      permit(s, space, 'read', row)
    }
    if (
      rule.predicate_ref != null &&
      formatSymbolRef(
        s.nexus
          .environment(space)
          .resolveSymbol('PredicateType', rule.predicate_ref, 'read'),
      ) !== rule.predicate_ref
    )
      invalid('trust predicates must use their exact schema references')
  }
}
export function setContextualTrust(
  s: Session,
  space: string,
  expected: number,
  configuration: TrustConfiguration,
): JsonMap {
  validateReferences(s, space, configuration)
  const value = {
    weights: configuration.weights,
    default_weight: configuration.default_weight,
    ...(configuration.rules?.length
      ? {
          rules: configuration.rules.map((r) => ({
            ...r,
            predicate_ref: r.predicate_ref ?? null,
            context_ref: r.context_ref ?? null,
          })),
        }
      : {}),
  }
  const row = publishControl(
    s.nexus.store,
    space,
    'trust',
    'trust',
    expected,
    json(value),
    { principal_id: s.auth.principal_id },
  )
  return { version: row.version, trust: row.value }
}
export function applyTrustCalibration(
  s: Session,
  space: string,
  expected: number,
  proposal: ArtifactPin,
  operationKey: string,
  authorizeWrite: () => void,
): JsonMap {
  if (
    typeof operationKey !== 'string' ||
    !operationKey ||
    bytes(operationKey) > 256 ||
    !safe(expected)
  )
    invalid('bounded trust operation key required')
  const store = s.nexus.store,
    authority = s.effectiveAuthority(space),
    { payload, sources } = authorizedArtifact(
      store,
      space,
      proposal.artifact_ref,
      authority,
      s.auth,
    )
  if (digest(payload) !== proposal.content_digest)
    invalid('proposal digest mismatch')
  exact(payload, [
    'format',
    'space_id',
    'expected_version',
    'configuration',
    'method',
    'evidence_refs',
    'uncertainty',
  ])
  const proposed = payload as unknown as TrustCalibrationProposal
  if (
    proposed.format !== 'nexus:trust-calibration-v1' ||
    proposed.space_id !== space ||
    proposed.expected_version !== expected ||
    !Array.isArray(proposed.evidence_refs) ||
    !proposed.evidence_refs.length ||
    proposed.evidence_refs.length > 128 ||
    !isJsonMap(proposed.uncertainty) ||
    !Object.keys(proposed.uncertainty).length
  )
    invalid('trust calibration needs scope, evidence and explicit uncertainty')
  validateReferences(s, space, proposed.configuration)
  const method = authorizedArtifact(
    store,
    space,
    proposed.method.artifact_ref,
    authority,
    s.auth,
  ).payload
  if (digest(method) !== proposed.method.content_digest)
    invalid('calibration method digest mismatch')
  const evidence = new Set<string>()
  for (const ref of proposed.evidence_refs) {
    if (evidence.has(ref) || !sources.includes(ref))
      invalid(
        'proposal must inherit every independent evidence material reference',
      )
    evidence.add(ref)
    const row = store.load(parseElementId(ref))
    if (
      !row ||
      row.kind !== 'Evidence' ||
      row.row.space !== space ||
      row.row.status === 'corrected' ||
      row.row.state !== 'active'
    )
      invalid('calibration evidence is no longer eligible')
    permit(s, space, 'read', row)
  }
  const key = requestKey(s, {
      operation: 'apply_trust_calibration',
      key: operationKey,
    }),
    hash = digest(json({ expected, proposal })),
    previous = replay(s, space, key, hash)
  if (previous) return previous
  authorizeWrite()
  const tx = transaction(s, space)
  stageControl(tx, 'trust', expected, 'trust', {
    ...proposed.configuration,
    calibration: {
      proposal,
      method: proposed.method,
      evidence_refs: proposed.evidence_refs,
    },
  })
  // Both writes are enclosed by the Session's SQLite transaction.
  store.governance.recordMutation({
    at: tx.cx.at,
    space_id: space,
    principal_id: s.auth.principal_id,
    operation: 'apply_trust_calibration',
    resource: 'trust',
    record: json({
      proposal,
      method: proposed.method,
      before_version: expected,
      after_version: expected + 1,
    }),
  })
  return commit(s, tx, key, hash, {
    version: expected + 1,
    proposal: json(proposal),
    audit_operation: 'apply_trust_calibration',
  })
}
