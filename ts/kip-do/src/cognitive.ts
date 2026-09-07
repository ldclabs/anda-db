/** Portable host contracts, mirrored by anda_kip::cognitive. */
import { errors } from './errors.js'
import type { JsonMap } from './json.js'

export interface ArtifactPin {
  artifact_ref: string
  content_digest: string
}
export interface ObserverControl {
  principal_id: string
  configuration_digest: string
  control_domain: string
}
export interface EvaluationPolicy {
  id: string
  version: string
  allowed_rules: string[]
  allowed_parameters: string[]
  observers: ObserverControl[]
  observer_control_digest: string
  minimum_independent_attempts: number
  allow_same_principal_observer: boolean
  retain_adoption_on_insufficient: boolean
}
export interface EvaluationSamples {
  treatment: Record<string, number[]>
  baseline: Record<string, number[]>
}

export function evaluateBinaryRule(
  rule: JsonMap,
  parameters: JsonMap,
  comparability: JsonMap,
  samples: EvaluationSamples,
): JsonMap {
  if (rule.engine !== 'kip:binary-stratified-v1')
    throw errors.unsupportedCapability('evaluation rule engine unavailable')
  if (!['stratified', 'randomized'].includes(String(comparability.method)))
    throw errors.unsupportedCapability(
      'this rule supports stratified and randomized trials',
    )
  const alpha = Number(parameters.alpha)
  if (!(alpha > 0 && alpha < 1))
    throw errors.constraintViolation('rule alpha must be in (0,1)')
  if (
    comparability.missingness_policy !== 'count_as_failure' ||
    comparability.uncertainty_rule !== 'hoeffding'
  )
    throw errors.constraintViolation(
      'rule requires count_as_failure missingness and hoeffding uncertainty',
    )
  const weights = comparability.strata_weights as Record<string, number>
  if (
    !weights ||
    !Object.keys(weights).length ||
    Math.abs(Object.values(weights).reduce((a, b) => a + b, 0) - 1) > 1e-12
  )
    throw errors.constraintViolation('strata weights must sum to one')
  let effect = 0,
    radius = 0
  for (const [stratum, weight] of Object.entries(weights)) {
    if (!(weight >= 0 && weight <= 1))
      throw errors.constraintViolation('invalid stratum weight')
    if (weight === 0) continue
    const treatment = samples.treatment[stratum] ?? [],
      baseline = samples.baseline[stratum] ?? []
    if (!treatment.length || !baseline.length)
      return {
        status: 'insufficient',
        effect: null,
        uncertainty: { method: 'hoeffding', alpha },
      }
    if (
      [...treatment, ...baseline].some(
        (n) => !Number.isFinite(n) || n < 0 || n > 1,
      )
    )
      throw errors.constraintViolation('invalid attempt aggregate')
    effect +=
      weight *
      (treatment.reduce((a, b) => a + b, 0) / treatment.length -
        baseline.reduce((a, b) => a + b, 0) / baseline.length)
    radius +=
      weight * Math.sqrt(Math.log(2 / alpha) / (2 * treatment.length)) +
      weight * Math.sqrt(Math.log(2 / alpha) / (2 * baseline.length))
  }
  const round = (n: number): number =>
    (Math.sign(n) * Math.round(Math.abs(n) * 1e12)) / 1e12
  const lower = round(effect - radius)
  return {
    status:
      lower >= Number(comparability.minimum_effect ?? 0)
        ? 'improved'
        : 'not_improved',
    effect: round(effect),
    uncertainty: {
      method: 'hoeffding',
      alpha,
      lower_bound: lower,
      radius: round(radius),
    },
  }
}

export interface DispatchRequest {
  attempt_ref: string
  task_ref: string
  fencing_token: number
  supports_idempotency: boolean
  supports_outcome_lookup: boolean
}

/** Deterministic, bounded, side-effect-free host rule over engine-verified material. */
export interface EvaluationInput {
  rule: JsonMap
  parameters: JsonMap
  trial: JsonMap
  attempts: JsonMap
  outcomes: JsonMap
  samples: EvaluationSamples
  minimum_independent_attempts: number
}
export type EvaluationRule = (input: Readonly<EvaluationInput>) => JsonMap
