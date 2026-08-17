/**
 * The Epistemic Policy a projection runs under.
 *
 * A policy is named and versioned, and every projection reports which one it
 * used (Spec §54): "accepted" with no policy attached is not an auditable
 * statement, because the next reader cannot tell what "accepted" meant.
 *
 * Overriding a threshold therefore produces a policy that is no longer the
 * baseline, and its identity changes with it. An answer reporting
 * `kip:policy:baseline` while running on different numbers would be a false
 * audit trail — worse than no trail, because it looks like one.
 */

import { errors } from '../errors.js'
import type { Json, JsonMap } from '../json.js'

export const BASELINE_ID = 'kip:policy:baseline'

/**
 * The baseline's version.
 *
 * Bumped whenever its numbers move, because that changes what a past
 * "accepted" would have meant.
 */
export const BASELINE_VERSION = 1

export interface Policy {
  id: string
  version: number
  /** The Assertion modes this policy admits as answers. */
  modes: string[]
  /** Support at or above which a claim is accepted. */
  accept: number
  /** Support below which a side is not material to the outcome. */
  material: number
  /** What an Assertion that stated no confidence contributes. */
  unstated_confidence: number
  /** Whether support for a rival value of a functional slot opposes this one. */
  expand_conflicts: boolean
}

/**
 * Ordinary factual recall: what somebody observed, said, inferred or imported.
 *
 * `hypothetical` is entertained without commitment and `predicted` is about the
 * future, so neither is an answer to "what is the case" (§38, §39).
 */
export function baseline(): Policy {
  return {
    id: BASELINE_ID,
    version: BASELINE_VERSION,
    modes: ['observed', 'stated', 'inferred', 'imported'],
    accept: 0.7,
    material: 0.3,
    unstated_confidence: 0.5,
    expand_conflicts: true,
  }
}

/**
 * Predictions instead of observations.
 *
 * A separate policy rather than a flag, because "what is the case" and "what is
 * expected" are different questions and an answer must say which one it
 * answered.
 */
export function forecast(): Policy {
  return { ...baseline(), id: 'kip:policy:forecast', modes: ['predicted', 'inferred'] }
}

/** Reads a `WITH EPISTEMIC { … }` block. */
export function policyFromSettings(settings: JsonMap): Policy {
  const named = settings.policy
  let policy: Policy
  if (named === undefined || named === null) {
    policy = baseline()
  } else if (typeof named !== 'string') {
    throw errors.typeMismatch('`policy` must be a policy identifier')
  } else if (named === BASELINE_ID || named === 'baseline') {
    policy = baseline()
  } else if (named === 'forecast' || named === 'kip:policy:forecast') {
    policy = forecast()
  } else {
    // Naming the policy rather than defaulting to the baseline: a caller that
    // asked for a stricter reading and silently got the ordinary one would act
    // on an answer it did not request.
    throw errors.projectionPolicyUnavailable(
      `no Epistemic Policy named ${JSON.stringify(named)} is available here; ` +
        `LIST EPISTEMIC POLICIES shows the ones that are`,
    )
  }

  let overridden = false
  for (const key of ['accept', 'material'] as const) {
    const value = settings[key]
    if (value === undefined || value === null) continue
    if (typeof value !== 'number' || value < 0 || value > 1) {
      throw errors.typeMismatch(`\`${key}\` must be a number in [0, 1]`)
    }
    policy[key] = value
    overridden = true
  }
  if (policy.accept < policy.material) {
    throw errors.constraintViolation(
      'an acceptance threshold below the materiality threshold would accept ' +
        'claims it had already called immaterial',
    )
  }

  const modes = settings.modes
  if (modes !== undefined && modes !== null) {
    if (!Array.isArray(modes) || modes.some((m) => typeof m !== 'string')) {
      throw errors.typeMismatch('`modes` must be a list of Assertion modes')
    }
    policy.modes = modes as string[]
    overridden = true
  }

  if (overridden) {
    policy.id = `${policy.id}+custom`
  }
  return policy
}

/** Whether a policy admits an Assertion's mode as an answer. */
export const admits = (policy: Policy, mode: string): boolean =>
  policy.modes.includes(mode)

/** Why a mode was excluded, in words a caller can act on. */
export function modeExclusion(mode: string): string {
  switch (mode) {
    case 'hypothetical':
      return 'hypothetical_not_requested'
    case 'predicted':
      return 'prediction_not_requested'
    case '':
      return 'invalid_schema'
    default:
      return 'policy_excluded'
  }
}

/** The policy identity a projection reports. */
export const policyIdentity = (policy: Policy): Json =>
  ({ id: policy.id, version: policy.version }) as Json
