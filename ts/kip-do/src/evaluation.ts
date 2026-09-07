/** Host code is bound to exact rule bytes; uploaded artifacts never execute code. */
import { errors } from './errors.js'
import { digest } from './schema/contracts.js'
import {
  evaluateBinaryRule,
  type EvaluationInput,
  type EvaluationRule,
} from './cognitive.js'
import type { Json, JsonMap } from './json.js'

export class EvaluationRules {
  private readonly rules = new Map<string, EvaluationRule>()
  constructor() {
    this.register({ engine: 'kip:binary-stratified-v1' }, (input) => {
      const count = Object.values(input.samples.treatment).reduce(
        (n, items) => n + items.length,
        0,
      )
      if (
        count <
        Math.max(Number(input.trial.quota), input.minimum_independent_attempts)
      )
        return {
          status: 'insufficient',
          effect: null,
          uncertainty: { method: 'hoeffding', alpha: input.parameters.alpha! },
        }
      return evaluateBinaryRule(
        input.rule,
        input.parameters,
        input.trial.comparability as JsonMap,
        input.samples,
      )
    })
  }
  register(artifact: Json, evaluator: EvaluationRule): string {
    const key = digest(artifact)
    if (this.rules.has(key))
      throw errors.constraintViolation(
        'registered rule digest cannot be rebound; publish new rule artifact',
      )
    this.rules.set(key, evaluator)
    return key
  }
  supports(digest: string): boolean {
    return this.rules.has(digest)
  }
  evaluate(input: EvaluationInput): JsonMap {
    const evaluator = this.rules.get(digest(input.rule))
    if (!evaluator)
      throw errors.unsupportedCapability(
        'no trusted host evaluator is registered for pinned rule digest',
      )
    const comparison = evaluator(structuredClone(input))
    digest(comparison)
    return comparison
  }
}
