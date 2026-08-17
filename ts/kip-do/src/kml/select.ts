/**
 * Selection blocks: the `WHERE` a mutation may carry.
 *
 * Three properties make this a contract rather than a convenience, and all
 * three are pinned by `fixtures/kip-conformance-2.0/mutation-selection.json`:
 *
 * **A selection block reads the state the transaction started from** (§24).
 * Clause order carries no mutation semantics, so a sweep must not see what an
 * earlier clause of the same `MUTATE` created — if it could, order would carry
 * semantics after all. This holds by construction here: an element this
 * transaction minted is `pending`, and no pattern matches a pending row.
 *
 * **`LIMIT` cuts in ascending element id.** §52.7 permits a runtime to document
 * an order, and documenting one is what makes a bounded sweep repeatable: run
 * the same capped sweep twice and it takes the same elements, rather than
 * whichever ones the storage layer felt like returning.
 *
 * **Matching nothing changes nothing.** An `UPDATE` never creates, so a block
 * that selects no elements is a `no_effect` receipt and not an error — the
 * caller asked about a set that turned out to be empty, which is an answer.
 */

import { errors } from '../errors.js'
import { compareElementId, formatElementId, type ElementId } from '../id.js'
import type { JsonMap } from '../json.js'
import type { ElementRef, Scalar, WhereClause } from '../kip/ast.js'
import { Context } from '../kql/context.js'
import { solveAll } from '../kql/matching.js'
import { baseline } from '../projection/policy.js'
import type { Transaction } from '../tx.js'
import { handleId, parameter, scalar, type Bindings } from './value.js'

/** What a clause's target names, once resolved. */
export interface Targets {
  /** The element ids, in the documented order. */
  ids: ElementId[]
  /** Whether the target was named directly rather than selected. */
  direct: boolean
}

/**
 * Resolves a mutation's target, whether it names one element or selects a set.
 *
 * A `?variable` target is bound by the `WHERE`; an `:id` or `"id"` target names
 * the element already and needs no block (§58).
 */
export function resolveTargets(
  tx: Transaction,
  b: Bindings,
  target: ElementRef,
  where: readonly WhereClause[] | null,
  limit: Scalar | null,
  request: JsonMap | undefined,
  operation: JsonMap | undefined,
  what: string,
): Targets {
  if (where === null) {
    if ('Handle' in target) {
      return { ids: [parse(handleId(b, target.Handle))], direct: true }
    }
    if ('Id' in target) return { ids: [parse(target.Id)], direct: true }
    const value = parameter(b, target.Param)
    if (typeof value !== 'string') {
      throw errors.typeMismatch(
        `${what} needs an element id, got ${JSON.stringify(value)}`,
      )
    }
    return { ids: [parse(value)], direct: true }
  }

  if (!('Handle' in target)) {
    // A WHERE block binds a variable. An id target with a block would name one
    // element *and* describe a set, and there is no reading of that which is
    // not a contradiction.
    throw errors.invalidSyntax(
      `${what} names its target directly, so it takes no WHERE block`,
    )
  }

  const cx = new Context(tx.store, tx.env, tx.cx.space)
  const solutions = solveAll(
    cx,
    where,
    [new Map()],
    { request: request ?? {}, operation: operation ?? {}, policy: baseline() },
  )

  const seen = new Map<string, ElementId>()
  for (const solution of solutions) {
    const bound = solution.get(target.Handle)
    if (bound?.kind !== 'element') continue
    seen.set(formatElementId(bound.id), bound.id)
  }

  // The documented order, applied before the cap: `LIMIT` says how many are
  // affected, not which, unless the runtime says which — and this one does.
  const ids = [...seen.values()].sort(compareElementId)
  const cap = limit === null ? null : count(b, limit, `${what} LIMIT`)
  return { ids: cap === null ? ids : ids.slice(0, cap), direct: false }
}

function parse(text: string): ElementId {
  const hyphen = text.indexOf('-')
  const kind = {
    C: 'Concept',
    P: 'Proposition',
    A: 'Assertion',
    E: 'Evidence',
    X: 'Activity',
  }[text.charAt(0)]
  if (kind === undefined || hyphen !== 1) {
    throw errors.invalidIdentifier(`${JSON.stringify(text)} is not an element id`)
  }
  return { kind: kind as ElementId['kind'], seq: Number(text.slice(2)) }
}

function count(b: Bindings, value: Scalar, what: string): number {
  const resolved = scalar(b, value)
  if (typeof resolved !== 'number' || !Number.isInteger(resolved) || resolved < 0) {
    throw errors.typeMismatch(
      `${what} must be a non-negative integer, got ${JSON.stringify(resolved)}`,
    )
  }
  return resolved
}
