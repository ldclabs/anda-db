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
import {
  compareElementId,
  formatElementId,
  parseElementId,
  type ElementId,
} from '../id.js'
import type { JsonMap } from '../json.js'
import type { Permission } from '../governance/index.js'
import type { ElementRef, Scalar, WhereClause } from '../kip/ast.js'
import { Context } from '../kql/context.js'
import { solveAll } from '../kql/matching.js'
import { baseline } from '../projection/policy.js'
import type { Transaction } from '../tx.js'
import { handleId, parameter, scalar, type Bindings } from './value.js'

/**
 * What a clause's target names, once resolved.
 *
 * The ids are private and reachable only through {@link Targets.authorized}, so
 * a clause cannot act on a selected element without having authorized it. A new
 * clause that forgets is a compile error rather than an ungoverned sweep.
 *
 * **A sweep that reaches something it may not touch fails.** It does not quietly
 * do less: an operation that reports success having skipped half its targets is
 * the defect shape this project keeps finding, and here it would also be a
 * disclosure — the caller could learn which elements exist outside its Grant by
 * counting what a sweep changed.
 */
export class Targets {
  /** Whether the target was named directly rather than selected. */
  readonly direct: boolean
  readonly #ids: readonly ElementId[]
  readonly #permission: Permission

  constructor(ids: readonly ElementId[], direct: boolean, permission: Permission) {
    this.#ids = ids
    this.direct = direct
    this.#permission = permission
  }

  /** How many elements were selected, without handing any of them over. */
  get size(): number {
    return this.#ids.length
  }

  /** The elements, once every one of them has been authorized. */
  authorized(tx: Transaction): ElementId[] {
    for (const id of this.#ids) tx.authorizeElement(id, this.#permission)
    return [...this.#ids]
  }
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
  permission: Permission,
): Targets {
  const direct = (id: ElementId) => new Targets([id], true, permission)
  if (where === null) {
    if ('Handle' in target) return direct(parseElementId(handleId(b, target.Handle)))
    if ('Id' in target) return direct(parseElementId(target.Id))
    const value = parameter(b, target.Param)
    if (typeof value !== 'string') {
      throw errors.typeMismatch(
        `${what} needs an element id, got ${JSON.stringify(value)}`,
      )
    }
    return direct(parseElementId(value))
  }

  if (!('Handle' in target)) {
    // A WHERE block binds a variable. An id target with a block would name one
    // element *and* describe a set, and there is no reading of that which is
    // not a contradiction.
    throw errors.invalidSyntax(
      `${what} names its target directly, so it takes no WHERE block`,
    )
  }

  const cx = new Context(tx.store, tx.env, tx.cx.space, tx.authority, tx.auth)
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
  return new Targets(cap === null ? ids : ids.slice(0, cap), false, permission)
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
