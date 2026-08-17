/**
 * Bindings and solution sets.
 *
 * A solution is one assignment of variables to values; a query's answer is the
 * set of them that every clause in the `WHERE` block agrees on. Clauses sharing
 * a variable therefore *join*, which is the whole of KQL's evaluation model.
 *
 * The one decision worth stating: a binding keeps **what kind of thing** it is,
 * not just its value. An element bound as the string `"C-1"` and a Literal
 * whose text happens to be `"C-1"` would compare equal, `IS_ELEMENT` would lie
 * about both, and `?a ASSERTION {proposition: ?p}` would never join with
 * `?p PROPOSITION (…)` — the reference would be a string and the pattern binds
 * an element. Keeping the kind is what stops all three.
 */

import {
  compareElementId,
  formatElementId,
  type ElementId,
} from '../id.js'
import { canonicalJson, type Json } from '../json.js'

/** One value a variable may be bound to. */
export type Binding =
  /** A Cognitive Element, by identity. */
  | { kind: 'element'; id: ElementId }
  /** A Core Literal value. */
  | { kind: 'literal'; value: Json }
  /** An exact schema symbol, e.g. a predicate reference. */
  | { kind: 'symbol'; value: string }

export const elementBinding = (id: ElementId): Binding => ({
  kind: 'element',
  id,
})
export const literalBinding = (value: Json): Binding => ({
  kind: 'literal',
  value,
})
export const symbolBinding = (value: string): Binding => ({
  kind: 'symbol',
  value,
})

/** The deterministic key two bindings share exactly when they are equal. */
export function bindingKey(binding: Binding): string {
  switch (binding.kind) {
    case 'element':
      return `e:${formatElementId(binding.id)}`
    case 'symbol':
      return `s:${binding.value}`
    case 'literal':
      return `l:${canonicalJson(binding.value)}`
  }
}

export const bindingsEqual = (a: Binding, b: Binding): boolean =>
  bindingKey(a) === bindingKey(b)

/** What a projection or a filter sees when it reads a bound variable bare. */
export function bindingValue(binding: Binding): Json {
  switch (binding.kind) {
    case 'element':
      return formatElementId(binding.id)
    case 'symbol':
      return binding.value
    case 'literal':
      return binding.value
  }
}

/** One assignment of variables to values. */
export type Solution = ReadonlyMap<string, Binding>

/** A mutable solution under construction. */
export type MutableSolution = Map<string, Binding>

export const emptySolution = (): MutableSolution => new Map()

/**
 * Extends a solution, or returns `null` when it disagrees.
 *
 * The disagreement is the join: a clause that binds `?c` to something the
 * incoming solution already bound differently does not produce a second row, it
 * produces no row.
 */
export function extend(
  solution: Solution,
  name: string,
  binding: Binding,
): MutableSolution | null {
  const existing = solution.get(name)
  if (existing !== undefined && !bindingsEqual(existing, binding)) return null
  const next = new Map(solution)
  next.set(name, binding)
  return next
}

/** Extends with several bindings at once, all or nothing. */
export function extendAll(
  solution: Solution,
  bindings: readonly [string, Binding][],
): MutableSolution | null {
  let next: MutableSolution = new Map(solution)
  for (const [name, binding] of bindings) {
    const extended = extend(next, name, binding)
    if (extended === null) return null
    next = extended
  }
  return next
}

/** The identity of a whole solution, for de-duplication after a UNION. */
export function solutionKey(solution: Solution): string {
  return [...solution.keys()]
    .sort()
    .map((name) => `${name}=${bindingKey(solution.get(name) as Binding)}`)
    .join('')
}

/** Removes solutions that bind every variable the same way. */
export function distinct(solutions: readonly Solution[]): Solution[] {
  const seen = new Set<string>()
  const out: Solution[] = []
  for (const solution of solutions) {
    const key = solutionKey(solution)
    if (seen.has(key)) continue
    seen.add(key)
    out.push(solution)
  }
  return out
}

/**
 * A stable total order over solutions, for a repeatable `LIMIT`.
 *
 * §52.7 permits a runtime to document an order, and documenting one is what
 * makes a bounded read repeatable rather than "whatever came back". Ascending
 * element id, then the remaining bindings by name — which is the same rule the
 * mutation sweeps use, so a read and a write over the same set agree about
 * which elements the cap kept.
 */
export function compareSolutions(a: Solution, b: Solution): number {
  const names = [...new Set([...a.keys(), ...b.keys()])].sort()
  for (const name of names) {
    const left = a.get(name)
    const right = b.get(name)
    if (left === undefined) return right === undefined ? 0 : 1
    if (right === undefined) return -1
    if (left.kind === 'element' && right.kind === 'element') {
      const order = compareElementId(left.id, right.id)
      if (order !== 0) return order
      continue
    }
    const lk = bindingKey(left)
    const rk = bindingKey(right)
    if (lk !== rk) return lk < rk ? -1 : 1
  }
  return 0
}
