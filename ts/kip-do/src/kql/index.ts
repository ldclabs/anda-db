/**
 * # Executing KQL
 *
 * `FIND` projects; `WHERE` decides which solutions there are to project. The
 * result is a **bare array**, and its shape follows the projection: one
 * expression gives one value per row, several give an array per row. Never an
 * object — a caller reads by position, which is what keeps a rename of an
 * internal field from being a wire change.
 */

import { errors } from '../errors.js'
import type { Json, JsonMap } from '../json.js'
import type {
  AggregationFunction,
  BoundValue,
  FindExpression,
  KqlQuery,
  OrderByItem,
  Scalar,
} from '../kip/ast.js'
import type { SchemaEnvironment } from '../schema/index.js'
import type { Store } from '../store/index.js'
import { Context } from './context.js'
import {
  kipLiteral,
  parameterValue,
  readVariable,
  solveAll,
  type ReadBindings,
} from './matching.js'
import { compareSolutions, type Solution } from './solution.js'
import { policyFromSettings } from '../projection/index.js'
import { boundValue } from '../kml/value.js'

/** What one KQL execution needs from its caller. */
export interface KqlContext {
  store: Store
  space: string
  env: SchemaEnvironment
  request?: JsonMap
  operation?: JsonMap
}

/** Runs one KQL query and returns the result array. */
export function executeKql(query: KqlQuery, cx: KqlContext): Json[] {
  if (query.as_of !== null) {
    throw errors.unsupportedCapability(
      'AS OF is not implemented by this engine yet; see DESCRIBE CAPABILITIES',
    )
  }
  if (query.for_time !== null) {
    throw errors.unsupportedCapability(
      'FOR TIME is not implemented by this engine yet; see DESCRIBE CAPABILITIES',
    )
  }
  const context = new Context(cx.store, cx.env, cx.space)
  const b: ReadBindings = {
    request: cx.request ?? {},
    operation: cx.operation ?? {},
    // Resolved before anything runs: a query that projected half its beliefs
    // under one policy and then failed on the settings would have reported an
    // answer nobody asked for.
    policy: policyFromSettings(epistemicSettings(query.epistemic, cx)),
  }

  const solutions = solveAll(context, query.where_clauses, [new Map()], b)
  const expressions = query.find_clause.expressions

  if (expressions.some((e) => 'Aggregation' in e)) {
    return aggregate(context, expressions, solutions, b)
  }

  const ordered = sort(context, solutions, query.order_by, b)
  const rows = page(ordered, query, b)
  return rows.map((solution) => project(context, expressions, solution))
}

/**
 * The `WITH EPISTEMIC { … }` block, with its parameters filled in.
 *
 * Evaluated through the same bound-value path a mutation uses, so a policy
 * named by `:parameter` means the same thing on both sides of the language.
 */
function epistemicSettings(
  epistemic: Record<string, BoundValue> | null,
  cx: KqlContext,
): JsonMap {
  if (epistemic === null) return {}
  const b = {
    tx: null as never,
    request: cx.request ?? {},
    operation: cx.operation ?? {},
  }
  return Object.fromEntries(
    Object.entries(epistemic).map(([key, value]) => [key, boundValue(b, value)]),
  )
}

/** One row of the result: a scalar for one expression, an array for several. */
function project(
  cx: Context,
  expressions: readonly FindExpression[],
  solution: Solution,
): Json {
  const values = expressions.map((expression) => {
    if ('Aggregation' in expression) return null
    return readVariable(
      cx,
      solution,
      expression.Variable.var,
      expression.Variable.path,
    )
  })
  return values.length === 1 ? (values[0] as Json) : values
}

/**
 * Aggregation over the whole solution set.
 *
 * Grouped aggregation — a plain variable projected beside an aggregate — is not
 * built yet and is refused rather than silently answered as a global one, which
 * would return a single row where the caller asked for one per group.
 */
function aggregate(
  cx: Context,
  expressions: readonly FindExpression[],
  solutions: readonly Solution[],
  _b: ReadBindings,
): Json[] {
  if (!expressions.every((e) => 'Aggregation' in e)) {
    throw errors.unsupportedCapability(
      'grouped aggregation — a plain variable projected beside an aggregate — ' +
        'is not implemented by this engine yet; see DESCRIBE CAPABILITIES',
    )
  }

  const values = expressions.map((expression) => {
    const { func, var: variable, distinct: isDistinct } = (
      expression as Extract<FindExpression, { Aggregation: unknown }>
    ).Aggregation
    let read = solutions.map((solution) =>
      readVariable(cx, solution, variable.var, variable.path),
    )
    if (func !== 'Count') {
      // Only COUNT is defined over an unbound variable: the others need a
      // value, and a row that has none contributes nothing rather than zero.
      read = read.filter((value) => value !== null)
    }
    if (isDistinct) {
      const seen = new Set<string>()
      read = read.filter((value) => {
        const key = JSON.stringify(value)
        if (seen.has(key)) return false
        seen.add(key)
        return true
      })
    }
    return reduce(func, read)
  })

  return values.length === 1 ? [values[0] as Json] : [values]
}

function reduce(func: AggregationFunction, values: readonly Json[]): Json {
  switch (func) {
    case 'Count':
      // COUNT over nothing is zero, and zero is not a falsehood: it is the
      // honest answer to "how many", not a claim that nothing exists.
      return values.filter((value) => value !== null).length
    case 'Sum':
    case 'Avg': {
      const numbers = values.filter(
        (value): value is number => typeof value === 'number',
      )
      if (func === 'Sum') return numbers.reduce((a, b) => a + b, 0)
      return numbers.length === 0
        ? null
        : numbers.reduce((a, b) => a + b, 0) / numbers.length
    }
    case 'Min':
    case 'Max': {
      if (values.length === 0) return null
      const sorted = [...values].sort(compareValues)
      return (func === 'Min' ? sorted[0] : sorted[sorted.length - 1]) ?? null
    }
  }
}

/**
 * Orders solutions, with nulls last whichever direction was asked for.
 *
 * A null is an absent value, not a small one: sorting it to the front under
 * `ASC` would put the rows that answered nothing above the rows that answered.
 */
function sort(
  cx: Context,
  solutions: readonly Solution[],
  orderBy: readonly OrderByItem[] | null,
  _b: ReadBindings,
): Solution[] {
  const out = [...solutions]
  if (orderBy === null || orderBy.length === 0) {
    // Documented rather than incidental: a bounded read has to be repeatable,
    // so the fallback order is the same total order the mutation sweeps use.
    return out.sort(compareSolutions)
  }
  for (const item of orderBy) {
    if (item.aggregation !== null) {
      throw errors.unsupportedCapability(
        'ORDER BY over an aggregate is not implemented by this engine yet',
      )
    }
  }
  return out.sort((a, b) => {
    for (const item of orderBy) {
      const left = readVariable(cx, a, item.variable.var, item.variable.path)
      const right = readVariable(cx, b, item.variable.var, item.variable.path)
      const nulls = nullOrder(left, right)
      if (nulls !== null) {
        if (nulls !== 0) return nulls
        continue
      }
      const sign = compareValues(left, right)
      if (sign !== 0) return item.direction === 'Desc' ? -sign : sign
    }
    return compareSolutions(a, b)
  })
}

/** `null` when neither side is null, otherwise the order between them. */
function nullOrder(left: Json, right: Json): number | null {
  const leftNull = left === null || left === undefined
  const rightNull = right === null || right === undefined
  if (!leftNull && !rightNull) return null
  if (leftNull && rightNull) return 0
  return leftNull ? 1 : -1
}

/** A total order over comparable values; unlike types fall back to their text. */
function compareValues(left: Json, right: Json): number {
  if (typeof left === 'number' && typeof right === 'number') {
    return left === right ? 0 : left < right ? -1 : 1
  }
  if (typeof left === 'string' && typeof right === 'string') {
    return left === right ? 0 : left < right ? -1 : 1
  }
  if (typeof left === 'boolean' && typeof right === 'boolean') {
    return left === right ? 0 : left ? 1 : -1
  }
  const a = JSON.stringify(left)
  const b = JSON.stringify(right)
  return a === b ? 0 : a < b ? -1 : 1
}

/**
 * Applies `LIMIT` and `CURSOR`.
 *
 * The cursor is a numeric offset over the documented order. It is deliberately
 * *not* interchangeable with the Rust engine's, which uses element-anchored
 * keyset tokens for some projection shapes — a token from one engine handed to
 * the other would page through a different sequence while looking valid.
 */
function page(
  solutions: readonly Solution[],
  query: KqlQuery,
  b: ReadBindings,
): Solution[] {
  const offset = query.cursor === null ? 0 : cursorOffset(query.cursor, b)
  const limit = query.limit === null ? null : count(query.limit, b, 'LIMIT')
  const from = solutions.slice(offset)
  return limit === null ? from : from.slice(0, limit)
}

function cursorOffset(cursor: Scalar, b: ReadBindings): number {
  const value = scalarValue(cursor, b)
  const offset = typeof value === 'string' ? Number(value) : value
  if (typeof offset !== 'number' || !Number.isInteger(offset) || offset < 0) {
    throw errors.cursorTypeMismatch(
      `a CURSOR from this engine is a non-negative offset, got ` +
        `${JSON.stringify(value)}`,
    )
  }
  return offset
}

function count(scalar: Scalar, b: ReadBindings, what: string): number {
  const value = scalarValue(scalar, b)
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
    throw errors.typeMismatch(
      `${what} must be a non-negative integer, got ${JSON.stringify(value)}`,
    )
  }
  return value
}

function scalarValue(scalar: Scalar, b: ReadBindings): Json {
  return 'Param' in scalar
    ? parameterValue(b, scalar.Param)
    : kipLiteral(scalar.Literal)
}

export { Context, LIMITS } from './context.js'
export { evaluateFilter } from './filter.js'
export {
  parameterValue,
  readVariable,
  solveAll,
  type ReadBindings,
} from './matching.js'
export {
  bindingValue,
  compareSolutions,
  distinct,
  elementBinding,
  literalBinding,
  symbolBinding,
  type Binding,
  type Solution,
} from './solution.js'
