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
import type { AuthContext, EffectiveAuthority } from '../governance/index.js'
import type { Json, JsonMap } from '../json.js'
import type {
  AsOf,
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
import { coordinateFromToken } from '../store/index.js'
import { normalizeTime } from '../time.js'
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
  /**
   * What the caller may see here, resolved once for the whole read.
   *
   * Required rather than optional. A default would have to be either "everything"
   * or "nothing", and both are wrong in a way that is invisible: the first turns
   * a forgotten argument into an unauthorized read, and the second turns it into
   * an empty answer that reads as an empty world.
   */
  authority: EffectiveAuthority
  /** Who the caller is. */
  auth: AuthContext
  /**
   * The `read.snapshot_token` the request envelope carried, if it carried one.
   *
   * A second way to name the same coordinate, and the two may not disagree —
   * see {@link bindCoordinate}.
   */
  snapshotToken?: string
  /**
   * The Schema Environment of a past coordinate (§144).
   *
   * A historical read resolves symbols through the environment that was in
   * force *then*, never today's: reconstructing the past under today's schema
   * answers a question nobody asked, and does it silently — a symbol that
   * resolves differently now returns different elements rather than an error.
   *
   * Supplied as a resolver because only the Nexus can build an environment: it
   * owns the installed package artifacts a lock resolves against. Required
   * rather than defaulted to `env`, because that default would be the silent
   * wrong answer above and nothing would report it.
   */
  environmentAt: (version: number) => SchemaEnvironment
}

/** Runs one KQL query and returns the result array. */
export function executeKql(query: KqlQuery, cx: KqlContext): Json[] {
  const b: ReadBindings = {
    request: cx.request ?? {},
    operation: cx.operation ?? {},
    // Resolved before anything runs: a query that projected half its beliefs
    // under one policy and then failed on the settings would have reported an
    // answer nobody asked for.
    policy: policyFromSettings(epistemicSettings(query.epistemic, cx)),
  }

  const asOf = bindCoordinate(query, cx, b)
  const env =
    asOf === null ? cx.env : cx.environmentAt(cx.store.schemaVersionAt(cx.space, asOf))
  const context = new Context(cx.store, env, cx.space, cx.authority, cx.auth, asOf)

  // `FOR TIME` names the world time a claim has to apply at, so a projection in
  // the same query answers about that instant rather than about now. A different
  // axis from `AS OF` — what was *true* then, not what this Brain *held* then
  // (§36.1) — and the two never default from each other.
  const validAt = query.for_time === null ? null : time(cx, query.for_time, b)

  const solutions = validAt === null
    ? solveAll(context, query.where_clauses, [new Map()], b)
    : restrictToValidTime(
        context,
        solveAll(context, query.where_clauses, [new Map()], b),
        validAt,
      )
  const expressions = query.find_clause.expressions

  if (expressions.some((e) => 'Aggregation' in e)) {
    return capResults(
      aggregate(context, expressions, solutions, b),
      context.resultLimit(),
    )
  }

  const ordered = sort(context, solutions, query.order_by, b)
  const rows = page(ordered, query, b, context.resultLimit())
  return rows.map((solution) => project(context, expressions, solution))
}

/**
 * Resolves the one coordinate this read answers at.
 *
 * `AS OF` names one and the request envelope may carry a snapshot token. Both
 * resolve to a Space sequence, and they may not disagree: a request pinned to
 * one coordinate whose command named another would leave the answer's own
 * `snapshot_seq` unable to say which it meant.
 *
 * A coordinate the Space has not reached is refused rather than rounded to the
 * present. Rounding would answer a different question and say nothing about
 * having done so, which is the worst available behaviour for a read whose whole
 * point is *when*.
 */
export function bindCoordinate(
  query: { as_of: AsOf | null },
  cx: KqlContext,
  b: ReadBindings,
): number | null {
  const fromToken =
    cx.snapshotToken === undefined
      ? null
      : coordinateFromToken(cx.snapshotToken, cx.space).seq
  const fromCommand = query.as_of === null ? null : resolveAsOf(query.as_of, cx, b)
  if (fromToken !== null && fromCommand !== null && fromToken !== fromCommand) {
    throw errors.invalidRequestEnvelope(
      `this request is bound to snapshot ${fromToken} and its command reads ` +
        `AS OF ${fromCommand}; one read answers at one coordinate`,
    )
  }
  const seq = fromCommand ?? fromToken
  if (seq === null) return null
  const current = cx.store.currentSeq(cx.space)
  if (seq > current) {
    throw errors.historicalSnapshotUnavailable(
      `this Space has reached sequence ${current}, so ${seq} names no ` +
        `coordinate it can answer at; a future coordinate is refused rather ` +
        `than rounded to the present`,
    )
  }
  return seq
}

/** Resolves an `AS OF` coordinate to a Space sequence. */
export function resolveAsOf(asOf: AsOf, cx: KqlContext, b: ReadBindings): number {
  if ('Seq' in asOf) {
    const value = scalarValue(asOf.Seq, b)
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
      throw errors.typeMismatch('AS OF SEQ takes a non-negative sequence')
    }
    return value
  }
  if ('Tx' in asOf) {
    const value = scalarValue(asOf.Tx, b)
    if (typeof value !== 'string') {
      throw errors.typeMismatch('AS OF TX takes a transaction id')
    }
    return cx.store.seqOfTransaction(cx.space, value)
  }
  const value = scalarValue(asOf.Time, b)
  if (typeof value !== 'string') {
    throw errors.typeMismatch('AS OF TIME takes an RFC 3339 timestamp')
  }
  return cx.store.seqAtTime(cx.space, normalizeTime(value, 'AS OF TIME'))
}

function time(cx: KqlContext, scalar: Scalar, b: ReadBindings): string {
  void cx
  const value = scalarValue(scalar, b)
  if (typeof value !== 'string') {
    throw errors.typeMismatch('FOR TIME takes an RFC 3339 timestamp')
  }
  return normalizeTime(value, 'FOR TIME')
}

/**
 * Keeps only the solutions whose Assertions applied at a world time.
 *
 * `FOR TIME` filters on `valid_time`, the axis that says when a claim *applies*
 * — never `asserted_at`, which says when somebody said it, and never the engine
 * sequence, which says when this Brain recorded it (§36). A solution binding no
 * Assertion is untouched: the clause narrows claims, and a Concept has no
 * validity interval to be outside of.
 */
function restrictToValidTime(
  cx: Context,
  solutions: readonly Solution[],
  at: string,
): Solution[] {
  return solutions.filter((solution) =>
    [...solution.values()].every((binding) => {
      if (binding.kind !== 'element' || binding.id.kind !== 'Assertion') return true
      const view = cx.view(binding.id)
      if (view === null) return true
      const validTime = view.valid_time
      const from = readInterval(validTime, 'from')
      const until = readInterval(validTime, 'until')
      return (from === '' || from <= at) && (until === '' || at < until)
    }),
  )
}

function readInterval(value: unknown, member: 'from' | 'until'): string {
  if (value === null || typeof value !== 'object') return ''
  const found = (value as Record<string, unknown>)[member]
  return typeof found === 'string' ? found : ''
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
  governedLimit: number | null,
): Solution[] {
  const offset = query.cursor === null ? 0 : cursorOffset(query.cursor, b)
  const requested = query.limit === null ? null : count(query.limit, b, 'LIMIT')
  const limit =
    requested === null
      ? governedLimit
      : governedLimit === null
        ? requested
        : Math.min(requested, governedLimit)
  const from = solutions.slice(offset)
  return limit === null ? from : from.slice(0, limit)
}

function capResults(rows: Json[], governedLimit: number | null): Json[] {
  return governedLimit === null ? rows : rows.slice(0, governedLimit)
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
