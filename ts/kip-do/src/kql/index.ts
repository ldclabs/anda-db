/**
 * # Executing KQL
 *
 * `FIND` projects; `WHERE` decides which solutions there are to project. The
 * result is a **bare array**, and its shape follows the projection: one
 * expression gives one value per row, several give an array per row. Never an
 * object — a caller reads by position, which is what keeps a rename of an
 * internal field from being a wire change.
 */

import { detailed, errors } from '../errors.js'
import type { AuthContext, EffectiveAuthority } from '../governance/index.js'
import type { Json, JsonMap } from '../json.js'
import type {
  AsOf,
  AggregationFunction,
  BoundValue,
  FindExpression,
  KqlQuery,
  OrderByItem,
  PathStep,
  Scalar,
} from '../kip/ast.js'
import type { SchemaEnvironment } from '../schema/index.js'
import type { Store } from '../store/index.js'
import { Context } from './context.js'
import {
  coordinateFromToken,
  pageCursorFromToken,
  pageToken,
  type PageCursor,
} from '../store/index.js'
import { normalizeTime } from '../time.js'
import {
  scalarValue,
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
   * The Schema Environment of a past coordinate (§20.9).
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

/** One KQL answer, with the coordinates it was produced under (§50). */
export interface KqlAnswer {
  rows: Json[]
  /** The coordinate this read answered at. */
  snapshotSeq: number
  /** The world-time basis, when `FOR TIME` named one. */
  validAt: string | null
  /** The cursor for the next page, when one remains. */
  nextCursor: string | null
}

/** Runs one KQL query and returns the result array. */
export function executeKql(query: KqlQuery, cx: KqlContext): Json[] {
  return executeKqlPage(query, cx).rows
}

/** Runs one KQL query, reporting its coordinates and page cursor. */
export function executeKqlPage(query: KqlQuery, cx: KqlContext): KqlAnswer {
  const b: ReadBindings = {
    request: cx.request ?? {},
    operation: cx.operation ?? {},
    // Resolved before anything runs: a query that projected half its beliefs
    // under one policy and then failed on the settings would have reported an
    // answer nobody asked for.
    policy: policyFromSettings(epistemicSettings(query.epistemic, cx)),
  }

  // The cursor is read before the coordinate is bound, because it *is* one of
  // the things that decides the coordinate.
  const cursor =
    query.cursor === null ? null : readCursor(query.cursor, b, cx.space)
  const currentSeq = cx.store.currentSeq(cx.space)
  const named = bindCoordinate(query, cx, b)
  if (cursor !== null && named !== null && named !== cursor.snapshotSeq) {
    throw errors.invalidRequestEnvelope(
      `this cursor continues a traversal pinned to snapshot ` +
        `${cursor.snapshotSeq}, and this read names ${named}; one traversal ` +
        `answers at one coordinate`,
    )
  }
  // A continuation is pinned to the coordinate its own first page read at
  // (§44.8). Reconstruction is engaged only when the Space has moved on: at
  // the current coordinate the version log would rebuild exactly what the live
  // tables already hold.
  const asOf =
    named ??
    (cursor !== null && cursor.snapshotSeq < currentSeq ? cursor.snapshotSeq : null)
  const pinnedSeq = cursor?.snapshotSeq ?? named ?? currentSeq
  const env =
    asOf === null ? cx.env : cx.environmentAt(cx.store.schemaVersionAt(cx.space, asOf))
  const context = new Context(cx.store, env, cx.space, cx.authority, cx.auth, asOf)

  // `FOR TIME` names the world time a claim has to apply at, so a projection in
  // the same query answers about that instant rather than about now. A different
  // axis from `AS OF` — what was *true* then, not what this Brain *held* then
  // (§36.1) — and the two never default from each other.
  const validAt = query.for_time === null ? null : time(query.for_time, b)

  const solutions = validAt === null
    ? solveAll(context, query.where_clauses, [new Map()], b)
    : restrictToValidTime(
        context,
        solveAll(context, query.where_clauses, [new Map()], b),
        validAt,
      )
  const expressions = query.find_clause.expressions

  // §44.6: an aggregate anywhere makes this a grouped projection, and the
  // non-aggregated `FIND` expressions are the grouping key. `ORDER BY
  // COUNT(?a)` counts too: it orders groups by an aggregate the caller did not
  // project, and sorting by the bare variable instead would answer a question
  // nobody asked.
  const grouped =
    expressions.some((e) => 'Aggregation' in e) ||
    (query.order_by ?? []).some((item) => item.aggregation !== null)
  if (grouped) {
    const rows = aggregate(context, expressions, solutions, query.order_by)
    const offset = 0
    const limit = query.limit === null ? null : count(query.limit, b, 'LIMIT')
    const capped = capResults(
      limit === null ? rows : rows.slice(offset, offset + limit),
      context.resultLimit(),
    )
    return {
      rows: capped,
      snapshotSeq: pinnedSeq,
      validAt,
      nextCursor:
        limit !== null && capped.length < rows.length
          ? pageToken(cx.space, {
              family: 'kql',
              snapshotSeq: pinnedSeq,
              offset: capped.length,
            })
          : null,
    }
  }

  const ordered = sort(context, solutions, query.order_by, b)
  const paged = page(ordered, query, b, context.resultLimit(), cursor)
  const consumed = paged.offset + paged.rows.length
  return {
    rows: paged.rows.map((solution) => project(context, expressions, solution)),
    snapshotSeq: pinnedSeq,
    validAt,
    // The cursor carries the coordinate this page was read at, so the next one
    // continues over the same canonical snapshot rather than over whatever the
    // Space holds by then (§44.8).
    nextCursor:
      query.limit !== null && consumed < paged.total
        ? pageToken(cx.space, {
            family: 'kql',
            snapshotSeq: pinnedSeq,
            offset: consumed,
          })
        : null,
  }
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

/**
 * Resolves an `AS OF` coordinate to a Space sequence.
 *
 * `AS OF SEQ` is the only historical axis (§48.1). A transaction id resolves
 * to its sequence through `DESCRIBE TRANSACTION`, and a wall-clock instant
 * through `DESCRIBE SNAPSHOT AT TIME` (§68) — the engine never guesses which
 * of several sequences an instant means, and a historical read always names
 * the exact coordinate it was served from.
 */
export function resolveAsOf(asOf: AsOf, _cx: KqlContext, b: ReadBindings): number {
  const value = scalarValue(asOf.Seq, b)
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
    throw errors.typeMismatch('AS OF SEQ takes a non-negative sequence')
  }
  return value
}

function time(scalar: Scalar, b: ReadBindings): string {
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
 * Grouped aggregation (§44.6).
 *
 * Grouping is implicit: the non-aggregated projected expressions are the
 * grouping key, and a `FIND` of aggregates alone is one global group — which is
 * why `COUNT` over an empty result is a row carrying `0` rather than no row at
 * all. The group order is the key's own ascending order unless `ORDER BY` says
 * otherwise, so a paged aggregate reads the same way twice.
 *
 * `ORDER BY` may name an aggregate the caller did not project; it is computed
 * for the sort and dropped from the row.
 *
 * @see rs/anda_cognitive_nexus/src/kql/project.rs — `project_grouped`
 */
function aggregate(
  cx: Context,
  expressions: readonly FindExpression[],
  solutions: readonly Solution[],
  orderBy: readonly OrderByItem[] | null,
): Json[] {
  const keys = expressions.flatMap((e) =>
    'Aggregation' in e ? [] : [e.Variable],
  )

  // One group per distinct key tuple, in first-appearance order until the sort
  // below fixes it.
  const groups: { key: Json[]; rows: Solution[] }[] = []
  const index = new Map<string, { key: Json[]; rows: Solution[] }>()
  for (const solution of solutions) {
    const key = keys.map((path) =>
      readVariable(cx, solution, path.var, path.path),
    )
    const token = JSON.stringify(key)
    const seen = index.get(token)
    if (seen === undefined) {
      const group = { key, rows: [solution] }
      index.set(token, group)
      groups.push(group)
    } else {
      seen.rows.push(solution)
    }
  }
  if (groups.length === 0 && keys.length === 0) {
    groups.push({ key: [], rows: [] })
  }

  // Every aggregate this projection needs: the projected ones, then the ones
  // only `ORDER BY` asks for.
  const plan = expressions.flatMap((e) =>
    'Aggregation' in e ? [e.Aggregation] : [],
  )
  const projectedAggregates = plan.length
  for (const item of orderBy ?? []) {
    if (item.aggregation === null) continue
    const already = plan.some(
      (entry) =>
        entry.func === item.aggregation && samePath(entry.var, item.variable),
    )
    if (!already) {
      plan.push({ func: item.aggregation, var: item.variable, distinct: false })
    }
  }

  const resolved = groups.map((group) => ({
    key: group.key,
    aggregates: plan.map(({ func, var: variable, distinct: isDistinct }) => {
      let read = group.rows.map((solution) =>
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
          const token = JSON.stringify(value)
          if (seen.has(token)) return false
          seen.add(token)
          return true
        })
      }
      return reduce(func, read)
    }),
  }))

  sortGroups(resolved, plan, keys, orderBy)

  return resolved.map(({ key, aggregates }) => {
    // Back into the order the caller wrote them in.
    let keyAt = 0
    let aggregateAt = 0
    const values = expressions.map((expression) =>
      'Aggregation' in expression
        ? (aggregates[aggregateAt++] ?? null)
        : (key[keyAt++] ?? null),
    )
    void projectedAggregates
    return values.length === 1 ? (values[0] as Json) : values
  })
}

/** Whether two projected paths name the same value. */
function samePath(
  left: { var: string; path: readonly PathStep[] },
  right: { var: string; path: readonly PathStep[] },
): boolean {
  return (
    left.var === right.var &&
    JSON.stringify(left.path) === JSON.stringify(right.path)
  )
}

/**
 * Orders groups by the projected columns `ORDER BY` names (§44.7).
 *
 * A key that is neither a projected variable nor a projected aggregate has no
 * value per group — it varies *inside* one — so it is refused rather than
 * resolved to whichever row happened to come first.
 */
function sortGroups(
  groups: { key: Json[]; aggregates: Json[] }[],
  plan: readonly { func: AggregationFunction; var: { var: string; path: readonly PathStep[] } }[],
  keys: readonly { var: string; path: readonly PathStep[] }[],
  orderBy: readonly OrderByItem[] | null,
): void {
  const sortPlan: { index: number; isAggregate: boolean; direction: string }[] = []
  if (orderBy === null || orderBy.length === 0) {
    // The grouping key's own ascending order, so a paged aggregate reads the
    // same way twice.
    keys.forEach((_, index) =>
      sortPlan.push({ index, isAggregate: false, direction: 'Asc' }),
    )
  } else {
    for (const item of orderBy) {
      const position =
        item.aggregation === null
          ? {
              index: keys.findIndex((path) => samePath(path, item.variable)),
              isAggregate: false,
            }
          : {
              index: plan.findIndex(
                (entry) =>
                  entry.func === item.aggregation &&
                  samePath(entry.var, item.variable),
              ),
              isAggregate: true,
            }
      if (position.index < 0) {
        throw errors.constraintViolation(
          `ORDER BY ?${item.variable.var} is not one of the projected ` +
            `columns; grouping makes the projected expressions the only ` +
            `values a group has, so a sort key that varies inside a group has ` +
            `no value to sort by`,
        )
      }
      sortPlan.push({ ...position, direction: item.direction })
    }
  }

  groups.sort((a, b) => {
    for (const { index, isAggregate, direction } of sortPlan) {
      const left = isAggregate ? a.aggregates[index] : a.key[index]
      const right = isAggregate ? b.aggregates[index] : b.key[index]
      const nulls = nullOrder(left as Json, right as Json)
      if (nulls !== null) {
        if (nulls !== 0) return nulls
        continue
      }
      const sign = compareValues(left as Json, right as Json)
      if (sign !== 0) return direction === 'Desc' ? -sign : sign
    }
    return 0
  })
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
 * The cursor is the opaque token this engine issued (§88.4), carrying the
 * coordinate the traversal began at so the next page continues over the same
 * canonical snapshot (§44.8) and the family that produced it so a `HISTORY`
 * cursor cannot resume a `FIND` (§102.28).
 */
function page(
  solutions: readonly Solution[],
  query: KqlQuery,
  b: ReadBindings,
  governedLimit: number | null,
  cursor: PageCursor | null,
): { rows: Solution[]; total: number; offset: number } {
  const offset = cursor?.offset ?? 0
  const requested = query.limit === null ? null : count(query.limit, b, 'LIMIT')
  const limit =
    requested === null
      ? governedLimit
      : governedLimit === null
        ? requested
        : Math.min(requested, governedLimit)
  const from = solutions.slice(offset)
  return {
    rows: limit === null ? from : from.slice(0, limit),
    total: solutions.length,
    offset,
  }
}

function capResults(rows: Json[], governedLimit: number | null): Json[] {
  return governedLimit === null ? rows : rows.slice(0, governedLimit)
}

/** Reads a `CURSOR` slot as the opaque token this engine issues. */
function readCursor(
  cursor: Scalar,
  b: ReadBindings,
  space: string,
): PageCursor {
  const value = scalarValue(cursor, b)
  if (typeof value !== 'string') {
    // Malformed, not a cross-family reuse: §87's `CursorTypeMismatch` is "the
    // cursor is for a different result kind", and a value that is not a token
    // at all is `CursorInvalid` with `details.reason: malformed` — which is
    // what the reference engine answers and what the retry class needs.
    throw detailed.cursorInvalid(
      'kql',
      'malformed',
      `a CURSOR is the opaque token this engine issued, got ` +
        `${JSON.stringify(value)}`,
    )
  }
  return pageCursorFromToken(value, space, 'kql')
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

export { Context, LIMITS } from './context.js'
export { evaluateFilter } from './filter.js'
export {
  parameterValue,
  readVariable,
  scalarValue,
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
