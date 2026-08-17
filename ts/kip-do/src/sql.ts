/**
 * SQL helpers shaped by Durable Object SQLite's hard limits.
 *
 * The one that shapes everything here: **a query may bind at most 100
 * parameters**. A graph engine's read path is fundamentally set intersection
 * over id lists, so the natural `WHERE id IN (?, ?, …)` is unavailable past
 * 100 ids. The workaround is to pass the whole set as a *single* JSON array
 * parameter and join against `json_each(?)`. One parameter, and the value may
 * be up to the 2 MB string limit — roughly 10^5 ids.
 *
 * Every id-set query in this package goes through `idSet()`. Hand-writing an
 * `IN (...)` list anywhere is a latent failure that only appears once a graph
 * grows past 100 matching rows, which is exactly the kind of bug that ships.
 */

import { errors } from './errors.js'
import { canonicalJson } from './json.js'

/** Durable Object SQLite: maximum bound parameters per query. */
export const MAX_BOUND_PARAMS = 100

/** Durable Object SQLite: maximum string / BLOB / row size. */
export const MAX_VALUE_BYTES = 2 * 1024 * 1024

/**
 * Renders an id set as a single bindable JSON parameter.
 *
 * Use as:
 * ```ts
 * sql.exec(
 *   `SELECT c.* FROM concepts c JOIN json_each(?) j ON c.id = j.value`,
 *   idSet(ids),
 * )
 * ```
 *
 * `json_each` over an array yields one row per element in `j.value`, so the
 * join is an ordinary indexed lookup — SQLite does not materialize the array
 * as a temp table.
 */
export function idSet(ids: Iterable<number | string>): string {
  const array = Array.isArray(ids) ? ids : Array.from(ids)
  // Measured through `checkValueSize` rather than against `json.length`: a
  // JS string counts UTF-16 code units and SQLite counts UTF-8 bytes, so a
  // set of proposition addresses with multi-byte predicates would slip past a
  // length check and surface as an opaque SQLITE_TOOBIG instead.
  return checkValueSize(
    JSON.stringify(array),
    `the id set of ${array.length} entries (page the query with LIMIT and ` +
      `CURSOR to shrink it)`,
  )
}

/**
 * Asserts a statement's parameter count fits the platform limit.
 *
 * Called on the dynamic paths that build parameter lists from user input.
 * Failing here with a KIP code beats letting workerd raise an opaque
 * `too many SQL variables`.
 */
export function checkParamCount(count: number, context: string): void {
  if (count > MAX_BOUND_PARAMS) {
    throw errors.resourceExhausted(
      `${context} needs ${count} bound parameters but Durable Object SQLite ` +
        `allows ${MAX_BOUND_PARAMS}; batch the values through json_each() instead`,
    )
  }
}

/**
 * Guards a value against the 2 MB row/string ceiling before it is written.
 *
 * The Rust engine has no such limit, so this is a new failure mode for a
 * ported workload. Checking here converts a raw `SQLITE_TOOBIG` into a KIP
 * error naming the offending field, which the agent can act on.
 */
export function checkValueSize(value: string, what: string): string {
  // JS strings are UTF-16; SQLite measures UTF-8 bytes. Only pay for the
  // exact measurement when the cheap upper bound (3 bytes per UTF-16 code
  // unit) says we might be over.
  if (value.length * 3 > MAX_VALUE_BYTES) {
    const bytes = new TextEncoder().encode(value).length
    if (bytes > MAX_VALUE_BYTES) {
      throw errors.resourceExhausted(
        `${what} is ${bytes} bytes, over the ${MAX_VALUE_BYTES}-byte limit ` +
          `for a single Durable Object SQLite value`,
      )
    }
  }
  return value
}

/** Serializes a value for storage, size-checked and canonically ordered. */
export function encodeJson(value: unknown, what: string): string {
  return checkValueSize(canonicalJson(value ?? {}), what)
}

/**
 * Escapes a string for an FTS5 MATCH expression.
 *
 * FTS5 query syntax gives `"`, `*`, `:`, `^`, `-`, `(`, `)` and `OR`/`AND`/
 * `NOT` special meaning. Search terms come from agents and end users, so they
 * must never be interpreted as operators: a term containing `-` would
 * silently become a negation and return the opposite of what was asked.
 * Wrapping each token in double quotes (with `"` doubled) makes it a literal
 * phrase.
 */
export function ftsQuote(tokens: readonly string[]): string {
  return tokens
    .map((t) => `"${t.replace(/"/g, '""')}"`)
    .join(' OR ')
}
