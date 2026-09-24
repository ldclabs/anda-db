import { canonicalize, parseCanonicalJson } from '@ldclabs/kip-lang'
export { parseCanonicalJson } from '@ldclabs/kip-lang'

/**
 * The JSON value model, and the one canonical way to write it down.
 *
 * Every persisted column that is not a scalar is JSON text, and several
 * engine-level identities are digests over JSON — Schema Package content,
 * Capsule integrity, purge stubs. A digest is only an identity if two runs
 * that hold the same value produce the same bytes, so serialization for those
 * purposes goes through {@link canonicalJson}, never `JSON.stringify`.
 */

export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json }

/** A JSON object. The shape every `attributes` / `facets` / `metadata` map takes. */
export type JsonMap = { [key: string]: Json }

export function isJsonMap(value: unknown): value is JsonMap {
  return (
    typeof value === 'object' && value !== null && !Array.isArray(value)
  )
}

/** The value as a JSON object, or an empty one when it is anything else. */
export function asJsonMap(value: unknown): JsonMap {
  return isJsonMap(value) ? value : {}
}

export function isJsonArray(value: unknown): value is Json[] {
  return Array.isArray(value)
}

/**
 * Serializes a value with object keys in code-unit order and no insignificant
 * whitespace.
 *
 * `JSON.stringify` preserves insertion order, so the same logical map written
 * by two code paths — one that filled `name` first, one that filled `type`
 * first — produces different bytes and therefore a different digest. That is
 * the failure this exists to prevent, and it is silent: nothing compares the
 * two strings, only the hashes derived from them.
 *
 * `undefined` is not a JSON value; a member holding one is dropped, matching
 * `JSON.stringify`, and a top-level one is written as `null` rather than
 * returning the string `"undefined"`.
 *
 * Uses kip-jcs-safe-v1: UTF-16 key order, ECMAScript number formatting,
 * safe integral values, scalar strings and no Unicode normalization.
 */
export function canonicalJson(value: unknown): string {
  // Internal rows have optional undefined members. Remove only those object
  // members before the strict canonicalizer checks the portable JSON domain.
  function present(item: unknown): unknown {
    if (Array.isArray(item)) return item.map(present)
    if (item && typeof item === 'object' &&
        (Object.getPrototypeOf(item) === Object.prototype || Object.getPrototypeOf(item) === null)) {
      return Object.fromEntries(Object.entries(item).filter(([, v]) => v !== undefined)
        .map(([k, v]) => [k, present(v)]))
    }
    return item
  }
  return canonicalize(value === undefined ? null : present(value))
}

/** Parses stored JSON text, returning `fallback` for an empty or absent column. */
export function parseJson<T extends Json>(text: string | null, fallback: T): T {
  if (text === null || text.length === 0) return fallback
  return parseCanonicalJson(text) as T
}

/**
 * Deep structural equality over JSON values.
 *
 * Object member order is not part of a JSON value's identity, so this compares
 * by key rather than by position — which is what reference equality on a
 * `{id: ...}` map and Facet comparison both need.
 */
export function jsonEquals(a: Json, b: Json): boolean {
  if (a === b) return true
  if (a === null || b === null) return false
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) {
      return false
    }
    return a.every((item, i) => jsonEquals(item, b[i] as Json))
  }
  if (typeof a !== 'object' || typeof b !== 'object') return false
  const ka = Object.keys(a)
  const kb = Object.keys(b)
  if (ka.length !== kb.length) return false
  return ka.every(
    (k) => Object.hasOwn(b, k) && jsonEquals(a[k] as Json, b[k] as Json),
  )
}

/**
 * Orders two strings by Unicode code point, the way Rust's `str` orders by
 * UTF-8 bytes.
 *
 * Not `<`, and not `localeCompare`: JavaScript compares strings by UTF-16 code
 * unit, which disagrees with both for anything past the BMP — a surrogate pair
 * sorts below `U+E000`, and its code point is above it. The lists this orders
 * are digested (a Capsule's external refs) or compared across engines (a symbol
 * listing), so an order only one engine produces reads as tampering or drift.
 */
export function compareCodePoints(left: string, right: string): number {
  const shared = Math.min(left.length, right.length)
  for (let i = 0; i < shared; i += 1) {
    const x = left.charCodeAt(i)
    const y = right.charCodeAt(i)
    if (x !== y) return codePointRank(x) < codePointRank(y) ? -1 : 1
  }
  return left.length === right.length ? 0 : left.length < right.length ? -1 : 1
}

/**
 * Where a UTF-16 code unit falls in code-point order at the first difference.
 *
 * Code-unit order already agrees with code-point order except that surrogates
 * (`D800`–`DFFF`, the halves of everything past the BMP) sort below
 * `E000`–`FFFF`. Lifting them above is the whole correction, and it saves
 * splitting both strings into code points on every comparison.
 */
function codePointRank(unit: number): number {
  if (unit < 0xd800) return unit
  return unit < 0xe000 ? unit + 0x2000 : unit - 0x800
}
