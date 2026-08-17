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
 * This is deliberately *not* presented as the KIP canonicalization profile:
 * the spec's profile is still a draft, and the Rust engine's
 * `store::schema::content_digest` makes the same reservation.
 */
export function canonicalJson(value: unknown): string {
  if (value === undefined || value === null) return 'null'
  if (typeof value === 'number') {
    // NaN and ±Infinity have no JSON spelling; `JSON.stringify` writes them as
    // `null`, which would make two different values digest identically.
    if (!Number.isFinite(value)) {
      throw new TypeError(`${value} has no canonical JSON form`)
    }
    return JSON.stringify(value)
  }
  if (typeof value === 'boolean' || typeof value === 'string') {
    return JSON.stringify(value)
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    return `{${entries
      .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`)
      .join(',')}}`
  }
  throw new TypeError(`${typeof value} has no canonical JSON form`)
}

/** Parses stored JSON text, returning `fallback` for an empty or absent column. */
export function parseJson<T extends Json>(text: string | null, fallback: T): T {
  if (text === null || text.length === 0) return fallback
  return JSON.parse(text) as T
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
