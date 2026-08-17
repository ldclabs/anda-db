/**
 * Normalization for the cross-engine conformance suite.
 *
 * Element ids are engine-assigned and differ between runs and between engines,
 * so a fixture cannot name them. They are rewritten to `C:<1>`, `P:<2>` and so
 * on by order of first appearance, which compares *structure* while still
 * catching a wrong reference: two fields naming the same element still
 * normalize to the same ordinal, and two naming different ones still differ.
 *
 * Everything else is compared exactly. A fixture that had to be loose about its
 * expected values would not be pinning behaviour down.
 */

import { tryParseElementId } from '../../src/id.js'
import type { Json } from '../../src/json.js'

/**
 * Members that differ every run and between engines.
 *
 * These are engine truth rather than behaviour: when they happen, which
 * transaction wrote them, and what the artifact digested to under one engine's
 * canonicalization.
 */
const VOLATILE = new Set([
  'created_at',
  'updated_at',
  'created_tx',
  'updated_tx',
  'tx_id',
  'committed_at',
  'valid_at',
  'content_digest',
  'score',
])

export class Normalizer {
  private readonly seen = new Map<string, string>()

  id(raw: string): string {
    const found = this.seen.get(raw)
    if (found !== undefined) return found
    const tag = raw.split('-')[0] ?? '?'
    const ordinal = `${tag}:<${this.seen.size + 1}>`
    this.seen.set(raw, ordinal)
    return ordinal
  }

  value(value: Json): Json {
    if (typeof value === 'string') {
      return tryParseElementId(value) === null ? value : this.id(value)
    }
    if (Array.isArray(value)) return value.map((item) => this.value(item))
    if (value !== null && typeof value === 'object') {
      const out: { [key: string]: Json } = {}
      for (const [key, item] of Object.entries(value)) {
        if (VOLATILE.has(key)) continue
        out[key] = this.value(item)
      }
      return out
    }
    return value
  }
}

/**
 * Sorted keys, so two structurally equal answers compare equal regardless of
 * how either engine ordered its object members.
 */
export function canonical(value: Json): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonical).join(',')}]`
  }
  if (value !== null && typeof value === 'object') {
    const keys = Object.keys(value).sort()
    return `{${keys
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key] as Json)}`)
      .join(',')}}`
  }
  return JSON.stringify(value) ?? 'null'
}

/**
 * Compares a result against a fixture's expectation.
 *
 * `ordered: false` means the order of the top-level array is not part of the
 * contract, so both sides are sorted by their canonical form first — comparing
 * unordered results by position would fail on a difference the fixture
 * deliberately did not pin.
 */
export function sameResult(
  actual: Json,
  expected: Json,
  ordered: boolean,
): boolean {
  const normalizer = new Normalizer()
  const left = normalizer.value(actual)
  if (!ordered && Array.isArray(left) && Array.isArray(expected)) {
    const sort = (list: Json[]) => [...list].map(canonical).sort()
    return canonical(sort(left) as unknown as Json) ===
      canonical(sort(expected) as unknown as Json)
  }
  return canonical(left) === canonical(expected)
}
