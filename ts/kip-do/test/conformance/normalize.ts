/**
 * Normalization for the cross-engine conformance fixtures.
 *
 * The rules are specified in `fixtures/kip-conformance/README.md` and are
 * implemented twice — here and in `rs/anda_cognitive_nexus/tests/conformance.rs`.
 * The duplication is deliberate: a shared implementation would have to live in
 * one language and be called from the other, which would make the suite
 * depend on the very interop it exists to avoid needing.
 *
 * The two implementations must agree exactly. `normalize.test.ts` pins the
 * behaviour with cases the Rust side mirrors.
 */

export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json }

/** Wall-clock fields that cannot match across two runs, let alone two engines. */
const VOLATILE_METADATA_KEYS = new Set(['_created_at', '_updated_at'])

const CONCEPT_ID = /^C:(\d+)$/
const PROPOSITION_ID = /^P:(\d+):(.+)$/s

/**
 * Rewrites entity ids to positional tokens.
 *
 * Absolute ids depend on how many rows an engine created before this one, and
 * the Rust engine seeds bootstrap capsules that the TypeScript engine does
 * not. Numbering by first appearance keeps identity *relationships* asserted
 * — the same id always maps to the same token — while dropping the absolute
 * values that carry no meaning.
 *
 * Traversal is depth-first with object keys visited in sorted order, so the
 * numbering is a pure function of the value and not of key insertion order,
 * which differs between `serde_json::Map` and a JS object.
 */
class IdMapper {
  private readonly map = new Map<number, number>()

  token(raw: string): string | null {
    const concept = CONCEPT_ID.exec(raw)
    if (concept) {
      return `C:<${this.ordinal(Number(concept[1]))}>`
    }
    const proposition = PROPOSITION_ID.exec(raw)
    if (proposition) {
      return `P:<${this.ordinal(Number(proposition[1]))}>:${proposition[2]}`
    }
    return null
  }

  private ordinal(id: number): number {
    const existing = this.map.get(id)
    if (existing !== undefined) return existing
    const next = this.map.size + 1
    this.map.set(id, next)
    return next
  }
}

/**
 * Applies the full normalization: id tokenization, volatile-key removal, and
 * (unless `ordered`) sorting of the top-level array.
 */
export function normalize(value: Json, ordered: boolean): Json {
  const mapper = new IdMapper()
  const walked = walk(value, mapper, false)
  if (!ordered && Array.isArray(walked)) {
    return [...walked].sort((a, b) =>
      canonical(a) < canonical(b) ? -1 : canonical(a) > canonical(b) ? 1 : 0,
    )
  }
  return walked
}

function walk(value: Json, mapper: IdMapper, inMetadata: boolean): Json {
  if (typeof value === 'string') {
    return mapper.token(value) ?? value
  }
  if (Array.isArray(value)) {
    // Nested arrays are data — their order is part of the value, so only the
    // top-level result array is ever reordered.
    return value.map((item) => walk(item, mapper, inMetadata))
  }
  if (value !== null && typeof value === 'object') {
    const out: Record<string, Json> = {}
    // Sorted key order makes the id numbering deterministic across engines.
    for (const key of Object.keys(value).sort()) {
      if (inMetadata && VOLATILE_METADATA_KEYS.has(key)) continue
      out[key] = walk(value[key]!, mapper, inMetadata || key === 'metadata')
    }
    return out
  }
  return value
}

/** Canonical JSON encoding: object keys sorted, for stable comparison. */
export function canonical(value: Json): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value)
  if (Array.isArray(value)) {
    return `[${value.map(canonical).join(',')}]`
  }
  const parts = Object.keys(value)
    .sort()
    .map((k) => `${JSON.stringify(k)}:${canonical(value[k]!)}`)
  return `{${parts.join(',')}}`
}

// ---------------------------------------------------------------------------
// Fixture schema
// ---------------------------------------------------------------------------

export interface FixtureCase {
  name: string
  command: string
  expect: {
    result?: Json
    next_cursor?: string | null
    error?: { code: string; message?: string }
  }
  ordered?: boolean
  skip?: { rust?: string; ts?: string }
}

export interface Fixture {
  name: string
  description?: string
  setup?: string[]
  cases: FixtureCase[]
}

/**
 * Validates a fixture's shape.
 *
 * A malformed fixture that silently runs zero assertions is worse than a
 * broken one — it reads as a passing suite. Both runners call this.
 */
export function validateFixture(fixture: unknown, source: string): Fixture {
  const f = fixture as Fixture
  if (!f || typeof f.name !== 'string') {
    throw new Error(`${source}: fixture is missing a string "name"`)
  }
  if (!Array.isArray(f.cases) || f.cases.length === 0) {
    throw new Error(`${source}: fixture "${f.name}" has no cases`)
  }
  const seen = new Set<string>()
  for (const c of f.cases) {
    if (typeof c.name !== 'string' || typeof c.command !== 'string') {
      throw new Error(
        `${source}: every case needs string "name" and "command" fields`,
      )
    }
    if (seen.has(c.name)) {
      throw new Error(`${source}: duplicate case name ${JSON.stringify(c.name)}`)
    }
    seen.add(c.name)
    const hasResult = c.expect && 'result' in c.expect
    const hasError = c.expect && 'error' in c.expect
    if (!hasResult && !hasError) {
      throw new Error(
        `${source}: case ${JSON.stringify(c.name)} asserts nothing; give it ` +
          `expect.result or expect.error`,
      )
    }
    if (hasResult && hasError) {
      throw new Error(
        `${source}: case ${JSON.stringify(c.name)} expects both a result and ` +
          `an error`,
      )
    }
  }
  return f
}
