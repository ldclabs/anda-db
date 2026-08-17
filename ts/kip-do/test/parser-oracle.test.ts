import { describe, expect, it } from 'vitest'
import {
  initSync,
  parse as wasmParse,
  error_catalog as wasmErrorCatalog,
} from '../vendor/anda_kip_wasm/anda_kip_wasm.js'
import wasmModule from '../vendor/anda_kip_wasm/anda_kip_wasm_bg.wasm'
import CORPUS from './oracle/corpus.generated.js'
import { parseKip } from '../src/kip/parser.js'
import {
  KIP_ERROR_CODES,
  KIP_ERROR_REGISTRY,
} from '../src/errors.generated.js'

/**
 * Differential test against the reference grammar.
 *
 * This engine parses with `@ldclabs/kip-lang`; `rs/anda_cognitive_nexus`
 * parses with `anda_kip`. Nothing structural forces those two to agree on what
 * a command means anymore — this test is what does. `rs/anda_kip` compiled to
 * WebAssembly is the oracle: it is not shipped, it is a devDependency of this
 * file, rebuilt with `pnpm run build:oracle-wasm`.
 *
 * A failure here is a divergence between the two KIP engines, which is the
 * most expensive kind of bug this project can have: the same command would
 * succeed on one deployment and fail on the other, or worse, mean two
 * different things.
 */

initSync({ module: wasmModule as WebAssembly.Module })

type Outcome =
  | { ok: unknown }
  | { error: { code: string; message: string } }

function reference(source: string): Outcome {
  try {
    return JSON.parse(wasmParse(source)) as Outcome
  } catch (err) {
    return { error: { code: 'THROW', message: String(err) } }
  }
}

/**
 * Structural equality, ignoring object key order.
 *
 * serde emits fields alphabetically and `lower` emits them in declaration
 * order; both are the same AST. Key *presence* still matters — an omitted
 * `expect_version` is not the same command as one set to `undefined`.
 */
function sameShape(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (typeof a !== typeof b) return false
  if (a === null || b === null) return false
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) {
      return false
    }
    return a.every((item, i) => sameShape(item, b[i]))
  }
  if (typeof a !== 'object') return false
  const left = a as Record<string, unknown>
  const right = b as Record<string, unknown>
  const keys = Object.keys(left)
  if (keys.length !== Object.keys(right).length) return false
  return keys.every(
    (key) =>
      Object.prototype.hasOwnProperty.call(right, key) &&
      sameShape(left[key], right[key]),
  )
}

/** Key-sorted JSON, so a reported difference is readable. */
function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, v) =>
    v && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v).sort(([x], [y]) => (x < y ? -1 : 1)))
      : v,
  )
}

function engine(source: string): Outcome {
  try {
    return { ok: parseKip(source) }
  } catch (err) {
    const e = err as { code?: string; message: string }
    return { error: { code: e.code ?? 'THROW', message: e.message } }
  }
}

/**
 * Commands the reference grammar rejects and `@ldclabs/kip-lang` accepts.
 *
 * Every entry here is an open bug in the language toolkit, not a licence: the
 * two engines currently disagree about whether these commands are executable
 * at all, and on a real deployment that is the same command succeeding in one
 * place and failing in another. They are listed rather than skipped so the
 * disagreement has a name, a reason and a size.
 *
 * The list is checked in both directions — `still diverges` below fails when
 * kip-lang starts rejecting one of these, which is what stops a fixed bug from
 * living on here as folklore. Delete the entry then.
 *
 * The first one is the worst: kip-lang does not reject the out-of-range
 * integer, it *rounds* it, so the command executes with a different number
 * than it says. This engine cannot defend against that on its own — by the
 * time it sees the lowered AST the digits are gone — which is why it is
 * recorded here rather than worked around in `src/`.
 */
const KNOWN_DIVERGENCES: readonly { source: string; why: string }[] = [
  {
    source:
      'CREATE CONCEPT ?c { TYPE "T" SET ATTRIBUTES { n: 18446744073709551617 } }',
    why: 'an integer past the representable range is rounded, not refused',
  },
  {
    source: 'EXPORT CAPSULE :out WHERE { }',
    why: 'an unbounded EXPORT is not a Capsule; the empty selection is accepted',
  },
  {
    source: 'UPSERT CONCEPT ?c { MATCH {id: ?anything} }',
    why: 'UPSERT MATCH must name a stable identity, never a variable',
  },
  {
    source: 'UPSERT CONCEPT ?c { SET FIELDS {name: "Alice"} }',
    why: 'UPSERT with no MATCH clause is accepted',
  },
]

const DIVERGENT = new Set(KNOWN_DIVERGENCES.map((d) => d.source))

describe('parser oracle', () => {
  it('has a corpus worth trusting', () => {
    // A shrinking corpus is a silent loss of coverage: the generator walks the
    // Rust sources and tests, so a bad path or a renamed directory shows up
    // here first.
    expect(CORPUS.length).toBeGreaterThan(500)
    const accepted = CORPUS.filter((c) => 'ok' in reference(c)).length
    expect(accepted).toBeGreaterThan(350)
    // Negative cases are the point of harvesting the Rust tests; without them
    // the oracle only proves the two parsers agree on valid input.
    expect(CORPUS.length - accepted).toBeGreaterThan(150)
  })

  it('still diverges on exactly the known set, no more and no fewer', () => {
    const fixed = KNOWN_DIVERGENCES.filter(
      (d) => !('ok' in engine(d.source)) || 'ok' in reference(d.source),
    ).map((d) => `${d.source}\n  was: ${d.why}`)

    expect(
      fixed,
      'kip-lang no longer diverges here — delete these entries from KNOWN_DIVERGENCES',
    ).toEqual([])
  })

  it('agrees with the reference grammar on every command in the corpus', () => {
    const astDiffers: string[] = []
    const overAccepted: string[] = []
    const overRejected: string[] = []

    for (const source of CORPUS) {
      const expected = reference(source)
      const actual = engine(source)

      if ('ok' in expected && 'ok' in actual) {
        if (!sameShape(expected.ok, actual.ok)) {
          astDiffers.push(
            `${source}\n  reference: ${canonical(expected.ok)}\n  engine:    ${canonical(actual.ok)}`,
          )
        }
      } else if ('ok' in actual) {
        if (!DIVERGENT.has(source)) overAccepted.push(source)
      } else if ('ok' in expected) {
        overRejected.push(`${source}\n  engine: ${actual.error.message}`)
      }
    }

    expect(
      { astDiffers, overAccepted, overRejected },
      'the two KIP engines disagree about what these commands mean',
    ).toEqual({ astDiffers: [], overAccepted: [], overRejected: [] })
  })

  it('rejects a parse failure with a KIP code, not a bare throw', () => {
    for (const source of CORPUS) {
      if ('ok' in reference(source)) continue
      const actual = engine(source)
      expect('error' in actual && actual.error.code).not.toBe('THROW')
      if ('error' in actual) {
        expect(KIP_ERROR_CODES).toContain(actual.error.code)
      }
    }
  })

  it('carries the reference registry verbatim', () => {
    // `src/errors.generated.ts` is produced from this same catalog, so on its
    // own this only proves the generator ran. What it does catch is a stale
    // checkout: the committed table against the grammar the suite actually
    // links, which is the pair that has to agree for `hint` and `retry` to
    // mean the same thing on both engines.
    const catalog = JSON.parse(wasmErrorCatalog()) as {
      code: string
      category: string
      retry: string
      hint: string
    }[]
    expect(catalog.map((e) => e.code)).toEqual([...KIP_ERROR_CODES])
    for (const entry of catalog) {
      expect(KIP_ERROR_REGISTRY[entry.code as never]).toEqual({
        category: entry.category,
        retry: entry.retry,
        hint: entry.hint,
      })
    }
  })
})
