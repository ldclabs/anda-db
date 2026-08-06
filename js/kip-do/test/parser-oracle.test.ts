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
  KIP_ERROR_HINTS,
  KIP_ERROR_NAMES,
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

describe('parser oracle', () => {
  it('has a corpus worth trusting', () => {
    // A shrinking corpus is a silent loss of coverage: the generator walks the
    // Rust tests, so a bad path or a renamed directory shows up here first.
    expect(CORPUS.length).toBeGreaterThan(700)
    const accepted = CORPUS.filter((c) => 'ok' in reference(c)).length
    expect(accepted).toBeGreaterThan(500)
    // Negative cases are the point of harvesting the Rust tests; without them
    // the oracle only proves the two parsers agree on valid input.
    expect(CORPUS.length - accepted).toBeGreaterThan(100)
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
        overAccepted.push(source)
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

  it('carries the reference taxonomy verbatim', () => {
    // `src/errors.generated.ts` is produced by reading `error.rs` as text.
    // The reference grammar reports the same table from the compiled enum, so
    // comparing them catches both a stale checkout and a reader too naive for
    // a change in how the Rust is written.
    const catalog = JSON.parse(wasmErrorCatalog()) as {
      code: string
      name: string
      hint: string
    }[]
    expect(catalog.map((e) => e.code)).toEqual([...KIP_ERROR_CODES])
    for (const entry of catalog) {
      expect(KIP_ERROR_NAMES[entry.code as never]).toBe(entry.name)
      expect(KIP_ERROR_HINTS[entry.code as never]).toBe(entry.hint)
    }
  })
})
