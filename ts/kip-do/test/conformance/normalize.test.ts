import { describe, expect, it } from 'vitest'
import { type Json, canonical, normalize } from './normalize.js'

/**
 * Pins the normalizer against the cases the Rust side mirrors
 * (`rs/anda_cognitive_nexus/tests/conformance.rs`,
 * `normalizer_matches_the_specification`).
 *
 * The two implementations are independent, so without this the suite could
 * drift into comparing differently-normalized values and quietly pass.
 */
describe('conformance normalizer', () => {
  it('numbers ids by first appearance and reuses tokens for repeats', () => {
    const value: Json = [
      { id: 'C:41', ref: 'C:41' },
      { id: 'P:7:treats', subject: 'C:41', object: 'C:99' },
    ]
    const expected: Json = [
      { id: 'C:<1>', ref: 'C:<1>' },
      { id: 'P:<2>:treats', subject: 'C:<1>', object: 'C:<3>' },
    ]
    expect(canonical(normalize(value, true))).toBe(canonical(expected))
  })

  it('drops volatile metadata but keeps _version', () => {
    const value = {
      metadata: { _version: 3, _created_at: 'x', _updated_at: 'y', src: 't' },
    }
    expect(canonical(normalize(value, true))).toBe(
      canonical({ metadata: { _version: 3, src: 't' } }),
    )
  })

  it('keeps a predicate containing a colon intact', () => {
    expect(canonical(normalize(['P:5:a:b'], true))).toBe(
      canonical(['P:<1>:a:b']),
    )
  })

  it('sorts unordered results but keeps nested array order', () => {
    expect(canonical(normalize([['b', 2], ['a', 1]], false))).toBe(
      canonical([
        ['a', 1],
        ['b', 2],
      ]),
    )
    expect(canonical(normalize([['b', 'a']], false))).toBe(
      canonical([['b', 'a']]),
    )
  })
})
