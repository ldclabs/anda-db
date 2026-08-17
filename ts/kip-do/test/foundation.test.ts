import { describe, expect, it } from 'vitest'
import { digestParts, sha256Hex, sha256Text } from '../src/digest.js'
import {
  compareElementId,
  elementId,
  formatElementId,
  parseElementId,
  parseElementIdOfKind,
  tryParseElementId,
  UNREACHABLE_SEQ,
} from '../src/id.js'
import { canonicalJson, jsonEquals } from '../src/json.js'
import { normalizeTime, TIME_MAX, TIME_MIN } from '../src/time.js'
import {
  endpointFromJson,
  endpointKey,
  endpointToJson,
  literalFromScalar,
  localRef,
  tupleKey,
} from '../src/term.js'

const key = (value: unknown) => endpointKey(endpointFromJson(value as never))

describe('digest', () => {
  it('agrees with the published SHA-256 vectors', () => {
    // Everything downstream — tuple identity, package identity, purge stubs —
    // is only as trustworthy as this.
    expect(sha256Text('')).toBe(
      'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    )
    expect(sha256Text('abc')).toBe(
      'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
    )
    expect(
      sha256Text(
        'abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq',
      ),
    ).toBe('248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1')
  })

  it('hashes past the one-block and length-field boundaries', () => {
    // 55, 56 and 64 bytes are where the padding either does or does not need a
    // second block; a wrong boundary passes the short vectors above.
    expect(sha256Text('a'.repeat(55))).toBe(
      '9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318',
    )
    expect(sha256Text('a'.repeat(56))).toBe(
      'b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a',
    )
    expect(sha256Text('a'.repeat(64))).toBe(
      'ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb',
    )
  })

  it('accepts raw bytes, not only text', () => {
    expect(sha256Hex(new Uint8Array([0x61, 0x62, 0x63]))).toBe(
      sha256Text('abc'),
    )
  })

  it('keeps a shifted boundary from digesting the same', () => {
    // Without length prefixes ("ab","c") and ("a","bc") are one byte string,
    // and two different tuples would share a tuple_key — which is to say,
    // silently become one Proposition.
    expect(digestParts(['ab', 'c'])).not.toBe(digestParts(['a', 'bc']))
    expect(digestParts(['ab', 'c'])).toBe(digestParts(['ab', 'c']))
  })
})

describe('element ids', () => {
  it('round-trips every kind through its wire form', () => {
    for (const kind of [
      'Concept',
      'Proposition',
      'Assertion',
      'Evidence',
      'Activity',
    ] as const) {
      const id = elementId(kind, 42)
      expect(parseElementId(formatElementId(id))).toEqual(id)
    }
  })

  it('gives every kind its own tag', () => {
    const tags = new Set(
      (['Concept', 'Proposition', 'Assertion', 'Evidence', 'Activity'] as const)
        .map((kind) => formatElementId(elementId(kind, 1)).charAt(0)),
    )
    expect(tags.size).toBe(5)
  })

  it('lets one element answer to exactly one spelling', () => {
    // Anything that would give a second spelling of the same id lets two
    // references disagree about equality while naming the same row.
    for (const bad of [
      'C-+1', 'C-01', 'C- 1', 'C-1 ', 'c-1', 'CC-1', 'C-', '-1', 'C1',
      'C-1.0', 'C-1-2', '', 'Q-1',
    ]) {
      expect(tryParseElementId(bad), bad).toBeNull()
    }
  })

  it('reports a wrong kind as structural, not as missing', () => {
    expect(() => parseElementIdOfKind('E-1', 'Proposition')).toThrowError(
      /StructuralReferenceInvalid|names a Evidence|names an Evidence/,
    )
    expect(parseElementIdOfKind('P-1', 'Proposition')).toEqual(
      elementId('Proposition', 1),
    )
  })

  it('treats an id past the safe integer range as unreachable, not malformed', () => {
    // Well-formed, but it cannot name a row this engine created — so it has to
    // report "not found" downstream rather than a syntax error here.
    expect(parseElementId('C-99999999999999999999').seq).toBe(UNREACHABLE_SEQ)
  })

  it('orders by kind tag then sequence', () => {
    const ids = [
      elementId('Proposition', 1),
      elementId('Concept', 2),
      elementId('Concept', 1),
    ].sort(compareElementId)
    expect(ids.map(formatElementId)).toEqual(['C-1', 'C-2', 'P-1'])
  })
})

describe('time', () => {
  it('treats an offset as a spelling, not a different instant', () => {
    const utc = normalizeTime('2026-08-16T02:00:00Z', 'observed_at')
    expect(normalizeTime('2026-08-16T10:00:00+08:00', 'observed_at')).toBe(utc)
    expect(utc).toBe('2026-08-16T02:00:00.000Z')
  })

  it('makes lexicographic order chronological order', () => {
    // This is the property every temporal range query depends on.
    const stamps = [
      '2026-01-01T00:00:00Z',
      '2025-12-31T23:59:59.999Z',
      '2026-01-01T00:00:00.001Z',
      '2099-12-31T23:59:59Z',
    ].map((s) => normalizeTime(s, 't'))
    const lexicographic = [...stamps].sort()
    const chronological = [...stamps].sort(
      (a, b) => Date.parse(a) - Date.parse(b),
    )
    expect(lexicographic).toEqual(chronological)
  })

  it('sorts the open-ended sentinel above every real timestamp', () => {
    expect(TIME_MAX > normalizeTime('9999-12-31T23:59:59Z', 't')).toBe(true)
    expect(TIME_MIN < normalizeTime('0001-01-01T00:00:00Z', 't')).toBe(true)
  })

  it('refuses what Date.parse would happily invent', () => {
    // `Date.parse` accepts several of these and produces an instant for them.
    // A `valid_from` that silently means whatever the host thought "tomorrow"
    // was is worse than one that refuses.
    for (const bad of ['yesterday', '2026-8-1T00:00:00Z', '2026/08/01', '12:00']) {
      expect(() => normalizeTime(bad, 'valid_from'), bad).toThrowError(
        /must be an RFC 3339 timestamp/,
      )
    }
  })

  it('names the field it rejected', () => {
    expect(() => normalizeTime('nope', 'valid_from')).toThrowError(/valid_from/)
  })

  it('is idempotent', () => {
    const once = normalizeTime('2026-08-16T10:00:00+08:00', 't')
    expect(normalizeTime(once, 't')).toBe(once)
  })
})

describe('endpoints and literals', () => {
  it('makes 1, 1.0 and 1e0 one Literal', () => {
    // Spec §9.4: three lexical forms of the same finite value must not become
    // three semantic Propositions.
    const one = key(1)
    expect(key(1.0)).toBe(one)
    expect(key(JSON.parse('1e0'))).toBe(one)
    expect(key(1.5)).not.toBe(one)
    expect(key('1')).not.toBe(one)
  })

  it('makes a language tag part of Literal identity', () => {
    // Spec §9.5.
    const bare = key('苹果')
    const tagged = key({ value: '苹果', language: 'zh-Hans' })
    expect(tagged).not.toBe(bare)
    expect(
      key({ value: '苹果', datatype: 'kip:string', language: 'zh-Hans' }),
    ).toBe(tagged)
  })

  it('makes null equal only to null', () => {
    const nul = key(null)
    expect(key('')).not.toBe(nul)
    expect(key(false)).not.toBe(nul)
    expect(key({ value: null })).toBe(nul)
  })

  it('survives a refined datatype through a round trip', () => {
    const value = { value: '2026-08-13T10:00:00Z', datatype: 'kip:datetime' }
    const endpoint = endpointFromJson(value)
    expect(endpointToJson(endpoint)).toEqual(value)
    // A datetime and a plain string with the same text are different Literals,
    // so they cannot silently share a Proposition.
    expect(endpointKey(endpoint)).not.toBe(key('2026-08-13T10:00:00Z'))
  })

  it('keeps the reference kinds distinguishable', () => {
    const local = key({ id: 'C-1' })
    const canonical = key({ canonical_id: 'did:example:123' })
    const foreign = key({ space_id: 'public://research', element_id: 'C-1' })
    expect(new Set([local, canonical, foreign]).size).toBe(3)
  })

  it('refuses a structured value as a Core Literal', () => {
    // Spec §9.2.
    expect(() => endpointFromJson([1, 2])).toThrow()
    expect(() => literalFromScalar({ a: 1 } as never)).toThrow()
    expect(() => literalFromScalar(Number.NaN)).toThrowError(/NaN/)
  })

  it('refuses a non-local reference in a structural slot', () => {
    expect(localRef({ id: 'P-1' }, 'proposition')).toEqual(
      elementId('Proposition', 1),
    )
    expect(() =>
      localRef({ canonical_id: 'did:example:1' }, 'proposition'),
    ).toThrowError(/must reference a local element/)
  })

  it('separates in a tuple key what a concatenation would merge', () => {
    const alice = endpointFromJson({ id: 'C-1' })
    const bob = endpointFromJson({ id: 'C-2' })
    const dark = endpointFromJson('dark')

    const base = tupleKey('s1', alice, 'prefers', dark)
    expect(tupleKey('s1', alice, 'prefers', dark)).toBe(base)
    // Every coordinate of the tuple participates in its identity.
    expect(tupleKey('s2', alice, 'prefers', dark)).not.toBe(base)
    expect(tupleKey('s1', bob, 'prefers', dark)).not.toBe(base)
    expect(tupleKey('s1', alice, 'likes', dark)).not.toBe(base)
    expect(tupleKey('s1', alice, 'prefers', bob)).not.toBe(base)
    expect(tupleKey('s', alice, 'ab', dark)).not.toBe(
      tupleKey('s', alice, 'a', dark),
    )
  })
})

describe('canonical JSON', () => {
  it('does not depend on which code path filled the map first', () => {
    // The failure this prevents is silent: nothing compares the two strings,
    // only the digests derived from them.
    expect(canonicalJson({ b: 1, a: 2 })).toBe(canonicalJson({ a: 2, b: 1 }))
    expect(canonicalJson({ a: 2, b: 1 })).toBe('{"a":2,"b":1}')
  })

  it('sorts nested members too', () => {
    expect(canonicalJson({ x: { d: 1, c: [{ b: 1, a: 2 }] } })).toBe(
      '{"x":{"c":[{"a":2,"b":1}],"d":1}}',
    )
  })

  it('drops undefined members rather than writing them', () => {
    expect(canonicalJson({ a: 1, b: undefined })).toBe('{"a":1}')
    expect(canonicalJson(undefined)).toBe('null')
  })

  it('refuses a number with no JSON spelling', () => {
    // `JSON.stringify` writes these as `null`, which would make two different
    // values digest identically.
    expect(() => canonicalJson(Number.NaN)).toThrow()
    expect(() => canonicalJson(Number.POSITIVE_INFINITY)).toThrow()
  })

  it('compares JSON by member, not by position', () => {
    expect(jsonEquals({ a: 1, b: 2 }, { b: 2, a: 1 })).toBe(true)
    expect(jsonEquals([1, 2], [2, 1])).toBe(false)
    expect(jsonEquals({ a: 1 }, { a: 1, b: null })).toBe(false)
  })
})
