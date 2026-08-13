import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { AlinkTokenizer, SimpleTokenizer, extractJsonText } from '../src/index.js'
import { executeTestKip, type TestKipDatabase } from './worker.js'

/**
 * Full-text search, including the path that only exists because Durable Object
 * SQLite cannot segment Chinese: an external tokenizer is the sole authority
 * for both indexing and querying.
 */

let counter = 1000
async function freshStub() {
  const stub = env.KIP_DB.getByName(`search-${counter++}`)
  await declareSchema(stub)
  return stub
}

async function expectOk(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<any> {
  const response = await executeTestKip(stub, command)
  if ('error' in (response as any)) {
    throw new Error(
      `expected success but got ${(response as any).error.code}: ${(response as any).error.message}`,
    )
  }
  return (response as any).result
}

/**
 * Stand-in for `cf-tokenizer`, speaking the same HTTP contract:
 * `POST /tokenize {texts, mode:"search"} -> {tokens}` with
 * `X-Tokenizer-Version` on every response.
 *
 * The segmentation is a fixed dictionary rather than jieba — the point of the
 * test is the *plumbing* (same tokenizer on write and read, version stamping,
 * batch chunking), which is where a real deployment breaks.
 */
function stubTokenizerFetcher(version = 'test-jieba-1') {
  const DICTIONARY = [
    '阿司匹林',
    '头痛',
    '解热镇痛',
    '知识图谱',
    '记忆',
    '药物',
  ]
  return {
    calls: 0,
    async fetch(_input: RequestInfo, init?: RequestInit): Promise<Response> {
      this.calls++
      const body = JSON.parse(String(init?.body)) as {
        texts: string[]
        mode: string
      }
      if (body.mode !== 'search') {
        return new Response('bad mode', { status: 400 })
      }
      const tokens = body.texts.map((text) => {
        const found: string[] = []
        let rest = text.normalize('NFKC').toLowerCase()
        for (const word of DICTIONARY) {
          if (rest.includes(word)) {
            found.push(word)
            rest = rest.split(word).join(' ')
          }
        }
        for (const match of rest.matchAll(/[a-z0-9]+/g)) found.push(match[0])
        return [...new Set(found)]
      })
      return new Response(JSON.stringify({ tokens }), {
        headers: {
          'content-type': 'application/json',
          'x-tokenizer-version': version,
        },
      })
    },
  }
}

/**
 * Declares the concept types and predicates a test uses.
 *
 * KIP is schema-first: a type must exist as a `$ConceptType` concept before
 * anything can be an instance of it, and a predicate as a `$PropositionType`
 * before any proposition can use it. The bundled capsules cover the base
 * vocabulary; these are the test-local additions.
 */
async function declareSchema(
  stub: DurableObjectStub<TestKipDatabase>,
): Promise<void> {
  const types = [
    'Drug', 'Symptom', 'Herb', 'Source', 'Category', 'Bulk', 'T', 'N',
  ]
  const predicates = [
    'treats', 'interacts_with', 'isa', 'rel', 'cited_by', 'made_by', 'banned',
    'r', 'p', 'q',
  ]
  const blocks = [
    ...types.map((t) => `CONCEPT ?t_${t} { {type: "$ConceptType", name: "${t}"} }`),
    ...predicates.map(
      (p) => `CONCEPT ?p_${p} { {type: "$PropositionType", name: "${p}"} }`,
    ),
  ].join('\n')
  const response = await executeTestKip(stub, `UPSERT {\n${blocks}\n}`)
  if ('error' in (response as any)) {
    throw new Error(
      `schema declaration failed: ${(response as any).error.message}`,
    )
  }
}

describe('AlinkTokenizer client', () => {
  it('speaks the documented contract and returns the service version', async () => {
    const fetcher = stubTokenizerFetcher()
    const tokenizer = new AlinkTokenizer(fetcher)
    const result = await tokenizer.tokenize(['阿司匹林可以缓解头痛'])
    expect(result.version).toBe('test-jieba-1')
    expect(result.tokens[0]).toContain('阿司匹林')
    expect(result.tokens[0]).toContain('头痛')
  })

  it('chunks batches to the service limit of 256 texts', async () => {
    const fetcher = stubTokenizerFetcher()
    const tokenizer = new AlinkTokenizer(fetcher)
    const texts = Array.from({ length: 600 }, (_, i) => `item ${i}`)
    const result = await tokenizer.tokenize(texts)
    expect(result.tokens).toHaveLength(600)
    // 600 texts must arrive as 3 requests, not 1 rejected one.
    expect(fetcher.calls).toBe(3)
  })

  it('fails rather than silently degrading when the service errors', async () => {
    // A fallback to local segmentation would write tokens that disagree with
    // everything indexed before and after, and the corruption would be
    // invisible until a query quietly returned nothing.
    const tokenizer = new AlinkTokenizer({
      async fetch() {
        return new Response('upstream down', { status: 503 })
      },
    })
    await expect(tokenizer.tokenize(['头痛'])).rejects.toThrow(/503/)
  })

  it('rejects a response with no version header', async () => {
    const tokenizer = new AlinkTokenizer({
      async fetch() {
        return new Response(JSON.stringify({ tokens: [[]] }), {
          headers: { 'content-type': 'application/json' },
        })
      },
    })
    await expect(tokenizer.tokenize(['x'])).rejects.toThrow(
      /X-Tokenizer-Version/,
    )
  })
})

describe('extractJsonText', () => {
  it('harvests nested strings and keys but skips reserved metadata', () => {
    const text = extractJsonText({
      title: '知识图谱',
      nested: { detail: 'memory', count: 3 },
      list: ['a', 'b'],
      _version: 7,
    })
    expect(text).toContain('知识图谱')
    expect(text).toContain('memory')
    expect(text).toContain('detail')
    // `_version` would let a query match on engine bookkeeping.
    expect(text).not.toContain('7')
  })
})

describe('SEARCH', () => {
  it('finds ASCII content through the default tokenizer', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a {
           {type: "Drug", name: "Aspirin"}
           SET ATTRIBUTES { note: "analgesic and antipyretic" }
         }
         CONCEPT ?b {
           {type: "Drug", name: "Warfarin"}
           SET ATTRIBUTES { note: "anticoagulant" }
         }
       }`,
    )
    const hits = await expectOk(stub, 'SEARCH CONCEPT "analgesic" LIMIT 10')
    expect(hits).toHaveLength(1)
    expect(hits[0].name).toBe('Aspirin')
    // Every hit carries a transient normalized relevance score in [0, 1).
    expect(hits[0].metadata._score).toBeGreaterThan(0)
    expect(hits[0].metadata._score).toBeLessThan(1)
  })

  it('indexes user metadata as searchable concept text', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "Drug", name: "Aspirin"} }
       }
       WITH METADATA {source: "clinicaltrialmarker"}`,
    )

    const hits = await expectOk(
      stub,
      'SEARCH CONCEPT "clinicaltrialmarker" LIMIT 10',
    )
    expect(hits.map((hit: any) => hit.name)).toEqual(['Aspirin'])
  })

  it('returns only the matching proposition link and honors WITH TYPE', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "T", name: "A"} }
         CONCEPT ?b { {type: "T", name: "B"} }
         PROPOSITION ?p {
           (?a, "p", ?b)
           SET ATTRIBUTES {note: "clinicalmarker"}
         }
         PROPOSITION ?q {
           (?a, "q", ?b)
           SET ATTRIBUTES {note: "regulatorymarker"}
         }
       }`,
    )
    await runInDurableObject(stub, async (instance) => {
      const nexus = (instance as unknown as { nexus: any }).nexus
      await nexus.reindexStale('simple-1')
    })

    const matching = await expectOk(
      stub,
      'SEARCH PROPOSITION "clinicalmarker" LIMIT 10',
    )
    expect(matching.map((hit: any) => hit.predicate)).toEqual(['p'])

    const wrongType = await expectOk(
      stub,
      'SEARCH PROPOSITION "clinicalmarker" WITH TYPE "q" LIMIT 10',
    )
    expect(wrongType).toEqual([])

    const matchingType = await expectOk(
      stub,
      'SEARCH PROPOSITION "regulatorymarker" WITH TYPE "q" LIMIT 10',
    )
    expect(matchingType.map((hit: any) => hit.predicate)).toEqual(['q'])
  })

  it('filters by type after scoring', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "Drug", name: "Aspirin"} SET ATTRIBUTES {note: "relief"} }
         CONCEPT ?b { {type: "Herb", name: "Willow"} SET ATTRIBUTES {note: "relief"} }
       }`,
    )
    const hits = await expectOk(
      stub,
      'SEARCH CONCEPT "relief" WITH TYPE "Herb" LIMIT 10',
    )
    expect(hits.map((h: any) => h.name)).toEqual(['Willow'])
  })

  it('returns nothing below THRESHOLD', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?a { {type: "T", name: "Alpha"} SET ATTRIBUTES {note: "beta"} } }',
    )
    const hits = await expectOk(
      stub,
      'SEARCH CONCEPT "beta" THRESHOLD 0.99 LIMIT 10',
    )
    expect(hits).toEqual([])
  })

  it('segments Chinese through the external tokenizer on both write and read', async () => {
    // This is the case Durable Object SQLite cannot handle alone: FTS5's
    // built-in tokenizers leave a Han run as one token, so "头痛" would never
    // match a document containing "阿司匹林可以缓解头痛".
    const stub = env.KIP_DB.getByName(`search-zh-${counter++}`)
    await declareSchema(stub)
    const fetcher = stubTokenizerFetcher()

    await runInDurableObject(stub, async (instance) => {
      // Swap in the stub service for this object.
      const nexus = (instance as unknown as { nexus: any }).nexus
      nexus.tokenizer = new AlinkTokenizer(fetcher)
    })

    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a {
           {type: "Drug", name: "阿司匹林"}
           SET ATTRIBUTES { note: "解热镇痛，可缓解头痛" }
         }
         CONCEPT ?b {
           {type: "Drug", name: "药物"}
           SET ATTRIBUTES { note: "知识图谱记忆" }
         }
       }`,
    )

    const byName = await expectOk(stub, 'SEARCH CONCEPT "阿司匹林" LIMIT 10')
    expect(byName.map((h: any) => h.name)).toEqual(['阿司匹林'])

    // The decisive assertion: a term that appears only *inside* a Han run of
    // the indexed attribute still matches, which requires the write path and
    // the read path to have segmented identically.
    const byInnerTerm = await expectOk(stub, 'SEARCH CONCEPT "头痛" LIMIT 10')
    expect(byInnerTerm.map((h: any) => h.name)).toEqual(['阿司匹林'])

    const other = await expectOk(stub, 'SEARCH CONCEPT "知识图谱" LIMIT 10')
    expect(other.map((h: any) => h.name)).toEqual(['药物'])
  })

  it('detects a tokenizer upgrade during an alarm without an intervening write', async () => {
    const stub = env.KIP_DB.getByName(`search-reindex-${counter++}`)
    await declareSchema(stub)
    await runInDurableObject(stub, async (instance) => {
      const nexus = (instance as unknown as { nexus: any }).nexus
      nexus.tokenizer = new AlinkTokenizer(stubTokenizerFetcher('v1'))
    })
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?a { {type: "T", name: "阿司匹林"} SET ATTRIBUTES {note: "头痛"} } }',
    )

    await runInDurableObject(stub, async (instance) => {
      const nexus = (instance as unknown as { nexus: any }).nexus
      while ((await nexus.reindexStale('v1')) > 0) {
        // Drain the bounded re-index batches so the stored version is exactly
        // v1 before the tokenizer changes with no intervening database write.
      }
      expect(nexus.tokenizerVersion()).toBe('v1')

      nexus.tokenizer = new AlinkTokenizer(stubTokenizerFetcher('v2'))
      await instance.alarm()
      expect(nexus.tokenizerVersion()).toBe('v2')
    })

    const hits = await expectOk(stub, 'SEARCH CONCEPT "头痛" LIMIT 10')
    expect(hits.map((h: any) => h.name)).toEqual(['阿司匹林'])
  })

  it('treats FTS5 operator characters in a term as literal text', async () => {
    // An unescaped `-` would be read as NOT and invert the query; `"` would be
    // a syntax error. Search terms come from agents, so they must never be
    // interpreted as FTS5 operators.
    const stub = await freshStub()
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?a { {type: "T", name: "Alpha"} SET ATTRIBUTES {note: "beta gamma"} } }',
    )
    const hits = await expectOk(
      stub,
      'SEARCH CONCEPT "beta -gamma OR \\"x" LIMIT 10',
    )
    expect(Array.isArray(hits)).toBe(true)
  })
})

describe('SimpleTokenizer', () => {
  it('is order-preserving and deduplicated', async () => {
    const result = await new SimpleTokenizer().tokenize(['b a b c'])
    expect(result.tokens[0]).toEqual(['b', 'a', 'c'])
  })
})
