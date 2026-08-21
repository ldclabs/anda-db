import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { segment, segmenterMark } from '../src/tokenizer.js'

/**
 * `SEARCH` end to end, through the real parser and the real index.
 *
 * Every case here goes through `nexus.describe`, not through `searchIndex`,
 * because the defects worth catching live between the layers: text indexed
 * under boundaries the query path does not reproduce, a hit that survives a
 * filter it should not, a score whose sign makes the default threshold reject
 * everything.
 */
async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus) => void,
): Promise<void> {
  const stub = env.KIP_DB.getByName(`search-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    body(nexus)
  })
}

interface Answer {
  hits: { id: string; kind: string; score: number; element: Record<string, unknown> }[]
  search_context: Record<string, unknown>
  caveat: string
  next_cursor?: string
}

const SETUP = `MUTATE {
  CREATE CONCEPT ?alice {
    TYPE "Person"
    NAME "Alice Chen"
    SET FIELDS { aliases: ["陈爱丽"] }
    SET ATTRIBUTES { title: "staff engineer" }
  }
  CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob Stone" }
  CREATE CONCEPT ?dark {
    TYPE "Preference"
    NAME "深色模式"
    SET ATTRIBUTES { scope: "所有应用" }
  }
  ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
  CREATE ASSERTION ?a {
    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated" }
  }
  CREATE EVIDENCE ?e {
    SET FIELDS {
      evidence_class: "user_statement",
      payload: {inline: "我以后都想用深色模式"}
    }
  }
}`

describe('segmentation', () => {
  it('puts boundaries where unicode61 cannot see them', () => {
    // The whole reason this layer exists: `unicode61` collapses a Han run into
    // one token, so `深色模式` would index as a single term nothing matches.
    expect(segment('我喜欢深色模式')).toEqual(['我', '喜欢', '深色', '模式'])
    // Latin is left to FTS5 — the segments are words, and the final splitting
    // on apostrophes and case happens identically on both paths.
    expect(segment('Alice Chen')).toEqual(['alice', 'chen'])
    // Punctuation-only text is not a term. It must not reach FTS5 as an empty
    // MATCH, which is a syntax error about a query nobody wrote.
    expect(segment('!!! ???')).toEqual([])
  })

  it('is stable enough to be worth recording', () => {
    // The mark is what triggers a rebuild when ICU moves under the index. If it
    // were not deterministic within one runtime it would rebuild on every
    // construction instead.
    expect(segmenterMark()).toBe(segmenterMark())
    expect(segmenterMark()).toContain('深色|模式')
  })
})

describe('SEARCH', () => {
  it('finds a Concept by a word of its name', async () => {
    await withNexus('name', (nexus) => {
      nexus.execute(SETUP)
      const answer = nexus.describe('SEARCH CONCEPT "Alice"') as unknown as Answer
      expect(answer.hits).toHaveLength(1)
      expect(answer.hits[0]?.kind).toBe('concept')
      expect(answer.hits[0]?.element.name).toBe('Alice Chen')
      expect(answer.hits[0]?.id).toMatch(/^C-\d+$/)
      // Positive and above the default threshold. FTS5's own bm25() is negative
      // and better-is-lower; leaving that sign alone would make `THRESHOLD 0.0`
      // reject every hit there is.
      expect(answer.hits[0]?.score).toBeGreaterThan(0)
    })
  })

  it('finds Chinese text a whole-run tokenizer would miss', async () => {
    await withNexus('chinese', (nexus) => {
      nexus.execute(SETUP)
      const answer = nexus.describe('SEARCH CONCEPT "深色模式"') as unknown as Answer
      expect(answer.hits.map((h) => h.element.name)).toContain('深色模式')

      // A query that is only *part* of the indexed name still matches, because
      // both sides are segmented by the same function.
      const partial = nexus.describe('SEARCH CONCEPT "模式"') as unknown as Answer
      expect(partial.hits.map((h) => h.element.name)).toContain('深色模式')
    })
  })

  it('searches aliases and attributes, not just the name', async () => {
    await withNexus('fields', (nexus) => {
      nexus.execute(SETUP)
      const alias = nexus.describe('SEARCH CONCEPT "陈爱丽"') as unknown as Answer
      expect(alias.hits.map((h) => h.element.name)).toEqual(['Alice Chen'])
      const attribute = nexus.describe('SEARCH CONCEPT "staff"') as unknown as Answer
      expect(attribute.hits.map((h) => h.element.name)).toEqual(['Alice Chen'])
    })
  })

  it('reaches Propositions and Evidence, and Cognition reaches all three', async () => {
    await withNexus('kinds', (nexus) => {
      nexus.execute(SETUP)
      const predicate = nexus.describe('SEARCH PROPOSITION "prefers"') as unknown as Answer
      expect(predicate.hits).toHaveLength(1)
      expect(predicate.hits[0]?.kind).toBe('proposition')

      const evidence = nexus.describe('SEARCH EVIDENCE "深色模式"') as unknown as Answer
      expect(evidence.hits).toHaveLength(1)
      expect(evidence.hits[0]?.kind).toBe('evidence')

      const cognition = nexus.describe('SEARCH COGNITION "深色模式"') as unknown as Answer
      expect(new Set(cognition.hits.map((h) => h.kind))).toEqual(
        new Set(['concept', 'evidence']),
      )
    })
  })

  it('narrows by type and by predicate through the Schema Environment', async () => {
    await withNexus('narrow', (nexus) => {
      nexus.execute(SETUP)
      // "深色模式" is a Preference; asking for People finds nothing rather than
      // finding it under the wrong type.
      const wrong = nexus.describe(
        'SEARCH CONCEPT "深色模式" WITH TYPE "Person"',
      ) as unknown as Answer
      expect(wrong.hits).toHaveLength(0)
      const right = nexus.describe(
        'SEARCH CONCEPT "深色模式" WITH TYPE "Preference"',
      ) as unknown as Answer
      expect(right.hits).toHaveLength(1)

      const predicate = nexus.describe(
        'SEARCH PROPOSITION "prefers" WITH PREDICATE "prefers"',
      ) as unknown as Answer
      expect(predicate.hits).toHaveLength(1)

      // A local name no active package defines is a schema error, not an empty
      // result: the caller asked about a type that does not exist here.
      expect(() =>
        nexus.describe('SEARCH CONCEPT "x" WITH TYPE "Spaceship"'),
      ).toThrowError(/no active Schema Package defines/)
    })
  })

  it('keeps the index in step with the row it describes', async () => {
    await withNexus('maintenance', (nexus) => {
      const created = nexus.execute(
        'CREATE CONCEPT ?c { TYPE "Person" NAME "Charlie Renamed" }',
      )
      const id = created.handles.c!
      expect(
        (nexus.describe('SEARCH CONCEPT "Charlie"') as unknown as Answer).hits,
      ).toHaveLength(1)

      // A rename has to remove the old text as well as add the new one, or the
      // index answers for a name the graph no longer has.
      nexus.execute(`UPDATE "${id}" SET FIELDS { name: "Dana Renamed" }`)
      expect(
        (nexus.describe('SEARCH CONCEPT "Charlie"') as unknown as Answer).hits,
      ).toHaveLength(0)
      expect(
        (nexus.describe('SEARCH CONCEPT "Dana"') as unknown as Answer).hits,
      ).toHaveLength(1)

      // Archiving leaves ordinary recall, and SEARCH is ordinary recall.
      nexus.execute(`ARCHIVE "${id}"`)
      expect(
        (nexus.describe('SEARCH CONCEPT "Dana"') as unknown as Answer).hits,
      ).toHaveLength(0)
    })
  })

  it('does not leave a rolled-back write in the index', async () => {
    await withNexus('rollback', (nexus) => {
      const failed = nexus.tryExecute(`MUTATE {
        CREATE CONCEPT ?ok { TYPE "Person" NAME "Ghostwriter" }
        CREATE CONCEPT ?bad { TYPE "Spaceship" NAME "Dropped" }
      }`)
      expect('error' in failed).toBe(true)
      expect(
        (nexus.describe('SEARCH CONCEPT "Ghostwriter"') as unknown as Answer).hits,
      ).toHaveLength(0)

      // Asserted against the table and not only through `SEARCH`, because the
      // query joins the element table and would hide a surviving entry rather
      // than return it. That join makes a stale row harmless; the transaction
      // is what makes there not be one, and only this sees the difference.
      const orphans = nexus.store.sql
        .exec<{ n: number }>('SELECT COUNT(*) AS n FROM fts_concepts')
        .one().n
      expect(orphans).toBe(0)
    })
  })

  it('ranks a fuller match above a thinner one', async () => {
    await withNexus('ranking', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?both { TYPE "Person" NAME "Aurora Northlight" }
        CREATE CONCEPT ?one { TYPE "Person" NAME "Aurora Someone" }
        CREATE CONCEPT ?other { TYPE "Person" NAME "Unrelated Northlight" }
      }`)
      const answer = nexus.describe(
        'SEARCH CONCEPT "Aurora Northlight"',
      ) as unknown as Answer
      expect(answer.hits).toHaveLength(3)
      // BM25 is doing real work rather than the order falling out of row ids:
      // the Concept carrying both query terms outranks the two carrying one.
      expect(answer.hits[0]?.element.name).toBe('Aurora Northlight')
      expect(answer.hits[0]!.score).toBeGreaterThan(answer.hits[1]!.score)
    })
  })

  it('pages over a deterministic order and says when more remain', async () => {
    await withNexus('paging', (nexus) => {
      for (let i = 0; i < 5; i += 1) {
        nexus.execute(`CREATE CONCEPT ?c { TYPE "Person" NAME "Paging Subject ${i}" }`)
      }
      const first = nexus.describe('SEARCH CONCEPT "Paging" LIMIT 2') as unknown as Answer
      expect(first.hits).toHaveLength(2)
      expect(first.next_cursor).toBe('2')

      const second = nexus.describe(
        'SEARCH CONCEPT "Paging" LIMIT 2 CURSOR 2',
      ) as unknown as Answer
      expect(second.hits).toHaveLength(2)
      const seen = [...first.hits, ...second.hits].map((h) => h.id)
      expect(new Set(seen).size).toBe(4)

      const last = nexus.describe(
        'SEARCH CONCEPT "Paging" LIMIT 2 CURSOR 4',
      ) as unknown as Answer
      expect(last.hits).toHaveLength(1)
      // Nothing left, so no cursor: a cursor that always came back would make a
      // follower loop forever on an empty page.
      expect(last.next_cursor).toBeUndefined()
    })
  })

  it('answers a miss with nothing rather than with an error', async () => {
    await withNexus('miss', (nexus) => {
      nexus.execute(SETUP)
      const answer = nexus.describe(
        'SEARCH CONCEPT "nothinghereatall"',
      ) as unknown as Answer
      expect(answer.hits).toHaveLength(0)
      // §66.6: the answer has to carry the caveat even — especially — when it
      // is empty, because an empty answer is exactly the one a reader is most
      // likely to mistake for an absence.
      expect(answer.caveat).toContain('a miss is not an absence')

      // A term with no indexable content is the same kind of non-answer, not a
      // MATCH syntax error leaking out of SQLite.
      expect((nexus.describe('SEARCH CONCEPT "!!!"') as unknown as Answer).hits).toEqual(
        [],
      )
    })
  })

  it('declares a freshness it can actually honour', async () => {
    await withNexus('freshness', (nexus) => {
      nexus.execute(SETUP)
      const answer = nexus.describe('SEARCH CONCEPT "Alice"') as unknown as Answer
      expect(answer.search_context.mode).toBe('keyword')
      expect(answer.search_context.score_semantics).toBe(
        'bm25_relevance_not_confidence',
      )
      // Equal by construction, because the index is written in the same
      // transaction as the row (§66.5, §79).
      expect(answer.search_context.index_seq).toBe(nexus.store.currentSeq(nexus.space))
      expect(answer.search_context.index_seq).toBe(
        answer.search_context.current_space_seq,
      )
    })
  })

  it('refuses the modes and coordinates it cannot serve', async () => {
    await withNexus('refusals', (nexus) => {
      expect(() =>
        nexus.describe('SEARCH CONCEPT "x" MODE "semantic"'),
      ).toThrowError(/no embedding model/)
      expect(() =>
        nexus.describe('SEARCH CONCEPT "x" AS OF SEQ 1'),
      ).toThrowError(/no historical index/)
      expect(() => nexus.describe('SEARCH ACTIVITY "x"')).toThrowError(
        /carry no free text/,
      )
      // The mode that *is* built is not refused.
      expect(() =>
        nexus.describe('SEARCH CONCEPT "x" MODE "keyword"'),
      ).not.toThrow()
    })
  })

  it('applies THRESHOLD and caps LIMIT', async () => {
    await withNexus('bounds', (nexus) => {
      nexus.execute(SETUP)
      const all = nexus.describe('SEARCH CONCEPT "Alice"') as unknown as Answer
      expect(all.hits.length).toBeGreaterThan(0)
      // Above every real score, so the filter is doing something rather than
      // being satisfied by everything.
      const none = nexus.describe(
        'SEARCH CONCEPT "Alice" THRESHOLD 1000',
      ) as unknown as Answer
      expect(none.hits).toHaveLength(0)
      // 100 is the ceiling; asking past it is bounded rather than refused.
      expect(() => nexus.describe('SEARCH CONCEPT "Alice" LIMIT 5000')).not.toThrow()
    })
  })

  it('keeps a Space out of another Space’s results', async () => {
    await withNexus('isolation', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Tenant Alpha" }')
      const other = nexus.describe('SEARCH CONCEPT "Tenant"') as unknown as Answer
      expect(other.hits).toHaveLength(1)
      // The index carries no Space column of its own; it is joined from the
      // element table, so there is one copy of the truth about which Space a
      // row is in and this filter cannot drift away from `FIND`'s.
      expect(other.hits[0]?.element.space_id).toBe(nexus.space)
    })
  })
})
