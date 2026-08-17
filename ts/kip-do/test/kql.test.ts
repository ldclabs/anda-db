import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'

/**
 * The read language, tested against the cases the conformance suite pins.
 *
 * `fixtures/kip-conformance-2.0/reads.json` is the contract both engines owe;
 * these are the same cases, run through the real parser so a disagreement about
 * what a command *means* shows up here rather than at cross-engine comparison
 * time.
 */
async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus) => void,
): Promise<void> {
  const stub = env.KIP_DB.getByName(`kql-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    nexus.execute(SETUP)
    body(nexus)
  })
}

/** The `reads` fixture's setup, verbatim. */
const SETUP = `MUTATE {
  CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" SET ATTRIBUTES { display_name: "Alice A" } }
  CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
  CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
  ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
}`

describe('KQL', () => {
  it('finds by type', async () => {
    await withNexus('by-type', (nexus) => {
      expect(
        nexus.query(
          'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.name',
        ),
      ).toEqual(['Alice', 'Bob'])
    })
  })

  it('binds both ends of a tuple and joins on them', async () => {
    await withNexus('tuple', (nexus) => {
      expect(
        nexus.query(
          'FIND(?person.name, ?thing.name) WHERE { ?p PROPOSITION (?person, "prefers", ?thing) }',
        ),
      ).toEqual([['Alice', 'Dark']])
    })
  })

  it('keeps what NOT could not extend, and asks only about the record', async () => {
    await withNexus('not', (nexus) => {
      // Bob having no `prefers` Proposition is Bob having nothing recorded,
      // not a claim that Bob prefers nothing.
      expect(
        nexus.query(`FIND(?c.name) WHERE {
          ?c CONCEPT {type: "Person"}
          NOT { ?p PROPOSITION (?c, "prefers", ?o) }
        }`),
      ).toEqual(['Bob'])
    })
  })

  it('pads with OPTIONAL rather than dropping', async () => {
    await withNexus('optional', (nexus) => {
      expect(
        nexus.query(`FIND(COUNT(?c)) WHERE {
          ?c CONCEPT {type: "Person"}
          OPTIONAL { ?p PROPOSITION (?c, "prefers", ?o) }
        }`),
      ).toEqual([2])
    })
  })

  it('widens with UNION rather than filtering', async () => {
    await withNexus('union', (nexus) => {
      expect(
        nexus.query(`FIND(?c.name) WHERE {
          ?c CONCEPT {name: "Alice"}
          UNION { ?c CONCEPT {name: "Dark"} }
        } ORDER BY ?c.name`),
      ).toEqual(['Alice', 'Dark'])
    })
  })

  it('decides nothing when a comparison crosses types', async () => {
    await withNexus('filter-types', (nexus) => {
      // Both directions are false: an engine that coerced would answer a
      // question nobody asked.
      expect(
        nexus.query(
          'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} FILTER(?c.name > 5) }',
        ),
      ).toEqual([])
      expect(
        nexus.query(
          'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} FILTER(?c.name <= 5) }',
        ),
      ).toEqual([])
    })
  })

  it('sorts nulls last under ASC', async () => {
    await withNexus('nulls-last', (nexus) => {
      // A null is an absent value, not a small one: Bob has no display_name,
      // and sorting him first would put the row that answered nothing above
      // the row that answered.
      expect(
        nexus.query(
          'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.attributes.display_name ASC',
        ),
      ).toEqual(['Alice', 'Bob'])
    })
  })

  it('reads a missing attribute as null, not as an error', async () => {
    await withNexus('missing', (nexus) => {
      expect(
        nexus.query(
          'FIND(?c.attributes.display_name) WHERE { ?c CONCEPT {name: "Bob"} }',
        ),
      ).toEqual([null])
    })
  })

  it('counts nothing as zero, and zero is not a falsehood', async () => {
    await withNexus('count-zero', (nexus) => {
      expect(
        nexus.query('FIND(COUNT(?c)) WHERE { ?c CONCEPT {name: "Nobody"} }'),
      ).toEqual([0])
    })
  })

  it('leaves an archived element out of ordinary recall', async () => {
    await withNexus('archived', (nexus) => {
      const ids = nexus.query('FIND(?c.id) WHERE { ?c CONCEPT {name: "Bob"} }')
      nexus.execute(`ARCHIVE "${ids[0] as string}"`)
      expect(
        nexus.query('FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "Person"} }'),
      ).toEqual([1])
      // That is what archiving *means*: out of recall, still readable when
      // asked for by name.
      expect(
        nexus.query(
          'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", state: "archived"} }',
        ),
      ).toEqual(['Bob'])
    })
  })

  it('projects the element itself and its kind', async () => {
    await withNexus('bare', (nexus) => {
      const [row] = nexus.query(
        'FIND(?c.id, ?c.kind) WHERE { ?c CONCEPT {name: "Bob"} }',
      ) as [string, string][]
      expect(row?.[0]).toMatch(/^C-\d+$/)
      expect(row?.[1]).toBe('concept')
    })
  })

  it('shapes the result by the projection, never as objects', async () => {
    await withNexus('shape', (nexus) => {
      // One expression gives one value per row; several give an array per row.
      // A caller reads by position, so renaming an internal field is not a
      // wire change.
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {name: "Bob"} }'),
      ).toEqual(['Bob'])
      expect(
        nexus.query('FIND(?c.name, ?c.kind) WHERE { ?c CONCEPT {name: "Bob"} }'),
      ).toEqual([['Bob', 'concept']])
    })
  })

  it('joins a reference field to the element it names', async () => {
    await withNexus('reference-join', (nexus) => {
      const [pid] = nexus.query(
        'FIND(?p.id) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }',
      ) as string[]
      nexus.execute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, stance: "support", mode: "stated", confidence: 0.9 }
         }`,
        { p: pid! },
      )
      // The reference has to bind as an element, not as the string "P-1", or
      // this join never matches while naming the same row.
      expect(
        nexus.query(`FIND(?a.confidence) WHERE {
          ?p PROPOSITION (?s, "prefers", ?o)
          ?a ASSERTION {proposition: ?p}
        }`),
      ).toEqual([0.9])
    })
  })

  it('narrows a tuple by an already-bound variable', async () => {
    await withNexus('narrow', (nexus) => {
      expect(
        nexus.query(`FIND(?o.name) WHERE {
          ?alice CONCEPT {name: "Alice"}
          ?p PROPOSITION (?alice, "prefers", ?o)
        }`),
      ).toEqual(['Dark'])
      expect(
        nexus.query(`FIND(?o.name) WHERE {
          ?bob CONCEPT {name: "Bob"}
          ?p PROPOSITION (?bob, "prefers", ?o)
        }`),
      ).toEqual([])
    })
  })

  it('resolves a local type name to its exact symbol on the read side too', async () => {
    await withNexus('symbols', (nexus) => {
      const CM = 'kip://profiles/cognitive-memory@2.0.0'
      expect(
        nexus.query(`FIND(?c.schema_ref) WHERE { ?c CONCEPT {name: "Alice"} }`),
      ).toEqual([`${CM}/Person`])
      // The exact reference selects the same Concepts as the local name.
      expect(
        nexus.query(
          `FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "${CM}/Person"} }`,
        ),
      ).toEqual([2])
    })
  })

  it('applies the filter functions to what a variable actually holds', async () => {
    await withNexus('functions', (nexus) => {
      expect(
        nexus.query(
          'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} FILTER(STARTS_WITH(?c.name, "Al")) }',
        ),
      ).toEqual(['Alice'])
      expect(
        nexus.query(
          'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} FILTER(IS_NULL(?c.attributes.display_name)) }',
        ),
      ).toEqual(['Bob'])
      // An element reference is not a Literal, whatever its text looks like.
      expect(
        nexus.query(
          'FIND(COUNT(?c)) WHERE { ?c CONCEPT {name: "Alice"} FILTER(IS_ELEMENT(?c)) }',
        ),
      ).toEqual([1])
    })
  })

  it('aggregates over the whole solution set', async () => {
    await withNexus('aggregate', (nexus) => {
      expect(
        nexus.query('FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }'),
      ).toEqual([3])
      expect(
        nexus.query('FIND(COUNT(DISTINCT ?c.name)) WHERE { ?c CONCEPT {type: "Person"} }'),
      ).toEqual([2])
      // Grouped aggregation would return one row where the caller asked for
      // one per group, so it is refused rather than silently answered.
      expect(() =>
        nexus.query('FIND(?c.name, COUNT(?c)) WHERE { ?c CONCEPT {} }'),
      ).toThrowError(/grouped aggregation/)
    })
  })

  it('pages over a documented order', async () => {
    await withNexus('paging', (nexus) => {
      const all = nexus.query(
        'FIND(?c.name) WHERE { ?c CONCEPT {} } ORDER BY ?c.name',
      )
      expect(all).toHaveLength(3)
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } ORDER BY ?c.name LIMIT 2'),
      ).toEqual(all.slice(0, 2))
      expect(
        nexus.query(
          'FIND(?c.name) WHERE { ?c CONCEPT {} } ORDER BY ?c.name LIMIT 2 CURSOR "2"',
        ),
      ).toEqual(all.slice(2))
    })
  })

  it('projects a belief now that BELIEF is built', async () => {
    await withNexus('belief', (nexus) => {
      // Covered properly in `projection.test.ts`; here only to keep the
      // "not built yet" list below honest about what is still missing.
      expect(
        nexus.query(
          'FIND(?b.status) WHERE { ?p PROPOSITION (?s, "prefers", ?o) ?b BELIEF (?p) }',
        ),
      ).toEqual(['insufficient'])
    })
  })

  it('refuses the read features it has not built', async () => {
    await withNexus('unsupported', (nexus) => {
      for (const command of [
        'FIND(?o) WHERE { ?p PROPOSITION (?s, "prefers"{1,3}, ?o) }',
      ]) {
        expect(() => nexus.query(command), command).toThrowError(
          /not implemented by this engine yet/,
        )
      }
    })
  })
})
