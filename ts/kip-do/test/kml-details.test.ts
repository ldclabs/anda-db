import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { parseElementId } from '../src/id.js'
import type { SchemaPackage } from '../src/schema/index.js'

const PKG = {
  format: 'KIP-Schema-Package',
  manifest: { package_id: 'kip://test/kml-detail', version: '1.0.0' },
  definitions: { concept_types: {
    Counter: { kind: 'ConceptType', attributes: { open: true } },
    Other: { kind: 'ConceptType' },
  }, structural_fields: {
    links: { kind: 'StructuralFieldDefinition', source: {kinds: ['Concept']}, target: {kinds: ['Concept']}, ordered: true },
    primary: { kind: 'StructuralFieldDefinition', source: {kinds: ['Concept']}, target: {kinds: ['Concept']}, cardinality: {max: 1} },
  }, facets: { Numeric: {kind: 'FacetDefinition', fields: {count: {type: 'number'}}} } },
} as unknown as SchemaPackage

async function withNexus(name: string, run: (n: CognitiveNexus) => void) {
  await runInDurableObject(env.KIP_DB.getByName(`kml-detail-${name}`), (_, state) => {
    const n = CognitiveNexus.connect(state.storage)
    n.activatePackages([PKG])
    run(n)
  })
}

function row(n: CognitiveNexus, id: string) {
  const element = n.store.load(parseElementId(id))
  if (element?.kind !== 'Concept') throw new Error('expected Concept')
  return element.row
}

describe('KML specification details', () => {
  it('reads an identical resend as no_effect whatever order its nested keys were written in', async () => {
    await withNexus('resend-no-effect', (n) => {
      // Stored rows decode with canonical key order; the command writes
      // `theme` before `lang`. An insertion-order comparison saw a change here
      // on every resend and bumped the version each time.
      const upsert = `UPSERT CONCEPT ?c { MATCH {type: "Counter", key: "ada"}
        SET ATTRIBUTES { prefs: {theme: "dark", lang: "en"} } }`
      expect(n.execute(upsert).status).toBe('committed')
      const version = row(n, 'C-1').version
      expect(n.execute(upsert).status).toBe('no_effect')
      expect(n.execute(`UPDATE "C-1" SET ATTRIBUTES { prefs: {theme: "dark", lang: "en"} }`).status)
        .toBe('no_effect')
      expect(n.execute(`UPDATE "C-1" SET FACET "Numeric" {count: 1}`).status).toBe('committed')
      expect(n.execute(`UPDATE "C-1" SET FACET "Numeric" {count: 1}`).status).toBe('no_effect')
      expect(row(n, 'C-1').version).toBe(version + 1)
      expect(n.execute(`UPDATE "C-1" SET ATTRIBUTES { prefs: {theme: "light", lang: "en"} }`).status)
        .toBe('committed')
    })
  })

  it('skips null/non-numeric numeric-expression keys and shallow-merges literal values', async () => {
    await withNexus('skip', (n) => {
      n.execute(`CREATE CONCEPT ?c { TYPE "Counter" SET ATTRIBUTES {
        count: 2, text: "hello", keep: true, bag: {old: 1, kept: 2}, list: [1, 2]
      } }`)
      n.execute(`UPDATE ?c SET ATTRIBUTES {
        missing: ADD(?c.attributes.missing, 1),
        count: ADD(?c.attributes.text, 1),
        text: COALESCE(?c.attributes.text, 9),
        fallback: COALESCE(?c.attributes.missing, 3),
        explicit: null, bag: {new: 3}, list: [4]
      } WHERE { ?c CONCEPT {id: "C-1"} }`)
      expect(row(n, 'C-1').attributes).toEqual({
        count: 2, text: 'hello', keep: true, bag: {new: 3}, list: [4], fallback: 3, explicit: null,
      })
      n.execute('UPDATE "C-1" UNSET ATTRIBUTES {explicit}')
      expect(Object.hasOwn(row(n, 'C-1').attributes, 'explicit')).toBe(false)
    })
  })

  it('deduplicates targets before LIMIT and reads one pre-update target view', async () => {
    await withNexus('dedup', (n) => {
      n.execute(`MUTATE {
        CREATE CONCEPT ?a { TYPE "Counter" SET ATTRIBUTES {count: 2} }
        CREATE CONCEPT ?b { TYPE "Counter" SET ATTRIBUTES {count: 5} }
      }`)
      n.execute(`UPDATE ?c SET ATTRIBUTES {count: ADD(?c.attributes.count, 1)}
        SET ATTRIBUTES {double: MUL(?c.attributes.count, 2)}
        WHERE { ?c CONCEPT {type: "Counter"} ?join CONCEPT {type: "Counter"} } LIMIT 2`)
      expect(row(n, 'C-1').attributes).toEqual({ count: 3, double: 4 })
      expect(row(n, 'C-2').attributes).toEqual({ count: 6, double: 10 })
      const version = row(n, 'C-1').version
      expect(n.execute(`UPDATE "C-1" SET FIELDS {name: "changed"}
        WHERE { ?c CONCEPT {type: "Counter"} } LIMIT 0`).status).toBe('no_effect')
      expect(row(n, 'C-1').version).toBe(version)
    })
  })

  it('rolls back the complete statement on unsafe arithmetic, underflow or invalid CLAMP', async () => {
    await withNexus('numbers', (n) => {
      n.execute(`MUTATE {
        CREATE CONCEPT ?a { TYPE "Counter" SET ATTRIBUTES {count: 2} }
        CREATE CONCEPT ?b { TYPE "Counter" SET ATTRIBUTES {count: 9007199254740991} }
      }`)
      expect(() => n.execute(`UPDATE ?c SET FIELDS {name: "changed"}
        SET ATTRIBUTES {count: ADD(?c.attributes.count, 1)}
        WHERE { ?c CONCEPT {type: "Counter"} }`)).toThrowError(/safe integral/)
      expect(row(n, 'C-1').attributes.count).toBe(2)
      expect(row(n, 'C-1').name).toBe('')
      expect(row(n, 'C-1').version).toBe(1)
      for (const expression of ['MUL(1e-300, 1e-100)', 'CLAMP(1, 3, 2)']) {
        expect(() => n.execute(`UPDATE ?c SET ATTRIBUTES {count: ${expression}}
          WHERE { ?c CONCEPT {id: "C-1"} }`)).toThrowError()
        expect(row(n, 'C-1').attributes.count).toBe(2)
      }
    })
  })

  it('replaces empty UPSERT fields and never falls back from an id selector', async () => {
    await withNexus('upsert-empty', (n) => {
      n.execute('CREATE CONCEPT ?c { TYPE "Counter" NAME "Before" SET FIELDS {key: "counter", aliases: ["old"]} }')
      n.execute('UPSERT CONCEPT ?c { MATCH {id: "C-1"} SET FIELDS {name: "", aliases: []} }')
      expect(row(n, 'C-1').name).toBe('')
      expect(row(n, 'C-1').aliases).toEqual([])
      expect(() => n.execute('UPSERT CONCEPT ?c { MATCH {id: "C-999", type: "Counter", key: "counter"} SET FIELDS {name: "wrong"} }')).toThrowError()
      expect(() => n.execute('UPSERT CONCEPT ?c { MATCH {id: "C-1", type: "Other", key: "counter"} SET FIELDS {name: "wrong"} }')).toThrowError()
      expect(() => n.execute('UPSERT CONCEPT ?c { MATCH {id: "C-1", name: "wrong"} SET FIELDS {name: "wrong"} }')).toThrowError()
      expect(() => n.execute('UPSERT CONCEPT ?c { MATCH {id: "C-1", key: "wrong"} SET FIELDS {name: "wrong"} }')).toThrowError()
      expect(row(n, 'C-1').name).toBe('')
      const version = row(n, 'C-1').version
      expect(() => n.execute('UPDATE "C-1" SET ATTRIBUTES {temporary: 1} UNSET ATTRIBUTES {temporary}')).toThrowError()
      expect(row(n, 'C-1').version).toBe(version)
    })
  })

  it('freezes forward block-output reads across UPDATE, NOT and UNION', async () => {
    await withNexus('output-scope', (n) => {
      n.execute('CREATE CONCEPT ?c { TYPE "Counter" }')
      n.execute(`MUTATE {
    UPDATE ?gate SET FIELDS {name: "Changed"} WHERE { FILTER(?gate.name == "Gate") }
    UPDATE ?target SET ATTRIBUTES {selected: true} WHERE {
      ?target CONCEPT {id: "C-1"} FILTER(?gate.name == "Gate")
      NOT { FILTER(?gate.name == "Stop") }
    }
    UPDATE ?target SET ATTRIBUTES {union_seen: true} WHERE {
      ?target CONCEPT {id: "C-1"} FILTER(?gate.name == "Wrong")
      UNION { ?target CONCEPT {id: "C-1"} FILTER(?gate.name == "Gate") }
    }
    UPDATE ?target SET ATTRIBUTES {activity_seen: true} WHERE {
      ?target CONCEPT {id: "C-1"} FILTER(?run.activity_class == "consolidation")
    }
    CREATE CONCEPT ?gate { TYPE "Counter" NAME "Gate" }
    CREATE ACTIVITY ?run { SET FIELDS {activity_class: "consolidation"} }
}`)
      expect(row(n, 'C-1').attributes).toEqual({ selected: true, union_seen: true, activity_seen: true })
      expect(row(n, 'C-2').name).toBe('Changed')
      expect(() => n.execute('UPDATE ?gate SET FIELDS {name: "leaked"}')).toThrowError()
    })
  })

  it('rejects opposing structural edge actions but allows replacing a single reference', async () => {
    await withNexus('structural-conflicts', (n) => {
      n.execute(`MUTATE { CREATE CONCEPT ?a {TYPE "Counter"} CREATE CONCEPT ?b {TYPE "Counter"} CREATE CONCEPT ?c {TYPE "Counter"} }`)
      const add = 'UPDATE "C-1" SET STRUCTURAL { ("links", "C-2") }'
      const remove = 'UPDATE "C-1" UNSET STRUCTURAL { ("links", "C-2") }'
      for (const plan of [`MUTATE { ${add} ${remove} }`, `MUTATE { ${remove} ${add} }`]) {
        expect(() => n.execute(plan)).toThrowError(/different final|both assigned/)
        expect(row(n, 'C-1').structural).toEqual({})
      }
      n.execute(`MUTATE {
        UPDATE "C-1" SET STRUCTURAL { ("links", "C-2") {index: 0} }
        UPDATE "C-1" SET STRUCTURAL { ("links", "C-2") {index: 0} }
      }`)
      n.execute('UPDATE "C-1" SET STRUCTURAL { ("primary", "C-2") }')
      n.execute(`UPDATE "C-1" SET STRUCTURAL { ("primary", "C-3") }
        UNSET STRUCTURAL { ("primary", "C-2") }`)
      expect(row(n, 'C-1').structural['kip://test/kml-detail@1.0.0/primary']).toEqual([{id:'C-3'}])
      const version = row(n, 'C-1').version
      expect(n.execute(`UPDATE ?c SET FACET "Numeric" {count: ADD(?c.attributes.missing, 1)}
        WHERE {?c CONCEPT {id: "C-1"}}`).status).toBe('no_effect')
      expect(row(n, 'C-1').facets).toEqual({})
      expect(row(n, 'C-1').version).toBe(version)
    })
  })

  it('archives a newly formed Concept through its forward output handle', async () => {
    await withNexus('new-concept-archive', (n) => {
      n.execute('CREATE CONCEPT ?old {TYPE "Counter" NAME "Kept"}')
      n.execute(`MUTATE {
        TRANSITION ?fresh TO "archived" WHERE {FILTER(?fresh.name == "New")}
        CREATE CONCEPT ?fresh {TYPE "Counter" NAME "New"}
      }`)
      expect(row(n, 'C-2').state).toBe('archived')
      expect(row(n, 'C-2').version).toBe(1)
      expect(row(n, 'C-1').state).toBe('active')
      expect(n.query('FIND(?c.name) WHERE {?c CONCEPT {type: "Counter"}}')).toEqual(['Kept'])
      expect(n.query('FIND(?c.name) WHERE {?c CONCEPT {id: "C-2", state: "archived"}}')).toEqual(['New'])
    })
  })

  it('treats self/canonical merge retries as no effect and refuses incompatible or ambiguous endpoints', async () => {
    await withNexus('merge', (n) => {
      n.execute(`MUTATE {
        CREATE CONCEPT ?a { TYPE "Counter" }
        CREATE CONCEPT ?b { TYPE "Counter" }
        CREATE CONCEPT ?c { TYPE "Counter" }
        CREATE CONCEPT ?d { TYPE "Other" }
      }`)
      expect(n.execute('MERGE CONCEPT "C-1" INTO "C-1"').status).toBe('no_effect')
      expect(() => n.execute('MERGE CONCEPT "C-1" INTO "C-4"')).toThrowError(/incompatible/)
      expect(() => n.execute(`MERGE CONCEPT ?source INTO "C-3"
        WHERE { ?source CONCEPT {type: "Counter"} }`)).toThrowError(/exactly one/)
      expect(() => n.execute(`MERGE CONCEPT ?source INTO "C-3"
        WHERE { ?source CONCEPT {name: "absent"} }`)).toThrowError(/visible/)
      n.execute('MERGE CONCEPT "C-1" INTO "C-2"')
      n.execute('MERGE CONCEPT "C-2" INTO "C-3"')
      const version = row(n, 'C-1').version
      expect(n.execute('MERGE CONCEPT "C-1" INTO "C-3"').status).toBe('no_effect')
      expect(row(n, 'C-1').version).toBe(version)
      expect(row(n, 'C-1').merged_into).toBe('C-2')
    })
  })
})
