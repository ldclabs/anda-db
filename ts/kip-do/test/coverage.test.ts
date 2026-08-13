import { env } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { executeTestKip, type TestKipDatabase } from './worker.js'

/**
 * Exercises the KIP surface the README claims to support, so "implemented"
 * means "there is a test" rather than "the code path exists".
 */

let counter = 2000
async function freshStub() {
  const stub = env.KIP_DB.getByName(`cov-${counter++}`)
  await declareSchema(stub)
  return stub
}

async function run(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<any> {
  return executeTestKip(stub, command)
}

async function ok(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<any> {
  const r = await run(stub, command)
  if ('error' in r) {
    throw new Error(`${r.error.code}: ${r.error.message}`)
  }
  return r.result
}

async function err(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<any> {
  const r = await run(stub, command)
  if (!('error' in r)) throw new Error(`expected error, got ${JSON.stringify(r)}`)
  return r.error
}

async function seed(stub: DurableObjectStub<TestKipDatabase>) {
  await ok(
    stub,
    `UPSERT {
       CONCEPT ?h { {type: "Symptom", name: "Headache"} }
       CONCEPT ?f { {type: "Symptom", name: "Fever"} }
       CONCEPT ?a {
         {type: "Drug", name: "Aspirin"}
         SET ATTRIBUTES { risk: 1, tags: ["otc"] }
         SET PROPOSITIONS { ("treats", ?h) ("treats", ?f) }
       }
       CONCEPT ?w {
         {type: "Drug", name: "Warfarin"}
         SET ATTRIBUTES { risk: 5 }
         SET PROPOSITIONS { ("interacts_with", ?a) }
       }
     }`,
  )
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

describe('KQL surface', () => {
  it('binds a predicate variable', async () => {
    const stub = await freshStub()
    await seed(stub)
    const predicates = await ok(
      stub,
      'FIND(?p) WHERE { ?d {type: "Drug", name: "Warfarin"} (?d, ?p, ?o) }',
    )
    expect(predicates).toEqual(['interacts_with'])
  })

  it('matches a predicate alternative', async () => {
    const stub = await freshStub()
    await seed(stub)
    const names = await ok(
      stub,
      `FIND(?d.name)
       WHERE { ?d {type: "Drug"} (?d, "treats"|"interacts_with", ?o) }`,
    )
    expect([...new Set(names)].sort()).toEqual(['Aspirin', 'Warfarin'])
  })

  it('supports UNION of two branches', async () => {
    const stub = await freshStub()
    await seed(stub)
    const names = await ok(
      stub,
      `FIND(?x.name)
       WHERE {
         ?x {type: "Drug", name: "Aspirin"}
         UNION { ?x {type: "Symptom", name: "Fever"} }
       }`,
    )
    expect(names.sort()).toEqual(['Aspirin', 'Fever'])
  })

  it('binds a nested proposition endpoint (meta-statement)', async () => {
    const stub = await freshStub()
    await seed(stub)
    // Assert something *about* the (Aspirin, treats, Headache) link.
    await ok(
      stub,
      `UPSERT {
         CONCEPT ?src { {type: "Source", name: "PubMed"} }
         PROPOSITION ?cite {
           (({type:"Drug",name:"Aspirin"}, "treats", {type:"Symptom",name:"Headache"}), "cited_by", ?src)
           SET ATTRIBUTES { year: 2011 }
         }
       }`,
    )
    const cites = await ok(
      stub,
      `FIND(?p)
       WHERE { ?p (({type:"Drug",name:"Aspirin"}, "treats", {type:"Symptom",name:"Headache"}), "cited_by", ?s) }`,
    )
    expect(cites).toHaveLength(1)
    // The subject is a link address, not a concept address.
    expect(cites[0].subject).toMatch(/^P:\d+:treats$/)
    expect(cites[0].attributes.year).toBe(2011)
  })

  it('aggregates with DISTINCT', async () => {
    const stub = await freshStub()
    await seed(stub)
    const distinct = await ok(
      stub,
      'FIND(COUNT(DISTINCT ?d)) WHERE { ?d {type: "Drug"} (?d, ?p, ?o) }',
    )
    expect(distinct).toBe(2)
  })

  it('combines comparison operators including <= and >=', async () => {
    const stub = await freshStub()
    await seed(stub)
    const le = await ok(
      stub,
      'FIND(?d.name) WHERE { ?d {type: "Drug"} FILTER(?d.attributes.risk <= 1) }',
    )
    expect(le).toEqual(['Aspirin'])
    const ge = await ok(
      stub,
      'FIND(?d.name) WHERE { ?d {type: "Drug"} FILTER(?d.attributes.risk >= 5) }',
    )
    expect(ge).toEqual(['Warfarin'])
  })

  it('matches a concept by literal id', async () => {
    const stub = await freshStub()
    await seed(stub)
    const all = await ok(stub, 'FIND(?d) WHERE { ?d {type: "Drug", name: "Aspirin"} }')
    const id = all[0].id
    const byId = await ok(stub, `FIND(?d.name) WHERE { ?d {id: "${id}"} }`)
    expect(byId).toEqual(['Aspirin'])
  })
})

describe('KML surface', () => {
  it('UPDATE applies arithmetic expressions over the element’s own fields', async () => {
    const stub = await freshStub()
    await ok(
      stub,
      'UPSERT { CONCEPT ?c { {type: "T", name: "A"} SET ATTRIBUTES {score: 0.8} } }',
    )
    const result = await ok(
      stub,
      `UPDATE ?c
       SET ATTRIBUTES { score: CLAMP(MUL(?c.attributes.score, 0.5), 0.0, 1.0) }
       WHERE { ?c {type: "T", name: "A"} }`,
    )
    expect(result.matched).toBe(1)
    expect(result.updated).toBe(1)
    const found = await ok(stub, 'FIND(?c) WHERE { ?c {type: "T", name: "A"} }')
    expect(found[0].attributes.score).toBeCloseTo(0.4)
    expect(found[0].metadata._version).toBe(2)
  })

  it('MERGE repoints links and records provenance', async () => {
    const stub = await freshStub()
    await ok(
      stub,
      `UPSERT {
         CONCEPT ?s { {type: "Symptom", name: "Headache"} }
         CONCEPT ?a { {type: "Drug", name: "ASA"} SET PROPOSITIONS { ("treats", ?s) } }
         CONCEPT ?b { {type: "Drug", name: "Aspirin"} }
       }`,
    )
    const result = await ok(
      stub,
      `MERGE CONCEPT ?src INTO ?dst
       WHERE {
         ?src {type: "Drug", name: "ASA"}
         ?dst {type: "Drug", name: "Aspirin"}
       }`,
    )
    expect(result.merged).toBe(true)

    // The source is gone.
    const remaining = await ok(stub, 'FIND(?d.name) WHERE { ?d {type: "Drug"} }')
    expect(remaining).toEqual(['Aspirin'])

    // Its edge now hangs off the target.
    const links = await ok(stub, 'FIND(?p) WHERE { ?p (?d, "treats", ?s) }')
    expect(links).toHaveLength(1)

    const target = await ok(
      stub,
      'FIND(?d) WHERE { ?d {type: "Drug", name: "Aspirin"} }',
    )
    expect(target[0].attributes.aliases).toContain('ASA')
    expect(target[0].metadata._merged_from).toHaveLength(1)
  })

  it('MERGE refuses operands of different types', async () => {
    const stub = await freshStub()
    await ok(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "Drug", name: "X"} }
         CONCEPT ?b { {type: "Herb", name: "Y"} }
       }`,
    )
    const error = await err(
      stub,
      `MERGE CONCEPT ?src INTO ?dst
       WHERE { ?src {type: "Drug", name: "X"} ?dst {type: "Herb", name: "Y"} }`,
    )
    // Merging across types is a constraint violation, not a reference error.
    expect(error.code).toBe('KIP_2002')
    expect(error.message).toMatch(/types differ/)
  })

  it('DELETE PROPOSITIONS removes one link and keeps siblings', async () => {
    const stub = await freshStub()
    await seed(stub)
    const before = await ok(stub, 'FIND(?p) WHERE { ?p (?d, "treats", ?s) }')
    expect(before).toHaveLength(2)

    const result = await ok(
      stub,
      `DELETE PROPOSITIONS ?p
       WHERE { ?p (?d, "treats", ?s) ?s {name: "Fever"} }`,
    )
    expect(result.deleted_propositions).toBe(1)

    const after = await ok(stub, 'FIND(?p) WHERE { ?p (?d, "treats", ?s) }')
    expect(after).toHaveLength(1)
  })

  it('DELETE METADATA rejects the reserved namespace', async () => {
    const stub = await freshStub()
    await seed(stub)
    const error = await err(
      stub,
      'DELETE METADATA {"_version"} FROM ?d WHERE { ?d {type: "Drug"} }',
    )
    expect(error.code).toBe('KIP_2002')
  })
})

describe('META surface', () => {
  it('DESCRIBE PRIMER advertises keyword-only search and the grammar version', async () => {
    const stub = await freshStub()
    const primer = await ok(stub, 'DESCRIBE PRIMER')
    expect(primer.search_modes).toEqual(['keyword'])
    expect(primer.parser_version).toMatch(/^\d+\.\d+\.\d+$/)
  })

  it('DESCRIBE type listings paginate', async () => {
    const stub = await freshStub()
    await ok(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "$ConceptType", name: "Drug"} }
         CONCEPT ?b { {type: "$ConceptType", name: "Symptom"} }
       }`,
    )
    // The listing includes the types the bundled capsules declare, so assert
    // on membership and ordering rather than on an exact set.
    const all = await ok(stub, 'DESCRIBE CONCEPT TYPES')
    expect(all).toContain('Drug')
    expect(all).toContain('Symptom')
    expect([...all]).toEqual([...all].sort())
    const first = await run(stub, 'DESCRIBE CONCEPT TYPES LIMIT 1')
    expect(first.result).toEqual([all[0]])
    expect(first.next_cursor).toBe('1')

    const second = await run(
      stub,
      `DESCRIBE CONCEPT TYPES LIMIT 1 CURSOR "${first.next_cursor}"`,
    )
    expect(second.result).toEqual([all[1]])
    expect(second.next_cursor).toBe('2')
  })

  it('reports EXPORT as unimplemented rather than answering wrongly', async () => {
    const stub = await freshStub()
    const error = await err(stub, 'EXPORT ?c WHERE { ?c {type: "T"} }')
    expect(error.code).toBe('KIP_4003')
    expect(error.message).toMatch(/not implemented/i)
  })

  it('reports grouped aggregation as unimplemented', async () => {
    const stub = await freshStub()
    await seed(stub)
    const error = await err(
      stub,
      'FIND(?d.name, COUNT(?s)) WHERE { ?d {type: "Drug"} (?d, "treats", ?s) }',
    )
    expect(error.code).toBe('KIP_3001')
    expect(error.message).toMatch(/grouping key/)
  })
})

describe('schema-definition protection', () => {
  it('allows defining a type but refuses to delete or merge one', async () => {
    const stub = await freshStub()
    // Defining a type is an ordinary UPSERT — this is how a schema is seeded.
    await ok(
      stub,
      `UPSERT {
         CONCEPT ?t { {type: "$ConceptType", name: "Drug"} SET ATTRIBUTES {desc: "a drug"} }
         CONCEPT ?u { {type: "$ConceptType", name: "Herb"} }
       }`,
    )
    const defined = await ok(stub, 'DESCRIBE CONCEPT TYPE "Drug"')
    expect(defined.attributes.desc).toBe('a drug')

    // Removing it would invalidate every instance that depends on it.
    const deleteError = await err(
      stub,
      'DELETE CONCEPT ?t DETACH WHERE { ?t {type: "$ConceptType", name: "Drug"} }',
    )
    expect(deleteError.code).toBe('KIP_3004')

    const mergeError = await err(
      stub,
      `MERGE CONCEPT ?a INTO ?b
       WHERE {
         ?a {type: "$ConceptType", name: "Drug"}
         ?b {type: "$ConceptType", name: "Herb"}
       }`,
    )
    expect(mergeError.code).toBe('KIP_3004')
  })
})
