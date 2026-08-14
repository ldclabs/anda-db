import { env, evictDurableObject, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { BOOTSTRAP_VERSION_KEY } from '../src/durable-object.js'
import { BOOTSTRAP_VERSION } from '../src/nexus.js'
import { SCHEMA_VERSION } from '../src/schema.js'
import { executeTestKip, type TestKipDatabase } from './worker.js'

/**
 * These run inside workerd against a real SQLite-backed Durable Object, not a
 * Node SQLite shim. The engine's contract *is* the platform's — transactionSync,
 * FTS5, the 100-bound-parameter ceiling — so anything less than workerd would
 * test a different system.
 */

let counter = 0
/** Fresh database per test; ids and versions are asserted absolutely. */
async function freshStub() {
  const stub = env.KIP_DB.getByName(`db-${counter++}`)
  await declareSchema(stub)
  return stub
}

async function exec(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<any> {
  const response = await executeTestKip(stub, command)
  return response
}

async function expectOk(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<any> {
  const response = await exec(stub, command)
  if ('error' in response) {
    throw new Error(
      `expected success but got ${response.error.code}: ${response.error.message}`,
    )
  }
  return response.result
}

async function expectError(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<{ code: string; name: string; message: string; hint: string }> {
  const response = await exec(stub, command)
  if (!('error' in response)) {
    throw new Error(`expected an error but got ${JSON.stringify(response)}`)
  }
  return response.error
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

describe('schema', () => {
  it('creates its tables and skips full bootstrap after eviction', async () => {
    const stub = await freshStub()
    await expectOk(stub, 'DESCRIBE PRIMER')
    await runInDurableObject(stub, async (_instance, state) => {
      const tables = state.storage.sql
        .exec<{ name: string }>(
          "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        )
        .toArray()
        .map((r) => r.name)
      expect(tables).toContain('concepts')
      expect(tables).toContain('propositions')
      expect(tables).toContain('proposition_links')
      expect(tables).toContain('concepts_fts')
      expect(tables).toContain('kip_meta')

      expect(await state.storage.get(BOOTSTRAP_VERSION_KEY)).toBe(
        BOOTSTRAP_VERSION,
      )
      state.storage.sql.exec(
        "UPDATE kip_meta SET v = 'test-stale' WHERE k LIKE 'capsule_hash:%'",
      )
      state.storage.sql.exec(
        "UPDATE kip_meta SET v = 'test-stale' WHERE k = 'schema_version'",
      )
    })

    await evictDurableObject(stub)
    await expectOk(stub, 'DESCRIBE PRIMER')

    await runInDurableObject(stub, (_instance, state) => {
      const stale = state.storage.sql
        .exec<{ count: number }>(
          `SELECT count(*) AS count FROM kip_meta
             WHERE k LIKE 'capsule_hash:%' AND v = 'test-stale'`,
        )
        .toArray()[0]
      // The deliberately stale per-capsule markers prove that neither DDL nor
      // the capsule loop ran merely because the object was constructed again.
      expect(stale?.count).toBeGreaterThan(0)
      const schemaVersion = state.storage.sql
        .exec<{ v: string }>(
          "SELECT v FROM kip_meta WHERE k = 'schema_version'",
        )
        .toArray()[0]
      expect(schemaVersion?.v).toBe('test-stale')

      const foreignKeys = state.storage.sql
        .exec<{ foreign_keys: number }>('PRAGMA foreign_keys')
        .toArray()[0]
      expect(foreignKeys?.foreign_keys).toBe(1)
    })
  })

  it('re-runs bootstrap when its persistent version is stale', async () => {
    const stub = await freshStub()
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE kip_meta SET v = 'test-stale' WHERE k LIKE 'capsule_hash:%'",
      )
      state.storage.sql.exec(
        "UPDATE kip_meta SET v = 'test-stale' WHERE k = 'schema_version'",
      )
      await state.storage.put(BOOTSTRAP_VERSION_KEY, 'previous-version')
    })

    await evictDurableObject(stub)
    await expectOk(stub, 'DESCRIBE PRIMER')

    await runInDurableObject(stub, async (_instance, state) => {
      expect(await state.storage.get(BOOTSTRAP_VERSION_KEY)).toBe(
        BOOTSTRAP_VERSION,
      )
      const stale = state.storage.sql
        .exec<{ count: number }>(
          `SELECT count(*) AS count FROM kip_meta
             WHERE k LIKE 'capsule_hash:%' AND v = 'test-stale'`,
        )
        .toArray()[0]
      expect(stale?.count).toBe(0)
      const schemaVersion = state.storage.sql
        .exec<{ v: string }>(
          "SELECT v FROM kip_meta WHERE k = 'schema_version'",
        )
        .toArray()[0]
      expect(schemaVersion?.v).toBe(String(SCHEMA_VERSION))
    })
  })

  it('migrates the proposition search index from schema v1', async () => {
    const stub = await freshStub()
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec('DROP TABLE propositions_fts')
      state.storage.sql.exec(
        `CREATE VIRTUAL TABLE propositions_fts
           USING fts5(tokens, tokenize = 'ascii')`,
      )
      state.storage.sql.exec(
        "UPDATE kip_meta SET v = '1' WHERE k = 'schema_version'",
      )
      await state.storage.put(BOOTSTRAP_VERSION_KEY, 'schema-v1')
    })

    await evictDurableObject(stub)
    await expectOk(stub, 'DESCRIBE PRIMER')

    await runInDurableObject(stub, (_instance, state) => {
      const columns = state.storage.sql
        .exec<{ name: string }>('PRAGMA table_info(propositions_fts)')
        .toArray()
        .map((column) => column.name)
      expect(columns).toEqual(['prop_id', 'predicate', 'tokens'])
    })
  })

  it('refuses to reuse a concept id after deletion', async () => {
    // AUTOINCREMENT is load-bearing: proposition endpoints reference concepts
    // by the string "C:<id>", so a reused id would silently graft a new
    // concept onto a deleted one's edges.
    const stub = await freshStub()
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?a { {type: "Drug", name: "A"} SET ATTRIBUTES {x: 1} } }',
    )
    const before = await expectOk(stub, 'FIND(?c) WHERE { ?c {type: "Drug"} }')
    const firstId = before[0].id

    await expectOk(stub, 'DELETE CONCEPT ?c DETACH WHERE { ?c {type: "Drug", name: "A"} }')
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?b { {type: "Drug", name: "B"} SET ATTRIBUTES {x: 2} } }',
    )
    const after = await expectOk(stub, 'FIND(?c) WHERE { ?c {type: "Drug"} }')
    expect(after[0].id).not.toBe(firstId)
  })
})

describe('KML: UPSERT', () => {
  it('creates a concept and initializes engine metadata', async () => {
    const stub = await freshStub()
    const result = await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?aspirin {
           {type: "Drug", name: "Aspirin"}
           SET ATTRIBUTES { molecular_formula: "C9H8O4", risk_level: 1 }
         }
       }`,
    )
    expect(result.blocks).toBe(1)
    expect(result.upsert_concept_nodes).toHaveLength(1)

    const found = await expectOk(
      stub,
      'FIND(?d) WHERE { ?d {type: "Drug", name: "Aspirin"} }',
    )
    expect(found).toHaveLength(1)
    expect(found[0].name).toBe('Aspirin')
    expect(found[0].attributes.molecular_formula).toBe('C9H8O4')
    expect(found[0].metadata._version).toBe(1)
    expect(typeof found[0].metadata._created_at).toBe('string')
  })

  it('shallow-merges attributes and bumps _version on re-upsert', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?d { {type: "Drug", name: "Aspirin"} SET ATTRIBUTES {a: 1, b: 2} } }',
    )
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?d { {type: "Drug", name: "Aspirin"} SET ATTRIBUTES {b: 3, c: 4} } }',
    )
    const found = await expectOk(
      stub,
      'FIND(?d) WHERE { ?d {type: "Drug", name: "Aspirin"} }',
    )
    // `a` survives: UPSERT is additive, not a replacement.
    expect(found[0].attributes).toEqual({ a: 1, b: 3, c: 4 })
    expect(found[0].metadata._version).toBe(2)
  })

  it('creates propositions through SET PROPOSITIONS', async () => {
    const stub = await freshStub()
    const result = await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?headache { {type: "Symptom", name: "Headache"} }
         CONCEPT ?aspirin {
           {type: "Drug", name: "Aspirin"}
           SET PROPOSITIONS { ("treats", ?headache) }
         }
       }`,
    )
    expect(result.upsert_concept_nodes).toHaveLength(2)

    const links = await expectOk(
      stub,
      'FIND(?p) WHERE { ?p (?d, "treats", ?s) }',
    )
    expect(links).toHaveLength(1)
    expect(links[0].predicate).toBe('treats')
    expect(links[0].subject).toMatch(/^C:\d+$/)
  })

  it('rejects self-loops', async () => {
    const stub = await freshStub()
    await expectOk(stub, 'UPSERT { CONCEPT ?a { {type: "T", name: "A"} } }')
    const error = await expectError(
      stub,
      `UPSERT {
         PROPOSITION ?p {
           ({type: "T", name: "A"}, "rel", {type: "T", name: "A"})
         }
       }`,
    )
    // A self-loop is a malformed statement, not a dangling reference.
    expect(error.code).toBe('KIP_1001')
    expect(error.message).toContain('cannot be the same')
  })

  it('rejects writes to the engine-reserved metadata namespace', async () => {
    const stub = await freshStub()
    const error = await expectError(
      stub,
      'UPSERT { CONCEPT ?a { {type: "T", name: "A"} SET ATTRIBUTES {_version: 99} } }',
    )
    expect(error.code).toBe('KIP_2002')
  })
})

describe('KML: EXPECT VERSION', () => {
  it('passes when the version matches and fails otherwise', async () => {
    const stub = await freshStub()
    // Version 0 means "must not exist yet".
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a {
           {type: "T", name: "A"}
           EXPECT VERSION 0
           SET ATTRIBUTES {v: 1}
         }
       }`,
    )
    const conflict = await expectError(
      stub,
      `UPSERT {
         CONCEPT ?a {
           {type: "T", name: "A"}
           EXPECT VERSION 0
           SET ATTRIBUTES {v: 2}
         }
       }`,
    )
    expect(conflict.code).toBe('KIP_3005')
    expect(conflict.name).toBe('VersionConflict')

    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a {
           {type: "T", name: "A"}
           EXPECT VERSION 1
           SET ATTRIBUTES {v: 2}
         }
       }`,
    )
  })

  it('updates a proposition addressed by id and enforces its version', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "T", name: "A"} }
         CONCEPT ?b { {type: "T", name: "B"} }
         PROPOSITION ?p {
           (?a, "rel", ?b)
           SET ATTRIBUTES {old: 1}
         }
         WITH METADATA {source: "original"}
       }`,
    )
    const [created] = await expectOk(
      stub,
      'FIND(?p) WHERE { ?p (?a, "rel", ?b) }',
    )

    await expectOk(
      stub,
      `UPSERT {
         PROPOSITION ?p {
           (id: "${created.id}")
           EXPECT VERSION 1
           SET ATTRIBUTES {new: 2}
         }
         WITH METADATA {source: "id-update"}
       }`,
    )
    const [updated] = await expectOk(
      stub,
      'FIND(?p) WHERE { ?p (?a, "rel", ?b) }',
    )
    expect(updated.attributes).toEqual({ old: 1, new: 2 })
    expect(updated.metadata.source).toBe('id-update')
    expect(updated.metadata._version).toBe(2)

    const conflict = await expectError(
      stub,
      `UPSERT {
         PROPOSITION ?p {
           (id: "${created.id}")
           EXPECT VERSION 1
           SET ATTRIBUTES {new: 3}
         }
       }`,
    )
    expect(conflict.code).toBe('KIP_3005')

    const [unchanged] = await expectOk(
      stub,
      'FIND(?p) WHERE { ?p (?a, "rel", ?b) }',
    )
    expect(unchanged.attributes.new).toBe(2)
    expect(unchanged.metadata._version).toBe(2)
  })
})

describe('atomicity', () => {
  it('rolls the whole statement back when a later block fails', async () => {
    // This is the property the Rust engine cannot offer: it documents that a
    // failed multi-block UPSERT "may leave a prefix of its blocks applied".
    const stub = await freshStub()
    const error = await expectError(
      stub,
      `UPSERT {
         CONCEPT ?good { {type: "T", name: "Good"} SET ATTRIBUTES {ok: true} }
         CONCEPT ?bad  { {type: "T", name: "Bad"}  SET ATTRIBUTES {_version: 1} }
       }`,
    )
    expect(error.code).toBe('KIP_2002')

    const survivors = await expectOk(stub, 'FIND(?c) WHERE { ?c {type: "T"} }')
    expect(survivors).toEqual([])
  })
})

describe('KQL', () => {
  async function seed(stub: DurableObjectStub<TestKipDatabase>) {
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?headache { {type: "Symptom", name: "Headache"} }
         CONCEPT ?fever    { {type: "Symptom", name: "Fever"} }
         CONCEPT ?aspirin {
           {type: "Drug", name: "Aspirin"}
           SET ATTRIBUTES { risk_level: 1 }
           SET PROPOSITIONS { ("treats", ?headache) ("treats", ?fever) }
         }
         CONCEPT ?warfarin {
           {type: "Drug", name: "Warfarin"}
           SET ATTRIBUTES { risk_level: 5 }
           SET PROPOSITIONS { ("treats", ?fever) }
         }
       }`,
    )
  }

  it('finds concepts by type', async () => {
    const stub = await freshStub()
    await seed(stub)
    const drugs = await expectOk(stub, 'FIND(?d.name) WHERE { ?d {type: "Drug"} }')
    expect(drugs.sort()).toEqual(['Aspirin', 'Warfarin'])
  })

  it('joins across a proposition pattern', async () => {
    const stub = await freshStub()
    await seed(stub)
    const names = await expectOk(
      stub,
      `FIND(?d.name)
       WHERE {
         ?d {type: "Drug"}
         ?s {name: "Headache"}
         (?d, "treats", ?s)
       }`,
    )
    expect(names).toEqual(['Aspirin'])
  })

  it('applies FILTER comparisons in the engine, not in SQL', async () => {
    const stub = await freshStub()
    await seed(stub)
    const safe = await expectOk(
      stub,
      `FIND(?d.name)
       WHERE {
         ?d {type: "Drug"}
         FILTER(?d.attributes.risk_level < 3)
       }`,
    )
    expect(safe).toEqual(['Aspirin'])
  })

  it('evaluates CONTAINS and REGEX beyond SQLite LIKE limits', async () => {
    const stub = await freshStub()
    await seed(stub)
    // A LIKE pattern this long would exceed the platform's 50-byte cap if it
    // were pushed into SQL; evaluating in TS sidesteps that entirely.
    const longNeedle = 'x'.repeat(80)
    await expectOk(
      stub,
      `UPSERT { CONCEPT ?l { {type: "Drug", name: "Long"} SET ATTRIBUTES {note: "${longNeedle}"} } }`,
    )
    const hit = await expectOk(
      stub,
      `FIND(?d.name)
       WHERE {
         ?d {type: "Drug"}
         FILTER(CONTAINS(?d.attributes.note, "${longNeedle}"))
       }`,
    )
    expect(hit).toEqual(['Long'])

    const rx = await expectOk(
      stub,
      `FIND(?d.name)
       WHERE {
         ?d {type: "Drug"}
         FILTER(REGEX(?d.name, "^(Asp|War).*n$"))
       }`,
    )
    expect(rx.sort()).toEqual(['Aspirin', 'Warfarin'])
  })

  it('supports NOT as an anti-join', async () => {
    const stub = await freshStub()
    await seed(stub)
    const names = await expectOk(
      stub,
      `FIND(?d.name)
       WHERE {
         ?d {type: "Drug"}
         ?h {name: "Headache"}
         NOT { (?d, "treats", ?h) }
       }`,
    )
    expect(names).toEqual(['Warfarin'])
  })

  it('supports OPTIONAL as a left join', async () => {
    const stub = await freshStub()
    await seed(stub)
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?o { {type: "Drug", name: "Orphan"} SET ATTRIBUTES {risk_level: 2} } }',
    )
    // Multi-column FIND is column-major: one array per projected variable,
    // index-aligned across columns. Rows are read by taking the same index
    // from each column.
    const [drugs, symptoms] = await expectOk(
      stub,
      `FIND(?d.name, ?s.name)
       WHERE {
         ?d {type: "Drug"}
         OPTIONAL { (?d, "treats", ?s) }
       }`,
    )
    const orphanAt = drugs.indexOf('Orphan')
    expect(orphanAt).toBeGreaterThanOrEqual(0)
    expect(symptoms[orphanAt]).toBeNull()
  })

  it('aggregates', async () => {
    const stub = await freshStub()
    await seed(stub)
    const count = await expectOk(
      stub,
      'FIND(COUNT(?d)) WHERE { ?d {type: "Drug"} }',
    )
    expect(count).toBe(2)
  })

  it('orders and paginates with a cursor', async () => {
    const stub = await freshStub()
    await seed(stub)
    const first = await executeTestKip(
      stub,
      'FIND(?d.name) WHERE { ?d {type: "Drug"} } ORDER BY ?d.name ASC LIMIT 1',
    )
    expect((first as any).result).toEqual(['Aspirin'])
    expect((first as any).next_cursor).toBe('1')

    const second = await executeTestKip(
      stub,
      `FIND(?d.name) WHERE { ?d {type: "Drug"} } ORDER BY ?d.name ASC LIMIT 1 CURSOR "1"`,
    )
    expect((second as any).result).toEqual(['Warfarin'])
    expect((second as any).next_cursor).toBeNull()
  })

  it('rejects a malformed cursor instead of restarting pagination', async () => {
    const stub = await freshStub()
    await seed(stub)
    const error = await expectError(
      stub,
      `FIND(?d.name) WHERE { ?d {type: "Drug"} } LIMIT 1 CURSOR "not-a-number"`,
    )
    expect(error.code).toBe('KIP_1001')
  })
})

describe('KQL: multi-hop via recursive CTE', () => {
  it('walks a chain within the hop bounds', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "N", name: "A"} }
         CONCEPT ?b { {type: "N", name: "B"} SET PROPOSITIONS { ("isa", ?a) } }
         CONCEPT ?c { {type: "N", name: "C"} SET PROPOSITIONS { ("isa", ?b) } }
         CONCEPT ?d { {type: "N", name: "D"} SET PROPOSITIONS { ("isa", ?c) } }
       }`,
    )
    const twoHops = await expectOk(
      stub,
      `FIND(?ancestor.name)
       WHERE {
         ?start {type: "N", name: "D"}
         (?start, "isa"{1,2}, ?ancestor)
       }`,
    )
    expect(twoHops.sort()).toEqual(['B', 'C'])

    const threeHops = await expectOk(
      stub,
      `FIND(?ancestor.name)
       WHERE {
         ?start {type: "N", name: "D"}
         (?start, "isa"{1,3}, ?ancestor)
       }`,
    )
    expect(threeHops.sort()).toEqual(['A', 'B', 'C'])
  })

  it('rejects a hop bound past the engine cap', async () => {
    const stub = await freshStub()
    // The start node must exist: an unsatisfiable earlier clause would
    // short-circuit the conjunction before the multi-hop clause is reached,
    // and the test would pass for the wrong reason.
    await expectOk(stub, 'UPSERT { CONCEPT ?d { {type: "N", name: "D"} } }')
    const error = await expectError(
      stub,
      `FIND(?x) WHERE { ?s {type: "N", name: "D"} (?s, "isa"{1,50}, ?x) }`,
    )
    expect(error.code).toBe('KIP_4002')
  })
})

describe('KML: DELETE', () => {
  it('cascades to propositions referencing a deleted concept', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?s { {type: "Symptom", name: "Headache"} }
         CONCEPT ?d { {type: "Drug", name: "Aspirin"} SET PROPOSITIONS { ("treats", ?s) } }
       }`,
    )
    const before = await expectOk(stub, 'FIND(?p) WHERE { ?p (?a, "treats", ?b) }')
    expect(before).toHaveLength(1)

    const result = await expectOk(
      stub,
      'DELETE CONCEPT ?d DETACH WHERE { ?d {type: "Drug", name: "Aspirin"} }',
    )
    expect(result.deleted_concepts).toBe(1)
    expect(result.deleted_propositions).toBe(1)

    const after = await expectOk(stub, 'FIND(?p) WHERE { ?p (?a, "treats", ?b) }')
    expect(after).toEqual([])
  })

  it('removes attribute keys without touching the rest', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?d { {type: "T", name: "A"} SET ATTRIBUTES {keep: 1, drop: 2} } }',
    )
    const result = await expectOk(
      stub,
      'DELETE ATTRIBUTES {"drop"} FROM ?d WHERE { ?d {type: "T", name: "A"} }',
    )
    expect(result.updated_concepts).toBe(1)
    const found = await expectOk(stub, 'FIND(?d) WHERE { ?d {type: "T", name: "A"} }')
    expect(found[0].attributes).toEqual({ keep: 1 })
  })
})

describe('platform limits', () => {
  it('handles id sets far past the 100-bound-parameter ceiling', async () => {
    // The whole read path is set intersection over ids. If anything built an
    // `IN (?, ?, ...)` list this would fail at exactly 100 concepts.
    const stub = await freshStub()
    const blocks = Array.from(
      { length: 250 },
      (_, i) => `CONCEPT ?c${i} { {type: "Bulk", name: "N${i}"} SET ATTRIBUTES {i: ${i}} }`,
    ).join('\n')
    await expectOk(stub, `UPSERT {\n${blocks}\n}`)

    const all = await expectOk(stub, 'FIND(?c.name) WHERE { ?c {type: "Bulk"} }')
    expect(all).toHaveLength(250)

    const filtered = await expectOk(
      stub,
      'FIND(?c.name) WHERE { ?c {type: "Bulk"} FILTER(?c.attributes.i >= 248) }',
    )
    expect(filtered.sort()).toEqual(['N248', 'N249'])
  })

  it('caps a single KIP command at the grammar input limit', async () => {
    // A 2 MB attribute can never reach storage through KIP text: the grammar
    // rejects any command over 256 KB first. The row-size guard below covers
    // the path that can actually exceed 2 MB — repeated merges growing a map.
    const stub = await freshStub()
    const huge = 'a'.repeat(300_000)
    const error = await expectError(
      stub,
      `UPSERT { CONCEPT ?c { {type: "T", name: "Big"} SET ATTRIBUTES {blob: "${huge}"} } }`,
    )
    // The grammar classifies an oversized command as ResourceExhausted
    // rather than a syntax error — the input is well-formed, just too big.
    expect(error.code).toBe('KIP_4002')
    expect(error.message).toMatch(/exceeds maximum/i)
  })

  it('rejects an over-sized stored value with an actionable code', async () => {
    const stub = await freshStub()
    await runInDurableObject(stub, (instance) => {
      const store = (instance as unknown as { nexus: { store: any } }).nexus.store
      expect(() =>
        store.insertConcept('T', 'Big', { blob: 'a'.repeat(2_200_000) }, {}),
      ).toThrow(/over the 2097152-byte limit/)
    })
  })
})

describe('error taxonomy', () => {
  it('returns the grammar’s own code, name and hint for syntax errors', async () => {
    const stub = await freshStub()
    const error = await expectError(stub, 'FIND ?x WHERE {{{')
    expect(error.code).toBe('KIP_1001')
    expect(error.name).toBe('InvalidSyntax')
    // The hint is what an agent uses to self-correct; it is generated from the
    // Rust taxonomy, so both engines send the same recovery text for a code.
    expect(error.hint).toContain('parenthesis matching')
    // Source position is what the WASM parser could not give: its errors
    // carried a nom context breadcrumb, not a line and column.
    expect(error.message).toMatch(/line \d+, column \d+/)
  })

  it('refuses a command past the parser budget instead of overflowing', async () => {
    const stub = await freshStub()
    // Recursive descent: without a depth ceiling this recurses until the
    // JavaScript stack overflows, and a Worker isolate does not survive that
    // — the whole runtime goes, not just this request.
    const deep = `FIND(?x) WHERE { ?x {type: "T"} FILTER(?x.n == ${'['.repeat(2000)}) }`
    const error = await expectError(stub, deep)
    expect(error.code).toBe('KIP_4002')

    // The isolate is still serving.
    await expectOk(stub, 'DESCRIBE PRIMER')
  })

  it('refuses a command past the input-length ceiling', async () => {
    const stub = await freshStub()
    const huge = `FIND(?x) WHERE { ?x {name: "${'a'.repeat(300_000)}"} }`
    const error = await expectError(stub, huge)
    expect(error.code).toBe('KIP_4002')
  })

  it('rejects what the syntax allows but the language does not', async () => {
    const stub = await freshStub()
    for (const command of [
      // A matcher that identifies no single node.
      'UPSERT { CONCEPT ?c { {type: "T"} } }',
      // A filter function outside the closed set.
      'FIND(?x) WHERE { ?x {type: "T"} FILTER(MEDIAN(?x.n) == 1) }',
      // An UPDATE expression reading a variable other than its target.
      'UPDATE ?a SET ATTRIBUTES { n: ADD(?b.attributes.n, 1) } WHERE { ?a {type: "T"} (?a, "p", ?b) }',
      // `LIMIT 0` is the engine's "no limit" sentinel, so it may not be written.
      'FIND(?x) WHERE { ?x {type: "T"} } LIMIT 0',
    ]) {
      const error = await expectError(stub, command)
      expect(error.code, command).toBe('KIP_1001')
    }
  })

  it('reports an unbound FILTER variable as a ReferenceError', async () => {
    const stub = await freshStub()
    await expectOk(stub, 'UPSERT { CONCEPT ?a { {type: "T", name: "A"} } }')
    const error = await expectError(
      stub,
      'FIND(?d) WHERE { ?d {type: "T"} FILTER(?other.name == "x") }',
    )
    expect(error.code).toBe('KIP_3001')
  })
})

describe('cross-engine semantics pinned by the 2026-08 review', () => {
  it('runs clauses after an empty disjoint group', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?h { {type: "Symptom", name: "Headache"} } }',
    )
    // "N" is declared but has no instances. Its empty group must not
    // suppress the later ?s clause: every WHERE clause executes and FIND
    // projects per covering table (`db/mod.rs:715-717`).
    const found = await expectOk(
      stub,
      'FIND(?s.name) WHERE { ?x {type: "N"} ?s {type: "Symptom"} }',
    )
    expect(found).toEqual(['Headache'])
  })

  it('treats an uncorrelated NOT as a no-op', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?d { {type: "Drug", name: "Aspirin"} }
         CONCEPT ?s { {type: "Symptom", name: "Headache"} }
       }`,
    )
    // The block matches (a Symptom exists) but shares no variable with the
    // outer scope, so it cannot exclude anything (`kql.rs:396-398`).
    const found = await expectOk(
      stub,
      'FIND(?d.name) WHERE { ?d {type: "Drug"} NOT { ?s {type: "Symptom"} } }',
    )
    expect(found).toEqual(['Aspirin'])
  })

  it('null-pads an OPTIONAL block that shares no variable and matched nothing', async () => {
    const stub = await freshStub()
    await expectOk(stub, 'UPSERT { CONCEPT ?d { {type: "Drug", name: "Aspirin"} } }')
    const found = await expectOk(
      stub,
      'FIND(?d.name, ?e) WHERE { ?d {type: "Drug"} OPTIONAL { ?e {type: "N"} } }',
    )
    // Column-major projection: the drug survives with ?e projected null.
    expect(found).toEqual([['Aspirin'], [null]])
  })

  it('keeps OPTIONAL variables bound when a dangling id degrades the block', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?s { {type: "Symptom", name: "Headache"} }
         CONCEPT ?d { {type: "Drug", name: "Aspirin"} SET PROPOSITIONS { ("treats", ?s) } }
       }`,
    )
    // The dangling {id:} degrades to an empty match inside OPTIONAL, but ?x
    // stays a bound column and pads null instead of erroring as unbound.
    const found = await expectOk(
      stub,
      'FIND(?d.name, ?x) WHERE { ?d {type: "Drug"} OPTIONAL { (?d, "treats", ?x) ?x {id: "C:99999"} } }',
    )
    expect(found).toEqual([['Aspirin'], [null]])
  })

  it('discards every solution on a constant false FILTER without unbinding', async () => {
    const stub = await freshStub()
    await expectOk(stub, 'UPSERT { CONCEPT ?d { {type: "Drug", name: "Aspirin"} } }')
    // Rows are cleared in place; the later re-declaration of ?d semi-joins
    // the emptied table rather than rebuilding the domain from scratch.
    const found = await expectOk(
      stub,
      'FIND(?d.name) WHERE { ?d {type: "Drug"} FILTER(1 > 2) ?d {type: "Drug"} }',
    )
    expect(found).toEqual([])
  })

  it('enforces equality when one variable names several pattern positions', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "T", name: "A"} }
         CONCEPT ?b { {type: "T", name: "B"} SET PROPOSITIONS { ("rel", ?a) } }
       }`,
    )
    // An entity binding can never equal a predicate binding...
    const viaPredicate = await expectOk(stub, 'FIND(?o) WHERE { (?x, ?x, ?o) }')
    expect(viaPredicate).toEqual([])
    // ...nor a link id its own subject.
    const viaLink = await expectOk(
      stub,
      'FIND(?o.name) WHERE { ?l (?l, "rel", ?o) }',
    )
    expect(viaLink).toEqual([])
  })

  it('does not bump _version on a bare re-declaration', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?d { {type: "T", name: "A"} SET ATTRIBUTES {a: 1} } }',
    )
    await expectOk(stub, 'UPSERT { CONCEPT ?d { {type: "T", name: "A"} } }')
    const found = await expectOk(stub, 'FIND(?d) WHERE { ?d {type: "T", name: "A"} }')
    expect(found[0].metadata._version).toBe(1)
  })

  it('does not bump a link version on a bare proposition re-declaration', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "T", name: "A"} }
         CONCEPT ?b { {type: "T", name: "B"} }
         PROPOSITION ?p { (?a, "rel", ?b) }
       }`,
    )
    await expectOk(
      stub,
      `UPSERT {
         PROPOSITION ?p { ({type: "T", name: "A"}, "rel", {type: "T", name: "B"}) }
       }`,
    )
    const links = await expectOk(stub, 'FIND(?p) WHERE { ?p (?a, "rel", ?b) }')
    expect(links[0].metadata._version).toBe(1)
  })

  it('UPDATE writes an explicit null attribute', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?d { {type: "T", name: "A"} SET ATTRIBUTES {x: 5} } }',
    )
    const result = await expectOk(
      stub,
      'UPDATE ?d SET ATTRIBUTES { x: null } WHERE { ?d {type: "T", name: "A"} }',
    )
    expect(result.updated).toBe(1)
    const found = await expectOk(stub, 'FIND(?d) WHERE { ?d {type: "T", name: "A"} }')
    expect(found[0].attributes.x).toBeNull()
  })

  it('counts cascaded higher-order deletions per link', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "T", name: "A"} }
         CONCEPT ?b { {type: "T", name: "B"} }
         CONCEPT ?s { {type: "Source", name: "S"} }
         PROPOSITION ?base { (?a, "rel", ?b) }
         PROPOSITION ?h1 { (?base, "cited_by", ?s) }
         PROPOSITION ?h2 { (?base, "made_by", ?s) }
       }`,
    )
    // Deleting the base link orphans one higher-order row carrying two
    // predicates: 1 target + 2 cascaded links (`kml.rs:1747`).
    const result = await expectOk(
      stub,
      'DELETE PROPOSITIONS ?p WHERE { ?p (?a, "rel", ?b) }',
    )
    expect(result.deleted_propositions).toBe(3)
  })

  it('MERGE bumps repointed link versions and dedups provenance', async () => {
    const stub = await freshStub()
    await expectOk(
      stub,
      `UPSERT {
         CONCEPT ?a { {type: "T", name: "A"} }
         CONCEPT ?b { {type: "T", name: "B"} }
         CONCEPT ?c { {type: "T", name: "C"} SET PROPOSITIONS { ("rel", ?a) } }
       }`,
    )
    const result = await expectOk(
      stub,
      'MERGE CONCEPT ?src INTO ?dst WHERE { ?src {type: "T", name: "A"} ?dst {type: "T", name: "B"} }',
    )
    expect(result.links_repointed).toBe(1)

    // Repointing is a mutation of the link element, so its version advances.
    const links = await expectOk(stub, 'FIND(?p) WHERE { ?p (?c, "rel", ?b) }')
    expect(links[0].metadata._version).toBe(2)

    const target = await expectOk(stub, 'FIND(?d) WHERE { ?d {type: "T", name: "B"} }')
    expect(target[0].metadata._merged_from).toEqual(['T:A'])
  })

  it('allows updating $self attributes but locks core_directives', async () => {
    const stub = await freshStub()
    // `persons/self.kip` is deliberately not bundled; applications create the
    // actor themselves. Creation may set core_directives — only later
    // modification is locked (`kml.rs:2083-2101`).
    await expectOk(
      stub,
      'UPSERT { CONCEPT ?s { {type: "Person", name: "$self"} SET ATTRIBUTES { person_class: "AI", core_directives: [{name: "root", description: "be kind"}] } } }',
    )
    const updated = await expectOk(
      stub,
      'UPDATE ?s SET ATTRIBUTES { mood: "curious" } WHERE { ?s {type: "Person", name: "$self"} }',
    )
    expect(updated.updated).toBe(1)

    const locked = await expectError(
      stub,
      'UPDATE ?s SET ATTRIBUTES { core_directives: "obey" } WHERE { ?s {type: "Person", name: "$self"} }',
    )
    expect(locked.code).toBe('KIP_3004')

    const destroy = await expectError(
      stub,
      'DELETE CONCEPT ?s DETACH WHERE { ?s {type: "Person", name: "$self"} }',
    )
    expect(destroy.code).toBe('KIP_3004')
  })
})
