import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { parseKip, parseKipAll, parseKipBatch } from '../src/kip/parser.js'
import type { KqlQuery } from '../src/kip/ast.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'

async function withNexus(name: string, body: (n: CognitiveNexus) => void): Promise<void> {
  await runInDurableObject(env.KIP_DB.getByName(`kql-scope-${name}`), (_instance, state) => {
    const n = CognitiveNexus.connect(state.storage)
    n.activatePackages([COGNITIVE_MEMORY])
    n.execute(`MUTATE {
      CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" SET ATTRIBUTES { display_name: "Alice A" } }
      CREATE CONCEPT ?b { TYPE "Person" NAME "Bob" }
      CREATE CONCEPT ?d { TYPE "Preference" NAME "Dark" }
      ENSURE PROPOSITION ?p (?a, "prefers", ?d)
    }`)
    body(n)
  })
}

describe('KQL scope and value contracts', () => {
  it('executes a nested UNION independently, then joins OPTIONAL compatibly', async () => {
    await withNexus('optional-union', (n) => {
      expect(n.query(`FIND(?c.name, ?x.name) WHERE {
        ?c CONCEPT {name: "Alice"}
        OPTIONAL { ?x CONCEPT {name: "Nobody"} UNION { ?x CONCEPT {name: "Bob"} } FILTER(IS_NULL(?c)) }
      }`)).toEqual([['Alice', 'Bob']])
      expect(n.query(`FIND(?c.name) WHERE {
        ?c CONCEPT {name: "Alice"}
        OPTIONAL { ?c CONCEPT {name: "Nobody"} UNION { ?c CONCEPT {name: "Bob"} } }
      }`)).toEqual(['Alice'])
    })
  })

  it('tests only compatible independent UNION results inside NOT', async () => {
    await withNexus('not-union', (n) => {
      expect(n.query(`FIND(?c.name) WHERE {
        ?c CONCEPT {name: "Alice"}
        NOT { ?x CONCEPT {name: "Nobody"} UNION { ?x CONCEPT {name: "Bob"} } FILTER(IS_NULL(?c)) }
      }`)).toEqual([])
      expect(n.query(`FIND(?c.name) WHERE {
        ?c CONCEPT {name: "Alice"}
        NOT { ?c CONCEPT {name: "Nobody"} UNION { ?c CONCEPT {name: "Bob"} } }
      }`)).toEqual(['Alice'])
    })
  })

  it('recovers empty UNION left and leaves branch-only names unbound', async () => {
    await withNexus('union-empty', (n) => {
      expect(n.query('FIND(?c.name, ?x.name) WHERE { ?c CONCEPT {name: "Nobody"} UNION { ?x CONCEPT {name: "Bob"} } }')).toEqual([[null, 'Bob']])
      expect(n.query('FIND(?c.name) WHERE { ?c CONCEPT {name: "Nobody"} ?c CONCEPT {name: "Bob"} }')).toEqual([])
    })
  })

  it('discards partial OPTIONAL bindings and allows later ordinary binding', async () => {
    await withNexus('optional-partial', (n) => {
      const where = '?c CONCEPT {name: "Alice"} OPTIONAL { ?x CONCEPT {name: "Bob"} FILTER(?x.name == "Nobody") }'
      expect(n.query(`FIND(?c.name, ?x.name) WHERE { ${where} }`)).toEqual([['Alice', null]])
      expect(n.query(`FIND(?x.name) WHERE { ${where} ?x CONCEPT {name: "Dark"} }`)).toEqual(['Dark'])
    })
  })

  it('rejects undefined, NOT-local, and foreign UNION expression names', async () => {
    await withNexus('scope', (n) => {
      for (const q of [
        'FIND(?missing) WHERE { ?c CONCEPT {name: "Nobody"} }',
        'FIND(?x) WHERE { ?c CONCEPT {} NOT { ?x CONCEPT {name: "Nobody"} } }',
        'FIND(?c) WHERE { ?c CONCEPT {} } ORDER BY ?missing',
        'FIND(?c) WHERE { ?c CONCEPT {} UNION { FILTER(?c.name == "Alice") } }',
        'FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} FILTER(IS_NULL(?missing)) }',
      ]) expect(() => n.query(q), q).toThrowError(/visible pattern binding/)
      expect(n.query('FIND(?x.name) WHERE { NOT { ?x CONCEPT {name: "Nobody"} } ?x CONCEPT {name: "Bob"} }')).toEqual(['Bob'])
    })
  })

  it('preserves unknown through null comparisons, negation, membership and strings', async () => {
    await withNexus('unknown', (n) => {
      for (const f of ['?c.attributes.missing == null', '?c.attributes.missing != "x"', '!(?c.attributes.missing == "x")', '!IN(?c.attributes.missing, [null, "x"])', '!CONTAINS(?c.attributes.missing, "x")', '!(?c.name > 5)']) {
        expect(n.query(`FIND(?c.name) WHERE { ?c CONCEPT {name: "Alice"} FILTER(${f}) }`), f).toEqual([])
      }
      expect(n.query('FIND(?c.name) WHERE { ?c CONCEPT {name: "Alice"} FILTER(?c.name == "Alice" || ?c.attributes.missing == "x") }')).toEqual(['Alice'])
      expect(n.query('FIND(?c.name) WHERE { ?c CONCEPT {name: "Alice"} FILTER(!(?c.name == "Bob" && ?c.attributes.missing == "x")) }')).toEqual(['Alice'])
    })
  })

  it('validates constant errors in empty and short-circuited branches', async () => {
    await withNexus('static', (n) => {
      for (const q of [
        'FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} FILTER(REGEX(?c.name, "[")) }',
        'FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} FILTER(CONTAINS(?c.name, 5)) }',
        'FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} FILTER(IN(?c.name, 5)) }',
        'FIND(?c) WHERE { ?c CONCEPT {} FILTER(1 == 1 || REGEX(?c.name, "[")) }',
        'FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} NOT { ?x CONCEPT {type: "MissingType"} } }',
        'FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} OPTIONAL { ?x CONCEPT {type: "MissingType"} } }',
      ]) expect(() => n.query(q), q).toThrow()
      expect(() => n.query('FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} FILTER(REGEX(?c.name, :pattern)) }', { pattern: '[' })).toThrowError(/regular expression/)
      expect(() => n.query('FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} FILTER(?c.name == :missing) }')).toThrowError(/does not bind/)
    })
  })

  it('deduplicates completed mappings before aggregation, preserving identical projected cells', async () => {
    await withNexus('dedup', (n) => {
      expect(n.query('FIND(COUNT(?c)) WHERE { ?c CONCEPT {name: "Alice"} UNION { ?x CONCEPT {name: "Alice"} } ?c CONCEPT {name: "Alice"} ?x CONCEPT {name: "Alice"} }')).toEqual([1])
      n.execute('CREATE CONCEPT ?a2 { TYPE "Person" NAME "Alice" }')
      expect(n.query('FIND(?c.name) WHERE { ?c CONCEPT {name: "Alice"} }')).toEqual(['Alice', 'Alice'])
      expect(n.query('FIND(?c.name, COUNT(?c)) WHERE { ?c CONCEPT {name: "Alice"} }')).toEqual([['Alice', 2]])
    })
  })

  it('returns empty aggregate identities and rejects wrong input types', async () => {
    await withNexus('aggregate', (n) => {
      expect(n.query('FIND(COUNT(?c), SUM(?c._system.version), AVG(?c._system.version), MIN(?c.name), MAX(?c.name)) WHERE { ?c CONCEPT {name: "Nobody"} }')).toEqual([[0, null, null, null, null]])
      expect(n.query('FIND(COUNT(?x), SUM(?x._system.version), AVG(?x._system.version)) WHERE { ?c CONCEPT {name: "Alice"} OPTIONAL { ?x CONCEPT {name: "Nobody"} } }')).toEqual([[0, null, null]])
      expect(n.query('FIND(?c.name, COUNT(?c)) WHERE { ?c CONCEPT {name: "Nobody"} }')).toEqual([])
      for (const expr of ['SUM(?c.name)', 'AVG(?c.name)', 'MIN(?c.attributes)', 'MAX(?c.attributes)']) expect(() => n.query(`FIND(${expr}) WHERE { ?c CONCEPT {name: "Alice"} }`)).toThrowError(/requires/)
    })
  })

  it('keeps element bindings distinct from literal ID-shaped strings', async () => {
    await withNexus('kinds', (n) => {
      expect(n.query('FIND(?c.name) WHERE { ?c CONCEPT {name: "Alice"} FILTER(IS_ELEMENT(?c) && IS_LITERAL(?c.id)) }')).toEqual(['Alice'])
      expect(n.query('FIND(?c) WHERE { ?c CONCEPT {name: "Alice"} }')[0]).toMatchObject({ kind: 'concept', name: 'Alice' })
    })
  })

  it('sorts nulls last under DESC and refuses unsafe LIMIT parameters', async () => {
    await withNexus('limits', (n) => {
      expect(n.query('FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.attributes.display_name DESC')).toEqual(['Alice', 'Bob'])
      expect(n.queryPage('FIND(?c.name) WHERE { ?c CONCEPT {} } LIMIT 0')).toMatchObject({ rows: [], nextCursor: null })
      expect(() => n.query('FIND(?c.name) WHERE { ?c CONCEPT {} } LIMIT :n', { n: 9007199254740992 })).toThrow()
    })
  })

  it('rejects paths statically, including predicate variable path forms', async () => {
    await withNexus('paths', (n) => {
      expect(() => n.query('FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} OPTIONAL { PROPOSITION (?c, "prefers"{0,2}, ?x) } }')).toThrowError(/not implemented/)
      expect(() => n.query('FIND(?c) WHERE { ?c CONCEPT {name: "Nobody"} PROPOSITION (?c, ?pred{0,2}, ?x) }')).toThrow()
    })
  })

  it('matches nested object subsets and binds their variables without leaking partial matches', async () => {
    await withNexus('nested-objects', (n) => {
      n.execute('UPDATE "C-1" SET ATTRIBUTES {bag: {a: 1, b: 2}, explicit: null, list: ["x", "y"]}')
      expect(n.query('FIND(?n, ?v) WHERE { ?c {name: "Alice", attributes: {display_name: ?n, bag: {a: ?v}}} }')).toEqual([['Alice A', 1]])
      expect(n.query('FIND(?c.name) WHERE { ?c {attributes: {bag: {a: 1}}} }')).toEqual(['Alice'])
      expect(n.query('FIND(?v) WHERE { ?c {attributes: {explicit: ?v}} }')).toEqual([null])
      expect(n.query('FIND(?c.name) WHERE { ?c {attributes: {missing: null}} }')).toEqual([])
      expect(n.query('FIND(?c.name, ?v) WHERE { ?c {name: "Alice"} OPTIONAL { ?c {attributes: {bag: {a: ?v, missing: 1}}} } }')).toEqual([['Alice', null]])
      expect(n.query('FIND(?x, ?y) WHERE { ?c {attributes: {list: [?x, ?y]}} }')).toEqual([['x', 'y']])
      expect(() => n.query('FIND(?c) WHERE { ?c {name: "Nobody"} ?x {attributes: {bag: {a: :missing}}} }')).toThrowError(/does not bind/)
    })
  })

  it('matches inline Concept endpoint descriptions and nested Proposition tuples without creation', async () => {
    await withNexus('inline-endpoints', (n) => {
      const before = n.store.currentSeq(n.space)
      expect(n.query('FIND(?name) WHERE { PROPOSITION ({type: "Person", name: ?name}, "prefers", {type: "Preference", name: "Dark"}) }')).toEqual(['Alice'])
      expect(n.query('FIND(?c.name) WHERE { ?c {name: "Alice"} PROPOSITION (?c, "prefers", {id: "C-3", name: "Wrong"}) }')).toEqual([])
      expect(n.query('FIND(?c.name) WHERE { ?c {name: "Alice"} PROPOSITION (?c, "prefers", {type: "Preference", name: "Missing"}) }')).toEqual([])
      expect(n.store.currentSeq(n.space)).toBe(before)
    })
  })

  it('does not interpret arbitrary value/datatype attribute objects as scalar Literals', async () => {
    await withNexus('attribute-object', (n) => {
      n.execute('UPDATE "C-1" SET ATTRIBUTES {measure: {value: 5, datatype: "a"}, empty: {value: null, datatype: "a"}}')
      n.execute('UPDATE "C-2" SET ATTRIBUTES {measure: {datatype: "b", value: 5}}')
      expect(n.query('FIND(COUNT(DISTINCT ?m)) WHERE { ?c {attributes: {measure: ?m}} }')).toEqual([2])
      expect(() => n.query('FIND(SUM(?m)) WHERE { ?c {attributes: {measure: ?m}} }')).toThrowError(/numeric/)
      expect(n.query('FIND(?c.name) WHERE { ?c {name: "Alice"} FILTER(IS_NOT_NULL(?c.attributes.empty)) }')).toEqual(['Alice'])
    })
  })

  it('binds stored scalar Literal endpoints as scalar values with distinct null/unbound identity', async () => {
    await withNexus('literal-endpoints', (n) => {
      n.activatePackages([COGNITIVE_MEMORY, {
        format: 'KIP-Schema-Package',
        manifest: {package_id: 'kip://test/kql-values', version: '1.0.0'},
        definitions: {predicates: {amount: {kind: 'PredicateType', object: {nullable: true}}}},
      }])
      n.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "amount", 5)')
      n.execute('ENSURE PROPOSITION ?p ({id: "C-2"}, "amount", null)')
      expect(n.query('FIND(?v) WHERE { PROPOSITION ({id: "C-1"}, "amount", ?v) }')).toEqual([5])
      expect(n.query('FIND(SUM(?v), AVG(?v), COUNT(?v)) WHERE { PROPOSITION (?c, "amount", ?v) }')).toEqual([[5, 5, 1]])
      expect(n.query('FIND(COUNT(?c), COUNT(?v)) WHERE { ?c {id: "C-2"} PROPOSITION (?c, "amount", ?v) UNION { ?c {id: "C-2"} } }')).toEqual([[2, 0]])
      n.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "amount", "é")')
      expect(n.query('FIND(?v) WHERE { PROPOSITION ({id: "C-1"}, "amount", ?v) FILTER(?v == :text) }', {text: 'e\u0301'})).toEqual(['é'])
    })
  })


  it('preserves DISTINCT sort operands through single, batch and multi-command lowering', () => {
    const source = 'FIND(?c.name, COUNT(DISTINCT ?c)) WHERE {?c {}} ORDER BY COUNT(DISTINCT ?c) DESC'
    const single = parseKip(source)
    expect('Kql' in single && (single.Kql as KqlQuery).order_by?.[0]?.distinct).toBe(true)
    const batch = parseKipBatch([source])[0]!
    expect('ok' in batch && 'Kql' in batch.ok && (batch.ok.Kql as KqlQuery).order_by?.[0]?.distinct).toBe(true)
    for (const command of parseKipAll(source + '\n' + source)) expect('Kql' in command && (command.Kql as KqlQuery).order_by?.[0]?.distinct).toBe(true)
  })

  it('sorts COUNT and COUNT DISTINCT by their different aggregate values', async () => {
    await withNexus('distinct-order', (n) => {
      n.execute(`MUTATE {
        CREATE CONCEPT ?a2 {TYPE "Person" NAME "Alice"}
        CREATE CONCEPT ?a3 {TYPE "Person" NAME "Alice"}
        CREATE CONCEPT ?coffee {TYPE "Preference" NAME "Coffee"}
        ENSURE PROPOSITION ?p2 (?a2, "prefers", {id: "C-3"})
        ENSURE PROPOSITION ?p3 (?a3, "prefers", {id: "C-3"})
        ENSURE PROPOSITION ?p4 ({id: "C-2"}, "prefers", {id: "C-3"})
        ENSURE PROPOSITION ?p5 ({id: "C-2"}, "prefers", ?coffee)
      }`)
      const source = 'FIND(?c.name, COUNT(?t), COUNT(DISTINCT ?t)) WHERE {?c {type: "Person"} (?c, "prefers", ?t)}'
      expect(n.query(source + ' ORDER BY COUNT(?t) DESC')).toEqual([['Alice', 3, 1], ['Bob', 2, 2]])
      expect(n.query(source + ' ORDER BY COUNT(DISTINCT ?t) DESC')).toEqual([['Bob', 2, 2], ['Alice', 3, 1]])
    })
  })


  it('deduplicates virtual beliefs by target and basis instead of identical insufficient payloads', async () => {
    await withNexus('belief-identity', (n) => {
      const first = '?b BELIEF ({id: "C-2"}, "prefers", {id: "C-3"})'
      const other = '?b BELIEF ({id: "C-2"}, "prefers", {id: "C-1"})'
      const rows = n.query(`FIND(?b) WHERE { ${first} UNION { ${other} } }`)
      expect(rows).toHaveLength(2)
      expect(rows[0]).toEqual(rows[1])
      expect(n.query(`FIND(?b) WHERE { ${first} UNION { ${first} } }`)).toHaveLength(1)
      expect(n.query(`FIND(COUNT(DISTINCT ?b)) WHERE { ${first} UNION { ${other} } }`)).toEqual([2])
      expect(n.query(`FIND(COUNT(DISTINCT ?b.status)) WHERE { ${first} UNION { ${other} } }`)).toEqual([1])
    })
  })

})
