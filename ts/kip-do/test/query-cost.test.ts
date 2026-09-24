import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { OPTIONS } from './support/options.js'

/**
 * How much work a read does, pinned by what it asks SQLite.
 *
 * Each of these was once proportional to something it should not be: a scan
 * repeated per incoming solution, every Activity in the Space read to find
 * one producer, a lookup repeated on every load of an element already loaded.
 * Nothing about the answers changes when that creeps back, so the statements
 * themselves are counted.
 */
function recorder(state: DurableObjectState): { statements: string[] } {
  const sql = state.storage.sql as unknown as { exec: (query: string, ...values: unknown[]) => unknown }
  const exec = sql.exec.bind(sql)
  const box = { statements: [] as string[] }
  sql.exec = (query: string, ...values: unknown[]) => {
    box.statements.push(query)
    return exec(query, ...values)
  }
  return box
}

async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus, recorded: { statements: string[] }) => void,
): Promise<void> {
  await runInDurableObject(env.KIP_DB.getByName(`query-cost-${name}`), (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY, OPTIONS])
    const people = Array.from(
      { length: 12 },
      (_, i) => `UPSERT CONCEPT ?p${i} { MATCH {type: "Person", key: "p${i}"} SET FIELDS {name: "P${i}"} }`,
    )
    nexus.execute(`MUTATE { ${people.join('\n')} }`)
    nexus.execute('MUTATE { CREATE CONCEPT ?a { TYPE "Option" NAME "tea" } CREATE CONCEPT ?b { TYPE "Option" NAME "coffee" } }')
    body(nexus, recorder(state))
  })
}

const count = (statements: string[], pattern: RegExp): number =>
  statements.filter((statement) => pattern.test(statement)).length

describe('query cost', () => {
  it('scans an independent pattern once, not once per incoming solution', async () => {
    await withNexus('independent', (nexus, recorded) => {
      const rows = nexus.query(
        'FIND(?a.name, ?b.name) WHERE { ?a CONCEPT {type: "Person"} ?b CONCEPT {type: "Option"} }',
      )
      expect(rows).toHaveLength(24)
      expect(count(recorded.statements, /^SELECT \* FROM concepts WHERE/)).toBe(2)
    })
  })

  it('checks identity reviews once per read when the Space has none', async () => {
    await withNexus('reviews', (nexus, recorded) => {
      nexus.query('FIND(?a.name) WHERE { ?a CONCEPT {type: "Person"} } ORDER BY ?a.name LIMIT 3')
      expect(count(recorded.statements, /identity_review/)).toBe(1)
    })
  })

  it('finds a derived element’s producers through the reverse index', async () => {
    await withNexus('producers', (nexus, recorded) => {
      nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "prefers", {id: "C-13"})')
      const inferred = Array.from(
        { length: 4 },
        (_, i) =>
          `CREATE ASSERTION ?a${i} { SET FIELDS {proposition: "P-1", asserted_by: "C-${i + 1}", stance: "support", mode: "inferred", confidence: 0.5} }`,
      )
      const activities = Array.from(
        { length: 8 },
        (_, i) => `CREATE ACTIVITY ?x${i} { SET FIELDS {activity_class: "extraction", status: "completed"} }`,
      )
      nexus.execute(`MUTATE { ${[...inferred, ...activities].join('\n')} }`)
      recorded.statements.length = 0
      expect(nexus.query('FIND(?a) WHERE { ?a ASSERTION {mode: "inferred"} }')).toHaveLength(4)
      expect(count(recorded.statements, /FROM activities WHERE space = \?\s*(ORDER BY id)?$/)).toBe(0)
      expect(count(recorded.statements, /field = 'outputs'/)).toBe(4)
    })
  })

  it('resolves a Space’s Schema Environment once until it changes', async () => {
    await withNexus('environment', (nexus) => {
      const env = nexus.environment()
      expect(nexus.environment()).toBe(env)
      nexus.execute('DEFINE CONCEPT TYPE "Gadget" {description: "A thing."}')
      const next = nexus.environment()
      expect(next).not.toBe(env)
      expect(next.version).toBe(env.version + 1)
      expect(nexus.environment()).toBe(next)
    })
  })
})
