import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { principalAuth } from '../src/governance/index.js'
import type { Json } from '../src/json.js'
import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { OPTIONS } from './support/options.js'

/**
 * The 2026-09-25 review, pinned on this engine as on the Rust one
 * (`rs/anda_cognitive_nexus/tests/review_fixes.rs`).
 */
async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus, recorded: { statements: string[] }) => void,
): Promise<void> {
  await runInDurableObject(env.KIP_DB.getByName(`review-${name}`), (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY, OPTIONS])
    const sql = state.storage.sql as unknown as {
      exec: (query: string, ...values: unknown[]) => unknown
    }
    const exec = sql.exec.bind(sql)
    const recorded = { statements: [] as string[] }
    sql.exec = (query: string, ...values: unknown[]) => {
      recorded.statements.push(query)
      return exec(query, ...values)
    }
    body(nexus, recorded)
  })
}

interface Hit {
  id: string
  score: number
}

const hitsOf = (answer: Json): Hit[] =>
  ((answer as unknown as { hits?: Hit[] }).hits ?? []).map(({ id, score }) => ({ id, score }))

describe('review fixes', () => {
  it('keeps the reference audit filtered on an endpoint reached through a tuple', async () => {
    await withNexus('reference-audit', (nexus) => {
      const created = nexus.execute(`MUTATE {
        CREATE CONCEPT ?secret {TYPE "Person" NAME "Secret"}
        CREATE CONCEPT ?ada {TYPE "Person" NAME "Ada"}
        CREATE CONCEPT ?party {TYPE "Event" NAME "Party" SET ATTRIBUTES {summary: "a party"} SET STRUCTURAL {("involves", ?secret)}}
        ENSURE PROPOSITION ?p (?ada, "prefers", ?party)
      }`) as { handles: Record<string, string> }
      const { secret, ada, party, p } = created.handles
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:limited' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:limited',
          actions: ['read'],
          scope: { elements: [ada!, party!, p!] },
        },
        SYSTEM_PRINCIPAL,
      )
      const reader = nexus.session(principalAuth('kip:principal:limited'))
      for (const command of [
        `FIND(?o._system.input_references) WHERE {?o CONCEPT {id: "${party}"}}`,
        'FIND(?o._system.input_references) WHERE { (?s, "prefers", ?o) }',
      ]) {
        expect(JSON.stringify(reader.query(command))).not.toContain(`"${secret}"`)
      }
    })
  })

  it('audits each resolution once per element', async () => {
    await withNexus('binding-fanout', (nexus) => {
      const clauses = ['CREATE CONCEPT ?ada {TYPE "Person" NAME "Ada"}']
      for (let i = 0; i < 10; i++) {
        clauses.push(`CREATE CONCEPT ?o${i} {TYPE "Person" NAME "P${i}"}`)
        clauses.push(`ENSURE PROPOSITION ?p${i} (?ada, "prefers", ?o${i})`)
      }
      const created = nexus.execute(`MUTATE { ${clauses.join('\n')} }`) as {
        handles: Record<string, string>
      }
      const audit = nexus.query(
        `FIND(?p._system.input_references) WHERE { ?p PROPOSITION (id: "${created.handles.p0}") }`,
      ) as Json[][]
      // Subject and object: two resolutions, however many clauses named Ada.
      expect(audit[0]).toHaveLength(2)
    })
  })

  it('ranks an unnarrowed SEARCH from the index as the authorized scan would', async () => {
    await withNexus('indexed-search', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?a {TYPE "Person" NAME "Alice Liddell" SET FIELDS {aliases: ["Alice"]}}
        CREATE CONCEPT ?b {TYPE "Person" NAME "Alice Cooper"}
        CREATE CONCEPT ?c {TYPE "Person" NAME "Bob" SET ATTRIBUTES {note: "knows Alice"}}
        CREATE CONCEPT ?d {TYPE "Event" NAME "Tea party" SET ATTRIBUTES {summary: "Alice at tea"}}
      }`)
      // A result cap narrows the reader's authority, so it ranks by the scan
      // over its authorized corpus — which here is the whole Space.
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:capped' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:capped',
          actions: ['read', 'search'],
          constraints: { max_results: 100 },
        },
        SYSTEM_PRINCIPAL,
      )
      const capped = nexus.session(principalAuth('kip:principal:capped'))
      for (const command of [
        'SEARCH CONCEPT "Alice" LIMIT 10',
        'SEARCH CONCEPT "Alice" WITH TYPE "Person" LIMIT 10',
        'SEARCH CONCEPT "alice tea" LIMIT 10',
      ]) {
        const indexed = hitsOf(nexus.describe(command))
        const scanned = hitsOf(capped.describe(command))
        expect(indexed.length).toBeGreaterThan(0)
        expect(indexed.map((hit) => hit.id)).toEqual(scanned.map((hit) => hit.id))
        indexed.forEach((hit, i) => expect(hit.score).toBeCloseTo(scanned[i]!.score, 12))
      }
    })
  })

  it('scores only visible inline Evidence content on both search paths', async () => {
    await withNexus('evidence-search', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE EVIDENCE ?a {SET FIELDS {evidence_class: "observation", payload: "apple"}}
        CREATE EVIDENCE ?b {SET FIELDS {evidence_class: "observation", payload: "apple banana cherry durian"}}
        CREATE EVIDENCE ?external {SET FIELDS {evidence_class: "observation", payload: {content_ref: "urn:example:offsite"}}}
        CREATE EVIDENCE ?empty {SET FIELDS {evidence_class: "observation", payload: ""}}
      }`)
      // A result cap selects the scan while preserving the same visible corpus.
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:capped' })
      gov.createGrant({
        space_id: nexus.space,
        grantee_principal: 'kip:principal:capped',
        actions: ['read', 'search'],
        constraints: { max_results: 100 },
      }, SYSTEM_PRINCIPAL)
      const capped = nexus.session(principalAuth('kip:principal:capped'))
      type Answer = { hits: (Hit & { snippet: string; element: { payload: { inline: string } } })[] }
      for (const [command, count] of [
        ['SEARCH EVIDENCE "apple" LIMIT 10', 2],
        ['SEARCH EVIDENCE "apple" THRESHOLD 0.187 LIMIT 10', 1],
        ['SEARCH EVIDENCE "inline" LIMIT 10', 0],
        ['SEARCH EVIDENCE "offsite" LIMIT 10', 0],
      ] as const) {
        const indexed = nexus.describe(command) as unknown as Answer
        const scanned = capped.describe(command) as unknown as Answer
        expect(indexed.hits).toHaveLength(count)
        expect(scanned.hits).toHaveLength(count)
        expect(indexed.hits.map((hit) => hit.id)).toEqual(scanned.hits.map((hit) => hit.id))
        indexed.hits.forEach((hit, i) => {
          expect(hit.score).toBeCloseTo(scanned.hits[i]!.score, 12)
          expect(hit.snippet).toBe(hit.element.payload.inline)
          expect(hit.snippet).toBe(scanned.hits[i]!.snippet)
        })
      }
      gov.ensurePrincipal({ principal_id: 'kip:principal:masked' })
      gov.createGrant({
        space_id: nexus.space,
        grantee_principal: 'kip:principal:masked',
        actions: ['read', 'search'],
        constraints: { fields: ['evidence_class'] },
      }, SYSTEM_PRINCIPAL)
      const masked = nexus.session(principalAuth('kip:principal:masked'))
      expect(hitsOf(masked.describe('SEARCH EVIDENCE "apple" LIMIT 10'))).toHaveLength(0)
    })
  })

  it('reads only the page an unnarrowed SEARCH returns', async () => {
    await withNexus('search-cost', (nexus, recorded) => {
      const people = Array.from(
        { length: 20 },
        (_, i) => `CREATE CONCEPT ?p${i} {TYPE "Person" NAME "Paging Person ${i}"}`,
      )
      nexus.execute(`MUTATE { ${people.join('\n')} }`)
      recorded.statements.length = 0
      const answer = nexus.describe('SEARCH CONCEPT "Paging" LIMIT 2') as { next_cursor?: string }
      expect(hitsOf(answer as Json)).toHaveLength(2)
      expect(answer.next_cursor).toBeDefined()
      const rowReads = recorded.statements.filter((statement) =>
        /^SELECT \* FROM concepts WHERE id = \?/.test(statement.trim()),
      )
      expect(rowReads.length).toBeLessThanOrEqual(3)
      expect(
        recorded.statements.filter((statement) =>
          /FROM concepts WHERE space = \? AND state = 'active' ORDER BY id/.test(statement),
        ),
      ).toHaveLength(0)
    })
  })
})
