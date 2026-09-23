import { env, runInDurableObject } from 'cloudflare:test'
import { expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { parseKip } from '../src/kip/parser.js'
import type { IngestEvidence } from '../src/kml/ingest.js'

async function withNexus(name: string, body: (nexus: CognitiveNexus) => void): Promise<void> {
  await runInDurableObject(env.KIP_DB.getByName(`ingest-identity-${name}`), (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    body(nexus)
  })
}
const original: IngestEvidence = {
  key: 'msg1', client_key: 'thread:submission:message', evidence_class: 'user_statement',
  payload: {text: 'first message'}, observed_at: '2026-09-07T00:00:00.000Z',
}
function mint(nexus: CognitiveNexus, evidence: IngestEvidence[]) {
  const command = parseKip('UPSERT CONCEPT ?c { MATCH {type: "Person", key: "actor"} SET FIELDS {name: "Actor"} }')
  if (!('Kml' in command)) throw new Error('fixture must be KML')
  return nexus.mutate(command.Kml, {}, {ingest: {evidence}})
}

it('rejects a reused key whose observation changed, while keeping retries idempotent', async () => {
  await withNexus('replay', (nexus) => {
    mint(nexus, [original]); mint(nexus, [original])
    expect(nexus.query('FIND(COUNT(?e)) WHERE { ?e EVIDENCE {} }')).toEqual([1])
    for (const conflicting of [
      {...original, payload: {text: 'changed message'}},
      {...original, evidence_class: 'tool_result'},
      {...original, observed_at: '2026-09-08T00:00:00.000Z'},
    ]) expect(() => mint(nexus, [conflicting])).toThrowError(/different observation/)
    expect(nexus.query('FIND(COUNT(?e)) WHERE { ?e EVIDENCE {} }')).toEqual([1])
  })
})

it('deduplicates identical keys within one transaction and aborts conflicting ones', async () => {
  await withNexus('batch', (nexus) => {
    mint(nexus, [original, {...original, key: 'msg2'}])
    expect(nexus.query('FIND(COUNT(?e)) WHERE { ?e EVIDENCE {} }')).toEqual([1])
    expect(() => mint(nexus, [
      {...original, client_key: 'another-observation'},
      {...original, key: 'msg2', client_key: 'another-observation', payload: 'different'},
    ])).toThrowError(/different observation/)
    expect(nexus.query('FIND(COUNT(?e)) WHERE { ?e EVIDENCE {} }')).toEqual([1])
  })
})

it('shares the tuple created by repeated ASSERT clauses in the same transaction', async () => {
  await withNexus('shared-tuple', (nexus) => {
    nexus.execute(`MUTATE {
      CREATE CONCEPT ?alice {TYPE "Person" NAME "Alice"}
      CREATE CONCEPT ?bob {TYPE "Person" NAME "Bob"}
      CREATE CONCEPT ?p {TYPE "Preference" NAME "Dark"}
      ASSERT ?a (?alice, "prefers", ?p) {by: ?alice, mode: "stated"}
      ASSERT ?b (?alice, "prefers", ?p) {by: ?bob, mode: "stated"}
    }`)
    expect(nexus.query('FIND(COUNT(?p)) WHERE { ?p (?s, ?predicate, ?o) }')).toEqual([1])
    expect(nexus.query('FIND(COUNT(?a)) WHERE { ?a ASSERTION {} }')).toEqual([2])
  })
})

it('preserves explicit null and empty payload values through ingestion and reads', async () => {
  for (const [index, payload] of [null, {}, [], ''].entries()) {
    await withNexus(`empty-payload-${index}`, nexus => {
      mint(nexus, [{...original, payload}])
      expect(nexus.query('FIND(?e.payload) WHERE { ?e EVIDENCE {} }')).toEqual([{mode:'inline', inline:payload}])
    })
  }
})
