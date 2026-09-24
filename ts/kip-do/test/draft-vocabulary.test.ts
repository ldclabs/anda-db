import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY, parsePackage } from '../src/schema/index.js'
import { SYSTEM_PRINCIPAL, principalAuth } from '../src/governance/index.js'
import { parseKip } from '../src/kip/parser.js'
import { KipError } from '../src/errors.js'
import type { Json, JsonMap } from '../src/json.js'

/**
 * The Space's draft vocabulary (§20.16): what the engine suite's
 * `draft-vocabulary.json` cannot drive — Space history, host activation,
 * idempotent retry, the `propose_schema` boundary and promotion.
 *
 * @see rs/anda_cognitive_nexus/tests/draft_vocabulary.rs — the same cases
 */

/** A package that later names what a Space drafted first. */
const MUSIC = parsePackage(JSON.stringify({
  format: 'KIP-Schema-Package',
  format_version: '2.0-draft',
  manifest: { package_id: 'kip://test/music', version: '1.0.0' },
  definitions: {
    concept_types: {
      Instrument: { kind: 'ConceptType', description: 'A musical instrument.', attributes: { open: true, fields: {} } },
    },
    predicates: {
      mentors: { kind: 'PredicateType', description: 'The subject mentors the object.' },
    },
  },
}))

const DEFINE_MENTORS = `DEFINE PREDICATE "mentors" {
  description: "The subject mentors the object.",
  subject: {concept_types: ["Person"]},
  object: {concept_types: ["Person"]}
}`

async function withNexus(name: string, body: (nexus: CognitiveNexus) => void): Promise<void> {
  const stub = env.KIP_DB.getByName(`draft-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    body(nexus)
  })
}

function codeOf(run: () => unknown): string {
  try {
    run()
  } catch (err) {
    return KipError.from(err).code
  }
  return 'no error'
}

describe('the draft vocabulary', () => {
  it('is a schema transaction in the Space history', async () => {
    await withNexus('history', (nexus) => {
      const before = (nexus.describe('DESCRIBE SCHEMA ENVIRONMENT') as JsonMap).version as number
      const outcome = nexus.execute(DEFINE_MENTORS)
      expect(outcome.result).toMatchObject({
        ref: 'kip://local/draft@0.0.0/mentors',
        schema_environment_version: before + 1,
      })
      const described = nexus.describe(`DESCRIBE TRANSACTION "${outcome.tx_id}"`) as JsonMap
      expect(described.control_changes).toContainEqual(expect.objectContaining({ kind: 'schema' }))

      const now = nexus.describe('DESCRIBE SCHEMA ENVIRONMENT') as JsonMap
      expect(now.packages).toContain('kip://local/draft@0.0.0')
      const past = nexus.describe(
        `DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ ${(outcome.space_seq as number) - 1}`,
      ) as JsonMap
      expect(past.packages).not.toContain('kip://local/draft@0.0.0')

      const pkg = nexus.describe('DESCRIBE PACKAGE "kip://local/draft@0.0.0"') as JsonMap
      expect(pkg.status).toBe('active')
      expect(String(pkg.content_digest)).toMatch(/^sha256:/)
      // Endpoint types persist as exact references.
      const predicate = nexus.describe('DESCRIBE PREDICATE "mentors"') as {
        definition: { subject: { concept_types: string[] } }
      }
      expect(predicate.definition.subject.concept_types).toEqual([
        'kip://profiles/cognitive-memory@2.0.0/Person',
      ])
    })
  })

  it('survives the activation of other packages', async () => {
    await withNexus('activation', (nexus) => {
      nexus.execute(DEFINE_MENTORS)
      const version = (nexus.describe('DESCRIBE SCHEMA ENVIRONMENT') as JsonMap).version
      // A host re-asserting its baseline on start changes nothing.
      nexus.activatePackages([COGNITIVE_MEMORY])
      expect((nexus.describe('DESCRIBE SCHEMA ENVIRONMENT') as JsonMap).version).toBe(version)
      // Activating another package carries the draft forward.
      const env = nexus.activatePackages([COGNITIVE_MEMORY, MUSIC])
      expect(Object.keys(env.lock.draft?.predicates ?? {})).toEqual(['mentors'])
      expect(env.lock.states['kip://local/draft']).toBe('active')
      // And refuses to switch it off.
      const lock = structuredClone(env.lock)
      lock.states['kip://local/draft'] = 'deprecated'
      expect(codeOf(() => nexus.ensureSchema(lock))).toBe('ConstraintViolation')
    })
  })

  it('replays a retry and refuses a repeat', async () => {
    await withNexus('retry', (nexus) => {
      const session = nexus.systemSession()
      const parsed = parseKip(DEFINE_MENTORS)
      if (!('Kml' in parsed)) throw new Error('DEFINE is KML')
      const first = session.mutate(parsed.Kml, {}, { idempotencyKey: 'define-mentors' })
      const again = session.mutate(parsed.Kml, {}, { idempotencyKey: 'define-mentors' })
      expect(again.tx_id).toBe(first.tx_id)
      expect(again.result).toEqual(first.result)
      // Without a key, a repeat is a second definition of a taken name.
      expect(codeOf(() => nexus.execute(DEFINE_MENTORS))).toBe('SchemaSymbolConflict')
    })
  })

  it('confers drafting through propose_schema and nothing more', async () => {
    await withNexus('authority', (nexus) => {
      const gov = nexus.store.governance
      for (const [principal, actions] of [
        ['kip:principal:drafter', ['propose_schema', 'read', 'discover']],
        ['kip:principal:reader', ['read', 'discover']],
      ] as const) {
        gov.ensurePrincipal({ principal_id: principal })
        gov.createGrant(
          { space_id: nexus.space, grantee_principal: principal, actions: [...actions] },
          SYSTEM_PRINCIPAL,
        )
      }
      const drafter = nexus.session(principalAuth('kip:principal:drafter'))
      const reader = nexus.session(principalAuth('kip:principal:reader'))
      expect(codeOf(() => reader.execute(DEFINE_MENTORS))).toBe('NotAuthorized')
      drafter.execute(DEFINE_MENTORS)
      expect(codeOf(() => drafter.installPackage(MUSIC, 'test'))).toBe('NotAuthorized')
      expect(codeOf(() => drafter.promoteDraftSymbol('PredicateType', 'mentors', 'prefers'))).toBe(
        'NotAuthorized',
      )
    })
  })

  it('promotes by joining lineages and keeps exact references', async () => {
    await withNexus('promotion', (nexus) => {
      nexus.execute('DEFINE CONCEPT TYPE "Instrument" {description: "A musical instrument."}')
      nexus.execute(DEFINE_MENTORS)
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?violin { TYPE "Instrument" NAME "Violin" }
        CREATE CONCEPT ?ada { TYPE "Person" NAME "Ada" }
        CREATE CONCEPT ?grace { TYPE "Person" NAME "Grace" }
        ASSERT (?ada, "mentors", ?grace) { by: ?ada, mode: "stated", at: "2026-01-01T00:00:00.000Z" }
      }`)
      // A package naming the same symbols makes the local names ambiguous
      // until the drafts are promoted to it (§20.7, §20.16).
      nexus.activatePackages([COGNITIVE_MEMORY, MUSIC])
      expect(codeOf(() => nexus.query('FIND(?c) WHERE { ?c CONCEPT {type: "Instrument"} }'))).toBe(
        'SchemaSymbolAmbiguous',
      )
      const unpromoted = nexus.store.currentSeq(nexus.space)
      const session = nexus.systemSession()
      session.promoteDraftSymbol('ConceptType', 'Instrument', 'kip://test/music@1.0.0/Instrument')
      session.promoteDraftSymbol('PredicateType', 'kip://local/draft@0.0.0/mentors', 'mentors')
      expect(
        codeOf(() => session.promoteDraftSymbol('ConceptType', 'Instrument', 'Instrument')),
      ).toBe('ConstraintViolation')
      const described = nexus.describe('DESCRIBE SCHEMA ENVIRONMENT') as JsonMap
      expect(described.lineage_maps).toContainEqual({
        kind: 'ConceptType',
        from: 'kip://local/draft/Instrument',
        to: 'kip://test/music/Instrument',
      })

      nexus.execute('MUTATE { CREATE CONCEPT ?cello { TYPE "Instrument" NAME "Cello" } }')
      expect(
        nexus.query(
          'FIND(?c.name, ?c.schema_ref) WHERE { ?c CONCEPT {type: "Instrument"} } ORDER BY ?c.name ASC',
        ),
      ).toEqual([
        ['Cello', 'kip://test/music@1.0.0/Instrument'],
        ['Violin', 'kip://local/draft@0.0.0/Instrument'],
      ] as Json[])
      expect(
        nexus.query('FIND(?o.name) WHERE { ?s CONCEPT {name: "Ada"} ?p (?s, "mentors", ?o) }'),
      ).toEqual(['Grace'])
      const past = nexus.describe(`DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ ${unpromoted}`) as JsonMap
      expect(past.lineage_maps).toEqual([])
    })
  })
})
