import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { SYSTEM_PRINCIPAL, principalAuth } from '../src/governance/index.js'
import { KipError } from '../src/errors.js'
import type { RecordingRepair } from '../src/repair.js'
import type { Json, JsonMap } from '../src/json.js'

/**
 * Recording repair (Spec §57.8): what `KIP2-REL-004` and the replacement
 * variant of `KIP2-MIF-017` ask of the engine.
 *
 * @see rs/anda_cognitive_nexus/tests/recording_repair.rs — the same cases
 */

const DIGEST = 'sha256:5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e'

async function withNexus(name: string, body: (nexus: CognitiveNexus) => void): Promise<void> {
  const stub = env.KIP_DB.getByName(`repair-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    nexus.execute('DEFINE PREDICATE "diet" {description: "What the subject eats."}')
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

type Executor = { execute(command: string, params?: JsonMap): { handles: Record<string, string> } }

function misrecord(executor: Executor): { alice: string; source: string; wrong: string } {
  const { handles } = executor.execute(
    `MUTATE {
      UPSERT CONCEPT ?alice { MATCH {type: "Person", key: "alice"} SET FIELDS {name: "Alice"} }
      CREATE EVIDENCE ?msg {
        SET FIELDS {
          evidence_class: "message", payload: :payload,
          content_digest: :digest, observed_at: "2026-01-01T00:00:00.000Z"
        }
      }
      ASSERT ?wrong (?alice, "diet", "vegetarian") {
        by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: ?msg
      }
    }`,
    {
      digest: DIGEST,
      payload: { role: 'user', content: 'I am not vegetarian; I just skipped meat today.' },
    },
  )
  return { alice: handles.alice!, source: handles.msg!, wrong: handles.wrong! }
}

function version(nexus: CognitiveNexus, id: string): number {
  return nexus.query('FIND(?a._system.version) WHERE { ?a ASSERTION {id: :id} }', { id })[0] as number
}

function repair(source: string, wrong: string, v: number, replacements: string[] = []): RecordingRepair {
  return {
    source_ref: source,
    source_digest: DIGEST,
    source_locator: '/content',
    invalidated_refs: [wrong],
    replacement_refs: replacements,
    reason: 'extraction_error',
    expected_versions: { [wrong]: v },
  }
}

function slot(nexus: CognitiveNexus, subject: string, predicate: string, at?: string): JsonMap {
  const time = at === undefined ? '' : ` FOR TIME "${at}"`
  return nexus.query(`FIND(?slot) WHERE { ?slot BELIEF SLOT (:s, "${predicate}") }${time}`, { s: subject })[0] as JsonMap
}

describe('recording repair', () => {
  it('repairs a misrecording without an actor withdrawal', async () => {
    await withNexus('repair', (nexus) => {
      const { alice, source, wrong } = misrecord(nexus)
      expect(slot(nexus, alice, 'diet').status).toBe('accepted')
      const original = version(nexus, wrong)
      const beforeSeq = (nexus.describe('DESCRIBE SPACE') as JsonMap).seq as number
      const replacement = nexus.execute(
        `MUTATE {
          ASSERT ?right (:alice, "diet", "vegetarian") {
            by: :alice, mode: "stated", stance: "reject", confidence: 0.9,
            at: "2026-01-01T00:00:00.000Z", evidence: :msg
          }
        }`,
        { alice: { id: alice }, msg: source },
      ).handles.right!

      const session = nexus.systemSession()
      const result = session.repairRecording(repair(source, wrong, original, [replacement]))
      const repairRef = result.repair_ref as string
      const receipt = result.receipt as JsonMap
      expect(receipt.space_seq as number).toBeGreaterThan(beforeSeq)
      const described = nexus.describe(`DESCRIBE TRANSACTION "${receipt.tx_id as string}"`) as JsonMap
      expect(described.control_changes).toContainEqual(expect.objectContaining({ kind: 'recording' }))

      const after = slot(nexus, alice, 'diet')
      expect(after.accepted_values).toEqual([])
      const candidate = (after.candidate_projections as JsonMap[])[0]!
      expect(candidate.candidate_status).toBe('rejected')
      expect((candidate.explanation as JsonMap).excluded).toContainEqual({
        assertion_id: wrong,
        reason: 'recording_invalidated',
      })

      const raw = nexus.query('FIND(?a) WHERE { ?a ASSERTION {id: :id} }', { id: wrong })[0] as JsonMap
      expect(raw.stance).toBe('support')
      expect((raw.lifecycle as JsonMap).status).toBe('active')
      const system = raw._system as JsonMap
      expect(system.version).toBe(original + 1)
      expect(system.recording_validity).toEqual({ status: 'invalidated', repair_ref: repairRef })
      expect(
        nexus.query('FIND(?a._system.recording_validity) WHERE { ?a ASSERTION {id: :id} }', { id: replacement })[0],
      ).toEqual({ status: 'valid', repair_ref: null })

      const activity = nexus.query('FIND(?x) WHERE { ?x ACTIVITY {id: :id} }', { id: repairRef })[0] as JsonMap
      expect(activity.activity_class).toBe('recording_repair')
      expect(activity.status).toBe('completed')
      expect(activity.inputs).toEqual([{ id: source }, { id: wrong }])
      const record = (activity.facets as JsonMap)['kip://profiles/cognitive-memory@2.0.0/RecordingRepair'] as JsonMap
      expect(record.replacement_refs).toEqual([replacement])

      const past = nexus.query(
        `FIND(?a._system.recording_validity.status) WHERE { ?a ASSERTION {id: :id} } AS OF SEQ ${beforeSeq}`,
        { id: wrong },
      )
      expect(past[0]).toBe('valid')

      const seq = (nexus.describe('DESCRIBE SPACE') as JsonMap).seq
      const replay = session.repairRecording(repair(source, wrong, original, [replacement]))
      expect(replay).toMatchObject({ repair_ref: repairRef, replayed: true })
      expect((nexus.describe('DESCRIBE SPACE') as JsonMap).seq).toBe(seq)
    })
  })

  it('lets only the recorder repair, and only against the exact source', async () => {
    await withNexus('guards', (nexus) => {
      const { source, wrong } = misrecord(nexus)
      const current = version(nexus, wrong)
      const session = nexus.systemSession()
      expect(codeOf(() => session.repairRecording({ ...repair(source, wrong, current), source_digest: `sha256:${'0'.repeat(64)}` }))).toBe('DigestMismatch')
      expect(codeOf(() => session.repairRecording({ ...repair(source, wrong, current), source_locator: '/missing' }))).toBe('ConstraintViolation')
      expect(codeOf(() => session.repairRecording(repair(source, wrong, current + 1)))).toBe('VersionConflict')

      const gov = nexus.store.governance
      for (const [principal, actions] of [
        ['kip:principal:other', ['discover', 'read', 'create', 'update', 'assert', 'record_attributed_assertion', 'repair_recording']],
        ['kip:principal:reader', ['discover', 'read']],
      ] as const) {
        gov.ensurePrincipal({ principal_id: principal })
        gov.createGrant({ space_id: nexus.space, grantee_principal: principal, actions: [...actions] }, SYSTEM_PRINCIPAL)
      }
      const other = nexus.session(principalAuth('kip:principal:other'))
      const reader = nexus.session(principalAuth('kip:principal:reader'))
      expect(codeOf(() => other.repairRecording(repair(source, wrong, current)))).toBe('NotAuthorized')
      expect(codeOf(() => reader.repairRecording(repair(source, wrong, current)))).toBe('NotAuthorized')

      const foreign = other.execute(
        `MUTATE {
          UPSERT CONCEPT ?alice { MATCH {type: "Person", key: "alice"} }
          ASSERT ?r (?alice, "diet", "omnivore") {
            by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: :msg
          }
        }`,
        { msg: source },
      ).handles.r!
      expect(codeOf(() => session.repairRecording(repair(source, wrong, current, [foreign])))).toBe('NotAuthorized')
      expect(version(nexus, wrong)).toBe(current)

      expect(
        codeOf(() =>
          nexus.execute('MUTATE { CREATE ACTIVITY ?x { SET FIELDS {activity_class: "recording_repair", status: "completed"} } }'),
        ),
      ).toBe('NotAuthorized')
    })
  })

  it('leaves an unreplaced slot insufficient, never "no"', async () => {
    await withNexus('own', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:recorder' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:recorder',
          actions: ['discover', 'read', 'create', 'update', 'assert', 'record_attributed_assertion', 'repair_recording'],
        },
        SYSTEM_PRINCIPAL,
      )
      const recorder = nexus.session(principalAuth('kip:principal:recorder'))
      const { alice, source, wrong } = misrecord(recorder)
      recorder.repairRecording(repair(source, wrong, version(nexus, wrong)))
      expect(slot(nexus, alice, 'diet').status).toBe('insufficient')
    })
  })

  it('keeps the original claim time, so a later genuine change still ends it', async () => {
    await withNexus('replacement', (nexus) => {
      nexus.execute('DEFINE CONCEPT TYPE "ColorScheme" {description: "A display color scheme."}')
      const { handles } = nexus.execute(
        `MUTATE {
          UPSERT CONCEPT ?alice { MATCH {type: "Person", key: "alice"} SET FIELDS {name: "Alice"} }
          UPSERT CONCEPT ?dark { MATCH {type: "ColorScheme", key: "dark"} SET FIELDS {name: "Dark"} }
          UPSERT CONCEPT ?blue { MATCH {type: "ColorScheme", key: "blue"} SET FIELDS {name: "Blue"} }
          UPSERT CONCEPT ?light { MATCH {type: "ColorScheme", key: "light"} SET FIELDS {name: "Light"} }
          CREATE EVIDENCE ?jan {
            SET FIELDS {evidence_class: "message", payload: "I prefer dark.",
                        content_digest: :digest, observed_at: "2026-01-01T00:00:00.000Z"}
          }
          CREATE EVIDENCE ?sep {
            SET FIELDS {evidence_class: "message", payload: "I prefer light now.",
                        observed_at: "2026-09-01T00:00:00.000Z"}
          }
          ASSERT ?wrong (?alice, "prefers", ?blue) {
            by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: ?jan
          }
          ASSERT ?right_light (?alice, "prefers", ?light) {
            by: ?alice, mode: "stated", at: "2026-09-01T00:00:00.000Z",
            valid: {from: "2026-09-01T00:00:00.000Z"}, evidence: ?sep
          }
        }`,
        { digest: DIGEST },
      )
      const [alice, jan, wrong, dark] = [handles.alice!, handles.jan!, handles.wrong!, handles.dark!]
      const late = nexus.execute(
        `MUTATE { ASSERT ?r (:alice, "prefers", :dark) { by: :alice, mode: "stated", at: "2026-09-24T00:00:00.000Z", evidence: :jan } }`,
        { alice: { id: alice }, dark: { id: dark }, jan },
      ).handles.r!
      const current = version(nexus, wrong)
      const session = nexus.systemSession()
      expect(
        codeOf(() => session.repairRecording({ ...repair(jan, wrong, current, [late]), source_locator: 'bytes=0-13' })),
      ).toBe('ConstraintViolation')

      const replacement = nexus.execute(
        `MUTATE { ASSERT ?r (:alice, "prefers", :dark) { by: :alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: :jan } }`,
        { alice: { id: alice }, dark: { id: dark }, jan },
      ).handles.r!
      session.repairRecording({ ...repair(jan, wrong, current, [replacement]), source_locator: 'bytes=0-13' })
      nexus.execute('TRANSITION :late TO "retracted"', { late })

      const proposition = (id: string): Json =>
        nexus.query('FIND(?a.proposition.id) WHERE { ?a ASSERTION {id: :id} }', { id })[0]!
      expect(slot(nexus, alice, 'prefers', '2026-02-01T00:00:00.000Z').accepted_values).toEqual([proposition(replacement)])
      expect(slot(nexus, alice, 'prefers', '2026-09-25T00:00:00.000Z').accepted_values).toEqual([
        proposition(handles.right_light!),
      ])
    })
  })
})
