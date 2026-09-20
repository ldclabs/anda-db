import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../src/nexus.js'
import { principalAuth } from '../src/governance/index.js'
import { parseElementId } from '../src/id.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import type { ActivityRow, AssertionRow, EvidenceRow } from '../src/store/index.js'

/**
 * `TRANSITION` — the one lifecycle statement (Spec §52.5).
 *
 * The quoted state names the move; the engine validates it against the
 * target's kind and current state. These tests pin the table in §52.5 row by
 * row, and the two properties that make it one statement rather than six:
 * a `:parameter` state pays exactly what a literal one does, and `WHERE` /
 * `LIMIT` / the trailing guards apply to every state alike.
 */
async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus) => void,
): Promise<void> {
  const stub = env.KIP_DB.getByName(`transition-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    body(nexus)
  })
}

const SETUP = `MUTATE {
  CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
  CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
  ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
  CREATE ASSERTION ?a {
    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 }
  }
}`

function assertion(nexus: CognitiveNexus, id: string): AssertionRow {
  const element = nexus.store.load(parseElementId(id))
  if (element?.kind !== 'Assertion') throw new Error(`${id} is not an Assertion`)
  return element.row
}

function refused(body: () => unknown): { code: string; details?: unknown } {
  try {
    body()
  } catch (err) {
    return err as { code: string; details?: unknown }
  }
  throw new Error('expected a refusal')
}

describe('TRANSITION', () => {
  it('retracts from active only, and repeats as no_effect', async () => {
    await withNexus('retract', (nexus) => {
      nexus.execute(SETUP)
      const first = nexus.execute('TRANSITION "A-1" TO "retracted"')
      expect(first.status).toBe('committed')
      expect(first.changes).toEqual([
        expect.objectContaining({
          op: 'lifecycle',
          kind: 'assertion',
          id: 'A-1',
          old_version: 1,
          new_version: 2,
          state: { from: 'active', to: 'retracted' },
          refs: { proposition: 'P-1' },
        }),
      ])
      expect(assertion(nexus, 'A-1').retracted_at).not.toBe('')
      // Retraction is an epistemic status, not a deletion (§57.3): the engine
      // state does not move.
      expect(assertion(nexus, 'A-1').state).toBe('active')

      // §34.4: a move to the state already held is no_effect — no version
      // bump, no envelope entry, no Space sequence.
      const again = nexus.execute('TRANSITION "A-1" TO "retracted"')
      expect(again.status).toBe('no_effect')
      expect(again.changes).toEqual([])
      expect(assertion(nexus, 'A-1').version).toBe(2)
      expect(nexus.store.currentSeq(nexus.space)).toBe(2)
    })
  })

  it('refuses a state that does not fit the kind, naming the move', async () => {
    await withNexus('wrong-kind', (nexus) => {
      nexus.execute(SETUP)
      // §52.5: the move is judged against the kind and the current state, and
      // the refusal names both ends so an Agent can see what it asked for.
      const assertionToRunning = refused(() =>
        nexus.execute('TRANSITION "A-1" TO "running"'),
      )
      expect(assertionToRunning.code).toBe('InvalidLifecycleTransition')
      expect(assertionToRunning.details).toEqual({ from: 'active', to: 'running' })

      const conceptToRetracted = refused(() =>
        nexus.execute('TRANSITION "C-1" TO "retracted"'),
      )
      expect(conceptToRetracted.code).toBe('InvalidLifecycleTransition')
      expect(conceptToRetracted.details).toEqual({ from: 'active', to: 'retracted' })

      const evidenceToSuperseded = refused(() =>
        nexus.execute('TRANSITION "P-1" TO "corrected" BY "P-1"'),
      )
      expect(evidenceToSuperseded.code).toBe('InvalidLifecycleTransition')

      // Nothing moved on the way to refusing.
      expect(assertion(nexus, 'A-1').status).toBe('active')
      expect(assertion(nexus, 'A-1').version).toBe(1)
    })
  })

  it('supersedes within one Proposition, from active only', async () => {
    await withNexus('supersede', (nexus) => {
      nexus.execute(SETUP)
      nexus.execute(`CREATE ASSERTION ?b {
        SET FIELDS { proposition: "P-1", asserted_by: "C-1", stance: "reject", mode: "stated" }
      }`)
      const moved = nexus.execute('TRANSITION "A-1" TO "superseded" BY "A-2"')
      expect(moved.status).toBe('committed')
      expect(assertion(nexus, 'A-1').status).toBe('superseded')
      expect(assertion(nexus, 'A-1').superseded_by).toEqual(['A-2'])
      expect(assertion(nexus, 'A-2').supersedes).toEqual(['A-1'])
      // Both ends of the lineage are in the envelope: the newer Assertion
      // gained a link, the older one moved.
      expect(moved.changes.map((c) => [c.id, c.op])).toEqual([
        ['A-1', 'lifecycle'],
        ['A-2', 'lifecycle'],
      ])

      // Saying so again is no_effect.
      expect(nexus.execute('TRANSITION "A-1" TO "superseded" BY "A-2"').status).toBe(
        'no_effect',
      )

      // Superseded is not a state a second supersession is legal from, and
      // neither is retracted (§57.4).
      nexus.execute(`CREATE ASSERTION ?c {
        SET FIELDS { proposition: "P-1", asserted_by: "C-1", stance: "support", mode: "stated" }
      }`)
      expect(
        refused(() => nexus.execute('TRANSITION "A-1" TO "superseded" BY "A-3"')).code,
      ).toBe('InvalidLifecycleTransition')

      // Self, and another Proposition, are mismatches rather than moves.
      expect(
        refused(() => nexus.execute('TRANSITION "A-3" TO "superseded" BY "A-3"')).code,
      ).toBe('SupersessionMismatch')
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?light { TYPE "Preference" NAME "Light" }
        ENSURE PROPOSITION ?q ({id: "C-1"}, "prefers", ?light)
        CREATE ASSERTION ?d { SET FIELDS { proposition: ?q, asserted_by: "C-1", stance: "support", mode: "stated" } }
      }`)
      expect(
        refused(() => nexus.execute('TRANSITION "A-3" TO "superseded" BY "A-4"')).code,
      ).toBe('SupersessionMismatch')
    })
  })

  it('corrects Evidence by a newer record, from active only', async () => {
    await withNexus('correct', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE EVIDENCE ?old { SET FIELDS { evidence_class: "observation", payload: "wrong" } }
        CREATE EVIDENCE ?new { SET FIELDS { evidence_class: "observation", payload: "right" } }
      }`)
      const moved = nexus.execute('TRANSITION "E-1" TO "corrected" BY "E-2"')
      expect(moved.status).toBe('committed')
      const older = nexus.store.load(parseElementId('E-1'))?.row as EvidenceRow
      const newer = nexus.store.load(parseElementId('E-2'))?.row as EvidenceRow
      expect(older.status).toBe('corrected')
      expect(older.corrected_by).toEqual(['E-2'])
      expect(newer.corrects).toEqual(['E-1'])
      // The old payload is never overwritten (§57.2).
      expect(older.payload_inline).toBe('wrong')

      expect(nexus.execute('TRANSITION "E-1" TO "corrected" BY "E-2"').status).toBe(
        'no_effect',
      )
      expect(
        refused(() => nexus.execute('TRANSITION "E-2" TO "corrected" BY "E-2"')).code,
      ).toBe('EvidenceCorrectionConflict')
      nexus.execute('CREATE EVIDENCE ?e { SET FIELDS { evidence_class: "observation", payload: "x" } }')
      expect(
        refused(() => nexus.execute('TRANSITION "E-1" TO "corrected" BY "E-3"')).code,
      ).toBe('InvalidLifecycleTransition')
    })
  })

  it('moves an Activity through its lifecycle and finalizes it on the way', async () => {
    await withNexus('activity', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE EVIDENCE ?e { SET FIELDS { evidence_class: "observation", payload: "seen" } }
        CREATE ACTIVITY ?x { SET FIELDS { activity_class: "extraction" } }
      }`)
      const activity = () => nexus.store.load(parseElementId('X-1'))?.row as ActivityRow
      expect(activity().status).toBe('pending')

      expect(nexus.execute('TRANSITION "X-1" TO "running"').status).toBe('committed')
      expect(activity().status).toBe('running')
      // `running` is legal from `pending` only (§16): repeating it is
      // no_effect, and there is no way back.
      expect(nexus.execute('TRANSITION "X-1" TO "running"').status).toBe('no_effect')

      // A terminal move finalizes fields and topology in the same statement
      // (§52.5), and the entry names what it finalized.
      const done = nexus.execute(`TRANSITION "X-1" TO "completed"
        SET FIELDS { ended_at: "2026-09-01T00:00:00.000Z", parameters_digest: "sha3-256:abc" }
        SET STRUCTURAL { ("outputs", "E-1") }`)
      expect(done.status).toBe('committed')
      const row = activity()
      expect(row.status).toBe('completed')
      expect(row.ended_at).toBe('2026-09-01T00:00:00.000Z')
      expect(row.parameters_digest).toBe('sha3-256:abc')
      expect(row.outputs).toEqual([{ id: 'E-1' }])
      expect(done.changes[0]).toEqual(
        expect.objectContaining({
          op: 'lifecycle',
          state: { from: 'running', to: 'completed' },
          touched: expect.arrayContaining([
            'fields.ended_at',
            'fields.parameters_digest',
            'structural.outputs',
          ]),
        }),
      )

      // Terminal topology freezes with the Activity (§16.6).
      expect(refused(() => nexus.execute('TRANSITION "X-1" TO "running"')).code).toBe(
        'ActivityTerminal',
      )
      expect(refused(() => nexus.execute('TRANSITION "X-1" TO "failed"')).code).toBe(
        'ActivityTerminal',
      )
      // The status is what TO names, never a field to set.
      nexus.execute('CREATE ACTIVITY ?y { SET FIELDS { activity_class: "extraction" } }')
      expect(
        refused(() =>
          nexus.execute('TRANSITION "X-2" TO "completed" SET FIELDS { status: "failed" }'),
        ).code,
      ).toBe('InvalidSyntax')
      // A terminal state is legal straight from pending, and stamps ended_at.
      expect(nexus.execute('TRANSITION "X-2" TO "cancelled"').status).toBe('committed')
      expect((nexus.store.load(parseElementId('X-2'))?.row as ActivityRow).ended_at).not.toBe('')
    })
  })

  it('archives and tombstones any element, and will not archive a tombstone', async () => {
    await withNexus('archive', (nexus) => {
      nexus.execute(SETUP)
      const archived = nexus.execute('TRANSITION "C-2" TO "archived"')
      expect(archived.changes[0]).toEqual(
        expect.objectContaining({
          op: 'lifecycle',
          kind: 'concept',
          schema_ref: expect.stringContaining('/Preference'),
          state: { from: 'active', to: 'archived' },
        }),
      )
      expect(nexus.execute('TRANSITION "C-2" TO "archived"').status).toBe('no_effect')
      // Archived to tombstoned is a further removal; the other way would be a
      // resurrection (§60.1, §60.2).
      expect(nexus.execute('TRANSITION "C-2" TO "tombstoned"').status).toBe('committed')
      const back = refused(() => nexus.execute('TRANSITION "C-2" TO "archived"'))
      expect(back.code).toBe('InvalidLifecycleTransition')
      expect(back.details).toEqual({ from: 'tombstoned', to: 'archived' })
      // Archiving an Assertion does not retract the claim (§60.1).
      nexus.execute('TRANSITION "A-1" TO "archived"')
      expect(assertion(nexus, 'A-1').status).toBe('active')
      expect(assertion(nexus, 'A-1').state).toBe('archived')
    })
  })

  it('selects with WHERE and LIMIT for every state, in ascending id order', async () => {
    await withNexus('sweep', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?a { TYPE "Person" NAME "A" }
        CREATE CONCEPT ?b { TYPE "Person" NAME "B" }
        CREATE CONCEPT ?c { TYPE "Person" NAME "C" }
      }`)
      const swept = nexus.execute(
        'TRANSITION ?p TO "archived" WHERE { ?p CONCEPT {type: "Person"} } LIMIT 2',
      )
      expect(swept.changes.map((c) => c.id)).toEqual(['C-1', 'C-2'])
      expect(nexus.query('FIND(?p.name) WHERE { ?p CONCEPT {type: "Person"} }')).toEqual(['C'])

      // A selection that matches nothing is a no_effect, not an error.
      expect(
        nexus.execute('TRANSITION ?p TO "tombstoned" WHERE { ?p CONCEPT {name: "Nobody"} }')
          .status,
      ).toBe('no_effect')
    })
  })

  it('checks the trailing guards on each selected target', async () => {
    await withNexus('guards', (nexus) => {
      nexus.execute(SETUP)
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')
      // C-1 is at version 2 and C-2 at 1: a guard of 1 fails on the first
      // target the sweep reaches, and nothing moves.
      const stale = refused(() =>
        nexus.execute(
          'TRANSITION ?c TO "archived" WHERE { ?c CONCEPT {} } EXPECT VERSION 1',
        ),
      )
      expect(stale.code).toBe('VersionConflict')
      expect(nexus.query('FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }')).toEqual([2])

      // A plane guard names the plane it refused (§35.1).
      const plane = refused(() =>
        nexus.execute('TRANSITION "C-1" TO "archived" EXPECT VERSION 0 OF ATTRIBUTES'),
      )
      expect(plane.code).toBe('VersionConflict')
      expect(plane.details).toEqual({ plane: 'attributes' })

      // The right guards pass, and a lifecycle move advances the version but
      // no plane.
      expect(
        nexus.execute(
          'TRANSITION "C-1" TO "archived" EXPECT VERSION 2 EXPECT VERSION 2 OF ATTRIBUTES EXPECT VERSION 0 OF STRUCTURAL',
        ).status,
      ).toBe('committed')
      expect(
        nexus.query(
          'FIND(?c._system.version, ?c._system.plane_versions.attributes) WHERE { ?c CONCEPT {id: "C-1", state: "archived"} }',
        ),
      ).toEqual([[3, 2]])
    })
  })

  it('checks a parameter state as the grammar would have checked a literal', async () => {
    await withNexus('param-state', (nexus) => {
      nexus.execute(SETUP)
      const parsed = (command: string, params: Record<string, unknown>) =>
        refused(() => nexus.execute(command, params as never))

      // The registry (§52.5), at the first moment the value exists.
      expect(parsed('TRANSITION "A-1" TO :s', { s: 'succeeded' }).code).toBe(
        'ConstraintViolation',
      )
      // BY exactly on superseded / corrected; SET only on an Activity state.
      expect(parsed('TRANSITION "A-1" TO :s BY "A-1"', { s: 'retracted' }).code).toBe(
        'InvalidSyntax',
      )
      expect(parsed('TRANSITION "A-1" TO :s', { s: 'superseded' }).code).toBe('InvalidSyntax')
      expect(
        parsed('TRANSITION "A-1" TO :s SET FIELDS { ended_at: "2026-01-01T00:00:00.000Z" }', {
          s: 'archived',
        }).code,
      ).toBe('InvalidSyntax')

      // And a legal parameter state runs exactly as the literal would.
      expect(nexus.execute('TRANSITION "A-1" TO :s', { s: 'retracted' }).status).toBe(
        'committed',
      )
      expect(assertion(nexus, 'A-1').status).toBe('retracted')
    })
  })

  it('authorizes a parameter state per element with the permission the literal pays', async () => {
    await withNexus('param-permission', (nexus) => {
      nexus.execute(SETUP)
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['read', 'update', 'archive'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))
      // The gate could not classify `:s`, so it asked for nothing extra; the
      // executor asks `tombstone` per element once the state is bound, and
      // this Grant does not confer it.
      const refusedMove = refused(() =>
        session.execute('TRANSITION "C-2" TO :s', { s: 'tombstoned' }),
      )
      expect(refusedMove.code).toBe('NotAuthorized')
      expect(nexus.store.load(parseElementId('C-2'))?.row.state).toBe('active')
      // `archive` is conferred, so the same statement with that state runs.
      expect(session.execute('TRANSITION "C-2" TO :s', { s: 'archived' }).status).toBe(
        'committed',
      )
    })
  })

  it('keeps the standing and moderation rules of the moves it replaced', async () => {
    await withNexus('standing', (nexus) => {
      nexus.execute(SETUP)
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['read', 'retract_own', 'archive'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))
      // §57.3: the agent neither wrote A-1 nor represents Alice, so it cannot
      // record that she withdrew the claim…
      expect(refused(() => session.execute('TRANSITION "A-1" TO "retracted"')).code).toBe(
        'RetractionNotAuthorized',
      )
      // …and excluding somebody else's claim from recall is moderation, which
      // `archive` alone does not buy (§29).
      expect(refused(() => session.execute('TRANSITION "A-1" TO "archived"')).code).toBe(
        'NotAuthorized',
      )
    })
  })

  it('lowers ASSERT ... SUPERSEDING to a superseded transition', async () => {
    await withNexus('sugar', (nexus) => {
      nexus.execute(SETUP)
      const outcome = nexus.execute(
        'ASSERT ({id: "C-1"}, "prefers", {id: "C-2"}) { by: {id: "C-1"}, mode: "stated", stance: "reject" } SUPERSEDING "A-1"',
      )
      expect(outcome.status).toBe('committed')
      expect(assertion(nexus, 'A-1').status).toBe('superseded')
      expect(assertion(nexus, 'A-1').superseded_by).toEqual(['A-2'])
      expect(assertion(nexus, 'A-2').supersedes).toEqual(['A-1'])
    })
  })

  it('leaves ordinary recall only from a state that still holds it', async () => {
    // §60.1, §60.2 and the table in §52.5: `archived` is legal from `active`
    // and `tombstoned` from `active` or `archived`. A merged-away identity, a
    // quarantined element and a purged stub are engine states with their own
    // exits — archiving out of one would overwrite the reason the element is
    // where it is, and the other reference engine refuses the same moves.
    await withNexus('removal-legality', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?a { TYPE "Person" NAME "Alicia" }
        CREATE CONCEPT ?b { TYPE "Person" NAME "Alice" }
      }`)
      nexus.execute('MERGE CONCEPT "C-1" INTO "C-2"')
      const merged = refused(() => nexus.execute('TRANSITION "C-1" TO "tombstoned"'))
      expect(merged.code).toBe('InvalidLifecycleTransition')
      expect(merged.details).toEqual({ from: 'merged', to: 'tombstoned' })
      expect(refused(() => nexus.execute('TRANSITION "C-1" TO "archived"')).code).toBe(
        'InvalidLifecycleTransition',
      )

      // An Activity that has left recall has no lifecycle left to move: a
      // finalizing transition would write provenance into an element a reader
      // is no longer meant to reach.
      nexus.execute('CREATE ACTIVITY ?x { SET FIELDS { activity_class: "extraction" } }')
      nexus.execute('TRANSITION "X-1" TO "archived"')
      const frozen = refused(() => nexus.execute('TRANSITION "X-1" TO "running"'))
      expect(frozen.code).toBe('InvalidLifecycleTransition')
      expect(frozen.details).toEqual({ from: 'archived', to: 'running' })
    })
  })

  it('refuses the removed statements at the grammar', async () => {
    await withNexus('removed', (nexus) => {
      nexus.execute(SETUP)
      for (const command of [
        'RETRACT ASSERTION "A-1"',
        'SUPERSEDE ASSERTION "A-1" BY "A-1"',
        'CORRECT EVIDENCE "E-1" BY "E-2"',
        'TRANSITION ACTIVITY "X-1" TO "running"',
        'ARCHIVE "C-1"',
        'TOMBSTONE "C-1"',
        'TRANSITION "A-1" TO "retracted" EXPECT STATE "active"',
      ]) {
        const result = nexus.tryExecute(command)
        expect('error' in result && result.error.code, command).toBe('InvalidSyntax')
      }
    })
  })
})
