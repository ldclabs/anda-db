import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { parseElementId } from '../src/id.js'
import { parseKip } from '../src/kip/parser.js'
import type { AssertionRow, ConceptRow, PropositionRow } from '../src/store/index.js'

/**
 * KML runs end to end, through the real parser.
 *
 * Every interesting defect this project has found came from a test written this
 * way and would have been invisible to a unit test of the same function: a
 * clause the grammar accepts, the engine ignores, and the receipt reports as
 * success. Hand-built ASTs cannot find those, because they skip the step where
 * the two disagree.
 */
async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus) => void,
): Promise<void> {
  const stub = env.KIP_DB.getByName(`kml-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    body(nexus)
  })
}

const CM = 'kip://profiles/cognitive-memory@2.0.0'

/** The setup every fixture in the conformance suite starts from. */
const SETUP = `MUTATE {
  CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
  CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
  ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
  CREATE ASSERTION ?a {
    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 }
  }
}`

function concept(nexus: CognitiveNexus, id: string): ConceptRow {
  const element = nexus.store.load(parseElementId(id))
  if (element?.kind !== 'Concept') throw new Error(`${id} is not a Concept`)
  return element.row
}

describe('KML', () => {
  it('forms a Proposition, an Assertion and their Concepts in one transaction', async () => {
    await withNexus('setup', (nexus) => {
      const outcome = nexus.execute(SETUP)
      expect(outcome.status).toBe('committed')
      expect(Object.keys(outcome.handles).sort()).toEqual(['a', 'alice', 'dark', 'p'])
      expect(outcome.changes).toHaveLength(4)

      // A Concept's type is persisted as its exact symbol, never as the local
      // name the command wrote (§13).
      expect(concept(nexus, outcome.handles.alice!).schema_ref).toBe(`${CM}/Person`)
      expect(concept(nexus, outcome.handles.alice!).name).toBe('Alice')

      const p = nexus.store.load(parseElementId(outcome.handles.p!))
      const prop = p?.row as PropositionRow
      expect(prop.predicate_ref).toBe(`${CM}/prefers`)
      expect(prop.subject).toEqual({ id: outcome.handles.alice })
      expect(prop.object).toEqual({ id: outcome.handles.dark })
      // A Proposition carries no confidence, and its absence is the point.
      expect(Object.hasOwn(prop, 'confidence')).toBe(false)

      const a = nexus.store.load(parseElementId(outcome.handles.a!))
      const assertion = a?.row as AssertionRow
      expect(assertion.confidence).toBe(0.9)
      expect(assertion.stance).toBe('support')
      expect(assertion.proposition_id).toBe(outcome.handles.p)
      // A reference, not the text `C-1`.
      expect(assertion.asserted_by).toEqual({ id: outcome.handles.alice })
      expect(assertion.asserted_by_key).toContain(outcome.handles.alice)
    })
  })

  it('starts every new element at version 1', async () => {
    await withNexus('version', (nexus) => {
      const outcome = nexus.execute(SETUP)
      for (const change of outcome.changes) expect(change.version).toBe(1)
      expect(concept(nexus, outcome.handles.alice!).version).toBe(1)
    })
  })

  it('takes one Space sequence for the whole transaction', async () => {
    await withNexus('seq', (nexus) => {
      // A transaction is one externally visible state transition, however many
      // elements it touched (§44).
      const first = nexus.execute(SETUP)
      expect(first.space_seq).toBe(1)
      expect(nexus.store.currentSeq(nexus.space)).toBe(1)
    })
  })

  it('refuses a type no active package defines, rather than inventing it', async () => {
    await withNexus('unknown-type', (nexus) => {
      const result = nexus.tryExecute(
        'CREATE CONCEPT ?x { TYPE "Spaceship" NAME "Enterprise" }',
      )
      expect('error' in result && result.error.code).toBe('SchemaSymbolNotFound')
    })
  })

  it('resolves the same tuple rather than duplicating it', async () => {
    await withNexus('ensure', (nexus) => {
      const first = nexus.execute(SETUP)
      const again = nexus.execute(`MUTATE {
        ENSURE PROPOSITION ?p ({id: "${first.handles.alice}"}, "prefers", {id: "${first.handles.dark}"})
      }`)
      // Nothing changed, so nothing is written: a Space clock that ticks for a
      // no-op makes every CHANGES cursor report a change that is not there.
      expect(again.handles.p).toBe(first.handles.p)
      expect(again.status).toBe('no_effect')
      expect(nexus.store.currentSeq(nexus.space)).toBe(1)
    })
  })

  it('rolls the whole statement back when one clause fails', async () => {
    await withNexus('rollback', (nexus) => {
      const before = nexus.store.currentSeq(nexus.space)
      const result = nexus.tryExecute(`MUTATE {
        CREATE CONCEPT ?ok { TYPE "Person" NAME "Kept" }
        CREATE CONCEPT ?bad { TYPE "Spaceship" NAME "Dropped" }
      }`)
      expect('error' in result).toBe(true)
      // Neither the good clause nor the shells its handles were minted from
      // survive: `transactionSync` unwinds the statement whole.
      expect(nexus.store.currentSeq(nexus.space)).toBe(before)
      expect(
        nexus.store.sql
          .exec<{ n: number }>('SELECT COUNT(*) AS n FROM concepts')
          .toArray()[0]?.n,
      ).toBe(0)
    })
  })

  it('lets a forward reference close a provenance cycle in one transaction', async () => {
    await withNexus('cycle', (nexus) => {
      // Evidence.generated_by → Activity and Activity.outputs → Evidence is a
      // legitimate structural cycle; a define-before-use ordering could not
      // express it at all.
      const outcome = nexus.execute(`MUTATE {
        CREATE EVIDENCE ?e {
          SET FIELDS { evidence_class: "observation", payload: {inline: "seen"} }
          SET STRUCTURAL { ("generated_by", ?x) }
        }
        CREATE ACTIVITY ?x {
          SET FIELDS { activity_class: "extraction" }
          SET STRUCTURAL { ("outputs", ?e) }
        }
      }`)
      expect(outcome.status).toBe('committed')
      const e = nexus.store.load(parseElementId(outcome.handles.e!))
      const x = nexus.store.load(parseElementId(outcome.handles.x!))
      expect((e?.row as { generated_by: string }).generated_by).toBe(outcome.handles.x)
      expect((x?.row as { outputs: unknown[] }).outputs).toEqual([
        { id: outcome.handles.e },
      ])
    })
  })

  it('refuses a handle declared twice', async () => {
    await withNexus('dup-handle', (nexus) => {
      // The grammar catches this one before the engine sees it. The engine
      // keeps its own check because a handle bound twice leaves every
      // reference to it ambiguous, and a lowering that stopped rejecting it
      // would otherwise turn into the engine picking a winner.
      const result = nexus.tryExecute(`MUTATE {
        CREATE CONCEPT ?x { TYPE "Person" NAME "A" }
        CREATE CONCEPT ?x { TYPE "Person" NAME "B" }
      }`)
      expect('error' in result && result.error.code).toBe('InvalidSyntax')
    })
  })

  it('keeps a bare string a Literal and an object a reference', async () => {
    await withNexus('literal-vs-ref', (nexus) => {
      const outcome = nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        ENSURE PROPOSITION ?p (?alice, "prefers", "dark")
      }`)
      const prop = nexus.store.load(parseElementId(outcome.handles.p!))
        ?.row as PropositionRow
      // The object is the *text* "dark", stored as an explicit Literal so a
      // Schema-refined datatype would survive a round trip.
      expect(prop.object).toEqual({ value: 'dark', datatype: 'kip:string' })
      expect(prop.subject).toEqual({ id: outcome.handles.alice })
    })
  })

  it('records a structural reference passed as a parameter as a reference', async () => {
    await withNexus('param-ref', (nexus) => {
      const first = nexus.execute(SETUP)
      const outcome = nexus.execute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, asserted_by: :who, stance: "support", mode: "inferred" }
         }`,
        { p: first.handles.p!, who: { id: first.handles.alice! } },
      )
      const assertion = nexus.store.load(parseElementId(outcome.handles.a!))
        ?.row as AssertionRow
      expect(assertion.proposition_id).toBe(first.handles.p)
      // A parameter carrying `"C-1"` in a reference slot is normalized rather
      // than stored verbatim: an edge nothing can traverse would be a defect
      // the write reports as success.
      expect(assertion.asserted_by).toEqual({ id: first.handles.alice })
    })
  })

  it('refuses a field the element kind does not have, rather than dropping it', async () => {
    await withNexus('unknown-field', (nexus) => {
      const first = nexus.execute(SETUP)
      const result = nexus.tryExecute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, stance: "support", mode: "stated", nonsense: 1 }
         }`,
        { p: first.handles.p! },
      )
      expect('error' in result && result.error.code).toBe('SchemaFieldNotFound')
    })
  })

  it('refuses cognitive content that tries to write engine state', async () => {
    await withNexus('protected', (nexus) => {
      // `_system` and `governance` record what the runtime and the control
      // plane observed. Content that could set them would be laundering
      // provenance and granting itself authority.
      for (const field of ['_system', 'governance', 'space_id']) {
        const result = nexus.tryExecute(
          `CREATE CONCEPT ?c { TYPE "Person" NAME "X" SET FIELDS { ${field}: {} } }`,
        )
        expect('error' in result, field).toBe(true)
      }
    })
  })

  it('keeps confidence inside [0, 1]', async () => {
    await withNexus('confidence', (nexus) => {
      const first = nexus.execute(SETUP)
      const result = nexus.tryExecute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, stance: "support", mode: "stated", confidence: 1.5 }
         }`,
        { p: first.handles.p! },
      )
      expect('error' in result && result.error.code).toBe('TypeMismatch')
    })
  })

  it('validates a Facet against its definition and stores it by exact symbol', async () => {
    await withNexus('facets', (nexus) => {
      const ok = nexus.execute(`CREATE CONCEPT ?c {
        TYPE "Person" NAME "Alice"
        SET FACET "MnemonicState" { memory_strength: 0.7 }
      }`)
      expect(concept(nexus, ok.handles.c!).facets).toEqual({
        [`${CM}/MnemonicState`]: { memory_strength: 0.7 },
      })

      // A Facet is a validated namespaced extension, not the untyped metadata
      // bag KIP 1.x had.
      const bad = nexus.tryExecute(`CREATE CONCEPT ?c {
        TYPE "Person" NAME "Bob"
        SET FACET "MnemonicState" { salience: 1.5 }
      }`)
      expect('error' in bad && bad.error.code).toBe('ConstraintViolation')
    })
  })

  it('retracts a claim without touching the record', async () => {
    await withNexus('retract', (nexus) => {
      const first = nexus.execute(SETUP)
      const outcome = nexus.execute(`RETRACT ASSERTION "${first.handles.a}"`)
      expect(outcome.status).toBe('committed')
      const assertion = nexus.store.load(parseElementId(first.handles.a!))
        ?.row as AssertionRow
      // Retraction is an epistemic status, not a deletion: the record stays
      // active and citable, and only the claim is withdrawn (§80).
      expect(assertion.status).toBe('retracted')
      expect(assertion.state).toBe('active')
      expect(assertion.retracted_at).not.toBe('')
      expect(assertion.version).toBe(2)
    })
  })

  it('supersedes only within one Proposition', async () => {
    await withNexus('supersede', (nexus) => {
      const first = nexus.execute(SETUP)
      const second = nexus.execute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, asserted_by: :who, stance: "reject", mode: "stated" }
         }`,
        { p: first.handles.p!, who: { id: first.handles.alice! } },
      )
      nexus.execute(
        `SUPERSEDE ASSERTION "${first.handles.a}" BY "${second.handles.a}"`,
      )
      const older = nexus.store.load(parseElementId(first.handles.a!))
        ?.row as AssertionRow
      const newer = nexus.store.load(parseElementId(second.handles.a!))
        ?.row as AssertionRow
      expect(older.status).toBe('superseded')
      expect(older.superseded_by).toEqual([second.handles.a])
      expect(newer.supersedes).toEqual([first.handles.a])

      // Across two Propositions it would silently retire a claim nobody
      // revised.
      const other = nexus.execute(`MUTATE {
        CREATE CONCEPT ?light { TYPE "Preference" NAME "Light" }
        ENSURE PROPOSITION ?q ({id: "${first.handles.alice}"}, "prefers", ?light)
        CREATE ASSERTION ?b {
          SET FIELDS { proposition: ?q, stance: "support", mode: "stated" }
        }
      }`)
      const mismatch = nexus.tryExecute(
        `SUPERSEDE ASSERTION "${second.handles.a}" BY "${other.handles.b}"`,
      )
      expect('error' in mismatch && mismatch.error.code).toBe('SupersessionMismatch')
    })
  })

  it('freezes an Activity once it reaches a terminal state', async () => {
    await withNexus('transition', (nexus) => {
      const created = nexus.execute(
        'CREATE ACTIVITY ?x { SET FIELDS { activity_class: "extraction" } }',
      )
      const id = created.handles.x!
      expect(nexus.execute(`TRANSITION ACTIVITY "${id}" TO "running"`).status).toBe(
        'committed',
      )
      nexus.execute(`TRANSITION ACTIVITY "${id}" TO "completed"`)
      // Terminal topology freezes with the Activity (§22.3): re-opening a
      // finished process would let its provenance be rewritten after the fact.
      const reopened = nexus.tryExecute(
        `TRANSITION ACTIVITY "${id}" TO "running"`,
      )
      expect('error' in reopened && reopened.error.code).toBe('ActivityTerminal')
      expect(
        (nexus.store.load(parseElementId(id))?.row as { ended_at: string }).ended_at,
      ).not.toBe('')
    })
  })

  it('archives without claiming the author took anything back', async () => {
    await withNexus('archive', (nexus) => {
      const first = nexus.execute(SETUP)
      nexus.execute(`ARCHIVE "${first.handles.a}"`)
      const assertion = nexus.store.load(parseElementId(first.handles.a!))
        ?.row as AssertionRow
      expect(assertion.state).toBe('archived')
      // Archiving the record does not retract the claim (§80).
      expect(assertion.status).toBe('active')
    })
  })

  it('reports a clause it has not built rather than reporting success', async () => {
    await withNexus('unsupported', (nexus) => {
      // The clauses that are still refused are refused by name, so a caller
      // learns what to do instead rather than watching a write report success
      // and change nothing.
      const result = nexus.tryExecute('SET RETENTION "C-1" { expires_at: "x" }')
      expect('error' in result && result.error.code).toBe('UnsupportedCapability')
    })
  })

  it('refuses to erase something references still point at', async () => {
    await withNexus('purge-denied', (nexus) => {
      const first = nexus.execute(SETUP)
      // The default reference policy refuses rather than cascading: a dangling
      // reference does not say "this was erased", it says nothing.
      const denied = nexus.tryExecute(
        `PURGE "${first.handles.alice}" CONFIRM "PURGE"`,
      )
      expect('error' in denied && denied.error.code).toBe('PurgeDenied')
    })
  })

  it('records every reference it wrote in the reverse index', async () => {
    await withNexus('refs', (nexus) => {
      const first = nexus.execute(SETUP)
      const alice = parseElementId(first.handles.alice!)
      expect(nexus.store.referrers(nexus.space, alice)).toEqual([
        {
          from: parseElementId(first.handles.a!),
          field: 'asserted_by',
        },
        { from: parseElementId(first.handles.p!), field: 'subject' },
      ])
    })
  })

  it('journals enough for a lost response to be looked up', async () => {
    await withNexus('idempotency', (nexus) => {
      const parsed = nexus.execute(SETUP)
      expect(parsed.status).toBe('committed')
      const journalled = nexus.store.transaction(parsed.tx_id)
      expect(journalled?.changes).toHaveLength(4)
      expect(journalled?.snapshot_seq).toBe(0)
      expect(journalled?.schema_environment_version).toBe(1)
    })
  })

  it('refuses a resend under the same key rather than replaying it', async () => {
    // This pins the gap `DESCRIBE CAPABILITIES` names as `idempotent_replay`
    // (§34.3). The write path never looks the key up, so a resend is not
    // replayed. What saves it from committing twice is the unique index, which
    // means the caller gets a failure instead of the original receipt — worth
    // pinning, because the reference engine has no such index and commits the
    // duplicate. When replay lands, this test fails and forces it and the
    // capability declaration to move together.
    await withNexus('idempotency-resend', (nexus) => {
      const statement = parseKip(
        'CREATE CONCEPT ?x { TYPE "Person" NAME "Alice" }',
      )
      if (!('Kml' in statement)) throw new Error('the setup is a KML statement')

      const first = nexus.mutate(statement.Kml, {}, { idempotencyKey: 'key-1' })
      expect(first.status).toBe('committed')

      expect(() =>
        nexus.mutate(statement.Kml, {}, { idempotencyKey: 'key-1' }),
      ).toThrow()

      // And the refusal left no second Alice behind.
      const found = nexus.query(
        'FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "Person", name: "Alice"} }',
      )
      expect(found).toEqual([1])
    })
  })

  /**
   * `UPSERT ... MATCH {id: …}` resolves; it never mints.
   *
   * The insert half used to be reachable from an `id` selector, so an id
   * nothing carried — or one carrying a type the MATCH did not declare —
   * quietly created a *different* Concept under a *different* id, and reported
   * success. §53 gives an upsert by id no create half at all.
   */
  it('refuses an upsert by an id nothing carries instead of creating one', async () => {
    await withNexus('upsert-by-id', (nexus) => {
      const alice = nexus.execute('CREATE CONCEPT ?p { TYPE "Person" NAME "Ada" }')
        .handles.p!

      for (const command of [
        'UPSERT CONCEPT ?p { MATCH {id: "C-9999"} SET FIELDS {name: "Nobody"} }',
        `UPSERT CONCEPT ?p { MATCH {type: "Person", id: "C-9999"} SET FIELDS {name: "Nobody"} }`,
        // A declared type the element does not carry is simply not a match,
        // and the refusal says no more than that (§86.4).
        `UPSERT CONCEPT ?p { MATCH {type: "Preference", id: "${alice}"} SET FIELDS {name: "Wrong"} }`,
      ]) {
        const outcome = nexus.tryExecute(command)
        expect('error' in outcome && outcome.error.code).toBe('NotFoundOrNotVisible')
      }

      // Nothing was minted along the way.
      expect(nexus.query('FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }')).toEqual([1])
    })
  })

  it('reads a MATCH member as a selector and never as seed state', async () => {
    await withNexus('upsert-match', (nexus) => {
      // `name` in a MATCH is not a second way to spell SET FIELDS: the create
      // half takes only the identity, so the same command cannot mean two
      // things depending on whether it resolved or created.
      const created = nexus.execute(
        'UPSERT CONCEPT ?p { MATCH {type: "Person", key: "person:ada", name: "Ignored"} }',
      )
      const row = concept(nexus, created.handles.p!)
      expect(row.key).toBe('person:ada')
      expect(row.schema_ref).toBe(`${CM}/Person`)
      expect(row.name).toBe('')

      // A member of the wrong type is refused rather than read as absent —
      // reading `{type: 42}` as "no type declared" would answer a different,
      // valid command than the one written.
      const badType = nexus.tryExecute(
        'UPSERT CONCEPT ?p { MATCH {type: 42, key: "person:eve"} }',
      )
      expect('error' in badType && badType.error.code).toBe('TypeMismatch')
      const badId = nexus.tryExecute('UPSERT CONCEPT ?p { MATCH {id: 42} }')
      expect('error' in badId && badId.error.code).toBe('TypeMismatch')
    })
  })

  it('previews without taking a sequence or writing anything', async () => {
    await withNexus('dry-run', (nexus) => {
      const parsed = parseKip(SETUP)
      if (!('Kml' in parsed)) throw new Error('the setup is a KML statement')
      // A dry run never establishes a durable cognitive commit (§69.3): it
      // reports what it would have changed and takes no Space sequence.
      const preview = nexus.mutate(parsed.Kml, {}, { dryRun: true })
      expect(preview.status).toBe('no_effect')
      expect(preview.changes).toHaveLength(4)
      expect(preview.space_seq).toBeNull()
      expect(nexus.store.currentSeq(nexus.space)).toBe(0)
      expect(
        nexus.store.sql
          .exec<{ n: number }>('SELECT COUNT(*) AS n FROM concepts')
          .toArray()[0]?.n,
      ).toBe(0)
    })
  })
})
