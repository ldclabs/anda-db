import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY, type SchemaPackage } from '../src/schema/index.js'
import { parseElementId } from '../src/id.js'
import { parseKip } from '../src/kip/parser.js'
import { render } from '../src/view.js'
import type {
  AssertionRow,
  ConceptRow,
  EvidenceRow,
  PropositionRow,
} from '../src/store/index.js'

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
  extra: readonly SchemaPackage[] = [],
): Promise<void> {
  const stub = env.KIP_DB.getByName(`kml-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY, ...extra])
    body(nexus)
  })
}

/**
 * A predicate that constrains neither end.
 *
 * Every predicate the Cognitive Memory Profile declares says what may occupy
 * its ends — `prefers` takes a Concept, not a Literal — and that is a
 * different question from how a value is *stored* once it is allowed there.
 * A test about storage form needs a slot that permits both.
 */
const OPEN_PACKAGE = {
  format: 'KIP-Schema-Package',
  manifest: { package_id: 'kip://test/open', version: '1.0.0' },
  definitions: {
    predicates: {
      notes: { kind: 'PredicateType', description: 'Anything, about anything.' },
    },
  },
} as unknown as SchemaPackage

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
      for (const change of outcome.changes) expect(change.new_version).toBe(1)
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
    await withNexus(
      'literal-vs-ref',
      (nexus) => {
        const outcome = nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        ENSURE PROPOSITION ?p (?alice, "notes", "dark")
      }`)
        const prop = nexus.store.load(parseElementId(outcome.handles.p!))
          ?.row as PropositionRow
        // The object is the *text* "dark", stored as an explicit Literal so a
        // Schema-refined datatype would survive a round trip.
        expect(prop.object).toEqual({ value: 'dark', datatype: 'kip:string' })
        expect(prop.subject).toEqual({ id: outcome.handles.alice })
      },
      [OPEN_PACKAGE],
    )
  })

  it('refuses a Literal where the predicate declares an element reference', async () => {
    // §42–§44: `prefers` relates a Person to a Concept, so the text "dark" is
    // not a quieter version of the Preference — it is a different endpoint,
    // and storing it would leave a tuple nothing can traverse.
    await withNexus('literal-where-ref', (nexus) => {
      const result = nexus.tryExecute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        ENSURE PROPOSITION ?p (?alice, "prefers", "dark")
      }`)
      expect('error' in result && result.error.code).toBe('ConstraintViolation')
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
           SET FIELDS { proposition: :p, asserted_by: "C-1", stance: "support", mode: "stated", nonsense: 1 }
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
      // A written literal is refused before the transaction opens: §13.6
      // fixes the range in Core, so no Schema Environment is needed to know
      // 1.5 is wrong. `ConstraintViolation` names the rule it broke.
      const written = nexus.tryExecute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, asserted_by: "C-1", stance: "support", mode: "stated", confidence: 1.5 }
         }`,
        { p: first.handles.p! },
      )
      expect('error' in written && written.error.code).toBe(
        'ConstraintViolation',
      )

      // A bound parameter is only knowable at execution time, so that one is
      // the engine's to refuse — under the same code, because it is the same
      // rule. A caller that switched from a literal to a parameter should not
      // have to switch error handlers too.
      const bound = nexus.tryExecute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, asserted_by: "C-1", stance: "support", mode: "stated", confidence: :c }
         }`,
        { p: first.handles.p!, c: 1.5 },
      )
      expect('error' in bound && bound.error.code).toBe('ConstraintViolation')

      // The lower bound matters as much as the upper one: -1 is the stored
      // stand-in for "the actor stated none", so a negative that got through
      // would not be stored wrong — it would be stored as silence.
      const negative = nexus.tryExecute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, asserted_by: "C-1", stance: "support", mode: "stated", confidence: :c }
         }`,
        { p: first.handles.p!, c: -0.5 },
      )
      expect('error' in negative && negative.error.code).toBe(
        'ConstraintViolation',
      )
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
      const outcome = nexus.execute(`TRANSITION "${first.handles.a}" TO "retracted"`)
      expect(outcome.status).toBe('committed')
      const assertion = nexus.store.load(parseElementId(first.handles.a!))
        ?.row as AssertionRow
      // Retraction is an epistemic status, not a deletion: the record stays
      // active and citable, and only the claim is withdrawn (§57.3).
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
        `TRANSITION "${first.handles.a}" TO "superseded" BY "${second.handles.a}"`,
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
          SET FIELDS { proposition: ?q, asserted_by: {id: "${first.handles.alice}"}, stance: "support", mode: "stated" }
        }
      }`)
      const mismatch = nexus.tryExecute(
        `TRANSITION "${second.handles.a}" TO "superseded" BY "${other.handles.b}"`,
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
      expect(nexus.execute(`TRANSITION "${id}" TO "running"`).status).toBe(
        'committed',
      )
      nexus.execute(`TRANSITION "${id}" TO "completed"`)
      // Terminal topology freezes with the Activity (§16.6): re-opening a
      // finished process would let its provenance be rewritten after the fact.
      const reopened = nexus.tryExecute(
        `TRANSITION "${id}" TO "running"`,
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
      nexus.execute(`TRANSITION "${first.handles.a}" TO "archived"`)
      const assertion = nexus.store.load(parseElementId(first.handles.a!))
        ?.row as AssertionRow
      expect(assertion.state).toBe('archived')
      // Archiving the record does not retract the claim (§60.1).
      expect(assertion.status).toBe('active')
    })
  })

  describe('SET RETENTION', () => {
    it('records storage lifecycle without touching what the element says', async () => {
      await withNexus('set-retention', (nexus) => {
        const alice = nexus.execute(SETUP).handles.alice!
        const before = concept(nexus, alice)
        nexus.execute(
          `SET RETENTION "${alice}" {retention_class: "short", expires_at: "2030-01-01T00:00:00Z"}`,
        )
        const after = concept(nexus, alice)
        expect(after.retention).toEqual({
          retention_class: 'short',
          expires_at: '2030-01-01T00:00:00Z',
        })
        // Lifted out of the block so the sweep can index it (§19.2), and
        // normalized on the way — the sweep compares strings.
        expect(after.expires_at).toBe('2030-01-01T00:00:00.000Z')
        // Storage lifecycle, never content: the Concept still says what it said.
        expect(after.name).toBe(before.name)
        expect(after.version).toBe(before.version + 1)
      })
    })

    it('takes no version when the block says what is already recorded', async () => {
      await withNexus('set-retention-idempotent', (nexus) => {
        const alice = nexus.execute(SETUP).handles.alice!
        const block = `SET RETENTION "${alice}" {retention_class: "short"}`
        nexus.execute(block)
        const once = concept(nexus, alice).version
        // Restating a policy is not a change to it.
        const again = nexus.execute(block)
        expect(again.status).toBe('no_effect')
        expect(concept(nexus, alice).version).toBe(once)
      })
    })

    it('sweeps the set its own WHERE selects, and nothing beside it', async () => {
      await withNexus('set-retention-sweep', (nexus) => {
        const handles = nexus.execute(SETUP).handles
        nexus.execute(
          `SET RETENTION ?c {retention_class: "short"}
           WHERE { ?c CONCEPT {type: "Person"} }`,
        )
        expect(concept(nexus, handles.alice!).retention).toEqual({
          retention_class: 'short',
        })
        // The Preference is not a Person, so the sweep never reached it.
        expect(concept(nexus, handles.dark!).retention).toEqual({})
      })
    })

    it('refuses a member the hook does not have, rather than losing it', async () => {
      await withNexus('set-retention-shape', (nexus) => {
        const alice = nexus.execute(SETUP).handles.alice!
        const result = nexus.tryExecute(
          `SET RETENTION "${alice}" {retention_class: "standard", review_at: "2030-01-01T00:00:00Z"}`,
        )
        expect('error' in result && result.error.code).toBe('SchemaFieldNotFound')
      })
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

  it('replays a resend under the same key instead of writing again', async () => {
    // §26, §33: a timeout is not an abort. A client that lost its response
    // resends the same key and gets back the outcome its first attempt
    // produced — the receipt it needs — rather than a second Alice or a
    // constraint failure about its own write.
    await withNexus('idempotency-resend', (nexus) => {
      const statement = parseKip(
        'CREATE CONCEPT ?x { TYPE "Person" NAME "Alice" }',
      )
      if (!('Kml' in statement)) throw new Error('the setup is a KML statement')

      const first = nexus.mutate(statement.Kml, {}, { idempotencyKey: 'key-1' })
      expect(first.status).toBe('committed')

      const again = nexus.mutate(statement.Kml, {}, { idempotencyKey: 'key-1' })
      // The whole receipt, member for member: a replay that reconstructed one
      // field through a second expression is exactly how the two would drift.
      // Warnings are the one honest difference, and they are checked below.
      expect({ ...again, warnings: [] }).toEqual({ ...first, warnings: [] })
      // And it says so, because the caller resent precisely to find out.
      expect(again.warnings.join(' ')).toMatch(/replayed/)

      // Nothing ran a second time.
      expect(
        nexus.query(
          'FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "Person", name: "Alice"} }',
        ),
      ).toEqual([1])

      // A different key is a different write, which is the whole point of the
      // key being the caller's to choose.
      const other = nexus.mutate(statement.Kml, {}, { idempotencyKey: 'key-2' })
      expect(other.tx_id).not.toBe(first.tx_id)
      expect(
        nexus.query(
          'FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "Person", name: "Alice"} }',
        ),
      ).toEqual([2])
    })
  })

  it('does not let a dry run replay, or be replayed', async () => {
    // A preview establishes no durable commit (§69.3), so there is nothing to
    // replay and nothing to record — and a dry run that answered from an
    // earlier real commit would report a write as a preview of itself.
    await withNexus('idempotency-dry-run', (nexus) => {
      const statement = parseKip(
        'CREATE CONCEPT ?x { TYPE "Person" NAME "Alice" }',
      )
      if (!('Kml' in statement)) throw new Error('the setup is a KML statement')

      nexus.mutate(statement.Kml, {}, { idempotencyKey: 'key-1' })
      const preview = nexus.mutate(
        statement.Kml,
        {},
        { idempotencyKey: 'key-1', dryRun: true },
      )
      expect(preview.status).toBe('no_effect')
      expect(preview.warnings.join(' ')).not.toMatch(/replayed/)
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

  it('binds the structural edge, with the position an ordered field has', async () => {
    await withNexus('edge-binding', (nexus) => {
      // §43.7: the bound edge is virtual structural query state, "not
      // necessarily a durable Cognitive Element" — so it binds as the value it
      // is, and §17.4 puts each reference's current position on it. An
      // unordered field has no positions, so the member reads null there.
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?exp {
          TYPE "Experience"
          SET ATTRIBUTES {goal: "learn", outcome_status: "success"}
        }
        CREATE CONCEPT ?one {
          TYPE "ExperienceStep" SET ATTRIBUTES {step_kind: "action", summary: "one"}
        }
        CREATE CONCEPT ?two {
          TYPE "ExperienceStep" SET ATTRIBUTES {step_kind: "action", summary: "two"}
        }
      }`)
      const step = (summary: string): string =>
        nexus.query(
          `FIND(?c.id) WHERE { ?c CONCEPT {type: "ExperienceStep"} FILTER(?c.attributes.summary == "${summary}") } LIMIT 1`,
        )[0] as string
      const exp = nexus.query(
        'FIND(?c.id) WHERE { ?c CONCEPT {type: "Experience"} } LIMIT 1',
      )[0] as string
      const params = {
        exp,
        one: { id: step('one') },
        two: { id: step('two') },
      }
      nexus.execute(`UPDATE :exp SET STRUCTURAL { ("has_step", :one) }`, params)
      // Position 0 puts the second step first, which is the whole point of an
      // explicit index.
      nexus.execute(
        `UPDATE :exp SET STRUCTURAL { ("has_step", :two) {index: 0} }`,
        params,
      )

      const ordered = nexus.query(
        'FIND(?step.attributes.summary, ?e.index) ' +
          'WHERE { ?e STRUCTURAL (?src, "has_step", ?step) } ORDER BY ?e.index',
      )
      expect(ordered).toEqual([
        ['two', 0],
        ['one', 1],
      ])
    })
  })

  it('replaces rather than appends on a single-cardinality structural field', async () => {
    await withNexus('single-cardinality', (nexus) => {
      // §17.5: `SET STRUCTURAL` on a field that holds at most one reference
      // *replaces* it. Appending and then failing the cardinality check would
      // refuse the one write this form exists for — and would disagree with
      // `rs/anda_cognitive_nexus`, which replaces.
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?exp {
          TYPE "Experience"
          SET ATTRIBUTES {goal: "learn", outcome_status: "success"}
        }
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
      }`)
      const exp = nexus.query(
        'FIND(?c.id) WHERE { ?c CONCEPT {type: "Experience"} } LIMIT 1',
      )[0] as string
      const person = (name: string): string =>
        nexus.query(
          `FIND(?c.id) WHERE { ?c CONCEPT {type: "Person", name: "${name}"} } LIMIT 1`,
        )[0] as string
      const alice = person('Alice')
      const bob = person('Bob')

      nexus.execute(`UPDATE :exp SET STRUCTURAL { ("experienced_by", :who) }`, {
        exp,
        who: { id: alice },
      })
      nexus.execute(`UPDATE :exp SET STRUCTURAL { ("experienced_by", :who) }`, {
        exp,
        who: { id: bob },
      })

      const row = nexus.store.sql
        .exec<{ structural: string }>(
          'SELECT structural FROM concepts WHERE id = ?',
          parseElementId(exp).seq,
        )
        .toArray()[0]
      const structural = JSON.parse(row?.structural ?? '{}') as Record<
        string,
        { id: string }[]
      >
      expect(structural[`${CM}/experienced_by`]).toEqual([{ id: bob }])

      // An unordered field has no positions, single-cardinality or not.
      const positioned = nexus.tryExecute(
        `UPDATE :exp SET STRUCTURAL { ("experienced_by", :who) {index: 0} }`,
        { exp, who: { id: alice } },
      )
      expect('error' in positioned && positioned.error.code).toBe(
        'ConstraintViolation',
      )
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

  // --- PURGE PAYLOAD (§60.6) ------------------------------------------------

  /** One Evidence record with an inline payload, cited by an Assertion. */
  const CITED_EVIDENCE = `MUTATE {
  CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
  CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
  ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
  CREATE EVIDENCE ?e {
    SET FIELDS {
      evidence_class: "user_statement",
      payload: "I prefer dark mode, and my address is 12 Elm Street.",
      content_digest: "sha3-256:d1ge5t",
      media_type: "text/plain",
      observed_at: "2026-08-16T09:00:00Z"
    }
  }
  CREATE ASSERTION ?a {
    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 }
    SET STRUCTURAL { ("evidence", ?e) {role: "support"} }
  }
}`

  function evidence(nexus: CognitiveNexus, id: string): EvidenceRow {
    const element = nexus.store.load(parseElementId(id))
    if (element?.kind !== 'Evidence') throw new Error(`${id} is not Evidence`)
    return element.row
  }

  it('destroys the payload bytes and keeps the Evidence', async () => {
    // §60.6: the data-minimization instrument. A Space can discard observed raw
    // bytes after digesting them without destroying the evidence event, its
    // citations, or its provenance role — which is exactly what makes it usable
    // where element purge is not.
    await withNexus('purge-payload', (nexus) => {
      nexus.execute(CITED_EVIDENCE)
      expect(evidence(nexus, 'E-1').payload_inline).toBe(
        'I prefer dark mode, and my address is 12 Elm Street.',
      )

      const outcome = nexus.execute('PURGE PAYLOAD "E-1" CONFIRM "PURGE"')
      expect(outcome.status).toBe('committed')

      const row = evidence(nexus, 'E-1')
      // Gone: the bytes, and nothing but the bytes.
      expect(row.payload_mode).toBe('purged')
      expect(row.content_ref).toBe('')
      const view = render({ kind: 'Evidence', row }) as {
        payload: Record<string, unknown>
      }
      expect(view.payload).toEqual({ mode: 'purged' })
      // The search index is the other copy of the payload. A purge that left
      // it behind would keep the bytes retrievable by the very words the
      // caller was minimizing away.
      const found = nexus.describe('SEARCH EVIDENCE "Elm"') as { hits: unknown[] }
      expect(found.hits).toHaveLength(0)

      // The change stream names it `payload_purge`, not `purge` (§36.1): a
      // follower that could not tell the two apart would read a
      // data-minimization decision as the loss of the record.
      const history = nexus.describe('HISTORY ELEMENT "E-1"') as unknown as {
        changes: { op: string }[]
      }[]
      expect(history.flatMap((e) => e.changes.map((c) => c.op))).toEqual([
        'create',
        'payload_purge',
      ])
      // Kept: everything §60.6 lists, so corroboration grouping and
      // independence counting keep operating on the surviving digest and
      // provenance (§23).
      expect(row.content_digest).toBe('sha3-256:d1ge5t')
      expect(row.evidence_class).toBe('user_statement')
      expect(row.media_type).toBe('text/plain')
      expect(row.state).toBe('active')

      // The citation still resolves: an Assertion whose Evidence went to a stub
      // would be a history pointing at nothing, which is the failure element
      // purge exists to refuse and this operation never risks.
      const cited = nexus.query(
        'FIND(?a.evidence) WHERE { ?a ASSERTION {id: "A-1"} }',
      )
      expect(JSON.stringify(cited)).toContain('E-1')
    })
  })

  it('reaches the version log, so no past coordinate hands the bytes back', async () => {
    // The half that is easy to forget and fatal to skip: every commit appends
    // the whole row it wrote, so a payload cleared only in the current row
    // stays fully readable through `AS OF`.
    await withNexus('purge-payload-history', (nexus) => {
      nexus.execute(CITED_EVIDENCE)
      const before = nexus.store.currentSeq(nexus.space)
      nexus.execute('PURGE PAYLOAD "E-1" CONFIRM "PURGE"')

      const historical = nexus.query(
        `FIND(?e.payload) WHERE { ?e EVIDENCE {id: "E-1"} } AS OF SEQ ${before}`,
      )
      expect(JSON.stringify(historical)).not.toContain('Elm Street')
      expect(JSON.stringify(historical)).toContain('purged')
    })
  })

  it('treats a repeat payload purge as a no_effect', async () => {
    // §60.6 states it outright, and it matters for a retry: a sweep that ran
    // twice must not burn a second version or emit a second change record.
    await withNexus('purge-payload-twice', (nexus) => {
      nexus.execute(CITED_EVIDENCE)
      nexus.execute('PURGE PAYLOAD "E-1" CONFIRM "PURGE"')
      const version = evidence(nexus, 'E-1').version

      const again = nexus.execute('PURGE PAYLOAD "E-1" CONFIRM "PURGE"')
      expect(again.status).toBe('no_effect')
      expect(evidence(nexus, 'E-1').version).toBe(version)
    })
  })

  it('refuses a payload purge of anything that has no payload', async () => {
    // §60.6: other kinds have no payload. Succeeding vacuously over a set of
    // Concepts would read as "the bytes are gone" when nothing was there.
    await withNexus('purge-payload-kind', (nexus) => {
      nexus.execute(CITED_EVIDENCE)
      expect(() =>
        nexus.execute('PURGE PAYLOAD "C-1" CONFIRM "PURGE"'),
      ).toThrowError(/has no payload/)
      // And the refusal erased nothing on the way to refusing.
      expect(evidence(nexus, 'E-1').payload_mode).toBe('inline')
    })
  })

  it('lets UPDATE reach a record\'s Facets while its payload stays immutable', async () => {
    // §18.1: a Facet is representation-local state and none of it is truth, so
    // a Facet on Evidence moves while what the Evidence observed does not.
    // The Profile relies on this: `OutcomeRecord` — the consequence channel's
    // graded index — lives on Evidence, and an optional member has to be
    // establishable after the instrument first wrote the record.
    await withNexus('record-facets', (nexus) => {
      nexus.execute(CITED_EVIDENCE)
      nexus.execute(
        'UPDATE "E-1" SET FACET "OutcomeRecord" ' +
          '{task_family: "prefs/stated", outcome_status: "unknown"}',
      )
      nexus.execute('UPDATE "E-1" SET FACET "OutcomeRecord" {magnitude: 0.5}')
      expect(
        nexus.query(
          'FIND(?e.facets["OutcomeRecord"].magnitude) WHERE { ?e EVIDENCE {} }',
        ),
      ).toEqual([0.5])

      // Established once, and not revised afterwards (§39).
      expect(() =>
        nexus.execute('UPDATE "E-1" SET FACET "OutcomeRecord" {magnitude: 0.9}'),
      ).toThrowError(/immutable/)

      // What the record itself says is still corrected, never edited.
      expect(() =>
        nexus.execute('UPDATE "E-1" SET FIELDS {payload: "something else"}'),
      ).toThrowError(/corrected/)
    })
  })

  it('lets a legal hold block a payload purge, as it blocks an element purge', async () => {
    // §60.6: a hold is most often placed precisely to preserve the bytes.
    await withNexus('purge-payload-hold', (nexus) => {
      nexus.execute(CITED_EVIDENCE)
      nexus.execute('SET RETENTION "E-1" {legal_hold: true}')
      expect(() =>
        nexus.execute('PURGE PAYLOAD "E-1" CONFIRM "PURGE"'),
      ).toThrowError(/legal hold/)
    })
  })
})
