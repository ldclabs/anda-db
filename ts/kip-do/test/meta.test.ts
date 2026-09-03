import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { parseElementId } from '../src/id.js'
import {
  capabilities,
  capabilityState,
  unsupportedCapabilityNames,
} from '../src/meta/capabilities.js'

/** The thirteen profile names §89 lists, in the order it lists them. */
const KIP_CONFORMANCE_PROFILES = [
  'KIP-Core',
  'KIP-Schema',
  'KIP-Epistemic',
  'KIP-Governance',
  'KIP-Transactions',
  'KIP-Capsule',
  'KIP-KQL',
  'KIP-KML',
  'KIP-META',
  'KIP-Runtime',
  'KIP-Historical',
  'KIP-High-Assurance',
  'KIP-1-Migration',
]

/**
 * META, and the five-layer discipline it exists to keep apart.
 *
 * The tests that matter most here are the refusals. An engine that answers
 * emptily where it cannot answer at all teaches the caller a falsehood: "no
 * results" and "this engine cannot answer that" are different, and only one of
 * them is true.
 */
async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus) => void,
): Promise<void> {
  const stub = env.KIP_DB.getByName(`meta-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    nexus.execute(SETUP)
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

const CM = 'kip://profiles/cognitive-memory@2.0.0'

describe('META', () => {
  it('reports what it cannot do, as data rather than as an error', async () => {
    await withNexus('capabilities', (nexus) => {
      // An Agent that has to discover a gap by triggering an error has already
      // wasted a turn; one that never discovers it reads an absent feature as
      // an absent fact.
      const report = nexus.describe('DESCRIBE CAPABILITIES') as {
        kip: string
        unsupported: { capability: string; reason: string }[]
      }
      expect(report.kip).toBe('2.0')
      const gaps = report.unsupported.map((entry) => entry.capability)
      // A partial capability names what is left rather than shrinking to one
      // word: Governance is enforced at command scope, so the gaps that remain
      // are the element-scope ones, and they are listed as themselves.
      expect(gaps).toContain('trust_model')
      expect(gaps).toContain('capsule_import')
      expect(gaps).toContain('hop_quantifiers')
      // Keyword SEARCH is built, so the gaps that remain are the specific ones
      // — a partial capability names what is left rather than shrinking to the
      // one word that would read as "no search at all".
      expect(gaps).not.toContain('search')
      expect(gaps).toContain('semantic_search')
      expect(gaps).toContain('historical_search')
      expect(gaps).toContain('search_over_assertions_and_activities')
      // Closed gaps stay closed: each of these was on the list, and leaving a
      // stale entry there is the same failure as a missing one — a caller
      // reads it and does not try.
      expect(gaps).not.toContain('space_self_identity')
      expect(gaps).not.toContain('retention_expiry')
      expect(gaps).not.toContain('set_retention')
      // Every gap carries a reason, not just a name.
      for (const entry of report.unsupported) {
        expect(entry.reason.length, entry.capability).toBeGreaterThan(20)
      }
    })
  })

  it('answers `requires` about every gap it documents', async () => {
    // `DESCRIBE CAPABILITIES` answers two audiences from one set of facts: an
    // Agent reading `unsupported` for a reason, and a `requires` check asking
    // a yes/no question (§67). A gap documented in the first and missing from
    // the second reports itself as *unrecognized* rather than as absent — and
    // §67's whole point is that an unrecognized requirement must not pass.
    const report = capabilities() as unknown as {
      unsupported: { capability: string }[]
    }
    for (const entry of report.unsupported) {
      expect(capabilityState(entry.capability), entry.capability).toBe(false)
    }
    for (const name of unsupportedCapabilityNames()) {
      expect(capabilityState(name), name).toBe(false)
    }
    // An unknown name is not "supported by omission".
    expect(capabilityState('read_everything')).toBeUndefined()
  })

  it('declares the §89 conformance profiles, by their §89 names', async () => {
    const report = capabilities() as unknown as { profiles: string[] }
    expect(report.profiles.length).toBeGreaterThan(0)
    for (const name of report.profiles) {
      expect(KIP_CONFORMANCE_PROFILES, name).toContain(name)
    }
    // Claimed only where it is true. Each absence has an entry in
    // `unsupported` a caller can read the reason from.
    expect(report.profiles).not.toContain('KIP-High-Assurance')
    expect(report.profiles).not.toContain('KIP-KQL')
    expect(report.profiles).not.toContain('KIP-Transactions')
    expect(report.profiles).not.toContain('KIP-Capsule')
  })

  it('orients an Agent before its first command', async () => {
    await withNexus('primer', (nexus) => {
      // The key structure is the reference engine's, member for member: a
      // Primer is the one document every client parses, and `primer.types`
      // where the other engine writes `primer.schema.types` reads as a Space
      // with no types rather than as a wrong path.
      const primer = nexus.describe('DESCRIBE PRIMER') as {
        execution_context: { principal: { authentication_strength: string } }
        space: { id: string; seq: number }
        contents: Record<string, number>
        schema: {
          environment_version: number
          types: string[]
          predicates: string[]
          facets: string[]
          structural_fields: string[]
          packages: string[]
        }
        golden_path: string[]
        capabilities?: unknown
      }
      expect(primer.schema.types).toContain(`${CM}/Person`)
      expect(primer.schema.predicates).toContain(`${CM}/prefers`)
      expect(primer.schema.packages).toContain(CM)
      expect(primer.space.id).toBe(nexus.space)
      // A count is a fact about elements a narrower Principal may not
      // discover, so it is answered whole or withheld with a reason (§88.6).
      expect(primer.contents.concept).toBe(2)
      expect(primer.golden_path[0]).toBe('SEARCH or FIND to ground')
      expect(primer.execution_context.principal.authentication_strength).toBe(
        'strong',
      )
      // `compact` leaves the long documents out; `full` is where they arrive.
      expect(primer.capabilities).toBeUndefined()
      expect(
        (nexus.describe('DESCRIBE PRIMER MODE "full"') as { capabilities: unknown })
          .capabilities,
      ).toBeDefined()
      // The grammar already closes the enum, so this is the parser refusing
      // before the engine ever sees a mode it does not know.
      expect(() => nexus.describe('DESCRIBE PRIMER MODE "verbose"')).toThrowError(
        /compact \| full/,
      )
    })
  })

  it('answers about a symbol with its canonical identity, never a local name', async () => {
    await withNexus('symbol', (nexus) => {
      // §88.6: a local name means nothing outside the environment that
      // resolved it, so what comes back is the exact reference.
      const answer = nexus.describe('DESCRIBE TYPE "Person"') as {
        ref: string
        definition: { kind: string }
      }
      expect(answer.ref).toBe(`${CM}/Person`)
      expect(answer.definition.kind).toBe('ConceptType')
      expect(() => nexus.describe('DESCRIBE TYPE "Spaceship"')).toThrowError(
        /no active Schema Package defines/,
      )
    })
  })

  it('keeps installed and active apart when listing packages', async () => {
    await withNexus('list', (nexus) => {
      // Conflating them would let a caller write against a package the Space
      // does not resolve.
      const packages = nexus.describe('LIST SCHEMA PACKAGES') as {
        package_ref: string
        state: string
      }[]
      const core = packages.find((p) => p.package_ref === 'kip://core@2.0.0')
      expect(core?.state).toBe('active')
      // A `LIST` row names the symbol both ways — what a command may write,
      // and what it resolves to — and says which package answers, which is the
      // same row the reference engine returns.
      expect(nexus.describe('LIST TYPES')).toContainEqual({
        ref: `${CM}/Person`,
        local_name: 'Person',
        package_ref: CM,
        status: 'active',
      })
      // A policy is listed as the policy, in the wire names the reference
      // engine writes: `accept` is a threshold and says so, and `modes` gates
      // eligibility rather than weighting a claim.
      expect(
        (nexus.describe('LIST EPISTEMIC POLICIES') as { id: string }[]).map(
          (policy) => policy.id,
        ),
      ).toEqual(['kip:policy:baseline', 'kip:policy:forecast'])
    })
  })

  it('hands back the opaque cursor a paged LIST needs to continue', async () => {
    await withNexus('list-paging', (nexus) => {
      // §88.4 makes a cursor opaque, so a caller cannot invent one — which
      // means an engine that accepts an opaque `list` cursor and never issues
      // one has made `LIST ... CURSOR` unreachable rather than merely awkward.
      const all = nexus.describe('LIST TYPES') as string[]
      expect(all.length).toBeGreaterThan(2)

      const first = nexus.describePage('LIST TYPES LIMIT 2')
      expect(first.result).toEqual(all.slice(0, 2))
      expect(first.nextCursor).not.toBeNull()
      // Opaque: not the offset a caller could have typed.
      expect(Number(first.nextCursor)).toBeNaN()

      const second = nexus.describePage(
        `LIST TYPES LIMIT 2 CURSOR "${first.nextCursor}"`,
      )
      expect(second.result).toEqual(all.slice(2, 4))

      // §102.28: one family's cursor must not continue another's.
      expect(() =>
        nexus.describe(`LIST TYPES LIMIT 2 CURSOR "${'2'}"`),
      ).toThrow()
    })
  })

  it('validates legality without promising a commit', async () => {
    await withNexus('validate', (nexus) => {
      expect(
        nexus.describe('VALIDATE KML "CREATE CONCEPT ?c { TYPE \\"Person\\" NAME \\"X\\" }"'),
      ).toEqual({ valid: true, violations: [] })

      const bad = nexus.describe('VALIDATE KML "CREATE CONCEPT ?c {"') as {
        valid: boolean
        violations: { code: string }[]
      }
      expect(bad.valid).toBe(false)
      expect(bad.violations[0]?.code).toBe('InvalidSyntax')

      // The actual parsed semantics rule, not the keyword the caller used.
      const mismatched = nexus.describe(
        'VALIDATE KQL "CREATE CONCEPT ?c { TYPE \\"Person\\" NAME \\"X\\" }"',
      ) as { valid: boolean; violations: { code: string }[] }
      expect(mismatched.valid).toBe(false)
      expect(mismatched.violations[0]?.code).toBe('LanguageMismatch')
    })
  })

  it('previews an effect through the real dry-run path and writes nothing', async () => {
    await withNexus('preview', (nexus) => {
      const before = nexus.store.currentSeq(nexus.space)
      const preview = nexus.describe(
        'PREVIEW KML "CREATE CONCEPT ?c { TYPE \\"Person\\" NAME \\"Ghost\\" }"',
      ) as { changes: unknown[]; status: string }
      expect(preview.changes).toHaveLength(1)
      // A preview written twice drifts from the commit it previews; this one
      // is the same code path, and it commits nothing.
      expect(nexus.store.currentSeq(nexus.space)).toBe(before)
      expect(
        nexus.query('FIND(COUNT(?c)) WHERE { ?c CONCEPT {name: "Ghost"} }'),
      ).toEqual([0])
    })
  })

  it('answers HISTORY in transition envelopes, for an element and for a Space', async () => {
    // §68.1 calls HISTORY a transition chronology and §36.2 makes a transition
    // one envelope, so an element's history is the same grain as a Space's —
    // narrowed to the changes that element took part in, not re-grained into
    // one row per version.
    await withNexus('history', (nexus) => {
      const [id] = nexus.query(
        'FIND(?c.id) WHERE { ?c CONCEPT {name: "Alice"} }',
      ) as string[]
      nexus.execute(`TRANSITION "${id!}" TO "archived"`)

      const element = nexus.describe(`HISTORY ELEMENT "${id!}"`) as {
        space_id: string
        space_seq: number
        tx_id: string
        status: string
        changes: {
          id: string
          kind: string
          op: string
          new_version: number
          old_version?: number
          state?: { from: string; to: string }
          schema_ref?: string
        }[]
      }[]
      // §36.1: the normative op vocabulary, with the move itself in `state`.
      expect(element.map((e) => e.changes.map((c) => c.op))).toEqual([
        ['create'],
        ['lifecycle'],
      ])
      expect(element.map((e) => e.changes.map((c) => c.new_version))).toEqual([
        [1],
        [2],
      ])
      expect(element[1]?.changes[0]?.state).toEqual({ from: 'active', to: 'archived' })
      expect(element[1]?.changes[0]?.old_version).toBe(1)
      expect(element[0]?.changes[0]?.schema_ref).toBe(`${CM}/Person`)
      // §36.3's deduplication key is present on every envelope, which is what
      // a follower needs and what a flattened change list cannot offer.
      for (const envelope of element) {
        expect(envelope.space_id).toBe('kip:space:default')
        expect(envelope.tx_id).not.toBe('')
        expect(envelope.status).toBe('committed')
        // Narrowed to the element asked about, not to the whole transition.
        expect(envelope.changes.every((c) => c.id === id)).toBe(true)
      }

      const space = nexus.describe('HISTORY SPACE') as { space_seq: number }[]
      expect(space).toHaveLength(2)
      expect(space.map((s) => s.space_seq)).toEqual([1, 2])
    })
  })

  it('reports CHANGES as envelopes and hands back where it got to', async () => {
    await withNexus('changes', (nexus) => {
      // One envelope per committed transition. The setup is a single MUTATE, so
      // its four changes arrive together rather than as four loose rows: a
      // consumer handed them loose could not tell they were one transition
      // (§36.2).
      const first = nexus.describePage('CHANGES AFTER SEQ 0')
      const envelopes = first.result as unknown as {
        space_seq: number
        changes: unknown[]
      }[]
      expect(envelopes).toHaveLength(1)
      expect(envelopes[0]!.changes).toHaveLength(4)
      expect(envelopes[0]!.space_seq).toBe(1)
      // The cursor rides the paging slot every other META command uses.
      expect(first.nextCursor).toBe('1')

      // A caller that saw nothing holds the same place rather than starting
      // over.
      const again = nexus.describePage('CHANGES AFTER SEQ 1')
      expect(again.result).toEqual([])
      expect(again.nextCursor).toBeNull()
    })
  })

  it('finds a transaction by its idempotency key, or says it never committed', async () => {
    await withNexus('transaction', (nexus) => {
      const [txs] = [nexus.describe('HISTORY SPACE') as { tx_id: string }[]]
      const found = nexus.describe(
        `DESCRIBE TRANSACTION "${txs[0]!.tx_id}"`,
      ) as { status: string }
      expect(found.status).toBe('committed')
      // A key nobody committed under is not something a different retry fixes:
      // it means the write never landed.
      expect(() =>
        nexus.describe('DESCRIBE TRANSACTION BY IDEMPOTENCY KEY "never"'),
      ).toThrowError(/no transaction committed under/)
    })
  })

  it('describes an error from the registry the engine actually uses', async () => {
    await withNexus('error', (nexus) => {
      const answer = nexus.describe('DESCRIBE ERROR "SchemaSymbolNotFound"') as {
        category: string
        retry: string
        hint: string
      }
      expect(answer.category).toBe('schema')
      expect(answer.retry).toBe('requires_different_input')
      expect(answer.hint.length).toBeGreaterThan(10)
    })
  })

  it('refuses rather than answering emptily where an empty answer is a judgement', async () => {
    await withNexus('refusals', (nexus) => {
      // "Nothing is trusted" is a judgement. An absent subsystem is not one, so
      // the trust report refuses instead of answering emptily.
      expect(() => nexus.describe('DESCRIBE TRUST')).toThrowError(
        /would read as a judgement that nothing is trusted/,
      )
      // A token that promises a coordinate can be read back is only issued
      // once the engine can honour it — and now it can, so it is issued and
      // binds a later read to that coordinate (§68).
      const snapshot = nexus.describe('DESCRIBE SNAPSHOT') as {
        space_seq: number
        snapshot_token: string
      }
      expect(snapshot.space_seq).toBe(nexus.store.currentSeq(nexus.space))
      expect(snapshot.snapshot_token).toMatch(/^[0-9a-f]+$/)
      // The statements §68 removed are refused by the grammar, not answered.
      expect(() => nexus.describe('SNAPSHOT')).toThrowError()
      expect(() => nexus.describe('DESCRIBE EXECUTION CONTEXT')).toThrowError()
      expect(() => nexus.describe('DESCRIBE PROJECTION CAPABILITY')).toThrowError()
      // Reporting an unchecked artifact as valid would cancel the point of
      // asking.
      expect(() =>
        nexus.describe('VERIFY SCHEMA PACKAGE "kip://core@2.0.0"'),
      ).toThrowError(/not implemented by this engine/)
      // An Assertion has no free text, so an empty answer would read as "no
      // such claim exists" rather than "nothing here is searchable".
      expect(() => nexus.describe('SEARCH ASSERTION "Alice"')).toThrowError(
        /carry no free text/,
      )
    })
  })
})

describe('Capsules', () => {
  it('exports a bounded excerpt with the exact symbols it depends on', async () => {
    await withNexus('export', (nexus) => {
      const capsule = nexus.describe(
        'EXPORT CAPSULE :out WHERE { ?a ASSERTION {} }',
      ) as {
        format: string
        version: string
        payload: {
          manifest: { completeness: string }
          records: Record<string, unknown[]>
          schema: { package: string; version: string; digest: string }[]
        }
        integrity: { content_digest: string; proofs: unknown[] }
      }

      // The frame discriminator is the artifact's contract, not this engine's
      // label for its own output: `anda_kip::Capsule::validate_frame` rejects
      // any other `format` outright, so getting this wrong makes every Capsule
      // this engine writes unreadable by the reference engine — which is the
      // only thing a Capsule is for.
      expect(capsule.format).toBe('KIP-Cognitive-Capsule')
      expect(capsule.version).toBe('2.0')

      // The closure follows references *outward* from the roots, which is why
      // rooting on the Assertion reaches the Proposition it is about, and the
      // Proposition reaches both its endpoints. Rooting on Alice would reach
      // Alice alone: a Concept points at nothing, and the Propositions point
      // at *it*.
      expect(capsule.payload.records.assertions).toHaveLength(1)
      expect(capsule.payload.records.propositions).toHaveLength(1)
      expect(capsule.payload.records.concepts).toHaveLength(2)
      expect(capsule.payload.manifest.completeness).toBe('referential_closure')
      // §20.4: the exact refs travel with the records, or the Capsule
      // arrives meaning whatever the destination happens to call them. The
      // split into `package` + `version` is the frame `anda_kip` decodes —
      // both are required there, so a single `package_ref` would make the
      // whole Capsule unreadable by the reference engine.
      expect(
        capsule.payload.schema.map((s) => `${s.package}@${s.version}`),
      ).toContain(CM)
      expect(capsule.payload.schema[0]?.digest).toMatch(/^[0-9a-f]{64}$/)
      // Unsigned, and it says so by carrying no proofs rather than by
      // implying provenance it cannot support.
      expect(capsule.integrity.proofs).toEqual([])
      // SHA3-256, the same profile `rs/anda_cognitive_nexus` writes: a
      // Capsule is the one artifact that leaves this engine and is checked by
      // another, so the algorithm is part of the contract rather than an
      // engine choice.
      expect(capsule.integrity.content_digest).toMatch(
        /^sha3-256:[0-9a-f]{64}$/,
      )
    })
  })

  it('exports only the roots under a selective closure', async () => {
    await withNexus('roots-only', (nexus) => {
      const capsule = nexus.describe(
        'EXPORT CAPSULE :out WHERE { ?a ASSERTION {} } WITH {closure: "selective"}',
      ) as { payload: { manifest: { completeness: string }; records: Record<string, unknown[]> } }
      expect(capsule.payload.records.assertions).toHaveLength(1)
      expect(capsule.payload.records.propositions).toHaveLength(0)
      // Claiming a completeness it does not have would import as a graph the
      // destination believes is whole.
      expect(capsule.payload.manifest.completeness).toBe('roots_only')
    })
  })

  it('verifies integrity, and reports signed separately from valid', async () => {
    await withNexus('verify', (nexus) => {
      const capsule = nexus.describe(
        'EXPORT CAPSULE :out WHERE { ?a ASSERTION {} }',
      )
      const report = nexus.describe('VERIFY CAPSULE :c', {
        c: JSON.stringify(capsule),
      }) as { valid: boolean; signed: boolean; note: string }
      expect(report.valid).toBe(true)
      // Intact is not trustworthy, and the answer keeps them apart.
      expect(report.signed).toBe(false)
      expect(report.note).toMatch(/intact, not that its claims are true/)
    })
  })

  it('catches a Capsule modified after it was written', async () => {
    await withNexus('tamper', (nexus) => {
      const capsule = nexus.describe(
        'EXPORT CAPSULE :out WHERE { ?c CONCEPT {name: "Alice"} }',
      ) as { payload: { records: { concepts: { name: string }[] } } }
      const tampered = structuredClone(capsule) as typeof capsule
      const concept = tampered.payload.records.concepts[0]
      if (concept !== undefined) concept.name = 'Mallory'

      expect(() =>
        nexus.describe('VERIFY CAPSULE :c', { c: JSON.stringify(tampered) }),
      ).toThrowError(/was modified after it was written/)
    })
  })

  it('refuses the import path rather than half-building it', async () => {
    await withNexus('import', (nexus) => {
      // A half-built import hands the destination a graph with broken edges
      // and no way to notice.
      expect(() =>
        nexus.describe('PREVIEW IMPORT CAPSULE :c INTO "kip:space:default"', {
          c: '{}',
        }),
      ).toThrowError(/import path, which this engine has not built/)
    })
  })

  // --- LIST DEPENDENTS (§63.5) ----------------------------------------------

  /**
   * Two derivation hops recorded as Activity provenance.
   *
   * ```text
   * Event ──inputs──▸ consolidation ──outputs──▸ Insight
   *                   Insight ──inputs──▸ compilation ──outputs──▸ Skill
   * ```
   */
  const DERIVED = [
    `MUTATE {
      CREATE CONCEPT ?event {
        TYPE "Event"
        NAME "Migration meeting"
        SET ATTRIBUTES {summary: "The team agreed to migrate on Friday"}
      }
      CREATE CONCEPT ?insight {
        TYPE "Insight"
        NAME "Migrations need a rollback plan"
        SET ATTRIBUTES {summary: "Every migration ships with a rollback"}
      }
      CREATE ACTIVITY ?consolidate {
        SET FIELDS {activity_class: "semantic_consolidation", status: "completed"}
        SET STRUCTURAL {
          ("inputs", ?event)
          ("outputs", ?insight)
        }
      }
    }`,
    `MUTATE {
      CREATE CONCEPT ?skill {
        TYPE "Skill"
        NAME "Plan a migration"
        SET ATTRIBUTES {
          skill_class: "workflow",
          task_family: "migration/rollback",
          summary: "Write the rollback first",
          procedure: "1. write the rollback 2. migrate",
          status: "proposed"
        }
      }
      CREATE ACTIVITY ?compile {
        SET FIELDS {activity_class: "procedural_consolidation", status: "completed"}
        SET STRUCTURAL {
          ("inputs", "C-4")
          ("outputs", ?skill)
        }
      }
    }`,
  ]

  interface Dependent {
    id: string
    kind: string
    distance: number
    via: { activity: string }
  }

  it('walks the provenance DAG in the derived direction', async () => {
    // §63.5: X ∈ Activity.inputs → that Activity → each element in its outputs.
    // This is the read §57.5 asks a Brain to make after revising a root: the
    // cognition built on the old claim is still active state, and it has to be
    // findable before it can be reviewed.
    await withNexus('dependents', (nexus) => {
      for (const statement of DERIVED) nexus.execute(statement)

      const answer = nexus.describePage('LIST DEPENDENTS "C-3"')
      // §63.5: nothing the caller may not discover cut the walk short. The
      // rows stay the bare array every LIST answers with; the flag rides
      // beside the page cursor.
      expect(answer.truncated).toBe(false)
      const one = answer.result as unknown as Dependent[]
      expect(one).toHaveLength(1)
      expect(one[0]!.id).toBe('C-4')
      expect(one[0]!.kind).toBe('concept')
      expect(one[0]!.distance).toBe(1)
      // The row names the Activity it was reached through.
      expect(one[0]!.via.activity).toBe('X-1')

      // DEPTH is what turns one hop into the closure. Default is one hop, so
      // the Skill two derivations away is out of reach until it is asked for.
      const two = nexus.describe(
        'LIST DEPENDENTS "C-3" DEPTH 2',
      ) as unknown as Dependent[]
      expect(two).toHaveLength(2)
      expect(two[1]!.id).toBe('C-5')
      expect(two[1]!.distance).toBe(2)
      expect(two[1]!.via.activity).toBe('X-2')

      // Reachability is provenance topology, not judgment (§57.5): nothing
      // about the listed element changed.
      const insight = nexus.store.load(parseElementId('C-4'))
      expect(insight?.row.state).toBe('active')
      expect(insight?.row.version).toBe(1)
    })
  })

  it('pages a dependents closure like every other LIST', async () => {
    await withNexus('dependents-paging', (nexus) => {
      for (const statement of DERIVED) nexus.execute(statement)
      const page = nexus.describe(
        'LIST DEPENDENTS "C-3" DEPTH 2 LIMIT 1',
      ) as unknown as Dependent[]
      expect(page).toHaveLength(1)
      expect(page[0]!.id).toBe('C-4')
    })
  })

  it('cannot discover a transformation that recorded no Activity lineage', async () => {
    // §63.5's own caveat, and the reason the Profile's consolidation guidance
    // insists on citing the inputs you actually relied on: an uncited input is
    // an invisible dependency, and this command cannot invent the edge.
    await withNexus('dependents-unlinked', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?event {
          TYPE "Event"
          NAME "Meeting"
          SET ATTRIBUTES {summary: "A meeting happened"}
        }
        CREATE CONCEPT ?insight {
          TYPE "Insight"
          NAME "Undeclared derivation"
          SET ATTRIBUTES {summary: "Derived from the meeting, but nobody said so"}
        }
      }`)
      expect(nexus.describe('LIST DEPENDENTS "C-3" DEPTH 4')).toEqual([])
    })
  })

  it('answers an unknown dependents root exactly as an absent one', async () => {
    // §30.4: omission is indistinguishable from absence. An error here would
    // turn the command into an existence oracle.
    await withNexus('dependents-absent', (nexus) => {
      expect(nexus.describe('LIST DEPENDENTS "C-999"')).toEqual([])
      // A string that is not an element id at all is a different mistake, and
      // is reported as one.
      expect(() => nexus.describe('LIST DEPENDENTS "not-an-id"')).toThrowError(
        /is not a Nexus element id/,
      )
    })
  })
})
