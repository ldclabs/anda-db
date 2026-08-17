import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'

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
      expect(gaps).toContain('set_retention')
      expect(gaps).toContain('trust_model')
      expect(gaps).toContain('search')
      expect(gaps).toContain('capsule_import')
      expect(gaps).toContain('hop_quantifiers')
      // Every gap carries a reason, not just a name.
      for (const entry of report.unsupported) {
        expect(entry.reason.length, entry.capability).toBeGreaterThan(20)
      }
    })
  })

  it('orients an Agent before its first command', async () => {
    await withNexus('primer', (nexus) => {
      const primer = nexus.describe('DESCRIBE PRIMER') as {
        types: string[]
        predicates: string[]
        packages: string[]
      }
      expect(primer.types).toContain(`${CM}/Person`)
      expect(primer.predicates).toContain(`${CM}/prefers`)
      expect(primer.packages).toContain(CM)
    })
  })

  it('answers about a symbol with its canonical identity, never a local name', async () => {
    await withNexus('symbol', (nexus) => {
      // §106: a local name means nothing outside the environment that
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
      expect(nexus.describe('LIST TYPES')).toContain(`${CM}/Person`)
      expect(nexus.describe('LIST EPISTEMIC POLICIES')).toEqual([
        'kip:policy:baseline',
        'kip:policy:forecast',
      ])
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

  it('answers HISTORY from the version log and the journal', async () => {
    await withNexus('history', (nexus) => {
      const [id] = nexus.query(
        'FIND(?c.id) WHERE { ?c CONCEPT {name: "Alice"} }',
      ) as string[]
      nexus.execute(`ARCHIVE "${id!}"`)

      const versions = nexus.describe(`HISTORY ELEMENT "${id!}"`) as {
        version: number
        op: string
      }[]
      expect(versions.map((v) => v.op)).toEqual(['create', 'archive'])
      expect(versions.map((v) => v.version)).toEqual([1, 2])

      const space = nexus.describe('HISTORY SPACE') as { space_seq: number }[]
      expect(space).toHaveLength(2)
    })
  })

  it('reports CHANGES after a coordinate and hands back where it got to', async () => {
    await withNexus('changes', (nexus) => {
      const first = nexus.describe('CHANGES AFTER SEQ 0') as {
        changes: { id: string; op: string }[]
        cursor: number
      }
      expect(first.changes).toHaveLength(4)
      expect(first.cursor).toBe(1)
      // A caller that saw nothing holds the same place rather than starting
      // over.
      const again = nexus.describe('CHANGES AFTER SEQ 1') as {
        changes: unknown[]
        cursor: number
      }
      expect(again.changes).toEqual([])
      expect(again.cursor).toBe(1)
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
      // binds a later read to that coordinate.
      const snapshot = nexus.describe('SNAPSHOT') as {
        snapshot_seq: number
        snapshot_token: string
      }
      expect(snapshot.snapshot_seq).toBe(nexus.store.currentSeq(nexus.space))
      expect(snapshot.snapshot_token).toMatch(/^[0-9a-f]+$/)
      // Reporting an unchecked artifact as valid would cancel the point of
      // asking.
      expect(() =>
        nexus.describe('VERIFY SCHEMA PACKAGE "kip://core@2.0.0"'),
      ).toThrowError(/not implemented by this engine/)
      // A keyword search over a narrower index than the caller expects is
      // indistinguishable from a narrower world.
      expect(() => nexus.describe('SEARCH CONCEPT "Alice"')).toThrowError(
        /builds no search index/,
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
      // §240.47: the exact refs travel with the records, or the Capsule
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
      expect(capsule.integrity.content_digest).toMatch(/^sha256:[0-9a-f]{64}$/)
    })
  })

  it('exports only the roots when the closure is turned off', async () => {
    await withNexus('roots-only', (nexus) => {
      const capsule = nexus.describe(
        'EXPORT CAPSULE :out WHERE { ?a ASSERTION {} } WITH {closure: "none"}',
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
})
