import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY, type SchemaPackage } from '../src/schema/index.js'

/**
 * The Epistemic Projection, against the cases the conformance suite pins.
 *
 * `fixtures/kip-conformance-2.0/epistemic-projection.json` is the contract both
 * engines owe. Its extra package is here for the same reason it is there: a
 * `functional` predicate is what makes support for one value opposition to
 * another, and the bundled profile declares none.
 */
const STATUS_PACKAGE: SchemaPackage = {
  format: 'KIP-Schema-Package',
  manifest: {
    package_id: 'kip://conformance/status',
    version: '1.0.0',
    package_ref: 'kip://conformance/status@1.0.0',
  },
  definitions: {
    concept_types: {
      Service: { kind: 'ConceptType', description: 'A service.' },
      Status: { kind: 'ConceptType', description: 'A status value.' },
    },
    predicates: {
      status: {
        kind: 'PredicateType',
        description: 'Single-valued current status.',
        functional: true,
      },
    },
  },
}

const SETUP = [
  `MUTATE {
     CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
     CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
     CREATE CONCEPT ?carol { TYPE "Person" NAME "Carol" }
     CREATE CONCEPT ?quiet { TYPE "Preference" NAME "Quiet" }
     CREATE CONCEPT ?loud { TYPE "Preference" NAME "Loud" }
     ENSURE PROPOSITION ?unspoken (?alice, "prefers", ?quiet)
     ENSURE PROPOSITION ?repeated (?alice, "prefers", ?loud)
   }`,
  `MUTATE {
     CREATE CONCEPT ?svc { TYPE "Service" NAME "api" }
     CREATE CONCEPT ?healthy { TYPE "Status" NAME "healthy" }
     CREATE CONCEPT ?degraded { TYPE "Status" NAME "degraded" }
     ENSURE PROPOSITION ?ok (?svc, "status", ?healthy)
     ENSURE PROPOSITION ?bad (?svc, "status", ?degraded)
   }`,
]

async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus) => void,
): Promise<void> {
  const stub = env.KIP_DB.getByName(`proj-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY, STATUS_PACKAGE])
    for (const setup of SETUP) nexus.execute(setup)
    body(nexus)
  })
}

/** The belief about `(Alice, prefers, <name>)`. */
const BELIEF = (name: string) => `WHERE {
  ?s CONCEPT {name: "Alice"}
  ?o CONCEPT {name: "${name}"}
  ?p PROPOSITION (?s, "prefers", ?o)
  ?b BELIEF (?p)
}`

/** Adds one Assertion about a Proposition, by actor name. */
function assert_(
  nexus: CognitiveNexus,
  object: string,
  actor: string,
  stance: string,
  confidence: number,
  evidence?: string,
): string {
  const [pid] = nexus.query(
    `FIND(?p.id) WHERE {
       ?s CONCEPT {name: "Alice"} ?o CONCEPT {name: "${object}"}
       ?p PROPOSITION (?s, "prefers", ?o)
     }`,
  ) as string[]
  const [aid] = nexus.query(
    `FIND(?c.id) WHERE { ?c CONCEPT {name: "${actor}"} }`,
  ) as string[]
  const cite = evidence === undefined ? '' : `SET STRUCTURAL { ("evidence", :e) }`
  const out = nexus.execute(
    `CREATE ASSERTION ?a {
       SET FIELDS { proposition: :p, asserted_by: :who, stance: "${stance}", mode: "stated", confidence: ${confidence} }
       ${cite}
     }`,
    { p: pid!, who: { id: aid! }, ...(evidence === undefined ? {} : { e: evidence }) },
  )
  return out.handles.a as string
}

function newEvidence(nexus: CognitiveNexus, text: string): string {
  return nexus.execute(
    `CREATE EVIDENCE ?e {
       SET FIELDS { evidence_class: "observation", payload: {inline: "${text}"} }
     }`,
  ).handles.e as string
}

describe('the Epistemic Projection', () => {
  it('reports nothing on record as insufficient, never as rejected', async () => {
    await withNexus('silence', (nexus) => {
      // The open-world state. Reporting it as `rejected` would turn an absence
      // of records into a claim about the world.
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Quiet')}`)).toEqual([
        'insufficient',
      ])
      expect(
        nexus.query(`FIND(?b.support.score, ?b.opposition.score) ${BELIEF('Quiet')}`),
      ).toEqual([[0, 0]])
    })
  })

  it('declares that its score is not a probability', async () => {
    await withNexus('semantics', (nexus) => {
      // A number between 0 and 1 looks like a probability, and this one is not
      // calibrated as one — so it says so rather than letting a reader assume.
      expect(
        nexus.query(`FIND(?b.support.score_semantics) ${BELIEF('Quiet')}`),
      ).toEqual(['normalized_support_not_probability'])
    })
  })

  it('reports the policy it ran under', async () => {
    await withNexus('policy', (nexus) => {
      // "accepted" with no policy attached is not an auditable statement.
      expect(nexus.query(`FIND(?b.policy.id) ${BELIEF('Quiet')}`)).toEqual([
        'kip:policy:baseline',
      ])
      expect(nexus.query(`FIND(?b.policy.version) ${BELIEF('Quiet')}`)).toEqual([1])
    })
  })

  it('names an unknown policy rather than defaulting to the baseline', async () => {
    await withNexus('unknown-policy', (nexus) => {
      // A caller that asked for a stricter reading and silently got the
      // ordinary one would act on an answer it did not request.
      expect(() =>
        nexus.query(`FIND(?b) ${BELIEF('Quiet')} WITH EPISTEMIC {policy: "strict"}`),
      ).toThrowError(/no Epistemic Policy named/)
    })
  })

  it('refuses to project over an unbound target rather than guessing', async () => {
    await withNexus('unbound', (nexus) => {
      // Projecting over every Proposition in the Space is a different question
      // from the one asked, not a slower version of it.
      expect(() =>
        nexus.query('FIND(?b) WHERE { ?b BELIEF (?nothing) }'),
      ).toThrowError(/not bound to a Proposition/)
      expect(() =>
        nexus.query('FIND(?slot) WHERE { ?slot BELIEF SLOT (?anything, "prefers") }'),
      ).toThrowError(/needs a bound subject/)
    })
  })

  it('does not count repetition as corroboration', async () => {
    await withNexus('repetition', (nexus) => {
      // One actor saying the same thing three times is one source.
      assert_(nexus, 'Loud', 'Alice', 'support', 0.6)
      assert_(nexus, 'Loud', 'Alice', 'support', 0.6)
      assert_(nexus, 'Loud', 'Alice', 'support', 0.6)
      expect(
        nexus.query(`FIND(?b.support.independent_groups) ${BELIEF('Loud')}`),
      ).toEqual([1])
      expect(nexus.query(`FIND(?b.support.score) ${BELIEF('Loud')}`)).toEqual([0.6])
      // Three independent moderate sources say more than one, and still not
      // enough to be a probability.
      assert_(nexus, 'Loud', 'Bob', 'support', 0.6)
      assert_(nexus, 'Loud', 'Carol', 'support', 0.6)
      const [groups] = nexus.query(
        `FIND(?b.support.independent_groups) ${BELIEF('Loud')}`,
      ) as number[]
      expect(groups).toBe(3)
      const [score] = nexus.query(`FIND(?b.support.score) ${BELIEF('Loud')}`) as number[]
      expect(score).toBeCloseTo(1 - 0.4 ** 3, 10)
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Loud')}`)).toEqual(['accepted'])
    })
  })

  it('merges groups that share Evidence', async () => {
    await withNexus('shared-evidence', (nexus) => {
      // Two people relaying the same observation are one observation. The
      // third claim citing both is what collapses two apparently independent
      // groups into one — the shape manufactured corroboration takes.
      const seen = newEvidence(nexus, 'the same observation')
      assert_(nexus, 'Loud', 'Alice', 'support', 0.6, seen)
      assert_(nexus, 'Loud', 'Bob', 'support', 0.6, seen)
      expect(
        nexus.query(`FIND(?b.support.independent_groups) ${BELIEF('Loud')}`),
      ).toEqual([1])
      expect(nexus.query(`FIND(?b.support.score) ${BELIEF('Loud')}`)).toEqual([0.6])
    })
  })

  it('holds a contested claim as contested rather than deciding it', async () => {
    await withNexus('contested', (nexus) => {
      assert_(nexus, 'Loud', 'Alice', 'support', 0.8)
      assert_(nexus, 'Loud', 'Bob', 'reject', 0.8)
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Loud')}`)).toEqual(['contested'])
      expect(
        nexus.query(`FIND(?b.uncertainty.level) ${BELIEF('Loud')}`),
      ).toEqual(['high'])
    })
  })

  it('never infers rejection from an absence of support', async () => {
    await withNexus('rejection', (nexus) => {
      // Rejection needs positive opposition. Silence stays insufficient.
      assert_(nexus, 'Loud', 'Alice', 'reject', 0.9)
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Loud')}`)).toEqual(['rejected'])
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Quiet')}`)).toEqual([
        'insufficient',
      ])
    })
  })

  it('leaves a retracted claim out and says why', async () => {
    await withNexus('retracted', (nexus) => {
      const id = assert_(nexus, 'Loud', 'Alice', 'support', 0.9)
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Loud')}`)).toEqual(['accepted'])
      nexus.execute(`RETRACT ASSERTION "${id}"`)
      // Withdrawn is history, and history is not what this Brain holds now.
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Loud')}`)).toEqual([
        'insufficient',
      ])
      expect(
        nexus.query(`FIND(?b.explanation.excluded) ${BELIEF('Loud')}`),
      ).toEqual([[{ assertion_id: id, reason: 'lifecycle_retracted' }]])
    })
  })

  it('admits a mode by policy, and excludes one by naming the reason', async () => {
    await withNexus('modes', (nexus) => {
      const [pid] = nexus.query(
        `FIND(?p.id) WHERE {
           ?s CONCEPT {name: "Alice"} ?o CONCEPT {name: "Loud"}
           ?p PROPOSITION (?s, "prefers", ?o)
         }`,
      ) as string[]
      nexus.execute(
        `CREATE ASSERTION ?a {
           SET FIELDS { proposition: :p, stance: "support", mode: "hypothetical", confidence: 0.95 }
         }`,
        { p: pid! },
      )
      // Entertained without commitment is not an answer to "what is the case".
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Loud')}`)).toEqual([
        'insufficient',
      ])
      expect(
        nexus.query(`FIND(?b.explanation.excluded) ${BELIEF('Loud')}`),
      ).toEqual([[{ assertion_id: 'A-1', reason: 'hypothetical_not_requested' }]])
      // A policy that asks for predictions is a different question, and says so
      // by carrying a different identity.
      expect(
        nexus.query(
          `FIND(?b.policy.id) ${BELIEF('Loud')} WITH EPISTEMIC {policy: "forecast"}`,
        ),
      ).toEqual(['kip:policy:forecast'])
    })
  })

  it('changes its identity when a threshold is overridden', async () => {
    await withNexus('custom-policy', (nexus) => {
      assert_(nexus, 'Loud', 'Alice', 'support', 0.6)
      expect(nexus.query(`FIND(?b.status) ${BELIEF('Loud')}`)).toEqual(['uncertain'])
      // An answer reporting `kip:policy:baseline` while running on different
      // numbers would be a false audit trail — worse than none, because it
      // looks like one.
      expect(
        nexus.query(
          `FIND(?b.status, ?b.policy.id) ${BELIEF('Loud')} WITH EPISTEMIC {accept: 0.5}`,
        ),
      ).toEqual([['accepted', 'kip:policy:baseline+custom']])
    })
  })

  it('warns about the stages it does not have', async () => {
    await withNexus('warnings', (nexus) => {
      // A caller reading `accepted` without these would believe the engine
      // weighed who said it and how good the evidence was.
      const [warnings] = nexus.query(
        `FIND(?b.explanation.warnings) ${BELIEF('Quiet')}`,
      ) as string[][]
      expect(warnings).toHaveLength(2)
      expect(warnings?.join(' ')).toMatch(/no trust model/)
      expect(warnings?.join(' ')).toMatch(/no evidence-quality evaluation/)
    })
  })

  it('turns support for a rival value into opposition on a functional slot', async () => {
    await withNexus('functional', (nexus) => {
      // The schema says the slot holds one value, so claiming another value
      // *is* disagreeing — even though no Assertion anywhere says "not this".
      const pid = (
        nexus.query(
          `FIND(?p.id) WHERE {
             ?s CONCEPT {name: "api"} ?o CONCEPT {name: "healthy"}
             ?p PROPOSITION (?s, "status", ?o)
           }`,
        ) as string[]
      )[0]
      const rival = (
        nexus.query(
          `FIND(?p.id) WHERE {
             ?s CONCEPT {name: "api"} ?o CONCEPT {name: "degraded"}
             ?p PROPOSITION (?s, "status", ?o)
           }`,
        ) as string[]
      )[0]
      const alice = (
        nexus.query('FIND(?c.id) WHERE { ?c CONCEPT {name: "Alice"} }') as string[]
      )[0]
      const bob = (
        nexus.query('FIND(?c.id) WHERE { ?c CONCEPT {name: "Bob"} }') as string[]
      )[0]
      const claim = (p: string, who: string, confidence: number) =>
        nexus.execute(
          `CREATE ASSERTION ?a {
             SET FIELDS { proposition: :p, asserted_by: :who, stance: "support", mode: "stated", confidence: ${confidence} }
           }`,
          { p, who: { id: who } },
        )
      claim(pid!, alice!, 0.8)
      claim(rival!, bob!, 0.8)

      const status = nexus.query(
        `FIND(?b.status) WHERE {
           ?s CONCEPT {name: "api"} ?o CONCEPT {name: "healthy"}
           ?p PROPOSITION (?s, "status", ?o)
           ?b BELIEF (?p)
         }`,
      )
      expect(status).toEqual(['contested'])

      // BELIEF SLOT reports the conflict set, not a winner: naming one would
      // take a side the record does not take.
      const [slot] = nexus.query(
        `FIND(?slot) WHERE {
           ?s CONCEPT {name: "api"}
           ?slot BELIEF SLOT (?s, "status")
         }`,
      ) as { candidate_projections: unknown[]; accepted_values: string[]; contested: boolean }[]
      expect(slot?.candidate_projections).toHaveLength(2)
      expect(slot?.accepted_values).toEqual([])
      expect(slot?.contested).toBe(true)
    })
  })
})
