import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'

/**
 * Reading the Space at a past coordinate.
 *
 * `AS OF` asks what this Brain *held* then; `FOR TIME` asks what was *true*
 * then (§36.1). They are different axes and the tests keep them apart on
 * purpose — an engine that let one default from the other would answer a
 * question nobody asked, and would do it silently.
 */
async function withNexus<T>(
  name: string,
  body: (nexus: CognitiveNexus) => T,
): Promise<T> {
  const stub = env.KIP_DB.getByName(`history-${name}`)
  return await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    return body(nexus)
  })
}

describe('AS OF', () => {
  it('finds an element in the state it had, not the state it has', async () => {
    await withNexus('states', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')

      expect(nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} }')).toEqual(['Alicia'])
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF SEQ 1'),
      ).toEqual(['Alice'])
    })
  })

  it('does not find an element that did not exist yet', async () => {
    await withNexus('not-yet', (nexus) => {
      nexus.execute('CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" }')
      nexus.execute('CREATE CONCEPT ?b { TYPE "Person" NAME "Bob" }')

      // Not "found in a state it never had" and not an error: it was not there.
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF SEQ 1'),
      ).toEqual(['Alice'])
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF SEQ 2').sort(),
      ).toEqual(['Alice', 'Bob'])
      // A coordinate before anything existed is an empty Space, not a failure.
      expect(nexus.query('FIND(?c) WHERE { ?c CONCEPT {} } AS OF SEQ 0')).toEqual([])
    })
  })

  it('reads the lifecycle state of the coordinate, not of today', async () => {
    await withNexus('lifecycle', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      nexus.execute('TRANSITION "C-1" TO "archived"')

      // A pattern matches active elements unless it says otherwise, and what
      // was active is a question about the coordinate: the index that says
      // "archived" describes today.
      expect(nexus.query('FIND(?c) WHERE { ?c CONCEPT {} }')).toEqual([])
      expect(nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF SEQ 1')).toEqual([
        'Alice',
      ])
    })
  })

  it('resolves a transaction id to its coordinate through DESCRIBE TRANSACTION', async () => {
    // §48.1: AS OF SEQ is the only historical axis. A transaction id resolves
    // to its sequence through DESCRIBE TRANSACTION, so a historical read
    // always names the exact coordinate it was served from.
    await withNexus('by-tx', (nexus) => {
      const receipt = nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')

      // The coordinate is `space_seq`, the name §36.1 gives it and the one
      // the reference engine answers with — not the storage column behind it.
      const described = nexus.describe('DESCRIBE TRANSACTION :tx', {
        tx: receipt.tx_id,
      }) as { space_seq: number; status: string }
      expect(described.space_seq).toBe(1)
      expect(described.status).toBe('committed')
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF SEQ :seq', {
          seq: described.space_seq,
        }),
      ).toEqual(['Alice'])
      // An unknown transaction names no coordinate — refusing beats answering
      // about the present under a name that meant something else.
      expect(() =>
        nexus.describe('DESCRIBE TRANSACTION :tx', { tx: 'tx-nope' }),
      ).toThrowError(/no transaction/)
      // And the grammar no longer admits the removed axes at all.
      expect(() =>
        nexus.query('FIND(?c) WHERE { ?c CONCEPT {} } AS OF TX :tx', { tx: 'x' }),
      ).toThrowError()
    })
  })

  it('refuses a coordinate the Space has not reached', async () => {
    await withNexus('future', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      // Rounding to the present would answer a different question and say
      // nothing about having done so, which is the worst available behaviour
      // for a read whose whole point is *when*.
      expect(() =>
        nexus.query('FIND(?c) WHERE { ?c CONCEPT {} } AS OF SEQ 9999'),
      ).toThrowError(/names no coordinate/)
    })
  })

  it('answers a tuple pattern at the coordinate too', async () => {
    await withNexus('tuples', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
      }`)
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?light { TYPE "Preference" NAME "Light" }
        ENSURE PROPOSITION ?q ({id: "C-1"}, "prefers", ?light)
      }`)

      expect(
        nexus.query('FIND(?o.name) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }').sort(),
      ).toEqual(['Dark', 'Light'])
      expect(
        nexus.query(
          'FIND(?o.name) WHERE { ?p PROPOSITION (?s, "prefers", ?o) } AS OF SEQ 1',
        ),
      ).toEqual(['Dark'])
    })
  })

  it('projects a belief from the Assertions of that coordinate', async () => {
    await withNexus('projection', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        CREATE ASSERTION ?a {
          SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 }
        }
      }`)
      nexus.execute('TRANSITION "A-1" TO "retracted"')

      const STATUS =
        'FIND(?b.status) WHERE { ?p PROPOSITION (?s, "prefers", ?o) ?b BELIEF (?p) }'
      // Retracted today: nobody is committed to it any more.
      expect(nexus.query(STATUS)).toEqual(['insufficient'])
      // At the coordinate before the retraction, the commitment stood. A
      // projection that read today's Assertions under a past coordinate would
      // answer neither question.
      expect(nexus.query(`${STATUS} AS OF SEQ 1`)).toEqual(['accepted'])
      expect(nexus.query(`${STATUS} AS OF SEQ 1 WITH EPISTEMIC {policy: \"baseline\"}`)).toEqual(['accepted'])
    })
  })

  it('resolves symbols through the Schema that was in force then', async () => {
    await withNexus('schema', (nexus) => {
      const before = nexus.environment().version
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')

      // A genuinely different lock, so a new environment version is really
      // minted rather than the activation being a no-op.
      nexus.activatePackages([])
      expect(nexus.environment().version).toBe(before + 1)

      // §20.9: the Concept at coordinate 1 was written under the environment
      // before the activation, and that is what a read there resolves through.
      // Reconstructing the past under today's schema would return different
      // elements rather than an error.
      const then = nexus.describe('DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ 1') as {
        version: number
        packages: string[]
      }
      expect(then.version).toBe(before)
      expect(then.packages.some((ref) => ref.includes('cognitive-memory'))).toBe(true)

      const now = nexus.describe('DESCRIBE SCHEMA ENVIRONMENT') as {
        packages: string[]
      }
      expect(now.packages.some((ref) => ref.includes('cognitive-memory'))).toBe(false)

      // And it is the query path that has to honour it, not only the report:
      // `"Person"` no longer resolves under today's environment, and still
      // resolves at the coordinate where it was written.
      const FIND = 'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }'
      expect(() => nexus.query(FIND)).toThrowError(
        /no active Schema Package defines/,
      )
      expect(nexus.query(`${FIND} AS OF SEQ 1`)).toEqual(['Alice'])
    })
  })

  it('reconstructs STRUCTURAL edges instead of reading the current index', async () => {
    await withNexus('historical-structural', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?s1 { TYPE "ExperienceStep" NAME "Step one" SET ATTRIBUTES {step_kind: "action", summary: "one"} }
        CREATE CONCEPT ?s2 { TYPE "ExperienceStep" NAME "Step two" SET ATTRIBUTES {step_kind: "action", summary: "two"} }
        CREATE CONCEPT ?exp {
          TYPE "Experience" NAME "Deploy"
          SET ATTRIBUTES {goal: "ship", outcome_status: "success"}
          SET STRUCTURAL { ("has_step", ?s1) }
        }
      }`)
      nexus.execute(
        `UPDATE "C-3"
           SET STRUCTURAL { ("has_step", :new) }
           UNSET STRUCTURAL { ("has_step", :old) }`,
        { new: 'C-2', old: 'C-1' },
      )
      const find = `FIND(?step.name) WHERE {
        ?exp CONCEPT {name: "Deploy"}
        STRUCTURAL (?exp, "has_step", ?step)
      }`
      expect(nexus.query(find)).toEqual(['Step two'])
      expect(nexus.query(`${find} AS OF SEQ 1`)).toEqual(['Step one'])
    })
  })

  it('gives a later Schema activation its own snapshot coordinate', async () => {
    await withNexus('schema-coordinate', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      const before = nexus.environment().version
      nexus.activatePackages([])
      const snapshot = nexus.describe('DESCRIBE SNAPSHOT') as {
        space_seq: number
        schema_environment_version: number
      }
      expect(snapshot.space_seq).toBe(2)
      expect(snapshot.schema_environment_version).toBe(before + 1)
    })
  })
})

describe('DESCRIBE SNAPSHOT', () => {
  it('describes a coordinate: the sequence, the transaction that committed it, and when', async () => {
    // §68: a snapshot coordinate is a description, not only a token. The
    // transaction id it names is what DESCRIBE TRANSACTION resolves back.
    await withNexus('coordinate', (nexus) => {
      const empty = nexus.describe('DESCRIBE SNAPSHOT') as {
        space_id: string
        space_seq: number
        tx_id: string | null
        committed_at: string | null
      }
      // Before the first commit: coordinate 0, and nothing committed it.
      expect(empty.space_seq).toBe(0)
      expect(empty.tx_id).toBeNull()
      expect(empty.committed_at).toBeNull()

      const receipt = nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      const head = nexus.describe('DESCRIBE SNAPSHOT') as typeof empty
      expect(head.space_id).toBe(nexus.space)
      expect(head.space_seq).toBe(1)
      expect(head.tx_id).toBe(receipt.tx_id)
      expect(head.committed_at).toBe(receipt.committed_at)

      // A past coordinate, named by sequence.
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')
      const past = nexus.describe('DESCRIBE SNAPSHOT AS OF SEQ 1') as typeof empty
      expect(past.space_seq).toBe(1)
      expect(past.tx_id).toBe(receipt.tx_id)
      // A coordinate the Space has not reached is refused, never rounded.
      expect(() => nexus.describe('DESCRIBE SNAPSHOT AS OF SEQ 9999')).toThrowError(
        /names no coordinate/,
      )
    })
  })

  it('resolves an instant to the last sequence committed at or before it', async () => {
    // §48.1, §68: this is how wall-clock time enters AS OF SEQ. The engine
    // never guesses which of several sequences an instant means; the caller
    // reads the coordinate and names it.
    await withNexus('at-time', (nexus) => {
      const first = nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      const second = nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')
      const at = (t: string) =>
        nexus.describe('DESCRIBE SNAPSHOT AT TIME :t', { t }) as {
          space_seq: number
          tx_id: string | null
        }

      // Before anything was committed: coordinate 0, an empty Space and not
      // an error.
      expect(at('2000-01-01T00:00:00Z').space_seq).toBe(0)
      // At the first commit's own instant, that commit; after the second,
      // the second.
      expect(at(first.committed_at!).space_seq).toBeGreaterThanOrEqual(1)
      expect(at(second.committed_at!).space_seq).toBe(2)
      expect(at(second.committed_at!).tx_id).toBe(second.tx_id)
      expect(at('2999-01-01T00:00:00Z').space_seq).toBe(2)
      expect(() => nexus.describe('DESCRIBE SNAPSHOT AT TIME "yesterday"')).toThrowError()
    })
  })

  it('issues a token that binds a later read to its coordinate', async () => {
    await withNexus('token', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      const snapshot = nexus.describe('DESCRIBE SNAPSHOT') as {
        space_seq: number
        snapshot_token: string
      }
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')

      expect(snapshot.space_seq).toBe(1)
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} }', {}, {
          snapshot_token: snapshot.snapshot_token,
        }),
      ).toEqual(['Alice'])
    })
  })

  it('refuses a token issued for another Space, as a malformed snapshot cursor', async () => {
    await withNexus('cross-space', (nexus) => {
      const snapshot = nexus.describe('DESCRIBE SNAPSHOT') as { snapshot_token: string }
      // The token carries its Space, because the same sequence means something
      // entirely different in another one. §87.7: the refusal names the
      // family and the reason.
      const forged = Buffer.from('kip:snapshot:kip:space:other:1').toString('hex')
      let refused: { code: string; details?: unknown } | null = null
      try {
        nexus.query('FIND(?c) WHERE { ?c CONCEPT {} }', {}, { snapshot_token: forged })
      } catch (err) {
        refused = err as { code: string; details?: unknown }
      }
      expect(refused?.code).toBe('CursorInvalid')
      expect(refused?.details).toEqual({ family: 'snapshot', reason: 'malformed' })
      expect(snapshot.snapshot_token).not.toBe(forged)
    })
  })

  it('refuses a request whose token and command name different coordinates', async () => {
    await withNexus('disagreement', (nexus) => {
      nexus.execute('CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" }')
      const snapshot = nexus.describe('DESCRIBE SNAPSHOT') as { snapshot_token: string }
      nexus.execute('CREATE CONCEPT ?b { TYPE "Person" NAME "Bob" }')

      // One read answers at one coordinate: an answer whose own `snapshot_seq`
      // could not say which of two it meant is worse than a refusal.
      expect(() =>
        nexus.query('FIND(?c) WHERE { ?c CONCEPT {} } AS OF SEQ 2', {}, {
          snapshot_token: snapshot.snapshot_token,
        }),
      ).toThrowError(/one read answers at one coordinate/)
      // Naming the same one is fine.
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF SEQ 1', {}, {
          snapshot_token: snapshot.snapshot_token,
        }),
      ).toEqual(['Alice'])
    })
  })
})

describe('FOR TIME', () => {
  it('filters on when a claim applied, not on when it was recorded', async () => {
    await withNexus('valid-time', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        CREATE ASSERTION ?a {
          SET FIELDS {
            proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated",
            confidence: 0.9,
            valid_time: {from: "2020-01-01T00:00:00Z", until: "2021-01-01T00:00:00Z"}
          }
        }
      }`)

      const FIND = 'FIND(?a) WHERE { ?a ASSERTION {} }'
      expect(nexus.query(FIND)).toHaveLength(1)
      // Inside the interval the claim applied…
      expect(
        nexus.query(`${FIND} FOR TIME "2020-06-01T00:00:00Z"`),
      ).toHaveLength(1)
      // …and outside it, it did not. The Assertion is still recorded and still
      // readable: `FOR TIME` narrows what applied, not what exists.
      expect(nexus.query(`${FIND} FOR TIME "2022-01-01T00:00:00Z"`)).toEqual([])
      expect(nexus.query(`${FIND} FOR TIME "2019-01-01T00:00:00Z"`)).toEqual([])
    })
  })

  it('is a different axis from AS OF and does not default from it', async () => {
    await withNexus('axes', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        CREATE ASSERTION ?a {
          SET FIELDS {
            proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated",
            confidence: 0.9, valid_time: {from: "2020-01-01T00:00:00Z"}
          }
        }
      }`)
      // The claim was recorded now and applies from 2020. Reading the Brain as
      // it stood at coordinate 1 finds it; asking what applied in 2019 does
      // not. Confusing the two is a semantic bug, not a formatting one.
      expect(
        nexus.query('FIND(?a) WHERE { ?a ASSERTION {} } AS OF SEQ 1'),
      ).toHaveLength(1)
      expect(
        nexus.query('FIND(?a) WHERE { ?a ASSERTION {} } FOR TIME "2019-01-01T00:00:00Z"'),
      ).toEqual([])
    })
  })
})
