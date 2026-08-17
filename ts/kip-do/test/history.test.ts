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
      nexus.execute('ARCHIVE "C-1"')

      // A pattern matches active elements unless it says otherwise, and what
      // was active is a question about the coordinate: the index that says
      // "archived" describes today.
      expect(nexus.query('FIND(?c) WHERE { ?c CONCEPT {} }')).toEqual([])
      expect(nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF SEQ 1')).toEqual([
        'Alice',
      ])
    })
  })

  it('names a coordinate by transaction as well as by sequence', async () => {
    await withNexus('by-tx', (nexus) => {
      const receipt = nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')

      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF TX :tx', {
          tx: receipt.tx_id,
        }),
      ).toEqual(['Alice'])
      // An unknown transaction names no coordinate — refusing beats answering
      // about the present under a name that meant something else.
      expect(() =>
        nexus.query('FIND(?c) WHERE { ?c CONCEPT {} } AS OF TX :tx', { tx: 'tx-nope' }),
      ).toThrowError(/no transaction/)
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
      nexus.execute('RETRACT ASSERTION "A-1"')

      const STATUS =
        'FIND(?b.status) WHERE { ?p PROPOSITION (?s, "prefers", ?o) ?b BELIEF (?p) }'
      // Retracted today: nobody is committed to it any more.
      expect(nexus.query(STATUS)).toEqual(['insufficient'])
      // At the coordinate before the retraction, the commitment stood. A
      // projection that read today's Assertions under a past coordinate would
      // answer neither question.
      expect(nexus.query(`${STATUS} AS OF SEQ 1`)).toEqual(['accepted'])
    })
  })

  it('resolves symbols through the Schema that was in force then', async () => {
    await withNexus('schema', (nexus) => {
      const before = nexus.environment().version
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      // Activating a Schema mints a new environment version; the coordinate
      // before it was written under the old one (§144).
      nexus.activatePackages([COGNITIVE_MEMORY])
      const env = nexus.describe('DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ 1') as {
        version: number
      }
      expect(env.version).toBe(before)
    })
  })
})

describe('SNAPSHOT', () => {
  it('issues a token that binds a later read to its coordinate', async () => {
    await withNexus('token', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      const snapshot = nexus.describe('SNAPSHOT') as {
        snapshot_seq: number
        snapshot_token: string
      }
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')

      expect(snapshot.snapshot_seq).toBe(1)
      expect(
        nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} }', {}, {
          snapshot_token: snapshot.snapshot_token,
        }),
      ).toEqual(['Alice'])
    })
  })

  it('refuses a token issued for another Space', async () => {
    await withNexus('cross-space', (nexus) => {
      const snapshot = nexus.describe('SNAPSHOT') as { snapshot_token: string }
      const elsewhere = CognitiveNexus.connect
      void elsewhere
      // The token carries its Space, because the same sequence means something
      // entirely different in another one.
      const forged = Buffer.from('kip:snapshot:kip:space:other:1').toString('hex')
      expect(() =>
        nexus.query('FIND(?c) WHERE { ?c CONCEPT {} }', {}, { snapshot_token: forged }),
      ).toThrowError(/issued for Space/)
      expect(snapshot.snapshot_token).not.toBe(forged)
    })
  })

  it('refuses a request whose token and command name different coordinates', async () => {
    await withNexus('disagreement', (nexus) => {
      nexus.execute('CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" }')
      const snapshot = nexus.describe('SNAPSHOT') as { snapshot_token: string }
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
