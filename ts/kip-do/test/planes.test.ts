import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { parseElementId } from '../src/id.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import type { ChangeEntry } from '../src/store/index.js'

/**
 * Version planes (Spec §6.3, §35.1, §35.2) and the Change Envelope entries
 * that report them (§36.1).
 *
 * `_system.version` advances on every committed change; each plane advances
 * only when its content changes. That is what lets a guard `OF ATTRIBUTES`
 * survive a concurrent Facet sweep on the same element — the property a
 * verdict and a decay pass both depend on (KIP2-TX-027/028).
 */
async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus) => void,
): Promise<void> {
  const stub = env.KIP_DB.getByName(`planes-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    body(nexus)
  })
}

const PLANES = `FIND(?c._system.version, ?c._system.plane_versions) WHERE { ?c CONCEPT {id: "C-1"} }`

function refused(body: () => unknown): { code: string; details?: unknown } {
  try {
    body()
  } catch (err) {
    return err as { code: string; details?: unknown }
  }
  throw new Error('expected a refusal')
}

describe('version planes', () => {
  it('advances each plane only when its content changes', async () => {
    await withNexus('counters', (nexus) => {
      nexus.execute(`CREATE CONCEPT ?c {
        TYPE "Person" NAME "Alice"
        SET ATTRIBUTES { note: "one" }
        SET FACET "MnemonicState" { memory_strength: 0.5 }
      }`)
      // A create writes every populated plane once.
      expect(nexus.query(PLANES)).toEqual([
        [1, { attributes: 1, structural: 0, retention: 0, facets: { MnemonicState: 1 } }],
      ])

      // A Facet sweep moves the Facet's counter and the version, nothing else.
      nexus.execute('UPDATE "C-1" SET FACET "MnemonicState" { memory_strength: 0.25 }')
      expect(nexus.query(PLANES)).toEqual([
        [2, { attributes: 1, structural: 0, retention: 0, facets: { MnemonicState: 2 } }],
      ])

      // An attribute write moves the attributes plane.
      nexus.execute('UPDATE "C-1" SET ATTRIBUTES { note: "two" }')
      expect(nexus.query(PLANES)).toEqual([
        [3, { attributes: 2, structural: 0, retention: 0, facets: { MnemonicState: 2 } }],
      ])

      // A retention change moves the retention plane.
      nexus.execute('SET RETENTION "C-1" { retention_class: "short" }')
      expect(nexus.query(PLANES)).toEqual([
        [4, { attributes: 2, structural: 0, retention: 1, facets: { MnemonicState: 2 } }],
      ])

      // A lifecycle move advances the version and no plane.
      nexus.execute('TRANSITION "C-1" TO "archived"')
      expect(
        nexus.query(
          'FIND(?c._system.version, ?c._system.plane_versions.attributes) WHERE { ?c CONCEPT {id: "C-1", state: "archived"} }',
        ),
      ).toEqual([[5, 2]])

      // The counters are readable one at a time, the Facet by its local name.
      expect(
        nexus.query(
          'FIND(?c._system.plane_versions.facets["MnemonicState"]) WHERE { ?c CONCEPT {id: "C-1", state: "archived"} }',
        ),
      ).toEqual([2])
    })
  })

  it('guards one plane without being spoiled by a write to another', async () => {
    await withNexus('guards', (nexus) => {
      nexus.execute(`CREATE CONCEPT ?c {
        TYPE "Person" NAME "Alice"
        SET ATTRIBUTES { note: "one" }
        SET FACET "MnemonicState" { memory_strength: 0.5 }
      }`)
      // The decay sweep runs, guarded on its own plane…
      nexus.execute(
        'UPDATE "C-1" SET FACET "MnemonicState" { memory_strength: 0.25 } EXPECT VERSION 1 OF FACET "MnemonicState"',
      )
      // …and does not invalidate a verdict guarded on the attributes plane,
      // although a bare guard on the version it read is now stale.
      expect(
        nexus.execute(
          'UPDATE "C-1" SET ATTRIBUTES { note: "two" } EXPECT VERSION 1 OF ATTRIBUTES',
        ).status,
      ).toBe('committed')
      const bare = refused(() =>
        nexus.execute('UPDATE "C-1" SET ATTRIBUTES { note: "three" } EXPECT VERSION 1'),
      )
      expect(bare.code).toBe('VersionConflict')
      expect(bare.details).toBeUndefined()

      // A stale plane guard names the plane (§35.1).
      const stale = refused(() =>
        nexus.execute(
          'UPDATE "C-1" SET FACET "MnemonicState" { salience: 0.1 } EXPECT VERSION 1 OF FACET "MnemonicState"',
        ),
      )
      expect(stale.code).toBe('VersionConflict')
      expect(stale.details).toEqual({ plane: 'facets.MnemonicState' })

      // Several guards on one statement are all checked, in order.
      const second = refused(() =>
        nexus.execute(
          'UPDATE "C-1" SET ATTRIBUTES { note: "three" } EXPECT VERSION 2 OF ATTRIBUTES EXPECT VERSION 1 OF STRUCTURAL',
        ),
      )
      expect(second.details).toEqual({ plane: 'structural' })
      expect(
        nexus.execute(
          'UPDATE "C-1" SET ATTRIBUTES { note: "three" } EXPECT VERSION 2 OF ATTRIBUTES EXPECT VERSION 0 OF STRUCTURAL EXPECT VERSION 0 OF RETENTION',
        ).status,
      ).toBe('committed')
    })
  })

  it('reads EXPECT VERSION 0 OF a plane as never written, not as create-only', async () => {
    await withNexus('zero', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" SET FIELDS {key: "person:alice"} }')
      // The structural plane has never been written, so 0 is a true statement
      // about an element that very much exists (§35.2).
      expect(
        nexus.execute(
          'UPSERT CONCEPT ?c { MATCH {key: "person:alice"} SET FIELDS {name: "Alicia"} } EXPECT VERSION 0 OF STRUCTURAL',
        ).status,
      ).toBe('committed')
      // The bare form stays create-only: the element exists, so it fails.
      const bare = refused(() =>
        nexus.execute(
          'UPSERT CONCEPT ?c { MATCH {key: "person:alice"} SET FIELDS {name: "Alice"} } EXPECT VERSION 0',
        ),
      )
      expect(bare.code).toBe('VersionConflict')
      // And on the create half, a plane guard other than 0 cannot be met.
      const create = refused(() =>
        nexus.execute(
          'UPSERT CONCEPT ?c { MATCH {type: "Person", key: "person:bob"} } EXPECT VERSION 1 OF ATTRIBUTES',
        ),
      )
      expect(create.details).toEqual({ plane: 'attributes' })
      expect(
        nexus.execute(
          'UPSERT CONCEPT ?c { MATCH {type: "Person", key: "person:bob"} } EXPECT VERSION 0 EXPECT VERSION 0 OF ATTRIBUTES',
        ).status,
      ).toBe('committed')
    })
  })

  it('refuses a plane named twice through a parameter, as the parser does for a literal', async () => {
    await withNexus('duplicate-plane', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      const twice = refused(() =>
        nexus.execute(
          'UPDATE "C-1" SET FIELDS {name: "B"} EXPECT VERSION 1 OF FACET :a EXPECT VERSION 1 OF FACET :b',
          { a: 'MnemonicState', b: 'kip://profiles/cognitive-memory@2.1.0/MnemonicState' },
        ),
      )
      expect(twice.code).toBe('InvalidSyntax')
    })
  })

  it('reports touched paths and plane counters on Change Envelope entries', async () => {
    await withNexus('envelope', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" SET ATTRIBUTES { note: "one" } }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
      }`)
      const created = nexus.describe('CHANGES AFTER SEQ 0') as unknown as {
        changes: ChangeEntry[]
      }[]
      // A create carries no `touched`, no `old_version` and no `planes`; a
      // Concept names its type, a Proposition its subject and predicate.
      expect(created[0]?.changes).toEqual([
        {
          op: 'create',
          kind: 'concept',
          id: 'C-1',
          schema_ref: 'kip://profiles/cognitive-memory@2.1.0/Person',
          new_version: 1,
        },
        {
          op: 'create',
          kind: 'concept',
          id: 'C-2',
          schema_ref: 'kip://profiles/cognitive-memory@2.1.0/Preference',
          new_version: 1,
        },
        {
          op: 'create',
          kind: 'proposition',
          id: 'P-1',
          new_version: 1,
          refs: {
            subject: 'C-1',
            predicate_ref: 'kip://profiles/cognitive-memory@2.1.0/prefers',
          },
        },
      ])

      const updated = nexus.execute(`UPDATE "C-2"
        SET ATTRIBUTES { note: "two", extra: 1 }
        SET FACET "MnemonicState" { salience: 0.5 }
        SET STRUCTURAL { ("about", "C-1") }`)
      expect(updated.changes).toEqual([
        {
          op: 'update',
          kind: 'concept',
          id: 'C-2',
          schema_ref: 'kip://profiles/cognitive-memory@2.1.0/Preference',
          old_version: 1,
          new_version: 2,
          // Names only, never values (§36.1), and sorted: the same commit
          // reports the same list whichever order its clauses ran in, so two
          // engines can be compared entry for entry.
          touched: [
            'attributes.extra',
            'attributes.note',
            'facets.MnemonicState',
            'structural.about',
          ],
          planes: {
            attributes: 2,
            structural: 1,
            retention: 0,
            facets: { MnemonicState: 1 },
          },
        },
      ])

      const retention = nexus.execute('SET RETENTION "C-1" { retention_class: "short" }')
      expect(retention.changes[0]).toEqual(
        expect.objectContaining({
          op: 'retention',
          touched: ['retention'],
          planes: expect.objectContaining({ retention: 1 }),
        }),
      )

      // A merge moves `merged_into` and the engine state, and neither belongs
      // to a plane (§6.3) — so the entry names both paths and reports no
      // counters at all.
      nexus.execute('CREATE CONCEPT ?canonical { TYPE "Preference" }')
      const merged = nexus.execute('MERGE CONCEPT "C-2" INTO "C-3"')
      expect(merged.changes).toEqual([
        expect.objectContaining({
          op: 'merge',
          id: 'C-2',
          old_version: 2,
          new_version: 3,
          refs: { merged_into: 'C-3' },
          touched: ['fields.merged_into', 'state'],
        }),
      ])
      expect(merged.changes[0]).not.toHaveProperty('planes')
    })
  })

  it('carries the counters onto a purge stub, advancing the plane it erased', async () => {
    await withNexus('stub', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')
      nexus.execute('PURGE "C-1" CONFIRM "PURGE"')
      const stub = nexus.store.load(parseElementId('C-1'))
      // The stub keeps the counters rather than restarting them, and the
      // erasure counts as a write to the plane it emptied: a guard taken
      // before the purge must not go on passing after it (§6.3, §60.3).
      expect(stub?.row.version).toBe(3)
      expect(stub?.row.plane_versions).toEqual({
        attributes: 3,
        structural: 0,
        retention: 0,
        facets: {},
      })
    })
  })
})
