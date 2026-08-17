import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { elementId, formatElementId } from '../src/id.js'
import { nowTime } from '../src/time.js'
import {
  endpointFromJson,
  endpointKey,
  endpointToJson,
  tupleKey,
} from '../src/term.js'
import {
  State,
  Store,
  type ConceptRow,
  type Element,
  type PropositionRow,
} from '../src/store/index.js'

/**
 * Storage tests run against the real platform, not a Node SQLite shim.
 *
 * The engine's contract *is* Durable Object SQLite — `transactionSync`, the
 * 100-parameter ceiling, `AUTOINCREMENT` semantics, partial indexes — so a
 * green run here means green on Cloudflare and a green run anywhere else means
 * very little. Each test takes its own object name because the pool is
 * configured without isolated storage.
 */
async function withStore(
  name: string,
  body: (store: Store) => void,
): Promise<void> {
  const stub = env.KIP_DB.getByName(name)
  await runInDurableObject(stub, (_instance, state) => {
    body(new Store(state.storage.sql))
  })
}

/** The columns a caller has to fill for a bare, active element. */
function envelope(space: string, id: number) {
  const at = nowTime()
  return {
    id,
    space,
    state: State.ACTIVE,
    version: 1,
    seq: 1,
    created_at: at,
    updated_at: at,
    created_tx: 'tx-1',
    updated_tx: 'tx-1',
    origin: {},
    facets: {},
    structural: {},
    governance: {},
    retention: {},
    expires_at: '',
  }
}

function concept(space: string, id: number, extra: Partial<ConceptRow> = {}) {
  const row: ConceptRow = {
    ...envelope(space, id),
    client_key: '',
    schema_ref: 'kip://profiles/cognitive-memory@2.0.0/Person',
    key: '',
    name: '',
    canonical_id: '',
    aliases: [],
    attributes: {},
    merged_into: '',
    ...extra,
  }
  return { kind: 'Concept', row } satisfies Element
}

function proposition(
  space: string,
  id: number,
  subject: unknown,
  predicate: string,
  object: unknown,
) {
  const s = endpointFromJson(subject as never)
  const o = endpointFromJson(object as never)
  const row: PropositionRow = {
    ...envelope(space, id),
    subject: endpointToJson(s),
    subject_key: endpointKey(s),
    predicate_ref: predicate,
    object: endpointToJson(o),
    object_key: endpointKey(o),
    tuple_key: tupleKey(space, s, predicate, o),
    attributes: {},
  }
  return { kind: 'Proposition', row } satisfies Element
}

function newSpace(store: Store, spaceId: string) {
  return store.createSpace({
    space_id: spaceId,
    uri: '',
    name: spaceId,
    description: '',
    owner_principal: 'system',
    owners: ['system'],
    status: 'active',
    default_policy_id: '',
    trust_policy_id: '',
    default_classification: 'internal',
    audit_mode: 'standard',
    created_at: nowTime(),
    seq: 0,
    schema_environment_version: 0,
    policies: {},
  })
}

describe('the store', () => {
  it('applies its schema and is safe to re-open', async () => {
    await withStore('schema', (store) => {
      // Construction applies the DDL; constructing again over the same storage
      // is the only recovery a Durable Object gets, since construction is not
      // a transaction.
      const again = new Store(store.sql)
      expect(again.spaces()).toEqual([])
    })
  })

  it('mints ids that carry their kind and are never reused', async () => {
    await withStore('ids', (store) => {
      newSpace(store, 'space://a')
      const first = store.reserve('Concept', 'space://a')
      const second = store.reserve('Concept', 'space://a')
      expect(formatElementId(first)).toBe('C-1')
      expect(formatElementId(second)).toBe('C-2')

      // A reserved shell is invisible until it is committed…
      expect(store.load(first)).toBeNull()

      // …and after a sweep its id must not come back, or a reference to the
      // swept element would resolve to a brand-new one.
      expect(store.sweepPending()).toBe(2)
      const third = store.reserve('Concept', 'space://a')
      expect(formatElementId(third)).toBe('C-3')
    })
  })

  it('gives each kind its own id sequence', async () => {
    await withStore('kinds', (store) => {
      newSpace(store, 'space://a')
      expect(formatElementId(store.reserve('Concept', 'space://a'))).toBe('C-1')
      expect(formatElementId(store.reserve('Assertion', 'space://a'))).toBe(
        'A-1',
      )
      expect(formatElementId(store.reserve('Evidence', 'space://a'))).toBe('E-1')
      expect(formatElementId(store.reserve('Activity', 'space://a'))).toBe('X-1')
    })
  })

  it('round-trips an element through its columns', async () => {
    await withStore('roundtrip', (store) => {
      newSpace(store, 'space://a')
      const id = store.reserve('Concept', 'space://a')
      const element = concept('space://a', id.seq, {
        key: 'person:alice',
        name: 'Alice',
        aliases: ['Al'],
        attributes: { display_name: 'Alice A' },
        facets: { MnemonicState: { salience: 0.5 } },
      })
      store.put(element, 'create', 'tx-1')

      const loaded = store.load(id)
      expect(loaded?.kind).toBe('Concept')
      expect(loaded?.row).toEqual(element.row)
    })
  })

  it('keeps one canonical Proposition per semantic tuple', async () => {
    await withStore('tuple', (store) => {
      newSpace(store, 'space://a')
      const alice = store.reserve('Concept', 'space://a')
      store.put(concept('space://a', alice.seq, { name: 'Alice' }), 'create', 't')

      const first = store.reserve('Proposition', 'space://a')
      const p = proposition('space://a', first.seq, { id: 'C-1' }, 'prefers', 'dark')
      store.put(p, 'create', 't')

      expect(store.propositionByTuple(p.row.tuple_key)?.id).toBe(first.seq)

      // A second row for the same tuple is what `ENSURE PROPOSITION` resolves
      // against instead of racing two writers into a duplicate — the index is
      // that rule, so the write has to fail rather than duplicate.
      const second = store.reserve('Proposition', 'space://a')
      expect(() =>
        store.put(
          proposition('space://a', second.seq, { id: 'C-1' }, 'prefers', 'dark'),
          'create',
          't',
        ),
      ).toThrowError(/UNIQUE/)
    })
  })

  it('scopes a logical key to its Space', async () => {
    await withStore('keys', (store) => {
      newSpace(store, 'space://a')
      newSpace(store, 'space://b')
      const a = store.reserve('Concept', 'space://a')
      const b = store.reserve('Concept', 'space://b')
      store.put(concept('space://a', a.seq, { key: 'person:alice' }), 'create', 't')
      store.put(concept('space://b', b.seq, { key: 'person:alice' }), 'create', 't')

      expect(store.conceptByKey('space://a', 'person:alice')?.id).toBe(a.seq)
      expect(store.conceptByKey('space://b', 'person:alice')?.id).toBe(b.seq)

      // Within one Space it is an identity, so a second claim on it fails.
      const clash = store.reserve('Concept', 'space://a')
      expect(() =>
        store.put(
          concept('space://a', clash.seq, { key: 'person:alice' }),
          'create',
          't',
        ),
      ).toThrowError(/UNIQUE/)
    })
  })

  it('lets many Concepts share a name, because a name is not identity', async () => {
    await withStore('names', (store) => {
      newSpace(store, 'space://a')
      for (let i = 0; i < 3; i++) {
        const id = store.reserve('Concept', 'space://a')
        store.put(concept('space://a', id.seq, { name: 'Alice' }), 'create', 't')
      }
      expect(
        store.sql
          .exec<{ n: number }>(
            'SELECT COUNT(*) AS n FROM concepts WHERE name = ?',
            'Alice',
          )
          .toArray()[0]?.n,
      ).toBe(3)
    })
  })

  it('advances one sequence per Space, not per element', async () => {
    await withStore('seq', (store) => {
      newSpace(store, 'space://a')
      newSpace(store, 'space://b')
      expect(store.nextSeq('space://a')).toBe(1)
      expect(store.nextSeq('space://a')).toBe(2)
      expect(store.nextSeq('space://b')).toBe(1)
      expect(store.currentSeq('space://a')).toBe(2)
      expect(() => store.nextSeq('space://missing')).toThrowError(
        /no MemorySpace/,
      )
    })
  })

  it('reconstructs an element as it was at a past coordinate', async () => {
    await withStore('history', (store) => {
      newSpace(store, 'space://a')
      const id = store.reserve('Concept', 'space://a')
      const v1 = concept('space://a', id.seq, { name: 'Alice', seq: 1 })
      store.put(v1, 'create', 'tx-1')
      const v2 = {
        kind: 'Concept',
        row: { ...v1.row, name: 'Alicia', version: 2, seq: 5 },
      } satisfies Element
      store.put(v2, 'update', 'tx-2')

      expect(store.load(id)?.row.version).toBe(2)
      // Before the update, the element was at version 1 — and a coordinate
      // before it existed at all is a different answer from an empty element.
      expect(store.versionAt('space://a', id, 4)?.version).toBe(1)
      expect(store.versionAt('space://a', id, 5)?.version).toBe(2)
      expect(store.versionAt('space://a', id, 0)).toBeNull()
      expect(
        (store.versionAt('space://a', id, 4)?.row as { name: string }).name,
      ).toBe('Alice')
    })
  })

  it('destroys the version log a purge has to erase', async () => {
    await withStore('purge', (store) => {
      newSpace(store, 'space://a')
      const id = store.reserve('Concept', 'space://a')
      store.put(concept('space://a', id.seq, { name: 'Alice' }), 'create', 't')
      expect(store.purgeVersions('space://a', id)).toBe(1)
      // An element scrubbed only in its current row stays fully readable
      // through AS OF, which is why the log goes first.
      expect(store.versionAt('space://a', id, 99)).toBeNull()
    })
  })

  it('answers what points at an element without scanning the Space', async () => {
    await withStore('referrers', (store) => {
      newSpace(store, 'space://a')
      const alice = store.reserve('Concept', 'space://a')
      const dark = store.reserve('Concept', 'space://a')
      store.put(concept('space://a', alice.seq, { name: 'Alice' }), 'create', 't')
      store.put(
        concept('space://a', dark.seq, {
          name: 'Dark',
          // A Profile structural field: the reference path that has no column
          // of its own, and that an incomplete walker would miss.
          structural: { related_to: [{ id: formatElementId(alice) }] },
        }),
        'create',
        't',
      )
      const p = store.reserve('Proposition', 'space://a')
      store.put(
        proposition(
          'space://a',
          p.seq,
          { id: formatElementId(alice) },
          'prefers',
          { id: formatElementId(dark) },
        ),
        'create',
        't',
      )

      expect(store.referrers('space://a', alice)).toEqual([
        { from: elementId('Concept', dark.seq), field: 'structural:related_to' },
        { from: elementId('Proposition', p.seq), field: 'subject' },
      ])
      expect(store.referrers('space://a', dark)).toEqual([
        { from: elementId('Proposition', p.seq), field: 'object' },
      ])
    })
  })

  it('re-indexes references when an element stops carrying one', async () => {
    await withStore('reindex', (store) => {
      newSpace(store, 'space://a')
      const alice = store.reserve('Concept', 'space://a')
      const other = store.reserve('Concept', 'space://a')
      store.put(concept('space://a', alice.seq), 'create', 't')
      const before = concept('space://a', other.seq, {
        structural: { related_to: [{ id: formatElementId(alice) }] },
      })
      store.put(before, 'create', 't')
      expect(store.referrers('space://a', alice)).toHaveLength(1)

      // A stale reverse index would let a purge conclude nothing points at an
      // element that something does — or the reverse.
      store.put(
        { kind: 'Concept', row: { ...before.row, structural: {} } },
        'update',
        't',
      )
      expect(store.referrers('space://a', alice)).toEqual([])
    })
  })

  it('replays a lost response from the idempotency key, not the mutation', async () => {
    await withStore('journal', (store) => {
      newSpace(store, 'space://a')
      store.putTransaction({
        tx_id: 'tx-1',
        space: 'space://a',
        seq: 1,
        snapshot_seq: 0,
        committed_at: nowTime(),
        status: 'committed',
        transaction_class: 'cognitive',
        idempotency_key: 'client-42',
        request_digest: '',
        semantic_plan_digest: '',
        result_digest: '',
        schema_environment_version: 1,
        result: { handles: { c: 'C-1' } },
        changes: [{ id: 'C-1', kind: 'Concept', op: 'create', version: 1 }],
      })

      expect(store.transactionByKey('space://a', 'client-42')?.tx_id).toBe('tx-1')
      expect(store.transaction('tx-1')?.result).toEqual({
        handles: { c: 'C-1' },
      })
      // Scoped per Space, so two Spaces may reuse a key without colliding.
      expect(store.transactionByKey('space://b', 'client-42')).toBeNull()
      // An absent key is not a key everything matches.
      expect(store.transactionByKey('space://a', '')).toBeNull()
    })
  })

  it('finds an element a client key already created', async () => {
    await withStore('clientkey', (store) => {
      newSpace(store, 'space://a')
      const id = store.reserve('Concept', 'space://a')
      store.put(
        concept('space://a', id.seq, { client_key: 'kip:import:abc' }),
        'create',
        't',
      )
      expect(
        store.byClientKey('Concept', 'space://a', 'kip:import:abc')?.row.id,
      ).toBe(id.seq)
      // Several elements may carry no client key at all, so '' must not match.
      expect(store.byClientKey('Concept', 'space://a', '')).toBeNull()
    })
  })

  it('appends Schema Environment versions instead of editing them', async () => {
    await withStore('schemaenv', (store) => {
      newSpace(store, 'space://a')
      const at = nowTime()
      store.appendSchemaEnv({
        space: 'space://a',
        version: 1,
        lock: { packages: {} },
        created_at: at,
        tx_id: 'tx-1',
      })
      store.appendSchemaEnv({
        space: 'space://a',
        version: 2,
        lock: { packages: { 'kip://core': '2.0.0' } },
        created_at: at,
        tx_id: 'tx-2',
      })

      // A transaction records which environment version it ran under, so an
      // earlier version has to stay readable exactly as it was.
      expect(store.schemaEnv('space://a')?.version).toBe(2)
      expect(store.schemaEnv('space://a', 1)?.lock).toEqual({ packages: {} })
      expect(() =>
        store.appendSchemaEnv({
          space: 'space://a',
          version: 2,
          lock: {},
          created_at: at,
          tx_id: 'tx-3',
        }),
      ).toThrowError(/UNIQUE/)
    })
  })

  it('rejects a package reference arriving twice with different content', async () => {
    await withStore('packages', (store) => {
      const install = (digest: string) =>
        store.installPackage({
          package_ref: 'kip://core@2.0.0',
          package_id: 'kip://core',
          version: '2.0.0',
          content_digest: digest,
          declared_digest: '',
          artifact: { format: 'KIP-Schema-Package' },
          installed_at: nowTime(),
          source: 'test',
        })
      install('aaaa')
      // package_id + version identifies one canonical content forever; the
      // same reference with different content is an integrity error, not an
      // update (§240.4).
      expect(() => install('bbbb')).toThrowError(/UNIQUE/)
      expect(store.packageByRef('kip://core@2.0.0')?.content_digest).toBe('aaaa')
    })
  })

  it('loads a set of ids without outgrowing the parameter ceiling', async () => {
    await withStore('idset', (store) => {
      newSpace(store, 'space://a')
      const seqs: number[] = []
      // Past 100, which is where `IN (?, ?, …)` stops working on this platform.
      for (let i = 0; i < 150; i++) {
        const id = store.reserve('Concept', 'space://a')
        store.put(concept('space://a', id.seq, { name: `n${i}` }), 'create', 't')
        seqs.push(id.seq)
      }
      expect(store.loadMany('Concept', seqs)).toHaveLength(150)
      expect(store.loadMany('Concept', [])).toEqual([])
    })
  })
})
