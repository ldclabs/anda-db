import {
  pin,
  expiry,
  version,
  fixture,
  wakeOf,
  evaluation,
  principal,
} from './support/attention.js'
import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it, vi } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { digest } from '../src/schema/contracts.js'
import { parseElementId } from '../src/id.js'
import type { JsonMap } from '../src/json.js'
import type {
  AttentionConfig,
  PreparedWatchPage,
} from '../src/attention/types.js'
import type { ControlRecord } from '../src/control.js'

describe('durable Watch handoff parity', () => {
  it('commits fire, Activity, wake and replay together and recovers across reconnect', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-atomic'),
      (_, state) => {
        const f = fixture(state.storage),
          result = f.advance(),
          ref = String(result.wake_ref)
        expect(result.fire_key).toBe(`watch_fire:${f.watch}:1:${f.matchedSeq}`)
        expect(
          f.n.query(
            'FIND(?a.id) WHERE {?a ACTIVITY {activity_class:"watch_fire"}}',
          ),
        ).toEqual([result.fire_activity_ref])
        const wake = f.s.readWake(ref)
        expect(wake).toMatchObject({
          version: 1,
          fence: 0,
          state: { stage: 'pending' },
          fire: {
            watch_ref: f.watch,
            arm_generation: 1,
            trigger: { kind: 'delta', matched_seq: f.matchedSeq },
          },
        })
        const wakeRow = f.n.store.controlAt(f.n.space, ref)!,
          activity = f.n.store.load(
            parseElementId(String(result.fire_activity_ref)),
          )!
        expect(wakeRow.seq).toBe(activity.row.seq)
        expect(wakeRow.seq).toBe(
          f.n.store.load(parseElementId(f.watch))!.row.seq,
        )
        const reopened = CognitiveNexus.connect(state.storage),
          head = reopened.store.currentSeq(reopened.space)
        expect(reopened.systemSession().advanceWatch(f.watch, 2, 1)).toEqual(
          result,
        )
        expect(reopened.store.currentSeq(reopened.space)).toBe(head)
        expect(reopened.systemSession().armWatch(f.watch, 1)).toEqual(f.armed)
        expect(reopened.systemSession().readWake(ref)).toEqual(wake)
      },
    )
  })
  it('rolls back partial native writes and retries without duplicates', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-fault'),
      (_, state) => {
        const f = fixture(state.storage),
          head = f.n.store.currentSeq(f.n.space),
          original = f.n.store.putControl.bind(f.n.store)
        const fault = vi
          .spyOn(f.n.store, 'putControl')
          .mockImplementation((row) => {
            const result = original(row)
            if (row.kind === 'wake') throw new Error('injected write failure')
            return result
          })
        expect(() => f.advance()).toThrow('injected')
        fault.mockRestore()
        expect(f.n.store.currentSeq(f.n.space)).toBe(head)
        expect(version(f.n, f.watch)).toBe(2)
        expect(
          f.n.query(
            'FIND(?a.id) WHERE {?a ACTIVITY {activity_class:"watch_fire"}}',
          ),
        ).toEqual([])
        expect(f.advance().status).toBe('fired')
      },
    )
  })
  it('atomically finishes outputs and continuations, retaining one replay', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-finish'),
      (_, state) => {
        const f = fixture(state.storage),
          ref = String(f.advance().wake_ref),
          until = expiry()
        const claimed = f.s.claimWake(ref, 1, 0, until)
        expect(f.s.claimWake(ref, 1, 0, until)).toEqual(claimed)
        const command = 'CREATE CONCEPT ?output {TYPE "Person" NAME "output"}',
          children = [{ key: 'next', not_before_ms: 0 }]
        expect(() =>
          f.s.finishWake(ref, 2, 1, command, {}, [children[0]!, children[0]!]),
        ).toThrow('duplicate')
        expect(f.s.readWake(ref).state.stage).toBe('running')
        expect(
          f.n.query('FIND(?c.id) WHERE {?c CONCEPT {name:"output"}}'),
        ).toEqual([])
        const finished = f.s.finishWake(ref, 2, 1, command, {}, children)
        expect(wakeOf(finished).state.stage).toBe('completed')
        expect(finished.outputs).toHaveLength(2)
        const child = (finished.outputs as string[]).find((r) =>
          r.startsWith('wake/'),
        )!
        expect(f.s.readWake(child)).toMatchObject({
          format: 'anda-brain:attention-continuation-v1',
          parent_ref: ref,
          continuation_key: 'next',
          version: 1,
          fence: 0,
        })
        const head = f.n.store.currentSeq(f.n.space)
        expect(f.s.finishWake(ref, 2, 1, command, {}, children)).toEqual(
          finished,
        )
        expect(f.n.store.currentSeq(f.n.space)).toBe(head)
        expect(() => f.s.finishWake(ref, 2, 1, '', {}, children)).toThrow(
          'idempotency_conflict',
        )
        expect(f.n.store.controlAt(f.n.space, ref)!.seq).toBe(
          f.n.store.controlAt(f.n.space, child)!.seq,
        )
        expect(f.s.claimWake(child, 1, 0, expiry())).toHaveProperty('wake')
      },
    )
  })
  it('bounds real-time leases, increments takeover fences and rejects stale completion', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-lease'),
      (_, state) => {
        const f = fixture(state.storage),
          ref = String(f.advance().wake_ref),
          start = Date.now()
        expect(() =>
          f.s.claimWake(ref, 1, 0, new Date(start + 360000).toISOString()),
        ).toThrow('five real-time')
        f.s.claimWake(ref, 1, 0, new Date(start + 1000).toISOString())
        expect(() =>
          f.s.renewWake(ref, 2, 1, new Date(start + 500).toISOString()),
        ).toThrow('shorten')
        const clock = vi.spyOn(Date, 'now').mockReturnValue(start + 2000)
        try {
          expect(() => f.s.finishWake(ref, 2, 1)).toThrow('lease_lost')
          const taken = f.s.claimWake(
            ref,
            2,
            1,
            new Date(start + 10000).toISOString(),
          )
          expect(wakeOf(taken).fence).toBe(2)
          expect(() => f.s.finishWake(ref, 3, 1)).toThrow('lease_lost')
        } finally {
          clock.mockRestore()
        }
      },
    )
  })
  it('checks lease expiry again after planning, with no outputs left behind', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-final-fence'),
      (_, state) => {
        const f = fixture(state.storage),
          ref = String(f.advance().wake_ref),
          start = Date.now()
        f.s.claimWake(ref, 1, 0, new Date(start + 1000).toISOString())
        const clock = vi
          .spyOn(Date, 'now')
          .mockReturnValueOnce(start)
          .mockReturnValue(start + 2000)
        try {
          expect(() =>
            f.s.finishWake(
              ref,
              2,
              1,
              'CREATE CONCEPT ?o {TYPE "Person" NAME "late"}',
            ),
          ).toThrow('lease_lost')
        } finally {
          clock.mockRestore()
        }
        expect(f.s.readWake(ref).version).toBe(2)
        expect(
          f.n.query('FIND(?c.id) WHERE {?c CONCEPT {name:"late"}}'),
        ).toEqual([])
      },
    )
  })
  it('blocks completion after rearming or changing trust and still permits cancellation', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-basis'),
      (_, state) => {
        const f = fixture(state.storage),
          ref = String(f.advance().wake_ref)
        f.s.claimWake(ref, 1, 0, expiry())
        f.s.setTrust(1, {}, 0.8)
        expect(() => f.s.finishWake(ref, 2, 1)).toThrow('basis_changed')
        expect(wakeOf(f.s.cancelWake(ref, 2, 1, 'replan')).state.stage).toBe(
          'cancelled',
        )
        f.s.armWatch(f.watch, version(f.n, f.watch))
        f.n.execute('UPDATE :target SET FIELDS {name:"third"}', {
          target: f.target,
        })
        const next = f.s.advanceWatch(f.watch, version(f.n, f.watch), 2),
          r = String(next.wake_ref)
        f.s.claimWake(r, 1, 0, expiry())
        f.s.armWatch(f.watch, version(f.n, f.watch))
        expect(() => f.s.finishWake(r, 2, 1)).toThrow('generation_conflict')
      },
    )
  })
  it('protects fire identities, Watch deadlines and every generic control-read path', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-access'),
      (_, state) => {
        const f = fixture(state.storage)
        expect(() =>
          f.n.execute(
            'CREATE ACTIVITY ?fake {CLIENT KEY :key SET FIELDS {activity_class:"watch_fire",status:"completed"}}',
            { key: `watch_fire:${f.watch}:1:${f.matchedSeq}` },
          ),
        ).toThrow('reserved')
        expect(() =>
          f.n.execute(
            'UPDATE :watch SET ATTRIBUTES {due_at:"2030-01-01T00:00:00.000Z"} EXPECT VERSION 2',
            { watch: f.watch },
          ),
        ).toThrow('protected new generation')
        const result = f.advance(),
          ref = String(result.wake_ref)
        const auditor = principal(f.n, 'kip:principal:auditor', [
          'read',
          'read_governance_history',
        ])
        expect(() => auditor.readWake(ref)).toThrow()
        expect(() => auditor.readControl(ref)).toThrow()
        expect(() =>
          auditor.readControl(`attention/watch/${f.watch}/1`),
        ).toThrow('dedicated')
        const masked = principal(
          f.n,
          'kip:principal:masked',
          ['read', 'read_governance_history', 'maintain', 'update'],
          ['_system'],
        )
        expect(() => masked.readControl(ref)).toThrow('fully visible')
        expect(() => masked.armWatch(f.watch, version(f.n, f.watch))).toThrow(
          'fully visible',
        )
      },
    )
  })
  it('requires read_history even when advancement replays a prior receipt', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-history-permission'),
      (_, state) => {
        const f = fixture(state.storage)
        const reader = principal(f.n, 'kip:principal:no-history', [
          'read',
          'update',
          'create',
          'derive',
        ])
        reader.armWatch(f.watch, 2)
        f.n.execute('UPDATE :t SET FIELDS {name:"next"}', { t: f.target })
        expect(() => reader.advanceWatch(f.watch, 3, 2)).toThrow()
      },
    )
  })
  it('discovers bounded snapshot pages, including empty pages, and invalidates changed bases', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-catalog'),
      (_, state) => {
        const f = fixture(state.storage),
          ref = String(f.advance().wake_ref)
        f.s.claimWake(ref, 1, 0, expiry())
        const first = f.s.listWakes(null, 1)
        expect(first).toMatchObject({ items: [], scanned: 1, complete: false })
        f.s.renewWake(ref, 2, 1, expiry())
        const second = f.s.listWakes(first.next_cursor, 1)
        expect(second.items[0]?.version).toBe(2)
        expect(second.complete).toBe(true)
        f.s.setTrust(1, {}, 0.9)
        expect(() => f.s.listWakes(first.next_cursor, 1)).toThrow(
          'wake_cursor_basis_changed',
        )
        expect(() => f.s.listWakes('zz', 1)).toThrow('invalid wake cursor')
      },
    )
  })
  it('requires executable resume verification and rechecks cancellation after async observation', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-resume'),
      async (_, state) => {
        const f = fixture(state.storage),
          ref = String(f.advance().wake_ref),
          condition = { ready: 'binding' }
        f.s.claimWake(ref, 1, 0, expiry())
        f.s.blockWake(ref, 2, 1, {
          reason: 'binding_unavailable',
          resume: { kind: 'on_change', condition_digest: digest(condition) },
        })
        await expect(f.s.resumeWake(ref, 3, 1)).rejects.toThrow(
          'registered verifier',
        )
        let ready = false
        f.n.registerWakeResumeVerifier(condition, pin('verifier'), () => ready)
        await expect(f.s.resumeWake(ref, 3, 1)).rejects.toThrow('not_ready')
        ready = true
        const resumed = await f.s.resumeWake(ref, 3, 1)
        expect(wakeOf(resumed).state.stage).toBe('pending')
        expect(await f.s.resumeWake(ref, 3, 1)).toEqual(resumed)
        f.s.claimWake(ref, 4, 1, expiry())
        const other = { ready: 'external' }
        f.s.blockWake(ref, 5, 2, {
          reason: 'binding_unavailable',
          resume: { kind: 'on_change', condition_digest: digest(other) },
        })
        let release!: (v: boolean) => void
        f.n.registerWakeResumeVerifier(
          other,
          pin('async'),
          () =>
            new Promise<boolean>((resolve) => {
              release = resolve
            }),
        )
        const pending = f.s.resumeWake(ref, 6, 2)
        f.s.cancelWake(ref, 6, 2, 'stop')
        release(true)
        await expect(pending).rejects.toThrow('version_conflict')
        expect(f.s.readWake(ref).state.stage).toBe('cancelled')
      },
    )
  })
  it('requires semantic pins and honors mixed selectors plus synchronous evaluator failure', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-mixed'),
      (_, state) => {
        const f = fixture(state.storage, true),
          before = f.n.store.currentSeq(f.n.space)
        expect(() => f.s.advanceWatch(f.watch, 2, 1)).toThrow(
          'registered host evaluator',
        )
        expect(() =>
          f.s.advanceWatch(f.watch, 2, 1, 100, f.n.space, () => {
            throw new Error('offline')
          }),
        ).toThrow('offline')
        expect(() =>
          f.s.advanceWatch(f.watch, 2, 1, 100, f.n.space, () => {
            f.n.execute(
              'CREATE CONCEPT ?c {TYPE "Person" NAME "callback write"}',
            )
            return true
          }),
        ).toThrow('evaluator_changed_snapshot')
        expect(f.n.store.currentSeq(f.n.space)).toBe(before)
        expect(
          f.s.advanceWatch(f.watch, 2, 1, 100, f.n.space, () => false).status,
        ).toBe('armed')
        f.n.execute('UPDATE :t SET FIELDS {name:"third"}', { t: f.target })
        expect(
          f.s.advanceWatch(f.watch, 3, 1, 100, f.n.space, () => true).status,
        ).toBe('fired')
      },
    )
  })
  it('pins complete semantic candidates and never advances coverage on unknown judgments', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-semantic'),
      (_, state) => {
        const f = fixture(state.storage, true),
          preparedResult = f.s.prepareWatchPage(f.watch, 2, 1, 100, 'page'),
          prepared = preparedResult.prepared as unknown as PreparedWatchPage
        expect(prepared.candidates).toHaveLength(1)
        expect(prepared.candidates[0]?.before).toMatchObject({ name: 'before' })
        expect(prepared.candidates[0]?.after).toMatchObject({ name: 'after' })
        expect(() =>
          f.s.commitWatchPage(prepared.ticket_ref, {
            ...evaluation(prepared, 'missing', 'match'),
            judgments: [],
          }),
        ).toThrow('cover')
        const deferred = f.s.commitWatchPage(
          prepared.ticket_ref,
          evaluation(prepared, 'unknown', 'unknown'),
        )
        expect(deferred.status).toBe('deferred')
        expect(version(f.n, f.watch)).toBe(2)
        expect(
          f.s.commitWatchPage(
            prepared.ticket_ref,
            evaluation(prepared, 'unknown', 'unknown'),
          ),
        ).toEqual(deferred)
        f.n.execute(
          'CREATE CONCEPT ?unrelated {TYPE "Person" NAME "new traffic"}',
        )
        const reopened = CognitiveNexus.connect(state.storage).systemSession()
        expect(reopened.readPreparedWatchPage(prepared.ticket_ref)).toEqual(
          prepared,
        )
        const committed = reopened.commitWatchPage(
          prepared.ticket_ref,
          evaluation(prepared, 'accepted', 'match'),
        )
        expect(committed.status).toBe('fired')
        expect((committed.coverage as JsonMap).through_seq).toBe(
          prepared.through_seq,
        )
        expect(
          reopened.commitWatchPage(
            prepared.ticket_ref,
            evaluation(prepared, 'accepted', 'match'),
          ),
        ).toEqual(committed)
        expect(() =>
          reopened.commitWatchPage(
            prepared.ticket_ref,
            evaluation(prepared, 'accepted', 'no_match'),
          ),
        ).toThrow('idempotency_conflict')
      },
    )
  })
  it('revokes prepared content without leaving material in the replay journal', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-erasure'),
      (_, state) => {
        const f = fixture(state.storage, true),
          result = f.s.prepareWatchPage(f.watch, 2, 1, 100, 'page'),
          page = result.prepared as unknown as PreparedWatchPage
        const rows = f.n.store.all<ControlRecord>(
          'kip_control_records',
          'SELECT * FROM kip_control_records WHERE space = ?',
          f.n.space,
        )
        expect(
          rows.find((r) => r.key === page.ticket_ref)?.value,
        ).not.toHaveProperty('candidates')
        expect(() => f.s.readControl(page.ticket_ref)).toThrow('dedicated')
        f.n.execute('PURGE :target CONFIRM "PURGE"', { target: f.target })
        expect(() => f.s.readPreparedWatchPage(page.ticket_ref)).toThrow()
        expect(() => f.s.prepareWatchPage(f.watch, 2, 1, 100, 'page')).toThrow()
        expect(() =>
          f.s.commitWatchPage(
            page.ticket_ref,
            evaluation(page, 'late', 'match'),
          ),
        ).toThrow()
      },
    )
  })
  it('rejects prepared pages after rearm or evaluator configuration changes', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-semantic-basis'),
      (_, state) => {
        const f = fixture(state.storage, true),
          page = f.s.prepareWatchPage(f.watch, 2, 1, 100, 'p')
            .prepared as unknown as PreparedWatchPage
        const cfg = f.n.store.controlAt(f.n.space, 'attention/config')!,
          config = cfg.value as unknown as AttentionConfig
        config.pins.evaluator = pin('new-evaluator')
        f.s.setAttentionConfig(cfg.version, config)
        expect(() =>
          f.s.commitWatchPage(
            page.ticket_ref,
            evaluation(page, 'late', 'match'),
          ),
        ).toThrow('basis_changed')
        f.s.armWatch(f.watch, 2)
        expect(() =>
          f.s.commitWatchPage(
            page.ticket_ref,
            evaluation(page, 'late', 'match'),
          ),
        ).toThrow('generation_conflict')
      },
    )
  })
  it('rejects missing old arm history and can recover retained legacy armed Watches', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('handoff-legacy'),
      (_, state) => {
        const f = fixture(state.storage)
        state.storage.sql.exec(
          'DELETE FROM kip_control_records WHERE key = ?',
          `attention/watch/${f.watch}/1`,
        )
        expect(f.advance().status).toBe('fired')
        f.s.armWatch(f.watch, 3)
        state.storage.sql.exec(
          'DELETE FROM kip_control_records WHERE key = ?',
          `attention/watch/${f.watch}/2`,
        )
        // Simulates retention deleting the original arm snapshot, not a new interval.
        vi.spyOn(f.n.store, 'elementAt').mockReturnValue(null)
        expect(() => f.s.advanceWatch(f.watch, 4, 2)).toThrow('history_gap')
      },
    )
  })
})
