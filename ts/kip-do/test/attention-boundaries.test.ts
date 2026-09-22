import { requireHostApproval } from './support/approval.js'
import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it, vi } from 'vitest'
import type { JsonMap } from '../src/json.js'
import { principalAuth } from '../src/governance/index.js'
import {
  pin,
  expiry,
  version,
  fixture,
  fresh,
  principal,
  evaluation,
} from './support/attention.js'
import type {
  AttentionConfig,
  PreparedWatchPage,
} from '../src/attention/types.js'

function dispatchFixture(
  storage: DurableObjectStorage,
  lookup = true,
  binding = true,
) {
  const f = fixture(storage, false, binding)
  const observerId = 'kip:principal:lookup-instrument'
  principal(f.n, observerId, [
    'read',
    'record_outcome',
    'read_governance_history',
  ])
  if (lookup)
    f.s.setDispatchLookupObserver(0, {
      binding: pin('executor'),
      principal_id: observerId,
      configuration_digest: pin('observer').digest,
    })
  f.s.armWatch(f.watch, 2)
  f.n.execute('UPDATE :t SET FIELDS {name:"ready"}', { t: f.target })
  const fired = f.s.advanceWatch(f.watch, 3, 2),
    wake = String(fired.wake_ref)
  f.s.claimWake(wake, 1, 0, expiry())
  const prop = f.n.execute(
    'MUTATE {CREATE CONCEPT ?o {TYPE "Preference" NAME "delivery"} ENSURE PROPOSITION ?p (:target,"prefers",?o)}',
    { target: f.target },
  ).handles.p!
  const basis = (
    f.n.query('FIND(?b) WHERE {?p PROPOSITION(id: :id) ?b BELIEF(?p)}', {
      id: prop,
    })[0] as JsonMap
  ).basis as JsonMap
  const selection = f.s.putArtifact({ policy: 'attention-test' }, [])
  const made = f.n.execute(
    `MUTATE {
    CREATE ACTIVITY ?decision {SET FIELDS {activity_class:"action_gate",status:"completed"}
      SET FACET "DecisionRecord" {decision:"act",retrieved_refs:[:watch],used_refs:[:watch],applied_revisions:[],basis: :basis}
      SET FACET "DependencyBasis" {basis_seq: :seq,policy_basis: :basis,groups:[{role:"context",pins:[{id: :watch,version: :version},{id: :prop,version:1}]}]}
      SET STRUCTURAL {("inputs",:watch) ("inputs",:prop)}}
    CREATE ACTIVITY ?attempt {SET FIELDS {activity_class:"action_attempt",status:"completed"}
      SET FACET "AttemptRecord" {attempt_id:"attention-test",decision_ref:?decision,applied_revisions:[],trial_ref:null,context:{task_family:"attention.test"},environment_digest: :environment,tool_versions:{fixture:"v1"},selection_policy: :selection,preconditions_satisfied:"yes",started_at: :started}
      SET STRUCTURAL {("inputs",?decision)}}
  }`,
    {
      watch: f.watch,
      version: version(f.n, f.watch),
      prop,
      basis,
      seq: basis.snapshot_seq!,
      environment: pin('environment').digest,
      selection: { ...selection },
      started: new Date().toISOString(),
    },
  )
  const auth = principalAuth(observerId)
  auth.auth_method = 'authenticated-test-instrument'
  return {
    ...f,
    wake,
    attempt: made.handles.attempt!,
    decision: made.handles.decision!,
    observer: f.n.session(auth),
  }
}

describe('attention deadline and dispatch boundaries', () => {
  it.each([
    ['2000-01-01T00:00:00.999Z', 'expired'],
    ['2000-01-01T00:00:01.000Z', 'expired'],
    ['2000-01-01T00:00:01.001Z', 'fired'],
  ])(
    'silence distinguishes update time %s at the exact deadline',
    async (at, expected) => {
      await runInDurableObject(
        env.KIP_DB.getByName(`silence-${at}`),
        (_, state) => {
          const n = fresh(state.storage),
            s = n.systemSession(),
            target = n.execute(
              'CREATE CONCEPT ?t {TYPE "Person" NAME "before"}',
            ).handles.t!
          const watch = n.execute(
            'CREATE CONCEPT ?w {TYPE "Watch" SET ATTRIBUTES {watch_class:"silence",summary:"wait",condition:{element: :target,ops:["update"]},due_at:"2000-01-01T00:00:01.000Z",status:"disarmed"}}',
            { target },
          ).handles.w!
          s.armWatch(watch, 1)
          expect(() =>
            s.rearmWatch(watch, 2, { element: target }, null),
          ).toThrow('deadline')
          state.storage.sql.exec(
            'UPDATE transactions SET committed_at = ?',
            '2000-01-01T00:00:00.000Z',
          )
          n.execute('UPDATE :target SET FIELDS {name:"reply"}', { target })
          const seq = n.store.currentSeq(n.space)
          state.storage.sql.exec(
            'UPDATE transactions SET committed_at = ? WHERE seq = ?',
            at,
            seq,
          )
          const result = s.advanceWatch(watch, 2, 1)
          expect(result.status).toBe(expected)
          if (expected === 'fired') {
            expect(result.fire_key).toBe(
              `watch_fire:${watch}:1:silence:2000-01-01T00:00:01.000Z`,
            )
            expect(
              s.readWake(String(result.wake_ref)).fire.trigger,
            ).toMatchObject({ due_seq: seq - 1 })
          }
        },
      )
    },
  )
  it('pins silence coverage despite new traffic between bounded pages', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('silence-fixed-page'),
      (_, state) => {
        const n = fresh(state.storage),
          s = n.systemSession(),
          target = n.execute('CREATE CONCEPT ?t {TYPE "Person" NAME "before"}')
            .handles.t!
        const watch = n.execute(
          'CREATE CONCEPT ?w {TYPE "Watch" SET ATTRIBUTES {watch_class:"silence",summary:"wait",condition:{element: :target,ops:["update"]},due_at:"2000-01-01T00:00:01.000Z",status:"disarmed"}}',
          { target },
        ).handles.w!
        s.armWatch(watch, 1)
        for (let i = 0; i < 6; i++)
          n.execute('CREATE CONCEPT ?noise {TYPE "Person" NAME "noise"}')
        const dueSeq = n.store.currentSeq(n.space)
        state.storage.sql.exec(
          'UPDATE transactions SET committed_at = ?',
          '2000-01-01T00:00:00.500Z',
        )
        n.execute('UPDATE :target SET FIELDS {name:"late reply"}', { target })
        let last: JsonMap = {}
        for (let i = 0; i < 10; i++) {
          last = s.advanceWatch(watch, version(n, watch), 1, 1)
          if (last.status === 'fired') break
          expect(last.status).toBe('armed')
          n.execute('CREATE CONCEPT ?noise {TYPE "Person" NAME "later noise"}')
        }
        expect(last.status).toBe('fired')
        expect((last.watch as JsonMap).consumed_seq).toBe(dueSeq)
        expect((last.watch as JsonMap).matched).toBe(false)
      },
    )
  })
  it('does not turn a page prepared before its deadline into deadline coverage', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('silence-prepared-clock'),
      (_, state) => {
        const f = fixture(state.storage, true)
        const deadline = '2100-01-01T00:00:00.000Z'
        const watch = f.n.execute(
          'CREATE CONCEPT ?w {TYPE "Watch" SET ATTRIBUTES {watch_class:"silence",summary:"wait",condition: "meaningful reply",due_at: :deadline,status:"disarmed"}}',
          { deadline },
        ).handles.w!
        f.s.armWatch(watch, 1)
        const page = f.s.prepareWatchPage(watch, 2, 1, 100, 'before-deadline')
          .prepared as unknown as PreparedWatchPage
        expect(page.deadline_covered).toBe(false)
        // New commits cannot broaden the frozen page or silently change its time basis.
        f.n.execute(
          'CREATE CONCEPT ?noise {TYPE "Person" NAME "after preparation"}',
        )
        vi.useFakeTimers()
        try {
          vi.setSystemTime(new Date('2101-01-01T00:00:00.000Z'))
          const result = f.s.commitWatchPage(
            page.ticket_ref,
            evaluation(page, 'no-match', 'no_match'),
          )
          expect(result.status).toBe('armed')
          expect((result.coverage as JsonMap).through_seq).toBe(
            page.through_seq,
          )
        } finally {
          vi.useRealTimers()
        }
      },
    )
  })
  it.each([
    [false, true, 'lookup'],
    [false, false, 'outcome_unknown'],
    [true, false, 'dispatch'],
  ] as const)(
    'repeated dispatch (idempotent=%s, lookup=%s) returns %s',
    async (idempotent, lookup, nextAction) => {
      await runInDurableObject(
        env.KIP_DB.getByName(`dispatch-${idempotent}-${lookup}`),
        (_, state) => {
          const f = dispatchFixture(state.storage, lookup)
          const first = f.s.beginWakeDispatch(
            f.wake,
            2,
            1,
            f.attempt,
            idempotent,
            lookup,
          )
          expect(first.action).toBe('dispatch')
          const second = f.s.beginWakeDispatch(
            f.wake,
            2,
            1,
            f.attempt,
            idempotent,
            lookup,
          )
          expect(second.action).toBe(nextAction)
          expect(second.dispatch_ref).toBe(first.dispatch_ref)
          expect(second.idempotency_key).toBe(first.idempotency_key)
          expect(() =>
            f.s.beginWakeDispatch(f.wake, 2, 0, f.attempt, idempotent, lookup),
          ).toThrow('lease_lost')
          expect(() => f.s.finishWake(f.wake, 2, 1)).toThrow('outcome_unknown')
          f.s.cancelWake(f.wake, 2, 1, 'stop new sends')
          expect(() =>
            f.s.beginWakeDispatch(f.wake, 3, 2, f.attempt, idempotent, lookup),
          ).toThrow('lease_lost')
          expect(
            f.n.store.controlAt(f.n.space, String(first.dispatch_ref)),
          ).not.toBeNull()
        },
      )
    },
  )
  it('requires a host binding and registered observer before accepting lookup capability', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('dispatch-no-observer'),
      (_, state) => {
        const f = dispatchFixture(state.storage, false)
        expect(() =>
          f.s.beginWakeDispatch(f.wake, 2, 1, f.attempt, false, true),
        ).toThrow('registered observer')
        expect(
          f.n.store.all(
            'kip_control_records',
            "SELECT * FROM kip_control_records WHERE kind = 'dispatch'",
          ),
        ).toEqual([])
      },
    )
    await runInDurableObject(
      env.KIP_DB.getByName('dispatch-no-binding'),
      (_, state) => {
        const f = dispatchFixture(state.storage, false, false)
        expect(() =>
          f.s.beginWakeDispatch(f.wake, 2, 1, f.attempt, false, false),
        ).toThrow('registered host binding')
      },
    )
  })
  it('uses direct authenticated lookup with CAS and never turns Finished into an Outcome', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('dispatch-lookup'),
      (_, state) => {
        const f = dispatchFixture(state.storage),
          first = f.s.beginWakeDispatch(f.wake, 2, 1, f.attempt, false, true),
          ref = String(first.dispatch_ref)
        expect(() => f.observer.readControl(ref)).toThrow()
        expect(
          f.s.beginWakeDispatch(f.wake, 2, 1, f.attempt, false, true).action,
        ).toBe('lookup')
        const observation = {
          observation_key: 'not-started',
          observed_at: new Date().toISOString(),
          configuration_digest: pin('observer').digest,
          status: 'not_started' as const,
        }
        expect(() => f.s.reconcileWakeLookup(ref, 2, observation)).toThrow(
          'directly authenticated',
        )
        const ready = f.observer.reconcileWakeLookup(ref, 2, observation)
        expect((ready.intent as JsonMap).state).toBe('ready')
        expect(
          f.s.beginWakeDispatch(f.wake, 2, 1, f.attempt, false, true).action,
        ).toBe('dispatch')
        expect(f.observer.reconcileWakeLookup(ref, 2, observation)).toEqual(
          ready,
        )
        expect((f.s.readControl(ref)!.value as JsonMap).state).toBe(
          'dispatching',
        )
        const finished = {
          ...observation,
          observation_key: 'finished',
          observed_at: new Date().toISOString(),
          status: 'finished' as const,
        }
        expect(() => f.observer.reconcileWakeLookup(ref, 2, finished)).toThrow(
          'version_conflict',
        )
        expect(() =>
          f.observer.reconcileWakeLookup(ref, 4, {
            ...finished,
            observed_at: expiry(),
          }),
        ).toThrow('interval')
        expect(
          (f.observer.reconcileWakeLookup(ref, 4, finished).intent as JsonMap)
            .state,
        ).toBe('dispatching')
        expect(
          f.n.query(
            'FIND(?e.id) WHERE {?e EVIDENCE {evidence_class:"outcome"}}',
          ),
        ).toEqual([])
        expect(() => f.s.finishWake(f.wake, 2, 1)).toThrow('outcome_unknown')
      },
    )
  })
  it('replays approved lookup without new approvals while enforcing authority and request identity', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('lookup-approval-replay'),
      (_, state) => {
        const f = dispatchFixture(state.storage),
          started = f.s.beginWakeDispatch(f.wake, 2, 1, f.attempt, false, true)
        const ref = String(started.dispatch_ref),
          observation = {
            observation_key: 'lookup-once',
            observed_at: new Date().toISOString(),
            configuration_digest: pin('observer').digest,
            status: 'not_started' as const,
          }
        const approval = requireHostApproval(f.n, 'record_outcome')
        expect(() =>
          f.observer.reconcileWakeLookup(ref, 1, observation),
        ).toThrow(/approval/i)
        const firstApproval = approval.approve()
        const first = f.observer.reconcileWakeLookup(ref, 1, observation)
        expect(f.n.store.governance.findApproval(firstApproval)?.status).toBe(
          'consumed',
        )
        const head = f.n.store.currentSeq(f.n.space)
        expect(f.observer.reconcileWakeLookup(ref, 1, observation)).toEqual(
          first,
        )
        expect(f.n.store.currentSeq(f.n.space)).toBe(head)
        expect(() =>
          f.observer.reconcileWakeLookup(ref, 2, {
            ...observation,
            observation_key: 'new-lookup',
          }),
        ).toThrow(/approval/i)
        expect(() =>
          f.observer.reconcileWakeLookup(ref, 1, {
            ...observation,
            status: 'running',
          }),
        ).toThrow('idempotency_conflict')
        const nextApproval = approval.approve()
        const nextHead = f.n.store.currentSeq(f.n.space)
        expect(f.observer.reconcileWakeLookup(ref, 1, observation)).toEqual(
          first,
        )
        expect(f.n.store.currentSeq(f.n.space)).toBe(nextHead)
        expect(f.n.store.governance.findApproval(nextApproval)?.status).toBe(
          'granted',
        )
        approval.deny()
        expect(() =>
          f.observer.reconcileWakeLookup(ref, 1, observation),
        ).toThrow(/record_outcome/)
      },
    )
  })
  it('invalidates a prepared page on a configuration update without modifying its Watch', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('dispatch-pins'),
      (_, state) => {
        const f = fixture(state.storage, true),
          page = f.s.prepareWatchPage(f.watch, 2, 1, 100, 'page')
            .prepared as unknown as PreparedWatchPage
        const saved = f.n.store.controlAt(f.n.space, 'attention/config')!,
          cfg = saved.value as unknown as AttentionConfig
        f.s.setAttentionConfig(saved.version, {
          ...cfg,
          pins: { ...cfg.pins, evaluator: pin('changed') },
        })
        expect(() =>
          f.s.commitWatchPage(
            page.ticket_ref,
            evaluation(page, 'result', 'match'),
          ),
        ).toThrow('basis_changed')
        expect(version(f.n, f.watch)).toBe(2)
      },
    )
  })
  it.each(['success', 'unknown'] as const)(
    'reconciles terminal %s after cancellation without allowing new sends',
    async (status) => {
      await runInDurableObject(
        env.KIP_DB.getByName(`dispatch-terminal-${status}`),
        (_, state) => {
          const f = dispatchFixture(state.storage),
            started = f.s.beginWakeDispatch(
              f.wake,
              2,
              1,
              f.attempt,
              false,
              true,
            ),
            ref = String(started.dispatch_ref)
          f.s.cancelWake(f.wake, 2, 1, 'stop')
          const outcome = f.n.execute(
            `MUTATE {
        CREATE EVIDENCE ?e {SET FIELDS {evidence_class:"outcome",payload:"target observation",observed_at: :at}
          SET FACET "OutcomeRecord" {attempt_ref: :attempt,metric:"completion",window:"run",terminal:true,observation_key: :key,observer_config_digest: :config,task_family:"attention.test",outcome_status: :status}}
        CREATE ACTIVITY ?o {SET FIELDS {activity_class:"outcome_observation",status:"completed"}
          SET STRUCTURAL {("inputs",:attempt) ("inputs",:decision) ("outputs",?e)}}
      }`,
            {
              at: new Date().toISOString(),
              attempt: f.attempt,
              decision: f.decision,
              key: `terminal-${status}`,
              config: pin('observer').digest,
              status,
            },
          ).handles.e!
          const approval = requireHostApproval(f.n, 'record_outcome')
          expect(() => f.s.reconcileWakeDispatch(ref, 1, outcome)).toThrow(
            /approval/i,
          )
          const firstApproval = approval.approve()
          const result = f.s.reconcileWakeDispatch(ref, 1, outcome)
          expect(f.n.store.governance.findApproval(firstApproval)?.status).toBe(
            'consumed',
          )
          expect(result.state).toBe(
            status === 'success' ? 'completed' : 'outcome_unknown',
          )
          const head = f.n.store.currentSeq(f.n.space)
          expect(f.s.reconcileWakeDispatch(ref, 1, outcome)).toEqual(result)
          expect(f.n.store.currentSeq(f.n.space)).toBe(head)
          const nextApproval = approval.approve()
          const nextHead = f.n.store.currentSeq(f.n.space)
          expect(f.s.reconcileWakeDispatch(ref, 1, outcome)).toEqual(result)
          expect(f.n.store.currentSeq(f.n.space)).toBe(nextHead)
          expect(f.n.store.governance.findApproval(nextApproval)?.status).toBe(
            'granted',
          )
          approval.deny()
          expect(() => f.s.reconcileWakeDispatch(ref, 1, outcome)).toThrow(
            /record_outcome/,
          )
          expect(f.s.readWake(f.wake).state.stage).toBe('cancelled')
          expect(() =>
            f.s.beginWakeDispatch(f.wake, 3, 2, f.attempt, false, true),
          ).toThrow('lease_lost')
        },
      )
    },
  )
})
