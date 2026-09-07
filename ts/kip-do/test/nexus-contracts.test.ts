import { env, runInDurableObject } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'
import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../src/nexus.js'
import { principalAuth } from '../src/governance/index.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { digest } from '../src/schema/contracts.js'
import { parseElementId } from '../src/id.js'
import type { Json, JsonMap } from '../src/json.js'

const SETUP = `MUTATE {CREATE CONCEPT ?a {TYPE "Person" NAME "Ada"} CREATE CONCEPT ?b {TYPE "Preference" NAME "tea"} ENSURE PROPOSITION ?p (?a,"prefers",?b) CREATE EVIDENCE ?e {SET FIELDS {evidence_class:"observation",payload:"material"}}}`
const BELIEF = 'FIND(?b) WHERE { ?p PROPOSITION (id:"P-1") ?b BELIEF (?p) }'
describe('Nexus host contracts', () => {
  it('opens a trial and commits a validated verdict with guarded caches', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-learning-flow'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(SETUP)
        n.execute(
          `MUTATE {
        CREATE CONCEPT ?s {TYPE "Skill" SET ATTRIBUTES {skill_class:"workflow",summary:"verify",status:"proposed"} SET STRUCTURAL {("current_revision",?r)}}
        CREATE CONCEPT ?r {TYPE "SkillRevision" SET ATTRIBUTES {task_family:"test",procedure:"verify",behavior_digest: :digest} SET STRUCTURAL {("revision_of",?s)}}
      }`,
          { digest: digest({ task_family: 'test', procedure: 'verify' }) },
        )
        const s = n.systemSession(),
          rule = { engine: 'kip:binary-stratified-v1' },
          parameters = { alpha: 0.05 }
        const rulePin = s.putArtifact(rule, []),
          parameterPin = s.putArtifact(parameters, [])
        const observers = [
            {
              principal_id: 'independent-instrument',
              configuration_digest: digest({ instrument: 'test' }),
              control_domain: 'independent-operator',
            },
          ],
          observerDigest = digest(observers)
        const policy = s.setEvaluationPolicy(0, {
          id: 'test-learning',
          version: '1',
          allowed_rules: [rulePin.content_digest],
          allowed_parameters: [parameterPin.content_digest],
          observers,
          observer_control_digest: observerDigest,
          minimum_independent_attempts: 2,
          allow_same_principal_observer: false,
          retain_adoption_on_insufficient: true,
        })
        const basis = (n.query(BELIEF)[0] as JsonMap).basis!
        const replay = s.putArtifact(
          {
            rule,
            parameters,
            basis,
            baseline_attempts: {},
            baseline_outcomes: {},
          },
          ['C-4'],
        )
        const trial: JsonMap = {
          revision_refs: ['C-4'],
          basis,
          rule: { ...rulePin },
          parameters: { ...parameterPin },
          baseline_attempt_refs: [],
          baseline_outcome_refs: [],
          comparability: {
            method: 'stratified',
            environment_digest: digest({}),
            strata_weights: { all: 1 },
            metric: 'success',
            minimum_effect: 0,
            uncertainty_rule: 'hoeffding',
            missingness_policy: 'count_as_failure',
            observer_control_digest: observerDigest,
            sampling_unit: 'attempt',
            correlation_policy: 'independent',
          },
          quota: 2,
          observation_window: 'one_action',
          replay_artifact: { ...replay },
          evaluation_policy: {
            id: 'test-learning',
            version: '1',
            content_digest: digest(policy.value),
          },
        }
        const trialRef = n.execute(
          `CREATE ACTIVITY ?trial {SET FIELDS {activity_class:"trial_open",status:"completed"} SET FACET "TrialRecord" ${JSON.stringify(trial)} SET STRUCTURAL {("inputs","C-4")}}`,
        ).handles.trial!
        const evaluationReplay = s.putArtifact(
          { rule, parameters, trial_record: trial, attempts: {}, outcomes: {} },
          ['C-4', trialRef],
        )
        const evaluation: JsonMap = {
          trial_ref: trialRef,
          revision_refs: ['C-4'],
          from_status: 'proposed',
          to_status: 'trialed',
          rule_digest: rulePin.content_digest,
          parameters_digest: parameterPin.content_digest,
          cutoff: new Date().toISOString(),
          attempt_refs: [],
          outcome_refs: [],
          excluded_samples: [],
          missing_attempt_refs: [],
          comparison: {
            status: 'insufficient',
            effect: null,
            uncertainty: { method: 'hoeffding', alpha: 0.05 },
          },
          replay_artifact: { ...evaluationReplay },
        }
        const write = (
          record: JsonMap,
          version: number,
          success: number,
        ): string => `MUTATE {
        CREATE ACTIVITY ?verdict {SET FIELDS {activity_class:"lifecycle_verdict",status:"completed"} SET FACET "EvaluationRecord" ${JSON.stringify(record)} SET STRUCTURAL {("inputs","C-4") ("inputs","${trialRef}") ("outputs","C-3")}}
        UPDATE "C-3" SET ATTRIBUTES {status:"trialed"} SET FACET "TrialState" {revision_ref:"C-4",trial_ref:"${trialRef}"} SET FACET "GradingState" {revision_ref:"C-4",evaluation_ref:?verdict,success_count:${success},failure_count:0,graded_count:0} EXPECT VERSION ${version}
      }`
        n.execute(write(evaluation, 1, 0))
        expect(
          n.query('FIND(?s.attributes.status) WHERE {?s CONCEPT {id:"C-3"}}'),
        ).toEqual(['trialed'])
        expect(() =>
          n.execute(write({ ...evaluation, from_status: 'trialed' }, 2, 99)),
        ).toThrow('counts')
      },
    )
  })
  it('keeps external attempt identity and requires the caller current fence on recovery', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-dispatch-recovery'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(SETUP)
        n.execute(
          'CREATE CONCEPT ?task {TYPE "SleepTask" SET ATTRIBUTES {task_class:"review_skill",summary:"action",status:"pending"}}',
        )
        const s = n.systemSession()
        s.leaseTask('C-3', 1, '2099-01-01T00:00:00Z')
        const selection = s.putArtifact(
            { selection: 'explicit action without a Skill' },
            [],
          ),
          basis = (n.query(BELIEF)[0] as JsonMap).basis as JsonMap
        const decision = {
          decision: 'act',
          retrieved_refs: ['E-1'],
          used_refs: ['E-1'],
          applied_revisions: [],
          basis,
        }
        const dependency = {
          basis_seq: basis.snapshot_seq!,
          groups: [{ role: 'all_of', pins: [{ id: 'E-1', version: 1 }] }],
          policy_basis: basis,
        }
        const gate = n.execute(
          `CREATE ACTIVITY ?gate {SET FIELDS {activity_class:"action_gate",status:"completed"} SET FACET "DecisionRecord" ${JSON.stringify(decision)} SET FACET "DependencyBasis" ${JSON.stringify(dependency)} SET STRUCTURAL {("inputs","E-1")}}`,
        ).handles.gate!
        for (const [key, idempotent] of [
          ['unsafe-effect', false],
          ['idempotent-effect', true],
        ] as const) {
          const record: Json = {
            attempt_id: key,
            decision_ref: gate,
            applied_revisions: [],
            trial_ref: null,
            context: {},
            environment_digest: digest({}),
            tool_versions: { test: '1' },
            selection_policy: { ...selection },
            preconditions_satisfied: 'yes',
            started_at: new Date().toISOString(),
          }
          const command = `CREATE ACTIVITY ?attempt {SET FIELDS {activity_class:"action_attempt",status:"completed"} SET FACET "AttemptRecord" ${JSON.stringify(record)} SET STRUCTURAL {("inputs","${gate}")}}`
          const attempt = n.execute(command).handles.attempt!
          expect(() => n.execute(command)).toThrow('Space-unique')
          s.enqueueDispatch({
            attempt_ref: attempt,
            task_ref: 'C-3',
            fencing_token: 1,
            supports_idempotency: idempotent,
            supports_outcome_lookup: false,
          })
          const first = s.beginDispatch(key, 1, 1)
          expect(first.action).toBe('dispatch')
          expect(first.idempotency_key).toBe(key)
        }
        expect(s.beginDispatch('unsafe-effect', 2, 1).action).toBe(
          'outcome_unknown',
        )
        n.execute(
          'UPDATE "C-3" SET ATTRIBUTES {status:"completed"} EXPECT VERSION 2',
        )
        n.execute(
          'UPDATE "C-3" SET ATTRIBUTES {status:"pending"} EXPECT VERSION 3',
        )
        expect(
          (s.leaseTask('C-3', 4, '2099-02-01T00:00:00Z').lease as JsonMap)
            .fencing_token,
        ).toBe(2)
        expect(() => s.beginDispatch('idempotent-effect', 2, 1)).toThrow()
        const resumed = s.beginDispatch('idempotent-effect', 2, 2)
        expect(resumed.action).toBe('dispatch')
        expect(resumed.idempotency_key).toBe('idempotent-effect')
      },
    )
  })
  it('revalidates exact outputs while preserving Assertion premises', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-revalidation'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(SETUP)
        let basis = (n.query(BELIEF)[0] as JsonMap).basis as JsonMap
        n.execute(
          `MUTATE {
        CREATE ASSERTION ?a {SET FIELDS {proposition:"P-1",asserted_by:"C-1",mode:"inferred",stance:"support",confidence:0.9}}
        CREATE ACTIVITY ?work {SET FIELDS {activity_class:"semantic_consolidation",status:"completed"} SET FACET "DependencyBasis" {basis_seq: :seq,groups:[{role:"all_of",pins:[{id:"C-1",version:1}]}],policy_basis: :basis} SET STRUCTURAL {("inputs","C-1") ("outputs",?a)}}
      }`,
          { seq: basis.snapshot_seq!, basis },
        )
        n.execute('UPDATE "C-1" SET FIELDS {name:"Ada Lovelace"}')
        expect((n.query(BELIEF)[0] as JsonMap).status).not.toBe('accepted')
        basis = (n.query(BELIEF)[0] as JsonMap).basis as JsonMap
        const validation = `CREATE ACTIVITY ?v {SET FIELDS {activity_class:"dependency_validation",status:"completed"} SET FACET "DependencyBasis" {basis_seq: :seq,groups:[{role:"all_of",pins:[{id: :source,version: :version}]}],policy_basis: :basis} SET STRUCTURAL {("inputs",:source) ("outputs","A-1")}}`
        expect(() =>
          n.execute(validation, {
            seq: basis.snapshot_seq!,
            basis,
            source: 'C-2',
            version: 1,
          }),
        ).toThrow('premises')
        n.execute(validation, {
          seq: basis.snapshot_seq!,
          basis,
          source: 'C-1',
          version: 2,
        })
        expect((n.query(BELIEF)[0] as JsonMap).status).toBe('accepted')
      },
    )
  })
  it('uses retained trust for historical belief and covers empty stream pages', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-controls'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(SETUP)
        n.execute(
          'CREATE ASSERTION ?a {SET FIELDS {proposition:"P-1",asserted_by:"C-1",mode:"stated",stance:"support",confidence:0.9}}',
        )
        const seq = n.store.currentSeq(n.space),
          s = n.systemSession()
        s.setTrust(1, { 'C-1': 0 })
        expect((n.query(BELIEF)[0] as JsonMap).status).not.toBe('accepted')
        expect(
          (n.query(`${BELIEF} AS OF SEQ ${seq}`)[0] as JsonMap).status,
        ).toBe('accepted')
        const page = s.changePage(seq)
        expect((page.coverage as JsonMap).complete).toBe(true)
        expect(
          (page.changes as JsonMap[]).some((e) =>
            (e.control_changes as JsonMap[]).some((c) => c.kind === 'trust'),
          ),
        ).toBe(true)
        const empty = s.changePage(n.store.currentSeq(n.space))
        expect(empty.changes).toEqual([])
        expect((empty.coverage as JsonMap).complete).toBe(true)
      },
    )
  })
  it('withdraws protected identity decisions without rewriting history', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-identity'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(SETUP)
        n.execute('CREATE CONCEPT ?alias {TYPE "Person" NAME "A. Lovelace"}')
        const merged = n.execute('MERGE CONCEPT "C-3" INTO "C-1"'),
          seq = n.store.currentSeq(n.space)
        const key = `identity:${merged.tx_id}:C-3`
        n.execute('ASSERT (:a,"prefers",:b) {by: :actor,mode:"stated"}', {
          a: 'C-3',
          b: 'C-2',
          actor: 'C-1',
        })
        const result = n.systemSession().withdrawIdentity(key, seq, ['E-1'])
        expect(result.identity_version).toBeGreaterThan(seq)
        const current = n.store.load(parseElementId('C-3'))!,
          past = n.store.elementAt(n.space, parseElementId('C-3'), seq)!
        expect(current.kind === 'Concept' && current.row.merged_into).toBe('')
        expect(past.kind === 'Concept' && past.row.merged_into).toBe('C-1')
        expect(() =>
          n.systemSession().withdrawIdentity(key, seq, ['E-1']),
        ).toThrow()
      },
    )
  })
  it('creates bidirectional revisions and refuses unvalidated standing', async () => {
    await runInDurableObject(env.KIP_DB.getByName('host-skill'), (_, state) => {
      const n = CognitiveNexus.connect(state.storage)
      n.activatePackages([COGNITIVE_MEMORY])
      n.execute(
        `MUTATE {
        CREATE CONCEPT ?skill {TYPE "Skill" NAME "verify" SET ATTRIBUTES {skill_class:"workflow",summary:"verify",status:"proposed"} SET STRUCTURAL {("current_revision",?revision)}}
        CREATE CONCEPT ?revision {TYPE "SkillRevision" SET ATTRIBUTES {task_family:"test",procedure:"verify before act",behavior_digest: :digest} SET STRUCTURAL {("revision_of",?skill)}}
      }`,
        {
          digest: digest({
            task_family: 'test',
            procedure: 'verify before act',
          }),
        },
      )
      expect(() =>
        n.execute('UPDATE "C-1" SET ATTRIBUTES {status:"adopted"}'),
      ).toThrow()
      expect(() =>
        n.execute('UPDATE "C-2" SET ATTRIBUTES {procedure:"changed"}'),
      ).toThrow()
      expect(
        n.query('FIND(?s.attributes.status) WHERE {?s CONCEPT {id:"C-1"}}'),
      ).toEqual(['proposed'])
    })
  })
  it('persists task fences and Watch generations across reconnect', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-durable'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(SETUP)
        n.execute(`MUTATE {
        CREATE CONCEPT ?task {TYPE "SleepTask" SET ATTRIBUTES {task_class:"review_skill",summary:"review",status:"pending"}}
        CREATE CONCEPT ?watch {TYPE "Watch" SET ATTRIBUTES {watch_class:"delta",summary:"name change",condition:{element:"C-1",ops:["update"],touched:["fields.name"]},status:"disarmed"}}
      }`)
        const s = n.systemSession()
        expect(
          (s.leaseTask('C-3', 1, '2099-01-01T00:00:00Z').lease as JsonMap)
            .fencing_token,
        ).toBe(1)
        expect(() => s.leaseTask('C-3', 1, '2099-02-01T00:00:00Z')).toThrow()
        s.armWatch('C-4', 1)
        expect(() =>
          n.execute(
            'UPDATE "C-4" SET ATTRIBUTES {status:"fired"} EXPECT VERSION 2',
          ),
        ).toThrow()
        s.putArtifact({ retained: 'watch-independent' }, ['E-1'])
        n.execute('UPDATE "C-1" SET FIELDS {name:"Ada Byron"}')
        const re = CognitiveNexus.connect(state.storage).systemSession()
        expect(re.advanceWatch('C-4', 2, 1).status).toBe('fired')
        re.armWatch('C-4', 3)
        expect(() => re.advanceWatch('C-4', 4, 1)).toThrow()
        expect(() =>
          n.execute('UPDATE "C-3" SET ATTRIBUTES {status:"completed"}'),
        ).toThrow()
        n.execute(
          'UPDATE "C-3" SET ATTRIBUTES {status:"completed"} EXPECT VERSION 2',
        )
      },
    )
  })
  it('compares lease expiry as an instant and persists UTC', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-offset-lease'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(
          'CREATE CONCEPT ?task {TYPE "SleepTask" SET ATTRIBUTES {task_class:"review_skill",summary:"action",status:"pending"}}',
        )
        const expires = new Date(Date.now() + 60 * 60 * 1000)
          .toISOString()
          .replace('Z', '+02:00')
        expect(Date.parse(expires)).toBeLessThan(Date.now())
        expect(() =>
          n.execute(
            `UPDATE "C-1" SET ATTRIBUTES {status:"running"} SET FACET "LeaseState" {owner:"${SYSTEM_PRINCIPAL}",fencing_token:1,attempt_count:1,expires_at:"${expires}"} EXPECT VERSION 1`,
          ),
        ).toThrow()
        expect(
          (n
            .systemSession()
            .leaseTask('C-1', 1, '2099-01-01T02:00:00+02:00')
            .lease as JsonMap).expires_at,
        ).toBe('2099-01-01T00:00:00.000Z')
      },
    )
  })
  it('scopes reference audit to each write and current visibility', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-reference-audit'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(SETUP)
        n.execute(
          `MUTATE {
            CREATE CONCEPT ?public {TYPE "Person" NAME "Public"}
            ASSERT (:private,"prefers",:value) {by: :private,mode:"stated"}
            CREATE ACTIVITY ?audit {SET FIELDS {activity_class:"audit",status:"completed"} SET STRUCTURAL {("inputs",:private)}}
          }`,
          { private: 'C-1', value: 'C-2' },
        )
        expect(
          JSON.stringify(
            n.query(
              'FIND(?c._system.input_references) WHERE {?c CONCEPT {id:"C-3"}}',
            ),
          ),
        ).not.toContain('C-1')

        const gov = n.store.governance
        gov.ensurePrincipal({ principal_id: 'kip:principal:limited' })
        gov.createGrant(
          {
            space_id: n.space,
            grantee_principal: 'kip:principal:limited',
            actions: ['read'],
            scope: { elements: ['X-1'] },
            constraints: { fields: ['_system'] },
          },
          SYSTEM_PRINCIPAL,
        )
        const reader = n.session(principalAuth('kip:principal:limited'))
        expect(
          JSON.stringify(
            reader.query(
              'FIND(?x._system.input_references) WHERE {?x ACTIVITY {id:"X-1"}}',
            ),
          ),
        ).not.toContain('C-1')
      },
    )
  })
  it('erases all owned replay copies when their payload source is purged', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('host-erasure'),
      (_, state) => {
        const n = CognitiveNexus.connect(state.storage)
        n.activatePackages([COGNITIVE_MEMORY])
        n.execute(SETUP)
        const s = n.systemSession(),
          pin = s.putArtifact({ source: 'material' }, ['E-1'])
        expect(s.readArtifact(pin)).toEqual({ source: 'material' })
        n.execute('PURGE PAYLOAD "E-1" CONFIRM "PURGE"')
        expect(() => s.readArtifact(pin)).toThrow()
        expect(() => s.putArtifact({ source: 'material' }, ['E-1'])).toThrow()
      },
    )
  })
})
