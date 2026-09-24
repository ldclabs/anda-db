import { requireHostApproval } from './support/approval.js'
import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it, vi } from 'vitest'
import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { principalAuth } from '../src/governance/index.js'
import { parseElementId } from '../src/id.js'
import { trustWeight, type TrustConfiguration } from '../src/trust.js'
import type { Json, JsonMap } from '../src/json.js'
import { OPTIONS } from './support/options.js'
const P = 'kip://profiles/cognitive-memory@2.0.0/prefers'
function fixture(storage: DurableObjectStorage) {
  const n = CognitiveNexus.connect(storage)
  n.activatePackages([COGNITIVE_MEMORY, OPTIONS])
  const ids = n.execute(`MUTATE {
    CREATE CONCEPT ?actor {TYPE "Person" NAME "source"}
    CREATE CONCEPT ?object {TYPE "Option" NAME "tea"}
    CREATE CONCEPT ?work {TYPE "Event" NAME "work" SET ATTRIBUTES {summary:"work context"}}
    CREATE CONCEPT ?home {TYPE "Event" NAME "home" SET ATTRIBUTES {summary:"home context"}}
    ENSURE PROPOSITION ?p (?actor,"prefers",?object)
    CREATE ASSERTION ?a {SET FIELDS {proposition:?p,asserted_by:?actor,mode:"stated",stance:"support",confidence:1}}
    CREATE EVIDENCE ?e {SET FIELDS {evidence_class:"observation",payload:"independent observation"}}
  }`).handles
  const config = (weight: number): TrustConfiguration => ({
    weights: {},
    default_weight: 1,
    rules: [
      {
        id: 'work-preference',
        actor_ref: ids.actor!,
        predicate_ref: P,
        context_ref: ids.work!,
        weight,
      },
    ],
  })
  const belief = (contexts: string[], history?: number): JsonMap =>
    n.query(
      `FIND(?b) WHERE {?p PROPOSITION(id: :id) ?b BELIEF(?p)} ${history === undefined ? '' : `AS OF SEQ ${history}`} WITH EPISTEMIC {context_refs: :contexts}`,
      { id: ids.p!, contexts },
    )[0] as JsonMap
  return { n, s: n.systemSession(), ids, config, belief }
}
describe('contextual trust parity', () => {
  it('projects scoped trust and retained history without changing assertion confidence', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('trust-scoped'),
      (_, state) => {
        const f = fixture(state.storage),
          seq = f.n.store.currentSeq(f.n.space),
          config = f.config(0.1)
        f.s.setContextualTrust(1, config)
        expect(f.belief([]).status).toBe('accepted')
        expect(f.belief([f.ids.home!]).status).toBe('accepted')
        expect(f.belief([f.ids.work!]).status).toBe('uncertain')
        expect(f.belief([f.ids.work!], seq).status).toBe('accepted')
        expect(
          (f.belief([f.ids.work!]).basis as JsonMap).trust_version,
        ).not.toBe(
          (f.belief([f.ids.work!], seq).basis as JsonMap).trust_version,
        )
        const assertion = f.n.store.load(parseElementId(f.ids.a!))!
        expect(assertion.kind === 'Assertion' && assertion.row.confidence).toBe(
          1,
        )
        f.s.setTrust(2, {}, 0.9)
        expect((f.s.readControl('trust')!.value as JsonMap).rules).toEqual(
          config.rules,
        )
        expect(f.belief([f.ids.work!]).status).toBe('uncertain')
        const reopened = CognitiveNexus.connect(state.storage)
        expect(
          (reopened.systemSession().readControl('trust')!.value as JsonMap)
            .rules,
        ).toEqual(config.rules)
      },
    )
  })
  it('rejects ambiguous scopes regardless of rule order, while combined scopes win', () => {
    const rules = [
      {
        id: 'predicate',
        actor_ref: 'C-1',
        predicate_ref: P,
        context_ref: null,
        weight: 0.1,
      },
      {
        id: 'context',
        actor_ref: 'C-1',
        predicate_ref: null,
        context_ref: 'C-2',
        weight: 0.9,
      },
    ]
    expect(() => trustWeight(rules, 1, 'C-1', P, ['C-2'])).toThrow('ambiguous')
    expect(() =>
      trustWeight([...rules].reverse(), 1, 'C-1', P, ['C-2']),
    ).toThrow('ambiguous')
    const combined = {
      id: 'combined',
      actor_ref: 'C-1',
      predicate_ref: P,
      context_ref: 'C-2',
      weight: 0.5,
    }
    expect(trustWeight([...rules, combined], 1, 'C-1', P, ['C-2'])).toBe(0.5)
    expect(trustWeight([combined, ...rules], 1, 'C-1', P, ['C-2'])).toBe(0.5)
    expect(trustWeight(rules, 0.8, 'C-9', P, ['C-2'])).toBe(0.8)
  })
  it('validates exact references, finite weights and unique selectors', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('trust-validation'),
      (_, state) => {
        const f = fixture(state.storage),
          cfg = f.config(0.1)
        expect(() =>
          f.s.setContextualTrust(1, { ...cfg, default_weight: NaN }),
        ).toThrow('bounded')
        expect(() =>
          f.s.setContextualTrust(1, {
            ...cfg,
            rules: [...cfg.rules!, { ...cfg.rules![0]!, id: 'duplicate' }],
          }),
        ).toThrow('unique')
        expect(() =>
          f.s.setContextualTrust(1, {
            ...cfg,
            rules: [{ ...cfg.rules![0]!, predicate_ref: 'prefers' }],
          }),
        ).toThrow('exact schema')
        expect(() =>
          f.s.setContextualTrust(1, {
            ...cfg,
            rules: [{ ...cfg.rules![0]!, actor_ref: f.ids.e! }],
          }),
        ).toThrow()
        expect(f.s.readControl('trust')!.version).toBe(1)
      },
    )
  })
  it('atomically binds proposal, method, evidence and governance audit with idempotent replay', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('trust-calibration'),
      (_, state) => {
        const f = fixture(state.storage),
          method = f.s.putArtifact({ method: 'independent calibration' }, [])
        const proposal = f.s.putArtifact(
          {
            format: 'nexus:trust-calibration-v1',
            space_id: f.n.space,
            expected_version: 1,
            configuration: f.config(0.2) as unknown as Json,
            method: { ...method },
            evidence_refs: [f.ids.e!],
            uncertainty: { interval: [0.1, 0.3] },
          },
          [f.ids.e!],
        )
        const principal = 'kip:principal:trust-manager'
        f.n.store.governance.ensurePrincipal({ principal_id: principal })
        f.n.store.governance.createGrant(
          {
            space_id: f.n.space,
            grantee_principal: principal,
            actions: ['manage_trust', 'read', 'read_governance_history'],
          },
          SYSTEM_PRINCIPAL,
        )
        const manager = f.n.session(principalAuth(principal))
        const audits = () =>
          state.storage.sql
            .exec<{
              n: number
            }>(
              "SELECT COUNT(*) AS n FROM gov_audit WHERE operation = 'apply_trust_calibration'",
            )
            .one().n
        const before = audits(),
          original = f.n.store.putControl.bind(f.n.store)
        const fault = vi
          .spyOn(f.n.store, 'putControl')
          .mockImplementation((row) => {
            original(row)
            if (row.key === 'trust') throw new Error('injected trust failure')
          })
        expect(() =>
          manager.applyTrustCalibration(1, proposal, 'apply'),
        ).toThrow('injected')
        fault.mockRestore()
        expect(audits()).toBe(before)
        expect(f.s.readControl('trust')!.version).toBe(1)
        const beforeSeq = f.n.store.currentSeq(f.n.space)
        const result = manager.applyTrustCalibration(1, proposal, 'apply'),
          head = f.n.store.currentSeq(f.n.space)
        expect(head).toBe(beforeSeq + 1)
        expect(manager.applyTrustCalibration(1, proposal, 'apply')).toEqual(
          result,
        )
        expect(f.n.store.currentSeq(f.n.space)).toBe(head)
        expect(audits()).toBe(before + 1)
        expect(
          (f.s.readControl('trust')!.value as JsonMap).calibration,
        ).toMatchObject({ proposal, method, evidence_refs: [f.ids.e!] })
        expect(f.belief([f.ids.work!]).status).toBe('uncertain')
        const reopened = CognitiveNexus.connect(state.storage).session(
          principalAuth(principal),
        )
        expect(reopened.applyTrustCalibration(1, proposal, 'apply')).toEqual(
          result,
        )
        expect(() =>
          manager.applyTrustCalibration(2, proposal, 'apply'),
        ).toThrow()
      },
    )
  })
  it('replays approved calibration without requiring or spending another approval', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('trust-approval-replay'),
      (_, state) => {
        const f = fixture(state.storage),
          method = f.s.putArtifact({ method: 'approved calibration' }, [])
        const proposal = f.s.putArtifact(
          {
            format: 'nexus:trust-calibration-v1',
            space_id: f.n.space,
            expected_version: 1,
            configuration: f.config(0.2) as unknown as Json,
            method: { ...method },
            evidence_refs: [f.ids.e!],
            uncertainty: { interval: [0.1, 0.3] },
          },
          [f.ids.e!],
        )
        const approval = requireHostApproval(f.n, 'manage_trust')
        expect(() => f.s.applyTrustCalibration(1, proposal, 'once')).toThrow(
          /approval/i,
        )
        expect(f.s.readControl('trust')!.version).toBe(1)
        const firstApproval = approval.approve()
        const first = f.s.applyTrustCalibration(1, proposal, 'once')
        expect(f.n.store.governance.findApproval(firstApproval)?.status).toBe(
          'consumed',
        )
        const head = f.n.store.currentSeq(f.n.space)
        expect(f.s.applyTrustCalibration(1, proposal, 'once')).toEqual(first)
        expect(f.n.store.currentSeq(f.n.space)).toBe(head)
        expect(() =>
          f.s.applyTrustCalibration(1, proposal, 'new-operation'),
        ).toThrow(/approval/i)
        const nextApproval = approval.approve()
        const nextHead = f.n.store.currentSeq(f.n.space)
        const reopened = CognitiveNexus.connect(state.storage).systemSession()
        expect(reopened.applyTrustCalibration(1, proposal, 'once')).toEqual(
          first,
        )
        expect(f.n.store.currentSeq(f.n.space)).toBe(nextHead)
        expect(f.n.store.governance.findApproval(nextApproval)?.status).toBe(
          'granted',
        )
        expect(() => f.s.applyTrustCalibration(2, proposal, 'once')).toThrow()
        expect(f.n.store.governance.findApproval(nextApproval)?.status).toBe(
          'granted',
        )
        expect(
          state.storage.sql
            .exec<{
              n: number
            }>("SELECT COUNT(*) AS n FROM gov_audit WHERE operation = 'apply_trust_calibration'")
            .one().n,
        ).toBe(1)
        approval.deny()
        expect(() => f.s.applyTrustCalibration(1, proposal, 'once')).toThrow(
          /manage_trust/,
        )
      },
    )
  })
  it('rejects proposals without inherited evidence and corrected evidence', async () => {
    await runInDurableObject(
      env.KIP_DB.getByName('trust-evidence'),
      (_, state) => {
        const f = fixture(state.storage),
          method = f.s.putArtifact({ method: 'test' }, [])
        const payload = {
          format: 'nexus:trust-calibration-v1',
          space_id: f.n.space,
          expected_version: 1,
          configuration: f.config(0.2) as unknown as Json,
          method: { ...method },
          evidence_refs: [f.ids.e!],
          uncertainty: { interval: [0, 1] },
        }
        const bad = f.s.putArtifact(payload, [])
        expect(() =>
          f.s.applyTrustCalibration(1, bad, 'missing-material'),
        ).toThrow('inherit')
        const good = f.s.putArtifact(
          { ...payload, uncertainty: { interval: [0.1, 0.3] } },
          [f.ids.e!],
        )
        const replacement = f.n.execute(
          'CREATE EVIDENCE ?e {SET FIELDS {evidence_class:"observation",payload:"corrected material"}}',
        ).handles.e!
        f.n.execute('TRANSITION :e TO "corrected" BY :replacement', {
          e: f.ids.e!,
          replacement,
        })
        expect(() => f.s.applyTrustCalibration(1, good, 'corrected')).toThrow(
          'eligible',
        )
        expect(f.s.readControl('trust')!.version).toBe(1)
      },
    )
  })
})
