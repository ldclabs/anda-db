import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { SYSTEM_PRINCIPAL, principalAuth } from '../src/governance/index.js'
import { KipError } from '../src/errors.js'
import type { JsonMap } from '../src/json.js'
import type { HostCapabilities } from '../src/meta/host.js'
import { checkEnvelope } from '../src/request.js'

/**
 * The exposure log (Spec §66.8, `KIP2-RT-035`) and the capabilities a host
 * declares around a Nexus (§67.4, Memory Interface §2).
 *
 * @see rs/anda_cognitive_nexus/tests/exposure_log.rs
 * @see rs/anda_cognitive_nexus/tests/meta.rs — host_capabilities_are_the_hosts_to_declare
 */

async function withNexus(name: string, body: (nexus: CognitiveNexus) => void): Promise<void> {
  const stub = env.KIP_DB.getByName(`exposure-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY])
    body(nexus)
  })
}

function codeOf(run: () => unknown): string {
  try {
    run()
  } catch (err) {
    return KipError.from(err).code
  }
  return 'no error'
}

const seqOf = (nexus: CognitiveNexus): number => (nexus.describe('DESCRIBE SPACE') as JsonMap).seq as number

describe('the exposure log', () => {
  it('is never cognition', async () => {
    await withNexus('never-cognition', (nexus) => {
      const { handles } = nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" SET FACET "MnemonicState" {memory_strength: 0.4} }
        CREATE ACTIVITY ?gate { SET FIELDS {activity_class: "action_gate", status: "completed"} }
      }`)
      const [alice, gate] = [handles.alice!, handles.gate!]
      const seq = seqOf(nexus)
      const before = nexus.query('FIND(?c) WHERE { ?c CONCEPT {id: :id} }', { id: alice })
      const session = nexus.systemSession()
      expect(
        session.recordExposures([
          { element_id: alice, exposure: 'retrieved', snapshot_seq: seq, recall_ref: 'recall-1' },
          { element_id: alice, exposure: 'used', snapshot_seq: seq, decision_ref: gate },
        ]),
      ).toEqual({ recorded: 2 })

      expect(seqOf(nexus)).toBe(seq)
      const changes = nexus.describe(`CHANGES AFTER SEQ ${seq}`) as JsonMap
      expect((changes.changes as unknown[] | undefined) ?? []).toEqual([])
      expect(nexus.query('FIND(?c) WHERE { ?c CONCEPT {id: :id} }', { id: alice })).toEqual(before)

      const page = session.readExposures()
      const records = page.records as JsonMap[]
      expect(records.map((r) => r.exposure)).toEqual(['retrieved', 'used'])
      expect(records[0]!.recall_ref).toBe('recall-1')
      expect(records[1]!.decision_ref).toBe(gate)
      expect(records[1]!.principal_id).toBe(SYSTEM_PRINCIPAL)
      expect(page.next_cursor).toBeNull()

      const first = session.readExposures({ limit: 1 })
      const second = session.readExposures({ cursor: first.next_cursor as string, limit: 1 })
      expect((second.records as JsonMap[])[0]!.exposure).toBe('used')

      expect(codeOf(() => session.recordExposures([{ element_id: alice, exposure: 'used', snapshot_seq: seq }]))).toBe(
        'ConstraintViolation',
      )
      expect(
        codeOf(() => session.recordExposures([{ element_id: alice, exposure: 'retrieved', snapshot_seq: seq + 10 }])),
      ).toBe('ConstraintViolation')

      nexus.execute(`PURGE "${alice}" CONFIRM "PURGE"`)
      expect(session.readExposures().records).toEqual([])
    })
  })

  it('is audit, and hides what a reader cannot discover', async () => {
    await withNexus('audit', (nexus) => {
      const { handles } = nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
        ASSERT ?a (?alice, "same_as", ?bob) { by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z" }
      }`)
      const [alice, assertion] = [handles.alice!, handles.a!]
      const seq = seqOf(nexus)
      nexus.systemSession().recordExposures([
        { element_id: alice, exposure: 'retrieved', snapshot_seq: seq },
        { element_id: assertion, exposure: 'retrieved', snapshot_seq: seq },
      ])
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:reader' })
      gov.createGrant({ space_id: nexus.space, grantee_principal: 'kip:principal:reader', actions: ['discover', 'read'] }, SYSTEM_PRINCIPAL)
      gov.ensurePrincipal({ principal_id: 'kip:principal:auditor' })
      gov.createGrant({ space_id: nexus.space, grantee_principal: 'kip:principal:auditor', actions: ['read_audit'] }, SYSTEM_PRINCIPAL)
      gov.createGrant(
        { space_id: nexus.space, grantee_principal: 'kip:principal:auditor', actions: ['discover', 'read'], scope: { kinds: ['concept'] } },
        SYSTEM_PRINCIPAL,
      )
      const reader = nexus.session(principalAuth('kip:principal:reader'))
      expect(codeOf(() => reader.readExposures())).toBe('NotAuthorized')
      const auditor = nexus.session(principalAuth('kip:principal:auditor'))
      expect((auditor.readExposures().records as JsonMap[]).map((r) => r.element_id)).toEqual([alice])
      expect(
        codeOf(() => auditor.recordExposures([{ element_id: assertion, exposure: 'retrieved', snapshot_seq: seq }])),
      ).toBe('NotFoundOrNotVisible')
    })
  })
})

describe('host capabilities', () => {
  it('are the host\'s to declare', async () => {
    await withNexus('host', (nexus) => {
      const registry = () => ((nexus.describe('DESCRIBE CAPABILITIES') as JsonMap).supported as JsonMap).registry as JsonMap
      for (const name of ['memory_interface', 'durable_brain_runtime', 'receiver_fencing']) {
        expect(registry()[name]).toBe(false)
      }
      expect((nexus.describe('DESCRIBE PRIMER') as JsonMap).extensions).toBeUndefined()

      const descriptor = {
        kip_memory: '2.0' as const,
        bundles: ['memory_basic' as const],
        default_budget: { max_output_tokens: 4096, deadline_ms: 30000 },
        tokenizer: 'o200k_base',
        minimum_response_tokens: 256,
      }
      // This engine does not claim KIP-CognitiveMemory, so memory_experience
      // cannot be advertised over it.
      expect(
        codeOf(() =>
          nexus.setHostCapabilities({
            memory_interface: { ...descriptor, bundles: ['memory_basic', 'memory_experience'] },
          }),
        ),
      ).toBe('UnsupportedCapability')
      expect(codeOf(() => nexus.setHostCapabilities({ receiver_fencing: true }))).toBe('UnsupportedCapability')

      const requires = (name: string): string =>
        codeOf(() =>
          checkEnvelope(
            { kip: '2.0', requires: { [name]: true }, operations: [{ command: 'DESCRIBE SPACE' }] } as never,
            { id: nexus.space, row: () => nexus.spaceRow(), host: nexus.store.hostCapabilities },
          ),
        )
      expect(requires('memory_interface')).toBe('UnsupportedCapability')

      const host: HostCapabilities = { memory_interface: descriptor, durable_brain_runtime: true }
      nexus.setHostCapabilities(host)
      expect(requires('memory_interface')).toBe('no error')
      expect(requires('durable_brain_runtime')).toBe('no error')
      expect(requires('receiver_fencing')).toBe('UnsupportedCapability')
      expect((registry().memory_interface as JsonMap).bundles).toEqual(['memory_basic'])
      expect(registry().durable_brain_runtime).toBe(true)
      expect(registry().receiver_fencing).toBe(false)
      const primer = nexus.describe('DESCRIBE PRIMER') as JsonMap
      expect(((primer.extensions as JsonMap).memory_interface as JsonMap).kip_memory).toBe('2.0')
      expect(nexus.hostCapabilities()).toEqual(host)
    })
  })
})
