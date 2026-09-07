import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import type { KipReceipt, KipResponse } from '../src/durable-object.js'
import { sha3_256Text } from '../src/digest.js'
import { canonicalJson } from '../src/json.js'
import { Store } from '../src/store/index.js'
import { TENANT_PRINCIPAL } from './worker.js'

/**
 * The request envelope after 793af73 (Spec §33.2, §67.4, §71.1, §75, §81):
 * per-operation Receipts, the idempotency echo, the `on_error` default, the
 * capability registry a `requires` block is checked against, and ingest's
 * element-reference `source_actor` and `facets`.
 */
async function post(name: string, body: unknown): Promise<KipResponse> {
  const stub = env.KIP_DB.getByName(`receipts-${name}`)
  const response = await stub.fetch('https://kip.invalid/', {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return (await response.json()) as KipResponse
}

async function tenantPost(name: string, body: unknown): Promise<KipResponse> {
  const stub = env.KIP_TENANT_DB.getByName(`receipts-${name}`)
  const response = await stub.fetch('https://kip.invalid/', {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return (await response.json()) as KipResponse
}

const CREATE = 'CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" SET FIELDS {key: "person:alice"} }'

describe('receipts', () => {
  it('verifies a Receipt it issued, and refuses one altered after sealing', async () => {
    const committed = await post('verify', { kip: '2.0', operations: [{ command: CREATE }] })
    const receipt = committed.results[0]?.receipt as unknown as Record<string, unknown>
    expect(receipt).toBeDefined()
    const verified = await post('verify', {
      kip: '2.0',
      operations: [{ command: 'VERIFY RECEIPT :r', parameters: { r: receipt } }],
    })
    const report = verified.results[0]?.result as Record<string, unknown>
    expect(report.valid).toBe(true)
    expect(report.receipt_digest).toBe(receipt.receipt_digest)
    expect(report.attestation).toMatchObject({ checked: true, known: true, matches: true })
    expect(report.signature).toMatchObject({ checked: false })
    // Altered after sealing: the content no longer digests to what it declares.
    const refused = await post('verify', {
      kip: '2.0',
      operations: [
        { command: 'VERIFY RECEIPT :r', parameters: { r: { ...receipt, space_seq: 999 } } },
      ],
    })
    expect(refused.results[0]?.error?.code).toBe('DigestMismatch')
  })

  it('puts a Receipt on every state-changing operation, and none on the envelope', async () => {
    const body = await post('per-operation', {
      kip: '2.0',
      execution: { mode: 'sequence', idempotency_key: 'req-1' },
      operations: [
        { command: CREATE },
        { command: 'FIND(?c.name) WHERE { ?c CONCEPT {} }' },
        { command: 'CREATE CONCEPT ?d { TYPE "Preference" NAME "Dark" }' },
      ],
    })
    expect(body.status).toBe('succeeded')
    // §75.2: each state-changing operation carries its own; a read carries
    // none; the top-level slot exists only in atomic mode.
    expect(body.receipt).toBeUndefined()
    expect(body.results[1]?.receipt).toBeUndefined()
    const [first, , third] = body.results
    const receipt = first?.receipt as KipReceipt
    expect(receipt.status).toBe('committed')
    expect(receipt.space_seq).toBe(1)
    expect(third?.receipt?.space_seq).toBe(2)
    expect(receipt.transaction_class).toBe('cognitive')
    expect(receipt.snapshot_seq).toBe(0)
    expect(receipt.committed_at).toBeDefined()

    // §33.2: the digest is over the Receipt without itself and `proofs`, so
    // a reader can recompute it.
    const { receipt_digest, ...unsigned } = receipt
    expect(receipt_digest).toBe(`sha3-256:${sha3_256Text(canonicalJson(unsigned))}`)

    // §33.2: origin names the Principal and, absent a binding or a
    // delegation, says so with null rather than omitting the members.
    expect(receipt.origin).toEqual({
      principal_id: 'kip:principal:system',
      actor_binding_id: null,
      delegation_digest: null,
    })

    // §81: the idempotency key is echoed back.
    expect(body.execution).toEqual({ mode: 'sequence', on_error: 'stop', idempotency_key: 'req-1' })
  })

  it('scopes one request idempotency key per operation, and still replays a retry', async () => {
    // §34.2 scopes the key by operation endpoint and §73 puts one on the
    // operation. This engine has no atomic batch, so each operation is its own
    // transaction: handing all three the envelope's one key would make the
    // second and third replay the first and commit nothing.
    const request = {
      kip: '2.0',
      execution: { mode: 'sequence', idempotency_key: 'batch-1' },
      operations: [
        { command: 'CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" }' },
        { command: 'CREATE CONCEPT ?b { TYPE "Person" NAME "Bob" }' },
      ],
    }
    const first = await post('batch-key', request)
    expect(first.results.map((r) => r.receipt?.space_seq)).toEqual([1, 2])

    // The same request again is the same two logical intents, so both replay
    // rather than minting a third and fourth Person (§34.3).
    const again = await post('batch-key', request)
    expect(again.results.map((r) => r.receipt?.tx_id)).toEqual(
      first.results.map((r) => r.receipt?.tx_id),
    )
    expect(
      (
        await post('batch-key', {
          kip: '2.0',
          operations: [{ command: 'FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }' }],
        })
      ).results[0]?.result,
    ).toEqual([2])
  })

  it('carries no space_seq on a no_effect Receipt', async () => {
    await post('no-effect', { kip: '2.0', operations: [{ command: CREATE }] })
    const body = await post('no-effect', {
      kip: '2.0',
      operations: [
        {
          command:
            'UPSERT CONCEPT ?c { MATCH {key: "person:alice"} SET FIELDS {name: "Alice"} }',
        },
      ],
    })
    const result = body.results[0]
    expect(result?.status).toBe('no_effect')
    expect(result?.receipt?.status).toBe('no_effect')
    expect(result?.receipt).not.toHaveProperty('space_seq')
    expect(result?.receipt).not.toHaveProperty('committed_at')
    expect(result?.receipt?.receipt_digest).toMatch(/^sha3-256:[0-9a-f]{64}$/)
  })

  it('names the ActorBinding a commit exercised', async () => {
    const stub = env.KIP_TENANT_DB.getByName('receipts-binding')
    await runInDurableObject(stub, (_instance, state) => {
      const store = new Store(state.storage.sql)
      store.governance.ensurePrincipal({ principal_id: TENANT_PRINCIPAL })
      store.governance.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: TENANT_PRINCIPAL,
          actions: ['create', 'read', 'assert'],
        },
        'kip:principal:system',
      )
    })
    const created = await tenantPost('binding', {
      kip: '2.0',
      operations: [
        {
          command: `MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
          }`,
        },
      ],
    })
    expect(created.status).toBe('succeeded')
    // Without a binding, an Assertion attributed to Alice is a recorded
    // attribution, and the Receipt names no binding.
    await runInDurableObject(stub, (_instance, state) => {
      const store = new Store(state.storage.sql)
      store.governance.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: TENANT_PRINCIPAL,
          actions: ['record_attributed_assertion'],
        },
        'kip:principal:system',
      )
    })
    const ASSERT =
      'CREATE ASSERTION ?a { SET FIELDS { proposition: "P-1", asserted_by: "C-1", stance: "support", mode: "stated" } }'
    const recorded = await tenantPost('binding', { kip: '2.0', operations: [{ command: ASSERT }] })
    expect(recorded.results[0]?.receipt?.origin.actor_binding_id).toBeNull()

    // Bound as Alice, the same write speaks as her, and the Receipt says
    // through which binding (§28.3, §33.2).
    let bindingId = 0
    await runInDurableObject(stub, (_instance, state) => {
      const store = new Store(state.storage.sql)
      bindingId = store.governance.createBinding(
        {
          principal_id: TENANT_PRINCIPAL,
          actor_ref: 'C-1',
          binding_class: 'self',
          assurance: 'verified',
        },
        'kip:principal:system',
      ).id
    })
    const spoken = await tenantPost('binding', { kip: '2.0', operations: [{ command: ASSERT }] })
    expect(spoken.results[0]?.receipt?.origin).toEqual({
      principal_id: TENANT_PRINCIPAL,
      actor_binding_id: `kip:binding:${bindingId}`,
      delegation_digest: null,
    })
  })

  it('stops a sequence by default and reports the rest skipped', async () => {
    // §75.2: `on_error` is `stop` when absent.
    const body = await post('on-error-default', {
      kip: '2.0',
      execution: { mode: 'sequence' },
      operations: [
        { command: 'CREATE CONCEPT ?x { TYPE "Spaceship" NAME "Nope" }' },
        { command: CREATE },
      ],
    })
    expect(body.execution?.on_error).toBe('stop')
    expect(body.results.map((r) => r.status)).toEqual(['failed', 'skipped'])
    expect(body.results[1]?.receipt).toBeUndefined()
  })

  it('answers requires from the §67.4 registry and fails fast on an unregistered name', async () => {
    const registry = await post('requires', {
      kip: '2.0',
      requires: {
        serializable_isolation: true,
        idempotency_retention: true,
        historical_reads: true,
        change_stream: true,
        belief_slot: true,
        payload_purge: true,
        list_dependents: true,
        capsule_export: true,
        ingestion_context: true,
        record_outcome_permission: true,
        search_index_freshness: true,
        weighted_projection: false,
        semantic_search: false,
        hybrid_search: false,
        capsule_import: false,
        derive_permission: false,
        signed_receipts: false,
        streaming: false,
      },
      operations: [{ command: 'DESCRIBE CAPABILITIES' }],
    })
    expect(registry.status).toBe('succeeded')
    const report = registry.results[0]?.result as {
      supported: { registry: Record<string, unknown> }
      limits: Record<string, unknown>
      projection: { policies: string[]; leading: string[] }
    }
    // §67.4 fixes the names; both engines report them from `supported.registry`
    // so one client can ask either the same question.
    expect(report.supported.registry.weighted_projection).toBe(false)
    expect(report.supported.registry.change_stream).toBe(true)
    expect(report.supported.registry.semantic_search).toBe(false)
    // An entry that carries a value reports it on the entry, not in `limits`.
    expect(report.supported.registry.idempotency_retention).toEqual({
      unbounded: true,
    })
    expect(report.limits.idempotency_retention).toBeUndefined()
    // §68: what DESCRIBE PROJECTION CAPABILITY used to answer lives here.
    expect(report.projection.policies).toContain('kip:policy:baseline')
    expect(report.projection.leading).toEqual(['support', 'opposition', 'none'])

    // A registry name this engine answers `false` for, required `true`.
    const unmet = await post('requires', {
      kip: '2.0',
      requires: { weighted_projection: true },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    expect(unmet.error?.code).toBe('UnsupportedCapability')
    // A name in neither the registry nor this engine's own list (§67.4).
    const unknown = await post('requires', {
      kip: '2.0',
      requires: { teleportation: false },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    expect(unknown.error?.code).toBe('UnsupportedCapability')
  })

  it('resolves an ingest source actor by type and key, and validates its facets', async () => {
    await post('ingest', { kip: '2.0', operations: [{ command: CREATE }] })
    const byKey = await post('ingest', {
      kip: '2.0',
      ingest: {
        evidence: [
          {
            key: 'msg',
            evidence_class: 'user_statement',
            payload: 'hello',
            source_actor: { type: 'Person', key: 'person:alice' },
          },
        ],
      },
      operations: [
        {
          command:
            'CREATE ACTIVITY ?x { SET FIELDS { activity_class: "extraction" } SET STRUCTURAL { ("inputs", :msg) } }',
        },
      ],
    })
    expect(byKey.status).toBe('succeeded')
    // Minted inside the statement's own transaction (§71.1), which is why the
    // operation is a KML one: the block rides a write, and the command cites
    // the Evidence as `:msg` rather than retyping the observation. Its source
    // is the Concept the `{type, key}` reference names.
    const sources = await post('ingest', {
      kip: '2.0',
      operations: [{ command: 'FIND(?e.source) WHERE { ?e EVIDENCE {} }' }],
    })
    expect(sources.results[0]?.result).toEqual([[{ id: 'C-1' }]])

    // A key nothing carries under that type is refused, existence-neutrally.
    const missing = await post('ingest', {
      kip: '2.0',
      ingest: {
        evidence: [
          {
            key: 'msg',
            evidence_class: 'user_statement',
            payload: 'hello',
            source_actor: { type: 'Person', key: 'person:nobody' },
          },
        ],
      },
      operations: [{ command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Bob" }' }],
    })
    expect(missing.results[0]?.error?.code).toBe('NotFoundOrNotVisible')

    // §71.1: `facets` is validated exactly as SET FACET on CREATE EVIDENCE.
    const graded = await post('ingest', {
      kip: '2.0',
      ingest: {
        evidence: [
          {
            key: 'win',
            evidence_class: 'outcome',
            payload: 'deploy 41 went fine',
            facets: {
              OutcomeRecord: { task_family: 'deploy/pre-flight', outcome_status: 'success', attempt_ref: null, metric: 'completion', window: 'run', terminal: true, observation_key: 'deploy-41', observer_config_digest: 'sha256:' + '0'.repeat(64) },
            },
          },
        ],
      },
      operations: [
        {
          command:
            'CREATE ACTIVITY ?x { SET FIELDS { activity_class: "outcome_observation" } SET STRUCTURAL { ("outputs", :win) } }',
        },
      ],
    })
    expect(graded.status).toBe('succeeded')
    const outcome = await post('ingest', {
      kip: '2.0',
      operations: [
        {
          command:
            'FIND(?e.facets["OutcomeRecord"].outcome_status) WHERE { ?e EVIDENCE {evidence_class: "outcome"} }',
        },
      ],
    })
    expect(outcome.results[0]?.result).toEqual(['success'])

    // A member the closed Facet does not declare, and a required member left
    // out, fail the whole request's transaction.
    const smuggled = await post('ingest', {
      kip: '2.0',
      ingest: {
        evidence: [
          {
            key: 'e',
            evidence_class: 'outcome',
            payload: 'x',
            facets: {
              OutcomeRecord: { task_family: 'f', outcome_status: 'success', verdict: 'promote' },
            },
          },
        ],
      },
      operations: [{ command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Bob" }' }],
    })
    expect(smuggled.results[0]?.error?.code).toBe('ConstraintViolation')
    const incomplete = await post('ingest', {
      kip: '2.0',
      ingest: {
        evidence: [
          {
            key: 'e',
            evidence_class: 'outcome',
            payload: 'x',
            facets: { OutcomeRecord: { task_family: 'f' } },
          },
        ],
      },
      operations: [{ command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Bob" }' }],
    })
    expect(incomplete.results[0]?.error?.code).toBe('ConstraintViolation')
    const unknownFacet = await post('ingest', {
      kip: '2.0',
      ingest: {
        evidence: [{ key: 'e', evidence_class: 'outcome', payload: 'x', facets: { Nope: {} } }],
      },
      operations: [{ command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Bob" }' }],
    })
    expect(unknownFacet.results[0]?.error?.code).toBe('SchemaSymbolNotFound')
    expect(
      (
        await post('ingest', {
          kip: '2.0',
          operations: [{ command: 'FIND(COUNT(?e)) WHERE { ?e EVIDENCE {} }' }],
        })
      ).results[0]?.result,
    ).toEqual([2])
  })

  it('needs record_outcome to ingest an outcome, and to create one', async () => {
    const stub = env.KIP_TENANT_DB.getByName('receipts-outcome')
    await runInDurableObject(stub, (_instance, state) => {
      const store = new Store(state.storage.sql)
      store.governance.ensurePrincipal({ principal_id: TENANT_PRINCIPAL })
      store.governance.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: TENANT_PRINCIPAL,
          actions: ['create', 'read'],
        },
        'kip:principal:system',
      )
    })
    // The ingest block rides the statement's transaction (§71.1), so the
    // operation is a write; what refuses it is the `outcome` class, not the
    // command.
    const ingested = await tenantPost('outcome', {
      kip: '2.0',
      ingest: { evidence: [{ key: 'e', evidence_class: 'outcome', payload: 'x' }] },
      operations: [{ command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Bob" }' }],
    })
    expect(ingested.results[0]?.error?.code).toBe('NotAuthorized')
    const created = await tenantPost('outcome', {
      kip: '2.0',
      operations: [
        { command: 'CREATE EVIDENCE ?e { SET FIELDS { evidence_class: "outcome", payload: "x" } }' },
      ],
    })
    expect(created.results[0]?.error?.code).toBe('NotAuthorized')
    const observed = await tenantPost('outcome', {
      kip: '2.0',
      operations: [
        { command: 'CREATE ACTIVITY ?x { SET FIELDS { activity_class: "outcome_observation" } }' },
      ],
    })
    expect(observed.results[0]?.error?.code).toBe('NotAuthorized')
    // Ordinary Evidence needs only `create`.
    const ordinary = await tenantPost('outcome', {
      kip: '2.0',
      operations: [
        { command: 'CREATE EVIDENCE ?e { SET FIELDS { evidence_class: "observation", payload: "x" } }' },
      ],
    })
    expect(ordinary.status).toBe('succeeded')

    // §29.8: `record_outcome`, and never `derive`, is what the channel needs.
    await runInDurableObject(stub, (_instance, state) => {
      const store = new Store(state.storage.sql)
      store.governance.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: TENANT_PRINCIPAL,
          actions: ['record_outcome'],
        },
        'kip:principal:system',
      )
    })
    const granted = await tenantPost('outcome', {
      kip: '2.0',
      ingest: { evidence: [{ key: 'e', evidence_class: 'outcome', payload: 'x' }] },
      operations: [
        { command: 'CREATE ACTIVITY ?x { SET FIELDS { activity_class: "outcome_observation" } SET STRUCTURAL { ("outputs", :e) } }' },
      ],
    })
    expect(granted.status).toBe('succeeded')
  })
})
