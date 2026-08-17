import { env, runInDurableObject, SELF } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import type { KipResponse } from '../src/durable-object.js'
import { Store } from '../src/store/index.js'

/**
 * The HTTP surface a host actually deploys.
 *
 * The status mapping is the part worth testing: a KIP error carries a retry
 * class, and answering with a status that disagrees with it tells a client's
 * recovery policy to do the wrong thing.
 */
async function post(name: string, body: unknown): Promise<Response> {
  const stub = env.KIP_DB.getByName(`do-${name}`)
  return stub.fetch('https://kip.invalid/', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

const request = (...commands: string[]) => ({
  kip: '2.0',
  operations: commands.map((command) => ({ command })),
})

describe('the Durable Object', () => {
  it('activates the bundled profile so a Concept can be typed at all', async () => {
    // A Space that activated nothing resolves Core, and Core declares no
    // Concept types — an object that skipped this would refuse every write
    // with a message about schema rather than about what the caller did.
    const response = await post(
      'bootstrap',
      request('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }'),
    )
    expect(response.status).toBe(200)
    const body = (await response.json()) as KipResponse
    expect(body.kip).toBe('2.0')
    expect(body.results[0]?.receipt?.status).toBe('committed')
  })

  it('runs each language through the surface it belongs to', async () => {
    const response = await post(
      'languages',
      request(
        'CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }',
        'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }',
        'DESCRIBE CAPABILITIES',
      ),
    )
    const body = (await response.json()) as KipResponse
    expect(body.results[0]?.receipt?.status).toBe('committed')
    expect(body.results[1]?.result).toEqual(['Alice'])
    expect((body.results[2]?.result as { kip: string }).kip).toBe('2.0')
  })

  it('reports a partial batch as 207, never as a failure', async () => {
    // The earlier operation has already committed and is durable. Reporting
    // the whole request as a failure invites the client to re-send a write
    // that landed.
    const response = await post(
      'partial',
      request(
        'CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }',
        'CREATE CONCEPT ?x { TYPE "Spaceship" NAME "Enterprise" }',
      ),
    )
    expect(response.status).toBe(207)
    const body = (await response.json()) as KipResponse
    expect(body.results[0]?.receipt?.status).toBe('committed')
    expect(body.results[1]?.error?.code).toBe('SchemaSymbolNotFound')
  })

  it('maps a status from the retry class rather than the error name', async () => {
    const response = await post(
      'status',
      request('CREATE CONCEPT ?x { TYPE "Spaceship" NAME "Enterprise" }'),
    )
    // `requires_different_input` — the request itself has to change.
    expect(response.status).toBe(400)
    const body = (await response.json()) as KipResponse
    expect(body.results[0]?.error?.retry.class).toBe('requires_different_input')
  })

  it('refuses an atomic batch rather than running it as a sequence', async () => {
    // Running them one by one would look like an atomic batch right up until
    // one failed and the earlier writes stayed.
    const response = await post('atomic', {
      kip: '2.0',
      execution: { mode: 'atomic' },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    expect(response.status).toBe(400)
    const body = (await response.json()) as KipResponse
    expect(body.error?.code).toBe('UnsupportedIsolation')
  })

  it('answers a malformed envelope with a code the client can act on', async () => {
    const empty = await post('empty', { kip: '2.0', operations: [] })
    expect(empty.status).toBe(400)
    expect(((await empty.json()) as KipResponse).error?.code).toBe(
      'InvalidRequestEnvelope',
    )

    const stub = env.KIP_DB.getByName('do-notjson')
    const bad = await stub.fetch('https://kip.invalid/', {
      method: 'POST',
      body: 'not json',
    })
    expect(bad.status).toBe(400)

    const wrongMethod = await stub.fetch('https://kip.invalid/')
    expect(wrongMethod.status).toBe(405)
  })

  it('is reachable through the Worker the host binds', async () => {
    const response = await SELF.fetch('https://kip.invalid/')
    expect(await response.text()).toBe('kip-do test harness')
  })
})

describe('a host that authenticates its callers', () => {
  async function tenantPost(name: string, body: unknown): Promise<Response> {
    const stub = env.KIP_TENANT_DB.getByName(`tenant-${name}`)
    return stub.fetch('https://kip.invalid/', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  }

  /** Reaches into the object's control plane, as a host's admin API would. */
  async function withGovernance(
    name: string,
    body: (store: Store) => void,
  ): Promise<void> {
    const stub = env.KIP_TENANT_DB.getByName(`tenant-${name}`)
    await runInDurableObject(stub, (_instance, state) => {
      body(new Store(state.storage.sql))
    })
  }

  it('refuses a caller the control plane has never heard of', async () => {
    // The object bootstrapped fine and the command is valid. What is missing is
    // the Principal, and a host naming an unregistered identity has a
    // configuration bug — reported as an authentication failure rather than
    // resolved to "a caller with no Grants", which would look like policy.
    const response = await tenantPost('unknown', request('DESCRIBE PRIMER'))
    const body = (await response.json()) as KipResponse
    expect(body.results[0]?.error?.code).toBe('Unauthenticated')
    expect(response.status).toBe(403)
  })

  it('gives an authenticated caller exactly what its Grants say', async () => {
    await withGovernance('granted', (store) => {
      store.governance.ensurePrincipal({ principal_id: 'kip:principal:tenant' })
      store.governance.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: 'kip:principal:tenant',
          actions: ['discover', 'read'],
        },
        'kip:principal:system',
      )
    })

    const allowed = await tenantPost(
      'granted',
      request('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }'),
    )
    expect(allowed.status).toBe(200)

    // …and nothing beside them. A `requires_authority` failure is 403, not 400:
    // the request was fine, the caller was not.
    const refused = await tenantPost(
      'granted',
      request('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }'),
    )
    expect(refused.status).toBe(403)
    const body = (await refused.json()) as KipResponse
    expect(body.results[0]?.error?.code).toBe('NotAuthorized')
    expect(body.results[0]?.error?.retry.class).toBe('requires_authority')
  })

  it('does not let the request body name the Principal', async () => {
    await withGovernance('injection', (store) => {
      store.governance.ensurePrincipal({ principal_id: 'kip:principal:tenant' })
      store.governance.ensurePrincipal({ principal_id: 'kip:principal:admin' })
      store.governance.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: 'kip:principal:admin',
          actions: ['create', 'read', 'discover'],
        },
        'kip:principal:system',
      )
    })

    // An Agent under prompt injection controls the envelope. It does not
    // control who it is: identity comes from what the host observed.
    const response = await tenantPost('injection', {
      kip: '2.0',
      context: { purpose: 'anything', principal_id: 'kip:principal:admin' },
      operations: [{ command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Mallory" }' }],
    })
    expect(response.status).toBe(403)
    const body = (await response.json()) as KipResponse
    expect(body.results[0]?.error?.code).toBe('NotAuthorized')
  })
})
