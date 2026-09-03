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
  // §75: a multi-operation request declares how its operations relate, so the
  // helper says `independent` rather than letting the object guess — which it
  // no longer does.
  ...(commands.length > 1
    ? { execution: { mode: 'independent' as const } }
    : {}),
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
    expect(body.results[0]?.status).toBe('succeeded')
    // §75: the operation's own Receipt; the top-level slot exists only in
    // atomic mode, which this engine has no.
    expect(body.results[0]?.receipt?.status).toBe('committed')
    expect(body.receipt).toBeUndefined()
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
    expect(body.results[0]?.status).toBe('succeeded')
    expect(body.results[0]?.receipt?.status).toBe('committed')
    expect(body.results[1]?.receipt).toBeUndefined()
    expect(body.results[1]?.result).toEqual(['Alice'])
    expect((body.results[2]?.result as { kip: string }).kip).toBe('2.0')
  })

  it('returns the coordinate a read answered at, and the cursor to continue it', async () => {
    // §50 and §44.8: a cursor is opaque, so the only way a caller can page a
    // `FIND` is to be handed the token — an object that computed one and kept
    // it made `CURSOR` unreachable over its own HTTP surface.
    const response = await post(
      'paging',
      request(
        'MUTATE { CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" } ' +
          'CREATE CONCEPT ?b { TYPE "Person" NAME "Bob" } ' +
          'CREATE CONCEPT ?c { TYPE "Person" NAME "Carol" } }',
        'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.name LIMIT 2',
      ),
    )
    const body = (await response.json()) as KipResponse
    const page = body.results[1]
    expect(page?.result).toEqual(['Alice', 'Bob'])
    expect(page?.context?.snapshot_seq).toBeGreaterThan(0)
    expect(page?.next_cursor).toBeDefined()

    const next = await post(
      'paging',
      request(
        'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.name ' +
          `LIMIT 2 CURSOR "${page?.next_cursor}"`,
      ),
    )
    const rest = (await next.json()) as KipResponse
    expect(rest.results[0]?.result).toEqual(['Carol'])
    expect(rest.results[0]?.next_cursor).toBeUndefined()
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
    expect(body.results[0]?.status).toBe('succeeded')
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
  it('answers in the envelope §81 fixes, not a shape of its own', async () => {
    // The normative response schema makes `status` required at both levels.
    // A consumer that read "no error" as "committed" would count a
    // `no_effect` — a transaction that changed nothing — as a write.
    const response = await post('envelope', {
      kip: '2.0',
      request_id: 'req-42',
      operations: [
        { op_id: 'op-1', command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Ann" }' },
      ],
    })
    const body = (await response.json()) as KipResponse
    expect(body.kip).toBe('2.0')
    expect(body.request_id).toBe('req-42')
    expect(body.status).toBe('succeeded')
    expect(body.results[0]?.op_id).toBe('op-1')
    expect(body.results[0]?.status).toBe('succeeded')
    expect(body.context?.space_id).toBe('kip:space:default')
    // §75: every state-changing operation carries its own Receipt, in the
    // shape §33.2 fixes; the rest of the outcome is namespaced.
    expect(body.results[0]?.receipt?.status).toBe('committed')
    expect(body.results[0]?.receipt?.transaction_class).toBe('cognitive')
    expect(
      body.results[0]?.extensions?.['kip-do/outcome']?.handles,
    ).toBeDefined()
  })

  it('refuses a request that declares another protocol version', async () => {
    // §87.1. Executing it anyway is the failure this exists for: the caller
    // believes it is talking to the version it named.
    const response = await post('version', {
      kip: '1.0',
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    expect(response.status).toBe(400)
    const body = (await response.json()) as KipResponse
    expect(body.status).toBe('failed')
    expect(body.error?.code).toBe('UnsupportedProtocolVersion')
  })

  it('makes a multi-operation request say how its operations relate', async () => {
    // §75: whether earlier commits survive a later failure is not a detail to
    // leave to an engine default.
    const response = await post('mode', {
      kip: '2.0',
      operations: [
        { command: 'DESCRIBE PROTOCOL' },
        { command: 'DESCRIBE CAPABILITIES' },
      ],
    })
    expect(response.status).toBe(400)
    const body = (await response.json()) as KipResponse
    expect(body.error?.code).toBe('InvalidRequestEnvelope')
  })

  it('stops a sequence after a failure, and says the rest was skipped', async () => {
    const response = await post('sequence', {
      kip: '2.0',
      execution: { mode: 'sequence', on_error: 'stop' },
      operations: [
        { command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Ann" }' },
        { command: 'CREATE CONCEPT ?x { TYPE "Spaceship" NAME "Enterprise" }' },
        { command: 'CREATE CONCEPT ?d { TYPE "Person" NAME "Bo" }' },
      ],
    })
    const body = (await response.json()) as KipResponse
    // §75.2: the earlier commit stays durable, so the request is `partial` and
    // not `failed` — telling the client it all failed invites a re-send.
    expect(body.status).toBe('partial')
    expect(body.results.map((r) => r.status)).toEqual([
      'succeeded',
      'failed',
      'skipped',
    ])
  })

  it('will not silently satisfy a request for stronger isolation', async () => {
    // §32.2. Echoing the field back while providing something weaker is the
    // one thing the clause forbids.
    const response = await post('isolation', {
      kip: '2.0',
      execution: { mode: 'independent', isolation: 'linearizable' },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    const body = (await response.json()) as KipResponse
    expect(body.error?.code).toBe('UnsupportedIsolation')
  })

  it('honours envelope preconditions and top-level parameters', async () => {
    // §35.4: a precondition the engine ignored is a guard the caller believes
    // it set.
    const stale = await post('preconditions', {
      kip: '2.0',
      preconditions: { space_seq: 9999 },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    expect(stale.status).toBe(409)
    expect(((await stale.json()) as KipResponse).error?.code).toBe(
      'PreconditionFailed',
    )

    // §74: a top-level `parameters` block is the request's binding
    // environment, and an operation's own block narrows it.
    const bound = await post('preconditions', {
      kip: '2.0',
      parameters: { who: 'Cass' },
      operations: [
        { command: 'CREATE CONCEPT ?c { TYPE "Person" NAME :who }' },
        { command: 'FIND(?c.name) WHERE { ?c CONCEPT {name: :who} }' },
      ],
      execution: { mode: 'sequence' },
    })
    const body = (await bound.json()) as KipResponse
    expect(body.results[1]?.result).toEqual(['Cass'])
  })

  it('mints ingested Evidence from the envelope, not from command text', async () => {
    // §71.1, and §88.12 is the reason: a model retyping an observation into
    // command text truncates it, normalizes its whitespace or paraphrases it,
    // and the record then says the source said something it did not. So the
    // payload rides the envelope and the command only cites `:msg`.
    const observed = 'I prefer   dark mode.\nAnd my address is 12 Elm Street.'
    const response = await post('ingest', {
      kip: '2.0',
      ingest: {
        evidence: [
          {
            key: 'msg',
            evidence_class: 'user_statement',
            payload: observed,
            media_type: 'text/plain',
          },
        ],
      },
      operations: [
        {
          command: `MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?a {
              SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "observed" }
              SET STRUCTURAL { ("evidence", :msg) {role: "support"} }
            }
          }`,
        },
      ],
    })
    const body = (await response.json()) as KipResponse
    expect(body.status).toBe('succeeded')

    // Byte for byte from the transport, whitespace and all.
    const read = await post('ingest', {
      kip: '2.0',
      operations: [{ command: 'FIND(?e.payload.inline) WHERE { ?e EVIDENCE {} }' }],
    })
    expect(((await read.json()) as KipResponse).results[0]?.result).toEqual([
      observed,
    ])
  })

  it('takes the ingested Evidence with the statement that failed', async () => {
    // Minted inside the statement's own transaction, so a Space never
    // accumulates observations whose claims were never recorded — Evidence for
    // nothing, indistinguishable later from an observation somebody chose not
    // to act on.
    const response = await post('ingest-abort', {
      kip: '2.0',
      ingest: {
        evidence: [{ key: 'msg', evidence_class: 'user_statement', payload: 'hi' }],
      },
      operations: [
        {
          command: `MUTATE {
            CREATE CONCEPT ?c { TYPE "Spaceship" NAME "Nope" }
            CREATE ASSERTION ?a {
              SET FIELDS { proposition: "P-1", stance: "support", mode: "observed" }
              SET STRUCTURAL { ("evidence", :msg) }
            }
          }`,
        },
      ],
    })
    expect(((await response.json()) as KipResponse).status).toBe('failed')

    const read = await post('ingest-abort', {
      kip: '2.0',
      operations: [{ command: 'FIND(?e) WHERE { ?e EVIDENCE {} }' }],
    })
    expect(((await read.json()) as KipResponse).results[0]?.result).toEqual([])
  })

  it('refuses an ingest key a request parameter already claims', async () => {
    // One is a caller-supplied value and the other is an element this request
    // created; a command citing `:msg` could mean either, and the two cannot
    // be reconciled.
    const response = await post('ingest-clash', {
      kip: '2.0',
      parameters: { msg: 'a plain value' },
      ingest: {
        evidence: [{ key: 'msg', evidence_class: 'user_statement', payload: 'hi' }],
      },
      operations: [{ command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }' }],
    })
    const body = (await response.json()) as KipResponse
    expect(body.results[0]?.error?.code).toBe('InvalidRequestEnvelope')
  })

  it('refuses an artifact handle rather than minting an empty record under it', async () => {
    // §85.2: a handle would name bytes this engine cannot read, and an Evidence
    // record with an empty payload under one is exactly the fabrication the
    // mechanism exists to prevent.
    const response = await post('ingest-artifact', {
      kip: '2.0',
      ingest: {
        evidence: [
          {
            key: 'msg',
            evidence_class: 'user_statement',
            payload_artifact: 'artifact:whatever',
          },
        ],
      },
      operations: [{ command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }' }],
    })
    const body = (await response.json()) as KipResponse
    expect(body.results[0]?.error?.code).toBe('UnsupportedCapability')
  })

  it('checks an ingest block before any operation of the batch runs', async () => {
    // The block decides what every operation can cite, so discovering it
    // malformed after the first statement committed would leave durable writes
    // behind a request that was never valid.
    const response = await post('ingest-shape', {
      kip: '2.0',
      ingest: { evidence: [{ key: 'msg', evidence_class: '', payload: 'hi' }] },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    const body = (await response.json()) as KipResponse
    expect(body.error?.code).toBe('InvalidRequestEnvelope')
  })

  it('fails fast on a capability requirement it cannot meet', async () => {
    // §67. A requirement nobody recognized must not pass: the caller believes
    // the check ran.
    const unknown = await post('requires', {
      kip: '2.0',
      requires: { read_everything: true },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    expect(((await unknown.json()) as KipResponse).error?.code).toBe(
      'UnsupportedCapability',
    )

    const known = await post('requires', {
      kip: '2.0',
      requires: { keyword_search: true, semantic_search: false },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    expect(((await known.json()) as KipResponse).status).toBe('succeeded')
  })
  it('is failed, not partial, when nothing succeeded', async () => {
    // §82: `partial` says some writes landed. A sequence that fails on its
    // first operation lands none, and reporting partial is what stops a client
    // making the retry it should make.
    const response = await post('nothing-succeeded', {
      kip: '2.0',
      execution: { mode: 'sequence', on_error: 'stop' },
      operations: [
        { command: 'CREATE CONCEPT ?x { TYPE "Spaceship" NAME "Enterprise" }' },
        { command: 'CREATE CONCEPT ?c { TYPE "Person" NAME "Ann" }' },
      ],
    })
    const body = (await response.json()) as KipResponse
    expect(body.results.map((r) => r.status)).toEqual(['failed', 'skipped'])
    expect(body.status).toBe('failed')
    expect(response.status).not.toBe(207)
  })

  it('refuses an on_error it does not recognize rather than defaulting', async () => {
    // Falling through to the default would mean `continue`: a sequence meant to
    // stop would commit the writes the caller asked to have skipped.
    const response = await post('on-error', {
      kip: '2.0',
      execution: { mode: 'sequence', on_error: 'halt' },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    const body = (await response.json()) as KipResponse
    expect(body.error?.code).toBe('InvalidRequestEnvelope')
  })

  it('requires a request to say which protocol version it speaks', async () => {
    // `kip` is in the request schema's `required` list. Running a request that
    // named no version is the same silent mismatch as running one that named
    // the wrong one, minus the evidence.
    const response = await post('no-version', {
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    const body = (await response.json()) as KipResponse
    expect(body.error?.code).toBe('UnsupportedProtocolVersion')
  })

  it('answers a requires check about a capability it does implement', async () => {
    // A name the engine supports but forgot to register reads as "unrecognized"
    // and fails the request — the opposite of what the fail-fast check is for.
    const response = await post('requires-supported', {
      kip: '2.0',
      requires: { snapshot_token: true, ingest: true },
      operations: [{ command: 'DESCRIBE PROTOCOL' }],
    })
    expect(((await response.json()) as KipResponse).status).toBe('succeeded')
  })
})
