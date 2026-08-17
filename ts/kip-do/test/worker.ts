import {
  KipDatabase,
  mergeRequestContext,
  principalAuth,
  type AuthContext,
  type RequestContext,
} from '../src/index.js'

/**
 * Test harness object.
 *
 * The real class, not a stand-in: the engine's own tests reach into the object
 * with `runInDurableObject` and build their own `CognitiveNexus`, but the HTTP
 * surface has to be exercised as a host would deploy it.
 */
export class TestKipDatabase extends KipDatabase {}

/**
 * A host that authenticates its callers, as a multi-tenant deployment would.
 *
 * The identity comes from `authenticate` and not from the request body — here
 * a fixed Principal standing in for whatever the real host would observe about
 * the connection. The envelope's context is still merged, because a declared
 * purpose may narrow the session and can never widen it.
 */
export const TENANT_PRINCIPAL = 'kip:principal:tenant'

export class TenantKipDatabase extends KipDatabase {
  protected override authenticate(
    context: RequestContext | undefined,
  ): AuthContext {
    return mergeRequestContext(principalAuth(TENANT_PRINCIPAL), context)
  }
}

export interface Env {
  KIP_DB: DurableObjectNamespace<TestKipDatabase>
  KIP_TENANT_DB: DurableObjectNamespace<TenantKipDatabase>
}

export default {
  async fetch(): Promise<Response> {
    return new Response('kip-do test harness')
  },
}
