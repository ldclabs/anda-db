import {
  KipDatabase,
  mergeRequestContext,
  principalAuth,
  type AuthContext,
  type RequestContext,
  type SchemaPackage,
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

/**
 * The object the conformance suite drives.
 *
 * The suite goes through this object's HTTP surface rather than calling the
 * engine's methods, because the request envelope is part of what a second
 * engine has to reproduce — Space selection, parameter binding, the result and
 * receipt shapes, and how an error reaches the caller. A harness that called
 * `nexus.find` directly would prove the two engines agree about everything
 * except the layer a client actually talks to.
 *
 * The one thing it adds is a way to install a fixture's own vocabulary. A
 * fixture declares the packages its cases need so it does not depend on what
 * some other fixture happened to install, and there is no KML clause that
 * installs a Schema Package — that is a host decision, deliberately out of
 * reach of anything a command could say.
 */
export class ConformanceKipDatabase extends KipDatabase {
  activateFixturePackages(packages: readonly SchemaPackage[]): void {
    if (packages.length === 0) return
    this.nexus.activatePackages([...this.packages(), ...packages])
  }
}

export interface Env {
  KIP_DB: DurableObjectNamespace<TestKipDatabase>
  KIP_TENANT_DB: DurableObjectNamespace<TenantKipDatabase>
  KIP_CONFORMANCE_DB: DurableObjectNamespace<ConformanceKipDatabase>
}

export default {
  async fetch(): Promise<Response> {
    return new Response('kip-do test harness')
  },
}
