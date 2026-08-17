/**
 * Ambient declarations the test suite needs but the compiler cannot infer.
 */

declare global {
  /**
   * `cloudflare:test` types the `env` it hands a test as `Cloudflare.Env`,
   * which `@cloudflare/workers-types` declares empty until a project says what
   * its bindings are. Declaring it here is what makes `env.KIP_DB` a typed
   * Durable Object namespace rather than an error.
   *
   * Test calls go through a shallow `executeTestKip` helper. That keeps this
   * binding faithful to the real Durable Object while avoiding expansion of
   * Cloudflare's recursive RPC mapped type over the engine's whole method
   * surface at every call site.
   */
  namespace Cloudflare {
    interface Env {
      KIP_DB: DurableObjectNamespace<import('./worker.js').TestKipDatabase>
      /**
       * A second object bound to a subclass that authenticates its callers, so
       * the multi-tenant path is exercised as a host would deploy it rather
       * than simulated by constructing a Session in-process.
       */
      KIP_TENANT_DB: DurableObjectNamespace<
        import('./worker.js').TenantKipDatabase
      >
      /**
       * The object the conformance suite drives through its HTTP surface, so
       * the shared fixtures exercise the request envelope rather than the
       * engine's methods.
       */
      KIP_CONFORMANCE_DB: DurableObjectNamespace<
        import('./worker.js').ConformanceKipDatabase
      >
    }
  }

  /**
   * `import.meta.glob` is Vite's, not TypeScript's. Declaring the one shape
   * `conformance/fixtures.ts` uses avoids adding `vite/client` to `types`,
   * which would mean depending on Vite directly just for an ambient file.
   */
  interface ImportMeta {
    glob<T>(pattern: string, options: { eager: true }): Record<string, T>
  }
}

export {}
