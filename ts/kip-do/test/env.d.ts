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
