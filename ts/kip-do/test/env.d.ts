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
   * Known gap: `tsc` reports TS2589 ("type instantiation is excessively deep")
   * at each `getByName` call. The RPC mapped type behind
   * `DurableObjectStub<TestKipDatabase>` expands over the engine's whole
   * method surface and exceeds the compiler's depth limit under TypeScript 7
   * and `@cloudflare/workers-types` 5. Narrowing the binding to the one method
   * the tests call silences it, at the cost of the stub no longer being the
   * type it actually is — not worth it. `vitest` type-strips rather than
   * type-checks, so the suite runs unaffected.
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
