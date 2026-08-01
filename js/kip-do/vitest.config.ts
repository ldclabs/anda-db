import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'

// Tests run inside workerd, not Node: the engine's whole contract is the
// Durable Object SQLite API (transactionSync, FTS5, the 100-parameter limit),
// and none of that is reproducible against better-sqlite3. A green test here
// means green on Cloudflare.
export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        // Isolated storage snapshots and rolls back per test. The conformance
        // fixtures run their cases in order against one accumulating database
        // (a KML case sets up state a later KQL case queries), which that
        // rollback would silently undo. Isolation is instead provided by
        // giving every fixture and every test its own Durable Object name.
        isolatedStorage: false,
        wrangler: { configPath: './test/wrangler.jsonc' },
        miniflare: {
          compatibilityDate: '2025-01-01',
          compatibilityFlags: ['nodejs_compat'],
        },
      },
    },
  },
})
