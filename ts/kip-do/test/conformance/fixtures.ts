/**
 * Bundles the shared conformance fixtures into the test worker.
 *
 * Tests execute inside workerd, which has no filesystem, so the JSON files
 * cannot be read at runtime the way the Rust runner reads them. Vite's
 * `import.meta.glob` inlines them at build time instead, from the same
 * directory the Rust runner reads — there is exactly one copy of each fixture.
 */

import type { Fixture } from './normalize.js'

const modules = import.meta.glob<{ default: Fixture }>(
  '../../../../fixtures/kip-conformance/*.json',
  { eager: true },
)

const fixtures: Fixture[] = Object.entries(modules)
  // Sort by path so the suite reports in a stable order regardless of how the
  // bundler enumerated the directory.
  .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
  .map(([, module]) => module.default)

export default fixtures
