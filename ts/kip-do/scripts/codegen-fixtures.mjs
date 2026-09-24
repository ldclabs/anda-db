#!/usr/bin/env node
/**
 * Inlines the cross-engine conformance fixtures into TypeScript.
 *
 * The fixtures in `fixtures/kip-conformance-2.0/` are KIP's engine suite
 * (`conformance/engine-suite/`, copied byte for byte by `make
 * sync-kip-conformance`), plain data so that both engines run the same cases:
 * the Rust harness reads them from disk, and this one cannot — tests run
 * inside workerd, which has no filesystem. Inlining is the only way to hand
 * workerd the same bytes. `manifest.json` is provenance, not a fixture.
 *
 * The generated module is committed. Regenerate whenever a fixture changes;
 * `test/conformance.test.ts` asserts the case count so a silent shrink shows up.
 */
import { readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const dir = join(pkgRoot, '..', '..', 'fixtures', 'kip-conformance-2.0')

const fixtures = readdirSync(dir)
  .filter((name) => name.endsWith('.json') && name !== 'manifest.json')
  .sort()
  .map((name) => JSON.parse(readFileSync(join(dir, name), 'utf8')))

const cases = fixtures.reduce((total, f) => total + (f.cases?.length ?? 0), 0)

const out = `/**
 * The KIP 2.0 cross-engine conformance fixtures — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: \`fixtures/kip-conformance-2.0/*.json\`, which the Rust
 * engine's \`tests/conformance.rs\` reads from disk. Regenerate with
 * \`pnpm run codegen:fixtures\`.
 */

/**
 * One expectation: a result to match, members and rows the result must
 * contain, or the registry code to fail with. An empty expectation passes on
 * any result.
 */
export interface Expectation {
  result?: unknown
  result_contains?: unknown
  error?: string
}

export interface Case {
  name: string
  command: string
  params?: Record<string, unknown>
  expect: Expectation
  /** Whether the order of a top-level result array is part of the contract. */
  ordered?: boolean
  /**
   * Extra request-envelope members, merged over the ones the harness builds.
   *
   * Most behaviour is decided by the command, but some of it is decided by the
   * envelope around the command — ingest, execution.idempotency_key — and
   * those are cross-engine contracts too.
   */
  envelope?: Record<string, unknown>
  /**
   * The normative conformance vectors this case pins, by their §27 short names
   * (CORE-001, KML-031, …) — the ones §102's invariant registry names. Read by
   * the Rust harness's coverage report; the
   * TypeScript harness carries them so the two run the same fixture file.
   */
  vectors?: string[]
}

/**
 * A setup step: a bare command, or one whose raw result is captured into
 * parameters (JSON Pointers) for later steps and every case of the fixture.
 */
export type Setup =
  | string
  | { command: string; params?: Record<string, unknown>; capture?: Record<string, string> }

export interface Fixture {
  name: string
  description: string
  /** \`pending_engine\` while no engine has verified the fixture. */
  status?: string
  /** Extra Schema Package artifacts to install and activate, inline. */
  packages?: unknown[]
  setup?: Setup[]
  cases: Case[]
}

export const FIXTURES: readonly Fixture[] = ${JSON.stringify(fixtures, null, 2)} as unknown as Fixture[]

/** The total number of cases, so a silent shrink is visible. */
export const CASE_COUNT = ${cases}
`

const target = join(pkgRoot, 'test', 'conformance', 'fixtures.generated.ts')
writeFileSync(target, out)
console.log(
  `wrote ${target} (${fixtures.length} fixtures, ${cases} cases)`,
)
