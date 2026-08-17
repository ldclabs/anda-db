#!/usr/bin/env node
/**
 * Inlines the cross-engine conformance fixtures into TypeScript.
 *
 * The fixtures in `fixtures/kip-conformance-2.0/` are plain data specifically
 * so that both engines run the same cases: the Rust harness reads them from
 * disk, and this one cannot — tests run inside workerd, which has no
 * filesystem. Inlining is the only way to hand workerd the same bytes.
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
  .filter((name) => name.endsWith('.json'))
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

/** One expectation: a result to match, or the registry code to fail with. */
export interface Expectation {
  result?: unknown
  error?: string
}

export interface Case {
  name: string
  command: string
  params?: Record<string, unknown>
  expect: Expectation
  /** Whether the order of a top-level result array is part of the contract. */
  ordered?: boolean
}

export interface Fixture {
  name: string
  description: string
  /** Extra Schema Package artifacts to install and activate, inline. */
  packages?: unknown[]
  setup?: string[]
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
