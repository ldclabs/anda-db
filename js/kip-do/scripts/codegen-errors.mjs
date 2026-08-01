#!/usr/bin/env node
/**
 * Generates `src/errors.generated.ts` from the Rust error taxonomy.
 *
 * The codes, names and agent-facing recovery hints all live in
 * `rs/anda_kip/src/error.rs`. Hand-copying them into TypeScript produces a
 * table that compiles, passes tests, and is quietly wrong — a mismatched
 * `hint` breaks the agent's self-correction loop with nothing to detect it.
 * So we read them out of the compiled WASM module instead.
 *
 * Run after `build:wasm`, and commit the output.
 */
import { writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createRequire } from 'node:module'
import { execFileSync, } from 'node:child_process'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const crateDir = join(pkgRoot, '..', '..', 'rs', 'anda_kip_wasm')

// The vendored module targets `bundler`, which Node cannot import directly.
// Build a throwaway `nodejs` copy purely to read the catalog out of it.
const scratch = mkdtempSync(join(tmpdir(), 'kip-errors-'))
execFileSync(
  'wasm-pack',
  [
    'build',
    crateDir,
    '--target',
    'nodejs',
    '--release',
    '--out-dir',
    scratch,
    '--out-name',
    'anda_kip_wasm',
  ],
  { stdio: 'inherit' },
)

const require = createRequire(import.meta.url)
const wasm = require(join(scratch, 'anda_kip_wasm.js'))
const catalog = JSON.parse(wasm.error_catalog())
const version = wasm.parser_version()

const codes = catalog.map((e) => e.code)
const lit = (s) => JSON.stringify(s)

const out = `/**
 * KIP error taxonomy — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: \`rs/anda_kip/src/error.rs\`.
 * Regenerate with \`pnpm run codegen:errors\` after changing the Rust enum.
 *
 * Grammar version: ${version}
 */

/** Every KIP error code, in the order the Rust enum declares them. */
export type KipErrorCode =
${codes.map((c) => `  | ${lit(c)}`).join('\n')}

export const KIP_ERROR_CODES: readonly KipErrorCode[] = [
${codes.map((c) => `  ${lit(c)},`).join('\n')}
]

/** Stable error name, e.g. \`"InvalidSyntax"\` for \`KIP_1001\`. */
export const KIP_ERROR_NAMES: Readonly<Record<KipErrorCode, string>> = {
${catalog.map((e) => `  ${e.code}: ${lit(e.name)},`).join('\n')}
}

/**
 * Agent-facing recovery hint. This is what makes KIP errors self-correcting;
 * it is part of the wire contract, not a developer comment.
 */
export const KIP_ERROR_HINTS: Readonly<Record<KipErrorCode, string>> = {
${catalog.map((e) => `  ${e.code}: ${lit(e.hint)},`).join('\n')}
}
`

const target = join(pkgRoot, 'src', 'errors.generated.ts')
writeFileSync(target, out)
console.log(`wrote ${target} (${catalog.length} codes, grammar ${version})`)
