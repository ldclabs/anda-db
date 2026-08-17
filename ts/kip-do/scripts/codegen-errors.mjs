#!/usr/bin/env node
/**
 * Generates `src/errors.generated.ts` from the KIP 2.0 Core Error Registry.
 *
 * The registry is `KipErrorCode` in `rs/anda_kip/src/error.rs`, and every entry
 * carries four things an Agent acts on without reading prose: a stable code, a
 * category, a retry class, and a recovery hint. Transcribing 79 of those by
 * hand produces a table that compiles, passes tests, and is quietly wrong — a
 * mismatched `hint` breaks the agent's self-correction loop and a widened
 * `retry` class turns a lost write into a duplicated one, neither with a test
 * to catch it.
 *
 * The source is `anda_kip_wasm::error_catalog()` in the vendored oracle rather
 * than the Rust text: it enumerates `KipErrorCode::ALL`, so a code added to
 * `anda_kip` appears here without anyone remembering to add it, and a code
 * declared but left out of `ALL` cannot slip in either. The WASM module is
 * committed, so regenerating still needs no Rust toolchain — but it is only as
 * current as the last `pnpm run build:oracle-wasm`, which is the same artifact
 * `test/parser-oracle.test.ts` compares the grammar against.
 *
 * Commit the output.
 */
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const vendor = join(pkgRoot, 'vendor', 'anda_kip_wasm')

const wasm = await import(join(vendor, 'anda_kip_wasm.js'))
await wasm.default({
  module_or_path: readFileSync(join(vendor, 'anda_kip_wasm_bg.wasm')),
})

/** @type {{code: string, name: string, category: string, retry: string, hint: string}[]} */
const catalog = JSON.parse(wasm.error_catalog())
if (catalog.length === 0) throw new Error('error_catalog() returned no entries')

const seen = new Set()
for (const entry of catalog) {
  for (const field of ['code', 'name', 'category', 'retry', 'hint']) {
    if (typeof entry[field] !== 'string' || entry[field].length === 0) {
      throw new Error(`catalog entry ${entry.code}: missing ${field}`)
    }
  }
  // Two codes sharing a name would make `KipError.name` ambiguous on the wire.
  if (seen.has(entry.code)) throw new Error(`duplicate code ${entry.code}`)
  seen.add(entry.code)
}

const version = wasm.parser_version()
const categories = [...new Set(catalog.map((e) => e.category))].sort()
const retries = [...new Set(catalog.map((e) => e.retry))].sort()

const lit = (s) => JSON.stringify(s)

const out = `/**
 * The KIP 2.0 Core Error Registry — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: \`KipErrorCode\` in \`rs/anda_kip/src/error.rs\`, read through
 * \`anda_kip_wasm::error_catalog()\`.
 * Regenerate with \`pnpm run codegen:errors\` after changing the Rust registry.
 *
 * Grammar version: ${version}
 */

/** The coarse family an error belongs to (Spec §86.2). */
export type KipErrorCategory =
${categories.map((c) => `  | ${lit(c)}`).join('\n')}

/** What kind of retry, if any, can make progress (Spec §86.3). */
export type KipRetryClass =
${retries.map((r) => `  | ${lit(r)}`).join('\n')}

/**
 * Every registered error code (Spec §87).
 *
 * KIP 2.0 codes are stable *names*, not the numbers 1.x used: an Agent
 * switching on \`EpistemicRevisionRequired\` keeps working across protocol
 * revisions in a way a renumbered \`KIP_3007\` would not.
 */
export type KipErrorCode =
${catalog.map((e) => `  | ${lit(e.code)}`).join('\n')}

export const KIP_ERROR_CODES: readonly KipErrorCode[] = [
${catalog.map((e) => `  ${lit(e.code)},`).join('\n')}
]

/** One registry entry. */
export interface KipErrorSpec {
  category: KipErrorCategory
  retry: KipRetryClass
  /**
   * Agent-facing recovery instruction. This is what makes KIP errors
   * self-correcting; it is part of the wire contract, not a developer comment.
   */
  hint: string
}

export const KIP_ERROR_REGISTRY: Readonly<Record<KipErrorCode, KipErrorSpec>> = {
${catalog
  .map(
    (e) => `  ${lit(e.code)}: {
    category: ${lit(e.category)},
    retry: ${lit(e.retry)},
    hint: ${lit(e.hint)},
  },`,
  )
  .join('\n')}
}
`

const target = join(pkgRoot, 'src', 'errors.generated.ts')
writeFileSync(target, out)
console.log(`wrote ${target} (${catalog.length} codes, grammar ${version})`)
