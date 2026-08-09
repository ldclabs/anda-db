#!/usr/bin/env node
/**
 * Generates `src/errors.generated.ts` from the Rust error taxonomy.
 *
 * The codes, names and agent-facing recovery hints all live in
 * `rs/anda_kip/src/error.rs`. Hand-copying them into TypeScript produces a
 * table that compiles, passes tests, and is quietly wrong — a mismatched
 * `hint` breaks the agent's self-correction loop with nothing to detect it.
 *
 * It reads `error.rs` directly rather than a compiled artifact, so
 * regenerating needs no Rust toolchain. `test/parser-oracle.test.ts` checks the
 * emitted table against the catalog the reference grammar reports at runtime,
 * which is what catches a change this reader is too naive to see.
 *
 * Commit the output.
 */
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const errorRs = join(pkgRoot, '..', '..', 'rs', 'anda_kip', 'src', 'error.rs')
const source = readFileSync(errorRs, 'utf8')

/**
 * Reads one `match self { Self::Variant => <string literal> }` table.
 *
 * Both arm shapes in `error.rs` are accepted: a bare literal, and a braced
 * block holding one literal (rustfmt wraps the long hints that way).
 */
function readTable(fnName) {
  const start = source.indexOf(`pub fn ${fnName}(&self)`)
  if (start === -1) throw new Error(`${errorRs}: no fn ${fnName}`)
  const body = source.slice(start, source.indexOf('\n    }\n', start))

  const table = new Map()
  const arm = /Self::(\w+)\s*=>\s*(\{\s*)?"/g
  let m
  while ((m = arm.exec(body)) !== null) {
    const [literal, end] = readRustString(body, arm.lastIndex - 1)
    table.set(m[1], literal)
    arm.lastIndex = end
  }
  if (table.size === 0) throw new Error(`${errorRs}: fn ${fnName} matched no arms`)
  return table
}

/** Scans a Rust string literal starting at the opening quote. */
function readRustString(text, openQuote) {
  let out = ''
  let i = openQuote + 1
  for (; i < text.length; i++) {
    const ch = text[i]
    if (ch === '\\') {
      const next = text[++i]
      out += next === 'n' ? '\n' : next === 't' ? '\t' : next
      continue
    }
    if (ch === '"') break
    out += ch
  }
  return [out, i + 1]
}

const codes = readTable('code')
const names = readTable('name')
const hints = readTable('hint')

for (const variant of codes.keys()) {
  for (const [what, table] of [['name', names], ['hint', hints]]) {
    if (!table.has(variant)) {
      throw new Error(`${errorRs}: ${variant} has a code but no ${what}`)
    }
  }
}

const catalog = [...codes].map(([variant, code]) => ({
  code,
  name: names.get(variant),
  hint: hints.get(variant),
}))

const version = /^version = "([^"]+)"/m.exec(
  readFileSync(join(pkgRoot, '..', '..', 'rs', 'anda_kip', 'Cargo.toml'), 'utf8'),
)?.[1]
if (!version) throw new Error('anda_kip/Cargo.toml: no version')

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
${catalog.map((e) => `  | ${lit(e.code)}`).join('\n')}

export const KIP_ERROR_CODES: readonly KipErrorCode[] = [
${catalog.map((e) => `  ${lit(e.code)},`).join('\n')}
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
