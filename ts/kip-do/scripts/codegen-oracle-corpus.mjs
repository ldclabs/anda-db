#!/usr/bin/env node
/**
 * Builds the differential corpus for `test/parser-oracle.test.ts`.
 *
 * The oracle is only as good as what it is fed, so the corpus is harvested
 * from everything in the repository that already encodes what a KIP command
 * means: the cross-engine conformance fixtures, every KIP literal in the Rust
 * parser's and engine's own tests (which is where the negative cases live —
 * the malformed commands somebody once had to fix a bug for), and the bundled
 * schema capsules.
 *
 * Harvesting beats hand-writing here: a case added to the Rust tests joins the
 * TypeScript oracle on the next regeneration, with nobody remembering to do it.
 *
 * Tests run inside workerd, which has no filesystem, so the result is inlined
 * into TypeScript. Regenerate and commit whenever the Rust tests or the
 * fixtures change.
 */
import { readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const repoRoot = join(pkgRoot, '..', '..')

const STARTS_A_COMMAND =
  /^\s*(FIND|UPSERT|UPDATE|MERGE|DELETE|DESCRIBE|SEARCH|EXPORT)\b/

const commands = new Set()

/** Every `*.rs` file under a directory, recursively. */
function rustFiles(dir) {
  const out = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) out.push(...rustFiles(path))
    else if (entry.name.endsWith('.rs')) out.push(path)
  }
  return out
}

for (const crate of ['anda_kip', 'anda_cognitive_nexus']) {
  for (const file of rustFiles(join(repoRoot, 'rs', crate, 'src'))) {
    const source = readFileSync(file, 'utf8')
    // Raw strings hold the multi-line commands; ordinary literals hold the
    // one-liners, mostly negative cases.
    for (const m of source.matchAll(/r#"([\s\S]*?)"#/g)) {
      if (STARTS_A_COMMAND.test(m[1])) commands.add(m[1].trim())
    }
    for (const m of source.matchAll(/"((?:[^"\\]|\\.)*)"/g)) {
      let text
      try {
        text = JSON.parse(`"${m[1]}"`)
      } catch {
        continue
      }
      if (text.length > 12 && STARTS_A_COMMAND.test(text)) commands.add(text.trim())
    }
  }
}

const capsuleDir = join(repoRoot, 'rs', 'anda_kip', 'capsules')
for (const name of readdirSync(capsuleDir).filter((f) => f.endsWith('.kip'))) {
  commands.add(readFileSync(join(capsuleDir, name), 'utf8').trim())
}

const fixtureDir = join(repoRoot, 'fixtures', 'kip-conformance')
for (const name of readdirSync(fixtureDir).filter((f) => f.endsWith('.json'))) {
  const fixture = JSON.parse(readFileSync(join(fixtureDir, name), 'utf8'))
  for (const setup of fixture.setup ?? []) commands.add(setup.trim())
  for (const testCase of fixture.cases ?? []) {
    if (testCase.command) commands.add(testCase.command.trim())
  }
}

const sorted = [...commands].sort()

const out = `/**
 * Differential corpus — GENERATED FILE, DO NOT EDIT.
 *
 * Regenerate with \`pnpm run codegen:oracle-corpus\`.
 * Sources: rs/anda_kip and rs/anda_cognitive_nexus tests, the bundled
 * capsules, and fixtures/kip-conformance.
 */

const CORPUS: readonly string[] = [
${sorted.map((c) => `  ${JSON.stringify(c)},`).join('\n')}
]

export default CORPUS
`

const target = join(pkgRoot, 'test', 'oracle', 'corpus.generated.ts')
writeFileSync(target, out)
console.log(`wrote ${target} (${sorted.length} commands)`)
