#!/usr/bin/env node
/** Inlines the bundled KIP capsules into `src/capsules.generated.ts`. */
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const src = join(pkgRoot, '..', '..', 'rs', 'anda_kip', 'capsules')

// Order is dependency order: Genesis defines `$ConceptType`, everything else
// declares types in terms of it.
const CAPSULES = [
  ['genesis', 'Genesis.kip', '$ConceptType'],
  ['person', 'Person.kip', 'Person'],
  ['preference', 'Preference.kip', 'Preference'],
  ['event', 'Event.kip', 'Event'],
  ['sleep_task', 'SleepTask.kip', 'SleepTask'],
  ['insight', 'Insight.kip', 'Insight'],
  ['commitment', 'Commitment.kip', 'Commitment'],
]

const entries = CAPSULES.map(([name, file, anchor]) => ({
  name,
  anchor,
  source: readFileSync(join(src, file), 'utf8'),
}))

const out = `/**
 * Bundled KIP bootstrap capsules — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: \`rs/anda_kip/capsules/*.kip\` — the same files the Rust
 * engine bundles. Regenerate with \`pnpm run codegen:capsules\`.
 *
 * KIP is schema-first: a concept type or predicate must be declared as a
 * \`$ConceptType\` / \`$PropositionType\` concept before anything may use it.
 * These capsules declare the base schema, starting with Genesis, which defines
 * \`$ConceptType\` by creating an instance of it that describes itself.
 *
 * \`persons/self.kip\` and \`persons/system.kip\` are deliberately not bundled:
 * \`$self\` attributes evolve with the agent and must never be reset to a
 * template by a re-applied capsule. Applications apply those themselves.
 */

export interface Capsule {
  /** Keys the persisted content hash. */
  name: string
  /** KIP source; may contain several statements. */
  source: string
  /**
   * The \`$ConceptType\` this capsule owns, used as a self-healing existence
   * check alongside the content hash — a database whose bootstrap was
   * interrupted is repaired even when the hash matches.
   */
  anchor: string
}

/** Applied in dependency order on first connect. */
export const BUNDLED_CAPSULES: readonly Capsule[] = [
${entries
  .map(
    (e) =>
      `  {\n    name: ${JSON.stringify(e.name)},\n    anchor: ${JSON.stringify(e.anchor)},\n    source: ${JSON.stringify(e.source)},\n  },`,
  )
  .join('\n')}
]
`

const target = join(pkgRoot, 'src', 'capsules.generated.ts')
writeFileSync(target, out)
const bytes = entries.reduce((n, e) => n + e.source.length, 0)
console.log(`wrote ${target} (${entries.length} capsules, ${bytes} bytes)`)
