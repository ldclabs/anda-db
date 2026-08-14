#!/usr/bin/env node
/** Inlines the bundled KIP capsules into `src/capsules.generated.ts`. */
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const src = join(pkgRoot, '..', '..', 'rs', 'anda_kip', 'capsules')

// Order is dependency order: Genesis defines `$ConceptType`, the concept-type
// capsules declare types in terms of it, and the predicate capsules come last
// because they name those types in `subject_types` / `object_types`.
//
// `[name, file, anchorType, anchorName]` — the anchor pair is the definition
// the capsule owns, checked for existence to self-heal an interrupted
// bootstrap. Predicate capsules anchor on `$PropositionType`.
const CAPSULES = [
  ['genesis', 'Genesis.kip', '$ConceptType', '$ConceptType'],
  ['person', 'Person.kip', '$ConceptType', 'Person'],
  ['preference', 'Preference.kip', '$ConceptType', 'Preference'],
  ['event', 'Event.kip', '$ConceptType', 'Event'],
  ['sleep_task', 'SleepTask.kip', '$ConceptType', 'SleepTask'],
  ['insight', 'Insight.kip', '$ConceptType', 'Insight'],
  ['commitment', 'Commitment.kip', '$ConceptType', 'Commitment'],
  ['experience', 'Experience.kip', '$ConceptType', 'Experience'],
  ['experience_step', 'ExperienceStep.kip', '$ConceptType', 'ExperienceStep'],
  ['skill', 'Skill.kip', '$ConceptType', 'Skill'],
  ['involves', 'involves.kip', '$PropositionType', 'involves'],
  ['mentions', 'mentions.kip', '$PropositionType', 'mentions'],
  [
    'consolidated_to',
    'consolidated_to.kip',
    '$PropositionType',
    'consolidated_to',
  ],
  ['derived_from', 'derived_from.kip', '$PropositionType', 'derived_from'],
  ['has_step', 'has_step.kip', '$PropositionType', 'has_step'],
  ['caused_by', 'caused_by.kip', '$PropositionType', 'caused_by'],
  [
    'derived_insight',
    'derived_insight.kip',
    '$PropositionType',
    'derived_insight',
  ],
  ['compiled_to', 'compiled_to.kip', '$PropositionType', 'compiled_to'],
]

const entries = CAPSULES.map(([name, file, anchorType, anchorName]) => ({
  name,
  anchorType,
  anchorName,
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
   * Meta-type of the definition this capsule owns — \`$ConceptType\` for type
   * capsules, \`$PropositionType\` for predicate capsules.
   */
  anchorType: string
  /**
   * The definition this capsule owns, used as a self-healing existence check
   * alongside the content hash — a database whose bootstrap was interrupted
   * is repaired even when the hash matches.
   */
  anchorName: string
}

/** Applied in dependency order on first connect. */
export const BUNDLED_CAPSULES: readonly Capsule[] = [
${entries
  .map(
    (e) =>
      `  {\n    name: ${JSON.stringify(e.name)},\n    anchorType: ${JSON.stringify(e.anchorType)},\n    anchorName: ${JSON.stringify(e.anchorName)},\n    source: ${JSON.stringify(e.source)},\n  },`,
  )
  .join('\n')}
]
`

const target = join(pkgRoot, 'src', 'capsules.generated.ts')
writeFileSync(target, out)
const bytes = entries.reduce((n, e) => n + e.source.length, 0)
console.log(`wrote ${target} (${entries.length} capsules, ${bytes} bytes)`)
