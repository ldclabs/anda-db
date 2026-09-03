#!/usr/bin/env node
/**
 * Generates `src/meta/capability-names.generated.ts` from the shared
 * capability vocabulary.
 *
 * The source is `rs/anda_kip/capabilities.json`, which the Rust engine reads
 * through `anda_kip::capability_engine_names()`. Both engines answering from
 * one list is the point: a name only one of them knows is refused as
 * `UnsupportedCapability` by the other even where the capability is built
 * (§67.4), and the caller cannot tell that apart from a real gap. Six names
 * were in exactly that state before this file existed.
 *
 * Only the *names* are shared. Whether a name is supported, and the prose
 * saying why not, stay with each engine: two engines lack a capability for
 * different reasons, and a shared paragraph would flatten that into a
 * uniformity neither of them actually has.
 *
 * Commit the output.
 */
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const source = join(pkgRoot, '..', '..', 'rs', 'anda_kip', 'capabilities.json')

const shared = JSON.parse(readFileSync(source, 'utf8'))
const registry = shared.registry
const engine = shared.engine

if (!Array.isArray(registry) || registry.length === 0) {
  throw new Error(`${source}: no \`registry\` names`)
}
if (!Array.isArray(engine) || engine.length === 0) {
  throw new Error(`${source}: no \`engine\` names`)
}

const list = (names) => names.map((name) => `  '${name}',`).join('\n')

const out = `/**
 * The shared capability vocabulary — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: \`rs/anda_kip/capabilities.json\`, read by the Rust engine
 * through \`anda_kip::capability_engine_names()\`. Regenerate with
 * \`pnpm run codegen:capabilities\`.
 *
 * Membership is a promise to answer, not a claim of support (§67.4): this
 * engine partitions {@link CAPABILITY_ENGINE_NAMES} into what it implements and
 * what it does not, so \`requires\` gets \`true\` or \`false\` for every name
 * rather than the \`unrecognized\` that §67.4 makes a failure.
 */

/** The §67.4 registry names, in the Specification's order. */
export const CAPABILITY_REGISTRY_NAMES: readonly string[] = [
${list(registry)}
]

/** The engine-local names every engine in this repository answers. */
export const CAPABILITY_ENGINE_NAMES: readonly string[] = [
${list(engine)}
]
`

const target = join(pkgRoot, 'src', 'meta', 'capability-names.generated.ts')
writeFileSync(target, out)
console.log(
  `capability-names.generated.ts: ${registry.length} registry names, ${engine.length} engine names`,
)
