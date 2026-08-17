#!/usr/bin/env node
/**
 * Vendors the bundled Schema Package artifacts into TypeScript.
 *
 * The KIP Cognitive Memory Profile is a first-class asset of both engines: the
 * Rust one ships it as `rs/anda_cognitive_nexus/profiles/`, and this reads
 * exactly those bytes. Two engines are only interoperable if they mean the same
 * thing by `Preference`, and they only do that if they resolve the same
 * artifact — so this copies rather than transcribes. A hand-maintained
 * TypeScript version would drift toward whatever this engine happens to
 * support, which is the one thing a profile must not do.
 *
 * The result is inlined into a module rather than imported as JSON because the
 * build is plain `tsc` with no bundler: a `.json` import would typecheck and
 * then be missing from `dist/`.
 *
 * Commit the output.
 */
import { readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = dirname(here)
const source = join(pkgRoot, '..', '..', 'rs', 'anda_cognitive_nexus', 'profiles')

const artifacts = readdirSync(source)
  .filter((name) => name.endsWith('.json'))
  .sort()
  .map((name) => {
    const artifact = JSON.parse(readFileSync(join(source, name), 'utf8'))
    const id = artifact.manifest?.package_id
    const version = artifact.manifest?.version
    if (!id || !version) {
      throw new Error(`${name}: the manifest declares no package_id/version`)
    }
    return { name, artifact, id, version }
  })

if (artifacts.length === 0) throw new Error(`${source}: no artifacts found`)

/**
 * `kip://profiles/cognitive-memory` → `COGNITIVE_MEMORY`.
 *
 * The last path segment, which is also what the Rust crate calls it — the two
 * are read side by side often enough that the names should match.
 */
const constantName = (id) =>
  id
    .split('/')
    .pop()
    .replace(/[^A-Za-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toUpperCase()

const names = artifacts.map(({ id }) => constantName(id))
if (new Set(names).size !== names.length) {
  // Two packages whose paths end in the same segment would silently overwrite
  // each other's export.
  throw new Error(`two artifacts share a constant name: ${names.join(', ')}`)
}

const bodies = artifacts.map(({ name, artifact, id, version }) => {
  const constant = constantName(id)
  return `/**
 * ${artifact.manifest?.name ?? id}, version ${version}.
 *
 * Vendored verbatim from \`rs/anda_cognitive_nexus/profiles/${name}\`.
 */
export const ${constant}: SchemaPackage = ${JSON.stringify(artifact, null, 2)} as SchemaPackage

/** The package id ${constant} declares. */
export const ${constant}_ID = ${JSON.stringify(id)}

/** The version ${constant} declares. */
export const ${constant}_VERSION = ${JSON.stringify(version)}
`
})

const out = `/**
 * Bundled Schema Package artifacts — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: \`rs/anda_cognitive_nexus/profiles/*.json\`, which are
 * themselves vendored from the specification repository. Regenerate with
 * \`pnpm run codegen:profiles\`.
 *
 * Installing is not activating (§240.18). Bundling the bytes says nothing about
 * which Space may resolve symbols through them.
 */

import type { SchemaPackage } from './package.js'

${bodies.join('\n')}
/** Every bundled artifact, in package-id order. */
export const BUNDLED_PACKAGES: readonly SchemaPackage[] = [
${artifacts.map(({ id }) => `  ${constantName(id)},`).join('\n')}
]
`

const target = join(pkgRoot, 'src', 'schema', 'profiles.generated.ts')
writeFileSync(target, out)
console.log(
  `wrote ${target} (${artifacts.length} artifact(s): ${artifacts
    .map((a) => `${a.id}@${a.version}`)
    .join(', ')})`,
)
