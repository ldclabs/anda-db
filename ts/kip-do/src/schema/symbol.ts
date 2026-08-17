/**
 * # Schema symbol identity
 *
 * A symbol means nothing outside its package and version lineage (Spec §16):
 *
 * ```text
 * kip://<package-path>@<exact-version>[/<symbol>]
 * ```
 *
 * The rule the rest of the engine leans on is §13: **every persisted schema
 * reference resolves to an exact version.** `Person@latest` stored on an
 * element would mean the element's meaning changes when someone else publishes
 * a package — the stored data would say one thing today and another thing
 * tomorrow, with no transaction in between. So a range may appear in a
 * *resolution input*, and never in a `schema_ref`.
 */

import { errors } from '../errors.js'

/** The URI scheme every package reference carries. */
export const SCHEME = 'kip://'

/** A SemVer-like version triplet with an optional pre-release tag. */
export interface Version {
  major: number
  minor: number
  patch: number
  /** The pre-release tag, empty for a release. */
  pre: string
}

export const version = (
  major: number,
  minor: number,
  patch: number,
  pre = '',
): Version => ({ major, minor, patch, pre })

export function formatVersion(v: Version): string {
  return `${v.major}.${v.minor}.${v.patch}${v.pre === '' ? '' : `-${v.pre}`}`
}

export function parseVersion(text: string): Version {
  const invalid = (why: string) =>
    errors.invalidIdentifier(
      `${JSON.stringify(text)} is not an exact schema version (${why}); a ` +
        `persisted schema reference must name one immutable package version, ` +
        `never a range such as "2.x" or "latest"`,
    )

  let triplet = text
  let pre = ''
  const hyphen = text.indexOf('-')
  if (hyphen !== -1) {
    triplet = text.slice(0, hyphen)
    pre = text.slice(hyphen + 1)
    if (pre === '') throw invalid('empty pre-release tag')
  }

  const parts = triplet.split('.')
  if (parts.length !== 3) throw invalid('not three numeric components')
  const [major, minor, patch] = parts.map((part) => {
    // `Number(part)` accepts `+2`, ` 2` and `2e0`, which would give one version
    // several spellings and break exact-reference equality.
    if (!/^\d+$/.test(part)) throw invalid(`${JSON.stringify(part)} is not numeric`)
    return Number(part)
  }) as [number, number, number]
  return { major, minor, patch, pre }
}

/**
 * Orders by triplet, with a pre-release sorting *below* its own release.
 *
 * `2.0.0-rc1 < 2.0.0`, per SemVer. Getting this backwards would make a release
 * candidate look like an upgrade from the release it preceded.
 */
export function compareVersion(a: Version, b: Version): number {
  if (a.major !== b.major) return a.major < b.major ? -1 : 1
  if (a.minor !== b.minor) return a.minor < b.minor ? -1 : 1
  if (a.patch !== b.patch) return a.patch < b.patch ? -1 : 1
  if (a.pre === b.pre) return 0
  if (a.pre === '') return 1
  if (b.pre === '') return -1
  return a.pre < b.pre ? -1 : 1
}

/** One immutable package version: `kip://profiles/cognitive-memory@2.0.0`. */
export interface PackageRef {
  /** The stable namespace-qualified name, including the `kip://` scheme. */
  packageId: string
  version: Version
}

export function formatPackageRef(ref: PackageRef): string {
  return `${ref.packageId}@${formatVersion(ref.version)}`
}

export function parsePackageRef(text: string): PackageRef {
  const invalid = (why: string) =>
    errors.invalidIdentifier(
      `${JSON.stringify(text)} is not a package reference (${why}); the form ` +
        `is kip://<path>@<major.minor.patch>`,
    )
  if (!text.startsWith(SCHEME)) throw invalid('missing the kip:// scheme')
  // Split on the last `@`: a package path may not contain one, but splitting
  // from the right is what makes that a rule rather than an assumption.
  const at = text.lastIndexOf('@')
  if (at === -1) throw invalid('no @version')
  const packageId = text.slice(0, at)
  const path = packageId.slice(SCHEME.length)
  if (path === '' || path.startsWith('/') || path.endsWith('/')) {
    throw invalid('empty or malformed package path')
  }
  return { packageId, version: parseVersion(text.slice(at + 1)) }
}

/** One canonical symbol: `kip://profiles/cognitive-memory@2.0.0/has_step`. */
export interface SymbolRef {
  package: PackageRef
  /** The local symbol name inside that package. */
  name: string
}

export function formatSymbolRef(symbol: SymbolRef): string {
  return `${formatPackageRef(symbol.package)}/${symbol.name}`
}

export function parseSymbolRef(text: string): SymbolRef {
  const invalid = (why: string) =>
    errors.invalidIdentifier(
      `${JSON.stringify(text)} is not a canonical schema symbol (${why}); the ` +
        `form is kip://<path>@<version>/<Symbol>`,
    )
  // The symbol name follows the version, so the split point is the first `/`
  // *after* the `@` — the package path has slashes of its own.
  const at = text.lastIndexOf('@')
  if (at === -1) throw invalid('no @version')
  const offset = text.slice(at).indexOf('/')
  if (offset === -1) throw invalid('no /symbol')
  const slash = at + offset
  const name = text.slice(slash + 1)
  if (name === '' || name.includes('/')) {
    throw invalid('empty or nested symbol name')
  }
  return { package: parsePackageRef(text.slice(0, slash)), name }
}

/**
 * Whether a string is already a canonical, fully-qualified reference.
 *
 * This is the test that separates "the caller named an exact symbol" from "the
 * caller wrote a local name for the environment to resolve" (§19).
 */
export const isQualified = (name: string): boolean => name.startsWith(SCHEME)

/**
 * The kinds of symbol a Schema Package defines (Spec §17).
 *
 * Core element kinds are not in this list: a package cannot redefine what an
 * Assertion is (§240.22).
 */
export type SymbolKind =
  | 'ConceptType'
  | 'PredicateType'
  | 'Facet'
  | 'StructuralField'
  | 'Enum'

/** The key each kind occupies in a package artifact's `definitions`. */
export const SECTIONS: Readonly<Record<SymbolKind, string>> = {
  ConceptType: 'concept_types',
  PredicateType: 'predicates',
  Facet: 'facets',
  StructuralField: 'structural_fields',
  Enum: 'enums',
}

/** How a symbol kind is named in a message to an Agent. */
export const KIND_NAMES: Readonly<Record<SymbolKind, string>> = {
  ConceptType: 'Concept type',
  PredicateType: 'predicate',
  Facet: 'Facet',
  StructuralField: 'structural field',
  Enum: 'enum',
}
