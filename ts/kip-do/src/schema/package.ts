/**
 * # The Schema Package artifact
 *
 * Authoritative Schema in KIP 2.0 is an immutable versioned artifact, not a set
 * of graph nodes (Spec §1, §240.1). This is the difference from 1.x that costs
 * the most to get wrong: in 1.x an ordinary `UPSERT` could reshape what a type
 * meant, so cognition and the rules governing cognition sat in the same mutable
 * store. Here a package is content-addressed and read-only, and changing
 * semantics means publishing a new version and activating it through
 * Governance.
 *
 * The types below mirror the shipped artifact format — the same JSON as
 * `KIP/v2/profiles/cognitive-memory-2.0.0.schema.json`, which the tests parse
 * rather than a hand-written imitation of it.
 *
 * Every member is optional and unknown members are kept: a package written
 * against a later minor format revision must stay readable, and an engine that
 * dropped the members it did not recognize would silently change the artifact's
 * digest-covered content.
 */

import { errors } from '../errors.js'
import { isJsonMap, type Json, type JsonMap } from '../json.js'
import {
  formatPackageRef,
  parsePackageRef,
  SECTIONS,
  type PackageRef,
  type SymbolKind,
  type SymbolRef,
} from './symbol.js'

/** One declared field of an attribute set or a Facet. */
export interface FieldSpec {
  /** The validation type: a name, or a list of accepted names. */
  type?: Json
  /** Whether the field must be present (§36). */
  required?: boolean
  /** Whether the field may change after creation (§39). Defaults to true. */
  mutable?: boolean
  minimum?: number
  maximum?: number
  /** The accepted values, when the field is a closed set. */
  enum?: Json[]
  /** The value used when the field is absent (§40). */
  default?: Json
  [extra: string]: Json | undefined
}

/** The attribute contract of a Concept type (§34–§40). */
export interface AttributeSpec {
  /** Whether attributes not named here are permitted (§37). */
  open?: boolean
  fields?: Record<string, FieldSpec>
}

/** What may occupy one end of a Proposition or structural edge. */
export interface EndpointSpec {
  /** Permitted Concept types, as canonical references. */
  concept_types?: string[]
  /** Permitted Core element kinds, e.g. `Concept`, `Assertion`. */
  kinds?: string[]
  /** Permitted Literal datatypes, when a Literal is allowed (§44). */
  datatypes?: string[]
  [extra: string]: Json | undefined
}

/** Whether an endpoint declares no restriction at all. */
export function isUnconstrained(spec: EndpointSpec | undefined): boolean {
  return (
    (spec?.concept_types?.length ?? 0) === 0 &&
    (spec?.kinds?.length ?? 0) === 0 &&
    (spec?.datatypes?.length ?? 0) === 0
  )
}

/**
 * A Concept type definition (§32).
 *
 * A Concept type says what a Concept *is*, never whether anything about it is
 * true (§33).
 */
export interface ConceptTypeDef {
  ref?: string
  kind?: string
  description?: string
  attributes?: AttributeSpec
  /**
   * Anything a later format revision added.
   *
   * Typed as `unknown` rather than `Json` because the declared members above
   * are structured: an index signature has to admit every member's type, and
   * narrowing it to `Json` would make the structured ones unassignable.
   */
  [extra: string]: unknown
}

/** A predicate type definition (§41). */
export interface PredicateDef {
  ref?: string
  kind?: string
  /** What the predicate means. Semantics must be explicit (§57). */
  description?: string
  subject?: EndpointSpec
  object?: EndpointSpec
  /**
   * Whether one subject has at most one true object (§45).
   *
   * This is an *epistemic* statement, not a storage constraint: a functional
   * predicate with two competing objects is a contested belief, which the
   * engine must be able to store in order to report it (§46, §95).
   */
  functional?: boolean
  /** Whether absence of a claim means unknown rather than false (§51). */
  open_world?: boolean
  complete?: boolean
  /**
   * Anything a later format revision added.
   *
   * Typed as `unknown` rather than `Json` because the declared members above
   * are structured: an index signature has to admit every member's type, and
   * narrowing it to `Json` would make the structured ones unassignable.
   */
  [extra: string]: unknown
}

/**
 * A Facet definition (§58).
 *
 * A Facet is a validated namespaced extension, not an untyped metadata bag
 * (§240.31) — which is exactly what KIP 1.x `metadata` had become.
 */
export interface FacetDef {
  ref?: string
  kind?: string
  description?: string
  /** Whether members not named here are rejected (§60). */
  closed?: boolean
  applicable_to?: EndpointSpec
  fields?: Record<string, FieldSpec>
  /**
   * Anything a later format revision added.
   *
   * Typed as `unknown` rather than `Json` because the declared members above
   * are structured: an index signature has to admit every member's type, and
   * narrowing it to `Json` would make the structured ones unassignable.
   */
  [extra: string]: unknown
}

/** A structural field's permitted reference count. */
export interface Cardinality {
  min?: number
  /** The maximum; absent means unbounded. */
  max?: number | null
}

/**
 * A structural field definition (§62).
 *
 * Structural fields are record topology, not semantic Propositions (§64): a
 * claim *about* a structural relation is a separate Proposition plus Assertion.
 */
export interface StructuralFieldDef {
  ref?: string
  kind?: string
  description?: string
  source?: EndpointSpec
  target?: EndpointSpec
  cardinality?: Cardinality
  /**
   * Whether edge order is meaningful (§66).
   *
   * Order is not causality: `has_step` being ordered says step 3 follows step
   * 2, never that it was caused by it.
   */
  ordered?: boolean
  unique?: boolean
  /**
   * Anything a later format revision added.
   *
   * Typed as `unknown` rather than `Json` because the declared members above
   * are structured: an index signature has to admit every member's type, and
   * narrowing it to `Json` would make the structured ones unassignable.
   */
  [extra: string]: unknown
}

/** The symbols a package defines, one map per symbol kind. */
export interface Definitions {
  concept_types?: Record<string, ConceptTypeDef>
  predicates?: Record<string, PredicateDef>
  facets?: Record<string, FacetDef>
  structural_fields?: Record<string, StructuralFieldDef>
  enums?: Record<string, Json>
  /** Additions to Core's open registries, e.g. `activity_classes` (§69). */
  registry_extensions?: Record<string, Json>
}

/** Identity and provenance. */
export interface Manifest {
  package_id?: string
  version?: string
  /** The two above, joined: `kip://core@2.0.0`. */
  package_ref?: string
  name?: string
  description?: string
  /** Who published it. Namespace identity does not prove this (§240.41). */
  publisher?: Json
  [extra: string]: Json | undefined
}

/** One resolved dependency. */
export interface Dependency {
  package_id?: string
  version?: string
  package_ref?: string
  /** Whether activation fails without it (§73). */
  required?: boolean
  [extra: string]: Json | undefined
}

/** The content digest and signatures. */
export interface Integrity {
  digest_profile?: string
  content_digest?: string
  covers?: string
  /** Signatures over the digest. A signature is not local approval (§90). */
  signatures?: Json[]
}

/** A published Schema Package. */
export interface SchemaPackage {
  format?: string
  format_version?: string
  manifest?: Manifest
  dependencies?: Dependency[]
  definitions?: Definitions
  constraints?: Json[]
  aliases?: JsonMap
  compatibility?: Json
  migrations?: Json
  /** Advisory guidance for an Agent. Never a validator (§240.34). */
  model_hints?: Json
  canonicalization?: Json
  integrity?: Integrity
  /**
   * Anything a later format revision added.
   *
   * Typed as `unknown` rather than `Json` because the declared members above
   * are structured: an index signature has to admit every member's type, and
   * narrowing it to `Json` would make the structured ones unassignable.
   */
  [extra: string]: unknown
}

/** Parses an artifact and checks the identity it declares is coherent. */
export function parsePackage(source: string | JsonMap): SchemaPackage {
  let value: unknown
  if (typeof source === 'string') {
    try {
      value = JSON.parse(source)
    } catch (err) {
      throw errors.artifactParseError(
        `this is not a readable Schema Package artifact: ${String(err)}`,
      )
    }
  } else {
    value = source
  }
  if (!isJsonMap(value)) {
    throw errors.artifactParseError(
      'a Schema Package artifact must be a JSON object',
    )
  }
  const artifact = value as SchemaPackage
  // Reject here rather than at the first symbol lookup: an artifact whose two
  // spellings of its own identity disagree could be installed under a name its
  // symbols do not claim.
  packageRefOf(artifact)
  return artifact
}

/**
 * The package's exact identity.
 *
 * `package_ref` is the authority when present, and `package_id@version` must
 * agree with it.
 */
export function packageRefOf(artifact: SchemaPackage): PackageRef {
  const manifest = artifact.manifest ?? {}
  const joined = `${manifest.package_id ?? ''}@${manifest.version ?? ''}`
  const declared =
    manifest.package_ref === undefined || manifest.package_ref === ''
      ? joined
      : manifest.package_ref
  if (
    (manifest.package_id ?? '') !== '' &&
    (manifest.version ?? '') !== '' &&
    declared !== joined
  ) {
    throw errors.capsuleValidationFailed(
      `the artifact calls itself ${JSON.stringify(declared)} but its ` +
        `package_id and version join to ${JSON.stringify(joined)}`,
    )
  }
  return parsePackageRef(declared)
}

function section(
  artifact: SchemaPackage,
  kind: SymbolKind,
): Record<string, unknown> {
  const definitions = artifact.definitions ?? {}
  return (
    (definitions[SECTIONS[kind] as keyof Definitions] as
      | Record<string, unknown>
      | undefined) ?? {}
  )
}

/** Whether this package defines a symbol of the given kind. */
export function defines(
  artifact: SchemaPackage,
  kind: SymbolKind,
  name: string,
): boolean {
  return Object.hasOwn(section(artifact, kind), name)
}

/** The local names this package defines for one symbol kind. */
export function symbols(
  artifact: SchemaPackage,
  kind: SymbolKind,
): string[] {
  return Object.keys(section(artifact, kind))
}

/** The canonical reference for one of this package's local names. */
export function symbolRefOf(
  artifact: SchemaPackage,
  name: string,
): SymbolRef {
  return { package: packageRefOf(artifact), name }
}

/** The canonical reference text for one of this package's local names. */
export function symbolTextOf(artifact: SchemaPackage, name: string): string {
  return `${formatPackageRef(packageRefOf(artifact))}/${name}`
}

export const conceptTypeDef = (
  artifact: SchemaPackage,
  name: string,
): ConceptTypeDef | undefined => artifact.definitions?.concept_types?.[name]

export const predicateDef = (
  artifact: SchemaPackage,
  name: string,
): PredicateDef | undefined => artifact.definitions?.predicates?.[name]

export const facetDef = (
  artifact: SchemaPackage,
  name: string,
): FacetDef | undefined => artifact.definitions?.facets?.[name]

export const structuralFieldDef = (
  artifact: SchemaPackage,
  name: string,
): StructuralFieldDef | undefined =>
  artifact.definitions?.structural_fields?.[name]

/** The values one of Core's open registries accepts, as this package sees it. */
export function registryValues(
  artifact: SchemaPackage,
  registry: string,
): string[] {
  const entry = artifact.definitions?.registry_extensions?.[registry]
  if (!isJsonMap(entry)) return []
  const values = entry.values
  return Array.isArray(values)
    ? values.filter((value): value is string => typeof value === 'string')
    : []
}
