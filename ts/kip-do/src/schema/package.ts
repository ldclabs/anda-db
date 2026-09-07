import { parseCanonicalJson } from '@ldclabs/kip-lang'
/**
 * # The Schema Package artifact
 *
 * Authoritative Schema in KIP 2.0 is an immutable versioned artifact, not a set
 * of graph nodes (Spec §20.1, §20.11). This is the difference from 1.x that costs
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
  value_schema?: Json
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
  /**
   * Permitted Literal datatypes, when a Literal is allowed (§20.15).
   *
   * §20.15 spells the member `literal_types` and draws its values from §9.2's
   * four names; `datatypes` is the older spelling, read the same way.
   */
  literal_types?: string[]
  datatypes?: string[]
  /** Whether `null` is a permitted object (§9.5, §20.15). */
  nullable?: boolean
  /**
   * The shape a string Literal must have — `timestamp`, `uri`, or a
   * package-defined name (§20.15). Validated on write, never part of identity.
   */
  format?: string
  [extra: string]: Json | undefined
}

/** The Literal datatypes an endpoint admits, whichever member spells them. */
export function literalTypesOf(spec: EndpointSpec | undefined): string[] {
  return spec?.literal_types ?? spec?.datatypes ?? []
}

/** Whether an endpoint declares no restriction at all. */
export function isUnconstrained(spec: EndpointSpec | undefined): boolean {
  return (
    (spec?.concept_types?.length ?? 0) === 0 &&
    (spec?.kinds?.length ?? 0) === 0 &&
    literalTypesOf(spec).length === 0
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
   * Whether one subject has at most one accepted object at one valid time
   * (§20.15, §25.1).
   *
   * This is an *epistemic* statement, not a storage constraint: a functional
   * predicate with two competing objects is a conflict set, which the engine
   * must be able to store in order to report it (§25, §95).
   */
  functional?: boolean
  /**
   * Whether absence of a Proposition means insufficient (§24) rather than a
   * closed-world absence (§24.2). Defaults to `true`.
   */
  open_world?: boolean
  /**
   * Whether the candidate objects of a functional slot are exclusive:
   * accepting one rejects the others (§20.15, §25). Defaults to `false`.
   */
  complete?: boolean
  /**
   * Whether, for a boolean-valued Predicate, object `false` is the negation
   * of object `true` (§12.7, §20.15). Defaults to `false`.
   */
  boolean_completeness?: boolean
  /**
   * When two accepted values of a functional slot conflict (§25.2):
   * `overlapping_valid_time` (the default) or `none`.
   */
  temporal_conflict?: string
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
 * (§18.1) — which is exactly what KIP 1.x `metadata` had become.
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
  /** Who published it. Namespace identity does not prove this (§20.11). */
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
  /** Advisory guidance for an Agent. Never a validator (§20.5). */
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
      value = parseCanonicalJson(source)
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

/**
 * The Core element kinds `kip://core` exports (§20.13).
 *
 * Mirrors `anda_kip::CORE_ELEMENT_KINDS`.
 */
export const CORE_ELEMENT_KINDS: readonly string[] = [
  'Concept',
  'Proposition',
  'Assertion',
  'Evidence',
  'Activity',
]

/**
 * The reserved Core structural fields `kip://core` exports, each with the Core
 * kind that owns it (§20.13).
 *
 * Mirrors `anda_kip::CORE_STRUCTURAL_FIELDS`. Resolved by the source element's
 * Core kind, never through a package alias — which is why a package field of
 * the same name, carried by the *same* kind, would change what an Assertion
 * cites without changing any command that reads it. On a different kind there
 * is nothing to shadow: a Concept owns no Core structural field, so a Profile
 * `evidence` on a Concept is a separate plane.
 */
export const CORE_STRUCTURAL_FIELD_OWNERS: Readonly<Record<string, string>> = {
  evidence: 'Assertion',
  context: 'Assertion',
  source: 'Evidence',
  generated_by: 'Evidence',
  inputs: 'Activity',
  outputs: 'Activity',
  associated_actors: 'Activity',
}

/**
 * Refuses a package that would shadow a reserved Core symbol (§20.13).
 *
 * `kip://core` is implicitly active in every Schema Environment and cannot be
 * deactivated, replaced, or shadowed. A package defining a Concept type named
 * `Assertion`, or an Activity-sourced structural field named `inputs`, would
 * put two meanings behind one word in one resolution scope — and the reader
 * that resolved the wrong one could not tell.
 *
 * The namespaces are checked apart. A type, Facet or Enum resolves by name
 * alone, so a Core element kind is a shadow wherever it appears. A structural
 * field resolves by source kind *and* name (§8.2), so it shadows only where
 * the kind that owns the Core field could carry it — a source constraining
 * nothing admits every kind, and therefore shadows. A Predicate named `source`
 * is a claim about origin and shadows nothing.
 */
export function rejectCoreShadowing(artifact: SchemaPackage): void {
  const refuse = (kind: SymbolKind, name: string, detail: string): never => {
    throw errors.constraintViolation(
      `this package defines the ${SECTIONS[kind]} \`${name}\`, which is a ` +
        `reserved Core symbol (§20.13)${detail}: \`kip://core\` is implicitly ` +
        `active in every Schema Environment and cannot be shadowed. Rename it, ` +
        `or address the Core symbol you meant`,
    )
  }

  for (const kind of ['ConceptType', 'Facet', 'Enum'] as const) {
    for (const name of symbols(artifact, kind)) {
      if (CORE_ELEMENT_KINDS.includes(name)) refuse(kind, name, '')
    }
  }

  const fields = artifact.definitions?.structural_fields ?? {}
  for (const [name, def] of Object.entries(fields)) {
    const owner = CORE_STRUCTURAL_FIELD_OWNERS[name]
    if (owner === undefined) continue
    const kinds = def.source?.kinds ?? []
    const types = def.source?.concept_types ?? []
    if (kinds.length === 0 && types.length === 0) {
      refuse(
        'StructuralField',
        name,
        ` on ${owner}: a source that constrains nothing admits every kind, ${owner} included, and ${owner}'s \`${name}\` is defined by the protocol itself`,
      )
    }
    if (kinds.includes(owner)) {
      refuse(
        'StructuralField',
        name,
        ` on ${owner}, whose \`${name}\` the protocol itself defines`,
      )
    }
  }
}

/** The canonical reference for one of this package's local names. */
export function symbolRefOf(
  artifact: SchemaPackage,
  name: string,
): SymbolRef {
  return { package: packageRefOf(artifact), name }
}

export const conceptTypeDef = (
  artifact: SchemaPackage,
  name: string,
): ConceptTypeDef | undefined => artifact.definitions?.concept_types?.[name]

export const predicateDef = (
  artifact: SchemaPackage,
  name: string,
): PredicateDef | undefined => artifact.definitions?.predicates?.[name]

/**
 * A Predicate definition's declarations with §20.15's defaults filled in.
 *
 * `functional: false`, `open_world: true`, `complete: false`,
 * `boolean_completeness: false`, `temporal_conflict: "overlapping_valid_time"`.
 * An absent definition — a predicate this environment cannot resolve —
 * declares nothing, and reads as the defaults too.
 */
export interface PredicateRules {
  functional: boolean
  open_world: boolean
  complete: boolean
  boolean_completeness: boolean
  temporal_conflict: 'overlapping_valid_time' | 'none'
}

export function predicateRules(def: PredicateDef | undefined): PredicateRules {
  return {
    functional: def?.functional === true,
    open_world: def?.open_world !== false,
    complete: def?.complete === true,
    boolean_completeness: def?.boolean_completeness === true,
    temporal_conflict: def?.temporal_conflict === 'none' ? 'none' : 'overlapping_valid_time',
  }
}

export const facetDef = (
  artifact: SchemaPackage,
  name: string,
): FacetDef | undefined => artifact.definitions?.facets?.[name]

export const structuralFieldDef = (
  artifact: SchemaPackage,
  name: string,
): StructuralFieldDef | undefined =>
  artifact.definitions?.structural_fields?.[name]

