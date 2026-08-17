/**
 * # Schema Packages and the Schema Environment
 *
 * In KIP 1.x, authoritative schema lived in the graph as `$ConceptType` and
 * `$PropositionType` nodes, which meant an ordinary write could change what a
 * type meant. KIP 2.0 moves it out: authoritative Schema is an **immutable
 * versioned Package Artifact**, and a graph node that mirrors one is a mirror,
 * never the authority (Spec §5, §240.1–§240.3).
 *
 * - `symbol` — canonical identity, `kip://<path>@<version>/<Symbol>`, and the
 *   rule that every persisted reference names an exact version;
 * - `package` — the artifact itself, modelled on the shipped format;
 * - `env` — the per-Space resolution set, which turns a model-facing local name
 *   into one exact symbol or fails saying why;
 * - `validate` — the package validation layer, deliberately narrower than Core
 *   validation and unable to weaken it;
 * - `profiles.generated` — the bundled artifacts, vendored from the Rust crate.
 *
 * ## The line this module will not cross
 *
 * A Schema Package declares what things *are*. It never declares what is
 * *true*, who may read anything, or how much anything is trusted (§33, §96,
 * §240.23–§240.25). Concretely: a `functional` predicate does not reject a
 * second competing object, it creates a conflict for the Epistemic Projection
 * to report — because a memory system that cannot store disagreement cannot
 * report it either.
 */

export {
  KIND_NAMES,
  SCHEME,
  SECTIONS,
  compareVersion,
  formatPackageRef,
  formatSymbolRef,
  formatVersion,
  isQualified,
  parsePackageRef,
  parseSymbolRef,
  parseVersion,
  version,
  type PackageRef,
  type SymbolKind,
  type SymbolRef,
  type Version,
} from './symbol.js'

export {
  conceptTypeDef,
  defines,
  facetDef,
  isUnconstrained,
  packageRefOf,
  parsePackage,
  predicateDef,
  registryValues,
  structuralFieldDef,
  symbolRefOf,
  symbolTextOf,
  symbols,
  type AttributeSpec,
  type Cardinality,
  type ConceptTypeDef,
  type Definitions,
  type Dependency,
  type EndpointSpec,
  type FacetDef,
  type FieldSpec,
  type Integrity,
  type Manifest,
  type PredicateDef,
  type SchemaPackage,
  type StructuralFieldDef,
} from './package.js'

export {
  ACTIVITY_CLASSES,
  ASSERTION_MODES,
  CORE_PACKAGE,
  CORE_PACKAGE_ID,
  CORE_PACKAGE_REF,
  EVIDENCE_CLASSES,
  STANCES,
  SchemaEnvironment,
  allowsRead,
  allowsWrite,
  answersLocalNames,
  emptyLock,
  lockFromJson,
  packageRefText,
  type Intent,
  type PackageState,
  type SchemaLock,
} from './env.js'

export {
  Validation,
  validateAttributeMutability,
  validateAttributes,
  validateFacet,
  validateStructural,
  type Severity,
  type Violation,
} from './validate.js'

export {
  BUNDLED_PACKAGES,
  COGNITIVE_MEMORY,
  COGNITIVE_MEMORY_ID,
  COGNITIVE_MEMORY_VERSION,
} from './profiles.generated.js'
