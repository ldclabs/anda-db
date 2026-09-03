/**
 * `@ldclabs/kip-do` — a KIP 2.0 knowledge graph on SQLite-backed Durable
 * Objects. One Cognitive Nexus per Durable Object.
 *
 * ## What this module is, and what it deliberately is not
 *
 * This is the surface a **host** holds: the Durable Object it deploys, the
 * engine that object owns, the identity it authenticates callers into, the
 * control plane it seeds, and the shapes it reads back. Nothing else.
 *
 * The engine's own working parts — the SQL codec, the DDL, the tokenizer, the
 * bound-parameter guards, the KML/KQL/META executors, the redaction and
 * lifecycle helpers — are reachable from the modules that own them and are not
 * re-exported here. That is the point: what is exported is a promise, and a
 * promise about `insertStatement` is a promise that the storage layer cannot
 * be rewritten. A host never needed those, so they never bought anything.
 *
 * Two doors, and only two:
 *
 * ```text
 * @ldclabs/kip-do          this file — deploy, authenticate, govern, read
 * @ldclabs/kip-do/schema   ./schema  — author and inspect Schema Packages
 * ```
 *
 * `test/surface.test.ts` pins the list below, so an internal symbol cannot
 * drift back out through a barrel nobody re-read.
 */

// --- The Durable Object a host deploys --------------------------------------

export {
  KipDatabase,
  receiptOf,
  type ExecutionMode,
  type KipDatabaseEnv,
  type KipOperationStatus,
  type KipReceipt,
  type KipReceiptOrigin,
  type KipResponse,
  type KipResult,
  type KipResultContext,
  type OnError,
} from './durable-object.js'

// --- The engine that object owns --------------------------------------------

export {
  CognitiveNexus,
  DEFAULT_SPACE,
  Session,
  type MutationOptions,
  type NexusOptions,
  type ReadOptions,
  type RetentionSweep,
} from './nexus.js'

export { KIP_VERSION, capabilities } from './meta/index.js'

// --- Failure ----------------------------------------------------------------
//
// A KIP error carries a category, a retry class and an agent-facing recovery
// hint, and a host maps the retry class onto its transport's status. Exported
// whole, because a host that can only catch the error but not classify it has
// to guess at exactly the point where guessing costs a duplicated write.

export {
  KIP_ERROR_CODES,
  KIP_ERROR_REGISTRY,
  KipError,
  detailed,
  errors,
  type CursorFamily,
  type CursorReason,
  type ErrorFactories,
  type KipErrorCategory,
  type KipErrorCode,
  type KipErrorJSON,
  type KipErrorSpec,
  type KipRetryClass,
  type RetryInfo,
} from './errors.js'

// --- Who is asking ----------------------------------------------------------
//
// What `KipDatabase.authenticate` returns. It is built from what the host
// *observed* about the connection, never from the request body — a request
// body is exactly what an Agent under prompt injection controls.

export {
  ANONYMOUS_PRINCIPAL,
  SYSTEM_PRINCIPAL,
  anonymousAuth,
  effectivePurpose,
  mergeRequestContext,
  principalAuth,
  systemAuth,
  type AuthContext,
  type RequestContext,
} from './governance/index.js'

// --- The Governance Control Plane -------------------------------------------
//
// Seeded through `nexus.store.governance` and through `Session`'s governed
// methods. No KML clause reaches any of it, which is what keeps a prompt
// injection into ordinary memory formation off the control plane.

export {
  ALL_PERMISSIONS,
  ANY_SPACE,
  EffectiveAuthority,
  PERMISSIONS,
  approvalId,
  assurance,
  authStrength,
  authority,
  authorityCeiling,
  bindingClass,
  bindingId,
  classification,
  delegationId,
  describePermission,
  familyOf,
  govStatus,
  grantId,
  isAlwaysAudited,
  isPermission,
  isPermitted,
  parsePermission,
  principalClass,
  purposeAssurance,
  requirePermitted,
  spaceResource,
  subjectDigest,
  type ActorBindingRow,
  type ApprovalRow,
  type Authorization,
  type AuthorityConditions,
  type AuthorityConstraints,
  type AuthorityScope,
  type Decision,
  type DelegationRow,
  type Family,
  type GovernanceAuditRow,
  type GovernancePolicyRow,
  type GrantRow,
  type Permission,
  type PolicyObligations,
  type PolicyStatement,
  type PrincipalGroupRow,
  type PrincipalRow,
  type ResourceContext,
} from './governance/index.js'

export {
  GovernanceStore,
  actorKey,
  type ActorBindingDraft,
  type ApprovalDraft,
  type DelegationDraft,
  type GrantDraft,
  type GroupDraft,
  type PolicyDraft,
  type PrincipalDraft,
} from './store/index.js'

// --- What a host reads back -------------------------------------------------
//
// The stored shapes, and the `Store` handle they come off. Not the codec that
// encodes them, not the DDL that declares them, and not the search index that
// maintains itself under them.

export {
  State,
  Store,
  TABLES,
  type ActivityRow,
  type AssertionRow,
  type ChangeEntry,
  type ChangeOp,
  type ConceptRow,
  type Element,
  type ElementRow,
  type ElementVersionRow,
  type Envelope,
  type EvidenceRow,
  type PlaneVersions,
  type PropositionRow,
  type SchemaEnvRow,
  type SchemaPackageRow,
  type SpaceRow,
  type TransactionRow,
} from './store/index.js'

export type { KqlAnswer, KqlContext } from './kql/index.js'
export type { IngestContext, IngestEvidence, Outcome } from './kml/index.js'

// --- The Epistemic Projection -----------------------------------------------
//
// What a Proposition is *believed* to be, as distinct from its existing. The
// distinction is the whole of KIP 2.0, so the policy that decides it is named
// rather than hidden.

export {
  BASELINE_ID,
  BASELINE_VERSION,
  baseline,
  beliefToJson,
  forecast,
  policyFromSettings,
  project,
  slotToJson,
  type Belief,
  type Policy,
} from './projection/index.js'

// --- Schema Packages --------------------------------------------------------
//
// The bundled artifacts and the environment a Space resolves through. Authoring
// or inspecting a package — the symbol grammar, the definition shapes, the
// validators — goes through `@ldclabs/kip-do/schema`, which is where that whole
// vocabulary lives.

export {
  BUNDLED_PACKAGES,
  COGNITIVE_MEMORY,
  COGNITIVE_MEMORY_ID,
  COGNITIVE_MEMORY_VERSION,
  CORE_PACKAGE,
  CORE_PACKAGE_ID,
  CORE_PACKAGE_REF,
  SchemaEnvironment,
  formatPackageRef,
  formatSymbolRef,
  packageRefOf,
  parsePackage,
  parsePackageRef,
  parseSymbolRef,
  type PackageRef,
  type SchemaLock,
  type SchemaPackage,
  type SymbolKind,
  type SymbolRef,
} from './schema/index.js'

// --- The language ------------------------------------------------------------
//
// A host that pre-parses — to cache an AST, to run one statement many times, or
// to inspect what a command would touch before running it — needs the parser
// and the tree it produces. `Session.mutate` and `Session.find` take the parsed
// forms directly.

export {
  parseKip,
  parseKipAll,
  parseKipBatch,
  parserVersion,
  specRevision,
} from './kip/parser.js'
export type * from './kip/ast.js'

// --- Values -------------------------------------------------------------------
//
// Results are `Json`. Element ids and timestamps arrive inside them as strings,
// and these are the readers that turn one back into something comparable.

export {
  canonicalJson,
  isJsonArray,
  isJsonMap,
  jsonEquals,
  parseJson,
  type Json,
  type JsonMap,
} from './json.js'

export {
  ELEMENT_KINDS,
  UNREACHABLE_SEQ,
  compareElementId,
  elementId,
  elementIdEquals,
  formatElementId,
  kindOfTag,
  parseElementId,
  parseElementIdOfKind,
  tagOf,
  tryParseElementId,
  type ElementId,
  type ElementKind,
} from './id.js'

export {
  TIME_MAX,
  TIME_MIN,
  formatTime,
  normalizeTime,
  nowTime,
  parseTime,
  type Timestamp,
} from './time.js'
