/**
 * The storage layer.
 *
 * One import for everything below the KIP semantics: the DDL, the row shapes,
 * the codec that is the single place a column list is written down, the
 * reference walker, and the `Store` that ties them together.
 */

export {
  SCHEMA_STATEMENTS,
  SCHEMA_VERSION,
  applySchema,
  configureSql,
  metaGet,
  metaSet,
} from './ddl.js'

export {
  State,
  TABLES,
  classificationOf,
  schemaRefOf,
  type ActivityRow,
  type AssertionRow,
  type ChangeEntry,
  type ConceptRow,
  type Element,
  type ElementRow,
  type ElementState,
  type ElementVersionRow,
  type Envelope,
  type EvidenceRef,
  type EvidenceRow,
  type PropositionRow,
  type SchemaEnvRow,
  type SchemaPackageRow,
  type SpaceRow,
  type TransactionRow,
} from './rows.js'

export {
  TABLE_SPECS,
  decodeRow,
  insertStatement,
  rowToJson,
  updateStatement,
  type SqlRow,
} from './codec.js'

export {
  elementReferences,
  referencedIds,
  type ElementReference,
} from './references.js'

export {
  Store,
  asActivity,
  asAssertion,
  asConcept,
  asEvidence,
  asProposition,
  type ChangeOp,
} from './store.js'

export {
  coordinateFromToken,
  elementOfVersion,
  snapshotJson,
  snapshotToken,
  type Coordinate,
} from './history.js'

export {
  GovernanceStore,
  actorKey,
  type ActorBindingDraft,
  type ApprovalDraft,
  type DelegationDraft,
  type GrantDraft,
  type GroupDraft,
  type MutationEntry,
  type PolicyDraft,
  type PrincipalDraft,
} from './governance.js'
