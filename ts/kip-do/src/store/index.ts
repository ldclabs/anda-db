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
  PAYLOAD_PURGED,
  State,
  TABLES,
  bumpPlane,
  changeEntryOf,
  classificationOf,
  emptyPlanes,
  erasePayload,
  planeCounter,
  planesFromJson,
  planesToJson,
  schemaRefOf,
  symbolLocalName,
  type ActivityRow,
  type AssertionRow,
  type ChangeEntry,
  type ChangeOp,
  type ConceptRow,
  type Element,
  type ElementRow,
  type ElementState,
  type ElementVersionRow,
  type Envelope,
  type EvidenceRef,
  type EvidenceRow,
  type PlaneKey,
  type PlaneVersions,
  type PropositionRow,
  type SchemaEnvRow,
  type SchemaPackageRow,
  type SpaceRow,
  type TransactionRow,
  type WirePlaneVersions,
} from './rows.js'

export {
  TABLE_SPECS,
  decodeRow,
  insertStatement,
  rowToJson,
  specOf,
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
  wireOp,
  type ChangeVerb,
} from './store.js'

export {
  SEARCHABLE,
  SEARCH_TABLES,
  indexElement,
  rebuildSearch,
  searchIndex,
  type SearchHit,
  type SearchQuery,
} from './search.js'

export {
  coordinateFromToken,
  elementOfVersion,
  pageCursorFromToken,
  pageToken,
  snapshotJson,
  snapshotToken,
  type Coordinate,
  type CursorFamily,
  type PageCursor,
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
