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
  blankEnvelope,
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
  specOf,
  updateStatement,
  type SqlRow,
} from './codec.js'

export {
  elementReferences,
  referencedIds,
  referenceText,
  type ElementReference,
} from './references.js'

export {
  Store,
  wireOp,
  type ChangeVerb,
} from './store.js'

export {
  SEARCHABLE,
  SEARCH_TABLES,
  indexElement,
  rebuildSearch,
  searchCorpus,
  termCounts,
  type CorpusDocument,
} from './search.js'

export {
  coordinateFromToken,
  elementOfVersion,
  pageCursorFromToken,
  pageToken,
  traversalOf,
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
