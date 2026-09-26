use croaring::{Portable, Treemap};
use futures::{StreamExt, future::try_join_all, try_join as try_join_await};
use object_store::path::Path;
use parking_lot::RwLock;
use rustc_hash::FxHashSet;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
};
use std::{borrow::Cow, future::Future, time::Instant};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

use crate::{
    database::AndaDB,
    error::{CollectionState, CollectionStateError, DBError},
    index::*,
    query::*,
    schema::*,
    storage::{ObjectVersion, Storage, StorageStats},
    unix_ms,
};

/// A Collection represents a logical grouping of documents with the same schema.
/// It provides methods for document storage, retrieval, and indexing.
///
/// Collections manage:
/// - Document storage and retrieval
/// - Schema validation
/// - Index creation and maintenance
/// - Search functionality
pub struct Collection {
    /// Collection name
    name: String,
    /// Collection metadata
    schema: Arc<Schema>,
    /// Storage backend for persisting collection data
    storage: Storage,
    /// BTree indexes for efficient exact-match queries
    btree_indexes: Vec<BTree>,
    /// BM25 (text search) indexes
    bm25_indexes: Vec<BM25>,
    /// HNSW (vector search) indexes
    hnsw_indexes: Vec<Hnsw>,
    /// Collection metadata including statistics and configuration
    metadata: RwLock<CollectionMetadata>,
    /// Highest document ID assigned so far
    max_document_id: AtomicU64,
    /// Counter for search operations
    search_count: AtomicU64,
    /// Counter for get operations
    get_count: AtomicU64,
    /// Text tokenization chain for text analysis
    tokenizer: TokenizerChain,
    /// Live ids in query order and bitmap form, with one membership dirty flag.
    /// A single lock keeps both representations coherent.
    doc_ids: RwLock<DocumentIds>,
    /// Whether the collection is in read-only mode
    read_only: AtomicBool,
    /// Database-level read-only state shared with every collection handle.
    /// This prevents a newly opened or retained collection from locally
    /// overriding `AndaDB::set_read_only(true)`.
    database_read_only: Arc<AtomicBool>,
    /// Irreversible handle lifecycle, separate from user-controlled
    /// `read_only`.  A handle that has started closing can never become a
    /// writer again, even if a caller later invokes `set_read_only(false)`.
    lifecycle: AtomicU8,
    /// Shared by every asynchronous mutation and taken exclusively by
    /// flush/close/delete.  Closing first shuts admission through
    /// `lifecycle`, then waits for this gate to drain operations that already
    /// passed admission.
    operation_gate: Arc<tokio::sync::RwLock<()>>,
    /// Unique indexes reserve their old keys through the complete document
    /// commit. Collections without unique indexes retain concurrent writes.
    unique_commit_gate: Arc<tokio::sync::RwLock<()>>,
    unique_key_locks: std::sync::OnceLock<Vec<tokio::sync::Mutex<()>>>,
    /// Configuration callbacks run before recovery; their first operation
    /// completes recovery using the hooks/tokenizer installed so far.
    recovery_pending: AtomicBool,
    recovery_gate: tokio::sync::Mutex<()>,
    io_concurrency: AtomicUsize,
    recovery_issues: RwLock<BTreeMap<DocumentId, String>>,
    /// Last saved version of the collection
    last_saved_version: AtomicU64,

    metadata_version: RwLock<ObjectVersion>,
    committed_indexes: RwLock<IndexRegistry>,
    ids_version: RwLock<ObjectVersion>,
    index_hooks: Arc<dyn IndexHooks>,

    /// Striped async locks serializing `update` / `remove` per document id
    /// (stripe = `id % DOC_LOCK_STRIPES`).
    ///
    /// Without this, two concurrent updates of the same document race between
    /// their index mutations and the versioned storage write: the loser's
    /// rollback can re-insert index entries for values the stored document no
    /// longer has, leaving phantom matches that nothing cleans up. `add` does
    /// not take a stripe: every add works on a freshly allocated unique id.
    doc_locks: Vec<tokio::sync::Mutex<()>>,

    /// Durable document-mutation intents (update/remove only) that have not
    /// yet been covered by a successful index/ids checkpoint.  See
    /// [`MutationIntent`].
    pending_mutations: parking_lot::Mutex<BTreeSet<u64>>,
    /// Unusable retained intent objects discovered during reopen. Their
    /// contents cannot drive recovery, but their paths are kept so the next
    /// successful checkpoint can retire them instead of logging them forever.
    stale_mutation_intents: parking_lot::Mutex<BTreeSet<String>>,
    /// Serializes concurrent extension writers' unclaimed metadata PUTs.
    /// They hold shared `operation_gate` leases, so without this two of them
    /// could race the same expected object version and one would fail with a
    /// spurious `Precondition`. Flush needs no part in this: it holds the
    /// exclusive gate.
    extension_write_gate: tokio::sync::Mutex<()>,
    /// Monotonic path component for mutation-intent objects.
    next_mutation_sequence: AtomicU64,
    /// Highest durably published allocation watermark. `add` guarantees
    /// `id <= watermark` **before** a document object may be written for the
    /// id (persisting the watermark in strides of
    /// [`Collection::ALLOCATION_WATERMARK_STRIDE`]), so the reopen repair
    /// scan can enumerate `checkpoint+1 ..= max(metadata max, watermark)`
    /// exhaustively instead of writing one durable intent per add.
    durable_alloc_watermark: AtomicU64,
    /// Serializes the rare watermark PUT when an allocation crosses it.
    watermark_gate: tokio::sync::Mutex<()>,
}

const LIFECYCLE_ACTIVE: u8 = 0;
const LIFECYCLE_CLOSING: u8 = 1;
const LIFECYCLE_CLOSED: u8 = 2;
const LIFECYCLE_DELETING: u8 = 3;
const LIFECYCLE_DELETED: u8 = 4;
/// A mutating future on this handle was dropped before completion.
///
/// Cancellation is treated exactly like a process crash: the in-memory
/// index/bitmap/version state may have diverged from storage in ways only the
/// reopen recovery path (mutation-intent replay plus the repair scan) can
/// reconcile. A poisoned handle rejects further mutations; reopening
/// the collection loads a fresh, consistent generation from storage.
const LIFECYCLE_POISONED: u8 = 5;

/// Poisons a collection handle when a mutating future is dropped before its
/// wrapped operation returned. Callers `disarm` the guard after the operation
/// completes (with either result); only cancellation leaves it armed.
struct CancelGuard<'a> {
    collection: &'a Collection,
    action: &'static str,
    armed: bool,
}

impl CancelGuard<'_> {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.collection.poison(self.action);
        }
    }
}

/// A write-ahead record for an update/remove of an existing document.
///
/// Document objects and derived indexes live in different object-store
/// objects, so no ordering alone can make an update atomic across a crash.
/// The before/after documents are recorded before either side changes. On
/// open, every retained intent removes both possible indexed states and then
/// re-indexes the document currently present in storage (or completes its
/// removal). One record is kept per mutation rather than overwriting a
/// per-document record so repeated updates remain recoverable even after a
/// partially successful flush.
///
/// `add` writes no intent: the allocation watermark (see
/// [`Collection::ensure_allocation_watermark`]) bounds the id window the
/// reopen repair scan probes, which recovers committed-but-unregistered adds
/// without per-add write amplification.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MutationIntent {
    sequence: u64,
    document_id: DocumentId,
    previous: Option<DocumentOwned>,
    proposed: Option<DocumentOwned>,
    /// A maintenance removal can have no decodable pre-image. Its replay
    /// must sweep by id even when the durable bitmap predates the removal.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    purge_by_id: bool,
}

/// The write-side form of [`MutationIntent`]: identical fields in the same
/// order, borrowing the documents instead of cloning them. `Document`
/// serializes exactly like `DocumentOwned`, so the encoded bytes match.
#[derive(Serialize)]
struct MutationIntentRef<'a> {
    sequence: u64,
    document_id: DocumentId,
    previous: Option<&'a Document>,
    proposed: Option<&'a Document>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    purge_by_id: bool,
}

/// Durable index references, independent from staged index creation. A
/// metadata-only write can remove references but cannot publish new ones.
#[derive(Default)]
struct IndexRegistry {
    btree: BTreeSet<String>,
    bm25: BTreeSet<String>,
    hnsw: BTreeSet<String>,
}

impl IndexRegistry {
    fn from_metadata(meta: &CollectionMetadata) -> Self {
        Self {
            btree: meta.btree_indexes.keys().cloned().collect(),
            bm25: meta.bm25_indexes.keys().cloned().collect(),
            hnsw: meta.hnsw_indexes.keys().cloned().collect(),
        }
    }

    fn retain_committed(&self, meta: &mut CollectionMetadata) {
        meta.btree_indexes
            .retain(|name, _| self.btree.contains(name));
        meta.bm25_indexes.retain(|name, _| self.bm25.contains(name));
        meta.hnsw_indexes.retain(|name, _| self.hnsw.contains(name));
    }
}

/// Which end of the matching set a bounded query keeps.
///
/// This is an **input** chosen by the caller, never inferred from the filter:
/// [`Collection::query_ids`] asks for the smallest ids, and
/// [`Collection::query_last_ids`] for the largest. Results always come back
/// in ascending id order — the direction decides *which* ids a bounded query
/// selects, not how they are ordered. Only walks over the id set itself
/// (`_id` filters and complements) run in the requested direction and stop
/// early; a B-tree field scan walks the *key* space, whose order is not id
/// order, so it is evaluated in full and trimmed afterwards.
///
/// Deriving it from the filter instead is what made 0.11.0 return opposite
/// pages for the same predicate depending on where it sat: a bare
/// `_id Lt cursor` kept the newest ids (its scan walks backwards), while
/// `And([user Eq, _id Lt cursor])` kept the oldest (composite filters evaluate
/// their operands unbounded and collect ascending). Guessing from the AST also
/// made logically equivalent filters (`Between(5, 14)` vs `And(Ge(5), Lt(15))`)
/// return disjoint pages, which is why the guess was removed rather than fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanOrder {
    Ascending,
    Descending,
}

impl ScanOrder {
    /// Whether scans should walk the key space from the largest key down.
    fn is_descending(self) -> bool {
        matches!(self, ScanOrder::Descending)
    }

    /// Keep the requested end without allocating space for every match.
    fn retain_id(self, ids: &mut BTreeSet<DocumentId>, id: DocumentId, limit: usize) {
        if limit > 0 && ids.len() == limit {
            let outside = match self {
                Self::Ascending => ids.last().is_some_and(|last| id >= *last),
                Self::Descending => ids.first().is_some_and(|first| id <= *first),
            };
            if outside {
                return;
            }
        }
        ids.insert(id);
        if limit > 0 && ids.len() > limit {
            match self {
                Self::Ascending => ids.pop_last(),
                Self::Descending => ids.pop_first(),
            };
        }
    }

    /// Trims an ascending `result` down to `limit`, keeping the requested end.
    fn truncate(self, result: &mut Vec<DocumentId>, limit: usize) {
        if limit == 0 || result.len() <= limit {
            return;
        }

        match self {
            ScanOrder::Ascending => result.truncate(limit),
            ScanOrder::Descending => {
                result.drain(0..(result.len() - limit));
            }
        }
    }
}

/// Collection configuration parameters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CollectionConfig {
    /// Collection name
    pub name: String,

    /// Collection description
    pub description: String,
}

/// Collection metadata containing configuration, schema, indexes, and statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// Collection configuration.
    pub config: CollectionConfig,

    /// Schema defining the structure of documents in this collection
    pub schema: Schema,

    /// Map of BTree index names to their field entries
    pub btree_indexes: BTreeMap<String, FieldEntry>,

    /// Map of BM25 index names to their field entries
    pub bm25_indexes: BTreeMap<String, FieldEntry>,

    /// Map of HNSW index names to their field entries
    pub hnsw_indexes: BTreeMap<String, FieldEntry>,

    /// Collection statistics.
    pub stats: CollectionStats,

    /// User-defined lightweight extension data persisted with collection metadata.
    #[serde(default)]
    pub extensions: BTreeMap<String, FieldValue>,
}

/// Statistics about the collection's usage and state.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CollectionStats {
    /// Highest document ID assigned so far
    pub max_document_id: u64,

    /// Last insertion timestamp (unix ms).
    pub last_inserted: u64,

    /// Last update timestamp (unix ms).
    pub last_updated: u64,

    /// Last deletion timestamp (unix ms).
    pub last_deleted: u64,

    /// Last saved timestamp (unix ms).
    pub last_saved: u64,

    /// Updated version for the collection. It will be incremented when the collection is updated.
    pub version: u64,

    /// Number of documents in the collection.
    pub num_documents: u64,

    /// Number of search operations performed.
    pub search_count: u64,

    /// Number of get operations performed.
    pub get_count: u64,

    /// Number of insert operations performed.
    pub insert_count: u64,

    /// Number of update operations performed.
    pub update_count: u64,

    /// Number of delete operations performed.
    pub delete_count: u64,

    /// Whether the collection handle is in read-only mode.
    ///
    /// This is **live handle state, not durable state**: [`Collection::open`]
    /// always starts a handle as writable, so the flag is deliberately
    /// written as `false` in every persisted snapshot (see
    /// [`Collection::store_metadata`]). Reading it back from a stored
    /// snapshot says nothing about the collection; only
    /// [`Collection::stats`] / [`Collection::metadata`] report the live value.
    pub read_only: bool,
}

/// Read-only access to a collection-owned B-tree index.
///
/// Collection-owned indexes are deliberately exposed through a query-only
/// view: mutating or flushing the raw wrapper would bypass the collection's
/// lifecycle lease and document/index recovery journal.
#[derive(Debug, Clone, Copy)]
pub struct BTreeIndexView<'a> {
    inner: &'a BTree,
}

impl BTreeIndexView<'_> {
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn virtual_field(&self) -> &[String] {
        self.inner.virtual_field()
    }

    pub fn allow_duplicates(&self) -> bool {
        self.inner.allow_duplicates()
    }

    pub fn stats(&self) -> BTreeStats {
        self.inner.stats()
    }

    pub fn metadata(&self) -> BTreeMetadata {
        self.inner.metadata()
    }

    pub fn query_with<F, R>(&self, field_value: &Fv, f: F) -> Option<R>
    where
        F: FnOnce(&Vec<DocumentId>) -> Option<R>,
    {
        self.inner.query_with(field_value, f)
    }

    pub fn try_range_query_ids<F>(
        &self,
        query: RangeQuery<Fv>,
        descending: bool,
        f: F,
    ) -> Result<(), DBError>
    where
        F: FnMut(&[DocumentId]) -> bool,
    {
        self.inner.try_range_query_ids(query, descending, f)
    }

    pub fn range_query_with<F, R>(&self, query: RangeQuery<Fv>, f: F) -> Vec<R>
    where
        F: FnMut(Fv, &Vec<DocumentId>) -> (bool, Vec<R>),
    {
        self.inner.range_query_with(query, f)
    }

    /// Capped upper estimate based on posting lengths, without copying ids.
    pub fn estimate_cardinality(
        &self,
        query: RangeQuery<Fv>,
        cap: usize,
    ) -> Result<usize, DBError> {
        self.inner.estimate_cardinality(query, cap)
    }

    pub fn keys(&self, cursor: Option<String>, limit: Option<usize>) -> Vec<Fv> {
        self.inner.keys(cursor, limit)
    }
}

/// Read-only access to a collection-owned BM25 index.
#[derive(Debug, Clone, Copy)]
pub struct BM25IndexView<'a> {
    inner: &'a BM25,
}

impl BM25IndexView<'_> {
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn virtual_field(&self) -> &[String] {
        self.inner.virtual_field()
    }

    pub fn stats(&self) -> BM25Stats {
        self.inner.stats()
    }

    pub fn metadata(&self) -> BM25Metadata {
        self.inner.metadata()
    }

    pub fn search(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Vec<(DocumentId, f32)> {
        self.inner.search(query, top_k, params)
    }

    pub fn search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Vec<(DocumentId, f32)> {
        self.inner.search_advanced(query, top_k, params)
    }

    /// Scores `ids` as a corpus of their own: document count, average length
    /// and document frequencies come from the scope alone, so documents
    /// outside it (another tenant, a record the caller may not see) cannot
    /// move a score inside it. Ids without indexed text are ignored.
    pub fn search_scoped(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
        ids: &[DocumentId],
    ) -> Vec<(DocumentId, f32)> {
        self.inner.search_scoped(query, top_k, params, ids)
    }

    /// Scoped top-k using a caller's total score/tie order.
    pub fn search_scoped_by<F>(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
        ids: &[u64],
        compare: F,
    ) -> Vec<(u64, f32)>
    where
        F: Fn(&(u64, f32), &(u64, f32)) -> std::cmp::Ordering,
    {
        self.inner
            .search_scoped_by(query, top_k, params, ids, compare)
    }

    /// Precomputes reusable corpus membership and length statistics.
    pub fn prepare_scope(&self, ids: &[u64]) -> anda_db_tfs::PreparedScope {
        self.inner.prepare_scope(ids)
    }
    /// Searches a prepared corpus with stable caller-defined boundary ties.
    pub fn search_prepared_by<F>(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
        scope: &anda_db_tfs::PreparedScope,
        compare: F,
    ) -> Vec<(u64, f32)>
    where
        F: Fn(&(u64, f32), &(u64, f32)) -> std::cmp::Ordering,
    {
        self.inner
            .search_prepared_by(query, top_k, params, scope, compare)
    }

    pub fn try_search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Result<Vec<(DocumentId, f32)>, DBError> {
        self.inner.try_search_advanced(query, top_k, params)
    }
}

/// Read-only access to a collection-owned HNSW index.
#[derive(Debug, Clone, Copy)]
pub struct HnswIndexView<'a> {
    inner: &'a Hnsw,
}

impl HnswIndexView<'_> {
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn field_name(&self) -> &str {
        self.inner.field_name()
    }

    pub fn dimension(&self) -> usize {
        self.inner.dimension()
    }

    pub fn stats(&self) -> HnswStats {
        self.inner.stats()
    }

    pub fn metadata(&self) -> HnswMetadata {
        self.inner.metadata()
    }

    pub fn try_search(
        &self,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<(DocumentId, f32)>, DBError> {
        self.inner.try_search(query, top_k)
    }

    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<(DocumentId, f32)> {
        self.inner.search(query, top_k)
    }
}

/// Normal unique-key transactions share admission and hold only the relevant
/// key stripes. A purge without a readable document takes exclusive admission.
struct UniqueKeyLease<'a> {
    _admission: tokio::sync::OwnedRwLockReadGuard<()>,
    _keys: Vec<tokio::sync::MutexGuard<'a, ()>>,
}

/// Recorded index changes, undone in reverse execution order. Unlike a map,
/// this also retains a partially applied operation that returned an error.
enum IndexUndo<'a> {
    BTreeAdded(&'a BTree, Cow<'a, FieldValue>),
    BTreeChanged(&'a BTree, Cow<'a, FieldValue>, Cow<'a, FieldValue>),
    BM25Added(&'a BM25, Cow<'a, str>),
    BM25Removed(&'a BM25, Cow<'a, str>),
    HnswAdded(&'a Hnsw),
    HnswRemoved(&'a Hnsw, Cow<'a, Vector>),
}

impl Debug for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Collection({})", self.name)
    }
}

impl Collection {
    /// Path to the collection metadata file
    const METADATA_PATH: &'static str = "meta.cbor";

    /// Path to the document IDs bitmap file
    const IDS_PATH: &'static str = "ids.cbor";

    /// Prefix for durable update/remove intents.
    const MUTATION_INTENT_PREFIX: &'static str = "mutation_intents/";

    /// Path of the durable allocation watermark object (a single `u64`).
    const ALLOCATION_WATERMARK_PATH: &'static str = "alloc_watermark.cbor";

    /// How far the allocation watermark is published ahead of the highest
    /// allocated id. One small PUT per this many adds replaces the previous
    /// one-durable-intent-per-add write amplification; the reopen repair scan
    /// probes at most this many ids beyond the last observed allocation.
    const ALLOCATION_WATERMARK_STRIDE: u64 = 64;

    /// Number of stripes in `doc_locks`. Power of two so the modulo is cheap.
    const DOC_LOCK_STRIPES: usize = 128;

    /// Maximum accepted `Query::limit` for `search_ids`; larger values are
    /// clamped. Bounds both the result size and the per-index recall breadth
    /// (`limit * 10`).
    pub const MAX_SEARCH_LIMIT: usize = 1000;

    fn new_doc_locks() -> Vec<tokio::sync::Mutex<()>> {
        (0..Self::DOC_LOCK_STRIPES)
            .map(|_| tokio::sync::Mutex::new(()))
            .collect()
    }

    /// Returns the stripe lock guarding mutations of document `id`.
    fn doc_lock(&self, id: DocumentId) -> &tokio::sync::Mutex<()> {
        &self.doc_locks[(id as usize) % Self::DOC_LOCK_STRIPES]
    }

    fn mutation_intent_path(sequence: u64) -> String {
        format!("{}{sequence:020}.cbor", Self::MUTATION_INTENT_PREFIX)
    }

    fn lifecycle_error(&self) -> DBError {
        DBError::Generic {
            name: self.name.clone(),
            // Typed, not stringly: `DBError::collection_state` downcasts this
            // back out so callers can tell "reopen and retry" from "give up"
            // without reading the message.
            source: CollectionStateError(self.state()).into(),
        }
    }

    /// Adds `id` to both id representations. Returns whether membership changed.
    fn register_doc_id(&self, id: DocumentId) -> bool {
        self.doc_ids.write().insert(id)
    }

    /// Drops `id` from the id set and its persisted bitmap form. Returns
    /// whether the set changed.
    fn unregister_doc_id(&self, id: DocumentId) -> bool {
        self.doc_ids.write().remove(&id)
    }

    /// Whether this handle or its database is in read-only mode.
    fn is_read_only(&self) -> bool {
        self.database_read_only.load(Ordering::Acquire) || self.read_only.load(Ordering::Acquire)
    }

    fn ensure_mutable(&self) -> Result<(), DBError> {
        if self.lifecycle.load(Ordering::Acquire) != LIFECYCLE_ACTIVE {
            return Err(self.lifecycle_error());
        }
        if self.is_read_only() {
            return Err(DBError::Generic {
                name: self.name.clone(),
                source: "Collection is read-only".into(),
            });
        }
        Ok(())
    }

    /// Returns the handle's current lifecycle state.
    ///
    /// Any state other than [`CollectionState::Active`] rejects operations
    /// with an error that carries the same value; see
    /// [`DBError::collection_state`] to classify an error you already hold
    /// without a second call.
    pub fn state(&self) -> CollectionState {
        match self.lifecycle.load(Ordering::Acquire) {
            LIFECYCLE_CLOSING => CollectionState::Closing,
            LIFECYCLE_CLOSED => CollectionState::Closed,
            LIFECYCLE_DELETING => CollectionState::Deleting,
            LIFECYCLE_DELETED => CollectionState::Deleted,
            LIFECYCLE_POISONED => CollectionState::Poisoned,
            _ => CollectionState::Active,
        }
    }

    /// Returns whether this registered handle still admits new operations.
    pub(crate) fn is_active_handle(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == LIFECYCLE_ACTIVE
    }

    /// Returns whether this handle was poisoned by a cancelled mutation.
    ///
    /// A poisoned handle rejects mutations; reopening the collection
    /// through the database discards it and loads a consistent generation from
    /// storage. Prefer [`DBError::is_poisoned`] when you already hold the
    /// error a rejected call returned.
    pub fn is_poisoned(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == LIFECYCLE_POISONED
    }

    /// Waits until every operation already admitted on this handle has
    /// drained. New operations are rejected by the terminal lifecycle state,
    /// so acquiring the exclusive gate once guarantees quiescence.
    pub(crate) async fn drain_operations(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.operation_gate.clone().write_owned().await
    }

    /// Transitions the handle to [`LIFECYCLE_POISONED`] after a mutating
    /// future was dropped mid-operation. Delete states are preserved: a
    /// deletion in progress already rejects mutations and its partial
    /// storage removal is not recoverable by reopening anyway.
    fn poison(&self, action: &'static str) {
        loop {
            let state = self.lifecycle.load(Ordering::Acquire);
            if !matches!(state, LIFECYCLE_ACTIVE | LIFECYCLE_CLOSING) {
                return;
            }
            if self
                .lifecycle
                .compare_exchange(
                    state,
                    LIFECYCLE_POISONED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                log::error!(
                    action = action,
                    collection = self.name;
                    "Mutating operation was cancelled mid-flight; the collection handle is poisoned and must be reopened",
                );
                return;
            }
        }
    }

    /// Arms a [`CancelGuard`] for `action`. Cancellation of the wrapped
    /// future is treated as a crash: recovery happens on reopen, never
    /// in place.
    fn cancel_guard(&self, action: &'static str) -> CancelGuard<'_> {
        CancelGuard {
            collection: self,
            action,
            armed: true,
        }
    }

    /// Runs `fut` under a [`CancelGuard`]: dropping the returned future
    /// before completion poisons the handle, completing it (with either
    /// result) disarms the guard.
    async fn guarded<F, T>(&self, action: &'static str, fut: F) -> T
    where
        F: Future<Output = T>,
    {
        let guard = self.cancel_guard(action);
        let rt = fut.await;
        guard.disarm();
        rt
    }

    /// Acquires an active-operation lease.  The state is deliberately checked
    /// after the shared gate is acquired: close/delete publish their terminal
    /// state before waiting for the exclusive gate, so queued operations are
    /// rejected instead of slipping in behind the drain boundary.
    async fn mutation_lease(&self) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, DBError> {
        self.ensure_recovered().await?;
        let guard = self.operation_gate.clone().read_owned().await;
        self.ensure_mutable()?;
        Ok(guard)
    }

    fn has_unique_indexes(&self) -> bool {
        self.btree_indexes
            .iter()
            .any(|index| !index.allow_duplicates())
    }

    async fn unique_key_lease<'a>(
        &'a self,
        documents: &[&Document],
    ) -> Result<Option<UniqueKeyLease<'a>>, DBError> {
        if !self.has_unique_indexes() {
            return Ok(None);
        }
        let admission = self.unique_commit_gate.clone().read_owned().await;
        const STRIPES: usize = 256;
        let mut stripes = Vec::new();
        for document in documents {
            for index in self
                .btree_indexes
                .iter()
                .filter(|index| !index.allow_duplicates())
            {
                if let Some(value) = self.index_hooks.btree_index_value(index, document) {
                    stripes.extend(index.lock_stripes(&value, STRIPES)?);
                }
            }
        }
        stripes.sort_unstable();
        stripes.dedup();
        let locks = self
            .unique_key_locks
            .get_or_init(|| (0..STRIPES).map(|_| tokio::sync::Mutex::new(())).collect());
        let mut keys = Vec::with_capacity(stripes.len());
        for stripe in stripes {
            keys.push(locks[stripe].lock().await);
        }
        // A previous holder may have been cancelled or failed while we waited.
        self.ensure_mutable()?;
        Ok(Some(UniqueKeyLease {
            _admission: admission,
            _keys: keys,
        }))
    }

    async fn ensure_recovered(&self) -> Result<(), DBError> {
        if !self.recovery_pending.load(Ordering::Acquire) {
            return Ok(());
        }
        let _recovery = self.recovery_gate.lock().await;
        if !self.recovery_pending.load(Ordering::Acquire) {
            return Ok(());
        }
        let _operations = self.operation_gate.clone().write_owned().await;
        if !self.is_active_handle() {
            return Err(self.lifecycle_error());
        }
        let result = self
            .guarded("Collection::recover", async {
                self.replay_mutation_intents().await?;
                self.auto_repair_indexes().await?;
                Ok(())
            })
            .await;
        match result {
            Ok(()) => self.recovery_pending.store(false, Ordering::Release),
            Err(_) => self.poison("Collection::recover"),
        }
        result
    }

    /// Sets the per-phase concurrency of document reads, recovery and intent
    /// retirement (1..=64, default 8). This runtime setting is not persisted.
    pub fn set_io_concurrency(&self, concurrency: usize) -> Result<(), DBError> {
        if !(1..=64).contains(&concurrency) {
            return Err(DBError::Generic {
                name: self.name.clone(),
                source: "I/O concurrency must be between 1 and 64".into(),
            });
        }
        self.io_concurrency.store(concurrency, Ordering::Release);
        Ok(())
    }

    fn io_concurrency(&self) -> usize {
        self.io_concurrency.load(Ordering::Acquire)
    }

    /// Stored documents skipped during this handle's recovery because their
    /// bytes or schema are invalid. Transient storage errors fail recovery.
    pub fn recovery_issues(&self) -> BTreeMap<DocumentId, String> {
        self.recovery_issues.read().clone()
    }

    /// Generates the storage path for a document with the given ID
    fn doc_path(id: DocumentId) -> String {
        format!("data/{id}.cbor")
    }

    /// Installs the tokenizer in this collection and its loaded BM25 indexes.
    /// Call this at the start of an open callback, before any document/query
    /// operation or index creation triggers recovery. Reinstall the same
    /// policy on every open; changing policy requires rebuilding the indexes.
    ///
    /// # Arguments
    /// * `tokenizer` - The tokenizer chain to use
    pub fn set_tokenizer(&mut self, tokenizer: TokenizerChain) {
        for index in &mut self.bm25_indexes {
            index.set_tokenizer(tokenizer.clone());
        }
        self.tokenizer = tokenizer;
    }

    /// Replaces the strategy used to derive indexable values from documents.
    ///
    /// Custom hooks are useful for virtual fields, precomputed search text, or
    /// alternative vector encodings that should be indexed without changing the
    /// stored document shape. Hooks must be deterministic and installed before
    /// the first open-callback operation triggers recovery.
    ///
    /// # Constraint
    ///
    /// [`Collection::update`] only refreshes an index when one of the updated
    /// fields is part of that index's declared field list
    /// (`index.virtual_field()` / the HNSW field name). A hook that derives an
    /// index value from *other* fields will therefore go stale on updates that
    /// touch only those other fields. Keep hook inputs within the index's
    /// declared fields, or update the declared fields together with the
    /// derived-from fields.
    pub fn set_index_hooks(&mut self, hooks: Arc<dyn IndexHooks>) {
        self.index_hooks = hooks;
    }

    /// Returns the collection name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the collection schema.
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Returns the collection metadata.
    /// This includes up-to-date statistics about the collection.
    pub fn metadata(&self) -> CollectionMetadata {
        let mut metadata = self.metadata.read().clone();
        self.overlay_live_stats(&mut metadata.stats);
        metadata
    }

    /// Gets current statistics about the collection
    pub fn stats(&self) -> CollectionStats {
        let mut stats = self.metadata.read().stats.clone();
        self.overlay_live_stats(&mut stats);
        stats
    }

    /// Replaces the counters kept outside the metadata lock with their live
    /// values.
    fn overlay_live_stats(&self, stats: &mut CollectionStats) {
        stats.max_document_id = self.max_document_id.load(Ordering::Relaxed);
        stats.num_documents = self.doc_ids.read().len() as u64;
        stats.search_count = self.search_count.load(Ordering::Relaxed);
        stats.get_count = self.get_count.load(Ordering::Relaxed);
        stats.read_only = self.is_read_only();
    }

    /// Returns the storage-level I/O statistics for this collection.
    pub fn storage_stats(&self) -> StorageStats {
        self.storage.stats()
    }

    /// Returns the maximum document ID in the collection.
    pub fn max_document_id(&self) -> DocumentId {
        self.max_document_id.load(Ordering::Relaxed)
    }

    /// Returns the latest (highest) document ID in the collection, if any.
    pub fn latest_document_id(&self) -> Option<DocumentId> {
        self.doc_ids.read().last()
    }

    /// Returns a vector of all document IDs in the collection in ascending order.
    pub fn ids(&self) -> Vec<DocumentId> {
        self.doc_ids.read().iter().collect()
    }

    /// Checks if a document with the given ID exists in the collection.
    ///
    /// # Arguments
    /// * `id` - The ID to check
    ///
    /// # Returns
    /// `true` if a document with the ID exists, `false` otherwise
    pub fn contains(&self, id: DocumentId) -> bool {
        self.doc_ids.read().contains(&id)
    }

    /// Gets the number of documents in the collection.
    ///
    /// # Returns
    /// The number of documents in the collection
    pub fn len(&self) -> usize {
        self.doc_ids.read().len()
    }

    /// Checks if the collection is empty.
    ///
    /// # Returns
    /// `true` if the collection contains no documents, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.doc_ids.read().is_empty()
    }

    /// Creates a document with the collection's schema and `_id: 0` placeholder.
    /// Fill the business fields and pass it to [`Self::add`] to allocate its id.
    pub fn new_document(&self) -> Document {
        let mut doc = Document::new(self.schema.clone());
        doc.set_id(0);
        doc
    }

    /// Updates the collection metadata with the provided function.
    ///
    /// # Arguments
    /// * `f` - A function that modifies the collection metadata
    fn update_metadata<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut CollectionMetadata) -> R,
    {
        let mut metadata = self.metadata.write();
        f(&mut metadata)
    }
}

mod crud;
mod extensions;
mod ids;
use ids::DocumentIds;
mod index_ops;
mod lifecycle;
mod persistence;
mod query;
mod recovery;
#[cfg(test)]
mod tests;
