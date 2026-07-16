use anda_db_utils::UniqueVec;
use croaring::{Portable, Treemap};
use futures::{StreamExt, future::try_join_all, try_join as try_join_await};
use object_store::path::Path;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};
use std::{borrow::Cow, time::Instant};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

use crate::{
    database::AndaDB,
    error::DBError,
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
    /// BTree index for document IDs
    doc_ids_index: RwLock<BTreeSet<DocumentId>>,
    /// Bitmap of document IDs for efficient membership tests
    doc_ids: RwLock<Treemap>,
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
    /// Last saved version of the collection
    last_saved_version: AtomicU64,

    metadata_version: RwLock<ObjectVersion>,
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
    pending_mutations: parking_lot::Mutex<BTreeMap<u64, MutationIntent>>,
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
/// reconcile. A poisoned handle rejects every further operation; reopening
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

    /// Whether the collection is in read-only mode
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

    pub fn try_range_query_ids<F>(&self, query: RangeQuery<Fv>, f: F) -> Result<(), DBError>
    where
        F: FnMut(&[DocumentId]) -> bool,
    {
        self.inner.try_range_query_ids(query, f)
    }

    pub fn range_query_with<F, R>(&self, query: RangeQuery<Fv>, f: F) -> Vec<R>
    where
        F: FnMut(Fv, &Vec<DocumentId>) -> (bool, Vec<R>),
    {
        self.inner.range_query_with(query, f)
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

    /// Upper bound for limit-driven speculative pre-allocations, so a huge
    /// caller-supplied limit (e.g. via `query_ids`) cannot allocate excessive
    /// memory up front. Result vectors still grow on demand beyond this hint.
    const MAX_RESERVE_HINT: usize = 1024;

    /// Number of stripes in `doc_locks`. Power of two so the modulo is cheap.
    const DOC_LOCK_STRIPES: usize = 128;

    /// Maximum accepted `Query::limit` for `search_ids`; larger values are
    /// clamped. Bounds both the result size and the per-index recall breadth
    /// (`limit * 10`).
    pub const MAX_SEARCH_LIMIT: usize = 1000;

    /// Returns a safe pre-allocation size for a caller-supplied limit.
    #[inline]
    fn reserve_hint(limit: usize) -> usize {
        limit.min(Self::MAX_RESERVE_HINT)
    }

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
        let state = match self.lifecycle.load(Ordering::Acquire) {
            LIFECYCLE_CLOSING => "closing",
            LIFECYCLE_CLOSED => "closed",
            LIFECYCLE_DELETING => "being deleted",
            LIFECYCLE_DELETED => "deleted",
            LIFECYCLE_POISONED => {
                "poisoned (a mutating call was cancelled mid-operation); reopen the collection to recover"
            }
            _ => "not writable",
        };
        DBError::Generic {
            name: self.name.clone(),
            source: format!("Collection handle is {state}").into(),
        }
    }

    fn ensure_mutable(&self) -> Result<(), DBError> {
        if self.lifecycle.load(Ordering::Acquire) != LIFECYCLE_ACTIVE {
            return Err(self.lifecycle_error());
        }
        if self.database_read_only.load(Ordering::Acquire) || self.read_only.load(Ordering::Acquire)
        {
            return Err(DBError::Generic {
                name: self.name.clone(),
                source: "Collection is read-only".into(),
            });
        }
        Ok(())
    }

    /// Returns whether this registered handle still admits new operations.
    pub(crate) fn is_active_handle(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == LIFECYCLE_ACTIVE
    }

    /// Returns whether this handle was poisoned by a cancelled mutation.
    pub(crate) fn is_poisoned(&self) -> bool {
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
    /// deletion in progress already rejects every operation and its partial
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

    /// Acquires an active-operation lease.  The state is deliberately checked
    /// after the shared gate is acquired: close/delete publish their terminal
    /// state before waiting for the exclusive gate, so queued operations are
    /// rejected instead of slipping in behind the drain boundary.
    async fn mutation_lease(&self) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, DBError> {
        let guard = self.operation_gate.clone().read_owned().await;
        self.ensure_mutable()?;
        Ok(guard)
    }

    /// Generates the storage path for a document with the given ID
    fn doc_path(id: DocumentId) -> String {
        format!("data/{id}.cbor")
    }

    /// Creates a new collection with the given schema and configuration.
    ///
    /// # Arguments
    /// * `db` - Reference to the database this collection belongs to
    /// * `schema` - Schema defining the structure of documents in this collection
    /// * `config` - Configuration parameters for the collection
    ///
    /// # Returns
    /// A new Collection instance or an error if creation fails
    pub(crate) async fn create(
        db: AndaDB,
        schema: Schema,
        config: CollectionConfig,
    ) -> Result<Self, DBError> {
        validate_field_name(config.name.as_str())?;

        let base_path = Path::from(db.name()).join(config.name.as_str());
        let db_metadata = db.metadata();
        if db_metadata.collections.contains(&config.name) {
            return Err(DBError::AlreadyExists {
                name: config.name,
                path: base_path.to_string(),
                source: "".into(),
                _id: 0,
            });
        }

        let storage = Storage::connect(
            base_path.to_string(),
            db.object_store(),
            db_metadata.config.storage.clone(),
        )
        .await?;
        let stats = CollectionStats {
            version: 1,
            ..Default::default()
        };
        let metadata = CollectionMetadata {
            config: config.clone(),
            schema: schema.clone(),
            btree_indexes: BTreeMap::new(),
            bm25_indexes: BTreeMap::new(),
            hnsw_indexes: BTreeMap::new(),
            stats,
            extensions: BTreeMap::new(),
        };

        let metadata_version = storage.create(Self::METADATA_PATH, &metadata).await?;
        let doc_ids = Treemap::new();
        let ids_data = {
            let mut ids = doc_ids.clone();
            ids.run_optimize();
            ids.serialize::<Portable>()
        };
        let ids_version = match storage.create(Self::IDS_PATH, &ids_data).await {
            Ok(ver) => ver,
            Err(err) => {
                // Remove the metadata object written above, otherwise the
                // half-created collection blocks re-creation under this name.
                let _ = storage.delete(Self::METADATA_PATH).await;
                return Err(err);
            }
        };

        // created successfully, and store storage metadata
        storage.store_metadata(0, unix_ms()).await?;

        Ok(Self {
            name: config.name.clone(),
            schema: Arc::new(schema),
            storage,
            btree_indexes: Vec::new(),
            bm25_indexes: Vec::new(),
            hnsw_indexes: Vec::new(),
            max_document_id: AtomicU64::new(0),
            search_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            tokenizer: default_tokenizer(),
            doc_ids_index: RwLock::new(BTreeSet::new()),
            doc_ids: RwLock::new(Treemap::new()),
            metadata: RwLock::new(metadata),
            read_only: AtomicBool::new(false),
            database_read_only: db.read_only_flag(),
            lifecycle: AtomicU8::new(LIFECYCLE_ACTIVE),
            operation_gate: Arc::new(tokio::sync::RwLock::new(())),
            last_saved_version: AtomicU64::new(0),
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
            index_hooks: Arc::new(DefaultIndexHooks),
            doc_locks: Self::new_doc_locks(),
            pending_mutations: parking_lot::Mutex::new(BTreeMap::new()),
            extension_write_gate: tokio::sync::Mutex::new(()),
            next_mutation_sequence: AtomicU64::new(unix_ms()),
            durable_alloc_watermark: AtomicU64::new(0),
            watermark_gate: tokio::sync::Mutex::new(()),
        })
    }

    /// Opens an existing collection.
    ///
    /// # Arguments
    /// * `db` - Reference to the database this collection belongs to
    /// * `name` - Name of the collection to open
    /// * `f` - Function to execute on the collection before it's fully loaded
    ///
    /// # Returns
    /// The opened Collection instance or an error if opening fails
    pub(crate) async fn open<F>(
        db: AndaDB,
        name: String,
        schema: Option<Schema>,
        f: F,
    ) -> Result<Self, DBError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        validate_field_name(name.as_str())?;
        let base_path = Path::from(db.name()).join(name.as_str());
        let db_metadata = db.metadata();
        let storage = Storage::connect(
            base_path.to_string(),
            db.object_store(),
            db_metadata.config.storage.clone(),
        )
        .await?;

        let (metadata, metadata_version) = storage
            .fetch::<CollectionMetadata>(Self::METADATA_PATH)
            .await?;

        let (ids, ids_version) = storage.fetch::<Vec<u8>>(Self::IDS_PATH).await?;
        let doc_ids =
            Treemap::try_deserialize::<Portable>(&ids).ok_or_else(|| DBError::Generic {
                name: name.clone(),
                source: "Failed to deserialize ids".into(),
            })?;
        let doc_ids_index = BTreeSet::from_iter(doc_ids.iter());

        // The durable allocation watermark bounds the id window the repair
        // scan below must probe. Collections created before the watermark
        // existed load as 0; the metadata max keeps their bound intact.
        let alloc_watermark = match storage.fetch::<u64>(Self::ALLOCATION_WATERMARK_PATH).await {
            Ok((watermark, _)) => watermark,
            Err(DBError::NotFound { .. }) => 0,
            Err(err) => return Err(err),
        };
        let metadata_max_document_id = metadata.stats.max_document_id;

        let mut collection = Self {
            name,
            schema: Arc::new(metadata.schema.clone()),
            storage,
            btree_indexes: Vec::new(),
            bm25_indexes: Vec::new(),
            hnsw_indexes: Vec::new(),
            max_document_id: AtomicU64::new(metadata.stats.max_document_id),
            search_count: AtomicU64::new(metadata.stats.search_count),
            get_count: AtomicU64::new(metadata.stats.get_count),
            last_saved_version: AtomicU64::new(metadata.stats.version),
            tokenizer: default_tokenizer(),
            doc_ids_index: RwLock::new(doc_ids_index),
            doc_ids: RwLock::new(doc_ids),
            metadata: RwLock::new(metadata),
            read_only: AtomicBool::new(false),
            database_read_only: db.read_only_flag(),
            lifecycle: AtomicU8::new(LIFECYCLE_ACTIVE),
            operation_gate: Arc::new(tokio::sync::RwLock::new(())),
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
            index_hooks: Arc::new(DefaultIndexHooks),
            doc_locks: Self::new_doc_locks(),
            pending_mutations: parking_lot::Mutex::new(BTreeMap::new()),
            extension_write_gate: tokio::sync::Mutex::new(()),
            next_mutation_sequence: AtomicU64::new(unix_ms()),
            durable_alloc_watermark: AtomicU64::new(alloc_watermark.max(metadata_max_document_id)),
            watermark_gate: tokio::sync::Mutex::new(()),
        };
        collection.load_indexes().await?;

        if let Some(schema) = schema {
            collection.try_upgrade_schema(schema).await?;
        }

        // The callback installs custom index hooks and may add indexes. Run it
        // before replay/repair so recovery derives values with the same
        // application semantics as normal CRUD. Replaying once with default
        // hooks would leave B-tree/BM25 phantom entries that a later replay
        // with custom hooks cannot identify and remove.
        f(&mut collection).await?;

        let replayed = collection.replay_mutation_intents().await?;
        if replayed > 0 {
            log::warn!(
                action = "Collection::replay_mutation_intents",
                collection = collection.name;
                "Replayed {replayed} uncheckpointed document mutations",
            );
        }
        let fixed = collection.auto_repair_indexes().await?;
        if fixed > 0 {
            log::warn!(
                action = "Collection::auto_repair_indexes",
                collection = collection.name;
                "Auto-repaired {fixed} documents",
            );
        }

        Ok(collection)
    }

    /// Loads all indexes from storage.
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if loading fails
    async fn load_indexes(&mut self) -> Result<(), DBError> {
        let meta = { self.metadata.read().clone() };
        let (btree_indexes, bm25_indexes, hnsw_indexes) = try_join_await!(
            async {
                let mut btree_indexes = Vec::new();
                for (name, field) in meta.btree_indexes.iter() {
                    let index =
                        BTree::bootstrap(name.clone(), field.r#type(), self.storage.clone())
                            .await?;
                    if field.unique() {
                        btree_indexes.insert(0, index);
                    } else {
                        btree_indexes.push(index);
                    }
                }
                Ok::<Vec<BTree>, DBError>(btree_indexes)
            },
            async {
                let mut bm25_indexes = Vec::new();
                for (name, _) in meta.bm25_indexes.iter() {
                    let index =
                        BM25::bootstrap(name.clone(), self.tokenizer.clone(), self.storage.clone())
                            .await?;

                    bm25_indexes.push(index);
                }
                Ok::<Vec<BM25>, DBError>(bm25_indexes)
            },
            async {
                let mut hnsw_indexes = Vec::new();
                for (name, _) in meta.hnsw_indexes.iter() {
                    let index = Hnsw::bootstrap(name.clone(), self.storage.clone()).await?;

                    hnsw_indexes.push(index);
                }
                Ok::<Vec<Hnsw>, DBError>(hnsw_indexes)
            },
        )?;

        self.btree_indexes = btree_indexes;
        self.bm25_indexes = bm25_indexes;
        self.hnsw_indexes = hnsw_indexes;
        Ok(())
    }

    fn remove_document_from_indexes(&self, id: DocumentId, doc: &Document, now_ms: u64) {
        for index in &self.btree_indexes {
            if let Some(value) = self.index_hooks.btree_index_value(index, doc)
                && value.as_ref() != &FieldValue::Null
            {
                index.remove(id, &value, now_ms);
            }
        }
        for index in &self.bm25_indexes {
            if let Some(text) = self.index_hooks.bm25_index_value(index, doc) {
                // BM25::remove is intentionally idempotent for replay: even
                // after doc_tokens was removed by an earlier historical
                // value, it still purges postings derived from `text`.
                index.remove(id, &text, now_ms);
            }
        }
        for index in &self.hnsw_indexes {
            index.remove(id, now_ms);
        }
    }

    fn insert_document_into_indexes(
        &self,
        id: DocumentId,
        doc: &Document,
        now_ms: u64,
    ) -> Result<(), DBError> {
        for index in &self.btree_indexes {
            if let Some(value) = self.index_hooks.btree_index_value(index, doc)
                && value.as_ref() != &FieldValue::Null
            {
                index.insert(id, &value, now_ms)?;
            }
        }
        for index in &self.bm25_indexes {
            if let Some(text) = self.index_hooks.bm25_index_value(index, doc) {
                index.insert(id, &text, now_ms)?;
            }
        }
        for index in &self.hnsw_indexes {
            if let Some(vector) = self.index_hooks.hnsw_index_value(index, doc) {
                index.insert(id, vector.into_owned(), now_ms)?;
            }
        }
        Ok(())
    }

    /// Writes an add/update/remove intent before either the in-memory indexes
    /// or the durable document object are changed.
    async fn record_mutation_intent(
        &self,
        id: DocumentId,
        previous: Option<&Document>,
        proposed: Option<&Document>,
    ) -> Result<(), DBError> {
        loop {
            let sequence = self.next_mutation_sequence.fetch_add(1, Ordering::AcqRel);
            let intent = MutationIntent {
                sequence,
                document_id: id,
                previous: previous.map(|document| document.clone().into()),
                proposed: proposed.map(|document| document.clone().into()),
            };
            let path = Self::mutation_intent_path(sequence);
            match self.storage.create(&path, &intent).await {
                Ok(_) => {
                    self.pending_mutations.lock().insert(sequence, intent);
                    return Ok(());
                }
                // A retained intent from an earlier process may use the same
                // wall-clock-derived sequence. Advance until a free path is
                // found instead of overwriting recovery evidence.
                Err(DBError::AlreadyExists { .. }) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Replays intents left by a crash or failed flush. Historical values are
    /// removed first; the document currently present in storage is then the
    /// sole source of truth for both the bitmap and every derived index.
    async fn replay_mutation_intents(&self) -> Result<usize, DBError> {
        let mut stream = self
            .storage
            .list::<MutationIntent>(Some(Self::MUTATION_INTENT_PREFIX), None);
        let mut intents = BTreeMap::<u64, MutationIntent>::new();
        while let Some(intent) = stream.next().await {
            let intent = intent?.0;
            if intent.document_id == 0 {
                return Err(DBError::Serialization {
                    name: self.name.clone(),
                    source: "mutation intent contains reserved document id 0".into(),
                });
            }
            intents.insert(intent.sequence, intent);
        }
        if intents.is_empty() {
            return Ok(0);
        }

        if let Some(last) = intents.last_key_value().map(|(sequence, _)| *sequence) {
            self.next_mutation_sequence
                .fetch_max(last.saturating_add(1), Ordering::AcqRel);
        }
        *self.pending_mutations.lock() = intents.clone();

        self.reconcile_mutation_intents(&intents).await?;
        Ok(intents.len())
    }

    async fn reconcile_mutation_intents(
        &self,
        intents: &BTreeMap<u64, MutationIntent>,
    ) -> Result<(), DBError> {
        let now_ms = unix_ms();
        let mut affected_ids = BTreeSet::new();
        for intent in intents.values() {
            affected_ids.insert(intent.document_id);
            for candidate in [&intent.previous, &intent.proposed].into_iter().flatten() {
                let document = Document::try_from_doc(self.schema(), candidate.clone())?;
                self.remove_document_from_indexes(intent.document_id, &document, now_ms);
            }
        }

        for id in affected_ids {
            match self
                .storage
                .fetch::<DocumentOwned>(&Self::doc_path(id))
                .await
            {
                Ok((current, _)) => {
                    let current = Document::try_from_doc(self.schema(), current)?;
                    // The final state may already have reached some index
                    // objects during a partial flush. Remove it before the
                    // idempotent insert so unique indexes cannot reject their
                    // own surviving posting.
                    self.remove_document_from_indexes(id, &current, now_ms);
                    self.insert_document_into_indexes(id, &current, now_ms)?;
                    self.max_document_id.fetch_max(id, Ordering::AcqRel);
                    self.doc_ids.write().add(id);
                    self.doc_ids_index.write().insert(id);
                }
                Err(DBError::NotFound { .. }) => {
                    // Complete a crashed remove. HNSW can be purged by id even
                    // when no historical vector could be decoded.
                    for index in &self.hnsw_indexes {
                        index.remove(id, now_ms);
                    }
                    self.doc_ids.write().remove(id);
                    self.doc_ids_index.write().remove(&id);
                }
                Err(err) => return Err(err),
            }
        }

        self.update_metadata(|meta| meta.stats.version += 1);
        Ok(())
    }

    async fn clear_mutation_intents(&self) -> Result<(), DBError> {
        let sequences: Vec<u64> = self.pending_mutations.lock().keys().copied().collect();
        for sequence in sequences {
            let path = Self::mutation_intent_path(sequence);
            match self.storage.delete(&path).await {
                Ok(()) | Err(DBError::NotFound { .. }) => {
                    self.pending_mutations.lock().remove(&sequence);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Reconciles the in-memory state with the document objects actually
    /// present in storage, in both directions:
    ///
    /// - documents on disk that are missing from the id bitmap are recovered
    ///   (added to the bitmap and re-indexed), and
    /// - bitmap ids without a backing object are dropped.
    ///
    /// This is an explicit **maintenance API**: unlike the bounded
    /// crash-recovery scan that runs on open ([`Self::auto_repair_indexes`],
    /// which stops after a run of consecutive missing ids and can therefore
    /// miss orphans beyond a large id gap), it lists the entire `data/`
    /// prefix — O(number of documents) in listing cost. Call it during
    /// quiescence (no concurrent writers), e.g. from an admin task after an
    /// unclean shutdown or when `len()` looks inconsistent.
    ///
    /// As a safety net against a concurrent `add`, the dead-id sweep only
    /// considers ids at or below the `max_document_id` snapshot taken before
    /// the listing: a document added after the listing started (present in
    /// the bitmap, absent from the listing) is never dropped.
    ///
    /// Returns `(recovered, dropped)`: documents recovered into the bitmap
    /// and dead ids removed from it. Changes are persisted by the next
    /// `flush()`.
    pub async fn reconcile_storage(&self) -> Result<(usize, usize), DBError> {
        let _operation_lease = self.operation_gate.clone().write_owned().await;
        self.ensure_mutable()?;
        let guard = self.cancel_guard("Collection::reconcile_storage");
        let rt = self.reconcile_storage_impl().await;
        guard.disarm();
        rt
    }

    async fn reconcile_storage_impl(&self) -> Result<(usize, usize), DBError> {
        let now_ms = unix_ms();
        // Ids allocated after this point belong to in-flight `add` calls
        // whose objects may not have been visible to the listing below.
        let scan_max_id = self.max_document_id.load(Ordering::Acquire);

        // Enumerate the ids of every document object under `data/`.
        let mut stored_ids: BTreeSet<DocumentId> = BTreeSet::new();
        {
            let mut stream = self.storage.list_meta(Some("data/"), None);
            while let Some(meta) = stream.next().await {
                let meta = meta?;
                if let Some(id) = meta
                    .location
                    .filename()
                    .and_then(|name| name.strip_suffix(".cbor"))
                    .and_then(|raw| raw.parse::<DocumentId>().ok())
                {
                    stored_ids.insert(id);
                }
            }
        }

        // Direction 1: recover documents that exist on disk but are missing
        // from the bitmap.
        let missing_in_bitmap: Vec<DocumentId> = {
            let doc_ids = self.doc_ids.read();
            stored_ids
                .iter()
                .copied()
                .filter(|id| !doc_ids.contains(*id))
                .collect()
        };
        let mut recovered = 0usize;
        for id in missing_in_bitmap {
            match self
                .storage
                .fetch::<DocumentOwned>(&Self::doc_path(id))
                .await
            {
                Ok((doc, _)) => {
                    if self.repair_document(id, doc, now_ms)? {
                        recovered += 1;
                    }
                }
                // Deleted between listing and fetch; nothing to recover.
                Err(DBError::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }

        // Direction 2: drop bitmap ids whose object no longer exists. Ids
        // beyond the pre-listing snapshot are skipped — they belong to adds
        // that raced the listing, not to dead documents.
        let dead_ids: Vec<DocumentId> = {
            let doc_ids = self.doc_ids.read();
            doc_ids
                .iter()
                .filter(|id| *id <= scan_max_id && !stored_ids.contains(id))
                .collect()
        };
        let dropped = dead_ids.len();
        for id in dead_ids {
            self.heal_missing_doc(id);
        }

        if recovered > 0 || dropped > 0 {
            log::warn!(
                action = "Collection::reconcile_storage",
                collection = self.name,
                recovered = recovered,
                dropped = dropped;
                "Reconciled collection with storage: recovered={recovered}, dropped={dropped}",
            );
        }

        Ok((recovered, dropped))
    }

    /// Crash-recovery scan run on open, after mutation-intent replay.
    ///
    /// Every id that may have a document object lies in
    /// `checkpoint+1 ..= max(max_document_id, allocation watermark)`: an add
    /// publishes the durable watermark before its document object can exist
    /// (see [`Self::ensure_allocation_watermark`]). The scan probes that
    /// exact window — holes (failed or cancelled adds, removed documents)
    /// read as one cheap NotFound each — so no consecutive-miss heuristics
    /// are needed and no committed document can be skipped. The window is
    /// bounded by the mutations since the last successful flush plus one
    /// watermark stride.
    async fn auto_repair_indexes(&self) -> Result<usize, DBError> {
        let check_point = self.storage.stats().check_point;
        let scan_max = self
            .max_document_id
            .load(Ordering::Acquire)
            .max(self.durable_alloc_watermark.load(Ordering::Acquire));

        let now_ms = unix_ms();
        let mut fixed = 0;
        for id in (check_point + 1)..=scan_max {
            match self
                .storage
                .fetch::<DocumentOwned>(&Self::doc_path(id))
                .await
            {
                Err(DBError::NotFound { .. }) => {}
                Err(err) => {
                    // Transient storage errors or corrupt objects are logged
                    // and skipped; `reconcile_storage` remains the manual
                    // backstop once the object is fixed. Burn the id so a
                    // future add cannot collide with the existing object.
                    self.max_document_id.fetch_max(id, Ordering::AcqRel);
                    log::warn!(
                        action = "Collection::auto_repair_indexes",
                        collection = self.name,
                        doc_id = id;
                        "Skipping document with unreadable object during repair scan: {err:?}",
                    );
                }
                Ok((doc, _)) => {
                    if self.repair_document(id, doc, now_ms)? {
                        fixed += 1;
                    }
                }
            }
        }

        if fixed > 0 {
            // Make the recovery observable to the version watermark so the
            // flush that follows in the open path persists the repaired
            // bitmap instead of taking the no-change fast path.
            self.update_metadata(|meta| meta.stats.version += 1);
        }

        Ok(fixed)
    }

    /// Registers a document found in storage into the in-memory id structures
    /// and (best-effort) re-inserts it into every index. Index insert
    /// failures are logged, not propagated: an idempotent re-insert of an
    /// already-indexed document commonly reports duplicates.
    ///
    /// Returns `true` when the id was missing from the bitmap (i.e. an
    /// orphan was recovered).
    fn repair_document(
        &self,
        id: DocumentId,
        doc: DocumentOwned,
        now_ms: u64,
    ) -> Result<bool, DBError> {
        // Keep the id allocator above every observed object even when the
        // document is skipped below, so future adds cannot collide with it.
        self.max_document_id.fetch_max(id, Ordering::AcqRel);

        // A document that no longer matches the schema must not brick the
        // whole collection open (index insert failures below are likewise
        // only logged). Skip it without registering; it stays recoverable
        // via `reconcile_storage` after the schema or the object is fixed.
        let doc = match Document::try_from_doc(self.schema(), doc) {
            Ok(doc) => doc,
            Err(err) => {
                log::warn!(
                    action = "Collection::repair_document",
                    collection = self.name,
                    doc_id = id;
                    "Skipping document that does not match the schema: {err:?}",
                );
                return Ok(false);
            }
        };

        let mut is_new = false;
        {
            let mut doc_ids = self.doc_ids.write();
            if !doc_ids.contains(id) {
                doc_ids.add(id);
                self.doc_ids_index.write().insert(id);
                is_new = true;
            }
        }

        // try to repair indexes
        for index in &self.btree_indexes {
            if let Some(fv) = self.index_hooks.btree_index_value(index, &doc) {
                if fv.as_ref() == &FieldValue::Null {
                    continue;
                }
                if let Err(err) = index.insert(id, &fv, now_ms) {
                    log::warn!(
                        action = "Collection::repair_document",
                        collection = self.name,
                        doc_id = id,
                        index = index.name();
                        "Failed to repair BTree index: {err:?}",
                    );
                }
            }
        }

        for index in &self.bm25_indexes {
            if let Some(text) = self.index_hooks.bm25_index_value(index, &doc)
                && let Err(err) = index.insert(id, &text, now_ms)
            {
                log::warn!(
                    action = "Collection::repair_document",
                    collection = self.name,
                    doc_id = id,
                    index = index.name();
                    "Failed to repair BM25 index: {err:?}",
                );
            }
        }

        for index in &self.hnsw_indexes {
            if let Some(vector) = self.index_hooks.hnsw_index_value(index, &doc)
                && let Err(err) = index.insert(id, vector.into_owned(), now_ms)
            {
                log::warn!(
                    action = "Collection::repair_document",
                    collection = self.name,
                    doc_id = id,
                    index = index.name();
                    "Failed to repair HNSW index: {err:?}",
                );
            }
        }

        if is_new {
            self.update_metadata(|meta| {
                meta.stats.version += 1;
            });
        }

        Ok(is_new)
    }

    /// Streams every existing document through `f`, one at a time.
    ///
    /// Documents are fetched with `Storage::fetch` (bypassing the cache) so a
    /// full backfill scan does not evict the hot working set, and are never
    /// collected into memory as a whole — large collections would otherwise
    /// risk OOM during index creation.
    async fn for_each_existing_document<F>(&self, mut f: F) -> Result<(), DBError>
    where
        F: FnMut(DocumentId, Document) -> Result<(), DBError>,
    {
        let ids = self.ids();
        let schema = self.schema();
        let mut stream = futures::stream::iter(ids)
            .map(|id| {
                let storage = self.storage.clone();
                async move {
                    (
                        id,
                        storage.fetch::<DocumentOwned>(&Self::doc_path(id)).await,
                    )
                }
            })
            .buffered(8);

        while let Some((id, result)) = stream.next().await {
            match result {
                Ok((doc, _)) => {
                    f(id, Document::try_from_doc(schema.clone(), doc)?)?;
                }
                Err(DBError::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    async fn backfill_btree_index(&self, index: &BTree, now_ms: u64) -> Result<(), DBError> {
        if self.is_empty() {
            return Ok(());
        }

        self.for_each_existing_document(|id, doc| {
            if let Some(fv) = self.index_hooks.btree_index_value(index, &doc) {
                if fv.as_ref() == &FieldValue::Null {
                    return Ok(());
                }
                index.insert(id, &fv, now_ms)?;
            }
            Ok(())
        })
        .await
    }

    async fn backfill_bm25_index(&self, index: &BM25, now_ms: u64) -> Result<(), DBError> {
        if self.is_empty() {
            return Ok(());
        }

        self.for_each_existing_document(|id, doc| {
            if let Some(text) = self.index_hooks.bm25_index_value(index, &doc) {
                index.insert(id, &text, now_ms)?;
            }
            Ok(())
        })
        .await
    }

    async fn backfill_hnsw_index(&self, index: &Hnsw, now_ms: u64) -> Result<(), DBError> {
        if self.is_empty() {
            return Ok(());
        }

        self.for_each_existing_document(|id, doc| {
            if let Some(vector) = self.index_hooks.hnsw_index_value(index, &doc) {
                index.insert(id, vector.into_owned(), now_ms)?;
            }
            Ok(())
        })
        .await
    }

    async fn try_upgrade_schema(&mut self, mut new_schema: Schema) -> Result<(), DBError> {
        if !new_schema.needs_upgrade(&self.schema) {
            return Ok(());
        }

        new_schema.upgrade_with(&self.schema)?;
        self.schema = Arc::new(new_schema.clone());
        self.update_metadata(|m| {
            m.schema = new_schema;
            m.stats.version += 1;
        });

        log::warn!(
            action = "Collection::upgrade_schema",
            collection = self.name,
            version = self.schema.version();
            "Schema upgraded to version {}",
            self.schema.version()
        );
        Ok(())
    }

    /// Sets the collection to read-only mode.
    ///
    /// # Arguments
    /// * `read_only` - Whether to enable read-only mode
    pub fn set_read_only(&self, read_only: bool) {
        if !read_only
            && (self.lifecycle.load(Ordering::Acquire) != LIFECYCLE_ACTIVE
                || self.database_read_only.load(Ordering::Acquire))
        {
            log::warn!(
                action = "Collection::set_read_only",
                collection = self.name;
                "Ignoring attempt to re-enable a closed collection handle",
            );
            return;
        }
        self.read_only.store(read_only, Ordering::Release);
        log::info!(
            action = "Collection::set_read_only",
            collection = self.name;
            "Collection is set to read-only: {read_only}",
        );
    }

    /// Closes the collection, ensuring all data is flushed to storage.
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if closing fails
    pub async fn close(&self) -> Result<(), DBError> {
        loop {
            match self.lifecycle.load(Ordering::Acquire) {
                LIFECYCLE_ACTIVE => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            LIFECYCLE_ACTIVE,
                            LIFECYCLE_CLOSING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                LIFECYCLE_CLOSING => break,
                LIFECYCLE_CLOSED | LIFECYCLE_DELETED => return Ok(()),
                LIFECYCLE_DELETING => return Err(self.lifecycle_error()),
                _ => return Err(self.lifecycle_error()),
            }
        }
        // Publish the user-visible read-only state as soon as admission
        // closes. Existing operations are drained by the exclusive gate.
        self.read_only.store(true, Ordering::Release);
        let _operation_guard = self.operation_gate.clone().write_owned().await;
        match self.lifecycle.load(Ordering::Acquire) {
            LIFECYCLE_CLOSED | LIFECYCLE_DELETED => return Ok(()),
            LIFECYCLE_DELETING => return Err(self.lifecycle_error()),
            LIFECYCLE_CLOSING => {}
            _ => return Err(self.lifecycle_error()),
        }

        let start = Instant::now();
        let now_ms = unix_ms();
        let guard = self.cancel_guard("Collection::close");
        let rt = self.flush_inner(now_ms).await;
        guard.disarm();
        let elapsed = start.elapsed();
        match rt {
            Ok(_) => {
                self.lifecycle.store(LIFECYCLE_CLOSED, Ordering::Release);
                log::warn!(
                    action = "Collection::close",
                    collection = self.name,
                    elapsed = elapsed.as_millis();
                    "Collection closed successfully in {elapsed:?}",
                );
                Ok(())
            }
            Err(err) => {
                // The failed flush may have completed some of its dependent
                // writes; the in-memory watermarks are no longer trustworthy.
                // Poison so a reopen loads a fresh generation from storage
                // instead of retrying with diverged state.
                self.poison("Collection::close");
                log::error!(
                    action = "Collection::close",
                    collection = self.name,
                    elapsed = elapsed.as_millis();
                    "Failed to close collection: {err:?}",
                );
                Err(err)
            }
        }
    }

    /// Flushes all pending changes to storage.
    ///
    /// # Arguments
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    /// `true` if changes were flushed, `false` if no changes needed to be flushed
    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        // The write guard both serializes complete flushes and freezes all
        // document/index mutations for the checkpoint transaction.
        let _operation_guard = self.operation_gate.clone().write_owned().await;
        self.ensure_mutable()?;
        let guard = self.cancel_guard("Collection::flush");
        let rt = self.flush_inner(now_ms).await;
        guard.disarm();
        if rt.is_err() {
            // A checkpoint is multiple dependent writes; after any failure the
            // in-memory watermarks no longer describe what is durable. Treat
            // it like a crash: reject further use and recover on reopen.
            self.poison("Collection::flush");
        }
        rt
    }

    /// A checkpoint is a sequence of dependent writes (collection metadata,
    /// indexes, ids bitmap, storage checkpoint, WAL retirement). Any error
    /// after the first write leaves memory and storage diverged in a way this
    /// handle no longer tracks — the caller ([`Collection::flush`]) poisons
    /// the handle, and reopening converges from storage. `flush` holds the
    /// exclusive `operation_gate`, so no mutation runs concurrently.
    async fn flush_inner(&self, now_ms: u64) -> Result<bool, DBError> {
        // On a live handle every retained intent belongs to a mutation that
        // either completed (indexes and document agree) or failed
        // deterministically before its storage write (memory was rolled back
        // to the stored state). Unknown-outcome failures and cancellations
        // poison the handle before reaching this point, so no reconciliation
        // is needed here: the checkpoint below captures a consistent state
        // and simply retires the intents afterwards. Reconciliation happens
        // only on reopen (`replay_mutation_intents`).
        let has_pending_mutations = { !self.pending_mutations.lock().is_empty() };
        let stored_check_point = self.store_metadata(now_ms).await?;

        // Fast path: no collection metadata update and no index has pending data.
        let has_pending_indexes = self.has_pending_index_flush();
        if stored_check_point.is_none() && !has_pending_indexes && !has_pending_mutations {
            return Ok(false);
        }

        let indexes_saved = if has_pending_indexes {
            self.store_indexes(now_ms).await?
        } else {
            false
        };

        if let Some(check_point) = stored_check_point {
            // The metadata snapshot was taken under the exclusive operation
            // gate, so every id at or below `check_point` is already visible
            // in the ids bitmap: there are no in-flight adds during a flush.
            self.store_ids().await?;
            // check_point is the last persisted document ID
            self.storage.store_metadata(check_point, now_ms).await?;
        }

        // The intent log is the commit record for document/index atomicity and
        // is removed last. A crash before this point replays the mutation; a
        // crash after it observes durable indexes, ids and checkpoint.
        if has_pending_mutations {
            self.clear_mutation_intents().await?;
        }

        Ok(stored_check_point.is_some() || indexes_saved || has_pending_mutations)
    }

    /// Irreversibly closes mutation admission before database metadata is
    /// unregistered. The database holds the per-name lifecycle lock when
    /// calling this; a cancelled delete leaves the tombstoned handle in the
    /// registry so a retry can finish draining and deleting it.
    pub(crate) fn begin_delete(&self) -> Result<(), DBError> {
        loop {
            let state = self.lifecycle.load(Ordering::Acquire);
            match state {
                LIFECYCLE_DELETED | LIFECYCLE_DELETING => break,
                // A poisoned handle may be deleted: deletion does not depend
                // on trustworthy in-memory state, it removes storage.
                LIFECYCLE_ACTIVE | LIFECYCLE_CLOSING | LIFECYCLE_CLOSED | LIFECYCLE_POISONED => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            state,
                            LIFECYCLE_DELETING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                _ => return Err(self.lifecycle_error()),
            }
        }
        self.read_only.store(true, Ordering::Release);
        Ok(())
    }

    /// Drops the collection, deleting all associated data from storage.
    pub(crate) async fn drop_data(&self) -> Result<(), DBError> {
        self.begin_delete()?;
        let _operation_guard = self.operation_gate.clone().write_owned().await;
        if self.lifecycle.load(Ordering::Acquire) == LIFECYCLE_DELETED {
            return Ok(());
        }

        let start = Instant::now();
        let total = self.len();

        // 并发删除集合存储下的全部对象（文档、元数据、ids 和索引）
        self.storage.drop_data().await?;
        self.lifecycle.store(LIFECYCLE_DELETED, Ordering::Release);
        let elapsed = start.elapsed();
        log::warn!(
            action = "Collection::drop_data",
            collection = self.name,
            deleted = total,
            elapsed = elapsed.as_millis();
            "Collection dropped. deleted={total}, elapsed={elapsed:?}"
        );

        Ok(())
    }

    /// Stores collection metadata to storage if it has changed.
    ///
    /// A single conditional PUT against the last observed object version is
    /// the remaining defense against a second writer, which the deployment
    /// contract forbids. A `Precondition` conflict is not reconciled in
    /// place: it propagates, the caller poisons the handle and recovery
    /// happens on reopen (which re-reads the durable object version).
    ///
    /// # Arguments
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    /// `Some(max_document_id)` if metadata was stored, `None` if no changes needed to be stored
    async fn store_metadata(&self, now_ms: u64) -> Result<Option<DocumentId>, DBError> {
        // Fast path: if version is already saved, avoid cloning metadata.
        let current_version = { self.metadata.read().stats.version };
        if self.last_saved_version.load(Ordering::Acquire) >= current_version {
            return Ok(None);
        }

        // Re-acquire metadata with lock to get a consistent snapshot for
        // saving. Complete flushes are serialized by `operation_gate`, so the
        // snapshot cannot change while the PUT is in flight.
        let mut metadata = self.metadata();
        if self.last_saved_version.load(Ordering::Acquire) >= metadata.stats.version {
            return Ok(None);
        }
        metadata.stats.last_saved = now_ms.max(metadata.stats.last_saved);
        let mut payload = Vec::new();
        cbor2::to_writer(&metadata, &mut payload).map_err(|err| DBError::Serialization {
            name: self.name.clone(),
            source: err.into(),
        })?;
        let expected_version = { self.metadata_version.read().clone() };
        let version = self
            .storage
            .put_bytes(
                Self::METADATA_PATH,
                payload.into(),
                crate::storage::PutMode::Update(expected_version.into()),
            )
            .await?;

        *self.metadata_version.write() = version;
        self.last_saved_version
            .fetch_max(metadata.stats.version, Ordering::Release);
        self.update_metadata(|m| {
            m.stats.last_saved = metadata.stats.last_saved.max(m.stats.last_saved);
        });
        Ok(Some(metadata.stats.max_document_id))
    }

    /// Persists the current collection metadata object once, **without**
    /// claiming the flush version watermark: `last_saved_version` is
    /// deliberately not advanced, so the next periodic flush still observes
    /// `version > last_saved_version` and runs the full path
    /// (`store_metadata` + `store_ids`) — a metadata-only write must never
    /// make a later flush skip persisting the ids bitmap.
    ///
    /// `Ok(())` means the snapshot containing this call's change was durably
    /// written. Extension writers are serialized against flush and each other
    /// by `operation_gate` leases plus the caller-held admission checks, so a
    /// `Precondition` here means a second writer and is not retried.
    async fn store_metadata_unclaimed(&self) -> Result<(), DBError> {
        let _gate = self.extension_write_gate.lock().await;
        let metadata = self.metadata();
        let mut payload = Vec::new();
        cbor2::to_writer(&metadata, &mut payload).map_err(|err| DBError::Serialization {
            name: self.name.clone(),
            source: err.into(),
        })?;
        let expected_version = { self.metadata_version.read().clone() };
        let version = self
            .storage
            .put_bytes(
                Self::METADATA_PATH,
                payload.into(),
                crate::storage::PutMode::Update(expected_version.into()),
            )
            .await?;
        *self.metadata_version.write() = version;
        Ok(())
    }

    /// Stores document IDs bitmap to storage.
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if storing fails
    async fn store_ids(&self) -> Result<(), DBError> {
        let data = {
            let mut ids = self.doc_ids.read().clone();
            ids.run_optimize();
            ids.serialize::<Portable>()
        };
        let ver = { self.ids_version.read().clone() };
        let ver = match self.storage.put(Self::IDS_PATH, &data, Some(ver)).await {
            Ok(ver) => ver,
            Err(err) => {
                return Err(err);
            }
        };

        *self.ids_version.write() = ver;
        Ok(())
    }

    /// Stores all indexes to storage.
    ///
    /// # Arguments
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if storing fails
    async fn store_indexes(&self, now_ms: u64) -> Result<bool, DBError> {
        let (btree_saved, bm25_saved, hnsw_saved) = try_join_await!(
            try_join_all(self.btree_indexes.iter().map(|index| index.flush(now_ms))),
            try_join_all(self.bm25_indexes.iter().map(|index| index.flush(now_ms))),
            try_join_all(self.hnsw_indexes.iter().map(|index| index.flush(now_ms))),
        )?;

        Ok(btree_saved.into_iter().any(|saved| saved)
            || bm25_saved.into_iter().any(|saved| saved)
            || hnsw_saved.into_iter().any(|saved| saved))
    }

    fn has_pending_index_flush(&self) -> bool {
        self.btree_indexes.iter().any(BTree::has_pending_flush)
            || self.bm25_indexes.iter().any(BM25::has_pending_flush)
            || self.hnsw_indexes.iter().any(Hnsw::has_pending_flush)
    }

    /// Sets the tokenizer for text analysis.
    ///
    /// # Arguments
    /// * `tokenizer` - The tokenizer chain to use
    pub fn set_tokenizer(&mut self, tokenizer: TokenizerChain) {
        self.tokenizer = tokenizer;
    }

    /// Replaces the strategy used to derive indexable values from documents.
    ///
    /// Custom hooks are useful for virtual fields, precomputed search text, or
    /// alternative vector encodings that should be indexed without changing the
    /// stored document shape.
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
        metadata.stats.max_document_id = self.max_document_id.load(Ordering::Relaxed);
        metadata.stats.num_documents = self.doc_ids_index.read().len() as u64;
        metadata.stats.search_count = self.search_count.load(Ordering::Relaxed);
        metadata.stats.get_count = self.get_count.load(Ordering::Relaxed);
        metadata.stats.read_only = self.read_only.load(Ordering::Relaxed)
            || self.database_read_only.load(Ordering::Relaxed);
        metadata
    }

    /// Gets current statistics about the collection
    pub fn stats(&self) -> CollectionStats {
        let mut stats = { self.metadata.read().stats.clone() };
        stats.max_document_id = self.max_document_id.load(Ordering::Relaxed);
        stats.num_documents = self.doc_ids_index.read().len() as u64;
        stats.search_count = self.search_count.load(Ordering::Relaxed);
        stats.get_count = self.get_count.load(Ordering::Relaxed);
        stats.read_only = self.read_only.load(Ordering::Relaxed)
            || self.database_read_only.load(Ordering::Relaxed);

        stats
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
        self.doc_ids_index.read().last().cloned()
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
        self.doc_ids_index.read().contains(&id)
    }

    /// Gets the number of documents in the collection.
    ///
    /// # Returns
    /// The number of documents in the collection
    pub fn len(&self) -> usize {
        self.doc_ids_index.read().len()
    }

    /// Checks if the collection is empty.
    ///
    /// # Returns
    /// `true` if the collection contains no documents, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.doc_ids_index.read().is_empty()
    }

    /// Creates a new empty document with the collection's schema.
    pub fn new_document(&self) -> Document {
        Document::new(self.schema.clone())
    }

    /// Gets the value of a user-defined extension key.
    pub fn get_extension(&self, key: &str) -> Option<FieldValue> {
        self.metadata.read().extensions.get(key).cloned()
    }

    /// Gets the value of a user-defined extension key and deserializes it into the specified type.
    pub fn get_extension_as<T>(&self, key: &str) -> Option<T>
    where
        T: DeserializeOwned,
    {
        self.get_extension(key).and_then(|v| v.deserialized().ok())
    }

    /// Sets a user-defined extension key-value pair.
    /// The change is persisted on the next `flush()`.
    /// The extensions should not be large, as they are stored in the same object as collection metadata which size is expected to be small (<= 1MB) and loaded frequently.
    /// Values that fail [`FieldValue::validate_complexity`] are dropped with a warning.
    pub fn set_extension(&self, key: String, value: FieldValue) {
        if let Err(err) = value.validate_complexity() {
            log::warn!(
                action = "Collection::set_extension",
                collection = self.name,
                key = key;
                "Dropping extension value that exceeds complexity limits: {err:?}",
            );
            return;
        }
        let mut meta = self.metadata.write();
        if let Err(err) = self.ensure_mutable() {
            log::warn!(
                action = "Collection::set_extension",
                collection = self.name;
                "Ignoring extension mutation on inactive handle: {err:?}",
            );
            return;
        }
        meta.extensions.insert(key, value);
        // Bump the version so the next `flush()` persists the change;
        // `store_metadata` skips the write when the version is unchanged.
        meta.stats.version += 1;
    }

    /// Sets a user-defined extension key-value pair with a serializable value.
    pub fn set_extension_from<T>(&self, key: String, value: T)
    where
        T: Serialize,
    {
        if let Ok(value) = FieldValue::serialized(&value, None) {
            self.set_extension(key, value);
        }
    }

    /// Updates a user-defined extension using a functional approach.
    ///
    /// This method retrieves the current value for the given key (if any) and computes
    /// a new value using the provided function. If the function returns `None`,
    /// no change is made to the extensions.
    ///
    /// # Arguments
    /// * `key` - The name of the extension key to update.
    /// * `f` - An update function that takes `Option<&FieldValue>` and returns `Option<FieldValue>`.
    ///
    /// # Returns
    /// Returns the previous value `Option<FieldValue>` if a change was made.
    ///
    /// # Notes
    /// The change is persisted to storage on the next `flush()` call.
    /// Values that fail [`FieldValue::validate_complexity`] are dropped with a warning.
    pub fn set_extension_with<F>(&self, key: String, f: F) -> Option<FieldValue>
    where
        F: FnOnce(Option<&FieldValue>) -> Option<FieldValue>,
    {
        let mut meta = self.metadata.write();
        if self.ensure_mutable().is_err() {
            return None;
        }
        let old_value = meta.extensions.get(&key);
        let new_value = f(old_value);
        if let Some(value) = new_value {
            if let Err(err) = value.validate_complexity() {
                log::warn!(
                    action = "Collection::set_extension_with",
                    collection = self.name,
                    key = key;
                    "Dropping extension value that exceeds complexity limits: {err:?}",
                );
                return None;
            }
            meta.stats.version += 1;
            meta.extensions.insert(key, value)
        } else {
            None
        }
    }

    /// Updates a user-defined extension with a serializable value using a functional approach.
    pub fn set_extension_from_with<F, T>(&self, key: String, f: F) -> Option<T>
    where
        F: FnOnce(Option<T>) -> Option<T>,
        T: Serialize + DeserializeOwned,
    {
        let mut meta = self.metadata.write();
        if self.ensure_mutable().is_err() {
            return None;
        }
        let old_value = meta.extensions.get(&key);
        let new_value = f(old_value.and_then(|v| v.clone().deserialized().ok()));
        if let Some(value) = new_value
            && let Ok(value) = FieldValue::serialized(&value, None)
        {
            if let Err(err) = value.validate_complexity() {
                log::warn!(
                    action = "Collection::set_extension_from_with",
                    collection = self.name,
                    key = key;
                    "Dropping extension value that exceeds complexity limits: {err:?}",
                );
                return None;
            }
            meta.stats.version += 1;
            let old = meta.extensions.insert(key, value);
            return old.and_then(|v| v.deserialized().ok());
        }
        None
    }

    /// Sets a user-defined extension key-value pair and immediately persists the change.
    /// The extensions should not be large, as they are stored in the same object as collection metadata which size is expected to be small (<= 1MB) and loaded frequently.
    pub async fn save_extension(&self, key: String, value: FieldValue) -> Result<(), DBError> {
        let _operation_lease = self.mutation_lease().await?;
        value.validate_complexity()?;

        let guard = self.cancel_guard("Collection::save_extension");
        self.update_metadata(|meta| {
            meta.extensions.insert(key, value);
            meta.stats.version += 1;
        });
        // Persist the metadata object directly (a single small put) instead
        // of running a full flush: extensions live only in the metadata
        // object, and the full flush caused write amplification plus an
        // unpersisted window — a concurrent flusher could claim the version
        // first, making this call take the fast path and return Ok while the
        // winner's snapshot (possibly without this extension) was still in
        // flight or failed. The unclaimed write keeps the "returning Ok
        // means persisted" contract and does not advance
        // `last_saved_version`, so the next full flush still persists the
        // ids bitmap alongside the metadata.
        let rt = self.store_metadata_unclaimed().await;
        guard.disarm();
        rt
    }

    /// Sets a user-defined extension key-value pair with a serializable value and immediately persists the change.
    pub async fn save_extension_from<T>(&self, key: String, value: &T) -> Result<(), DBError>
    where
        T: Serialize,
    {
        let field_value = FieldValue::serialized(value, None)?;
        self.save_extension(key, field_value).await
    }

    /// Removes a user-defined extension key and immediately persists the change.
    /// Returns the previous value if the key existed.
    pub async fn remove_extension(&self, key: &str) -> Result<Option<FieldValue>, DBError> {
        let _operation_lease = self.mutation_lease().await?;

        let guard = self.cancel_guard("Collection::remove_extension");
        let rt = async {
            let old = self.update_metadata(|meta| {
                let old = meta.extensions.remove(key);
                if old.is_some() {
                    meta.stats.version += 1;
                }
                old
            });
            if old.is_some() {
                // See `save_extension` for why this is a direct, unclaimed
                // metadata write instead of a full flush.
                self.store_metadata_unclaimed().await?;
            }
            Ok(old)
        }
        .await;
        guard.disarm();
        rt
    }

    /// Provides access to the entire extensions map for advanced use cases.
    pub fn extensions_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BTreeMap<String, FieldValue>) -> R,
    {
        f(&self.metadata.read().extensions)
    }

    /// Tokenizes the given text using the collection's tokenizer.
    pub fn tokenize(&self, text: &str) -> Vec<String> {
        BM25::collect_tokens(&self.tokenizer, text)
    }

    /// Creates a BTree index on the specified field.
    ///
    /// # Uniqueness semantics
    ///
    /// - A **single-field** index enforces uniqueness only when the field is
    ///   declared `unique` in the schema.
    /// - A **multi-field** index (two or more fields) always acts as a
    ///   composite **unique** index: inserting a second document with the
    ///   same combination of field values is rejected. Existing documents
    ///   are backfilled at creation time and can make creation fail if they
    ///   already violate the constraint.
    ///
    /// # Arguments
    /// * `fields` - Fields to index
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_btree_index(&mut self, fields: &[&str]) -> Result<(), DBError> {
        self.ensure_mutable()?;
        if fields.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "BTree index requires at least one field".into(),
            });
        }

        let now_ms = unix_ms();
        let name = virtual_field_name(fields);

        {
            if self.metadata.read().btree_indexes.contains_key(&name) {
                return Err(DBError::AlreadyExists {
                    name: name.to_string(),
                    path: self.name.clone(),
                    source: "BTree index already exists".into(),
                    _id: 0,
                });
            }
        }

        if fields.len() == 1 {
            let field = self.schema.get_field_or_err(fields[0])?;

            let index = BTree::new(field.clone(), self.storage.clone(), now_ms).await?;
            if let Err(err) = self.backfill_btree_index(&index, now_ms).await {
                index.drop_data().await;
                return Err(err);
            }
            if field.unique() {
                self.btree_indexes.insert(0, index);
            } else {
                self.btree_indexes.push(index);
            }
            let mut meta = self.metadata.write();
            meta.btree_indexes.insert(name.to_string(), field.clone());
            meta.stats.version += 1;
        } else {
            for field in fields {
                self.schema.get_field_or_err(field)?;
            }
            let field = FieldEntry::new("_virtual_field_".to_string(), FieldType::Bytes)?
                .with_unique()
                .with_description(name.clone());
            let index = BTree::with_virtual_field(
                fields.iter().map(|s| s.to_string()).collect(),
                self.storage.clone(),
                now_ms,
            )
            .await?;

            if let Err(err) = self.backfill_btree_index(&index, now_ms).await {
                index.drop_data().await;
                return Err(err);
            }
            self.btree_indexes.insert(0, index);
            let mut meta = self.metadata.write();
            meta.btree_indexes.insert(name, field);
            meta.stats.version += 1;
        }

        Ok(())
    }

    /// Creates a BTree index if it doesn't already exist.
    ///
    /// # Arguments
    /// * `fields` - Fields to index
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_btree_index_nx(&mut self, fields: &[&str]) -> Result<(), DBError> {
        match self.create_btree_index(fields).await {
            Ok(_) => Ok(()),
            Err(DBError::AlreadyExists { .. }) => {
                // Ignore the error if the index already exists
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Creates a BM25 text search index.
    ///
    /// # Arguments
    /// * `field` - Name of the field to index
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_bm25_index(&mut self, fields: &[&str]) -> Result<(), DBError> {
        self.ensure_mutable()?;
        if fields.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "BM25 index requires at least one field".into(),
            });
        }

        let now_ms = unix_ms();
        let name = virtual_field_name(fields);

        {
            if self.metadata.read().bm25_indexes.contains_key(&name) {
                return Err(DBError::AlreadyExists {
                    name: name.clone(),
                    path: self.name.clone(),
                    source: "BM25 index already exists".into(),
                    _id: 0,
                });
            }
        }

        for field in fields {
            self.schema.get_field_or_err(field)?;
        }

        let index = BM25::new(
            fields.iter().map(|s| s.to_string()).collect(),
            self.tokenizer.clone(),
            self.storage.clone(),
            now_ms,
        )
        .await?;

        if let Err(err) = self.backfill_bm25_index(&index, now_ms).await {
            index.drop_data().await;
            return Err(err);
        }

        {
            let mut meta = self.metadata.write();
            meta.stats.version += 1;
            let field = FieldEntry::new("_virtual_field_".to_string(), FieldType::Text)?
                .with_description(name.clone());
            meta.bm25_indexes.insert(name, field);
        }

        self.bm25_indexes.push(index);
        Ok(())
    }

    /// Creates a BM25 index if it doesn't already exist.
    ///
    /// # Arguments
    /// * `fields` - Fields to index
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_bm25_index_nx(&mut self, fields: &[&str]) -> Result<(), DBError> {
        match self.create_bm25_index(fields).await {
            Ok(_) => Ok(()),
            Err(DBError::AlreadyExists { .. }) => {
                // Ignore the error if the index already exists
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Creates a HNSW (vector) search index.
    ///
    /// # Arguments
    /// * `field` - Name of the field to index
    /// * `config` - HNSW index configuration
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_hnsw_index(
        &mut self,
        field: &str,
        config: HnswConfig,
    ) -> Result<(), DBError> {
        self.ensure_mutable()?;
        validate_field_name(field)?;

        let name = field.to_string();
        let now_ms = unix_ms();

        {
            if self.metadata.read().hnsw_indexes.contains_key(&name) {
                return Err(DBError::AlreadyExists {
                    name: name.clone(),
                    path: self.name.clone(),
                    source: "HNSW index already exists".into(),
                    _id: 0,
                });
            }
        }

        let field = self
            .schema
            .get_field(field)
            .ok_or_else(|| DBError::NotFound {
                name: field.to_string(),
                path: self.name.clone(),
                source: "field not found".into(),
                _id: 0,
            })?;
        if field.r#type() != &FieldType::Vector {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "The type of field for HNSW index should be FieldType::Vector".into(),
            });
        }

        let index = Hnsw::new(field, config, self.storage.clone(), now_ms).await?;
        if let Err(err) = self.backfill_hnsw_index(&index, now_ms).await {
            index.drop_data().await;
            return Err(err);
        }

        {
            let mut meta = self.metadata.write();
            meta.stats.version += 1;
            meta.hnsw_indexes.insert(name, field.clone());
        }

        self.hnsw_indexes.push(index);
        Ok(())
    }

    /// Creates a HNSW index if it doesn't already exist.
    ///
    /// # Arguments
    /// * `field` - Name of the field to index
    /// * `config` - HNSW index configuration
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_hnsw_index_nx(
        &mut self,
        field: &str,
        config: HnswConfig,
    ) -> Result<(), DBError> {
        match self.create_hnsw_index(field, config).await {
            Ok(_) => Ok(()),
            Err(DBError::AlreadyExists { .. }) => {
                // Ignore the error if the index already exists
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Removes a B-tree index and its persisted files.
    ///
    /// Returns `true` when either metadata or an in-memory index entry was
    /// removed. Returns `false` if the requested index did not exist.
    pub async fn remove_btree_index(&mut self, fields: &[&str]) -> Result<bool, DBError> {
        self.ensure_mutable()?;
        if fields.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "BTree index requires at least one field".into(),
            });
        }

        let name = virtual_field_name(fields);
        let removed_index = self
            .btree_indexes
            .iter()
            .position(|index| index.name() == name)
            .map(|position| self.btree_indexes.remove(position))
            .is_some();

        let removed_metadata = {
            let mut meta = self.metadata.write();
            let removed = meta.btree_indexes.remove(&name).is_some();
            if removed {
                meta.stats.version += 1;
            }
            removed
        };

        let removed = removed_index || removed_metadata;
        if removed {
            self.cleanup_removed_index(&BTree::dir_path(&name)).await?;
        }

        Ok(removed)
    }

    /// Persists the metadata change of a removed index, then best-effort deletes
    /// its storage files.
    ///
    /// The order matters for crash safety: metadata must stop referencing the
    /// index before its files disappear, otherwise reopening the collection
    /// would fail to bootstrap the index. Leftover files from a failed deletion
    /// are harmless and will be overwritten if the index is re-created.
    async fn cleanup_removed_index(&self, dir_path: &str) -> Result<(), DBError> {
        self.store_metadata_unclaimed().await?;
        if let Err(err) = self.storage.drop_prefix(dir_path).await {
            log::warn!(
                action = "Collection::cleanup_removed_index",
                collection = self.name,
                index_dir = dir_path;
                "Failed to drop index data: {err:?}",
            );
        }
        Ok(())
    }

    /// Removes a BM25 full-text index and its persisted files.
    ///
    /// Returns `true` when either metadata or an in-memory index entry was
    /// removed. Returns `false` if the requested index did not exist.
    pub async fn remove_bm25_index(&mut self, fields: &[&str]) -> Result<bool, DBError> {
        self.ensure_mutable()?;
        if fields.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "BM25 index requires at least one field".into(),
            });
        }

        let name = virtual_field_name(fields);
        let removed_index = self
            .bm25_indexes
            .iter()
            .position(|index| index.name() == name)
            .map(|position| self.bm25_indexes.remove(position))
            .is_some();

        let removed_metadata = {
            let mut meta = self.metadata.write();
            let removed = meta.bm25_indexes.remove(&name).is_some();
            if removed {
                meta.stats.version += 1;
            }
            removed
        };

        let removed = removed_index || removed_metadata;
        if removed {
            self.cleanup_removed_index(&BM25::dir_path(&name)).await?;
        }

        Ok(removed)
    }

    /// Removes an HNSW vector index and its persisted files.
    ///
    /// Returns `true` when either metadata or an in-memory index entry was
    /// removed. Returns `false` if the requested field has no HNSW index.
    pub async fn remove_hnsw_index(&mut self, field: &str) -> Result<bool, DBError> {
        self.ensure_mutable()?;
        if field.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "HNSW index requires a non-empty field name".into(),
            });
        }

        validate_field_name(field)?;

        let removed_index = self
            .hnsw_indexes
            .iter()
            .position(|index| index.field_name() == field)
            .map(|position| self.hnsw_indexes.remove(position))
            .is_some();

        let removed_metadata = {
            let mut meta = self.metadata.write();
            let removed = meta.hnsw_indexes.remove(field).is_some();
            if removed {
                meta.stats.version += 1;
            }
            removed
        };

        let removed = removed_index || removed_metadata;
        if removed {
            self.cleanup_removed_index(&Hnsw::dir_path(field)).await?;
        }

        Ok(removed)
    }

    /// Returns the B-tree index over `fields`.
    ///
    /// Multi-field indexes are addressed by the same virtual field name used
    /// during index creation.
    pub fn get_btree_index(&self, fields: &[&str]) -> Result<BTreeIndexView<'_>, DBError> {
        self.find_btree_index(fields)
            .map(|inner| BTreeIndexView { inner })
    }

    fn find_btree_index(&self, fields: &[&str]) -> Result<&BTree, DBError> {
        let name = virtual_field_name(fields);
        if let Some(index) = self.btree_indexes.iter().find(|i| i.name() == name) {
            return Ok(index);
        }

        Err(DBError::Index {
            name,
            source: "BTree index not found".into(),
        })
    }

    /// Returns the BM25 full-text index over `fields`.
    ///
    /// Multi-field indexes are addressed by the same virtual field name used
    /// during index creation.
    pub fn get_bm25_index(&self, fields: &[&str]) -> Result<BM25IndexView<'_>, DBError> {
        self.find_bm25_index(fields)
            .map(|inner| BM25IndexView { inner })
    }

    fn find_bm25_index(&self, fields: &[&str]) -> Result<&BM25, DBError> {
        let name = virtual_field_name(fields);
        if let Some(index) = self.bm25_indexes.iter().find(|i| i.name() == name) {
            return Ok(index);
        }

        Err(DBError::Index {
            name,
            source: "BM25 index not found".into(),
        })
    }

    /// Returns the HNSW vector index for `field`.
    pub fn get_hnsw_index(&self, field: &str) -> Result<HnswIndexView<'_>, DBError> {
        self.find_hnsw_index(field)
            .map(|inner| HnswIndexView { inner })
    }

    fn find_hnsw_index(&self, field: &str) -> Result<&Hnsw, DBError> {
        if let Some(index) = self.hnsw_indexes.iter().find(|i| i.field_name() == field) {
            return Ok(index);
        }

        Err(DBError::Index {
            name: field.to_string(),
            source: "HNSW index not found".into(),
        })
    }

    /// Compacts the specified BM25 index to optimize storage and performance.
    pub async fn compact_bm25_index(&self, fields: &[&str]) -> Result<(), DBError> {
        let _operation_lease = self.mutation_lease().await?;
        let index = self.find_bm25_index(fields)?;
        let guard = self.cancel_guard("Collection::compact_bm25_index");
        let rt = index.compact_index().await;
        guard.disarm();
        rt
    }

    /// Compacts the specified BTree index to optimize storage and performance.
    pub async fn compact_btree_index(&self, fields: &[&str]) -> Result<(), DBError> {
        let _operation_lease = self.mutation_lease().await?;
        let index = self.find_btree_index(fields)?;
        let guard = self.cancel_guard("Collection::compact_btree_index");
        let rt = index.compact_index().await;
        guard.disarm();
        rt
    }

    /// Adds a new document to the collection.
    ///
    /// This method:
    /// 1. Validates the document against the collection schema
    /// 2. Assigns a unique ID to the document
    /// 3. Updates all relevant indexes
    /// 4. Persists the document to storage
    ///
    /// # Arguments
    /// * `doc` - The document to add to the collection
    ///
    /// # Returns
    /// The ID of the newly added document, or an error if addition fails
    ///
    /// # Errors
    /// Returns an error if:
    /// - The collection is in read-only mode
    /// - The document fails schema validation
    /// - Any index update fails
    /// - Storage operations fail
    pub async fn add(&self, doc: Document) -> Result<DocumentId, DBError> {
        let _operation_lease = self.mutation_lease().await?;
        // Past this point a dropped future is treated as a crash: in-memory
        // index/bitmap state may already diverge from storage, so the guard
        // poisons the handle and recovery happens on reopen.
        let guard = self.cancel_guard("Collection::add");
        let rt = self.add_impl(doc).await;
        guard.disarm();
        rt
    }

    /// Guarantees `id` is at or below the durable allocation watermark before
    /// any document object may be written for it. Persisted in strides, so
    /// this is one small PUT per [`Self::ALLOCATION_WATERMARK_STRIDE`] adds.
    /// A failed watermark PUT fails the add before anything else was written:
    /// the id is skipped and the handle stays healthy.
    async fn ensure_allocation_watermark(&self, id: DocumentId) -> Result<(), DBError> {
        if id <= self.durable_alloc_watermark.load(Ordering::Acquire) {
            return Ok(());
        }
        let _gate = self.watermark_gate.lock().await;
        if id <= self.durable_alloc_watermark.load(Ordering::Acquire) {
            return Ok(());
        }
        let target = self
            .max_document_id
            .load(Ordering::Acquire)
            .max(id)
            .saturating_add(Self::ALLOCATION_WATERMARK_STRIDE);
        self.storage
            .put(Self::ALLOCATION_WATERMARK_PATH, &target, None)
            .await?;
        self.durable_alloc_watermark
            .fetch_max(target, Ordering::AcqRel);
        Ok(())
    }

    async fn add_impl(&self, mut doc: Document) -> Result<DocumentId, DBError> {
        self.schema.validate(doc.fields())?;
        // Flush holds the exclusive `operation_gate` while this add holds a
        // shared lease, so a checkpoint can never observe this id before the
        // bitmap registration below completes. A failed add simply skips the
        // id forever; a cancelled add poisons the handle and the reopen
        // repair scan (bounded by the allocation watermark) recovers or
        // retires the id.
        let id = self.max_document_id.fetch_add(1, Ordering::Acquire) + 1;
        doc.set_id(id);

        // Adds write no per-mutation intent. The durable allocation watermark
        // guarantees the reopen repair scan enumerates every id that may have
        // a document object, so a committed-but-unacknowledged add is found
        // there instead of through a WAL record.
        self.ensure_allocation_watermark(id).await?;

        let now_ms = unix_ms();
        #[allow(clippy::mutable_key_type)]
        let mut btree_inserted: FxHashMap<&BTree, Cow<FieldValue>> = FxHashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut bm25_inserted: FxHashMap<&BM25, (u64, Cow<str>)> = FxHashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut hnsw_inserted: FxHashMap<&Hnsw, u64> = FxHashMap::default();

        let rt: Result<(), DBError> = (|| {
            for index in &self.btree_indexes {
                if let Some(fv) = self.index_hooks.btree_index_value(index, &doc) {
                    if fv.as_ref() == &FieldValue::Null {
                        continue;
                    }

                    btree_inserted.insert(index, fv.clone());
                    index.insert(id, &fv, now_ms)?;
                }
            }

            for index in &self.bm25_indexes {
                if let Some(text) = self.index_hooks.bm25_index_value(index, &doc) {
                    index.insert(id, &text, now_ms)?;
                    bm25_inserted.insert(index, (id, text));
                }
            }

            for index in &self.hnsw_indexes {
                if let Some(vector) = self.index_hooks.hnsw_index_value(index, &doc) {
                    hnsw_inserted.insert(index, id);
                    index.insert(id, vector.into_owned(), now_ms)?;
                }
            }

            Ok(())
        })();

        let rollback_indexes = || {
            for (k, v) in btree_inserted {
                k.remove(id, &v, now_ms);
            }
            for (k, v) in bm25_inserted {
                k.remove(v.0, &v.1, now_ms);
            }
            for (k, v) in hnsw_inserted {
                k.remove(v, now_ms);
            }
        };

        if let Err(err) = rt {
            rollback_indexes();
            return Err(err);
        }

        let path = Self::doc_path(id);
        if let Err(err) = self.storage.create(&path, &doc).await {
            rollback_indexes();
            // The PUT outcome is unknown: it may have committed. Delete the
            // object so the id cannot survive as an orphan below a future
            // checkpoint. If even the delete outcome is unknown, treat it
            // like a crash — the reopen repair scan (whose checkpoint has not
            // advanced past this id) decides whether the document exists.
            match self.storage.delete(&path).await {
                Ok(()) | Err(DBError::NotFound { .. }) => {}
                Err(delete_err) => {
                    log::error!(
                        action = "Collection::add",
                        collection = self.name,
                        doc_id = id;
                        "Failed to clean up document after failed add: {delete_err:?}",
                    );
                    self.poison("Collection::add");
                }
            }
            return Err(err);
        }

        self.doc_ids.write().add(id);
        self.doc_ids_index.write().insert(id);

        self.update_metadata(|meta| {
            meta.stats.last_inserted = now_ms;
            meta.stats.version += 1;
            meta.stats.insert_count += 1;
        });

        Ok(id)
    }

    /// Adds a new document to the collection from a serializable value.
    ///
    /// This method:
    /// 1. Converts the value into a Document using the collection's schema
    /// 2. Validates the document against the schema
    /// 3. Assigns a unique ID to the document
    /// 4. Updates all relevant indexes
    /// 5. Persists the document to storage
    /// # Arguments
    /// * `val` - The value to convert into a document
    ///
    /// # Returns
    /// The ID of the newly added document, or an error if addition fails
    pub async fn add_from<T>(&self, val: &T) -> Result<DocumentId, DBError>
    where
        T: Serialize,
    {
        let doc = Document::try_from(self.schema(), val)?;
        self.add(doc).await
    }

    /// Updates an existing document with new field values.
    ///
    /// Concurrent `update` / `remove` calls for the same document id are
    /// serialized internally (striped per-id locks), so index state and the
    /// stored document cannot diverge under in-process concurrency. The
    /// version precondition on the storage write additionally guards against
    /// writers outside this process.
    ///
    /// # Durability
    ///
    /// Before changing either the document or any derived index, `update`
    /// durably records the document's previous indexed values. A successful
    /// call means the document object itself is durable; the next `flush`
    /// commits the corresponding index/ids generation and removes the intent.
    /// If the process stops first, collection open replays the intent and
    /// makes the stored document authoritative for every index.
    ///
    /// # Arguments
    /// * `id` - The ID of the document to update
    /// * `fields` - The new field values to apply
    ///
    /// # Returns
    /// Ok(Document) if successful, or an error if update fails
    ///
    /// # Errors
    /// Returns an error if:
    /// - The collection is in read-only mode
    /// - The document doesn't exist
    /// - The updated document fails schema validation
    /// - The updated document version not matching the stored version because of concurrent update
    /// - Any index update fails
    /// - Storage operations fail
    pub async fn update(
        &self,
        id: DocumentId,
        fields: BTreeMap<String, Fv>,
    ) -> Result<Document, DBError> {
        let _operation_lease = self.mutation_lease().await?;
        let guard = self.cancel_guard("Collection::update");
        let rt = self.update_impl(id, fields).await;
        guard.disarm();
        rt
    }

    async fn update_impl(
        &self,
        id: DocumentId,
        fields: BTreeMap<String, Fv>,
    ) -> Result<Document, DBError> {
        if !self.doc_ids.read().contains(id) {
            return Err(DBError::NotFound {
                name: "document".to_string(),
                path: self.name.clone(),
                source: format!("Document with ID {id} not found").into(),
                _id: id,
            });
        }

        if fields.is_empty() {
            return Err(DBError::Generic {
                name: self.name.clone(),
                source: "No fields to update".into(),
            });
        }

        // Serialize mutations of the same document (see `doc_locks`): the
        // read-modify-write below must not interleave with another update or
        // remove of this id, or rolled-back index entries could diverge from
        // the stored document.
        let _doc_guard = self.doc_lock(id).lock().await;

        let (doc, ver) = self
            .storage
            .get::<DocumentOwned>(&Self::doc_path(id))
            .await?;
        let mut doc = Document::try_from_doc(self.schema(), doc)?;
        let old_doc = doc.clone();

        // apply the new values
        let mut fields_keys = FxHashSet::default();
        for (field_name, fv) in fields {
            doc.set_field(&field_name, fv)?;
            fields_keys.insert(field_name);
        }

        // validate the updated document
        self.schema.validate(doc.fields())?;

        // Persist the old indexable values before changing either side of the
        // document/index pair. The intent is cleared only by a successful
        // full flush after both sides are durable.
        self.record_mutation_intent(id, Some(&old_doc), Some(&doc))
            .await?;

        let now_ms = unix_ms();

        // record the updated and removed indexes for rollback
        #[allow(clippy::mutable_key_type)]
        let mut btree_updated: FxHashMap<&BTree, (Cow<FieldValue>, Cow<FieldValue>)> =
            FxHashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut bm25_inserted: FxHashMap<&BM25, (u64, Cow<str>)> = FxHashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut hnsw_inserted: FxHashMap<&Hnsw, u64> = FxHashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut bm25_removed: FxHashMap<&BM25, (u64, Cow<str>)> = FxHashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut hnsw_removed: FxHashMap<&Hnsw, (u64, Cow<Vector>)> = FxHashMap::default();

        // update the indexes
        let rt: Result<(), DBError> = (|| {
            for index in &self.btree_indexes {
                let fields = index.virtual_field();
                if fields_keys.iter().any(|v| fields.contains(v)) {
                    let old_value = self
                        .index_hooks
                        .btree_index_value(index, &old_doc)
                        .unwrap_or(Cow::Owned(FieldValue::Null));
                    let new_value = self
                        .index_hooks
                        .btree_index_value(index, &doc)
                        .unwrap_or(Cow::Owned(FieldValue::Null));

                    index.update(id, &old_value, &new_value, now_ms)?;
                    btree_updated.insert(index, (old_value, new_value));
                }
            }

            for index in &self.bm25_indexes {
                let fields = index.virtual_field();
                if fields_keys.iter().any(|v| fields.contains(v)) {
                    if let Some(text) = self.index_hooks.bm25_index_value(index, &old_doc) {
                        index.remove(id, &text, now_ms);
                        bm25_removed.insert(index, (id, text));
                    }

                    if let Some(text) = self.index_hooks.bm25_index_value(index, &doc) {
                        index.insert(id, &text, now_ms)?;
                        bm25_inserted.insert(index, (id, text));
                    }
                }
            }

            for index in &self.hnsw_indexes {
                let field_name = index.field_name();
                if fields_keys.contains(field_name) {
                    if let Some(vector) = self.index_hooks.hnsw_index_value(index, &old_doc) {
                        index.remove(id, now_ms);
                        hnsw_removed.insert(index, (id, vector));
                    }

                    if let Some(vector) = self.index_hooks.hnsw_index_value(index, &doc) {
                        hnsw_inserted.insert(index, id);
                        index.insert(id, vector.into_owned(), now_ms)?;
                    }
                }
            }

            Ok(())
        })();

        let rollback_indexes = || {
            for (k, v) in bm25_inserted {
                k.remove(v.0, &v.1, now_ms);
            }
            for (k, v) in hnsw_inserted {
                k.remove(v, now_ms);
            }

            for (k, v) in btree_updated {
                let _ = k.update(id, &v.1, &v.0, now_ms);
            }

            for (k, v) in bm25_removed {
                let _ = k.insert(v.0, &v.1, now_ms);
            }
            for (k, v) in hnsw_removed {
                let _ = k.insert(v.0, v.1.to_vec(), now_ms);
            }
        };

        if let Err(err) = rt {
            rollback_indexes();
            return Err(err);
        }

        // persist the updated document with update version
        let path = Self::doc_path(id);
        if let Err(err) = self.storage.put(&path, &doc, Some(ver)).await {
            rollback_indexes();
            // The PUT outcome is unknown: the new document may be durable
            // while memory was just rolled back. The retained intent plus a
            // reopen reconcile the divergence; this handle must not continue.
            self.poison("Collection::update");
            return Err(err);
        }

        self.update_metadata(|meta| {
            meta.stats.last_updated = now_ms;
            meta.stats.version += 1;
            meta.stats.update_count += 1;
        });

        Ok(doc)
    }

    /// Removes a document from the collection by its ID.
    ///
    /// This method (deliberately in this order, for crash safety):
    /// 1. Removes the document from all relevant indexes
    /// 2. Deletes the document object from storage
    /// 3. Removes the document ID from the bitmap
    ///
    /// Deleting the object before the bitmap update means a crash in between
    /// leaves a dead id that the reopen intent replay retires, instead of an
    /// orphaned object beyond the repair scan window.
    /// A durable mutation intent containing the old indexed values is written
    /// before phase 1. It is retired only after a full flush, so reopening
    /// after a crash can finish removing stale B-Tree, BM25 and HNSW entries.
    ///
    /// # Arguments
    /// * `id` - The ID of the document to remove
    ///
    /// # Returns
    /// Ok(Some(Document)) if successful, or Ok(None) if the document was not found, or an error if removal fails
    ///
    /// # Errors
    /// Returns an error if:
    /// - The collection is in read-only mode
    /// - Any index update fails
    /// - Storage operations fail
    pub async fn remove(&self, id: DocumentId) -> Result<Option<Document>, DBError> {
        let _operation_lease = self.mutation_lease().await?;
        let guard = self.cancel_guard("Collection::remove");
        let rt = self.remove_impl(id).await;
        guard.disarm();
        rt
    }

    async fn remove_impl(&self, id: DocumentId) -> Result<Option<Document>, DBError> {
        // Membership check is non-authoritative; the bitmap mutation below
        // serializes concurrent removes and is the source of truth.
        if !self.doc_ids.read().contains(id) {
            return Ok(None);
        }

        // Serialize mutations of the same document (see `doc_locks`).
        let _doc_guard = self.doc_lock(id).lock().await;

        let now_ms = unix_ms();
        let path = Self::doc_path(id);

        // Best-effort fetch to drive index cleanup. If the document has already
        // been deleted from storage we still want to clear the in-memory state
        // (treat as a normal removal) but cannot retire stale index entries.
        let doc = match self.storage.get::<DocumentOwned>(&path).await {
            Ok((doc, _)) => Some(Document::try_from_doc(self.schema(), doc)?),
            Err(DBError::NotFound { .. }) => None,
            Err(err) => {
                log::warn!(
                    action = "Collection::remove",
                    collection = self.name,
                    doc_id = id;
                    "Failed to fetch document for removal, aborting: {err:?}",
                );
                return Err(err);
            }
        };

        if let Some(doc) = &doc {
            self.record_mutation_intent(id, Some(doc), None).await?;
        }

        #[allow(clippy::mutable_key_type)]
        let mut btree_removed: FxHashMap<&BTree, Cow<FieldValue>> = FxHashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut bm25_removed: FxHashMap<&BM25, (u64, Cow<str>)> = FxHashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut hnsw_removed: FxHashMap<&Hnsw, (u64, Cow<Vector>)> = FxHashMap::default();

        // Phase 1: remove index entries while we still hold the original
        // contents. Record actual removals so a storage delete failure can
        // restore the in-memory indexes before returning.
        if let Some(doc) = &doc {
            for index in &self.btree_indexes {
                if let Some(fv) = self.index_hooks.btree_index_value(index, doc)
                    && fv.as_ref() != &FieldValue::Null
                    && index.remove(id, &fv, now_ms)
                {
                    btree_removed.insert(index, fv);
                }
            }

            for index in &self.bm25_indexes {
                if let Some(text) = self.index_hooks.bm25_index_value(index, doc)
                    && index.remove(id, &text, now_ms)
                {
                    bm25_removed.insert(index, (id, text));
                }
            }

            for index in &self.hnsw_indexes {
                if let Some(vector) = self.index_hooks.hnsw_index_value(index, doc)
                    && index.remove(id, now_ms)
                {
                    hnsw_removed.insert(index, (id, vector));
                }
            }
        }

        let rollback_indexes = || {
            for (index, value) in btree_removed {
                let _ = index.insert(id, &value, now_ms);
            }
            for (index, (id, text)) in bm25_removed {
                let _ = index.insert(id, &text, now_ms);
            }
            for (index, (id, vector)) in hnsw_removed {
                let _ = index.insert(id, vector.to_vec(), now_ms);
            }
        };

        // Phase 2: delete the document object before the bitmap so that a
        // failure here keeps the document visible (and recoverable) rather
        // than producing an orphan beyond the auto-repair scan window.
        if doc.is_some()
            && let Err(err) = self.storage.delete(&path).await
        {
            rollback_indexes();
            log::error!(
                action = "Collection::remove",
                collection = self.name,
                doc_id = id;
                "Failed to delete document from storage: {err:?}",
            );
            // The DELETE outcome is unknown: the object may be gone while the
            // bitmap and indexes were just restored. The retained intent plus
            // a reopen complete the removal; this handle must not continue.
            self.poison("Collection::remove");
            return Err(err);
        }

        // Phase 3: finalise by updating the in-memory bitmap. Locks are taken
        // in the same order as add()/auto_repair_indexes() to avoid deadlocks.
        let removed = {
            let mut doc_ids = self.doc_ids.write();
            let mut doc_ids_index = self.doc_ids_index.write();
            let removed = doc_ids_index.remove(&id);
            if removed {
                doc_ids.remove(id);
            }
            removed
        };

        if removed {
            self.update_metadata(|meta| {
                meta.stats.last_deleted = now_ms;
                meta.stats.version += 1;
                meta.stats.delete_count += 1;
            });
        }

        Ok(doc)
    }

    /// Searches for documents matching the given query and returns them.
    ///
    /// # Arguments
    /// * `query` - The search query parameters
    ///
    /// # Returns
    /// A vector of matching documents, or an error if the search fails
    pub async fn search(&self, query: Query) -> Result<Vec<Document>, DBError> {
        let ids = self.search_ids(query).await?;
        let schema = self.schema();
        let mut docs = Vec::with_capacity(ids.len());
        let mut stream = futures::stream::iter(ids)
            .map(|id| {
                let storage = self.storage.clone();
                async move { (id, storage.get::<DocumentOwned>(&Self::doc_path(id)).await) }
            })
            .buffered(8);
        while let Some((id, result)) = stream.next().await {
            match result {
                Ok((doc, _)) => {
                    let doc = Document::try_from_doc(schema.clone(), doc)?;
                    docs.push(doc);
                }
                Err(DBError::NotFound { .. }) => {
                    // Under the poison-on-unknown-outcome contract a live
                    // handle should never observe a dead id: crash recovery
                    // happens on reopen. Log the anomaly; `reconcile_storage`
                    // is the explicit repair path.
                    log::warn!(
                        action = "Collection::search",
                        collection = self.name,
                        doc_id = id;
                        "Skipping dead document id without a backing object",
                    );
                }
                Err(err) => return Err(err),
            }
        }
        Ok(docs)
    }

    /// Drops a document id whose object is missing from storage from the
    /// in-memory id structures, so the next flush persists the repair.
    ///
    /// Only [`Self::reconcile_storage`] calls this: under the
    /// poison-on-unknown-outcome contract, dead ids (bitmap entry without a
    /// backing object) can only be produced by a crash, and reopen recovery
    /// resolves them before the handle serves reads. No-op in read-only mode
    /// or when the id is not in the bitmap.
    fn heal_missing_doc(&self, id: DocumentId) {
        if self.read_only.load(Ordering::Relaxed) {
            return;
        }

        // Same lock order as add() / remove(): doc_ids, then doc_ids_index.
        let removed = {
            let mut doc_ids = self.doc_ids.write();
            let mut doc_ids_index = self.doc_ids_index.write();
            if doc_ids.contains(id) {
                doc_ids.remove(id);
                doc_ids_index.remove(&id);
                true
            } else {
                false
            }
        };

        if removed {
            self.update_metadata(|meta| {
                meta.stats.version += 1;
            });
            log::warn!(
                action = "Collection::heal_missing_doc",
                collection = self.name,
                doc_id = id;
                "Removed dead document id without a backing object",
            );
        }
    }

    /// Searches for documents matching the given query and deserializes them into the specified type.
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize documents into
    ///
    /// # Arguments
    /// * `query` - The search query parameters
    ///
    /// # Returns
    /// A vector of deserialized objects of type T, or an error if the search or deserialization fails
    pub async fn search_as<T>(&self, query: Query) -> Result<Vec<T>, DBError>
    where
        T: DeserializeOwned,
    {
        let docs = self.search(query).await?;
        let mut rt = Vec::with_capacity(docs.len());
        for doc in docs {
            rt.push(doc.try_into()?);
        }
        Ok(rt)
    }

    /// Searches for documents matching the given query and returns only their IDs.
    ///
    /// This is more efficient than retrieving full documents when only IDs are needed.
    ///
    /// # Limit semantics
    ///
    /// `Query::limit` defaults to `10` and is clamped to
    /// [`Collection::MAX_SEARCH_LIMIT`]. An explicit `limit` of `0` returns
    /// an empty result (consistent with the underlying indexes' `top_k = 0`
    /// behavior). Each search index is asked for up to `limit * 10`
    /// candidates before reranking and filtering, capped at 4096 to bound
    /// the per-query search breadth.
    ///
    /// # Arguments
    /// * `query` - The search query parameters
    ///
    /// # Returns
    /// A vector of matching document IDs, or an error if the search fails
    pub async fn search_ids(&self, query: Query) -> Result<Vec<DocumentId>, DBError> {
        query
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;

        self.search_count.fetch_add(1, Ordering::Relaxed);
        let limit = query.limit.unwrap_or(10).min(Self::MAX_SEARCH_LIMIT);
        if limit == 0 {
            // A zero limit previously behaved inconsistently: empty for
            // search queries (top_k = 0) but unlimited for filter-only
            // queries. Normalize to "no results" for both.
            return Ok(Vec::new());
        }

        let top_k = (limit * 10).min(4096);
        let mut candidates = Vec::new();
        let mut result = Vec::new();

        if let Some(params) = query.search {
            let mut results: Vec<Vec<u64>> = Vec::new();

            if let Some(ref text) = params.text {
                if self.bm25_indexes.is_empty() {
                    return Err(DBError::Index {
                        name: self.name.clone(),
                        source: "text search requires a BM25 index, but none exists".into(),
                    });
                }
                for index in self.bm25_indexes.iter() {
                    let rt = if params.logical_search {
                        index.try_search_advanced(text, top_k, params.bm25_params.clone())?
                    } else {
                        index.search(text, top_k, params.bm25_params.clone())
                    };
                    results.push(rt.into_iter().map(|r| r.0).collect());
                }
            }

            if let Some(ref vector) = params.vector {
                // Only query the HNSW indexes whose dimension matches the
                // query vector: with multiple vector indexes of different
                // dimensions, failing the whole query on the first mismatch
                // would make vector search permanently unusable.
                let mut searched = false;
                for index in self.hnsw_indexes.iter() {
                    if index.dimension() != vector.len() {
                        continue;
                    }
                    searched = true;
                    let rt = index.try_search(vector, top_k)?;
                    results.push(rt.into_iter().map(|r| r.0).collect());
                }
                if !searched {
                    // A pure vector query with no usable HNSW index is a
                    // caller bug and must surface as an error. A hybrid
                    // text+vector query whose text part already ran degrades
                    // to text-only results instead (best effort), so one
                    // mismatched vector dimension does not fail an otherwise
                    // valid full-text search.
                    if params.text.is_none() {
                        return Err(DBError::Index {
                            name: self.name.clone(),
                            source: format!(
                                "no HNSW index matches the query vector dimension {}",
                                vector.len()
                            )
                            .into(),
                        });
                    }
                    log::warn!(
                        action = "Collection::search_ids",
                        collection = self.name,
                        dimension = vector.len();
                        "no HNSW index matches the query vector dimension; degrading to text-only results",
                    );
                }
            }

            let reranker = params.reranker.unwrap_or_default();
            let reranked = reranker.rerank(&results);
            let mut uniq_candidates = UniqueVec::with_capacity(top_k);
            uniq_candidates.extend(reranked.into_iter().map(|(id, _)| id));
            candidates = uniq_candidates.into();

            if candidates.is_empty() {
                return Ok(result);
            }
        }

        let mut truncate_head = false;
        match query.filter {
            Some(filter) => {
                // 「保留尾部」截断只适用于纯过滤路径（结果按 id 升序）；
                // 混合搜索的结果按相关性降序排列，必须保留头部，
                // 否则会丢弃最相关的命中。
                truncate_head = candidates.is_empty() && Self::filter_has_lt_or_le(&filter);
                result = self.filter_by_field(filter, &candidates, top_k)?;
            }
            None => result = candidates,
        };
        if limit > 0 && result.len() > limit {
            if truncate_head {
                result.drain(0..(result.len() - limit));
            } else {
                result.truncate(limit);
            }
        }

        Ok(result)
    }

    /// Queries document IDs based on a filter condition.
    ///
    /// # Arguments
    /// * `filter` - The filter condition to apply
    /// * `limit` - Maximum number of results to return. `None` means no
    ///   limit; an explicit `Some(0)` returns an empty result, consistent
    ///   with [`Collection::search_ids`].
    /// # Returns
    /// A vector of document IDs matching the filter, or an error if filtering fails.
    pub async fn query_ids(
        &self,
        filter: Filter,
        limit: Option<usize>,
    ) -> Result<Vec<DocumentId>, DBError> {
        filter
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;

        self.search_count.fetch_add(1, Ordering::Relaxed);
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let limit = limit.unwrap_or(0);
        let truncate_head = Self::filter_has_lt_or_le(&filter);

        let mut rt = self.filter_by_field(filter, &[], limit)?;
        if limit > 0 && rt.len() > limit {
            if truncate_head {
                rt.drain(0..(rt.len() - limit));
            } else {
                rt.truncate(limit);
            }
        }
        Ok(rt)
    }

    // 临时策略：当过滤条件中出现 Lt/Le 时，结果采用“保留尾部”的截断方式
    fn filter_has_lt_or_le(filter: &Filter) -> bool {
        match filter {
            Filter::Field((_name, rq)) => Self::range_has_lt_or_le(rq),
            Filter::Or(qs) | Filter::And(qs) => qs.iter().any(|q| Self::filter_has_lt_or_le(q)),
            Filter::Not(_) => false,
        }
    }

    fn range_has_lt_or_le<T>(rq: &RangeQuery<T>) -> bool {
        match rq {
            RangeQuery::Lt(_) | RangeQuery::Le(_) => true,
            RangeQuery::And(qs) | RangeQuery::Or(qs) => {
                qs.iter().any(|q| Self::range_has_lt_or_le(q))
            }
            _ => false,
        }
    }

    /// Gets a document by its ID.
    ///
    /// # Arguments
    /// * `id` - The ID of the document to retrieve
    ///
    /// # Returns
    /// The document if found, or an error if retrieval fails
    pub async fn get(&self, id: DocumentId) -> Result<Document, DBError> {
        if self.doc_ids.read().contains(id) {
            self.get_count.fetch_add(1, Ordering::Relaxed);

            let path = Self::doc_path(id);
            match self.storage.get::<DocumentOwned>(&path).await {
                Ok((doc, _)) => {
                    let doc = Document::try_from_doc(self.schema(), doc)?;
                    return Ok(doc);
                }
                Err(DBError::NotFound { .. }) => {
                    // See the search path: a dead id on a live handle is an
                    // anomaly, repaired explicitly via `reconcile_storage`.
                    log::warn!(
                        action = "Collection::get",
                        collection = self.name,
                        doc_id = id;
                        "Document id has no backing object",
                    );
                }
                Err(err) => return Err(err),
            }
        }

        Err(DBError::NotFound {
            name: "document".to_string(),
            path: self.name.clone(),
            source: format!("Document {id} not found").into(),
            _id: id,
        })
    }

    /// Gets a document by its ID and deserializes it into the specified type.
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize the document into
    ///
    /// # Arguments
    /// * `id` - The ID of the document to retrieve
    ///
    /// # Returns
    /// The deserialized object of type T if found, or an error if retrieval or deserialization fails
    pub async fn get_as<T>(&self, id: DocumentId) -> Result<T, DBError>
    where
        T: DeserializeOwned,
    {
        let doc = self.get(id).await?;
        let obj = doc.try_into()?;
        Ok(obj)
    }

    /// Filters documents by a field condition.
    ///
    /// # Arguments
    /// * `filter` - The filter condition to apply
    /// * `candidates` - Optional list of document IDs to filter (if empty, all documents are considered)
    /// * `limit` - The number of results to stop retrieving. The returned vector may be shorter or larger than this limit.
    ///
    /// # Returns
    /// A vector of document IDs matching the filter, or an error if filtering fails
    fn filter_by_field(
        &self,
        filter: Filter,
        candidates: &[DocumentId],
        limit: usize,
    ) -> Result<Vec<DocumentId>, DBError> {
        if candidates.is_empty() {
            let mut result = self.filter_by_field_with(filter, None, limit)?;
            result.sort_unstable();
            Ok(result)
        } else {
            let cand_set: FxHashSet<DocumentId> = candidates.iter().copied().collect();
            let matched: FxHashSet<DocumentId> = self
                .filter_by_field_with(filter, Some(&cand_set), 0)?
                .into_iter()
                .collect();

            let mut result = Vec::with_capacity(matched.len().min(candidates.len()));
            for id in candidates {
                if matched.contains(id) {
                    result.push(*id);
                }
            }
            Ok(result)
        }
    }

    /// Inner implementation of `filter_by_field` using a `FxHashSet` for O(1) candidate lookups.
    fn filter_by_field_with(
        &self,
        filter: Filter,
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
    ) -> Result<Vec<DocumentId>, DBError> {
        let mut result = Vec::new();
        match filter {
            Filter::Field((index_name, filter)) => {
                if index_name == Schema::ID_KEY {
                    let filter: RangeQuery<u64> =
                        RangeQuery::try_convert_from(filter).map_err(|err| DBError::Generic {
                            name: self.name.clone(),
                            source: err,
                        })?;
                    Ok(self.filter_by_id(filter, candidates, limit))
                } else if let Some(index) =
                    self.btree_indexes.iter().find(|i| i.name() == index_name)
                {
                    result.reserve_exact(Self::reserve_hint(limit));
                    index.try_range_query_ids(filter, |ids| {
                        for id in ids {
                            if candidates.is_none_or(|s| s.contains(id)) {
                                result.push(*id);
                                if limit > 0 && result.len() >= limit {
                                    return false;
                                }
                            }
                        }
                        true
                    })?;
                    Ok(result)
                } else {
                    Err(DBError::Index {
                        name: self.name.clone(),
                        source: format!("BTree index {index_name:?} not found").into(),
                    })
                }
            }
            Filter::Or(queries) => {
                let mut rt: UniqueVec<u64> = UniqueVec::with_capacity(Self::reserve_hint(limit));
                // Evaluate every branch (each bounded by `limit` on its own)
                // instead of stopping once the union reaches `limit`: the
                // early stop made the result depend on operand order.
                for query in queries {
                    let ids = self.filter_by_field_with(*query, candidates, limit)?;
                    rt.extend(ids);
                }

                result = rt.into();
                // Canonical order, so equal boolean sets yield equal results
                // regardless of branch order; the caller applies `limit`.
                result.sort_unstable();
                Ok(result)
            }
            Filter::And(queries) => {
                let mut iter = queries.into_iter();
                if let Some(query) = iter.next() {
                    let mut rt: FxHashSet<DocumentId> = self
                        .filter_by_field_with(*query, candidates, 0)?
                        .into_iter()
                        .collect();

                    for query in iter {
                        rt = self
                            .filter_by_field_with(*query, Some(&rt), 0)?
                            .into_iter()
                            .collect();
                        if rt.is_empty() {
                            return Ok(vec![]);
                        }
                    }

                    result = rt.into_iter().collect();
                    // 由调用方控制结果长度
                    // if limit > 0 && result.len() > limit {
                    //     result.truncate(limit);
                    // }
                }
                Ok(result)
            }
            Filter::Not(query) => {
                result.reserve_exact(Self::reserve_hint(limit));
                let exclude: FxHashSet<u64> = self
                    .filter_by_field_with(*query, None, 0)?
                    .into_iter()
                    .collect();
                for id in self.doc_ids_index.read().iter() {
                    if !exclude.contains(id) && candidates.is_none_or(|s| s.contains(id)) {
                        result.push(*id);
                        if limit > 0 && result.len() >= limit {
                            break;
                        }
                    }
                }
                Ok(result)
            }
        }
    }

    /// Filters documents by ID using a range query.
    ///
    /// # Arguments
    /// * `query` - The range query to apply to document IDs
    /// * `candidates` - Optional set of document IDs to filter (if None, all documents are considered)
    /// * `limit` - The number of results to stop retrieving. The returned vector may be shorter or larger than this limit.
    ///
    /// # Returns
    /// A vector of document IDs matching the range query
    fn filter_by_id(
        &self,
        query: RangeQuery<DocumentId>,
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
    ) -> Vec<DocumentId> {
        let mut result = Vec::new();
        match query {
            RangeQuery::Eq(id) => {
                if self.doc_ids_index.read().contains(&id)
                    && candidates.is_none_or(|s| s.contains(&id))
                {
                    result.push(id);
                }
            }
            RangeQuery::Gt(start_key) => {
                result.reserve_exact(Self::reserve_hint(limit));
                for id in self.doc_ids_index.read().range((
                    std::ops::Bound::Excluded(start_key),
                    std::ops::Bound::Unbounded,
                )) {
                    if candidates.is_none_or(|s| s.contains(id)) {
                        result.push(*id);
                        if limit > 0 && result.len() >= limit {
                            return result;
                        }
                    }
                }
            }
            RangeQuery::Ge(start_key) => {
                result.reserve_exact(Self::reserve_hint(limit));
                for id in self
                    .doc_ids_index
                    .read()
                    .range(std::ops::RangeFrom { start: start_key })
                {
                    if candidates.is_none_or(|s| s.contains(id)) {
                        result.push(*id);
                        if limit > 0 && result.len() >= limit {
                            return result;
                        }
                    }
                }
            }
            RangeQuery::Lt(end_key) => {
                // 倒序遍历以便在有上限时尽快终止，最终结果按正序返回
                let mut tmp = Vec::with_capacity(Self::reserve_hint(limit));
                for id in self
                    .doc_ids_index
                    .read()
                    .range(std::ops::RangeTo { end: end_key })
                    .rev()
                {
                    if candidates.is_none_or(|s| s.contains(id)) {
                        tmp.push(*id);
                        if limit > 0 && tmp.len() >= limit {
                            break;
                        }
                    }
                }
                tmp.reverse();
                result.extend(tmp);
            }
            RangeQuery::Le(end_key) => {
                // 倒序遍历以便在有上限时尽快终止，最终结果按正序返回
                let mut tmp = Vec::with_capacity(Self::reserve_hint(limit));
                for id in self
                    .doc_ids_index
                    .read()
                    .range(std::ops::RangeToInclusive { end: end_key })
                    .rev()
                {
                    if candidates.is_none_or(|s| s.contains(id)) {
                        tmp.push(*id);
                        if limit > 0 && tmp.len() >= limit {
                            break;
                        }
                    }
                }
                tmp.reverse();
                result.extend(tmp);
            }
            RangeQuery::Between(start_key, end_key) => {
                result.reserve_exact(Self::reserve_hint(
                    limit.min(end_key.saturating_sub(start_key).saturating_add(1) as usize),
                ));
                for id in self.doc_ids_index.read().range(start_key..=end_key) {
                    if candidates.is_none_or(|s| s.contains(id)) {
                        result.push(*id);
                        if limit > 0 && result.len() >= limit {
                            return result;
                        }
                    }
                }
            }
            RangeQuery::Include(ids) => {
                result.reserve_exact(limit.min(ids.len()));
                let doc_ids_index = self.doc_ids_index.read();
                for id in ids.into_iter() {
                    if doc_ids_index.contains(&id) && candidates.is_none_or(|s| s.contains(&id)) {
                        result.push(id);
                        if limit > 0 && result.len() >= limit {
                            return result;
                        }
                    }
                }
            }
            RangeQuery::And(queries) => {
                let mut iter = queries.into_iter();
                if let Some(query) = iter.next() {
                    let mut rt: UniqueVec<u64> = self.filter_by_id(*query, candidates, 0).into();

                    for query in iter {
                        let keys: UniqueVec<u64> = self.filter_by_id(*query, candidates, 0).into();
                        rt.intersect_with(&keys);
                        if rt.is_empty() {
                            return vec![];
                        }
                    }

                    result = rt.into();
                    // 由调用方控制结果长度
                    // if limit > 0 && result.len() > limit {
                    //     result.truncate(limit);
                    // }
                }
            }
            RangeQuery::Or(queries) => {
                let mut rt = UniqueVec::new();
                for query in queries {
                    let keys = self.filter_by_id(*query, candidates, 0);
                    rt.extend(keys);
                    if limit > 0 && rt.len() > limit {
                        break;
                    }
                }

                result = rt.into();
                // 由调用方控制结果长度
                // if limit > 0 && result.len() > limit {
                //     result.truncate(limit);
                // }
            }
            RangeQuery::Not(query) => {
                result.reserve_exact(Self::reserve_hint(limit));
                // 先收集要排除的 key，再遍历全集差集
                let exclude: FxHashSet<u64> =
                    self.filter_by_id(*query, None, 0).into_iter().collect();
                for id in self.doc_ids_index.read().iter() {
                    if !exclude.contains(id) && candidates.is_none_or(|s| s.contains(id)) {
                        result.push(*id);
                        if limit > 0 && result.len() >= limit {
                            return result;
                        }
                    }
                }
            }
        }

        result
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        database::{AndaDB, DBConfig},
        error::DBError,
        index::HnswConfig,
        query::{Filter, Query, RangeQuery, Search},
        schema::{AndaDBSchema, Document, Fv, Json, Schema, Vector},
        storage::{PutMode, StorageConfig},
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::{StreamExt, stream::BoxStream};
    use ic_auth_types::ByteArrayB64;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as ObjectStoreResult, memory::InMemory, path::Path,
    };
    use serde::{Deserialize, Serialize};
    use std::{
        borrow::Cow,
        collections::BTreeMap,
        fmt,
        sync::{
            Arc,
            atomic::{AtomicBool as TestAtomicBool, Ordering as TestOrdering},
        },
        time::Duration,
    };

    // 测试用的文档结构
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, AndaDBSchema)]
    struct TestDoc {
        pub _id: u64,
        pub name: String,
        pub age: u32,
        pub tags: Vec<String>,
        pub metadata: BTreeMap<String, Json>,

        pub data: BTreeMap<ByteArrayB64<4>, u64>,
        pub vector: Vector,
    }

    // 创建测试数据库和集合的辅助函数
    async fn setup_test_db() -> Result<AndaDB, DBError> {
        let object_store = Arc::new(InMemory::new());

        let db_config = DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };

        let db = AndaDB::connect(object_store, db_config).await?;
        Ok(db)
    }

    // 创建测试集合的辅助函数
    async fn create_test_collection<F>(db: &AndaDB, f: F) -> Result<Arc<Collection>, DBError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        // 创建测试文档的模式
        let schema = TestDoc::schema()?;
        let collection_config = CollectionConfig {
            name: "test_collection".to_string(),
            description: "Test collection".to_string(),
        };

        let collection = db
            .open_or_create_collection(schema, collection_config, f)
            .await?;

        Ok(collection)
    }

    // 创建测试文档的辅助函数
    fn create_test_doc(_id: u64, name: &str, age: u32, tags: Vec<&str>) -> TestDoc {
        TestDoc {
            _id,
            name: name.to_string(),
            age,
            tags: tags.iter().map(|s| s.to_string()).collect(),
            metadata: BTreeMap::new(),
            data: BTreeMap::new(),
            vector: vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
                .into_iter()
                .map(bf16::from_f32)
                .collect(),
        }
    }

    struct BadArrayBTreeHooks;

    impl IndexHooks for BadArrayBTreeHooks {
        fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
            if index.name() == "tags" {
                return Some(Cow::Owned(Fv::Array(vec![
                    Fv::Text("valid".to_string()),
                    Fv::I64(42),
                ])));
            }

            match index.virtual_field() {
                [] => None,
                [name] => doc.get_field(name).map(Cow::Borrowed),
                _ => None,
            }
        }
    }

    struct BackfillErrorHooks {
        btree_value: Option<Fv>,
        hnsw_vector: Option<Vector>,
    }

    impl BackfillErrorHooks {
        fn btree(value: Fv) -> Self {
            Self {
                btree_value: Some(value),
                hnsw_vector: None,
            }
        }

        fn hnsw(vector: Vector) -> Self {
            Self {
                btree_value: None,
                hnsw_vector: Some(vector),
            }
        }
    }

    impl IndexHooks for BackfillErrorHooks {
        fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
            if let Some(value) = &self.btree_value {
                return Some(Cow::Owned(value.clone()));
            }

            IndexHooks::btree_index_value(&DefaultIndexHooks, index, doc)
        }

        fn hnsw_index_value<'a>(&self, index: &Hnsw, doc: &'a Document) -> Option<Cow<'a, Vector>> {
            if let Some(vector) = &self.hnsw_vector {
                return Some(Cow::Owned(vector.clone()));
            }

            IndexHooks::hnsw_index_value(&DefaultIndexHooks, index, doc)
        }
    }

    struct UpdateTagsBTreeErrorHooks;

    impl IndexHooks for UpdateTagsBTreeErrorHooks {
        fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
            if index.name() == "tags" {
                return match doc.get_field("age") {
                    Some(Fv::U64(age)) if *age >= 31 => Some(Cow::Owned(Fv::Array(vec![
                        Fv::Text("new".to_string()),
                        Fv::I64(-1),
                    ]))),
                    _ => Some(Cow::Owned(Fv::Array(vec![Fv::Text("old".to_string())]))),
                };
            }

            IndexHooks::btree_index_value(&DefaultIndexHooks, index, doc)
        }
    }

    struct RecoveryCustomHooks;

    impl IndexHooks for RecoveryCustomHooks {
        fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
            if index.name() == "name" {
                let name = match doc.get_field("name") {
                    Some(Fv::Text(name)) => name,
                    _ => return None,
                };
                return Some(Cow::Owned(Fv::Text(format!(
                    "hook:{}",
                    name.to_lowercase()
                ))));
            }
            IndexHooks::btree_index_value(&DefaultIndexHooks, index, doc)
        }

        fn bm25_index_value<'a>(&self, index: &BM25, doc: &'a Document) -> Option<Cow<'a, str>> {
            if index.name() == "name" && doc.get_field("name").is_some() {
                return Some(Cow::Borrowed("hooktoken"));
            }
            IndexHooks::bm25_index_value(&DefaultIndexHooks, index, doc)
        }
    }

    #[derive(Debug)]
    struct FailDeleteStore {
        inner: Arc<InMemory>,
        fail_delete_suffix: String,
        fail_next_delete: Arc<TestAtomicBool>,
    }

    impl FailDeleteStore {
        fn new(fail_delete_suffix: impl Into<String>) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                fail_delete_suffix: fail_delete_suffix.into(),
                fail_next_delete: Arc::new(TestAtomicBool::new(false)),
            }
        }

        fn fail_next_delete(&self) {
            self.fail_next_delete.store(true, TestOrdering::Release);
        }
    }

    impl fmt::Display for FailDeleteStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("FailDeleteStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FailDeleteStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            let inner = self.inner.clone();
            let fail_delete_suffix = self.fail_delete_suffix.clone();
            let fail_next_delete = self.fail_next_delete.clone();

            locations
                .then(move |location| {
                    let inner = inner.clone();
                    let fail_delete_suffix = fail_delete_suffix.clone();
                    let fail_next_delete = fail_next_delete.clone();
                    async move {
                        let location = location?;
                        if location.to_string().ends_with(&fail_delete_suffix)
                            && fail_next_delete.swap(false, TestOrdering::AcqRel)
                        {
                            return Err(object_store::Error::Generic {
                                store: "fail_delete",
                                source: "injected delete failure".into(),
                            });
                        }

                        inner.delete(&location).await?;
                        Ok(location)
                    }
                })
                .boxed()
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn test_reconcile_storage_recovers_orphans_and_drops_dead_ids() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        let id1 = collection
            .add_from(&create_test_doc(0, "alice", 30, vec!["a"]))
            .await?;
        let id2 = collection
            .add_from(&create_test_doc(0, "bob", 40, vec!["b"]))
            .await?;
        collection.flush(unix_ms()).await?;

        // Simulate a crash between object deletion and the ids flush: the
        // object is gone but the bitmap still references it.
        collection
            .storage
            .delete(&Collection::doc_path(id2))
            .await?;
        // Simulate an orphan document written but never registered: present
        // on disk, absent from the bitmap (well beyond any repair scan).
        let orphan_id = id2 + 500;
        let mut orphan = Document::new(collection.schema());
        orphan.set_doc(
            Document::try_from(
                collection.schema(),
                &create_test_doc(orphan_id, "carol", 50, vec!["c"]),
            )?
            .into(),
        )?;
        collection
            .storage
            .create(&Collection::doc_path(orphan_id), &orphan)
            .await?;

        let (recovered, dropped) = collection.reconcile_storage().await?;
        assert_eq!(recovered, 1);
        assert_eq!(dropped, 1);
        assert!(collection.contains(id1));
        assert!(!collection.contains(id2));
        assert!(collection.contains(orphan_id));
        // The recovered document is readable and indexed again.
        let doc: TestDoc = collection.get_as(orphan_id).await?;
        assert_eq!(doc.name, "carol");
        assert!(collection.max_document_id() >= orphan_id);

        // Idempotent: a second reconcile finds nothing.
        assert_eq!(collection.reconcile_storage().await?, (0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn test_collection_create() -> Result<(), DBError> {
        let db = setup_test_db().await?;

        let collection = create_test_collection(&db, async |c| {
            c.create_bm25_index_nx(&["name", "tags", "metadata"])
                .await?;
            c.create_hnsw_index_nx("vector", HnswConfig::default())
                .await?;
            Ok(())
        })
        .await?;

        assert_eq!(collection.name(), "test_collection");
        assert_eq!(collection.metadata().config.description, "Test collection");

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_collection_open() -> Result<(), DBError> {
        let db = setup_test_db().await?;

        // 首先创建集合
        {
            let collection = create_test_collection(&db, async |_| Ok(())).await?;
            assert_eq!(collection.name(), "test_collection");

            // 添加一个文档以确保有数据可以在重新打开时加载
            let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
            let doc_obj = Document::try_from(collection.schema(), &doc)?;
            let id = collection.add(doc_obj).await?;
            assert_eq!(id, 1);

            // 刷新以确保数据被持久化
            collection.flush(unix_ms()).await?;
        }

        // 关闭并重新打开数据库
        db.close().await?;
        let db = AndaDB::connect(
            db.object_store(),
            DBConfig {
                name: "test_db".to_string(),
                description: "Test database".to_string(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;

        // 重新打开集合
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;

        assert_eq!(collection.name(), "test_collection");
        assert_eq!(collection.metadata().stats.num_documents, 1);

        // 验证文档是否正确加载
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "_id".to_string(),
                    RangeQuery::Eq(Fv::U64(1)),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Alice");

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_document_operations() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        // 添加文档
        let doc1 = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
        let doc_obj1 = Document::try_from(collection.schema(), &doc1)?;
        let id1 = collection.add(doc_obj1).await?;
        assert_eq!(id1, 1);

        let doc2 = create_test_doc(0, "Bob", 25, vec!["tall", "quiet"]);
        let doc_obj2 = Document::try_from(collection.schema(), &doc2)?;
        let id2 = collection.add(doc_obj2).await?;
        assert_eq!(id2, 2);

        // 获取文档
        let result: TestDoc = collection.get_as(id1).await?;
        assert_eq!(result.name, "Alice");
        assert_eq!(result.age, 30);

        // 删除文档
        collection.remove(id2).await?;

        // 验证删除
        let result = collection.get(id2).await;
        assert!(result.is_err());

        // 验证集合统计信息
        let stats = collection.stats();
        assert_eq!(stats.num_documents, 1);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_rolls_back_indexes_when_storage_delete_fails() -> Result<(), DBError> {
        let object_store = Arc::new(FailDeleteStore::new("data/1.cbor"));
        let db_config = DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), db_config).await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["name"]).await?;
            collection
                .create_bm25_index_nx(&["name", "tags", "metadata"])
                .await?;
            collection
                .create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

        let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
        let doc_obj = Document::try_from(collection.schema(), &doc)?;
        let id = collection.add(doc_obj).await?;
        assert_eq!(id, 1);

        object_store.fail_next_delete();
        let err = collection.remove(id).await.unwrap_err();
        assert!(matches!(err, DBError::Storage { .. }));

        assert!(collection.contains(id));
        let stored: TestDoc = collection.get_as(id).await?;
        assert_eq!(stored.name, "Alice");

        let btree_ids = collection
            .query_ids(
                Filter::Field(("name".to_string(), RangeQuery::Eq(Fv::Text("Alice".into())))),
                Some(10),
            )
            .await?;
        assert_eq!(btree_ids, vec![id]);

        let bm25_ids = collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("Alice".to_string()),
                    ..Default::default()
                }),
                limit: Some(10),
                ..Default::default()
            })
            .await?;
        assert!(bm25_ids.contains(&id));

        let hnsw_ids = collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(
                        doc.vector
                            .iter()
                            .map(|value| value.to_f32())
                            .collect::<Vec<_>>(),
                    ),
                    ..Default::default()
                }),
                limit: Some(10),
                ..Default::default()
            })
            .await?;
        assert!(hnsw_ids.contains(&id));

        // The DELETE outcome was unknown, so the handle is poisoned: reads
        // above still serve the rolled-back in-memory state, but mutations
        // are rejected until the collection is reopened.
        assert!(collection.is_poisoned());
        assert!(collection.remove(id).await.is_err());

        // Reopening replays the retained remove intent against storage: the
        // object still exists (the injected failure happened before erasing
        // it), so the document survives fully indexed.
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert!(collection.contains(id));
        let stored: TestDoc = collection.get_as(id).await?;
        assert_eq!(stored.name, "Alice");

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_create_indexes_backfills_existing_documents() -> Result<(), DBError> {
        let object_store = Arc::new(InMemory::new());
        let db_config = DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };

        let db = AndaDB::connect(object_store.clone(), db_config.clone()).await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        let alice = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
        let alice_id = collection
            .add(Document::try_from(collection.schema(), &alice)?)
            .await?;
        let bob = create_test_doc(0, "Bob", 42, vec!["careful", "focused"]);
        let bob_id = collection
            .add(Document::try_from(collection.schema(), &bob)?)
            .await?;
        assert_eq!((alice_id, bob_id), (1, 2));
        db.close().await?;

        let db = AndaDB::connect(object_store.clone(), db_config).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |collection| {
                collection.create_btree_index_nx(&["name"]).await?;
                collection
                    .create_bm25_index_nx(&["name", "tags", "metadata"])
                    .await?;
                collection
                    .create_hnsw_index_nx(
                        "vector",
                        HnswConfig {
                            dimension: 10,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(())
            })
            .await?;

        let btree_ids = collection
            .query_ids(
                Filter::Field(("name".to_string(), RangeQuery::Eq(Fv::Text("Alice".into())))),
                Some(10),
            )
            .await?;
        assert_eq!(btree_ids, vec![alice_id]);

        let bm25_ids = collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("focused".to_string()),
                    ..Default::default()
                }),
                limit: Some(10),
                ..Default::default()
            })
            .await?;
        assert!(bm25_ids.contains(&bob_id));

        assert_eq!(collection.get_hnsw_index("vector")?.stats().num_elements, 2);
        let hnsw_ids = collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(
                        alice
                            .vector
                            .iter()
                            .map(|value| value.to_f32())
                            .collect::<Vec<_>>(),
                    ),
                    ..Default::default()
                }),
                limit: Some(10),
                ..Default::default()
            })
            .await?;
        assert!(
            hnsw_ids.contains(&alice_id),
            "HNSW results should contain {alice_id}, got {hnsw_ids:?}",
        );

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_index_operations() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            // 创建索引
            collection.create_btree_index_nx(&["name"]).await?;
            collection.create_btree_index_nx(&["age"]).await?;
            collection.create_btree_index_nx(&["tags"]).await?;

            // 创建搜索索引
            collection
                .create_bm25_index_nx(&["name", "tags", "metadata"])
                .await?;
            collection
                .create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

        // 添加测试文档
        for (name, age, tags) in [
            ("Alice", 30, vec!["smart", "friendly"]),
            ("Bob", 25, vec!["tall", "quiet"]),
            ("Charlie", 35, vec!["smart", "tall"]),
            ("David", 40, vec!["friendly", "quiet"]),
        ] {
            let doc = create_test_doc(0, name, age, tags);
            let doc_obj = Document::try_from(collection.schema(), &doc)?;
            collection.add(doc_obj).await?;
        }

        // 刷新以确保索引更新
        collection.flush(unix_ms()).await?;

        // 测试精确匹配查询
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("Alice".to_string())),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Alice");

        // 测试范围查询
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "age".to_string(),
                    RangeQuery::Gt(Fv::U64(30)),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|doc| doc.name == "Charlie"));
        assert!(result.iter().any(|doc| doc.name == "David"));

        // 测试数组字段查询
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "tags".to_string(),
                    RangeQuery::Eq(Fv::Text("smart".to_string())),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|doc| doc.name == "Alice"));
        assert!(result.iter().any(|doc| doc.name == "Charlie"));

        // 测试文本搜索
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                search: Some(Search {
                    text: Some("Alice".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Alice");

        // 测试向量搜索
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                search: Some(Search {
                    vector: Some(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;

        assert!(!result.is_empty());

        // 测试复合查询
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                search: Some(Search {
                    text: Some("tall".to_string()),
                    ..Default::default()
                }),
                filter: Some(Filter::Field((
                    "age".to_string(),
                    RangeQuery::Lt(Fv::U64(30)),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Bob");

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_indexes() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let object_store = db.object_store();

        {
            let collection = create_test_collection(&db, async |collection| {
                collection.create_btree_index_nx(&["name"]).await?;
                collection
                    .create_bm25_index_nx(&["name", "tags", "metadata"])
                    .await?;
                collection
                    .create_hnsw_index_nx(
                        "vector",
                        HnswConfig {
                            dimension: 10,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(())
            })
            .await?;

            assert!(collection.metadata().btree_indexes.contains_key("name"));
            assert!(
                collection
                    .metadata()
                    .bm25_indexes
                    .contains_key("name-tags-metadata")
            );
            assert!(collection.metadata().hnsw_indexes.contains_key("vector"));
            assert!(collection.get_btree_index(&["name"]).is_ok());
            assert!(
                collection
                    .get_bm25_index(&["name", "tags", "metadata"])
                    .is_ok()
            );
            assert!(collection.get_hnsw_index("vector").is_ok());
        }

        db.close().await?;
        let db = AndaDB::connect(
            object_store.clone(),
            DBConfig {
                name: "test_db".to_string(),
                description: "Test database".to_string(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;

        let collection = db
            .open_collection("test_collection".to_string(), async |collection| {
                assert!(collection.remove_btree_index(&["name"]).await?);
                assert!(
                    collection
                        .remove_bm25_index(&["name", "tags", "metadata"])
                        .await?
                );
                assert!(collection.remove_hnsw_index("vector").await?);

                assert!(!collection.remove_btree_index(&["name"]).await?);
                assert!(
                    !collection
                        .remove_bm25_index(&["name", "tags", "metadata"])
                        .await?
                );
                assert!(!collection.remove_hnsw_index("vector").await?);

                assert!(collection.get_btree_index(&["name"]).is_err());
                assert!(
                    collection
                        .get_bm25_index(&["name", "tags", "metadata"])
                        .is_err()
                );
                assert!(collection.get_hnsw_index("vector").is_err());

                let meta = collection.metadata();
                assert!(!meta.btree_indexes.contains_key("name"));
                assert!(!meta.bm25_indexes.contains_key("name-tags-metadata"));
                assert!(!meta.hnsw_indexes.contains_key("vector"));

                collection.flush(unix_ms()).await?;
                Ok(())
            })
            .await?;

        assert!(collection.get_btree_index(&["name"]).is_err());
        assert!(
            collection
                .get_bm25_index(&["name", "tags", "metadata"])
                .is_err()
        );
        assert!(collection.get_hnsw_index("vector").is_err());
        assert!(!collection.metadata().btree_indexes.contains_key("name"));
        assert!(
            !collection
                .metadata()
                .bm25_indexes
                .contains_key("name-tags-metadata")
        );
        assert!(!collection.metadata().hnsw_indexes.contains_key("vector"));

        db.close().await?;
        let db = AndaDB::connect(
            object_store,
            DBConfig {
                name: "test_db".to_string(),
                description: "Test database".to_string(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;

        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;

        assert!(collection.get_btree_index(&["name"]).is_err());
        assert!(
            collection
                .get_bm25_index(&["name", "tags", "metadata"])
                .is_err()
        );
        assert!(collection.get_hnsw_index("vector").is_err());
        assert!(!collection.metadata().btree_indexes.contains_key("name"));
        assert!(
            !collection
                .metadata()
                .bm25_indexes
                .contains_key("name-tags-metadata")
        );
        assert!(!collection.metadata().hnsw_indexes.contains_key("vector"));

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_array_btree_index_update_behavior() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["tags"]).await?;
            Ok(())
        })
        .await?;

        // 添加一个包含 ["a", "b"] 标签的文档
        let doc = create_test_doc(0, "Eve", 22, vec!["a", "b"]);
        let id = collection.add_from(&doc).await?;

        // 刷新确保建立索引
        collection.flush(unix_ms()).await?;

        // 查询 tags == "a" 应命中
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "tags".to_string(),
                    RangeQuery::Eq(Fv::Text("a".to_string())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Eve");

        // 更新 tags 为 ["b", "c"]，应移除 "a"，新增 "c"
        let mut fields = BTreeMap::new();
        fields.insert(
            "tags".to_string(),
            Fv::Array(vec![Fv::Text("b".to_string()), Fv::Text("c".to_string())]),
        );
        collection.update(id, fields).await?;
        collection.flush(unix_ms()).await?;

        // 查询 tags == "a" 应不命中
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "tags".to_string(),
                    RangeQuery::Eq(Fv::Text("a".to_string())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 0);

        // 查询 tags == "c" 应命中
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "tags".to_string(),
                    RangeQuery::Eq(Fv::Text("c".to_string())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Eve");

        // 验证 BTree 索引的 keys: 不应包含 "a"，应包含 "b" 和 "c"
        let idx = collection.get_btree_index(&["tags"])?;
        let keys = idx.keys(None, None);
        let keys_text: Vec<String> = keys
            .into_iter()
            .filter_map(|fv| match fv {
                Fv::Text(s) => Some(s),
                _ => None,
            })
            .collect();
        assert!(!keys_text.contains(&"a".to_string()));
        assert!(keys_text.contains(&"b".to_string()));
        assert!(keys_text.contains(&"c".to_string()));

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_map_btree_index_update_behavior() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["metadata"]).await?;
            collection.create_btree_index_nx(&["data"]).await?;
            Ok(())
        })
        .await?;

        let mut doc = create_test_doc(0, "Eve", 22, vec![]);
        doc.metadata.insert("key1".to_string(), "a".into());
        doc.metadata.insert("key2".to_string(), "b".into());
        doc.data.insert([0, 0, 0, 1].into(), 1);
        let id = collection.add_from(&doc).await?;

        // 刷新确保建立索引
        collection.flush(unix_ms()).await?;

        // 查询 metadata.key == "key1" 应命中
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "metadata".to_string(),
                    RangeQuery::Eq(Fv::Text("key1".to_string())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Eve");
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "data".to_string(),
                    RangeQuery::Eq(Fv::Bytes([0, 0, 0, 1].into())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Eve");

        println!("Initial search tests passed.");

        let mut fields = BTreeMap::new();
        fields.insert(
            "metadata".to_string(),
            Fv::Map(BTreeMap::from([
                ("key2".into(), Fv::Text("b".to_string())),
                ("key3".into(), Fv::Text("c".to_string())),
            ])),
        );
        fields.insert(
            "data".to_string(),
            Fv::Map(BTreeMap::from([
                ([0, 0, 0, 2].into(), Fv::U64(2)),
                ([0, 0, 0, 3].into(), Fv::U64(3)),
            ])),
        );
        collection.update(id, fields).await?;
        collection.flush(unix_ms()).await?;

        // 查询 metadata.key == "key1" 应不命中
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "metadata".to_string(),
                    RangeQuery::Eq(Fv::Text("key1".to_string())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 0);
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "data".to_string(),
                    RangeQuery::Eq(Fv::Bytes([0, 0, 0, 1].into())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 0);

        // 查询 metadata.key == "key2" 应命中
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "metadata".to_string(),
                    RangeQuery::Eq(Fv::Text("key2".to_string())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Eve");
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "data".to_string(),
                    RangeQuery::Eq(Fv::Bytes([0, 0, 0, 3].into())),
                ))),
                ..Default::default()
            })
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Eve");

        let idx = collection.get_btree_index(&["metadata"])?;
        let keys = idx.keys(None, None);
        let keys_text: Vec<String> = keys
            .into_iter()
            .filter_map(|fv| match fv {
                Fv::Text(s) => Some(s),
                _ => None,
            })
            .collect();
        assert!(!keys_text.contains(&"key1".to_string()));
        assert!(keys_text.contains(&"key2".to_string()));
        assert!(keys_text.contains(&"key3".to_string()));

        let idx = collection.get_btree_index(&["data"])?;
        let keys = idx.keys(None, None);
        let keys_values: Vec<Vec<u8>> = keys
            .into_iter()
            .filter_map(|fv| match fv {
                Fv::Bytes(b) => Some(b),
                _ => None,
            })
            .collect();
        assert!(!keys_values.contains(&[0, 0, 0, 1].to_vec()));
        assert!(keys_values.contains(&[0, 0, 0, 2].to_vec()));
        assert!(keys_values.contains(&[0, 0, 0, 3].to_vec()));

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_compound_btree_index_query() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["name", "age"]).await?;
            Ok(())
        })
        .await?;

        // 添加三条数据
        for (name, age) in [("Alice", 30), ("Alice", 31), ("Bob", 25)] {
            let doc = create_test_doc(0, name, age as u32, vec!["x"]);
            collection.add_from(&doc).await?;
        }

        collection.flush(unix_ms()).await?;

        // 通过虚拟字段值（name-age）做 Eq 查询
        let bytes = crate::index::virtual_field_value(&[
            Some(&Fv::Text("Alice".to_string())),
            Some(&Fv::U64(30)),
        ])
        .expect("virtual_field_value should produce bytes for composite fields");

        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "name-age".to_string(),
                    RangeQuery::Eq(bytes),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Alice");
        assert_eq!(result[0].age, 30);

        // 错误的组合应不命中
        let invalid = crate::index::virtual_field_value(&[
            Some(&Fv::Text("Alice".to_string())),
            Some(&Fv::U64(32)),
        ])
        .unwrap();

        let result_none: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "name-age".to_string(),
                    RangeQuery::Eq(invalid),
                ))),
                ..Default::default()
            })
            .await?;
        assert!(result_none.is_empty());

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_persistence() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let object_store = db.object_store();

        // 创建集合并添加文档
        {
            let collection = create_test_collection(&db, async |collection| {
                // 创建索引
                collection.create_btree_index_nx(&["name"]).await?;
                collection.create_btree_index_nx(&["age"]).await?;
                collection.create_btree_index_nx(&["tags"]).await?;

                // 创建搜索索引
                collection
                    .create_bm25_index_nx(&["name", "tags", "metadata"])
                    .await?;
                collection
                    .create_hnsw_index_nx(
                        "vector",
                        HnswConfig {
                            dimension: 10,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(())
            })
            .await?;

            // 添加文档
            let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
            let doc_obj = Document::try_from(collection.schema(), &doc)?;
            collection.add(doc_obj).await?;

            // 刷新以确保数据被持久化
            // collection.flush(unix_ms()).await?;

            // 关闭集合
            // collection.close().await?;
        }

        // 关闭并持久化数据库
        db.close().await?;

        // 重新打开数据库和集合
        let db = AndaDB::connect(
            object_store.clone(),
            DBConfig {
                name: "test_db".to_string(),
                description: "Test database".to_string(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;

        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;

        // 验证文档是否正确加载
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("Alice".to_string())),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Alice");
        assert_eq!(result[0].age, 30);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_read_only_mode() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        // 添加一个文档
        let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
        let doc_obj = Document::try_from(collection.schema(), &doc)?;
        collection.add(doc_obj).await?;

        let mut too_deep = Fv::Text("leaf".to_string());
        for _ in 0..70 {
            too_deep = Fv::Array(vec![too_deep]);
        }
        let err = collection
            .save_extension("too_deep".to_string(), too_deep)
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Schema { .. }));

        // 设置为只读模式
        collection.set_read_only(true);

        let err = collection
            .save_extension("blocked".to_string(), Fv::Text("value".to_string()))
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Generic { .. }));

        let err = collection.remove_extension("blocked").await.unwrap_err();
        assert!(matches!(err, DBError::Generic { .. }));

        // 尝试添加另一个文档，应该失败
        let doc2 = create_test_doc(0, "Bob", 25, vec!["tall", "quiet"]);
        let doc_obj2 = Document::try_from(collection.schema(), &doc2)?;
        let result = collection.add(doc_obj2).await;

        assert!(result.is_err());

        // 验证读取操作仍然有效
        let result: TestDoc = collection.get_as(1).await?;
        assert_eq!(result.name, "Alice");

        // 恢复为读写模式
        collection.set_read_only(false);

        // 现在应该可以添加文档
        let doc3 = create_test_doc(0, "Charlie", 35, vec!["smart", "tall"]);
        let doc_obj3 = Document::try_from(collection.schema(), &doc3)?;
        let id = collection.add(doc_obj3).await?;
        assert_eq!(id, 2);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_error_handling() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            // 测试创建已存在的索引
            collection.create_btree_index_nx(&["name"]).await?;
            let result = collection.create_btree_index(&["name"]).await;
            assert!(result.is_err());
            Ok(())
        })
        .await?;

        // 测试获取不存在的文档
        let result = collection.get(999).await;
        assert!(result.is_err());

        // 测试删除不存在的文档
        let result = collection.remove(999).await;
        assert!(result.is_ok());

        // 测试无效的查询
        let result: Result<Vec<TestDoc>, DBError> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "non_existent_field".to_string(),
                    RangeQuery::Eq(Fv::Text("value".to_string())),
                ))),
                ..Default::default()
            })
            .await;

        assert!(result.is_err());

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_get_and_search_propagate_corrupt_document_errors() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        let doc = create_test_doc(0, "Alice", 30, vec!["smart"]);
        let id = collection.add_from(&doc).await?;
        collection.flush(unix_ms()).await?;
        collection
            .storage
            .put_bytes(
                &Collection::doc_path(id),
                Bytes::from_static(b"not valid cbor"),
                PutMode::Overwrite,
            )
            .await?;

        let err = collection.get(id).await.unwrap_err();
        assert!(matches!(err, DBError::Serialization { .. }));

        let err = collection
            .search(Query {
                filter: Some(Filter::Field((
                    Schema::ID_KEY.to_string(),
                    RangeQuery::Eq(Fv::U64(id)),
                ))),
                limit: Some(1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Serialization { .. }));

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_btree_array_index_rejects_mismatched_hook_values() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["tags"]).await?;
            collection.set_index_hooks(Arc::new(BadArrayBTreeHooks));
            Ok(())
        })
        .await?;

        let doc = create_test_doc(0, "Alice", 30, vec!["smart"]);
        let doc = Document::try_from(collection.schema(), &doc)?;
        let err = collection.add(doc).await.unwrap_err();
        assert!(matches!(err, DBError::Index { .. }));
        assert!(collection.is_empty());

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_index_backfill_and_update_errors_cleanup_partial_state() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        let doc = create_test_doc(0, "Alice", 30, vec!["smart"]);
        let id = collection.add_from(&doc).await?;
        assert_eq!(id, 1);

        let object_store = db.object_store();
        db.close().await?;
        let db = AndaDB::connect(
            object_store,
            DBConfig {
                name: "test_db".to_string(),
                description: "Test database".to_string(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;

        let collection = db
            .open_collection("test_collection".to_string(), async |collection| {
                assert!(matches!(
                    collection.create_btree_index_nx(&["missing"]).await,
                    Err(DBError::Schema { .. })
                ));
                assert!(matches!(
                    collection.create_bm25_index_nx(&["missing"]).await,
                    Err(DBError::Schema { .. })
                ));
                assert!(matches!(
                    collection
                        .create_hnsw_index_nx("age", HnswConfig::default())
                        .await,
                    Err(DBError::Schema { .. })
                ));
                assert!(matches!(
                    collection
                        .create_hnsw_index_nx(
                            "vector",
                            HnswConfig {
                                dimension: 10,
                                ef_search: HnswConfig::MAX_EF_SEARCH + 1,
                                ..Default::default()
                            },
                        )
                        .await,
                    Err(DBError::Index { .. })
                ));

                collection.set_index_hooks(Arc::new(BackfillErrorHooks::btree(Fv::I64(-1))));
                let err = collection.create_btree_index(&["tags"]).await.unwrap_err();
                assert!(matches!(err, DBError::Index { .. }));
                assert!(collection.get_btree_index(&["tags"]).is_err());

                let err = collection
                    .create_btree_index(&["name", "age"])
                    .await
                    .unwrap_err();
                assert!(matches!(err, DBError::Index { .. }));
                assert!(collection.get_btree_index(&["name", "age"]).is_err());

                collection.set_index_hooks(Arc::new(BackfillErrorHooks::hnsw(vec![
                    bf16::from_f32(0.1),
                ])));
                let err = collection
                    .create_hnsw_index(
                        "vector",
                        HnswConfig {
                            dimension: 10,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap_err();
                assert!(matches!(err, DBError::Index { .. }));
                assert!(collection.get_hnsw_index("vector").is_err());

                collection.set_index_hooks(Arc::new(DefaultIndexHooks));
                collection.create_btree_index(&["tags"]).await?;
                collection.set_index_hooks(Arc::new(UpdateTagsBTreeErrorHooks));

                let err = collection
                    .update(
                        id,
                        BTreeMap::from([
                            ("age".to_string(), Fv::U64(31)),
                            (
                                "tags".to_string(),
                                Fv::Array(vec![Fv::Text("updated".to_string())]),
                            ),
                        ]),
                    )
                    .await
                    .unwrap_err();
                assert!(matches!(err, DBError::Index { .. }));

                let stored: TestDoc = collection.get_as(id).await?;
                assert_eq!(stored.age, 30);
                assert_eq!(stored.tags, vec!["smart".to_string()]);

                Ok(())
            })
            .await?;

        assert_eq!(collection.len(), 1);
        db.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_operations() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            // 创建索引
            collection.create_btree_index_nx(&["name"]).await?;
            Ok(())
        })
        .await?;

        // 并发添加多个文档
        let mut handles = Vec::new();
        for i in 0..10 {
            let collection_clone = collection.clone();
            let handle = tokio::spawn(async move {
                let doc = create_test_doc(0, &format!("Person{i}"), 20 + i, vec!["tag"]);
                let doc_obj = Document::try_from(collection_clone.schema(), &doc).unwrap();
                collection_clone.add(doc_obj).await
            });
            handles.push(handle);
        }

        // 等待所有任务完成
        let mut ids = Vec::new();
        for handle in handles {
            let id = handle.await.unwrap()?;
            ids.push(id);
        }

        // 验证所有文档都被添加
        assert_eq!(ids.len(), 10);
        // 验证文档数量
        let stats = collection.stats();
        assert_eq!(stats.num_documents, 10);

        // 并发获取文档
        let mut handles = Vec::new();
        for id in ids {
            let collection_clone = collection.clone();
            let handle = tokio::spawn(async move { collection_clone.get_as::<TestDoc>(id).await });
            handles.push(handle);
        }

        // 等待所有任务完成
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_updates() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        // 记录初始版本
        let initial_version = collection.metadata().stats.version;

        // 添加文档应该更新元数据
        let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
        let doc_obj = Document::try_from(collection.schema(), &doc)?;
        collection.add(doc_obj).await?;

        // 验证版本已更新
        let new_version = collection.metadata().stats.version;
        assert!(new_version > initial_version);

        // 验证统计信息已更新
        let stats = collection.stats();
        assert_eq!(stats.num_documents, 1);
        assert_eq!(stats.insert_count, 1);

        // 删除文档应该更新元数据
        collection.remove(1).await?;

        // 验证统计信息已更新
        let stats = collection.stats();
        assert_eq!(stats.num_documents, 0);
        assert_eq!(stats.delete_count, 1);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_persists_dirty_indexes_even_when_collection_metadata_unchanged()
    -> Result<(), DBError> {
        let db = setup_test_db().await?;

        {
            let collection = create_test_collection(&db, async |collection| {
                collection.create_btree_index_nx(&["name"]).await?;
                Ok(())
            })
            .await?;

            let doc = create_test_doc(0, "Alice", 30, vec!["smart"]);
            let id = collection.add_from(&doc).await?;
            assert_eq!(id, 1);

            // First flush to persist the baseline state.
            // Use the same millisecond timestamp as the initial storage metadata save
            // to ensure checkpoint persistence is not skipped by rate limiting.
            let same_ms = collection.storage.stats().last_saved;
            assert!(collection.flush(same_ms).await?);

            // Mutate index-only state directly: remove the mapping from btree index.
            let index = collection.find_btree_index(&["name"])?;
            assert!(index.remove(id, &Fv::Text("Alice".to_string()), unix_ms()));

            // Collection metadata version is unchanged, but index is dirty and must still flush.
            assert!(collection.flush(unix_ms()).await?);
        }

        // Reopen and verify the index-only change is durable.
        db.close().await?;
        let db = AndaDB::connect(
            db.object_store(),
            DBConfig {
                name: "test_db".to_string(),
                description: "Test database".to_string(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;

        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;

        let ids = collection
            .search_ids(Query {
                filter: Some(Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("Alice".to_string())),
                ))),
                ..Default::default()
            })
            .await?;
        assert!(ids.is_empty());

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_by_field_result_ordering() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        for (name, age) in [("Alice", 30_u32), ("Bob", 25_u32), ("Charlie", 35_u32)] {
            let doc = create_test_doc(0, name, age, vec!["x"]);
            collection.add_from(&doc).await?;
        }

        // candidates 为空：结果应为正序
        let filter_all = Filter::Field((Schema::ID_KEY.to_string(), RangeQuery::Gt(Fv::U64(0))));
        let ids = collection.filter_by_field(filter_all, &[], 0)?;
        assert_eq!(ids, vec![1, 2, 3]);

        // candidates 非空：结果顺序应遵循 candidates 顺序
        let filter_subset = Filter::Field((Schema::ID_KEY.to_string(), RangeQuery::Ge(Fv::U64(2))));
        let ids = collection.filter_by_field(filter_subset, &[3, 1, 2], 0)?;
        assert_eq!(ids, vec![3, 2]);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_document_updates() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            // 创建索引以测试更新对索引的影响
            collection.create_btree_index_nx(&["name"]).await?;
            collection.create_btree_index_nx(&["age"]).await?;
            collection.create_btree_index_nx(&["tags"]).await?;
            Ok(())
        })
        .await?;

        // 添加文档
        let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
        let doc_obj = Document::try_from(collection.schema(), &doc)?;
        let id = collection.add(doc_obj).await?;

        // 更新文档
        let mut update_fields = BTreeMap::new();
        update_fields.insert("name".to_string(), Fv::Text("Alice Updated".to_string()));
        update_fields.insert("age".to_string(), Fv::U64(31));
        update_fields.insert(
            "tags".to_string(),
            Fv::Array(vec![
                Fv::Text("smart".to_string()),
                Fv::Text("friendly".to_string()),
                Fv::Text("updated".to_string()),
            ]),
        );

        collection.update(id, update_fields.clone()).await?;

        // 获取并验证更新后的文档
        let updated_doc: TestDoc = collection.get_as(id).await?;
        assert_eq!(updated_doc.name, "Alice Updated");
        assert_eq!(updated_doc.age, 31);
        assert_eq!(updated_doc.tags.len(), 3);
        assert!(updated_doc.tags.contains(&"updated".to_string()));

        // 通过索引验证更新是否生效
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("Alice Updated".to_string())),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].age, 31);

        // 验证原来的值不再能被索引查询到
        let result: Vec<TestDoc> = collection
            .search_as(Query {
                filter: Some(Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("Alice".to_string())),
                ))),
                ..Default::default()
            })
            .await?;

        assert_eq!(result.len(), 0);

        // 测试部分更新
        let mut partial_update = BTreeMap::new();
        partial_update.insert("age".to_string(), Fv::U64(32));

        collection.update(id, partial_update).await?;

        let partially_updated: TestDoc = collection.get_as(id).await?;
        assert_eq!(partially_updated.name, "Alice Updated"); // 未更改
        assert_eq!(partially_updated.age, 32); // 已更改

        // 测试更新不存在的文档
        let result = collection.update(999, update_fields.clone()).await;
        assert!(result.is_err());

        // 测试只读模式下的更新失败
        collection.set_read_only(true);
        let result = collection.update(id, update_fields.clone()).await;
        assert!(result.is_err());

        // 恢复读写模式
        collection.set_read_only(false);

        // 测试更新元数据字段
        let mut metadata_update = BTreeMap::new();
        let mut metadata_map = BTreeMap::new();
        metadata_map.insert("key1".into(), Fv::Text("value1".to_string()));
        metadata_map.insert("key2".into(), Fv::U64(42));
        metadata_update.insert("metadata".into(), Fv::Map(metadata_map));

        collection.update(id, metadata_update).await?;

        let doc_with_metadata: TestDoc = collection.get_as(id).await?;
        assert_eq!(doc_with_metadata.metadata.len(), 2);
        assert!(
            matches!(doc_with_metadata.metadata.get("key1"), Some(Json::String(s)) if s == "value1")
        );
        assert!(
            matches!(doc_with_metadata.metadata.get("key2"), Some(Json::Number(n)) if n.as_i64() == Some(42))
        );

        // 验证统计信息已更新
        let stats = collection.stats();
        assert_eq!(stats.update_count, 3); // 初始更新 + 部分更新 + 元数据更新，只读失败不计数

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_extension_get_set_remove() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        // 初始状态：无扩展数据
        assert!(collection.get_extension("key1").is_none());
        assert!(collection.metadata().extensions.is_empty());

        // set_extension：设置后可以 get 到
        collection.set_extension("key1".into(), FieldValue::Text("hello".into()));
        assert_eq!(
            collection.get_extension("key1"),
            Some(FieldValue::Text("hello".into()))
        );

        // 支持不同类型
        collection.set_extension("count".into(), FieldValue::U64(42));
        collection.set_extension("flag".into(), FieldValue::Bool(true));
        assert_eq!(collection.get_extension("count"), Some(FieldValue::U64(42)));
        assert_eq!(
            collection.get_extension("flag"),
            Some(FieldValue::Bool(true))
        );

        // 覆盖已有 key
        collection.set_extension("key1".into(), FieldValue::I64(-1));
        assert_eq!(collection.get_extension("key1"), Some(FieldValue::I64(-1)));

        // metadata() 中也能看到 extensions
        let meta = collection.metadata();
        assert_eq!(meta.extensions.len(), 3);
        assert_eq!(meta.extensions.get("key1"), Some(&FieldValue::I64(-1)));

        // remove_extension：移除存在的 key
        let old = collection.remove_extension("count").await?;
        assert_eq!(old, Some(FieldValue::U64(42)));
        assert!(collection.get_extension("count").is_none());

        // remove_extension：移除不存在的 key 返回 None
        let old = collection.remove_extension("nonexistent").await?;
        assert!(old.is_none());

        db.close().await?;
        Ok(())
    }

    // Reconnects a fresh AndaDB over the same object store, so tests verify
    // what was actually persisted instead of hitting the in-memory collection
    // cache of the original AndaDB instance.
    async fn reconnect_test_db(db: AndaDB) -> Result<AndaDB, DBError> {
        let object_store = db.object_store();
        db.close().await?;
        drop(db);
        AndaDB::connect(
            object_store,
            DBConfig {
                name: "test_db".to_string(),
                description: "Test database".to_string(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_vector_get_field_is_canonical_after_reconnect() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;
        let source = create_test_doc(0, "vector", 1, vec![]);
        let expected = source.vector.clone();
        let id = collection.add_from(&source).await?;
        collection.flush(unix_ms()).await?;

        drop(collection);
        let db = reconnect_test_db(db).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        let document = collection.get(id).await?;
        assert_eq!(document.get_field("vector"), Some(&Fv::Vector(expected)));

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_extension_save_and_persist() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        // save_extension 会立即持久化
        collection
            .save_extension("persist_key".into(), FieldValue::Text("persisted".into()))
            .await?;
        assert_eq!(
            collection.get_extension("persist_key"),
            Some(FieldValue::Text("persisted".into()))
        );

        // 验证 last_saved 已被更新（save_extension 调用了 store_metadata）
        let stats = collection.stats();
        assert!(stats.last_saved > 0);

        // 重新连接数据库（绕过 AndaDB 的集合缓存），验证扩展数据真正落盘
        drop(collection);
        let db = reconnect_test_db(db).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;

        assert_eq!(
            collection.get_extension("persist_key"),
            Some(FieldValue::Text("persisted".into()))
        );

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_extension_flush_persist() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        // 使用 set_extension（不立即持久化），再 flush
        collection.set_extension("lazy_key".into(), FieldValue::Bytes(vec![1, 2, 3]));
        collection.flush(unix_ms()).await?;

        // 重新连接数据库（绕过 AndaDB 的集合缓存），验证扩展数据真正落盘。
        // 此前 set_extension 不递增元数据版本，flush 的快路径会跳过写盘，
        // 而旧测试从缓存拿到同一内存实例导致误通过。
        drop(collection);
        let db = reconnect_test_db(db).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;

        assert_eq!(
            collection.get_extension("lazy_key"),
            Some(FieldValue::Bytes(vec![1, 2, 3]))
        );

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_extension_persists() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        collection
            .save_extension("k".into(), FieldValue::U64(7))
            .await?;
        let old = collection.remove_extension("k").await?;
        assert_eq!(old, Some(FieldValue::U64(7)));

        drop(collection);
        let db = reconnect_test_db(db).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert!(collection.get_extension("k").is_none());

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_collection_set_extension_with() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        let key = "test_key".to_string();

        // 1. Initial state: None
        let old = collection.set_extension_with(key.clone(), |val| {
            assert!(val.is_none());
            Some(FieldValue::U64(100))
        });
        assert!(old.is_none());
        assert_eq!(collection.get_extension(&key), Some(FieldValue::U64(100)));

        // 2. Update existing value: 100 -> 200
        let old = collection.set_extension_with(key.clone(), |val| {
            if let Some(FieldValue::U64(v)) = val {
                return Some(FieldValue::U64(v + 100));
            }
            None
        });
        assert_eq!(old, Some(FieldValue::U64(100)));
        assert_eq!(collection.get_extension(&key), Some(FieldValue::U64(200)));

        // 3. Return None: No change
        let old = collection.set_extension_with(key.clone(), |_| None);
        assert!(old.is_none());
        assert_eq!(collection.get_extension(&key), Some(FieldValue::U64(200)));

        db.close().await?;
        Ok(())
    }

    async fn count_objects(object_store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
        let mut stream = object_store.list(Some(&Path::from(prefix)));
        let mut count = 0;
        while let Some(item) = stream.next().await {
            item.expect("list should succeed");
            count += 1;
        }
        count
    }

    #[tokio::test]
    async fn test_removed_index_can_be_recreated() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let object_store = db.object_store();
        let db_config = || DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };

        {
            let collection = create_test_collection(&db, async |collection| {
                collection.create_btree_index_nx(&["name"]).await?;
                collection.create_bm25_index_nx(&["name", "tags"]).await?;
                collection
                    .create_hnsw_index_nx(
                        "vector",
                        HnswConfig {
                            dimension: 10,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(())
            })
            .await?;

            for i in 1..=3u64 {
                let doc = create_test_doc(0, &format!("user_{i}"), 20 + i as u32, vec!["x"]);
                collection.add_from(&doc).await?;
            }
            db.close().await?;
        }

        // Remove the indexes; their storage objects must be deleted.
        let db = AndaDB::connect(object_store.clone(), db_config()).await?;
        let _ = db
            .open_collection("test_collection".to_string(), async |collection| {
                assert!(collection.remove_btree_index(&["name"]).await?);
                assert!(collection.remove_bm25_index(&["name", "tags"]).await?);
                assert!(collection.remove_hnsw_index("vector").await?);
                Ok(())
            })
            .await?;
        assert_eq!(
            count_objects(&object_store, "test_db/test_collection/btree_indexes").await,
            0
        );
        assert_eq!(
            count_objects(&object_store, "test_db/test_collection/bm25_indexes").await,
            0
        );
        assert_eq!(
            count_objects(&object_store, "test_db/test_collection/hnsw_indexes").await,
            0
        );
        db.close().await?;

        // Simulate leftover files from a crashed index creation: a stale meta
        // object must not block re-creation.
        object_store
            .put_opts(
                &Path::from("test_db/test_collection/btree_indexes/name/meta.cbor"),
                PutPayload::from(Bytes::from_static(b"stale")),
                PutOptions::default(),
            )
            .await
            .unwrap();

        // Re-creating the same indexes used to fail with AlreadyExists because
        // the old index files were left behind.
        let db = AndaDB::connect(object_store.clone(), db_config()).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |collection| {
                collection.create_btree_index(&["name"]).await?;
                collection.create_bm25_index(&["name", "tags"]).await?;
                collection
                    .create_hnsw_index(
                        "vector",
                        HnswConfig {
                            dimension: 10,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(())
            })
            .await?;

        // Backfill repopulated the fresh indexes from the existing documents.
        let ids = collection
            .query_ids(
                Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("user_2".to_string())),
                )),
                None,
            )
            .await?;
        assert_eq!(ids.len(), 1);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_query_ids_with_huge_limit_does_not_overallocate() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["age"]).await?;
            Ok(())
        })
        .await?;

        for i in 1..=5u64 {
            let doc = create_test_doc(0, &format!("user_{i}"), 20 + i as u32, vec!["x"]);
            collection.add_from(&doc).await?;
        }

        // A huge limit must not pre-allocate a huge buffer up front (this
        // previously aborted with a capacity overflow via reserve_exact).
        let ids = collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Ge(Fv::U64(21)))),
                Some(usize::MAX),
            )
            .await?;
        assert_eq!(ids.len(), 5);

        let ids = collection
            .query_ids(
                Filter::Field((Schema::ID_KEY.to_string(), RangeQuery::Ge(Fv::U64(1)))),
                Some(usize::MAX),
            )
            .await?;
        assert_eq!(ids.len(), 5);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_search_with_lt_filter_keeps_most_relevant() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            collection
                .create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

        // 4 documents at increasing distance from the query vector, so the
        // relevance order of the candidates is deterministic: 1, 2, 3, 4.
        for (i, base) in [0.1f32, 0.2, 0.4, 0.8].into_iter().enumerate() {
            let mut doc = create_test_doc(0, &format!("doc{i}"), 20 + i as u32, vec!["x"]);
            doc.vector = std::iter::repeat_n(bf16::from_f32(base), 10).collect();
            collection.add_from(&doc).await?;
        }

        // Hybrid search + Lt filter matching everything, with more hits than
        // the limit: the retained results must be the MOST relevant ones
        // (head of the relevance-ordered candidates), not the tail.
        let ids = collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(vec![0.1; 10]),
                    ..Default::default()
                }),
                filter: Some(Filter::Field((
                    Schema::ID_KEY.to_string(),
                    RangeQuery::Lt(Fv::U64(100)),
                ))),
                limit: Some(2),
            })
            .await?;
        assert_eq!(ids, vec![1, 2]);

        // Pure filter queries with Lt keep the tail (largest ids), unchanged.
        let ids = collection
            .search_ids(Query {
                filter: Some(Filter::Field((
                    Schema::ID_KEY.to_string(),
                    RangeQuery::Lt(Fv::U64(100)),
                ))),
                limit: Some(2),
                ..Default::default()
            })
            .await?;
        assert_eq!(ids, vec![3, 4]);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_type_mismatch_returns_error() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["age"]).await?;
            Ok(())
        })
        .await?;
        collection
            .add_from(&create_test_doc(0, "Alice", 30, vec!["x"]))
            .await?;

        // A filter value whose type does not match the index key type is a
        // caller bug and must surface as an error, not an empty result.
        let err = collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::Text("30".into())))),
                Some(10),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Index { .. }));

        db.close().await?;
        Ok(())
    }

    #[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
    struct DualVectorDoc {
        pub _id: u64,
        pub name: String,
        pub vec_a: Vector,
        pub vec_b: Vector,
    }

    #[tokio::test]
    async fn test_search_with_multiple_hnsw_dimensions_and_missing_indexes() -> Result<(), DBError>
    {
        let db = setup_test_db().await?;
        let collection = db
            .open_or_create_collection(
                DualVectorDoc::schema()?,
                CollectionConfig {
                    name: "dual_vec".to_string(),
                    description: "two vector fields".to_string(),
                },
                async |c| {
                    c.create_hnsw_index_nx(
                        "vec_a",
                        HnswConfig {
                            dimension: 4,
                            ..Default::default()
                        },
                    )
                    .await?;
                    c.create_hnsw_index_nx(
                        "vec_b",
                        HnswConfig {
                            dimension: 8,
                            ..Default::default()
                        },
                    )
                    .await?;
                    Ok(())
                },
            )
            .await?;

        let id = collection
            .add_from(&DualVectorDoc {
                _id: 0,
                name: "a".into(),
                vec_a: std::iter::repeat_n(bf16::from_f32(0.1), 4).collect(),
                vec_b: std::iter::repeat_n(bf16::from_f32(0.2), 8).collect(),
            })
            .await?;

        // A 4-dim query searches only the matching index and succeeds even
        // though another index has a different dimension.
        let ids = collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(vec![0.1; 4]),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;
        assert_eq!(ids, vec![id]);

        // An 8-dim query hits the other index.
        let ids = collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(vec![0.2; 8]),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;
        assert_eq!(ids, vec![id]);

        // A query dimension matching no index is an error, not silence.
        let err = collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(vec![0.3; 5]),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Index { .. }));

        // Text search without any BM25 index is an error, not silence.
        let err = collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("a".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Index { .. }));

        db.close().await?;
        Ok(())
    }

    /// Regression (#20): a hybrid text+vector query whose vector dimension
    /// matches no HNSW index degrades to text-only results instead of
    /// failing the whole query; a pure vector query still errors.
    #[tokio::test]
    async fn test_hybrid_search_degrades_when_vector_matches_no_index() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_bm25_index_nx(&["name"]).await?;
            collection
                .create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;
        let id = collection
            .add_from(&create_test_doc(0, "alpha beta", 30, vec!["x"]))
            .await?;

        // Hybrid: the BM25 hit is kept, the mismatched vector part is
        // dropped with a warning.
        let ids = collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("alpha".to_string()),
                    vector: Some(vec![0.1; 5]),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;
        assert_eq!(ids, vec![id]);

        // Pure vector misuse still surfaces as an error.
        let err = collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(vec![0.1; 5]),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Index { .. }));

        db.close().await?;
        Ok(())
    }

    // Document with a signed counter, for the I64/U64 read-back regressions.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, AndaDBSchema)]
    struct CounterDoc {
        pub _id: u64,
        pub name: String,
        pub count: i64,
    }

    fn counter_config() -> CollectionConfig {
        CollectionConfig {
            name: "counters".to_string(),
            description: "signed counters".to_string(),
        }
    }

    /// Regression (#1/#9, cases a+b): a non-negative i64 field reads back
    /// from storage as U64. `update` must retire the old B-tree entry (no
    /// phantom match on the old value) and `remove` must not leak entries.
    #[tokio::test]
    async fn test_i64_btree_index_update_and_remove_after_read_back() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = db
            .open_or_create_collection(CounterDoc::schema()?, counter_config(), async |c| {
                c.create_btree_index_nx(&["count"]).await?;
                Ok(())
            })
            .await?;

        let id = collection
            .add_from(&CounterDoc {
                _id: 0,
                name: "a".to_string(),
                count: 5,
            })
            .await?;

        // (a) update: the old document is materialized from storage where
        // `count: 5` reads back as U64(5); the old index entry must still be
        // retired.
        collection
            .update(id, BTreeMap::from([("count".to_string(), Fv::I64(7))]))
            .await?;
        let stale = collection
            .query_ids(
                Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(5)))),
                Some(10),
            )
            .await?;
        assert!(
            stale.is_empty(),
            "stale index entry for the old value must be removed, got {stale:?}",
        );
        let current = collection
            .query_ids(
                Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(7)))),
                Some(10),
            )
            .await?;
        assert_eq!(current, vec![id]);

        // (b) remove: the index entry must be dropped, not leaked.
        assert!(collection.remove(id).await?.is_some());
        let leaked = collection
            .query_ids(
                Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(7)))),
                Some(10),
            )
            .await?;
        assert!(leaked.is_empty(), "removed document must leave no entries");
        assert_eq!(
            collection.get_btree_index(&["count"])?.stats().num_elements,
            0,
        );

        db.close().await?;
        Ok(())
    }

    /// Regression (#1/#9, case c): creating a B-tree index over existing
    /// non-negative i64 data backfills from storage, where the values read
    /// back as U64 — index creation must succeed and the index must be
    /// queryable.
    #[tokio::test]
    async fn test_i64_btree_index_backfill_over_existing_data() -> Result<(), DBError> {
        let db = setup_test_db().await?;
        let collection = db
            .open_or_create_collection(CounterDoc::schema()?, counter_config(), async |_| Ok(()))
            .await?;
        let mut ids = Vec::new();
        for (i, count) in [5i64, -2, 0].into_iter().enumerate() {
            ids.push(
                collection
                    .add_from(&CounterDoc {
                        _id: 0,
                        name: format!("doc{i}"),
                        count,
                    })
                    .await?,
            );
        }
        collection.flush(unix_ms()).await?;
        drop(collection);
        db.close_collection("counters").await?;

        // Reopen from storage and build the index over the existing data.
        let collection = db
            .open_collection("counters".to_string(), async |c| {
                c.create_btree_index(&["count"]).await?;
                Ok(())
            })
            .await?;
        let found = collection
            .query_ids(
                Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(5)))),
                Some(10),
            )
            .await?;
        assert_eq!(found, vec![ids[0]]);
        let found = collection
            .query_ids(
                Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(-2)))),
                Some(10),
            )
            .await?;
        assert_eq!(found, vec![ids[1]]);

        db.close().await?;
        Ok(())
    }

    /// Regression (#18): `save_extension` / `remove_extension` persist with
    /// a single metadata put ("Ok means persisted") and must not advance the
    /// flush watermark — the next full flush still runs its complete path.
    #[tokio::test]
    async fn test_save_extension_single_put_without_claiming_flush() -> Result<(), DBError> {
        let object_store = Arc::new(InMemory::new());
        let db_config = DBConfig {
            name: "ext_db".to_string(),
            description: "extension persistence".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), db_config.clone()).await?;
        let collection = db
            .open_or_create_collection(CounterDoc::schema()?, counter_config(), async |_| Ok(()))
            .await?;

        let puts_before = collection.storage_stats().total_put_count;
        collection
            .save_extension("k".to_string(), Fv::Text("v".to_string()))
            .await?;
        assert_eq!(
            collection.storage_stats().total_put_count,
            puts_before + 1,
            "save_extension must perform exactly one metadata put",
        );

        // Persisted immediately: a second database instance reads it back
        // without any flush on the first one.
        {
            let db2 = AndaDB::connect(object_store.clone(), db_config.clone()).await?;
            let c2 = db2
                .open_collection("counters".to_string(), async |_| Ok(()))
                .await?;
            assert_eq!(c2.get_extension("k"), Some(Fv::Text("v".to_string())));
        }

        // The unclaimed write did not advance `last_saved_version`: the next
        // full flush still persists metadata + ids, and only then does the
        // fast path apply.
        assert!(collection.flush(unix_ms()).await?);
        assert!(!collection.flush(unix_ms()).await?);

        // remove_extension mirrors the single-put behaviour.
        let puts_before = collection.storage_stats().total_put_count;
        let old = collection.remove_extension("k").await?;
        assert_eq!(old, Some(Fv::Text("v".to_string())));
        assert_eq!(
            collection.storage_stats().total_put_count,
            puts_before + 1,
            "remove_extension must perform exactly one metadata put",
        );
        assert!(collection.get_extension("k").is_none());
        // Removing a missing key writes nothing.
        let puts_before = collection.storage_stats().total_put_count;
        assert!(collection.remove_extension("missing").await?.is_none());
        assert_eq!(collection.storage_stats().total_put_count, puts_before);

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_open_repair_scan_skips_unreadable_and_mismatched_documents() -> Result<(), DBError>
    {
        let db = setup_test_db().await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;
        let id1 = collection
            .add_from(&create_test_doc(0, "Alice", 30, vec!["a"]))
            .await?;
        collection.flush(unix_ms()).await?; // persisted checkpoint = 1

        // id 2: corrupt CBOR object inside the repair scan window.
        collection
            .storage
            .put_bytes(
                &Collection::doc_path(2),
                Bytes::from_static(b"not valid cbor"),
                PutMode::Overwrite,
            )
            .await?;
        // id 3: valid CBOR that does not match the collection schema.
        let bad_doc = DocumentOwned {
            fields: BTreeMap::from([(0usize, Fv::Text("not an id".to_string()))]),
        };
        collection
            .storage
            .create(&Collection::doc_path(3), &bad_doc)
            .await?;
        // id 4: a valid orphan document, recoverable.
        let orphan = Document::try_from(
            collection.schema(),
            &create_test_doc(4, "Carol", 40, vec!["c"]),
        )?;
        collection
            .storage
            .create(&Collection::doc_path(4), &orphan)
            .await?;

        // Reopening must not fail on the corrupt or mismatched objects
        // (previously a schema mismatch bricked the whole collection open),
        // and the valid orphan behind them must still be recovered.
        drop(collection);
        let db = reconnect_test_db(db).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert!(collection.contains(id1));
        assert!(!collection.contains(2));
        assert!(!collection.contains(3));
        assert!(collection.contains(4));
        let recovered: TestDoc = collection.get_as(4).await?;
        assert_eq!(recovered.name, "Carol");

        db.close().await?;
        Ok(())
    }

    /// An object store that blocks `put` for paths ending in `gate_suffix`
    /// until the watch gate is opened, to deterministically hold an `add`
    /// in flight while a flush runs.
    #[derive(Debug)]
    struct GatedPutStore {
        inner: Arc<InMemory>,
        gate_suffix: String,
        gate: tokio::sync::watch::Receiver<bool>,
        blocked: Arc<TestAtomicBool>,
    }

    impl fmt::Display for GatedPutStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("GatedPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for GatedPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            if location.to_string().ends_with(&self.gate_suffix) {
                let mut rx = self.gate.clone();
                while !*rx.borrow() {
                    self.blocked.store(true, TestOrdering::Release);
                    rx.changed()
                        .await
                        .map_err(|_| object_store::Error::Generic {
                            store: "gated_put",
                            source: "gate sender dropped".into(),
                        })?;
                }
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[derive(Debug)]
    enum PutFault {
        FailOnce {
            armed: Arc<TestAtomicBool>,
        },
        BlockAfterCommit {
            gate: tokio::sync::watch::Receiver<bool>,
            blocked: Arc<TestAtomicBool>,
        },
    }

    /// Injects one path-specific PUT failure, or blocks after the delegated
    /// PUT is already durable but before success is returned to the caller.
    #[derive(Debug)]
    struct FaultPutStore {
        inner: Arc<InMemory>,
        suffix: String,
        fault: PutFault,
    }

    impl fmt::Display for FaultPutStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("FaultPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FaultPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            let matches = location.to_string().ends_with(&self.suffix);
            if matches
                && let PutFault::FailOnce { armed } = &self.fault
                && armed.swap(false, TestOrdering::AcqRel)
            {
                return Err(object_store::Error::Generic {
                    store: "fault_put",
                    source: "injected one-shot PUT failure".into(),
                });
            }

            let result = self.inner.put_opts(location, payload, opts).await?;
            if matches && let PutFault::BlockAfterCommit { gate, blocked } = &self.fault {
                let mut rx = gate.clone();
                while !*rx.borrow() {
                    blocked.store(true, TestOrdering::Release);
                    rx.changed()
                        .await
                        .map_err(|_| object_store::Error::Generic {
                            store: "fault_put",
                            source: "gate sender dropped".into(),
                        })?;
                }
            }
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Regression (P0-04): F1 is held after serializing its old ids snapshot
    /// but before the conditional ids PUT completes. An add and F2 queue
    /// behind the collection-wide gate in that order. Once released, F1 must
    /// publish its version, the add completes, and only then may F2 take the
    /// new snapshot/version and advance the checkpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_flushes_bind_checkpoint_to_serialized_ids_generation()
    -> Result<(), DBError> {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(true);
        let blocked = Arc::new(TestAtomicBool::new(false));
        let object_store: Arc<dyn ObjectStore> = Arc::new(GatedPutStore {
            inner: Arc::new(InMemory::new()),
            gate_suffix: "test_collection/ids.cbor".to_string(),
            gate: gate_rx,
            blocked: blocked.clone(),
        });
        let db_config = || DBConfig {
            name: "test_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), db_config()).await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;
        assert_eq!(
            collection
                .add_from(&create_test_doc(0, "one", 20, vec!["one"]))
                .await?,
            1
        );

        gate_tx.send(false).expect("gate receiver dropped");
        let first_flush = {
            let collection = collection.clone();
            tokio::spawn(async move { collection.flush(unix_ms()).await })
        };
        while !blocked.load(TestOrdering::Acquire) {
            tokio::task::yield_now().await;
        }

        // Queue the mutation before F2. Tokio's fair RwLock makes F2 observe
        // the completed mutation instead of overtaking it with a stale ids
        // snapshot and then treating a CAS conflict as success.
        let adding = {
            let collection = collection.clone();
            tokio::spawn(async move {
                collection
                    .add_from(&create_test_doc(0, "two", 21, vec!["two"]))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let second_flush = {
            let collection = collection.clone();
            tokio::spawn(async move { collection.flush(unix_ms()).await })
        };
        assert!(!adding.is_finished());
        assert!(!second_flush.is_finished());

        gate_tx.send(true).expect("gate receiver dropped");
        assert!(first_flush.await.expect("first flush panicked")?);
        assert_eq!(adding.await.expect("add task panicked")?, 2);
        assert!(second_flush.await.expect("second flush panicked")?);
        assert!(collection.storage.stats().check_point >= 2);

        drop(collection);
        drop(db);
        let db = AndaDB::connect(object_store, db_config()).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(collection.ids(), vec![1, 2]);
        db.close().await?;
        Ok(())
    }

    /// Once collection metadata is durable, a failure in ids.cbor poisons the
    /// handle: the checkpoint is a sequence of dependent writes and the
    /// in-memory watermarks no longer describe what is durable. Reopening
    /// converges from the WAL and the repair scan; no document is lost.
    #[tokio::test]
    async fn test_failed_ids_phase_poisons_handle_and_reopen_converges() -> Result<(), DBError> {
        let armed = Arc::new(TestAtomicBool::new(false));
        let object_store: Arc<dyn ObjectStore> = Arc::new(FaultPutStore {
            inner: Arc::new(InMemory::new()),
            suffix: "test_collection/ids.cbor".to_string(),
            fault: PutFault::FailOnce {
                armed: armed.clone(),
            },
        });
        let config = DBConfig {
            name: "test_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;
        collection
            .add_from(&create_test_doc(0, "one", 20, vec!["one"]))
            .await?;
        collection.flush(unix_ms()).await?;
        collection
            .add_from(&create_test_doc(0, "two", 21, vec!["two"]))
            .await?;

        let same_ms = unix_ms();
        armed.store(true, TestOrdering::Release);
        assert!(collection.flush(same_ms).await.is_err());
        assert!(collection.is_poisoned());
        // Every further operation on the poisoned handle is rejected.
        let err = collection
            .flush(same_ms)
            .await
            .expect_err("poisoned handle must reject flush");
        assert!(err.to_string().contains("poisoned"), "{err}");
        assert!(
            collection
                .add_from(&create_test_doc(0, "three", 22, vec!["three"]))
                .await
                .is_err()
        );

        // Reopening through the same database discards the poisoned handle
        // (without flushing it) and loads a fresh generation. The WAL replay
        // recovers document 2 and the flush inside `open_collection` already
        // persists the converged checkpoint and retires the WAL.
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(collection.ids(), vec![1, 2]);
        assert!(collection.pending_mutations.lock().is_empty());
        let mut intents = collection
            .storage
            .list_meta(Some(Collection::MUTATION_INTENT_PREFIX), None);
        assert!(intents.next().await.is_none(), "WAL retired after reopen");
        assert!(collection.storage.stats().check_point >= 2);

        drop(collection);
        db.close().await?;
        let db = AndaDB::connect(object_store, config).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(collection.ids(), vec![1, 2]);
        db.close().await?;
        Ok(())
    }

    /// A conditional metadata PUT can commit before its future reports
    /// success. Aborting at that boundary poisons the handle (cancellation is
    /// treated as a crash); reopening loads the durable state and the
    /// retained WAL converges ids and indexes without losing the document.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancelled_metadata_put_poisons_handle_and_reopen_converges() -> Result<(), DBError>
    {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(true);
        let blocked = Arc::new(TestAtomicBool::new(false));
        let object_store: Arc<dyn ObjectStore> = Arc::new(FaultPutStore {
            inner: Arc::new(InMemory::new()),
            suffix: "test_collection/meta.cbor".to_string(),
            fault: PutFault::BlockAfterCommit {
                gate: gate_rx,
                blocked: blocked.clone(),
            },
        });
        let config = DBConfig {
            name: "test_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["name"]).await
        })
        .await?;
        let id = collection
            .add_from(&create_test_doc(0, "one", 20, vec!["one"]))
            .await?;
        assert_eq!(id, 1);

        gate_tx.send(false).expect("gate receiver dropped");
        let first_now = unix_ms();
        let flushing = {
            let collection = collection.clone();
            tokio::spawn(async move { collection.flush(first_now).await })
        };
        while !blocked.load(TestOrdering::Acquire) {
            tokio::task::yield_now().await;
        }

        flushing.abort();
        assert!(
            flushing
                .await
                .expect_err("flush should be cancelled")
                .is_cancelled()
        );
        gate_tx.send(true).expect("gate receiver dropped");
        assert!(collection.is_poisoned());

        // Every further operation on the poisoned handle is rejected.
        let err = collection
            .save_extension("after_cancel".to_string(), Fv::Text("durable".to_string()))
            .await
            .expect_err("poisoned handle must reject writes");
        assert!(err.to_string().contains("poisoned"), "{err}");

        // Reopening through the same database discards the poisoned handle
        // without flushing it. The watermark-bounded repair scan recovers the
        // committed document; a full flush then completes a checkpoint.
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(collection.ids(), vec![id]);
        collection.flush(unix_ms()).await?;
        let mut intents = collection
            .storage
            .list_meta(Some(Collection::MUTATION_INTENT_PREFIX), None);
        assert!(intents.next().await.is_none(), "WAL retired after reopen");
        assert!(collection.storage.stats().check_point >= id);

        drop(collection);
        db.close().await?;
        let db = AndaDB::connect(object_store, config).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(collection.ids(), vec![id]);
        let reopened: TestDoc = collection.get_as(id).await?;
        assert_eq!(reopened.name, "one");
        assert!(collection.storage.stats().check_point >= id);
        assert!(collection.pending_mutations.lock().is_empty());
        db.close().await?;
        Ok(())
    }

    /// An immediate metadata-only write has the same post-commit cancellation
    /// window as a full flush: aborting it poisons the handle. The committed
    /// extension write is durable and visible after a reopen; the unclaimed
    /// write never publishes the full-flush watermark.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancelled_unclaimed_metadata_put_poisons_handle() -> Result<(), DBError> {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(true);
        let blocked = Arc::new(TestAtomicBool::new(false));
        let object_store: Arc<dyn ObjectStore> = Arc::new(FaultPutStore {
            inner: Arc::new(InMemory::new()),
            suffix: "test_collection/meta.cbor".to_string(),
            fault: PutFault::BlockAfterCommit {
                gate: gate_rx,
                blocked: blocked.clone(),
            },
        });
        let config = DBConfig {
            name: "test_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;
        let saved_before = collection.last_saved_version.load(Ordering::Acquire);

        gate_tx.send(false).expect("gate receiver dropped");
        let saving = {
            let collection = collection.clone();
            tokio::spawn(async move {
                collection
                    .save_extension("before_cancel".to_string(), Fv::Text("durable".to_string()))
                    .await
            })
        };
        while !blocked.load(TestOrdering::Acquire) {
            tokio::task::yield_now().await;
        }
        saving.abort();
        assert!(
            saving
                .await
                .expect_err("metadata-only writer should be cancelled after commit")
                .is_cancelled()
        );
        gate_tx.send(true).expect("gate receiver dropped");
        assert!(collection.is_poisoned());
        assert_eq!(
            collection.last_saved_version.load(Ordering::Acquire),
            saved_before,
            "an unclaimed generation must not publish the full-flush watermark",
        );
        assert!(
            collection
                .add_from(&create_test_doc(0, "after", 21, vec!["after"]))
                .await
                .is_err(),
            "poisoned handle must reject mutations",
        );

        // Reopen through the same database: the committed extension write is
        // durable and visible in the fresh generation, which stays writable.
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(
            collection.get_extension("before_cancel"),
            Some(Fv::Text("durable".to_string()))
        );
        let id = collection
            .add_from(&create_test_doc(0, "after", 21, vec!["after"]))
            .await?;
        assert!(collection.flush(unix_ms()).await?);
        assert!(collection.pending_mutations.lock().is_empty());
        assert!(collection.storage.stats().check_point >= id);

        drop(collection);
        db.close().await?;
        let db = AndaDB::connect(object_store, config).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(collection.ids(), vec![id]);
        assert_eq!(
            collection.get_extension("before_cancel"),
            Some(Fv::Text("durable".to_string()))
        );
        let reopened: TestDoc = collection.get_as(id).await?;
        assert_eq!(reopened.name, "after");
        assert!(collection.storage.stats().check_point >= id);
        db.close().await?;
        Ok(())
    }

    /// A second writer replacing the metadata object makes the next flush
    /// fail with `Precondition` and poisons the handle: single-writer
    /// violations are never reconciled in place.
    #[tokio::test]
    async fn test_foreign_metadata_writer_poisons_handle() -> Result<(), DBError> {
        let db = AndaDB::connect(
            Arc::new(InMemory::new()),
            DBConfig {
                name: "conflict_db".to_string(),
                description: String::new(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;
        collection
            .add_from(&create_test_doc(0, "one", 20, vec!["one"]))
            .await?;
        collection.flush(unix_ms()).await?;

        // Simulate a second writer bumping the durable metadata object.
        let (mut foreign, _) = collection
            .storage
            .fetch::<CollectionMetadata>(Collection::METADATA_PATH)
            .await?;
        foreign.config.description = "foreign writer".to_string();
        collection
            .storage
            .put(Collection::METADATA_PATH, &foreign, None)
            .await?;

        collection
            .add_from(&create_test_doc(0, "two", 21, vec!["two"]))
            .await?;
        let err = collection
            .flush(unix_ms())
            .await
            .expect_err("stale CAS token must remain a conflict");
        assert!(matches!(err, DBError::Precondition { .. }));
        assert!(collection.is_poisoned());
        Ok(())
    }

    /// An object-store PUT may commit before the caller observes its result.
    /// Cancelling `add` in that interval poisons the handle; the durable WAL
    /// intent makes the reopened generation recover the committed document
    /// instead of skipping it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancelled_add_poisons_handle_and_reopen_recovers_document() -> Result<(), DBError>
    {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
        let blocked = Arc::new(TestAtomicBool::new(false));
        let object_store: Arc<dyn ObjectStore> = Arc::new(FaultPutStore {
            inner: Arc::new(InMemory::new()),
            suffix: "data/1.cbor".to_string(),
            fault: PutFault::BlockAfterCommit {
                gate: gate_rx,
                blocked: blocked.clone(),
            },
        });
        let config = DBConfig {
            name: "test_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["name"]).await
        })
        .await?;

        let adding = {
            let collection = collection.clone();
            tokio::spawn(async move {
                collection
                    .add_from(&create_test_doc(0, "committed", 20, vec!["x"]))
                    .await
            })
        };
        while !blocked.load(TestOrdering::Acquire) {
            tokio::task::yield_now().await;
        }
        adding.abort();
        assert!(
            adding
                .await
                .expect_err("add should be cancelled")
                .is_cancelled()
        );
        gate_tx.send(true).expect("gate receiver dropped");
        assert!(collection.is_poisoned());
        assert!(
            collection
                .add_from(&create_test_doc(0, "second", 21, vec!["y"]))
                .await
                .is_err(),
            "poisoned handle must reject mutations",
        );

        // Reopen through the same database: WAL replay recovers the committed
        // document and the id allocator, so the next add gets a fresh id.
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(
            collection
                .add_from(&create_test_doc(0, "second", 21, vec!["y"]))
                .await?,
            2
        );
        collection.flush(unix_ms()).await?;
        assert_eq!(collection.ids(), vec![1, 2]);
        assert_eq!(
            collection
                .query_ids(
                    Filter::Field((
                        "name".to_string(),
                        RangeQuery::Eq(Fv::Text("committed".to_string())),
                    )),
                    Some(10),
                )
                .await?,
            vec![1]
        );

        drop(collection);
        drop(db);
        let db = AndaDB::connect(object_store, config).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(collection.ids(), vec![1, 2]);
        assert_eq!(collection.get_as::<TestDoc>(1).await?.name, "committed");
        db.close().await?;
        Ok(())
    }

    /// A cancelled update can leave its proposed in-memory index value
    /// applied while the document PUT is still blocked. The handle is
    /// poisoned; the WAL stores both before/after values so the reopened
    /// generation removes that phantom.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancelled_update_poisons_handle_and_reopen_removes_phantom() -> Result<(), DBError>
    {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(true);
        let blocked = Arc::new(TestAtomicBool::new(false));
        let object_store: Arc<dyn ObjectStore> = Arc::new(GatedPutStore {
            inner: Arc::new(InMemory::new()),
            gate_suffix: "data/1.cbor".to_string(),
            gate: gate_rx,
            blocked: blocked.clone(),
        });
        let db = AndaDB::connect(
            object_store,
            DBConfig {
                name: "test_db".to_string(),
                description: String::new(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["name"]).await
        })
        .await?;
        let id = collection
            .add_from(&create_test_doc(0, "before", 20, vec!["x"]))
            .await?;
        collection.flush(unix_ms()).await?;

        gate_tx.send(false).expect("gate receiver dropped");
        let updating = {
            let collection = collection.clone();
            tokio::spawn(async move {
                collection
                    .update(
                        id,
                        BTreeMap::from([("name".to_string(), Fv::Text("cancelled".to_string()))]),
                    )
                    .await
            })
        };
        while !blocked.load(TestOrdering::Acquire) {
            tokio::task::yield_now().await;
        }
        updating.abort();
        assert!(
            updating
                .await
                .expect_err("update should be cancelled")
                .is_cancelled()
        );
        gate_tx.send(true).expect("gate receiver dropped");
        assert!(collection.is_poisoned());
        assert!(
            collection.flush(unix_ms()).await.is_err(),
            "poisoned handle must reject flush",
        );

        // The reopened generation replays the WAL: both historical values are
        // removed and the document still present in storage is re-indexed.
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        collection.flush(unix_ms()).await?;
        let before = collection
            .query_ids(
                Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("before".to_string())),
                )),
                Some(10),
            )
            .await?;
        let cancelled = collection
            .query_ids(
                Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("cancelled".to_string())),
                )),
                Some(10),
            )
            .await?;
        assert_eq!(before, vec![id]);
        assert!(cancelled.is_empty());
        db.close().await?;
        Ok(())
    }

    /// Recovery must wait until the open callback installs custom hooks.
    /// Replaying once with default derivation would persist raw `After`
    /// B-tree/BM25 entries that the custom hook cannot subsequently identify.
    #[tokio::test]
    async fn test_mutation_replay_uses_custom_hooks_before_clearing_intent() -> Result<(), DBError>
    {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = DBConfig {
            name: "custom_replay_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
        let collection = db
            .create_collection(
                TestDoc::schema()?,
                CollectionConfig {
                    name: "documents".to_string(),
                    description: String::new(),
                },
                async |collection| {
                    collection.set_index_hooks(Arc::new(RecoveryCustomHooks));
                    collection.create_btree_index_nx(&["name"]).await?;
                    collection.create_bm25_index_nx(&["name"]).await?;
                    Ok(())
                },
            )
            .await?;
        let id = collection
            .add_from(&create_test_doc(0, "Before", 20, vec!["x"]))
            .await?;
        collection.flush(unix_ms()).await?;
        collection
            .update(
                id,
                BTreeMap::from([("name".to_string(), Fv::Text("After".to_string()))]),
            )
            .await?;
        drop(collection);
        drop(db);

        let db = AndaDB::connect(object_store, config).await?;
        let collection = db
            .open_collection("documents".to_string(), async |collection| {
                collection.set_index_hooks(Arc::new(RecoveryCustomHooks));
                Ok(())
            })
            .await?;
        let raw = collection
            .query_ids(
                Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("After".to_string())),
                )),
                Some(10),
            )
            .await?;
        let hooked = collection
            .query_ids(
                Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("hook:after".to_string())),
                )),
                Some(10),
            )
            .await?;
        assert!(raw.is_empty(), "default-hook B-tree value must not survive");
        assert_eq!(hooked, vec![id]);

        let raw_text = collection
            .get_bm25_index(&["name"])?
            .search("after", 10, None);
        let hooked_text = collection
            .get_bm25_index(&["name"])?
            .search("hooktoken", 10, None);
        assert!(raw_text.iter().all(|(found, _)| *found != id));
        assert!(hooked_text.iter().any(|(found, _)| *found == id));
        db.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_drains_in_flight_add_before_checkpoint() -> Result<(), DBError> {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
        let object_store: Arc<dyn ObjectStore> = Arc::new(GatedPutStore {
            inner: Arc::new(InMemory::new()),
            gate_suffix: "data/2.cbor".to_string(),
            gate: gate_rx,
            blocked: Arc::new(TestAtomicBool::new(false)),
        });
        let db_config = || DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store.clone(), db_config()).await?;
        let collection = create_test_collection(&db, async |_| Ok(())).await?;

        let id1 = collection
            .add_from(&create_test_doc(0, "a", 20, vec!["x"]))
            .await?;
        assert_eq!(id1, 1);
        collection.flush(unix_ms()).await?;

        // Start an add whose document write (data/2.cbor) is blocked,
        // simulating an add still in flight while a flush runs.
        let blocked = {
            let collection = collection.clone();
            tokio::spawn(async move {
                collection
                    .add_from(&create_test_doc(0, "b", 21, vec!["y"]))
                    .await
            })
        };
        // Wait until the blocked add has allocated its id.
        while collection.max_document_id() < 2 {
            tokio::task::yield_now().await;
        }

        // A third add completes fully and bumps the metadata version so the
        // flush below has something to persist.
        let id3 = collection
            .add_from(&create_test_doc(0, "c", 22, vec!["z"]))
            .await?;
        assert_eq!(id3, 3);

        // A complete flush now owns the collection operation gate
        // exclusively. It must wait for add(2), rather than checkpointing a
        // bitmap/index snapshot while that mutation is only half complete.
        let flushing = {
            let collection = collection.clone();
            tokio::spawn(async move { collection.flush(unix_ms()).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!flushing.is_finished(), "flush must drain the active add");

        // Unblock the in-flight add, let the serialized flush checkpoint the
        // complete state, then simulate a crash and reopen.
        gate_tx.send(true).expect("gate receiver dropped");
        let id2 = blocked.await.expect("add task panicked")?;
        assert_eq!(id2, 2);
        assert!(flushing.await.expect("flush task panicked")?);
        assert!(collection.storage.stats().check_point >= 3);
        drop(collection);
        drop(db);

        let db = AndaDB::connect(object_store, db_config()).await?;
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await?;
        assert!(collection.contains(1));
        assert!(
            collection.contains(2),
            "document written by the in-flight add must be recovered by the repair scan"
        );
        assert!(collection.contains(3));

        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_mutation_intents_replay_update_and_remove_after_crash() -> Result<(), DBError> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db_config = || DBConfig {
            name: "mutation_replay_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };

        let db = AndaDB::connect(object_store.clone(), db_config()).await?;
        let collection = db
            .create_collection(
                TestDoc::schema()?,
                CollectionConfig {
                    name: "documents".to_string(),
                    description: String::new(),
                },
                async |c| {
                    c.create_btree_index_nx(&["name"]).await?;
                    c.create_bm25_index_nx(&["name"]).await?;
                    c.create_hnsw_index_nx(
                        "vector",
                        HnswConfig {
                            dimension: 10,
                            ..Default::default()
                        },
                    )
                    .await?;
                    Ok(())
                },
            )
            .await?;

        let mut first = create_test_doc(0, "before", 20, vec!["old"]);
        first.vector = vec![bf16::from_f32(0.0); 10];
        let first_id = collection.add_from(&first).await?;
        let mut second = create_test_doc(0, "other", 21, vec!["other"]);
        second.vector = vec![bf16::from_f32(10.0); 10];
        let second_id = collection.add_from(&second).await?;
        collection.flush(unix_ms()).await?;

        // The document PUT succeeds, but no index flush follows before the
        // process disappears. The retained intent must make reopen converge
        // every index to the new document rather than the old checkpoint.
        collection
            .update(
                first_id,
                BTreeMap::from([
                    ("name".to_string(), Fv::Text("middle".to_string())),
                    (
                        "vector".to_string(),
                        Fv::Vector(vec![bf16::from_f32(15.0); 10]),
                    ),
                ]),
            )
            .await?;
        collection
            .update(
                first_id,
                BTreeMap::from([
                    ("name".to_string(), Fv::Text("after".to_string())),
                    (
                        "vector".to_string(),
                        Fv::Vector(vec![bf16::from_f32(20.0); 10]),
                    ),
                ]),
            )
            .await?;
        assert!(!collection.pending_mutations.lock().is_empty());
        drop(collection);
        drop(db);

        let db = AndaDB::connect(object_store.clone(), db_config()).await?;
        let collection = db
            .open_collection("documents".to_string(), async |_| Ok(()))
            .await?;

        let by_name = |name: &str| Query {
            filter: Some(Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text(name.to_string())),
            ))),
            ..Default::default()
        };
        assert!(collection.search_ids(by_name("before")).await?.is_empty());
        assert!(collection.search_ids(by_name("middle")).await?.is_empty());
        assert_eq!(
            collection.search_ids(by_name("after")).await?,
            vec![first_id]
        );

        let old_text = collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("before".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;
        assert!(!old_text.contains(&first_id));
        let new_text = collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("after".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;
        assert!(new_text.contains(&first_id));

        let hnsw = collection.get_hnsw_index("vector")?;
        assert_eq!(hnsw.try_search(&[0.0; 10], 1)?[0].0, second_id);
        assert_eq!(hnsw.try_search(&[20.0; 10], 1)?[0].0, first_id);
        assert!(collection.pending_mutations.lock().is_empty());

        // Exercise the other terminal state: a remove whose object delete is
        // durable while the derived indexes are not yet flushed.
        collection.remove(first_id).await?;
        assert!(!collection.pending_mutations.lock().is_empty());
        drop(collection);
        drop(db);

        let db = AndaDB::connect(object_store, db_config()).await?;
        let collection = db
            .open_collection("documents".to_string(), async |_| Ok(()))
            .await?;
        assert!(!collection.contains(first_id));
        assert!(collection.search_ids(by_name("after")).await?.is_empty());
        let removed_text = collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("after".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;
        assert!(!removed_text.contains(&first_id));
        assert!(
            !collection
                .get_hnsw_index("vector")?
                .try_search(&[20.0; 10], 2)?
                .iter()
                .any(|(id, _)| *id == first_id)
        );
        assert!(collection.pending_mutations.lock().is_empty());

        db.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_close_drains_update_and_old_handle_cannot_reenable() -> Result<(), DBError> {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(true);
        let blocked = Arc::new(TestAtomicBool::new(false));
        let object_store: Arc<dyn ObjectStore> = Arc::new(GatedPutStore {
            inner: Arc::new(InMemory::new()),
            gate_suffix: "data/1.cbor".to_string(),
            gate: gate_rx,
            blocked: blocked.clone(),
        });
        let config = DBConfig {
            name: "close_drain_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::connect(object_store, config).await?;
        let old = db
            .create_collection(
                TestDoc::schema()?,
                CollectionConfig {
                    name: "documents".to_string(),
                    description: String::new(),
                },
                async |_| Ok(()),
            )
            .await?;

        let id = old
            .add_from(&create_test_doc(0, "before", 20, vec!["x"]))
            .await?;
        gate_tx.send(false).expect("gate receiver dropped");
        let updating = {
            let collection = old.clone();
            tokio::spawn(async move {
                collection
                    .update(
                        id,
                        BTreeMap::from([("name".to_string(), Fv::Text("after".to_string()))]),
                    )
                    .await
            })
        };
        while !blocked.load(TestOrdering::Acquire) {
            tokio::task::yield_now().await;
        }

        let closing = {
            let db = db.clone();
            tokio::spawn(async move { db.close_collection("documents").await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !closing.is_finished(),
            "close must wait for admitted update"
        );

        // Cancelling close must not create an empty registry slot. Opening the
        // same name takes over the retiring handle and remains blocked until
        // its admitted update is drained and flushed.
        closing.abort();
        assert!(
            closing
                .await
                .expect_err("close should be cancelled")
                .is_cancelled()
        );
        let opening = {
            let db = db.clone();
            tokio::spawn(async move {
                db.open_or_create_collection(
                    TestDoc::schema()?,
                    CollectionConfig {
                        name: "documents".to_string(),
                        description: String::new(),
                    },
                    async |_| Ok(()),
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !opening.is_finished(),
            "open must finish the cancelled close before loading a fresh handle"
        );

        gate_tx.send(true).expect("gate receiver dropped");
        let updated = updating.await.expect("update task panicked")?;
        assert_eq!(updated.get_field("name"), Some(&Fv::Text("after".into())));
        let fresh = opening.await.expect("open task panicked")?;
        assert_eq!(fresh.len(), 1);
        let persisted: TestDoc = fresh.get_as(id).await?;
        assert_eq!(persisted.name, "after");
        assert!(!Arc::ptr_eq(&old, &fresh));

        // The user-controlled read-only flag is reversible only while the
        // handle's lifecycle lease is active. The retired Arc must never
        // become a second writer over the same prefix.
        old.set_read_only(false);
        assert!(
            old.add_from(&create_test_doc(0, "zombie", 21, vec!["z"]))
                .await
                .is_err()
        );
        assert!(
            old.save_extension("zombie".to_string(), Fv::Bool(true))
                .await
                .is_err()
        );
        old.set_extension("zombie".to_string(), Fv::Bool(true));
        assert!(old.get_extension("zombie").is_none());
        assert!(old.flush(unix_ms()).await.is_err());

        db.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_delete_drains_add_before_prefix_removal() -> Result<(), DBError> {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
        let object_store: Arc<dyn ObjectStore> = Arc::new(GatedPutStore {
            inner: Arc::new(InMemory::new()),
            gate_suffix: "data/1.cbor".to_string(),
            gate: gate_rx,
            blocked: Arc::new(TestAtomicBool::new(false)),
        });
        let db = AndaDB::connect(
            object_store,
            DBConfig {
                name: "delete_drain_db".to_string(),
                description: String::new(),
                storage: StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
                lock: None,
            },
        )
        .await?;
        let old = db
            .create_collection(
                TestDoc::schema()?,
                CollectionConfig {
                    name: "documents".to_string(),
                    description: String::new(),
                },
                async |_| Ok(()),
            )
            .await?;
        let adding = {
            let collection = old.clone();
            tokio::spawn(async move {
                collection
                    .add_from(&create_test_doc(0, "deleted", 20, vec!["x"]))
                    .await
            })
        };
        while old.max_document_id() == 0 {
            tokio::task::yield_now().await;
        }

        let deleting = {
            let db = db.clone();
            tokio::spawn(async move { db.delete_collection("documents").await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !deleting.is_finished(),
            "delete must drain the active add before listing the prefix"
        );
        deleting.abort();
        assert!(
            deleting
                .await
                .expect_err("delete should be cancelled")
                .is_cancelled()
        );
        assert!(
            db.open_collection("documents".to_string(), async |_| Ok(()))
                .await
                .is_err(),
            "the deletion tombstone must block open after cancellation"
        );
        let deleting = {
            let db = db.clone();
            tokio::spawn(async move { db.delete_collection("documents").await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !deleting.is_finished(),
            "retry must take over and continue draining the retained handle"
        );
        gate_tx.send(true).expect("gate receiver dropped");
        adding.await.expect("add task panicked")?;
        deleting.await.expect("delete task panicked")?;

        let fresh = db
            .create_collection(
                TestDoc::schema()?,
                CollectionConfig {
                    name: "documents".to_string(),
                    description: String::new(),
                },
                async |_| Ok(()),
            )
            .await?;
        assert!(
            fresh.is_empty(),
            "deleted add must leave no residual object"
        );
        old.set_read_only(false);
        assert!(
            old.add_from(&create_test_doc(0, "zombie", 21, vec!["z"]))
                .await
                .is_err()
        );

        db.close().await?;
        Ok(())
    }
}
