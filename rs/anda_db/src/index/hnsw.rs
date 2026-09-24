use anda_db_hnsw::{FlushOptions, FlushOutcome, HnswError, HnswIndex};
use bytes::Bytes;
use futures::StreamExt;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use std::{
    fmt::Debug,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub use anda_db_hnsw::{HnswConfig, HnswMetadata, HnswStats};

use crate::{
    error::DBError,
    schema::{BoxError, Fe, Vector},
    storage::{ObjectVersion, PutMode, Storage},
};

/// Collection-level wrapper around an HNSW vector index.
///
/// The wrapper owns persistence paths and object versions for index metadata,
/// id lists, and graph nodes while delegating search behavior to
/// `anda_db_hnsw::HnswIndex`.
///
/// Per-node, metadata and IDs CAS tokens are the last defense against a second
/// writer, which the single-writer deployment contract forbids. A `Precondition`
/// conflict (or a cancelled flush) is never reconciled in place: the error
/// propagates, the collection poisons its handle and reopening rebuilds this
/// wrapper from the durable objects.
pub struct Hnsw {
    name: String,
    index: HnswIndex,
    storage: Storage, // shared with the owning collection
    metadata_version: RwLock<ObjectVersion>,
    ids_version: RwLock<ObjectVersion>,
    node_versions: Arc<RwLock<FxHashMap<u64, ObjectVersion>>>,
    /// A read-only bootstrap defers its storage-mutating orphan sweep until
    /// the first writable flush.
    orphan_cleanup_pending: AtomicBool,
}

impl Debug for Hnsw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HnswIndex({})", self.name)
    }
}

impl PartialEq for &Hnsw {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for &Hnsw {}
impl Hash for &Hnsw {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl Hnsw {
    /// Exact scoring of a bounded external candidate set. This does not walk
    /// the graph, so it does not increment graph-search statistics.
    pub(crate) fn search_in_ids(
        &self,
        query: &[f32],
        top_k: usize,
        ids: &[u64],
    ) -> Result<Vec<(u64, f32)>, DBError> {
        Ok(self.index.search_f32_in_ids(query, top_k, ids)?)
    }

    pub(crate) fn dir_path(name: &str) -> String {
        format!("hnsw_indexes/{name}/")
    }

    fn metadata_path(name: &str) -> String {
        format!("hnsw_indexes/{name}/meta.cbor")
    }

    fn ids_path(name: &str) -> String {
        format!("hnsw_indexes/{name}/ids.cbor")
    }

    fn node_path(name: &str, node: u64) -> String {
        format!("hnsw_indexes/{name}/n_{node}.cbor")
    }

    /// Creates a new persisted HNSW index for `field`.
    pub async fn new(
        field: &Fe,
        config: HnswConfig,
        storage: Storage,
        now_ms: u64,
    ) -> Result<Self, DBError> {
        let name = field.name().to_string();
        // Collection metadata has not published this index yet, so any
        // objects under its path belong to an interrupted earlier creation.
        // Remove them before node writes begin using create-only CAS.
        storage.drop_prefix(&Hnsw::dir_path(&name)).await?;
        let index = HnswIndex::try_new(name.clone(), Some(config))?;
        let mut metadata = Vec::new();
        let mut ids = Vec::new();
        index
            .flush(&mut metadata, &mut ids, now_ms, async |_, _| Ok(true))
            .await?;
        // Publish the id set before the metadata commit record, matching the
        // steady-state crash contract used by `flush` below.
        let ids_version = storage
            .put_internal_bytes(&Hnsw::ids_path(&name), ids.into(), PutMode::Create)
            .await?;
        let metadata_version = storage
            .put_internal_bytes(
                &Hnsw::metadata_path(&name),
                metadata.into(),
                PutMode::Create,
            )
            .await?;
        Ok(Self {
            name,
            index,
            storage,
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
            node_versions: Arc::new(RwLock::new(FxHashMap::default())),
            orphan_cleanup_pending: AtomicBool::new(false),
        })
    }

    pub(crate) async fn drop_data(&self) {
        // Delete the metadata, ids and all node objects under the index directory.
        if let Err(err) = self.storage.drop_prefix(&Hnsw::dir_path(&self.name)).await {
            log::warn!(
                action = "Hnsw::drop_data",
                index = self.name;
                "Failed to drop HNSW index data: {err:?}",
            );
        }
    }

    /// Loads an existing HNSW index from metadata, id list, and node objects.
    pub async fn bootstrap(name: String, storage: Storage) -> Result<Self, DBError> {
        Self::bootstrap_with_cleanup(name, storage, true).await
    }

    pub(crate) async fn bootstrap_with_cleanup(
        name: String,
        storage: Storage,
        cleanup: bool,
    ) -> Result<Self, DBError> {
        let (metadata, metadata_version) = storage
            .fetch_internal_bytes(&Hnsw::metadata_path(&name))
            .await?;
        let (ids, ids_version) = storage.fetch_internal_bytes(&Hnsw::ids_path(&name)).await?;
        let node_storage = storage.clone();
        let node_name = name.clone();
        let node_versions = Arc::new(RwLock::new(FxHashMap::default()));
        let loaded_node_versions = node_versions.clone();
        let index = HnswIndex::load_all(&metadata[..], &ids[..], async move |id: u64| {
            // Owned copies across the await; see `BTree::bootstrap`.
            let path = Hnsw::node_path(&node_name, id);
            let storage = node_storage.clone();
            match storage.fetch_internal_bytes(&path).await {
                Ok((data, version)) => {
                    loaded_node_versions.write().insert(id, version);
                    Ok(Some(data.into()))
                }
                Err(DBError::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await?;

        let this = Self {
            name,
            index,
            storage,
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
            node_versions,
            orphan_cleanup_pending: AtomicBool::new(!cleanup),
        };
        // Loading tombstone versions is read-only and is required if this
        // handle later becomes writable: a reused id must update its existing
        // node blob with the observed CAS token instead of attempting Create.
        this.load_tombstone_versions().await?;
        if cleanup {
            let (complete, _) = this.purge_orphan_node_blobs().await;
            this.orphan_cleanup_pending
                .store(!complete, Ordering::Release);
        }
        Ok(this)
    }

    /// Captures the CAS tokens for committed tombstone blobs. Live-node tokens
    /// are collected by the load callback above. A missing tombstone blob is
    /// valid (a previous purge may already have deleted it).
    async fn load_tombstone_versions(&self) -> Result<(), DBError> {
        let mut stream = futures::stream::iter(self.index.removed_node_ids())
            .map(|id| async move {
                let path = Hnsw::node_path(&self.name, id);
                match self.storage.fetch_internal_bytes(&path).await {
                    Ok((_, version)) => {
                        self.node_versions.write().insert(id, version);
                        Ok(())
                    }
                    Err(DBError::NotFound { .. }) => Ok(()),
                    Err(error) => Err(error),
                }
            })
            .buffer_unordered(16);
        while let Some(result) = stream.next().await {
            result?;
        }
        Ok(())
    }

    /// Best-effort deletion of node blobs that neither the committed id set
    /// nor the tombstone set references.
    ///
    /// A crash between the ids PUT (which already excluded a removed node)
    /// and the metadata PUT (which would have carried its tombstone) leaves
    /// a blob that no later load or purge would ever visit — a permanent
    /// space leak, and vector data lingering longer than intended. Bootstrap
    /// already pays O(nodes) to fetch every referenced blob, so one listing
    /// of the index directory to sweep unreferenced ones is proportional.
    async fn purge_orphan_node_blobs(&self) -> (bool, bool) {
        let referenced: std::collections::BTreeSet<u64> = self
            .index
            .node_ids()
            .into_iter()
            .chain(self.index.removed_node_ids())
            .collect();

        let dir = Hnsw::dir_path(&self.name);
        let mut stream = self.storage.list_meta(Some(&dir), None);
        let mut orphans: Vec<u64> = Vec::new();
        while let Some(meta) = stream.next().await {
            let Ok(meta) = meta else {
                // Listing failures must not fail bootstrap; the sweep is
                // retained as pending and retried on a writable flush.
                return (false, false);
            };
            if let Some(id) = meta
                .location
                .filename()
                .and_then(|f| f.strip_prefix("n_"))
                .and_then(|f| f.strip_suffix(".cbor"))
                .and_then(|id| id.parse::<u64>().ok())
                && !referenced.contains(&id)
            {
                orphans.push(id);
            }
        }

        let mut complete = true;
        let mut deleted = false;
        for id in orphans {
            let path = Hnsw::node_path(&self.name, id);
            match self.storage.delete(&path).await {
                Ok(()) => {
                    deleted = true;
                    self.node_versions.write().remove(&id);
                    log::warn!(
                        action = "Hnsw::purge_orphan_node_blobs",
                        index = self.name,
                        node_id = id;
                        "Deleted orphan HNSW node blob left by a crash",
                    );
                }
                Err(DBError::NotFound { .. }) => {
                    self.node_versions.write().remove(&id);
                }
                Err(err) => {
                    complete = false;
                    log::warn!(
                        action = "Hnsw::purge_orphan_node_blobs",
                        index = self.name,
                        node_id = id;
                        "Failed to delete orphan HNSW node blob: {err:?}",
                    );
                }
            }
        }
        (complete, deleted)
    }

    /// Persists a fixed-key node with an object-store precondition. `Create`
    /// protects the first publication and `Update` protects every replacement.
    /// If the backend committed but the result was lost, the local token stays
    /// stale and a retry conflicts instead of overwriting newer durable bytes.
    async fn persist_node(&self, id: u64, data: Vec<u8>) -> Result<bool, BoxError> {
        let mode = self
            .node_versions
            .read()
            .get(&id)
            .cloned()
            .map(|version| PutMode::Update(version.into()))
            .unwrap_or(PutMode::Create);
        let version = self
            .storage
            .put_internal_bytes(&Hnsw::node_path(&self.name, id), Bytes::from(data), mode)
            .await?;
        self.node_versions.write().insert(id, version);
        Ok(true)
    }

    /// Persists one coherent graph snapshot, then deletes removed-node blobs.
    ///
    /// Node uploads are bounded to eight in flight and use per-object CAS,
    /// followed by conditional IDs and metadata writes. This provides
    /// recoverable partial progress, not a multi-object transaction: generation
    /// markers let bootstrap recover a mixed graph, and Collection replays
    /// authoritative document intents.
    /// The owning Collection serializes persistence and excludes mutations.
    ///
    /// Returns `true` when any object was written or deleted.
    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let orphans_deleted = if self.orphan_cleanup_pending.load(Ordering::Acquire) {
            let (complete, deleted) = self.purge_orphan_node_blobs().await;
            if complete {
                self.orphan_cleanup_pending.store(false, Ordering::Release);
            }
            deleted
        } else {
            false
        };
        let had_removed = self.index.has_removed_nodes();
        let ids_path = Hnsw::ids_path(&self.name);
        let metadata_path = Hnsw::metadata_path(&self.name);
        // The ids and metadata objects are each one conditional PUT: the
        // remaining second-writer defense. A `Precondition` conflict is not
        // reconciled in place — it propagates, the collection poisons its
        // handle and recovery happens on reopen.
        let saved = self
            .index
            .flush_with_options(
                now_ms,
                FlushOptions {
                    node_concurrency: 8,
                    ..Default::default()
                },
                |id, data| self.persist_node(id, data),
                |data| {
                    super::persistence::commit_metadata(
                        &self.storage,
                        &ids_path,
                        &self.ids_version,
                        data,
                    )
                },
                |data| {
                    super::persistence::commit_metadata(
                        &self.storage,
                        &metadata_path,
                        &self.metadata_version,
                        data,
                    )
                },
            )
            .await?;

        let saved = match saved {
            FlushOutcome::Committed => true,
            FlushOutcome::NoChanges => false,
            FlushOutcome::Stopped => {
                return Err(HnswError::Generic {
                    name: self.name.clone(),
                    source: "HNSW flush stopped before commit".into(),
                }
                .into());
            }
        };

        // Delete the persisted blobs of removed nodes; without this they
        // would leak forever. "Not found" is success (already deleted).
        let storage = self.storage.clone();
        let name = self.name.clone();
        let versions = self.node_versions.clone();
        self.index
            .purge_removed_nodes(async move |id| {
                let path = Hnsw::node_path(&name, id);
                let storage = storage.clone();
                match storage.delete(&path).await {
                    Ok(()) | Err(DBError::NotFound { .. }) => {
                        versions.write().remove(&id);
                        Ok(true)
                    }
                    Err(err) => Err(err.into()),
                }
            })
            .await?;

        Ok(orphans_deleted || saved || had_removed)
    }

    /// Returns whether metadata, nodes, or removed-node tombstones have
    /// in-memory changes to flush.
    pub fn has_pending_flush(&self) -> bool {
        if self.orphan_cleanup_pending.load(Ordering::Acquire)
            || self.index.has_dirty_nodes()
            || self.index.has_removed_nodes()
        {
            return true;
        }

        self.index.has_pending_metadata_flush()
    }

    /// Returns the stable index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field indexed by this HNSW index.
    pub fn field_name(&self) -> &str {
        &self.name
    }

    /// Returns the vector dimension this index was configured with.
    pub fn dimension(&self) -> usize {
        self.index.dimension()
    }

    /// Returns a snapshot of HNSW runtime statistics.
    pub fn stats(&self) -> HnswStats {
        self.index.stats()
    }

    /// Returns a snapshot of HNSW metadata.
    pub fn metadata(&self) -> HnswMetadata {
        self.index.metadata()
    }

    /// Inserts or updates the vector for `id`.
    pub fn insert(&self, id: u64, vector: Vector, now_ms: u64) -> Result<(), DBError> {
        self.index.insert(id, vector, now_ms)?;
        Ok(())
    }

    /// Removes the vector for `id` if present.
    pub fn remove(&self, id: u64, now_ms: u64) -> bool {
        self.index.remove(id, now_ms)
    }

    /// Searches for the nearest vectors and returns `(document_id, distance)` pairs.
    pub fn try_search(&self, query: &[f32], top_k: usize) -> Result<Vec<(u64, f32)>, DBError> {
        self.index.search_f32(query, top_k).map_err(DBError::from)
    }

    /// Searches for nearest vectors, returning an empty result on search errors.
    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<(u64, f32)> {
        self.try_search(query, top_k).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{Ft, bf16},
        storage::StorageConfig,
    };
    use anda_object_store::{FaultHandle, FaultKind, FaultOp, FaultOutcome, FaultRule, FaultStore};
    use object_store::memory::InMemory;

    /// Rejects the next PUT whose path contains `path` before it reaches the
    /// store.
    fn fail_next_put(faults: &FaultHandle, path: &str) {
        faults.push_rule(FaultRule::fail_once(FaultOp::Put, path));
    }

    /// Lets the next matching PUT become durable, then reports an error:
    /// a crash immediately after that atomic PUT.
    fn crash_after_next_put(faults: &FaultHandle, path: &str) {
        faults.push_rule(FaultRule {
            kind: FaultKind::ErrorAfter,
            ..FaultRule::fail_once(FaultOp::Put, path)
        });
    }

    /// Every attempted PUT since the last reset, rejected ones included.
    fn put_paths(faults: &FaultHandle) -> Vec<String> {
        faults
            .event_log()
            .into_iter()
            .filter(|e| e.op == FaultOp::Put && e.outcome == FaultOutcome::Attempted)
            .map(|e| e.path)
            .collect()
    }

    async fn fault_index() -> (Hnsw, Storage, FaultHandle) {
        let (object_store, faults) = FaultStore::wrap(InMemory::new());
        let storage = Storage::connect(
            "hnsw_fault_tests".to_string(),
            Arc::new(object_store),
            StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let field = Fe::new("embedding".to_string(), Ft::Vector).unwrap();
        let index = Hnsw::new(
            &field,
            HnswConfig {
                dimension: 2,
                ..Default::default()
            },
            storage.clone(),
            1,
        )
        .await
        .unwrap();
        faults.reset();
        index
            .insert(1, vec![bf16::from_f32(1.0), bf16::from_f32(1.0)], 2)
            .unwrap();
        (index, storage, faults)
    }

    /// A crash between the ids PUT (node already excluded) and the metadata
    /// PUT (tombstone never persisted) leaves an unreferenced node blob that
    /// no load or purge would ever visit. Bootstrap must sweep it while
    /// keeping every referenced blob.
    #[tokio::test]
    async fn bootstrap_sweeps_orphan_node_blobs() {
        let (index, storage, _faults) = fault_index().await;
        assert!(index.flush(3).await.unwrap());

        // Simulate the crash leftover: a blob at an id that neither the ids
        // set nor the tombstone set references.
        storage
            .put_bytes(
                &Hnsw::node_path("embedding", 99),
                Bytes::from_static(b"orphan"),
                PutMode::Overwrite,
            )
            .await
            .unwrap();

        let reopened = Hnsw::bootstrap("embedding".to_string(), storage.clone())
            .await
            .unwrap();
        assert_eq!(reopened.search(&[1.0, 1.0], 1), vec![(1, 0.0)]);
        assert!(matches!(
            storage.fetch_bytes(&Hnsw::node_path("embedding", 99)).await,
            Err(DBError::NotFound { .. })
        ));
        // The referenced blob survives the sweep.
        assert!(
            storage
                .fetch_bytes(&Hnsw::node_path("embedding", 1))
                .await
                .is_ok()
        );
    }

    async fn assert_retry_recovers(index: &Hnsw, storage: &Storage) {
        assert!(index.has_pending_flush());
        assert!(index.flush(4).await.unwrap());
        assert!(!index.has_pending_flush());
        let reloaded = Hnsw::bootstrap("embedding".to_string(), storage.clone())
            .await
            .unwrap();
        assert_eq!(reloaded.search(&[1.0, 1.0], 1), vec![(1, 0.0)]);
    }

    /// Models "PUT committed, then crash/cancellation before the result was
    /// observed" at the given boundary, asserts the exact PUT ordering, and
    /// verifies what a bootstrap from the durable image alone can see.
    /// Returns the stale writer and storage for per-boundary follow-ups.
    async fn assert_crash_after_put(
        suffix: &str,
        expected_puts: &[&str],
        visible: bool,
    ) -> (Hnsw, Storage) {
        let (index, storage, faults) = fault_index().await;
        crash_after_next_put(&faults, suffix);
        assert!(index.flush(3).await.is_err());

        let puts = put_paths(&faults);
        assert_eq!(puts.len(), expected_puts.len());
        for (actual, expected) in puts.iter().zip(expected_puts) {
            assert!(actual.ends_with(expected), "unexpected PUT path: {actual}");
        }

        // Drop the writer state conceptually and bootstrap only from the
        // durable object-store image left at the selected crash boundary.
        let reopened = Hnsw::bootstrap("embedding".to_string(), storage.clone())
            .await
            .unwrap();
        assert_eq!(!reopened.search(&[1.0, 1.0], 1).is_empty(), visible);
        (index, storage)
    }

    #[tokio::test]
    async fn node_put_failure_does_not_publish_ids_or_metadata() {
        let (index, storage, faults) = fault_index().await;
        let old_ids = storage
            .fetch_bytes(&Hnsw::ids_path("embedding"))
            .await
            .unwrap()
            .0;
        let old_metadata = storage
            .fetch_bytes(&Hnsw::metadata_path("embedding"))
            .await
            .unwrap()
            .0;

        fail_next_put(&faults, "n_1.cbor");
        assert!(index.flush(3).await.is_err());
        assert_eq!(put_paths(&faults).len(), 1);
        assert!(put_paths(&faults)[0].ends_with("n_1.cbor"));
        assert_eq!(
            storage
                .fetch_bytes(&Hnsw::ids_path("embedding"))
                .await
                .unwrap()
                .0,
            old_ids
        );
        assert_eq!(
            storage
                .fetch_bytes(&Hnsw::metadata_path("embedding"))
                .await
                .unwrap()
                .0,
            old_metadata
        );
        let crashed = Hnsw::bootstrap("embedding".to_string(), storage.clone())
            .await
            .unwrap();
        assert!(crashed.search(&[1.0, 1.0], 1).is_empty());

        assert_retry_recovers(&index, &storage).await;
    }

    #[tokio::test]
    async fn ids_put_failure_leaves_metadata_at_previous_commit() {
        let (index, storage, faults) = fault_index().await;
        let old_ids = storage
            .fetch_bytes(&Hnsw::ids_path("embedding"))
            .await
            .unwrap()
            .0;
        let old_metadata = storage
            .fetch_bytes(&Hnsw::metadata_path("embedding"))
            .await
            .unwrap()
            .0;

        fail_next_put(&faults, "ids.cbor");
        assert!(index.flush(3).await.is_err());
        let puts = put_paths(&faults);
        assert_eq!(puts.len(), 2);
        assert!(puts[0].ends_with("n_1.cbor"));
        assert!(puts[1].ends_with("ids.cbor"));
        assert_eq!(
            storage
                .fetch_bytes(&Hnsw::ids_path("embedding"))
                .await
                .unwrap()
                .0,
            old_ids
        );
        assert_eq!(
            storage
                .fetch_bytes(&Hnsw::metadata_path("embedding"))
                .await
                .unwrap()
                .0,
            old_metadata
        );
        // Retry on the same writer before opening another wrapper: the node
        // PUT succeeded and its CAS token was observed, while IDs did not.
        assert_retry_recovers(&index, &storage).await;
    }

    #[tokio::test]
    async fn metadata_put_failure_is_last_and_retryable() {
        let (index, storage, faults) = fault_index().await;
        let old_metadata = storage
            .fetch_bytes(&Hnsw::metadata_path("embedding"))
            .await
            .unwrap()
            .0;

        fail_next_put(&faults, "meta.cbor");
        assert!(index.flush(3).await.is_err());
        let puts = put_paths(&faults);
        assert_eq!(puts.len(), 3);
        assert!(puts[0].ends_with("n_1.cbor"));
        assert!(puts[1].ends_with("ids.cbor"));
        assert!(puts[2].ends_with("meta.cbor"));
        assert_eq!(
            storage
                .fetch_bytes(&Hnsw::metadata_path("embedding"))
                .await
                .unwrap()
                .0,
            old_metadata
        );

        // Nodes and ids are already durable. Loading with the previous empty
        // metadata self-repairs its stale entry point instead of pruning the
        // live node, and the original writer can still retry the metadata CAS.
        let crashed = Hnsw::bootstrap("embedding".to_string(), storage.clone())
            .await
            .unwrap();
        assert_eq!(crashed.search(&[1.0, 1.0], 1), vec![(1, 0.0)]);

        assert_retry_recovers(&index, &storage).await;
    }

    #[tokio::test]
    async fn crash_after_node_put_reopens_previous_commit() {
        let (index, storage, faults) = fault_index().await;
        crash_after_next_put(&faults, "n_1.cbor");
        assert!(index.flush(3).await.is_err());
        assert_eq!(put_paths(&faults).len(), 1);
        assert!(put_paths(&faults)[0].ends_with("n_1.cbor"));

        // The create became durable but its result was lost. The writer did
        // not learn a token, so retry must conflict instead of overwriting it.
        assert!(index.flush(4).await.is_err());
        let recovered = Hnsw::bootstrap("embedding".to_string(), storage.clone())
            .await
            .unwrap();
        assert!(recovered.search(&[1.0, 1.0], 1).is_empty());
        assert!(matches!(
            storage.fetch_bytes(&Hnsw::node_path("embedding", 1)).await,
            Err(DBError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn crash_after_node_update_cannot_be_overwritten_by_a_stale_retry() {
        let (index, storage, faults) = fault_index().await;
        assert!(index.flush(3).await.unwrap());
        faults.reset();
        assert!(index.remove(1, 4));
        index
            .insert(1, vec![bf16::from_f32(9.0), bf16::from_f32(9.0)], 5)
            .unwrap();

        crash_after_next_put(&faults, "n_1.cbor");
        assert!(index.flush(6).await.is_err());
        assert!(
            index.flush(7).await.is_err(),
            "the stale node token must conflict"
        );

        let recovered = Hnsw::bootstrap("embedding".to_string(), storage)
            .await
            .unwrap();
        assert_eq!(recovered.search(&[9.0, 9.0], 1), vec![(1, 0.0)]);
    }

    #[tokio::test]
    async fn crash_after_ids_put_fails_stale_writer_and_reopen_recovers() {
        let (index, storage) =
            assert_crash_after_put("ids.cbor", &["n_1.cbor", "ids.cbor"], true).await;
        // The committed-but-unobserved conditional ids PUT left this writer's
        // CAS token stale. In-place retry is not supported: the conflict
        // propagates (the owning collection poisons its handle) and a reopen
        // recovers from the durable objects.
        index
            .insert(2, vec![bf16::from_f32(2.0), bf16::from_f32(2.0)], 4)
            .unwrap();
        assert!(
            index.flush(4).await.is_err(),
            "stale CAS token must remain a conflict",
        );
        let reopened = Hnsw::bootstrap("embedding".to_string(), storage)
            .await
            .unwrap();
        assert_eq!(reopened.search(&[1.0, 1.0], 1), vec![(1, 0.0)]);
    }

    #[tokio::test]
    async fn crash_after_metadata_put_fails_stale_writer_and_reopen_recovers() {
        let (index, storage) =
            assert_crash_after_put("meta.cbor", &["n_1.cbor", "ids.cbor", "meta.cbor"], true).await;
        // Same contract as the ids boundary: the stale metadata token is a
        // hard conflict, and the durable image is already fully committed.
        index
            .insert(2, vec![bf16::from_f32(2.0), bf16::from_f32(2.0)], 4)
            .unwrap();
        assert!(
            index.flush(4).await.is_err(),
            "stale CAS token must remain a conflict",
        );
        let reopened = Hnsw::bootstrap("embedding".to_string(), storage)
            .await
            .unwrap();
        assert_eq!(reopened.search(&[1.0, 1.0], 1), vec![(1, 0.0)]);
    }
}
