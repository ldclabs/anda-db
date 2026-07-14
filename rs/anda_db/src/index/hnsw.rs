use anda_db_hnsw::HnswIndex;
use bytes::Bytes;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use std::{fmt::Debug, hash::Hash, sync::Arc};

pub use anda_db_hnsw::{HnswConfig, HnswMetadata, HnswStats};

use crate::{
    error::DBError,
    schema::{BoxError, Fe, Vector},
    storage::{ObjectVersion, PutMode, Storage},
};

#[derive(Clone)]
struct PendingVersionedWrite {
    payload: Vec<u8>,
    expected_version: ObjectVersion,
}

#[derive(Clone, Copy)]
enum PayloadMatch {
    Exact,
    Metadata,
}

/// Collection-level wrapper around an HNSW vector index.
///
/// The wrapper owns persistence paths and object versions for index metadata,
/// id lists, and graph nodes while delegating search behavior to
/// `anda_db_hnsw::HnswIndex`.
pub struct Hnsw {
    name: String,
    index: HnswIndex,
    storage: Storage, // 与 Collection 共享同一个 Storage 实例
    metadata_version: Arc<RwLock<ObjectVersion>>,
    ids_version: Arc<RwLock<ObjectVersion>>,
    pending_metadata_write: Arc<ParkingMutex<Option<PendingVersionedWrite>>>,
    pending_ids_write: Arc<ParkingMutex<Option<PendingVersionedWrite>>>,
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
        let index = HnswIndex::try_new(name.clone(), Some(config))?;
        let mut metadata = Vec::new();
        let mut ids = Vec::new();
        index
            .flush(&mut metadata, &mut ids, now_ms, async |_, _| Ok(true))
            .await?;
        // The collection metadata is the source of truth for which indexes
        // exist, so overwrite any leftover files from a crashed creation or a
        // previously removed index instead of failing with AlreadyExists.
        // Publish the id set before the metadata commit record, matching the
        // steady-state crash contract used by `flush` below.
        let ids_version = storage
            .put_bytes(&Hnsw::ids_path(&name), ids.into(), PutMode::Overwrite)
            .await?;
        let metadata_version = storage
            .put_bytes(
                &Hnsw::metadata_path(&name),
                metadata.into(),
                PutMode::Overwrite,
            )
            .await?;
        Ok(Self {
            name,
            index,
            storage,
            metadata_version: Arc::new(RwLock::new(metadata_version)),
            ids_version: Arc::new(RwLock::new(ids_version)),
            pending_metadata_write: Arc::new(ParkingMutex::new(None)),
            pending_ids_write: Arc::new(ParkingMutex::new(None)),
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
        let (metadata, metadata_version) = storage.fetch_bytes(&Hnsw::metadata_path(&name)).await?;
        let (ids, ids_version) = storage.fetch_bytes(&Hnsw::ids_path(&name)).await?;
        let n = Arc::new(name.clone());
        let s = Arc::new(storage.clone());
        let index = HnswIndex::load_all(&metadata[..], &ids[..], async move |id: u64| {
            let path = Hnsw::node_path(n.clone().as_str(), id);
            match s.clone().fetch_bytes(&path).await {
                Ok((data, _)) => Ok(Some(data.into())),
                Err(DBError::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await?;

        Ok(Self {
            name,
            index,
            storage,
            metadata_version: Arc::new(RwLock::new(metadata_version)),
            ids_version: Arc::new(RwLock::new(ids_version)),
            pending_metadata_write: Arc::new(ParkingMutex::new(None)),
            pending_ids_write: Arc::new(ParkingMutex::new(None)),
        })
    }

    fn payloads_match(kind: PayloadMatch, left: &[u8], right: &[u8]) -> Result<bool, BoxError> {
        match kind {
            PayloadMatch::Exact => Ok(left == right),
            PayloadMatch::Metadata => {
                Ok(HnswIndex::metadata_payloads_logically_equal(left, right)?)
            }
        }
    }

    /// Persists a versioned artifact while retaining the exact in-flight
    /// generation before every await. If an older PUT committed but was
    /// cancelled, a retry first reconciles that payload to recover its object
    /// version, then writes the callback's newer mutation generation.
    async fn persist_retained_snapshot(
        storage: Storage,
        path: String,
        object_version: Arc<RwLock<ObjectVersion>>,
        pending_write: Arc<ParkingMutex<Option<PendingVersionedWrite>>>,
        data: Vec<u8>,
        kind: PayloadMatch,
    ) -> Result<(), BoxError> {
        loop {
            let pending = {
                let mut slot = pending_write.lock();
                slot.get_or_insert_with(|| PendingVersionedWrite {
                    payload: data.clone(),
                    expected_version: object_version.read().clone(),
                })
                .clone()
            };

            let version = match storage
                .put_bytes(
                    &path,
                    Bytes::from(pending.payload.clone()),
                    PutMode::Update(pending.expected_version.clone().into()),
                )
                .await
            {
                Ok(version) => version,
                Err(err @ DBError::Precondition { .. }) => {
                    let (persisted, version) = storage.fetch_bytes(&path).await?;
                    if !Self::payloads_match(kind, &pending.payload, &persisted)? {
                        return Err(BoxError::from(err));
                    }
                    version
                }
                Err(err) => return Err(BoxError::from(err)),
            };

            *object_version.write() = version;
            *pending_write.lock() = None;
            if Self::payloads_match(kind, &pending.payload, &data)? {
                return Ok(());
            }

            // `data` belongs to a newer structural generation. The next loop
            // registers it with the token recovered above before awaiting its
            // own conditional PUT.
        }
    }

    /// Persists one coherent graph snapshot, then deletes removed-node blobs.
    ///
    /// [`HnswIndex::flush_with`] owns the crash contract and generation gate:
    /// dirty nodes are durable first, then the ids bitmap, and finally the
    /// metadata is compare-and-swap updated as the commit record. Mutations
    /// crossing the I/O window remain pending as the next snapshot rather than
    /// being mixed into the metadata or ids of this one.
    ///
    /// Returns `true` when any object was written or deleted.
    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let had_removed = self.index.has_removed_nodes();
        let node_name = Arc::new(self.name.clone());
        let node_storage = Arc::new(self.storage.clone());
        let ids_path = Hnsw::ids_path(&self.name);
        let ids_storage = self.storage.clone();
        let ids_version = self.ids_version.clone();
        let pending_ids_write = self.pending_ids_write.clone();
        let metadata_path = Hnsw::metadata_path(&self.name);
        let metadata_storage = self.storage.clone();
        let metadata_version = self.metadata_version.clone();
        let pending_metadata_write = self.pending_metadata_write.clone();
        let saved = self
            .index
            .flush_with(
                now_ms,
                move |id, data| {
                    let name = node_name.clone();
                    let storage = node_storage.clone();
                    async move {
                        let path = Hnsw::node_path(name.as_str(), id);
                        storage
                            .put_bytes(&path, Bytes::from(data), PutMode::Overwrite)
                            .await
                            .map_err(BoxError::from)?;
                        Ok(true)
                    }
                },
                move |data| {
                    Self::persist_retained_snapshot(
                        ids_storage,
                        ids_path,
                        ids_version,
                        pending_ids_write,
                        data,
                        PayloadMatch::Exact,
                    )
                },
                move |data| {
                    Self::persist_retained_snapshot(
                        metadata_storage,
                        metadata_path,
                        metadata_version,
                        pending_metadata_write,
                        data,
                        PayloadMatch::Metadata,
                    )
                },
            )
            .await?;

        // Delete the persisted blobs of removed nodes; without this they
        // would leak forever. "Not found" is success (already deleted).
        let n = Arc::new(self.name.clone());
        let s = Arc::new(self.storage.clone());
        self.index
            .purge_removed_nodes(async move |id| {
                let path = Hnsw::node_path(n.clone().as_str(), id);
                match s.clone().delete(&path).await {
                    Ok(()) | Err(DBError::NotFound { .. }) => Ok(true),
                    Err(err) => Err(err.into()),
                }
            })
            .await?;

        Ok(saved || had_removed)
    }

    /// Returns whether metadata, nodes, or removed-node tombstones have
    /// in-memory changes to flush.
    pub fn has_pending_flush(&self) -> bool {
        if self.index.has_dirty_nodes() || self.index.has_removed_nodes() {
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
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
        memory::InMemory, path::Path,
    };
    use parking_lot::Mutex;
    use std::fmt;

    /// In-memory object store with a one-shot path-targeted PUT failure and a
    /// write log, used to assert both failure retryability and durable order.
    #[derive(Debug, Default)]
    struct FailPutStore {
        inner: Arc<InMemory>,
        fault: Mutex<Option<(String, bool)>>,
        puts: Mutex<Vec<String>>,
    }

    impl FailPutStore {
        fn fail_next_put(&self, suffix: impl Into<String>) {
            *self.fault.lock() = Some((suffix.into(), false));
        }

        /// Persists the target object and then reports an injected error. The
        /// caller is dropped after the error, modeling a crash immediately
        /// after that atomic PUT became durable.
        fn crash_after_next_put(&self, suffix: impl Into<String>) {
            *self.fault.lock() = Some((suffix.into(), true));
        }

        fn clear_puts(&self) {
            self.puts.lock().clear();
        }

        fn put_suffixes(&self) -> Vec<String> {
            self.puts.lock().clone()
        }
    }

    impl fmt::Display for FailPutStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("FailPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FailPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            let path = location.to_string();
            self.puts.lock().push(path.clone());
            let fault = {
                let mut fault = self.fault.lock();
                if fault
                    .as_ref()
                    .is_some_and(|(suffix, _)| path.ends_with(suffix))
                {
                    fault.take()
                } else {
                    None
                }
            };
            if matches!(fault.as_ref(), Some((_, false))) {
                return Err(object_store::Error::Generic {
                    store: "fail_put",
                    source: "injected put failure".into(),
                });
            }
            let result = self.inner.put_opts(location, payload, opts).await?;
            if matches!(fault.as_ref(), Some((_, true))) {
                return Err(object_store::Error::Generic {
                    store: "fail_put",
                    source: "injected crash after durable put".into(),
                });
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

    async fn fault_index() -> (Hnsw, Storage, Arc<FailPutStore>) {
        let object_store = Arc::new(FailPutStore::default());
        let storage = Storage::connect(
            "hnsw_fault_tests".to_string(),
            object_store.clone(),
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
        object_store.clear_puts();
        index
            .insert(1, vec![bf16::from_f32(1.0), bf16::from_f32(1.0)], 2)
            .unwrap();
        (index, storage, object_store)
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

    async fn assert_crash_after_put(suffix: &str, expected_puts: &[&str], visible: bool) {
        let (index, storage, object_store) = fault_index().await;
        object_store.crash_after_next_put(suffix);
        assert!(index.flush(3).await.is_err());

        let puts = object_store.put_suffixes();
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

        // The structural version does not change for searches. A live query
        // between cancellation and retry must therefore be ignored by
        // metadata read-back reconciliation just like `last_saved`.
        let _ = index.search(&[1.0, 1.0], 1);

        // Advance both ids and metadata before retrying the stale writer.
        // Any committed-but-unobserved generation must be reconciled first,
        // then this newer generation must use the recovered object token.
        index
            .insert(2, vec![bf16::from_f32(2.0), bf16::from_f32(2.0)], 4)
            .unwrap();

        // The original writer did not observe the durable PUT result and thus
        // still holds the old CAS token. It must be able to read back an
        // identical artifact, repair that token, and finish the same logical
        // generation without requiring a process restart.
        assert_retry_recovers(&index, &storage).await;
        let recovered = Hnsw::bootstrap("embedding".to_string(), storage)
            .await
            .unwrap();
        assert_eq!(recovered.search(&[2.0, 2.0], 1), vec![(2, 0.0)]);
    }

    #[tokio::test]
    async fn node_put_failure_does_not_publish_ids_or_metadata() {
        let (index, storage, object_store) = fault_index().await;
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

        object_store.fail_next_put("n_1.cbor");
        assert!(index.flush(3).await.is_err());
        assert_eq!(object_store.put_suffixes().len(), 1);
        assert!(object_store.put_suffixes()[0].ends_with("n_1.cbor"));
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
        let (index, storage, object_store) = fault_index().await;
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

        object_store.fail_next_put("ids.cbor");
        assert!(index.flush(3).await.is_err());
        let puts = object_store.put_suffixes();
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
        let crashed = Hnsw::bootstrap("embedding".to_string(), storage.clone())
            .await
            .unwrap();
        assert!(crashed.search(&[1.0, 1.0], 1).is_empty());

        assert_retry_recovers(&index, &storage).await;
    }

    #[tokio::test]
    async fn metadata_put_failure_is_last_and_retryable() {
        let (index, storage, object_store) = fault_index().await;
        let old_metadata = storage
            .fetch_bytes(&Hnsw::metadata_path("embedding"))
            .await
            .unwrap()
            .0;

        object_store.fail_next_put("meta.cbor");
        assert!(index.flush(3).await.is_err());
        let puts = object_store.put_suffixes();
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
        assert_crash_after_put("n_1.cbor", &["n_1.cbor"], false).await;
    }

    #[tokio::test]
    async fn crash_after_ids_put_reopens_recoverable_snapshot() {
        assert_crash_after_put("ids.cbor", &["n_1.cbor", "ids.cbor"], true).await;
    }

    #[tokio::test]
    async fn crash_after_metadata_put_reopens_committed_snapshot() {
        assert_crash_after_put("meta.cbor", &["n_1.cbor", "ids.cbor", "meta.cbor"], true).await;
    }
}
