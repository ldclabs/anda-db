use anda_db_hnsw::HnswIndex;
use bytes::Bytes;
use parking_lot::RwLock;
use std::{fmt::Debug, hash::Hash, sync::Arc};

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
pub struct Hnsw {
    name: String,
    index: HnswIndex,
    storage: Storage, // 与 Collection 共享同一个 Storage 实例
    metadata_version: RwLock<ObjectVersion>,
    ids_version: RwLock<ObjectVersion>,
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
        let metadata_version = storage
            .put_bytes(
                &Hnsw::metadata_path(&name),
                metadata.into(),
                PutMode::Overwrite,
            )
            .await?;
        let ids_version = storage
            .put_bytes(&Hnsw::ids_path(&name), ids.into(), PutMode::Overwrite)
            .await?;
        Ok(Self {
            name,
            index,
            storage,
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
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
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
        })
    }

    /// Persists dirty metadata, id lists, and graph nodes, then deletes the
    /// blobs of removed nodes.
    ///
    /// Node-blob deletion runs last: the ids bitmap persisted in this same
    /// flush already excludes removed ids, so a crash right before the purge
    /// only leaves orphaned blobs that are never loaded again, while a crash
    /// right after is fully clean.
    ///
    /// Returns `true` when any object was written or deleted.
    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let meta_saved = {
            let path = Hnsw::metadata_path(&self.name);
            self.index
                .store_metadata_with(now_ms, async |data| {
                    let metadata_version = { self.metadata_version.read().clone() };
                    let metadata_version = self
                        .storage
                        .put_bytes(
                            &path,
                            Bytes::copy_from_slice(data),
                            PutMode::Update(metadata_version.into()),
                        )
                        .await
                        .map_err(BoxError::from)?;
                    *self.metadata_version.write() = metadata_version;
                    Ok(())
                })
                .await?
        };
        let had_dirty = self.index.has_dirty_nodes();
        let had_removed = self.index.has_removed_nodes();

        if !meta_saved && !had_dirty && !had_removed {
            return Ok(false);
        }

        if meta_saved {
            let mut buf = Vec::with_capacity(256);
            self.index.store_ids(&mut buf)?;
            let path = Hnsw::ids_path(&self.name);
            let ids_version = { self.ids_version.read().clone() };
            let ids_version = self
                .storage
                .put_bytes(&path, buf.into(), PutMode::Update(ids_version.into()))
                .await?;
            {
                *self.ids_version.write() = ids_version;
            }
        }

        let n = Arc::new(self.name.clone());
        let s = Arc::new(self.storage.clone());
        self.index
            .store_dirty_nodes(async move |id, data| {
                let path = Hnsw::node_path(n.clone().as_str(), id);
                let _ = s
                    .clone()
                    .put_bytes(&path, Bytes::copy_from_slice(data), PutMode::Overwrite)
                    .await?;
                Ok(true)
            })
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

        Ok(meta_saved || had_dirty || had_removed)
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
