use anda_db_hnsw::HnswIndex;
use bytes::Bytes;
use parking_lot::RwLock;
use std::{fmt::Debug, hash::Hash, sync::Arc};

pub use anda_db_hnsw::{HnswConfig, HnswMetadata, HnswStats};

use crate::{
    error::DBError,
    schema::{Fe, Vector},
    storage::{ObjectVersion, PutMode, Storage},
};

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
    fn metadata_path(name: &str) -> String {
        format!("hnsw_indexes/{name}/meta.cbor")
    }

    fn ids_path(name: &str) -> String {
        format!("hnsw_indexes/{name}/ids.cbor")
    }

    fn node_path(name: &str, node: u64) -> String {
        format!("hnsw_indexes/{name}/n_{node}.cbor")
    }

    pub async fn new(
        field: &Fe,
        config: HnswConfig,
        storage: Storage,
        now_ms: u64,
    ) -> Result<Self, DBError> {
        let name = field.name().to_string();
        let index = HnswIndex::new(name.clone(), Some(config));
        let mut metadata = Vec::new();
        let mut ids = Vec::new();
        index
            .flush(&mut metadata, &mut ids, now_ms, async |_, _| Ok(true))
            .await?;
        let metadata_version = storage
            .put_bytes(
                &Hnsw::metadata_path(&name),
                metadata.into(),
                PutMode::Create,
            )
            .await?;
        let ids_version = storage
            .put_bytes(&Hnsw::ids_path(&name), ids.into(), PutMode::Create)
            .await?;
        Ok(Self {
            name,
            index,
            storage,
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
        })
    }

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

    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let mut buf = Vec::with_capacity(256);
        let meta_saved = self.index.store_metadata(&mut buf, now_ms)?;
        let had_dirty = self.index.has_dirty_nodes();

        if !meta_saved && !had_dirty {
            return Ok(false);
        }

        if meta_saved {
            let path = Hnsw::metadata_path(&self.name);
            let metadata_version = { self.metadata_version.read().clone() };
            let metadata_version = self
                .storage
                .put_bytes(
                    &path,
                    Bytes::copy_from_slice(&buf[..]),
                    PutMode::Update(metadata_version.into()),
                )
                .await?;
            {
                *self.metadata_version.write() = metadata_version;
            }

            buf.clear();
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

        Ok(meta_saved || had_dirty)
    }

    pub fn has_pending_flush(&self) -> bool {
        if self.index.has_dirty_nodes() {
            return true;
        }

        self.index.has_pending_metadata_flush()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_name(&self) -> &str {
        &self.name
    }

    pub fn stats(&self) -> HnswStats {
        self.index.stats()
    }

    pub fn metadata(&self) -> HnswMetadata {
        self.index.metadata()
    }

    pub fn insert(&self, id: u64, vector: Vector, now_ms: u64) -> Result<(), DBError> {
        self.index.insert(id, vector, now_ms)?;
        Ok(())
    }

    pub fn remove(&self, id: u64, now_ms: u64) -> bool {
        self.index.remove(id, now_ms)
    }

    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<(u64, f32)> {
        self.index.search_f32(query, top_k).unwrap_or_default()
    }
}
