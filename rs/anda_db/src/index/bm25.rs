use anda_db_tfs::BM25Index;
use bytes::Bytes;
use parking_lot::RwLock;
use std::{fmt::Debug, hash::Hash};

use super::from_virtual_field_name;
pub use anda_db_tfs::{
    BM25Config, BM25Error, BM25Metadata, BM25Params, BM25Stats, TokenizerChain, default_tokenizer,
    jieba_tokenizer,
};

use crate::{
    error::DBError,
    schema::DocumentId,
    storage::{ObjectVersion, PutMode, Storage},
};

pub struct BM25 {
    name: String,
    fields: Vec<String>,
    index: BM25Index<TokenizerChain>,
    storage: Storage, // 与 Collection 共享同一个 Storage 实例
    metadata_version: RwLock<ObjectVersion>,
}

impl Debug for BM25 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BM25Index({})", self.name)
    }
}

impl PartialEq for &BM25 {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for &BM25 {}
impl Hash for &BM25 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl BM25 {
    fn metadata_path(name: &str) -> String {
        format!("bm25_indexes/{name}/meta.cbor")
    }

    fn segment_path(name: &str, bucket: u32) -> String {
        format!("bm25_indexes/{name}/s_{bucket}.cbor")
    }

    fn posting_path(name: &str, bucket: u32) -> String {
        format!("bm25_indexes/{name}/p_{bucket}.cbor")
    }

    pub async fn new(
        fields: Vec<String>,
        tokenizer: TokenizerChain,
        storage: Storage,
        now_ms: u64,
    ) -> Result<Self, DBError> {
        let name = fields.join("-");
        let config = BM25Config {
            bucket_overload_size: storage.object_chunk_size() as u32 * 2,
            ..Default::default()
        };
        let index = BM25Index::new(name.clone(), tokenizer, Some(config));
        let mut data = Vec::new();
        index
            .flush(
                &mut data,
                now_ms,
                async |_, _| Ok(true),
                async |_, _| Ok(true),
            )
            .await?;
        let ver = storage
            .put_bytes(&BM25::metadata_path(&name), data.into(), PutMode::Create)
            .await?;
        Ok(Self {
            name,
            fields,
            index,
            storage,
            metadata_version: RwLock::new(ver),
        })
    }

    pub async fn bootstrap(
        name: String,
        tokenizer: TokenizerChain,
        storage: Storage,
    ) -> Result<Self, DBError> {
        let fields = from_virtual_field_name(&name);
        let (metadata, ver) = storage.fetch_bytes(&BM25::metadata_path(&name)).await?;
        let index = BM25Index::load_all(
            tokenizer,
            &metadata[..],
            async |id: u32| {
                let path = BM25::segment_path(&name, id);
                match storage.fetch_bytes(&path).await {
                    Ok((data, _)) => Ok(Some(data.into())),
                    Err(DBError::NotFound { .. }) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            },
            async |id: u32| {
                let path = BM25::posting_path(&name, id);
                match storage.fetch_bytes(&path).await {
                    Ok((data, _)) => Ok(Some(data.into())),
                    Err(DBError::NotFound { .. }) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            },
        )
        .await?;

        Ok(Self {
            name,
            fields,
            index,
            storage,
            metadata_version: RwLock::new(ver),
        })
    }

    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let mut data = Vec::new();
        if !self.index.store_metadata(&mut data, now_ms)? {
            return Ok(false);
        }

        let path = BM25::metadata_path(&self.name);
        let ver = { self.metadata_version.read().clone() };
        let ver = self
            .storage
            .put_bytes(&path, data.into(), PutMode::Update(ver.into()))
            .await?;
        {
            *self.metadata_version.write() = ver;
        }

        self.index
            .store_dirty_segments(async |id, data| {
                let path = BM25::segment_path(&self.name, id);
                let _ = self
                    .storage
                    .put_bytes(&path, Bytes::copy_from_slice(data), PutMode::Overwrite)
                    .await?;
                Ok(true)
            })
            .await?;
        self.index
            .store_dirty_postings(async |id, data| {
                let path = BM25::posting_path(&self.name, id);
                let _ = self
                    .storage
                    .put_bytes(&path, Bytes::copy_from_slice(data), PutMode::Overwrite)
                    .await?;
                Ok(true)
            })
            .await?;

        Ok(true)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn virtual_field(&self) -> &[String] {
        &self.fields
    }

    pub fn stats(&self) -> BM25Stats {
        self.index.stats()
    }

    pub fn metadata(&self) -> BM25Metadata {
        self.index.metadata()
    }

    pub fn insert(&self, id: DocumentId, text: &str, now_ms: u64) -> Result<(), DBError> {
        match self.index.insert(id, text, now_ms) {
            Ok(()) => Ok(()),
            Err(BM25Error::TokenizeFailed { .. }) => Ok(()), // Ignore tokenize errors
            Err(e) => Err(e.into()),
        }
    }

    pub fn remove(&self, id: DocumentId, text: &str, now_ms: u64) -> bool {
        self.index.remove(id, text, now_ms)
    }

    pub fn search(&self, query: &str, top_k: usize, params: Option<BM25Params>) -> Vec<(u64, f32)> {
        self.index.search(query, top_k, params)
    }

    pub fn search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Vec<(u64, f32)> {
        self.index.search_advanced(query, top_k, params)
    }
}
