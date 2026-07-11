use anda_db_tfs::BM25Index;
use bytes::Bytes;
use parking_lot::RwLock;
use std::{fmt::Debug, hash::Hash, sync::Arc};

use super::from_virtual_field_name;
pub use anda_db_tfs::{
    BM25Config, BM25Error, BM25Metadata, BM25Params, BM25Stats, TokenizerChain, collect_tokens,
    default_tokenizer, jieba_tokenizer,
};

use crate::{
    error::DBError,
    schema::{BoxError, DocumentId},
    storage::{ObjectVersion, PutMode, Storage},
    unix_ms,
};

/// Collection-level wrapper around the full-text BM25 index.
///
/// The wrapper keeps the index name, virtual field list, storage namespace, and
/// object versions needed for optimistic metadata updates.
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
    pub(crate) fn dir_path(name: &str) -> String {
        format!("bm25_indexes/{name}/")
    }

    fn metadata_path(name: &str) -> String {
        format!("bm25_indexes/{name}/meta.cbor")
    }

    fn bucket_path(name: &str, bucket: u32) -> String {
        format!("bm25_indexes/{name}/b_{bucket}.cbor")
    }

    /// Tokenizes `text` with `tokenizer` and returns the unique indexed terms.
    pub fn collect_tokens(tokenizer: &TokenizerChain, text: &str) -> Vec<String> {
        let mut tokenizer = tokenizer.clone();
        let token_freqs = collect_tokens(&mut tokenizer, text, None);
        token_freqs.into_keys().collect()
    }

    /// Creates a new persisted BM25 index for the provided fields.
    ///
    /// The initial metadata object is written immediately; the caller is
    /// responsible for backfilling existing documents before publishing the
    /// index in collection metadata.
    pub async fn new(
        fields: Vec<String>,
        tokenizer: TokenizerChain,
        storage: Storage,
        now_ms: u64,
    ) -> Result<Self, DBError> {
        let name = fields.join("-");
        let config = BM25Config {
            bucket_overload_size: storage.bucket_overload_size(),
            ..Default::default()
        };
        let index = BM25Index::new(name.clone(), tokenizer, Some(config));
        let mut data = Vec::new();
        index
            .flush(&mut data, now_ms, |_, _| std::future::ready(Ok(true)))
            .await?;
        // The collection metadata is the source of truth for which indexes
        // exist, so overwrite any leftover files from a crashed creation or a
        // previously removed index instead of failing with AlreadyExists.
        let ver = storage
            .put_bytes(&BM25::metadata_path(&name), data.into(), PutMode::Overwrite)
            .await?;
        Ok(Self {
            name,
            fields,
            index,
            storage,
            metadata_version: RwLock::new(ver),
        })
    }

    pub(crate) async fn drop_data(&self) {
        // Delete the metadata and all bucket objects under the index directory.
        if let Err(err) = self.storage.drop_prefix(&BM25::dir_path(&self.name)).await {
            log::warn!(
                action = "BM25::drop_data",
                index = self.name;
                "Failed to drop BM25 index data: {err:?}",
            );
        }
    }

    /// Loads an existing BM25 index from persisted metadata and bucket objects.
    pub async fn bootstrap(
        name: String,
        tokenizer: TokenizerChain,
        storage: Storage,
    ) -> Result<Self, DBError> {
        let fields = from_virtual_field_name(&name);
        let (metadata, ver) = storage.fetch_bytes(&BM25::metadata_path(&name)).await?;
        let n = Arc::new(name.clone());
        let s = Arc::new(storage.clone());
        let index = BM25Index::load_all(tokenizer, &metadata[..], async move |id: u32| {
            let path = BM25::bucket_path(n.clone().as_str(), id);
            match s.clone().fetch_bytes(&path).await {
                Ok((data, _)) => Ok(Some(data.into())),
                Err(DBError::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await?;

        Ok(Self {
            name,
            fields,
            index,
            storage,
            metadata_version: RwLock::new(ver),
        })
    }

    /// Persists metadata through [`BM25Index::store_metadata_with`], so the
    /// saved-version watermark only advances after the object-store write
    /// succeeds and a failed write is retried by the next flush.
    async fn store_metadata(&self, now_ms: u64) -> Result<bool, DBError> {
        let path = BM25::metadata_path(&self.name);
        let meta_saved = self
            .index
            .store_metadata_with(now_ms, async |data| {
                let ver = { self.metadata_version.read().clone() };
                let ver = self
                    .storage
                    .put_bytes(
                        &path,
                        Bytes::copy_from_slice(data),
                        PutMode::Update(ver.into()),
                    )
                    .await
                    .map_err(BoxError::from)?;
                *self.metadata_version.write() = ver;
                Ok(())
            })
            .await?;
        Ok(meta_saved)
    }

    async fn store_dirty_buckets(&self) -> Result<(), DBError> {
        let n = Arc::new(self.name.clone());
        let s = Arc::new(self.storage.clone());
        self.index
            .store_dirty_buckets(move |id, data| {
                let n = n.clone();
                let s = s.clone();
                async move {
                    let path = BM25::bucket_path(n.as_str(), id);
                    let _ = s
                        .put_bytes(&path, Bytes::from(data), PutMode::Overwrite)
                        .await?;
                    Ok(true)
                }
            })
            .await?;
        Ok(())
    }

    /// Persists dirty metadata and buckets.
    ///
    /// Delegates to [`BM25Index::flush_with`], which implements the
    /// crash-safe grow-then-shrink ordering: buckets not yet referenced by
    /// the persisted metadata (token-migration targets) are written first,
    /// then the metadata, then the rewrites of already-referenced buckets.
    /// See `BM25Index::flush_with` for the crash-window analysis; keeping a
    /// single implementation in the crate prevents this production path from
    /// drifting out of sync with it again.
    ///
    /// Returns `true` when any object was written.
    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let metadata_path = BM25::metadata_path(&self.name);
        let saved = self
            .index
            .flush_with(
                now_ms,
                move |data: Vec<u8>| async move {
                    let ver = { self.metadata_version.read().clone() };
                    let ver = self
                        .storage
                        .put_bytes(&metadata_path, Bytes::from(data), PutMode::Update(ver.into()))
                        .await
                        .map_err(BoxError::from)?;
                    *self.metadata_version.write() = ver;
                    Ok(())
                },
                |id: u32, data: Vec<u8>| async move {
                    let path = BM25::bucket_path(&self.name, id);
                    let _ = self
                        .storage
                        .put_bytes(&path, Bytes::from(data), PutMode::Overwrite)
                        .await?;
                    Ok(true)
                },
            )
            .await?;
        Ok(saved)
    }

    /// Compacts bucket layout and persists the new layout if the bucket count
    /// shrinks.
    ///
    /// Compaction persists **buckets before metadata** because it is the only
    /// operation that shrinks `max_bucket_id`: the loader only scans bucket
    /// ids up to the persisted `max_bucket_id`, so committing the reduced
    /// range first would hide repacked tokens if the process crashed before
    /// the low-id bucket files were rewritten (see `BTree::compact_index` for
    /// the same rationale). This is the inverse of the steady-state ordering
    /// in [`Self::flush`], where *newly allocated* buckets must precede the
    /// metadata that first references them; `BM25Index::flush_with` detects a
    /// shrunken `max_bucket_id` and applies this buckets-first order too, so
    /// both paths agree. Stale bucket files beyond the compacted range are
    /// deleted best-effort afterwards.
    pub async fn compact_index(&self) -> Result<(), DBError> {
        let old_max_bucket_id = self.index.stats().max_bucket_id;
        let (old_bucket_count, new_bucket_count) = self.index.compact_buckets();
        if new_bucket_count >= old_bucket_count {
            return Ok(());
        }

        log::warn!(
            "Compacted BM25 index '{}': {} -> {} buckets",
            self.name,
            old_bucket_count,
            new_bucket_count
        );

        // Buckets first, then metadata (which shrinks max_bucket_id).
        self.store_dirty_buckets().await?;
        self.store_metadata(unix_ms()).await?;

        // Best-effort cleanup of bucket files beyond the compacted range.
        for id in (new_bucket_count as u32)..=old_max_bucket_id {
            let path = BM25::bucket_path(&self.name, id);
            match self.storage.delete(&path).await {
                Ok(()) | Err(DBError::NotFound { .. }) => {}
                Err(err) => {
                    log::warn!(
                        action = "BM25::compact_index",
                        index = self.name,
                        bucket = id;
                        "Failed to delete stale bucket file: {err:?}",
                    );
                }
            }
        }

        Ok(())
    }

    /// Returns whether metadata or buckets have in-memory changes to flush.
    pub fn has_pending_flush(&self) -> bool {
        if self.index.has_dirty_buckets() {
            return true;
        }

        self.index.has_pending_metadata_flush()
    }

    /// Returns the stable index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the physical fields represented by this BM25 index.
    pub fn virtual_field(&self) -> &[String] {
        &self.fields
    }

    /// Returns a snapshot of BM25 runtime statistics.
    pub fn stats(&self) -> BM25Stats {
        self.index.stats()
    }

    /// Returns a snapshot of BM25 metadata.
    pub fn metadata(&self) -> BM25Metadata {
        self.index.metadata()
    }

    /// Inserts or updates the text indexed for `id`.
    ///
    /// Empty-token documents are ignored because they are not searchable.
    pub fn insert(&self, id: DocumentId, text: &str, now_ms: u64) -> Result<(), DBError> {
        match self.index.insert(id, text, now_ms) {
            Ok(()) => Ok(()),
            Err(BM25Error::TokenizeFailed { .. }) => Ok(()), // Ignore tokenize errors
            Err(e) => Err(e.into()),
        }
    }

    /// Removes the indexed text for `id`.
    pub fn remove(&self, id: DocumentId, text: &str, now_ms: u64) -> bool {
        self.index.remove(id, text, now_ms)
    }

    /// Searches the index and returns `(document_id, score)` pairs.
    pub fn search(&self, query: &str, top_k: usize, params: Option<BM25Params>) -> Vec<(u64, f32)> {
        self.index.search(query, top_k, params)
    }

    /// Searches with advanced query parsing and returns `(document_id, score)` pairs.
    pub fn search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Vec<(u64, f32)> {
        self.index.search_advanced(query, top_k, params)
    }

    /// Searches with advanced query parsing and resource guards.
    pub fn try_search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Result<Vec<(u64, f32)>, DBError> {
        Ok(self.index.try_search_advanced(query, top_k, params)?)
    }
}
