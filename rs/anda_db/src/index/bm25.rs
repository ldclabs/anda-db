use anda_db_tfs::{BM25Index, BucketObject};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::BTreeSet, fmt::Debug, hash::Hash, sync::Arc};
use tokio::sync::Mutex;

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
///
/// The metadata CAS token is the last defense against a second writer, which
/// the single-writer deployment contract forbids. A `Precondition` conflict
/// (or a cancelled flush) is never reconciled in place: the error propagates,
/// the collection poisons its handle and reopening rebuilds this wrapper from
/// the durable objects.
pub struct BM25 {
    name: String,
    fields: Vec<String>,
    index: BM25Index<TokenizerChain>,
    storage: Storage, // 与 Collection 共享同一个 Storage 实例
    metadata_version: RwLock<ObjectVersion>,
    /// Serializes complete object-store flushes for this wrapper. Mutation
    /// consistency is handled by `BM25Index::flush_with`; this gate prevents
    /// two frozen generations from being uploaded in reverse order (compact
    /// runs under a shared collection lease, so two compacts can overlap).
    flush_gate: Arc<Mutex<()>>,
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

    /// Object path for a bucket generation. Generation `0` is the legacy
    /// (pre-manifest) un-suffixed object and is only ever read, never
    /// written; the manifest protocol writes generation-suffixed objects.
    fn bucket_path(name: &str, object: BucketObject) -> String {
        if object.generation == 0 {
            format!("bm25_indexes/{name}/b_{}.cbor", object.bucket_id)
        } else {
            format!(
                "bm25_indexes/{name}/b_{}_{}.cbor",
                object.bucket_id, object.generation
            )
        }
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
            .flush(&mut data, now_ms, |_, _| std::future::ready(Ok(())))
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
            flush_gate: Arc::new(Mutex::new(())),
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
        let index = BM25Index::load_all(tokenizer, &metadata[..], async move |object| {
            let path = BM25::bucket_path(n.clone().as_str(), object);
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
            flush_gate: Arc::new(Mutex::new(())),
        })
    }

    /// Persists dirty metadata and buckets.
    ///
    /// Delegates to [`BM25Index::flush_with`], which implements the manifest
    /// commit protocol: every dirty bucket is written to a fresh
    /// generation-suffixed object, then the metadata (whose manifest
    /// references them) is committed with a single conditional PUT. Objects
    /// the new manifest no longer references are deleted best-effort
    /// afterwards.
    ///
    /// Returns `true` when any object was written.
    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let _flush_guard = self.flush_gate.clone().lock_owned().await;
        self.flush_inner(now_ms).await
    }

    /// The sole production persistence path. Callers must hold `flush_gate`.
    async fn flush_inner(&self, now_ms: u64) -> Result<bool, DBError> {
        let metadata_path = BM25::metadata_path(&self.name);
        let outcome = self
            .index
            .flush_with(
                now_ms,
                move |data: Vec<u8>| {
                    // The manifest commit. A single conditional PUT is the
                    // remaining second-writer defense: a `Precondition`
                    // conflict is not reconciled in place — the error
                    // propagates, the collection poisons its handle and
                    // recovery happens on reopen.
                    let metadata_path = metadata_path.clone();
                    async move {
                        let expected = { self.metadata_version.read().clone() };
                        let version = self
                            .storage
                            .put_bytes(
                                &metadata_path,
                                Bytes::from(data),
                                PutMode::Update(expected.into()),
                            )
                            .await
                            .map_err(BoxError::from)?;
                        *self.metadata_version.write() = version;
                        Ok(())
                    }
                },
                |object: BucketObject, data: Vec<u8>| async move {
                    let path = BM25::bucket_path(&self.name, object);
                    let _ = self
                        .storage
                        .put_bytes(&path, Bytes::from(data), PutMode::Overwrite)
                        .await?;
                    Ok(())
                },
            )
            .await?;

        // Best-effort retirement of bucket objects the committed manifest no
        // longer references. A failed deletion only leaks storage space and
        // never affects loads: the manifest is the loader's single source of
        // truth.
        for object in &outcome.obsolete {
            let path = BM25::bucket_path(&self.name, *object);
            match self.storage.delete(&path).await {
                Ok(()) | Err(DBError::NotFound { .. }) => {}
                Err(err) => {
                    log::warn!(
                        action = "BM25::flush",
                        index = self.name,
                        bucket = object.bucket_id,
                        generation = object.generation;
                        "Failed to delete obsolete bucket object: {err:?}",
                    );
                }
            }
        }

        Ok(outcome.saved)
    }

    /// Compacts bucket layout and persists the new layout if the bucket count
    /// shrinks.
    ///
    /// Under the manifest protocol compaction needs no special write
    /// ordering: the repacked layout becomes visible atomically with the
    /// manifest commit of the following flush, which also retires every
    /// pre-compaction bucket object best-effort.
    pub async fn compact_index(&self) -> Result<(), DBError> {
        let _flush_guard = self.flush_gate.clone().lock_owned().await;
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

        // Delegate to the same coordinated persistence path used by normal
        // production flushes; it commits the manifest and deletes the
        // replaced objects.
        self.flush_inner(unix_ms()).await?;

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

    /// Erases `ids` from the index without needing their indexed text.
    ///
    /// Used by the dead-id repair path, where the document bodies are gone and
    /// [`remove`](Self::remove) therefore has no text to re-tokenize. Sweeps
    /// every posting list once for the whole set; see
    /// [`BM25Index::purge_ids`] for the cost and consistency guarantees.
    ///
    /// Returns the number of ids that were actually present in the index.
    pub fn purge_ids(&self, ids: &BTreeSet<DocumentId>, now_ms: u64) -> usize {
        self.index.purge_ids(ids, now_ms)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use object_store::memory::InMemory;

    /// The wrapper's metadata CAS token is the last defense against a second
    /// writer: after a foreign overwrite the next flush must fail instead of
    /// being silently reconciled in place. Recovery is a reopen, which reads
    /// the durable objects and their fresh versions.
    #[tokio::test]
    async fn test_foreign_metadata_writer_fails_flush() -> Result<(), DBError> {
        let object_store = Arc::new(InMemory::new());
        let storage = Storage::connect(
            "bm25_foreign_writer".to_string(),
            object_store,
            StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
        )
        .await?;
        let bm25 = BM25::new(
            vec!["body".to_string()],
            default_tokenizer(),
            storage.clone(),
            1,
        )
        .await?;
        bm25.insert(1, "alpha", 2)?;
        assert!(bm25.flush(3).await?);

        // Simulate a second writer replacing the durable metadata object.
        let (data, _) = storage.fetch_bytes(&BM25::metadata_path("body")).await?;
        storage
            .put_bytes(&BM25::metadata_path("body"), data, PutMode::Overwrite)
            .await?;

        bm25.insert(2, "beta", 4)?;
        assert!(
            bm25.flush(5).await.is_err(),
            "stale CAS token must remain a conflict",
        );

        // Reopening loads the durable state (generation "alpha"); the
        // unflushed beta insert is recovered by the collection's WAL replay,
        // not by this wrapper.
        let reopened = BM25::bootstrap("body".to_string(), default_tokenizer(), storage).await?;
        assert!(
            reopened
                .search("alpha", 10, None)
                .iter()
                .any(|(id, _)| *id == 1)
        );
        Ok(())
    }
}
