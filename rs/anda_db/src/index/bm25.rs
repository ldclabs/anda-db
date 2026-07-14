use anda_db_tfs::BM25Index;
use bytes::Bytes;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::Hash, sync::Arc};
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

/// On-disk envelope emitted by `anda_db_tfs::BM25Index` metadata writers.
///
/// Decode this directly instead of calling `BM25Index::metadata()` on a
/// metadata-only shell: that accessor overlays live counters from the empty
/// shell and would erase persisted `num_elements` during conflict comparison.
#[derive(Deserialize, Serialize)]
struct PersistedBM25Index {
    metadata: BM25Metadata,
}

#[derive(Clone)]
struct PendingMetadataWrite {
    payload: Vec<u8>,
    expected_version: ObjectVersion,
}

fn normalize_metadata_payload(data: &[u8]) -> Result<Vec<u8>, BoxError> {
    let mut payload: PersistedBM25Index = cbor2::from_reader(data)?;
    payload.metadata.stats.last_saved = 0;
    payload.metadata.stats.search_count = 0;
    let mut normalized = Vec::new();
    cbor2::to_writer(&payload, &mut normalized)?;
    Ok(normalized)
}

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
    /// Exact metadata generation registered before its conditional PUT is
    /// awaited. A committed-but-cancelled older generation must be reconciled
    /// before a later mutation generation can use the refreshed CAS token.
    pending_metadata_write: ParkingMutex<Option<PendingMetadataWrite>>,
    /// Serializes complete object-store flushes for this wrapper. Mutation
    /// consistency is handled by `BM25Index::flush_with`; this gate prevents
    /// two frozen generations from being uploaded in reverse order.
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
            pending_metadata_write: ParkingMutex::new(None),
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
            pending_metadata_write: ParkingMutex::new(None),
            flush_gate: Arc::new(Mutex::new(())),
        })
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
        let _flush_guard = self.flush_gate.clone().lock_owned().await;
        self.flush_inner(now_ms).await
    }

    /// The sole production persistence path. Callers must hold `flush_gate`.
    async fn flush_inner(&self, now_ms: u64) -> Result<bool, DBError> {
        let metadata_path = BM25::metadata_path(&self.name);
        let saved = self
            .index
            .flush_with(
                now_ms,
                move |data: Vec<u8>| async move {
                    self.persist_metadata_snapshot(metadata_path, data).await
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

    /// Persists one metadata generation and repairs the local object-version
    /// token when a previously cancelled PUT committed remotely but its
    /// `PutResult` was never observed.
    async fn persist_metadata_snapshot(&self, path: String, data: Vec<u8>) -> Result<(), BoxError> {
        let intended = normalize_metadata_payload(&data)?;
        loop {
            let pending = {
                let mut slot = self.pending_metadata_write.lock();
                slot.get_or_insert_with(|| PendingMetadataWrite {
                    payload: data.clone(),
                    expected_version: self.metadata_version.read().clone(),
                })
                .clone()
            };
            let pending_logical = normalize_metadata_payload(&pending.payload)?;

            let version = match self
                .storage
                .put_bytes(
                    &path,
                    Bytes::from(pending.payload.clone()),
                    PutMode::Update(pending.expected_version.clone().into()),
                )
                .await
            {
                Ok(version) => version,
                Err(err @ DBError::Precondition { .. }) => {
                    // `fetch_bytes` bypasses Storage's cache. This is
                    // essential after a post-commit cancellation because the
                    // cache invalidation happens only after `put_bytes`
                    // returns. Never consume a true conflicting payload.
                    let (persisted, version) = self.storage.fetch_bytes(&path).await?;
                    if pending_logical != normalize_metadata_payload(&persisted)? {
                        return Err(BoxError::from(err));
                    }
                    version
                }
                Err(err) => return Err(BoxError::from(err)),
            };

            // No await follows the successful reconciliation: cancellation
            // cannot expose a refreshed token while leaving the completed
            // pending generation registered.
            *self.metadata_version.write() = version;
            *self.pending_metadata_write.lock() = None;
            if pending_logical == intended {
                return Ok(());
            }

            // A mutation arrived after the cancelled generation. Register the
            // callback's newer immutable payload with the refreshed token and
            // persist it before allowing the low-level flush to commit.
        }
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
        let _flush_guard = self.flush_gate.clone().lock_owned().await;
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

        // Delegate to the same coordinated snapshot and ordering used by
        // normal production flushes.
        self.flush_inner(unix_ms()).await?;

        // Best-effort cleanup of bucket files beyond the compacted range.
        // If a concurrent mutation allocated a bucket in the stale range,
        // leave all files there alone; the next flush will reconcile them.
        // A mutation beginning after this check is not referenced by the
        // metadata just persisted, so deleting an orphan remains safe.
        if self.index.stats().max_bucket_id < new_bucket_count as u32 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use object_store::memory::InMemory;

    /// Regression (P0): object-store PUT futures may be cancelled after the
    /// conditional write committed but before the returned version reached
    /// this wrapper. A retry must read back the identical logical metadata,
    /// refresh its local CAS token, finish the bucket phase, and remain able
    /// to commit the following generation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_metadata_post_commit_cancellation_repairs_version_and_reopens()
    -> Result<(), DBError> {
        let object_store = Arc::new(InMemory::new());
        let storage = Storage::connect(
            "bm25_post_commit_cancel".to_string(),
            object_store,
            StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
        )
        .await?;
        let bm25 = Arc::new(
            BM25::new(
                vec!["body".to_string()],
                default_tokenizer(),
                storage.clone(),
                1,
            )
            .await?,
        );
        bm25.insert(1, "alpha", 2)?;

        let metadata_committed = Arc::new(tokio::sync::Notify::new());
        let interrupted = {
            let bm25 = bm25.clone();
            let metadata_bm25 = bm25.clone();
            let bucket_bm25 = bm25.clone();
            let metadata_committed = metadata_committed.clone();
            tokio::spawn(async move {
                let metadata_path = BM25::metadata_path(&bm25.name);
                bm25.index
                    .flush_with(
                        3,
                        move |data| {
                            let bm25 = metadata_bm25.clone();
                            let metadata_committed = metadata_committed.clone();
                            async move {
                                let expected = { bm25.metadata_version.read().clone() };
                                *bm25.pending_metadata_write.lock() = Some(PendingMetadataWrite {
                                    payload: data.clone(),
                                    expected_version: expected.clone(),
                                });
                                let _committed_version = bm25
                                    .storage
                                    .put_bytes(
                                        &metadata_path,
                                        Bytes::from(data),
                                        PutMode::Update(expected.into()),
                                    )
                                    .await
                                    .map_err(BoxError::from)?;
                                metadata_committed.notify_one();
                                // Model cancellation after the backend has
                                // committed but before the wrapper observes
                                // and publishes `_committed_version`.
                                std::future::pending::<Result<(), BoxError>>().await
                            }
                        },
                        move |id, data| {
                            let bm25 = bucket_bm25.clone();
                            async move {
                                let path = BM25::bucket_path(&bm25.name, id);
                                bm25.storage
                                    .put_bytes(&path, Bytes::from(data), PutMode::Overwrite)
                                    .await
                                    .map_err(BoxError::from)?;
                                Ok(true)
                            }
                        },
                    )
                    .await
            })
        };

        metadata_committed.notified().await;
        interrupted.abort();
        assert!(
            interrupted
                .await
                .expect_err("flush should be cancelled after metadata commit")
                .is_cancelled()
        );
        assert!(bm25.has_pending_flush());

        // Search counters are observational and can advance without changing
        // the structural metadata version. They must not block reconciliation.
        assert!(
            bm25.search("alpha", 10, None)
                .iter()
                .any(|(id, _)| *id == 1)
        );

        // Advance the structural generation before retrying. The wrapper must
        // first reconcile the retained alpha generation with its stale token,
        // then use the refreshed token to commit this newer beta generation.
        bm25.insert(2, "beta", 4)?;

        // The retry encounters Precondition with its stale local token. It
        // must accept only the retained equivalent payload, refresh the token,
        // then persist the newer callback payload and finish bucket writes.
        assert!(bm25.flush(5).await?);

        // A second logical generation proves the repaired token—not merely
        // the read-back acceptance—was published locally.
        bm25.insert(3, "gamma", 6)?;
        assert!(bm25.flush(7).await?);

        let reopened = BM25::bootstrap("body".to_string(), default_tokenizer(), storage).await?;
        assert!(
            reopened
                .search("alpha", 10, None)
                .iter()
                .any(|(id, _)| *id == 1)
        );
        assert!(
            reopened
                .search("beta", 10, None)
                .iter()
                .any(|(id, _)| *id == 2)
        );
        assert!(
            reopened
                .search("gamma", 10, None)
                .iter()
                .any(|(id, _)| *id == 3)
        );
        Ok(())
    }
}
