//! # BM25 index implementation
//!
//! This module contains [`BM25Index`], the concurrent, bucket-sharded BM25
//! index that backs the crate. See the crate-level documentation for a
//! high-level overview.

use anda_db_utils::UniqueVec;
use dashmap::DashMap;
use parking_lot::RwLock;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::{Read, Write},
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use crate::error::*;
use crate::query::*;
use crate::tokenizer::*;

const MAX_NOT_COMPLEMENT_DOCS: usize = 10_000;

/// Estimates the CBOR-serialized size of `value`.
///
/// The result only drives the bucket-packing heuristic
/// ([`BM25Config::bucket_overload_size`]), never correctness, so failures —
/// which cannot happen for the plain integer/string shapes this index
/// serializes — degrade to `0` instead of panicking on the insert/remove
/// hot path.
fn cbor_serialized_size<T: ?Sized + Serialize>(value: &T) -> usize {
    cbor2::serialized_size(value)
        .ok()
        .and_then(|size| usize::try_from(size).ok())
        .unwrap_or(0)
}

/// Identifies one durable bucket object.
///
/// A bucket's content is stored in immutable, generation-suffixed objects.
/// `generation == 0` refers to the legacy (pre-manifest) object that was
/// keyed by bucket id alone; generations `>= 1` are produced by the manifest
/// protocol and each flush writes replaced buckets to a **new** generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketObject {
    /// Stable bucket id.
    pub bucket_id: u32,
    /// Object generation; `0` denotes the legacy un-suffixed object.
    pub generation: u64,
}

/// Result of a [`BM25Index::flush`] / [`BM25Index::flush_with`] call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Whether anything (metadata and possibly buckets) was persisted.
    pub saved: bool,
    /// Bucket objects that the newly committed manifest no longer references.
    /// The caller should delete them best-effort; a failed deletion only
    /// leaks storage space and never affects future loads.
    pub obsolete: Vec<BucketObject>,
}

/// A serialized bucket captured at one flush boundary.
struct BucketSnapshot {
    bucket_id: u32,
    version: u64,
    buf: Vec<u8>,
}

/// Concurrent, bucket-sharded full-text index using BM25 scoring.
///
/// The index keeps its in-memory state in a handful of `DashMap`s so that
/// inserts, deletes and searches can run concurrently from many threads.
/// Persistence is split into two parts:
///
/// * **Metadata** — name, configuration, statistics and the *bucket manifest*
///   mapping every live bucket id to the generation of its current durable
///   object. Committing the metadata is the single atomic point of a flush.
/// * **Buckets** — the actual postings and per-document token counts. Each
///   token is assigned to exactly one *bucket*, a self-contained CBOR blob
///   whose serialized size is bounded by [`BM25Config::bucket_overload_size`].
///   Only buckets whose `dirty_version` has advanced past their
///   `saved_version` are re-written on [`flush`], which makes repeated flushes
///   cheap even for large indices.
///
/// # Concurrency contract
///
/// Concurrent `insert`/`remove`/`search` calls are safe, and so is running
/// [`compact_buckets`](Self::compact_buckets) alongside them: compaction
/// rebuilds the bucket map non-atomically, so it holds an internal mutation
/// gate exclusively while mutations hold it shared. Coordinating mutations
/// against [`flush`]/[`flush_with`], and flushes against each other or against
/// compaction, is the **caller's** responsibility (`anda_db`'s `Collection`
/// holds an exclusive operation gate across every flush). Running a flush
/// concurrently with mutations, or two flushes concurrently, is unsupported.
/// A single writer per durable index is a deployment contract.
///
/// [`flush`]: Self::flush
/// [`flush_with`]: Self::flush_with
pub struct BM25Index<T: Tokenizer + Clone> {
    /// Index name
    name: String,

    /// Tokenizer used to process text
    tokenizer: T,

    /// BM25 algorithm parameters
    config: BM25Config,

    /// Maps document IDs to their token counts
    doc_tokens: DashMap<u64, usize>,

    /// Buckets store information about where posting entries are stored and their current state
    buckets: DashMap<u32, Bucket>,

    /// Inverted index mapping tokens to (bucket id, Vec<(document_id, term_frequency)>)
    postings: DashMap<String, PostingValue>,

    /// Index metadata.
    metadata: RwLock<BM25Metadata>,

    /// Maximum bucket ID currently in use
    max_bucket_id: AtomicU32,

    /// Maximum document ID currently in use
    max_document_id: AtomicU64,

    /// Total number of tokens indexed. The average document length is derived
    /// from it and `doc_tokens.len()` on demand (see
    /// [`avg_doc_tokens`](BM25Index::avg_doc_tokens)); caching the quotient
    /// only created a value that could disagree with its own inputs.
    total_tokens: AtomicU64,

    /// Number of search operations performed.
    search_count: AtomicU64,

    /// Last saved version of the index
    last_saved_version: AtomicU64,

    /// Held *shared* by every synchronous mutation and *exclusively* by
    /// [`BM25Index::compact_buckets`], which rebuilds the whole bucket map
    /// non-atomically: a posting created after compaction snapshotted
    /// `postings` would otherwise be re-binned into nothing and silently lost
    /// on the next flush. Mutations still run concurrently with each other —
    /// they only take the shared side — and this is the first lock a mutation
    /// acquires, so it never nests inside a DashMap shard guard.
    mutation_gate: RwLock<()>,
}

#[derive(Default)]
struct Bucket {
    /// Version counter incremented on each modification
    dirty_version: u64,
    /// Version that was last successfully persisted
    saved_version: u64,
    // Current size of the bucket in bytes
    size: usize,
    // List of tokens stored in this bucket
    tokens: UniqueVec<String>,
    // Set of document IDs associated with this bucket
    doc_ids: FxHashSet<u64>,
}

impl Bucket {
    #[inline]
    fn is_dirty(&self) -> bool {
        self.dirty_version > self.saved_version
    }

    #[inline]
    fn mark_dirty(&mut self) {
        self.dirty_version += 1;
    }
}

/// Parameters controlling the BM25 scoring formula.
///
/// BM25 ranks a document `d` against a multi-term query `q` as:
///
/// ```text
/// score(d, q) = Σ_{t ∈ q} idf(t) · (tf · (k1 + 1))
///                                 / (tf + k1 · (1 − b + b · |d| / avgdl))
/// ```
///
/// - `k1` controls **term frequency saturation**. Larger values give more
///   weight to repeated occurrences of a term. Typical values: `1.2..=2.0`.
/// - `b` controls **document length normalization**. `0.0` disables length
///   normalization; `1.0` applies full normalization. Typical value: `0.75`.
///
/// Values outside their natural ranges are clamped at scoring time
/// (`k1` to `[0, `[`BM25Params::MAX_K1`]`]`, `b` to `[0, 1]`, non-finite values
/// back to the defaults) to avoid producing `NaN`/`inf` scores.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BM25Params {
    /// Term-frequency saturation factor.
    ///
    /// Higher values make repeated occurrences of a term contribute more to
    /// the score. Typical values are in the `1.2..=2.0` range.
    pub k1: f32,
    /// Document-length normalization factor.
    ///
    /// `0.0` disables length normalization; `1.0` applies full normalization.
    /// The usual BM25 default is `0.75`.
    pub b: f32,
}

impl Default for BM25Params {
    /// Returns default BM25 parameters (`k1 = 1.2`, `b = 0.75`) which work well
    /// for most use cases.
    fn default() -> Self {
        BM25Params { k1: 1.2, b: 0.75 }
    }
}

impl BM25Params {
    /// Largest `k1` honored at scoring time.
    ///
    /// Term-frequency saturation is already effectively linear far below this
    /// value, while an arbitrary finite `k1` (`f32::MAX` passes an `is_finite`
    /// check) overflows `tf + k1 · (1 − b + b · |d| / avgdl)` to `inf` and
    /// turns the score into `inf / inf = NaN`. Clamping here removes no useful
    /// ranking behavior and keeps the formula finite for any document length
    /// that can exist in memory.
    pub const MAX_K1: f32 = 1_000.0;

    /// Returns `(k1, b)` clamped into the domain where the BM25 formula is
    /// guaranteed to stay finite: non-finite values fall back to the defaults,
    /// `k1` to `[0, MAX_K1]` and `b` to `[0, 1]`.
    ///
    /// Parameters reach scoring straight from deserialized queries, so this
    /// runs per scored term instead of trusting the caller.
    fn sanitized(&self) -> (f32, f32) {
        let defaults = Self::default();
        let k1 = if self.k1.is_finite() {
            self.k1.clamp(0.0, Self::MAX_K1)
        } else {
            defaults.k1
        };
        let b = if self.b.is_finite() {
            self.b.clamp(0.0, 1.0)
        } else {
            defaults.b
        };
        (k1, b)
    }
}

/// Top-level configuration of a [`BM25Index`].
///
/// * `bm25` — the scoring parameters, see [`BM25Params`].
/// * `bucket_overload_size` — the soft upper bound, in bytes of the serialized
///   CBOR payload, of a single bucket. When inserting a new token would push a
///   bucket past this limit the token is routed to a fresh bucket instead.
///   Smaller values produce more, smaller buckets (cheaper incremental flushes
///   but more I/O per full reload); larger values do the opposite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BM25Config {
    /// BM25 scoring parameters used for all query scoring.
    pub bm25: BM25Params,
    /// Maximum size of a bucket before creating a new one
    /// When a bucket's stored data exceeds this size,
    /// a new bucket should be created for new data
    pub bucket_overload_size: usize,
}

impl Default for BM25Config {
    /// Returns a default configuration with [`BM25Params::default`] and a
    /// 512 KiB bucket size limit.
    fn default() -> Self {
        BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 1024 * 512,
        }
    }
}

/// Type alias for posting values: (bucket id, Vec<(document_id, token_frequency)>)
/// - bucket_id: The bucket where this posting is stored
/// - Vec<(document_id, token_frequency)>: List of documents and their term frequencies
pub type PostingValue = (u32, UniqueVec<(u64, usize)>);

/// Index metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BM25Metadata {
    /// Index name.
    pub name: String,

    /// BM25 algorithm parameters
    pub config: BM25Config,

    /// Index statistics.
    pub stats: BM25Stats,

    /// Bucket manifest: `bucket_id -> generation` of the durable object that
    /// currently holds the bucket's content (`0` = legacy un-suffixed object).
    ///
    /// The manifest is the loader's single source of truth: a token or
    /// posting exists only in the bucket objects it references. Metadata
    /// persisted before the manifest protocol deserializes with an empty map,
    /// which selects the legacy bucket-id-scan load path.
    #[serde(default)]
    pub buckets: BTreeMap<u32, u64>,
}

/// Index statistics.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BM25Stats {
    /// Last insertion timestamp (unix ms).
    pub last_inserted: u64,

    /// Last deletion timestamp (unix ms).
    pub last_deleted: u64,

    /// Last saved timestamp (unix ms).
    pub last_saved: u64,

    /// Updated version for the index. It will be incremented when the index is updated.
    pub version: u64,

    /// Number of elements in the index.
    pub num_elements: u64,

    /// Number of search operations performed.
    pub search_count: u64,

    /// Number of insert operations performed.
    pub insert_count: u64,

    /// Number of delete operations performed.
    pub delete_count: u64,

    /// Maximum bucket ID currently in use
    pub max_bucket_id: u32,

    /// Maximum document ID currently in use
    pub max_document_id: u64,

    /// Average number of tokens per document
    pub avg_doc_tokens: f32,
}

/// Serializable BM25 index structure (owned version).
#[derive(Clone, Serialize, Deserialize)]
struct BM25IndexOwned {
    // postings: DashMap<String, PostingValue>,
    metadata: BM25Metadata,
}

#[derive(Clone, Serialize)]
struct BM25IndexRef<'a> {
    // postings: &'a DashMap<String, PostingValue>,
    metadata: &'a BM25Metadata,
}

// Helper structure for serialization and deserialization of bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BucketOwned {
    #[serde(rename = "p")]
    postings: FxHashMap<String, PostingValue>,

    #[serde(rename = "d")]
    doc_tokens: FxHashMap<u64, usize>,
}

// Reference structure for serializing bucket
#[derive(Serialize)]
struct BucketRef<'a> {
    #[serde(rename = "p")]
    postings: &'a FxHashMap<&'a String, dashmap::mapref::one::Ref<'a, String, PostingValue>>,

    #[serde(rename = "d")]
    doc_tokens: &'a FxHashMap<u64, usize>,
}

impl<T> BM25Index<T>
where
    T: Tokenizer + Clone,
{
    /// Creates a new empty BM25 index with the given tokenizer and optional config.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the index
    /// * `tokenizer` - Tokenizer to use for processing text
    /// * `config` - Optional BM25 configuration parameters
    ///
    /// # Returns
    ///
    /// * `BM25Index` - A new instance of the BM25 index
    pub fn new(name: String, tokenizer: T, config: Option<BM25Config>) -> Self {
        let config = config.unwrap_or_default();
        let stats = BM25Stats {
            version: 1,
            ..Default::default()
        };
        BM25Index {
            name: name.clone(),
            tokenizer,
            config: config.clone(),
            doc_tokens: DashMap::new(),
            postings: DashMap::new(),
            buckets: DashMap::from_iter([(0, Bucket::default())]),
            metadata: RwLock::new(BM25Metadata {
                name,
                config,
                stats,
                buckets: BTreeMap::new(),
            }),
            max_bucket_id: AtomicU32::new(0),
            max_document_id: AtomicU64::new(0),
            total_tokens: AtomicU64::new(0),
            search_count: AtomicU64::new(0),
            last_saved_version: AtomicU64::new(0),
            mutation_gate: RwLock::new(()),
        }
    }

    /// Loads a complete index (metadata and all buckets) in one call.
    ///
    /// This is a convenience wrapper around [`load_metadata`](Self::load_metadata)
    /// followed by [`load_buckets`](Self::load_buckets).
    ///
    /// # Arguments
    ///
    /// * `tokenizer` — tokenizer to attach to the loaded index. It does not
    ///   need to be identical to the one originally used, but queries will
    ///   only be meaningful if the tokenization is compatible.
    /// * `metadata` — reader positioned at the start of the CBOR metadata blob.
    /// * `f` — async function invoked once per referenced [`BucketObject`];
    ///   return `Ok(Some(bytes))` for present buckets or `Ok(None)` to skip.
    ///
    /// # Returns
    ///
    /// The fully-loaded index, or a [`BM25Error`] if metadata could not be
    /// parsed or a bucket failed to load.
    pub async fn load_all<R: Read, F>(tokenizer: T, metadata: R, f: F) -> Result<Self, BM25Error>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut index = Self::load_metadata(tokenizer, metadata)?;
        index.load_buckets(f).await?;
        Ok(index)
    }

    /// Loads only the index metadata, returning an empty shell.
    ///
    /// The returned index contains the correct configuration, statistics and
    /// id watermarks, but no postings or `doc_tokens`. Call
    /// [`load_buckets`](Self::load_buckets) afterwards to populate the inverted
    /// index (possibly on demand, or only for a subset of buckets).
    pub fn load_metadata<R: Read>(tokenizer: T, r: R) -> Result<Self, BM25Error> {
        let index: BM25IndexOwned =
            cbor2::from_reader(r).map_err(|err| BM25Error::Serialization {
                name: "unknown".to_string(),
                source: err.into(),
            })?;
        let max_bucket_id = AtomicU32::new(index.metadata.stats.max_bucket_id);
        let max_document_id = AtomicU64::new(index.metadata.stats.max_document_id);
        let search_count = AtomicU64::new(index.metadata.stats.search_count);
        let last_saved_version = AtomicU64::new(index.metadata.stats.version);

        Ok(BM25Index {
            name: index.metadata.name.clone(),
            tokenizer,
            config: index.metadata.config.clone(),
            doc_tokens: DashMap::new(),
            postings: DashMap::new(),
            buckets: DashMap::from_iter([(0, Bucket::default())]),
            metadata: RwLock::new(index.metadata),
            max_bucket_id,
            max_document_id,
            search_count,
            last_saved_version,
            // No document is loaded yet; `load_buckets` seeds this from the
            // documents it actually loads. The persisted
            // `stats.avg_doc_tokens` is not carried over — it would disagree
            // with an empty `doc_tokens` until then.
            total_tokens: AtomicU64::new(0),
            mutation_gate: RwLock::new(()),
        })
    }

    /// Populates the inverted index from previously persisted buckets.
    ///
    /// Intended to be called right after [`load_metadata`](Self::load_metadata).
    /// When the loaded metadata carries a bucket manifest, `f` is invoked once
    /// per referenced [`BucketObject`]; without a manifest (data persisted by
    /// a pre-manifest release) every bucket id in `0..=max_bucket_id` is
    /// probed at generation `0` (the legacy object). Returning `Ok(None)`
    /// leaves that bucket empty, which allows read-only partial loads; a
    /// partially loaded index must not be flushed, since a flush persists
    /// exactly the loaded content.
    ///
    /// After this call, `total_tokens` — and therefore the average document
    /// length derived from it — reflects exactly the documents that were
    /// loaded.
    ///
    /// Posting entries that reference a document with no token count in any
    /// loaded bucket are pruned and the affected buckets are marked dirty, so
    /// the next [`flush`](Self::flush) persists the cleanup. The same applies
    /// to a token present in more than one legacy bucket object (a leftover
    /// of the pre-manifest flush protocol): the copy in the highest-numbered
    /// bucket wins and the stale copy is dropped.
    pub async fn load_buckets<F>(&mut self, mut f: F) -> Result<(), BM25Error>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut doc_token_lengths: FxHashMap<u64, usize> = self
            .doc_tokens
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();

        let manifest = { self.metadata.read().buckets.clone() };
        let legacy = manifest.is_empty();
        let objects: Vec<BucketObject> = if legacy {
            (0..=self.max_bucket_id.load(Ordering::Relaxed))
                .map(|bucket_id| BucketObject {
                    bucket_id,
                    generation: 0,
                })
                .collect()
        } else {
            manifest
                .iter()
                .map(|(bucket_id, generation)| BucketObject {
                    bucket_id: *bucket_id,
                    generation: *generation,
                })
                .collect()
        };

        let mut loaded_bucket_ids: Vec<u32> = Vec::new();
        for object in objects {
            let i = object.bucket_id;
            let data = f(object).await.map_err(|err| BM25Error::Generic {
                name: self.name.clone(),
                source: err,
            })?;
            if data.is_none() && !legacy {
                // The manifest references this bucket but the caller skipped
                // (or lost) its object. Keep an empty placeholder so the next
                // flush carries the manifest entry forward instead of
                // silently dropping the durable object.
                self.buckets.entry(i).or_default();
                continue;
            }
            if let Some(data) = data {
                loaded_bucket_ids.push(i);
                let bucket: BucketOwned =
                    cbor2::from_reader(&data[..]).map_err(|err| BM25Error::Serialization {
                        name: self.name.clone(),
                        source: err.into(),
                    })?;

                let mut b = Bucket {
                    size: data.len(),
                    ..Default::default()
                };
                if !bucket.doc_tokens.is_empty() {
                    b.doc_ids = bucket.doc_tokens.keys().cloned().collect();
                    for (doc_id, token_count) in bucket.doc_tokens {
                        doc_token_lengths.insert(doc_id, token_count);
                    }
                }

                if !bucket.postings.is_empty() {
                    for (token, mut posting) in bucket.postings {
                        // The bucket file path is the source of truth for ownership.
                        // If a stale lower-numbered bucket is still present after a
                        // partial flush, later buckets win and the old bucket is
                        // marked dirty so the stale token is removed on the next flush.
                        posting.0 = i;
                        if let Some(previous) = self.postings.insert(token.clone(), posting) {
                            let previous_bucket_id = previous.0;
                            if previous_bucket_id != i
                                && let Some(mut previous_bucket) =
                                    self.buckets.get_mut(&previous_bucket_id)
                                && previous_bucket
                                    .tokens
                                    .swap_remove_if(|k| &token == k)
                                    .is_some()
                            {
                                let previous_size = cbor_serialized_size(&(&token, &previous)) + 2;
                                previous_bucket.size =
                                    previous_bucket.size.saturating_sub(previous_size);
                                previous_bucket.mark_dirty();
                            }
                        }

                        b.tokens.push(token);
                    }
                }

                self.buckets.insert(i, b);
            }
        }

        let mut doc_ids_by_bucket: FxHashMap<u32, FxHashSet<u64>> = FxHashMap::default();
        let mut loaded_doc_tokens: FxHashMap<u64, usize> = FxHashMap::default();
        let mut empty_tokens: Vec<(u32, String)> = Vec::new();
        let mut bucket_size_decrease: FxHashMap<u32, usize> = FxHashMap::default();

        for mut posting in self.postings.iter_mut() {
            let bucket_id = posting.0;
            let doc_ids = doc_ids_by_bucket.entry(bucket_id).or_default();
            // Prune entries whose document has no token length anywhere.
            // Buckets are self-contained (a bucket's doc_tokens cover every
            // document referenced by its postings), so after loading, an entry
            // without a token length can only be a stale leftover from a
            // remove() that was given non-original text. Dropping it here makes
            // the index self-healing on reload. Documents from buckets that
            // were intentionally skipped (partial load) are not affected.
            let mut removed_entries: Vec<(u64, usize)> = Vec::new();
            posting.1.retain(|entry| {
                if let Some(token_count) = doc_token_lengths.get(&entry.0) {
                    loaded_doc_tokens.insert(entry.0, *token_count);
                    doc_ids.insert(entry.0);
                    true
                } else {
                    removed_entries.push(*entry);
                    false
                }
            });

            if !removed_entries.is_empty() {
                let size_decrease = if posting.1.is_empty() {
                    empty_tokens.push((bucket_id, posting.key().clone()));
                    cbor_serialized_size(&(posting.key(), (bucket_id, &removed_entries))) + 2
                } else {
                    removed_entries
                        .iter()
                        .map(|entry| cbor_serialized_size(entry) + 2)
                        .sum()
                };
                *bucket_size_decrease.entry(bucket_id).or_default() += size_decrease;
            }
        }

        for (bucket_id, token) in empty_tokens {
            self.postings.remove(&token);
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.tokens.swap_remove_if(|k| k == &token);
            }
        }

        for (bucket_id, size_decrease) in bucket_size_decrease {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.size = bucket.size.saturating_sub(size_decrease);
                bucket.mark_dirty();
            }
        }

        self.doc_tokens.clear();
        self.doc_tokens.extend(loaded_doc_tokens);

        let bucket_ids: Vec<u32> = self.buckets.iter().map(|b| *b.key()).collect();
        for bucket_id in bucket_ids {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                let doc_ids = doc_ids_by_bucket.remove(&bucket_id).unwrap_or_default();
                if bucket.doc_ids != doc_ids {
                    bucket.doc_ids = doc_ids;
                    bucket.mark_dirty();
                }
            }
        }

        let total_tokens: usize = self.doc_tokens.iter().map(|r| *r.value()).sum();
        self.total_tokens
            .store(total_tokens as u64, Ordering::Relaxed);

        if legacy && !loaded_bucket_ids.is_empty() {
            // Record in memory where each loaded bucket's durable object
            // lives (generation 0 = legacy object). The next flush commits a
            // real manifest whose clean buckets keep referencing these legacy
            // objects until they are rewritten.
            self.update_metadata(|m| {
                m.buckets = loaded_bucket_ids.iter().map(|id| (*id, 0)).collect();
            });
        }

        Ok(())
    }

    /// Returns the number of documents in the index
    pub fn len(&self) -> usize {
        self.doc_tokens.len()
    }

    /// Returns whether the index is empty
    pub fn is_empty(&self) -> bool {
        self.doc_tokens.is_empty()
    }

    /// Returns the index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the index metadata
    pub fn metadata(&self) -> BM25Metadata {
        let mut metadata = self.metadata.read().clone();
        self.refresh_live_stats(&mut metadata.stats);
        metadata
    }

    /// Gets current statistics about the index
    ///
    /// # Returns
    ///
    /// * `IndexStats` - Current statistics
    pub fn stats(&self) -> BM25Stats {
        let mut stats = self.metadata.read().stats.clone();
        self.refresh_live_stats(&mut stats);
        stats
    }

    /// Overlays the live atomic counters onto a snapshot of the persisted
    /// statistics so callers always observe up-to-date values.
    fn refresh_live_stats(&self, stats: &mut BM25Stats) {
        stats.search_count = self.search_count.load(Ordering::Relaxed);
        stats.num_elements = self.doc_tokens.len() as u64;
        stats.max_bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
        stats.max_document_id = self.max_document_id.load(Ordering::Relaxed);
        stats.avg_doc_tokens = self.avg_doc_tokens();
    }

    /// Average number of tokens per document, derived on demand.
    ///
    /// Deriving instead of caching keeps the value consistent with its inputs
    /// by construction: a cached copy has to be resynchronized on every
    /// insert/remove and still disagrees with `total_tokens` in between (and
    /// after a `load_metadata` that has no documents yet). The division is
    /// performed once per query and once per `stats()` call, never per
    /// document.
    fn avg_doc_tokens(&self) -> f32 {
        let doc_count = self.doc_tokens.len();
        if doc_count == 0 {
            return 0.0;
        }
        self.total_tokens.load(Ordering::Relaxed) as f32 / doc_count as f32
    }

    /// Inserts a document into the index.
    ///
    /// The text is tokenized with a clone of the index's tokenizer; token
    /// frequencies and the document length (total token count) are then used
    /// to update the posting list and the `total_tokens` counter that the
    /// average document length is derived from. Updates to buckets are staged
    /// and then applied in a second phase so that at most one bucket is marked
    /// dirty per affected bucket.
    ///
    /// # Arguments
    ///
    /// * `id` — unique, caller-assigned document identifier.
    /// * `text` — document text to index.
    /// * `now_ms` — wall-clock time in milliseconds, stored in
    ///   `stats.last_inserted`.
    ///
    /// # Errors
    ///
    /// * [`BM25Error::TokenizeFailed`] if the tokenizer produces no tokens.
    /// * [`BM25Error::AlreadyExists`] if `id` is already present.
    ///
    /// # Concurrency
    ///
    /// Safe to call concurrently with other `insert`/`remove`/`search` calls.
    pub fn insert(&self, id: u64, text: &str, now_ms: u64) -> Result<(), BM25Error> {
        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        // Tokenize the document
        let token_freqs = {
            let mut tokenizer = self.tokenizer.clone();
            collect_tokens(&mut tokenizer, text, None)
        };

        // Count token frequencies
        if token_freqs.is_empty() {
            return Err(BM25Error::TokenizeFailed {
                name: self.name.clone(),
                id,
                text: text.to_string(),
            });
        }

        // Phase 1: Update the postings collection
        let bucket_id = self.max_bucket_id.load(Ordering::Acquire);
        let tokens: usize = token_freqs.values().sum();
        // buckets_to_update: FxHashMap<bucketid, FxHashMap<token, size_increase>>
        let mut buckets_to_update: FxHashMap<u32, FxHashMap<String, usize>> = FxHashMap::default();
        match self.doc_tokens.entry(id) {
            dashmap::Entry::Occupied(_) => {
                return Err(BM25Error::AlreadyExists {
                    name: self.name.clone(),
                    id,
                });
            }
            dashmap::Entry::Vacant(v) => {
                v.insert(tokens);
                let _ = self.max_document_id.fetch_max(id, Ordering::Relaxed);

                // The document's tokens are counted right after its
                // `doc_tokens` entry is published, so the two only disagree
                // inside this window; the average document length is derived
                // from them at read time and needs no separate synchronization.
                self.total_tokens
                    .fetch_add(tokens as u64, Ordering::Relaxed);

                // Update inverted index
                for (token, freq) in token_freqs {
                    match self.postings.entry(token.clone()) {
                        dashmap::Entry::Occupied(mut entry) => {
                            let val = (id, freq);
                            let e = entry.get_mut();
                            // `push` is a no-op when the exact (doc, freq) pair is
                            // already present (a stale entry left by a remove() with
                            // non-original text). Don't count its size again, but
                            // still mark the bucket dirty below so the refreshed
                            // doc_tokens snapshot gets persisted.
                            let size_increase = if e.1.push(val) {
                                cbor_serialized_size(&val) + 2
                            } else {
                                0
                            };
                            let b = buckets_to_update.entry(e.0).or_default();
                            b.insert(token, size_increase);
                        }
                        dashmap::Entry::Vacant(entry) => {
                            // Create new posting
                            let val = (bucket_id, vec![(id, freq)].into());
                            let size_increase =
                                cbor_serialized_size(&(&token, (bucket_id, &[(id, freq)]))) + 2;
                            entry.insert(val);
                            let b = buckets_to_update.entry(bucket_id).or_default();
                            b.insert(token, size_increase);
                        }
                    };
                }
            }
        }

        // Phase 2: Update bucket states
        // tokens_to_migrate: (old_bucket_id, token, size)
        let mut tokens_to_migrate: Vec<(u32, String, usize)> = Vec::new();
        for (bid, val) in buckets_to_update {
            let mut bucket = self.buckets.entry(bid).or_default();
            // Mark as dirty, needs to be persisted
            bucket.mark_dirty();
            let mut bucket_contains_doc = false;
            for (token, size) in val {
                if bucket.tokens.contains(&token) {
                    // Token already tracked in this bucket; just account for the new posting entry.
                    bucket.size += size;
                    bucket_contains_doc = true;
                } else if bucket.tokens.is_empty()
                    || bucket.size + size < self.config.bucket_overload_size
                {
                    bucket.tokens.push(token);
                    bucket.size += size;
                    bucket_contains_doc = true;
                } else {
                    tokens_to_migrate.push((bid, token, size));
                }
            }
            if bucket_contains_doc {
                bucket.doc_ids.insert(id);
            }
        }

        // Phase 3: Create new buckets if needed
        if !tokens_to_migrate.is_empty() {
            let mut next_bucket_id = self.max_bucket_id.fetch_add(1, Ordering::Release) + 1;

            for (old_bucket_id, token, size) in tokens_to_migrate {
                if let Some(mut posting) = self.postings.get_mut(&token) {
                    posting.0 = next_bucket_id;
                }

                if let Some(mut ob) = self.buckets.get_mut(&old_bucket_id)
                    && ob.tokens.swap_remove_if(|k| &token == k).is_some()
                {
                    ob.size = ob.size.saturating_sub(size);
                    ob.mark_dirty();
                }

                let mut next_new_bucket = false;
                {
                    let mut nb = self.buckets.entry(next_bucket_id).or_default();

                    if nb.tokens.is_empty() || nb.size + size < self.config.bucket_overload_size {
                        // Bucket has enough space, update directly
                        nb.mark_dirty();
                        nb.size += size;
                        nb.tokens.push(token.clone());
                        nb.doc_ids.insert(id);
                    } else {
                        // Bucket doesn't have enough space, need to migrate to the next bucket
                        next_new_bucket = true;
                    }
                }

                if next_new_bucket {
                    next_bucket_id = self.max_bucket_id.fetch_add(1, Ordering::Release) + 1;
                    // update the posting's bucket_id again
                    if let Some(mut posting) = self.postings.get_mut(&token) {
                        posting.0 = next_bucket_id;
                    }
                    let mut nb = self.buckets.entry(next_bucket_id).or_default();
                    nb.mark_dirty();
                    nb.size += size;
                    nb.tokens.push(token.clone());
                    nb.doc_ids.insert(id);
                }
            }
        }

        self.update_metadata(|m| {
            m.stats.version += 1;
            m.stats.last_inserted = now_ms;
            m.stats.insert_count += 1;
        });

        Ok(())
    }

    /// Removes a document from the index.
    ///
    /// The caller must provide the *original text* that was used on
    /// [`insert`](Self::insert); it is re-tokenized to identify which posting
    /// lists should drop this document. If the text does not match, postings
    /// may retain stale entries — searches still skip them because scoring
    /// filters by `doc_tokens` membership, and the stale entries are pruned
    /// the next time the index is loaded via
    /// [`load_buckets`](Self::load_buckets). For idempotent recovery, cleanup
    /// by `text` still runs when `id` is already absent from `doc_tokens`; in
    /// that case the method returns `false` and deletion statistics are not
    /// incremented again.
    ///
    /// # Arguments
    ///
    /// * `id` — identifier of the document to remove.
    /// * `text` — original text of the document.
    /// * `now_ms` — wall-clock time, stored in `stats.last_deleted`.
    ///
    /// # Returns
    ///
    /// * `true` if a document with the given id was found and removed.
    /// * `false` otherwise.
    pub fn remove(&self, id: u64, text: &str, now_ms: u64) -> bool {
        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        // Even when `doc_tokens` was already removed, continue through the
        // supplied text and bucket bookkeeping. Crash-replay may encounter a
        // prefix of an earlier remove, and the retry must still purge stale
        // postings without double-counting the logical deletion.
        let removed_tokens = self.doc_tokens.remove(&id).map(|(_k, v)| v);
        let was_present = removed_tokens.is_some();

        if let Some(removed_tokens) = removed_tokens {
            // Mirror of `insert`: the token counter follows the `doc_tokens`
            // entry it belongs to, and the average document length is derived
            // from the pair at read time.
            self.total_tokens
                .fetch_sub(removed_tokens as u64, Ordering::Relaxed);
        }

        // Tokenize the document
        let token_freqs = {
            let mut tokenizer = self.tokenizer.clone();
            collect_tokens(&mut tokenizer, text, None)
        };

        // buckets_to_update: FxHashMap<bucketid, FxHashMap<token, size_decrease>>
        let mut buckets_to_update: FxHashMap<u32, FxHashMap<String, usize>> = FxHashMap::default();
        // Remove from inverted index
        let mut maybe_empty_tokens: Vec<String> = Vec::new();
        for (token, _) in token_freqs {
            if let Some(mut posting) = self.postings.get_mut(&token) {
                // Remove every entry for this document. Duplicates can exist
                // when a previous remove() was given non-original text and the
                // document was re-inserted afterwards.
                let mut removed_vals: Vec<(u64, usize)> = Vec::new();
                while let Some(val) = posting.1.swap_remove_if(|&(idx, _)| idx == id) {
                    removed_vals.push(val);
                }
                if removed_vals.is_empty() {
                    continue;
                }

                let size_decrease = if posting.1.is_empty() {
                    maybe_empty_tokens.push(token.clone());
                    cbor_serialized_size(&(&token, (posting.0, &removed_vals))) + 2
                } else {
                    removed_vals
                        .iter()
                        .map(|val| cbor_serialized_size(val) + 2)
                        .sum()
                };
                let b = buckets_to_update.entry(posting.0).or_default();
                b.insert(token, size_decrease);
            }
        }

        // Drop empty postings atomically: a concurrent insert may have appended
        // a new entry after the guard above was released, in which case the
        // posting must survive. `remove_if` re-checks under the shard lock.
        let mut removed_postings: FxHashSet<String> =
            FxHashSet::with_capacity_and_hasher(maybe_empty_tokens.len(), FxBuildHasher);
        for token in maybe_empty_tokens {
            if self
                .postings
                .remove_if(&token, |_, posting| posting.1.is_empty())
                .is_some()
            {
                removed_postings.insert(token);
            }
        }

        for (bucket_id, val) in buckets_to_update {
            if let Some(mut b) = self.buckets.get_mut(&bucket_id) {
                // Mark as dirty, needs to be persisted
                b.mark_dirty();
                for (token, size_decrease) in val {
                    b.size = b.size.saturating_sub(size_decrease);
                    if removed_postings.contains(&token) {
                        // `removed_postings` is a snapshot: a concurrent insert
                        // may have re-created the posting in this very bucket
                        // afterwards. Only drop the token when the posting is
                        // genuinely gone or now owned by another bucket,
                        // otherwise no bucket would list it and `serialize_bucket`
                        // would silently lose the term.
                        let remove_from_bucket = match self.postings.get(&token) {
                            Some(posting) => posting.0 != bucket_id,
                            None => true,
                        };
                        if remove_from_bucket {
                            b.tokens.swap_remove_if(|k| &token == k);
                        }
                    }
                }
                b.doc_ids.remove(&id);
            }
        }

        // Other buckets may still reference this document in their serialized
        // doc_tokens (e.g. stale postings left by a remove() with non-original
        // text); mark them dirty so the next flush drops the reference.
        // Read-scan first to avoid write-locking every shard on each remove.
        let stale_buckets: Vec<u32> = self
            .buckets
            .iter()
            .filter(|bucket| bucket.doc_ids.contains(&id))
            .map(|bucket| *bucket.key())
            .collect();
        for bucket_id in stale_buckets {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id)
                && bucket.doc_ids.remove(&id)
            {
                bucket.mark_dirty();
            }
        }

        if was_present {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_deleted = now_ms;
                m.stats.delete_count += 1;
            });
        }

        was_present
    }

    /// Erases a set of document ids from the index **without their text**.
    ///
    /// [`remove`](Self::remove) needs the document's original text to know
    /// which posting lists mention the document. A repair path that lost the
    /// document body — `anda_db`'s `Collection::reconcile_storage`, which
    /// drops ids whose stored object vanished in a crash — has no text to
    /// give, so this method sweeps the inverted index instead and drops every
    /// posting entry whose document id is in `ids`.
    ///
    /// # Cost, and why there is no cheaper route
    ///
    /// One pass over every posting list: `O(distinct tokens + posting
    /// entries)`. The per-bucket `doc_ids` sets look like a document → bucket
    /// index that could narrow the sweep, but they are a best-effort
    /// dirty-tracking hint, not a reverse index: a [`remove`](Self::remove)
    /// given non-original text clears a document from `doc_ids` while leaving
    /// its posting entries behind (that is exactly the state
    /// [`load_buckets`](Self::load_buckets) self-heals), so `doc_ids` can
    /// *under*-report. A repair path must not trust the bookkeeping it exists
    /// to repair, hence the full sweep. That is acceptable here because this
    /// is a maintenance operation whose caller already enumerates the
    /// collection's entire document prefix — and because it takes a *set*, so
    /// N dead ids cost one pass rather than N.
    ///
    /// # Consistency
    ///
    /// Every counter is left exactly consistent with the surviving postings:
    ///
    /// * each purged id's `doc_tokens` entry is dropped and its token count
    ///   subtracted from `total_tokens`, so the average document length
    ///   derived from the two stays correct (a wrong average silently skews
    ///   every subsequent BM25 score);
    /// * bucket sizes are decremented by the same estimates
    ///   [`insert`](Self::insert) accumulated, and a token whose posting list
    ///   became empty is unlisted from its bucket;
    /// * every bucket whose serialized content mentioned a purged id is marked
    ///   dirty, so the purge survives a flush + reload instead of being
    ///   resurrected from a stale bucket object's `doc_tokens`.
    ///
    /// # Arguments
    ///
    /// * `ids` — document ids to erase.
    /// * `now_ms` — wall-clock time, stored in `stats.last_deleted`.
    ///
    /// # Returns
    ///
    /// The number of ids that were actually present in the index.
    ///
    /// # Concurrency
    ///
    /// Takes the mutation gate *shared*, exactly like `insert`/`remove`: safe
    /// alongside them and alongside searches, exclusive against
    /// [`compact_buckets`](Self::compact_buckets). Like every other mutation
    /// it must not run concurrently with a flush (see the [`BM25Index`]
    /// concurrency contract).
    pub fn purge_ids(&self, ids: &BTreeSet<u64>, now_ms: u64) -> usize {
        if ids.is_empty() {
            return 0;
        }

        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        // Phase 1: drop the document lengths. As in `insert`/`remove`, the
        // token counter follows the `doc_tokens` entries it accounts for.
        let mut removed_docs = 0usize;
        let mut removed_tokens = 0u64;
        for id in ids {
            if let Some((_, tokens)) = self.doc_tokens.remove(id) {
                removed_docs += 1;
                removed_tokens += tokens as u64;
            }
        }
        if removed_tokens > 0 {
            self.total_tokens
                .fetch_sub(removed_tokens, Ordering::Relaxed);
        }

        // Phase 2: sweep every posting list once, collecting bucket updates
        // instead of applying them, so no `postings` shard guard is held while
        // the `buckets` map is touched.
        let mut bucket_size_decrease: FxHashMap<u32, usize> = FxHashMap::default();
        let mut emptied_tokens: Vec<(u32, String)> = Vec::new();
        for mut posting in self.postings.iter_mut() {
            let bucket_id = posting.0;
            let mut removed_entries: Vec<(u64, usize)> = Vec::new();
            posting.1.retain(|entry| {
                if ids.contains(&entry.0) {
                    removed_entries.push(*entry);
                    false
                } else {
                    true
                }
            });
            if removed_entries.is_empty() {
                continue;
            }

            // Mirror of `remove`: the whole `(token, (bucket, entries))` tuple
            // when the posting disappears — that is what `insert` charged for
            // a brand-new token — and the per-entry cost otherwise.
            let size_decrease = if posting.1.is_empty() {
                emptied_tokens.push((bucket_id, posting.key().clone()));
                cbor_serialized_size(&(posting.key(), (bucket_id, &removed_entries))) + 2
            } else {
                removed_entries
                    .iter()
                    .map(|entry| cbor_serialized_size(entry) + 2)
                    .sum()
            };
            *bucket_size_decrease.entry(bucket_id).or_default() += size_decrease;
        }

        // Phase 3: drop the emptied posting lists atomically. A concurrent
        // insert may have appended an entry after the sweep released the shard
        // guard, in which case the posting must survive; `remove_if` re-checks
        // under the shard lock.
        let mut removed_postings: FxHashSet<String> =
            FxHashSet::with_capacity_and_hasher(emptied_tokens.len(), FxBuildHasher);
        for (_, token) in emptied_tokens.iter() {
            if self
                .postings
                .remove_if(token, |_, posting| posting.1.is_empty())
                .is_some()
            {
                removed_postings.insert(token.clone());
            }
        }

        // Phase 4: resize and dirty every bucket that owned an affected token.
        let mut purged_postings = !bucket_size_decrease.is_empty();
        for (bucket_id, size_decrease) in bucket_size_decrease {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.mark_dirty();
                bucket.size = bucket.size.saturating_sub(size_decrease);
            }
        }

        // Phase 5: unlist the tokens whose posting is genuinely gone.
        // `removed_postings` is a snapshot: a concurrent insert may have
        // re-created the posting, possibly in another bucket. Only drop the
        // token when no bucket claims it or a different one does, otherwise no
        // bucket would list it and `serialize_bucket` would lose the term.
        for (bucket_id, token) in emptied_tokens {
            if !removed_postings.contains(&token) {
                continue;
            }
            let unlist = match self.postings.get(&token) {
                Some(posting) => posting.0 != bucket_id,
                None => true,
            };
            if unlist && let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.tokens.swap_remove_if(|k| k == &token);
            }
        }

        // Phase 6: drop the purged ids from every bucket's doc-id set. A
        // bucket can still list one without owning a posting for it, and its
        // serialized `doc_tokens` would resurrect the id on reload. Read-scan
        // first so a purge that touches nothing does not write-lock every
        // shard; probe by `ids` (the dead set is small) rather than by
        // `doc_ids` (which can hold the whole collection).
        let stale_buckets: Vec<u32> = self
            .buckets
            .iter()
            .filter(|bucket| ids.iter().any(|id| bucket.doc_ids.contains(id)))
            .map(|bucket| *bucket.key())
            .collect();
        purged_postings |= !stale_buckets.is_empty();
        for bucket_id in stale_buckets {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                let before = bucket.doc_ids.len();
                bucket.doc_ids.retain(|id| !ids.contains(id));
                if bucket.doc_ids.len() != before {
                    bucket.mark_dirty();
                }
            }
        }

        if removed_docs > 0 || purged_postings {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_deleted = now_ms;
                m.stats.delete_count += removed_docs as u64;
            });
        }

        removed_docs
    }

    /// Searches the index and returns the highest-scoring documents.
    ///
    /// The query is tokenized with the index's tokenizer. Multiple tokens are
    /// treated as a disjunction (OR). Use [`search_advanced`](Self::search_advanced)
    /// for boolean expressions with `AND` / `OR` / `NOT` and parentheses.
    ///
    /// # Arguments
    ///
    /// * `query` — raw query text.
    /// * `top_k` — maximum number of results to return; `0` yields an empty vector.
    /// * `params` — override the default [`BM25Params`] for this call only.
    ///
    /// # Returns
    ///
    /// A vector of `(document_id, score)` pairs sorted by descending score.
    pub fn search(&self, query: &str, top_k: usize, params: Option<BM25Params>) -> Vec<(u64, f32)> {
        if top_k == 0 {
            return Vec::new();
        }

        let params = params.as_ref().unwrap_or(&self.config.bm25);
        let scored_docs = self.score_term(query.trim(), params);
        // Count only queries that actually reached scoring (`top_k == 0`
        // short-circuits above), matching the HNSW index's semantics.
        self.search_count.fetch_add(1, Ordering::Relaxed);

        Self::top_k_results(scored_docs, top_k)
    }

    /// Searches the index with a boolean query expression.
    ///
    /// Unlike [`search`](Self::search), the query string is first parsed by
    /// [`QueryType::parse`] and may contain `AND`, `OR`, `NOT` operators and
    /// parentheses. Operator precedence is `OR < AND < NOT`; multiple bare
    /// terms default to `OR`.
    ///
    /// # Arguments
    ///
    /// * `query` — e.g. `"(hello AND world) OR (rust AND NOT java)"`.
    /// * `top_k` — maximum number of results to return.
    /// * `params` — optional BM25 parameters override.
    ///
    /// # Returns
    ///
    /// A vector of `(document_id, score)` pairs sorted by descending score.
    pub fn search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Vec<(u64, f32)> {
        self.try_search_advanced(query, top_k, params)
            .unwrap_or_default()
    }

    /// Searches the index with a boolean query expression and resource guards.
    pub fn try_search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Result<Vec<(u64, f32)>, BM25Error> {
        if top_k == 0 {
            return Ok(Vec::new());
        }

        let query_expr = QueryType::try_parse(query).map_err(|source| BM25Error::Generic {
            name: self.name.clone(),
            source: source.into(),
        })?;
        if query_expr.may_materialize_not_complement()
            && self.doc_tokens.len() > MAX_NOT_COMPLEMENT_DOCS
        {
            return Err(BM25Error::Generic {
                name: self.name.clone(),
                source: format!(
                    "logical NOT complement over {} documents exceeds maximum {}",
                    self.doc_tokens.len(),
                    MAX_NOT_COMPLEMENT_DOCS
                )
                .into(),
            });
        }

        let params = params.as_ref().unwrap_or(&self.config.bm25);
        let scored_docs = self.execute_query(&query_expr, params, false);
        // Count only queries that actually reached scoring: `top_k == 0`,
        // parse failures and rejected NOT complements all return above,
        // matching the HNSW index's search_count semantics.
        self.search_count.fetch_add(1, Ordering::Relaxed);

        Ok(Self::top_k_results(scored_docs, top_k))
    }

    /// Extracts the top-k results from scored documents using partial sorting.
    /// Uses `select_nth_unstable_by` for O(n + k·log(k)) instead of O(n·log(n)).
    fn top_k_results(scored_docs: FxHashMap<u64, f32>, top_k: usize) -> Vec<(u64, f32)> {
        if top_k == 0 || scored_docs.is_empty() {
            return Vec::new();
        }

        let mut results: Vec<(u64, f32)> = scored_docs.into_iter().collect();
        if results.len() > top_k {
            results.select_nth_unstable_by(top_k - 1, Self::compare_scored_docs);
            results.truncate(top_k);
        }
        results.sort_unstable_by(Self::compare_scored_docs);
        results
    }

    /// Total order over scored documents: descending score, `NaN` last, ties
    /// broken by ascending document id.
    ///
    /// `partial_cmp(..).unwrap_or(Equal)` is **not** a total order once a
    /// single `NaN` is present (it degrades to id-order against the `NaN` while
    /// the other pairs stay score-ordered, which produces comparison cycles and
    /// can make `sort_unstable_by` / `select_nth_unstable_by` panic). Scoring
    /// sanitizes its parameters so a `NaN` should be impossible, but the sort
    /// must not depend on that: `total_cmp` orders every `f32` bit pattern, and
    /// the explicit `NaN` bucket keeps unscorable documents at the end.
    fn compare_scored_docs(a: &(u64, f32), b: &(u64, f32)) -> std::cmp::Ordering {
        match (a.1.is_nan(), b.1.is_nan()) {
            (true, true) => a.0.cmp(&b.0),
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            (false, false) => b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)),
        }
    }

    /// Execute a query expression, returning a mapping of document IDs to scores
    fn execute_query(
        &self,
        query: &QueryType,
        params: &BM25Params,
        negated_not: bool,
    ) -> FxHashMap<u64, f32> {
        match query {
            QueryType::Term(term) => self.score_term(term, params),
            QueryType::And(subqueries) => self.score_and(subqueries, params),
            QueryType::Or(subqueries) => self.score_or(subqueries, params),
            QueryType::Not(subquery) => self.score_not(subquery, params, negated_not),
        }
    }

    /// Scores a single term (or multi-term query text) using BM25.
    /// Accumulates scores directly without intermediate allocations.
    fn score_term(&self, term: &str, params: &BM25Params) -> FxHashMap<u64, f32> {
        if self.postings.is_empty() {
            return FxHashMap::default();
        }

        // Be defensive against invalid params to avoid NaNs/inf in ranking.
        let (k1, b) = params.sanitized();

        let mut tokenizer = self.tokenizer.clone();
        let query_terms = collect_tokens(&mut tokenizer, term, None);
        if query_terms.is_empty() {
            return FxHashMap::default();
        }

        let doc_count = self.doc_tokens.len() as f32;
        if doc_count == 0.0 {
            return FxHashMap::default();
        }

        let mut scores: FxHashMap<u64, f32> =
            FxHashMap::with_capacity_and_hasher(self.doc_tokens.len().min(1000), FxBuildHasher);
        let avg_doc_tokens = self.avg_doc_tokens().max(1.0);

        // Per-token dedup buffer, reused across query terms so a multi-term
        // query does not reallocate a fresh map for every term.
        let mut valid: FxHashMap<u64, (f32, f32)> = FxHashMap::default();
        for query_token in query_terms.keys() {
            if let Some(postings) = self.postings.get(query_token) {
                // Single-pass: collect doc_id -> (tf, doc_len) for valid documents
                // in one sweep over the postings.
                // Filter out deleted / not-loaded documents:
                // `remove()` depends on the caller providing original text; if they don't,
                // postings can become stale. Also, when only part of buckets are loaded,
                // postings might contain docs missing in `doc_tokens`.
                // Keyed by doc_id so a stale duplicate entry (left by a remove()
                // with non-original text followed by a re-insert) cannot be
                // scored twice or inflate the document frequency; the newest
                // (last) entry wins.
                valid.clear();
                valid.reserve(postings.1.len());
                for (doc_id, token_freq) in postings.1.iter() {
                    if let Some(v) = self.doc_tokens.get(doc_id) {
                        valid.insert(*doc_id, (*token_freq as f32, *v as f32));
                    }
                }

                if valid.is_empty() {
                    continue;
                }

                // Classic Okapi BM25: ln(1 + (N - df + 0.5)/(df + 0.5))
                let df = valid.len() as f32;
                let idf = ((doc_count - df + 0.5) / (df + 0.5) + 1.0).ln();

                // Compute BM25 score for each valid document. `drain` empties the
                // map while keeping its allocation for the next query term.
                for (doc_id, (tf, doc_len)) in valid.drain() {
                    let tf_component =
                        (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * doc_len / avg_doc_tokens));
                    *scores.entry(doc_id).or_default() += idf * tf_component;
                }
            }
        }

        scores
    }

    /// Scores an OR query
    fn score_or(&self, subqueries: &[Box<QueryType>], params: &BM25Params) -> FxHashMap<u64, f32> {
        if subqueries.is_empty() {
            return FxHashMap::default();
        }
        if subqueries.len() == 1 {
            return self.execute_query(&subqueries[0], params, false);
        }

        // Execute all subqueries and merge results
        let mut result = FxHashMap::default();
        for subquery in subqueries {
            let sub_result = self.execute_query(subquery, params, false);

            for (doc_id, score) in sub_result {
                *result.entry(doc_id).or_insert(0.0) += score;
            }
        }

        result
    }

    /// Scores an AND query
    fn score_and(&self, subqueries: &[Box<QueryType>], params: &BM25Params) -> FxHashMap<u64, f32> {
        if subqueries.is_empty() {
            return FxHashMap::default();
        }
        if subqueries.len() == 1 {
            return self.execute_query(&subqueries[0], params, false);
        }

        // Evaluate non-NOT subqueries first so that a leading NOT does not
        // force building the full complement document set: `NOT a AND b`
        // takes the same cheap path as `b AND NOT a`. The result is
        // order-independent (intersection then subtraction).
        let (positives, negatives): (Vec<&QueryType>, Vec<&QueryType>) = subqueries
            .iter()
            .map(|q| q.as_ref())
            .partition(|q| !matches!(q, QueryType::Not(_)));

        let mut result = if let Some((&first, _)) = positives.split_first() {
            self.execute_query(first, params, false)
        } else {
            // All subqueries are NOT: start from the complement of the first
            // and subtract the rest below.
            self.execute_query(negatives[0], params, false)
        };

        // Intersect the remaining positive subqueries, merging scores.
        for &subquery in positives.iter().skip(1) {
            if result.is_empty() {
                return result;
            }
            let sub_result = self.execute_query(subquery, params, false);

            // Keep only documents present in both results, summing their scores
            // in a single pass over the (already intersected, smaller) result.
            result.retain(|doc_id, score| {
                if let Some(sub_score) = sub_result.get(doc_id) {
                    *score += *sub_score;
                    true
                } else {
                    false
                }
            });
        }

        // Subtract documents matching the negated subqueries.
        let skip_negatives = if positives.is_empty() { 1 } else { 0 };
        for &subquery in negatives.iter().skip(skip_negatives) {
            if result.is_empty() {
                return result;
            }
            let excluded = self.execute_query(subquery, params, true);
            for doc_id in excluded.keys() {
                result.remove(doc_id);
            }
        }

        result
    }

    /// Scores a NOT query.
    ///
    /// The subquery is always evaluated in normal (non-negated) mode;
    /// `negated_not` only selects what to return: the matching documents
    /// (the AND caller subtracts them) or their complement. Evaluating the
    /// subquery with the parent's negation flag would mis-handle double
    /// negation such as `a AND NOT (NOT b)`.
    fn score_not(
        &self,
        subquery: &QueryType,
        params: &BM25Params,
        negated_not: bool,
    ) -> FxHashMap<u64, f32> {
        let exclude = self.execute_query(subquery, params, false);
        if negated_not {
            return exclude;
        }

        let mut result = FxHashMap::default();
        for entry in self.doc_tokens.iter() {
            let doc_id = *entry.key();
            if !exclude.contains_key(&doc_id) {
                result.insert(doc_id, 0.0);
            }
        }
        result
    }

    /// Persists metadata and every currently-dirty bucket.
    ///
    /// This is a convenience wrapper around [`flush_with`](Self::flush_with)
    /// that writes the metadata blob to `metadata`; see `flush_with` for the
    /// manifest commit protocol.
    ///
    /// # Arguments
    ///
    /// * `metadata` — writer that receives the CBOR-encoded metadata blob.
    /// * `now_ms` — wall-clock time stored in `stats.last_saved`.
    /// * `f` — async function used to persist each dirty bucket.
    ///
    /// # Returns
    ///
    /// See [`flush_with`](Self::flush_with).
    pub async fn flush<W: Write, F, Fut>(
        &self,
        metadata: W,
        now_ms: u64,
        f: F,
    ) -> Result<FlushOutcome, BM25Error>
    where
        F: FnMut(BucketObject, Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        self.flush_with(
            now_ms,
            move |data: Vec<u8>| {
                let mut metadata = metadata;
                async move {
                    metadata.write_all(&data)?;
                    Ok(())
                }
            },
            f,
        )
        .await
    }

    /// Persists every dirty bucket to a new immutable object, then commits
    /// the metadata whose manifest references them.
    ///
    /// # Manifest commit protocol
    ///
    /// Every dirty bucket is serialized and written to a **fresh** object
    /// keyed by `(bucket_id, generation)` — the generation is this flush's
    /// metadata version, so a bucket object is never mutated in place once a
    /// committed manifest references it. The metadata (carrying the manifest
    /// `bucket_id -> generation`) is written last; that single write is the
    /// atomic commit point:
    ///
    /// * A crash or error **before** the metadata commit leaves the new
    ///   bucket objects as unreferenced garbage. A loader still sees the
    ///   previous manifest — a complete, consistent snapshot. Nothing is
    ///   lost; the next flush retries at a later generation.
    /// * **After** the commit, the objects replaced by this flush are
    ///   garbage. They are returned as [`FlushOutcome::obsolete`] for the
    ///   caller to delete best-effort; a failed deletion only leaks space.
    ///
    /// [`compact_buckets`](Self::compact_buckets) needs no special write
    /// ordering under this protocol: the repacked layout becomes visible
    /// atomically with the manifest commit, and every pre-compaction object
    /// is reported as obsolete.
    ///
    /// # Concurrency
    ///
    /// The caller must not run a flush concurrently with mutations,
    /// compaction, or another flush (see the [`BM25Index`] concurrency
    /// contract). Bucket payloads and the metadata are serialized before the
    /// first await, so callback latency never holds internal locks.
    ///
    /// # Arguments
    ///
    /// * `now_ms` — wall-clock time stored in `stats.last_saved`.
    /// * `metadata_f` — async function that durably persists the CBOR
    ///   metadata blob; it must return `Ok(())` only after the write is
    ///   durable, because it is the commit point.
    /// * `f` — async function invoked once per dirty bucket with the target
    ///   [`BucketObject`] and the CBOR payload. It must create/overwrite the
    ///   object addressed by `(bucket_id, generation)`.
    ///
    /// # Returns
    ///
    /// * `Ok(outcome)` with [`FlushOutcome::saved`] `== false` when the index
    ///   was already fully persisted (no callback was invoked).
    /// * `Ok(outcome)` with `saved == true` after a successful commit;
    ///   [`FlushOutcome::obsolete`] lists the replaced bucket objects.
    /// * `Err` on serialization failure or when any callback fails (nothing
    ///   was committed in that case).
    pub async fn flush_with<M, MFut, F, FFut>(
        &self,
        now_ms: u64,
        metadata_f: M,
        mut f: F,
    ) -> Result<FlushOutcome, BM25Error>
    where
        M: FnOnce(Vec<u8>) -> MFut,
        MFut: Future<Output = Result<(), BoxError>>,
        F: FnMut(BucketObject, Vec<u8>) -> FFut,
        FFut: Future<Output = Result<(), BoxError>>,
    {
        // Synchronous snapshot phase: serialize dirty buckets and the
        // manifest-bearing metadata before the first await.
        let has_dirty = self.has_dirty_buckets();
        if !has_dirty && !self.has_pending_metadata_flush() {
            return Ok(FlushOutcome::default());
        }

        // A bucket object only becomes reachable through the manifest, so
        // dirty buckets always require a metadata commit. Loading can mark
        // buckets dirty (stale-entry pruning) without bumping the stats
        // version; force a fresh version in that case.
        if has_dirty && !self.has_pending_metadata_flush() {
            self.update_metadata(|m| m.stats.version += 1);
        }

        let mut dirty = Vec::new();
        for (bucket_id, version) in self.collect_dirty_buckets() {
            if let Some(buf) = self.serialize_bucket(bucket_id)? {
                dirty.push(BucketSnapshot {
                    bucket_id,
                    version,
                    buf,
                });
            }
        }
        // Deterministic write order simplifies fault injection and traces.
        dirty.sort_unstable_by_key(|snapshot| snapshot.bucket_id);

        let mut meta = self.metadata();
        meta.stats.last_saved = now_ms.max(meta.stats.last_saved);
        // This flush's generation: unique per committed manifest because the
        // stats version increases monotonically and is claimed exactly once.
        let generation = meta.stats.version;

        // Build the new manifest: dirty buckets move to this generation,
        // clean buckets keep their committed object. In-memory buckets that
        // were never persisted (e.g. the empty initial bucket) stay out.
        let committed = meta.buckets.clone();
        let dirty_ids: FxHashSet<u32> = dirty.iter().map(|s| s.bucket_id).collect();
        let mut manifest = BTreeMap::new();
        for entry in self.buckets.iter() {
            let id = *entry.key();
            if dirty_ids.contains(&id) {
                manifest.insert(id, generation);
            } else if let Some(committed_generation) = committed.get(&id) {
                manifest.insert(id, *committed_generation);
            }
        }
        meta.buckets = manifest.clone();

        let mut meta_buf = Vec::with_capacity(256);
        cbor2::to_writer(&BM25IndexRef { metadata: &meta }, &mut meta_buf).map_err(|err| {
            BM25Error::Serialization {
                name: self.name.clone(),
                source: err.into(),
            }
        })?;

        // Objects the previous manifest referenced that the new one replaces
        // or drops (bucket rewrites, compaction leftovers, legacy objects).
        let obsolete: Vec<BucketObject> = committed
            .iter()
            .filter(|(id, generation)| manifest.get(id) != Some(generation))
            .map(|(id, generation)| BucketObject {
                bucket_id: *id,
                generation: *generation,
            })
            .collect();

        // NOTE: the callbacks are plain `FnMut`/`FnOnce` closures returning a
        // named future type and take owned `Vec<u8>` blobs: `AsyncFn*` bounds
        // here make the resulting future's `Send`-ness non-generalizable over
        // lifetimes (rustc: "implementation of `Send` is not general
        // enough"), which would break every downstream `tokio::spawn` of a
        // flush.

        // Phase 1: write every dirty bucket to its new immutable object.
        // Unreachable until the commit below, so any failure here leaves the
        // previous durable snapshot fully intact.
        let saved_marks: Vec<(u32, u64)> = dirty
            .iter()
            .map(|snapshot| (snapshot.bucket_id, snapshot.version))
            .collect();
        for snapshot in dirty {
            f(
                BucketObject {
                    bucket_id: snapshot.bucket_id,
                    generation,
                },
                snapshot.buf,
            )
            .await
            .map_err(|err| BM25Error::Generic {
                name: self.name.clone(),
                source: err,
            })?;
        }

        // Phase 2: the manifest commit — the single atomic point.
        metadata_f(meta_buf)
            .await
            .map_err(|err| BM25Error::Generic {
                name: self.name.clone(),
                source: err,
            })?;

        // Publish the committed state in memory.
        self.last_saved_version
            .fetch_max(generation, Ordering::Release);
        self.update_metadata(|m| {
            m.stats.last_saved = meta.stats.last_saved.max(m.stats.last_saved);
            m.buckets = manifest;
        });
        for (bucket_id, version) in saved_marks {
            self.mark_bucket_saved(bucket_id, version);
        }

        Ok(FlushOutcome {
            saved: true,
            obsolete,
        })
    }

    /// Returns whether there are dirty buckets pending persistence.
    pub fn has_dirty_buckets(&self) -> bool {
        self.buckets.iter().any(|b| b.is_dirty())
    }

    /// Returns whether metadata has a newer logical version than the last
    /// serialized metadata snapshot.
    pub fn has_pending_metadata_flush(&self) -> bool {
        let current_version = { self.metadata.read().stats.version };
        self.last_saved_version.load(Ordering::Acquire) < current_version
    }

    /// Repacks all tokens into a minimal set of buckets.
    ///
    /// Over the lifetime of an index — especially before bug fixes that tuned
    /// the bucket splitting logic — repeated inserts and removes can leave
    /// behind many under-filled buckets. `compact_buckets` estimates each
    /// posting's serialized CBOR size and performs a Best-Fit-Decreasing bin
    /// packing with [`BM25Config::bucket_overload_size`] as the bin capacity.
    ///
    /// After compaction:
    ///
    /// * bucket ids are reassigned to a contiguous `0..new_count` range;
    /// * every resulting bucket is marked dirty so the next
    ///   [`flush`](Self::flush) will rewrite the full on-disk layout.
    ///
    /// The operation runs in `O(n log n)` over the number of distinct tokens
    /// and requires a fully loaded index. It rebuilds the bucket map
    /// non-atomically, so it takes the index's mutation gate **exclusively**:
    /// concurrent `insert`/`remove` calls block for its duration instead of
    /// creating a posting that lands in no bucket at all (only bucket contents
    /// are serialized, so such a token would be silently dropped by the next
    /// flush). Excluding *flushes* remains the caller's responsibility (see
    /// the [`BM25Index`] concurrency contract). The repacked layout becomes
    /// durable atomically with the next flush's manifest commit; the
    /// pre-compaction bucket objects are reported in that flush's
    /// [`FlushOutcome::obsolete`].
    ///
    /// # Returns
    ///
    /// `(old_bucket_count, new_bucket_count)`.
    pub fn compact_buckets(&self) -> (usize, usize) {
        // Exclusive: no mutation may observe — or add to — the half-rebuilt
        // bucket map. Every mutator takes the shared side of this gate before
        // touching any other lock, so the ordering is uniform and deadlock-free.
        let _mutation_guard = self.mutation_gate.write();

        let old_count = self.buckets.len();
        if old_count <= 1 {
            return (old_count, old_count);
        }

        // Step 1: Estimate each token's serialized contribution.
        let mut token_sizes: Vec<(String, usize)> = self
            .postings
            .iter()
            .map(|entry| {
                let size = cbor_serialized_size(&(entry.key(), entry.value())) + 2;
                (entry.key().clone(), size)
            })
            .collect();

        if token_sizes.is_empty() {
            self.buckets.clear();
            self.buckets.insert(
                0,
                Bucket {
                    dirty_version: 1,
                    ..Default::default()
                },
            );
            self.max_bucket_id.store(0, Ordering::Relaxed);
            self.update_metadata(|m| {
                m.stats.version += 1;
            });
            return (old_count, 1);
        }

        // Step 2: Sort by size descending for better packing.
        token_sizes.sort_unstable_by_key(|b| std::cmp::Reverse(b.1));

        // Step 3: Best-fit-decreasing bin packing in O(n log n).
        // `by_remaining` maps remaining-capacity -> bin indices. We pick the bin with the
        // smallest remaining capacity that still fits the token (best fit), which keeps
        // bucket count low without scanning all bins per token.
        let limit = self.config.bucket_overload_size;
        // Each bin: (accumulated_size, tokens)
        let mut bins: Vec<(usize, Vec<String>)> = Vec::new();
        // remaining_capacity -> bin indices with that capacity
        let mut by_remaining: std::collections::BTreeMap<usize, Vec<usize>> =
            std::collections::BTreeMap::new();

        for (token, size) in token_sizes {
            // Find smallest remaining capacity >= size + 1 (preserve `<` limit semantics).
            let needed = size.saturating_add(1);
            let chosen = by_remaining
                .range_mut(needed..)
                .next()
                .and_then(|(_, idxs)| idxs.pop().map(|i| (i, idxs.is_empty())));

            match chosen {
                Some((idx, bucket_now_empty)) => {
                    let old_remaining = limit.saturating_sub(bins[idx].0);
                    if bucket_now_empty {
                        by_remaining.remove(&old_remaining);
                    }
                    bins[idx].0 += size;
                    bins[idx].1.push(token);
                    let new_remaining = limit.saturating_sub(bins[idx].0);
                    by_remaining.entry(new_remaining).or_default().push(idx);
                }
                None => {
                    let idx = bins.len();
                    bins.push((size, vec![token]));
                    let new_remaining = limit.saturating_sub(size);
                    by_remaining.entry(new_remaining).or_default().push(idx);
                }
            }
        }

        // Step 4: Rebuild buckets.
        self.buckets.clear();
        let new_count = bins.len();
        let max_id = new_count.saturating_sub(1) as u32;

        for (i, (size, tokens)) in bins.into_iter().enumerate() {
            let bucket_id = i as u32;

            // Update posting references and collect doc_ids.
            let mut doc_ids = FxHashSet::default();
            for token in &tokens {
                if let Some(mut posting) = self.postings.get_mut(token) {
                    posting.0 = bucket_id;
                    for (doc_id, _) in posting.1.iter() {
                        doc_ids.insert(*doc_id);
                    }
                }
            }

            self.buckets.insert(
                bucket_id,
                Bucket {
                    dirty_version: 1,
                    saved_version: 0,
                    size,
                    tokens: tokens.into(),
                    doc_ids,
                },
            );
        }

        self.max_bucket_id.store(max_id, Ordering::Relaxed);
        self.update_metadata(|m| {
            m.stats.version += 1;
        });

        (old_count, new_count)
    }

    /// Collects the ids and dirty-version snapshots of dirty buckets,
    /// releasing all DashMap iter locks before the caller starts making async
    /// persistence calls.
    fn collect_dirty_buckets(&self) -> Vec<(u32, u64)> {
        self.buckets
            .iter()
            .filter(|b| b.is_dirty())
            .map(|b| (*b.key(), b.dirty_version))
            .collect()
    }

    /// Serializes one bucket, dropping every DashMap guard before returning
    /// so no lock is held across the caller's async persistence call.
    /// Returns `Ok(None)` when the bucket no longer exists or is no longer
    /// dirty and should be skipped.
    fn serialize_bucket(&self, bucket_id: u32) -> Result<Option<Vec<u8>>, BM25Error> {
        let bucket = match self.buckets.get(&bucket_id) {
            Some(b) if b.is_dirty() => b,
            _ => return Ok(None),
        };

        let mut referenced_doc_ids = FxHashSet::default();
        let postings: FxHashMap<_, _> = bucket
            .tokens
            .iter()
            .filter_map(|k| {
                let posting = self.postings.get(k)?;
                if posting.0 != bucket_id {
                    return None;
                }
                for (doc_id, _) in posting.1.iter() {
                    referenced_doc_ids.insert(*doc_id);
                }
                Some((k, posting))
            })
            .collect();

        let doc_tokens: FxHashMap<_, _> = referenced_doc_ids
            .iter()
            .filter_map(|id| self.doc_tokens.get(id).map(|v| (*id, *v)))
            .collect();

        let mut buf = Vec::with_capacity(4096);
        cbor2::to_writer(
            &BucketRef {
                postings: &postings,
                doc_tokens: &doc_tokens,
            },
            &mut buf,
        )
        .map_err(|err| BM25Error::Serialization {
            name: self.name.clone(),
            source: err.into(),
        })?;
        Ok(Some(buf))
    }

    /// Records that `bucket_id` was persisted at `snapshot_version`.
    ///
    /// Uses version-based dirty tracking: only marks as saved up to the
    /// snapshot version, so a bucket modified concurrently after the snapshot
    /// stays dirty and is re-persisted on the next flush.
    fn mark_bucket_saved(&self, bucket_id: u32, snapshot_version: u64) {
        if let Some(mut b) = self.buckets.get_mut(&bucket_id) {
            b.saved_version = b.saved_version.max(snapshot_version);
        }
    }

    /// Gets the number of tokens for a document by its ID
    pub fn get_doc_tokens(&self, id: u64) -> Option<usize> {
        self.doc_tokens.get(&id).map(|v| *v)
    }

    /// Updates the index metadata
    ///
    /// # Arguments
    ///
    /// * `f` - Function that modifies the metadata
    fn update_metadata<F>(&self, f: F)
    where
        F: FnOnce(&mut BM25Metadata),
    {
        let mut metadata = self.metadata.write();
        f(&mut metadata);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::{self, Write};
    use std::sync::Arc;

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("writer failed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// In-memory model of the durable store: one metadata object plus one
    /// object per `(bucket_id, generation)`, mirroring the production
    /// object-store layout.
    #[derive(Default, Clone)]
    struct MemStore {
        metadata: Vec<u8>,
        buckets: HashMap<BucketObject, Vec<u8>>,
    }

    /// Flushes `index` into `store` the way the production adapter does:
    /// buckets first, then the manifest commit, then best-effort deletion of
    /// the obsolete objects.
    async fn flush_to(
        index: &BM25Index<TokenizerChain>,
        store: &mut MemStore,
        now_ms: u64,
    ) -> FlushOutcome {
        let mut meta_buf: Vec<u8> = Vec::new();
        let buckets = &mut store.buckets;
        let outcome = index
            .flush_with(
                now_ms,
                |data| {
                    meta_buf = data;
                    std::future::ready(Ok(()))
                },
                |object, data| {
                    buckets.insert(object, data);
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap();
        if outcome.saved {
            store.metadata = meta_buf;
            for object in &outcome.obsolete {
                store.buckets.remove(object);
            }
        }
        outcome
    }

    /// Loads a complete index from `store`.
    async fn load_from(store: &MemStore) -> BM25Index<TokenizerChain> {
        BM25Index::load_all(default_tokenizer(), &store.metadata[..], async |object| {
            Ok(store.buckets.get(&object).cloned())
        })
        .await
        .unwrap()
    }

    // 创建一个简单的测试索引
    fn create_test_index() -> BM25Index<TokenizerChain> {
        let index = BM25Index::new("anda_db_tfs_bm25".to_string(), default_tokenizer(), None);

        // 添加一些测试文档
        index
            .insert(1, "The quick brown fox jumps over the lazy dog", 0)
            .unwrap();
        index
            .insert(2, "A fast brown fox runs past the lazy dog", 0)
            .unwrap();
        index.insert(3, "The lazy dog sleeps all day", 0).unwrap();
        index
            .insert(4, "Quick brown foxes are rare in the wild", 0)
            .unwrap();

        index
    }

    fn encode_bucket_owned(
        postings: FxHashMap<String, PostingValue>,
        doc_tokens: FxHashMap<u64, usize>,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        cbor2::to_writer(
            &BucketOwned {
                postings,
                doc_tokens,
            },
            &mut buf,
        )
        .unwrap();
        buf
    }

    #[test]
    fn test_insert() {
        let index = create_test_index();
        assert_eq!(index.len(), 4);

        // 测试添加新文档
        index
            .insert(5, "A new document about cats and dogs", 0)
            .unwrap();
        assert_eq!(index.len(), 5);

        // 测试添加已存在的文档ID
        let result = index.insert(3, "This should fail", 0);
        assert!(matches!(
            result,
            Err(BM25Error::AlreadyExists { id: 3, .. })
        ));

        // 测试添加空文档
        let result = index.insert(6, "", 0);
        assert!(matches!(
            result,
            Err(BM25Error::TokenizeFailed { id: 6, .. })
        ));
    }

    #[tokio::test]
    async fn test_metadata_accessors_empty_compaction_and_writer_error_paths() {
        let load_result: Result<BM25Index<_>, _> =
            BM25Index::load_metadata(default_tokenizer(), &b"not cbor"[..]);
        assert!(matches!(load_result, Err(BM25Error::Serialization { .. })));

        let index = BM25Index::new("empty_bm25".to_string(), default_tokenizer(), None);
        assert_eq!(index.name(), "empty_bm25");
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
        assert!(index.has_pending_metadata_flush());
        assert_eq!(index.metadata().name, "empty_bm25");

        index.buckets.insert(1, Bucket::default());
        let (old_count, new_count) = index.compact_buckets();
        assert_eq!((old_count, new_count), (2, 1));
        assert_eq!(index.max_bucket_id.load(Ordering::Relaxed), 0);
        assert!(index.has_dirty_buckets());

        // A failing metadata writer surfaces as an error and commits nothing.
        let mut writer = FailingWriter;
        let err = index
            .flush(&mut writer, 123, |_, _| std::future::ready(Ok(())))
            .await
            .unwrap_err();
        assert!(matches!(err, BM25Error::Generic { .. }));
        assert!(index.has_pending_metadata_flush());
        assert!(index.has_dirty_buckets());
    }

    #[test]
    fn test_remove() {
        let index = create_test_index();
        assert_eq!(index.len(), 4);

        // 测试移除存在的文档
        let removed = index.remove(2, "A fast brown fox runs past the lazy dog", 0);
        assert!(removed);
        assert_eq!(index.len(), 3);

        // 测试移除不存在的文档
        let removed = index.remove(99, "This document doesn't exist", 0);
        assert!(!removed);
        assert_eq!(index.len(), 3);
    }

    /// Legacy (pre-manifest) data may contain the same token in two bucket
    /// objects — a leftover of the old multi-phase flush protocol. The legacy
    /// loader must keep the copy in the highest-numbered bucket, mark the
    /// stale bucket dirty, and the first manifest flush must persist the
    /// repaired layout.
    #[tokio::test]
    async fn test_legacy_load_reconciles_duplicate_token_bucket_ownership() {
        let index = BM25Index::new(
            "duplicate_token_load".to_string(),
            default_tokenizer(),
            Some(BM25Config {
                bm25: BM25Params::default(),
                bucket_overload_size: 64,
            }),
        );
        index.insert(1, "alpha", 0).unwrap();

        // Craft a legacy layout by hand: metadata without a manifest, bucket
        // objects at generation 0, and the token duplicated in buckets 0 and 1.
        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;
        let stale_bucket0 = store
            .buckets
            .values()
            .next()
            .expect("bucket 0 must be persisted")
            .clone();

        let mut metadata = index.metadata();
        metadata.stats.version += 1;
        metadata.stats.max_bucket_id = 1;
        metadata.buckets = BTreeMap::new(); // legacy: no manifest
        let mut metadata_buf = Vec::new();
        cbor2::to_writer(
            &BM25IndexRef {
                metadata: &metadata,
            },
            &mut metadata_buf,
        )
        .unwrap();

        let mut newer_postings = FxHashMap::default();
        newer_postings.insert("alpha".to_string(), (1, vec![(1, 1)].into()));
        let newer_bucket1 = encode_bucket_owned(newer_postings, FxHashMap::from_iter([(1, 1)]));

        let legacy_store = MemStore {
            metadata: metadata_buf,
            buckets: HashMap::from_iter([
                (
                    BucketObject {
                        bucket_id: 0,
                        generation: 0,
                    },
                    stale_bucket0,
                ),
                (
                    BucketObject {
                        bucket_id: 1,
                        generation: 0,
                    },
                    newer_bucket1,
                ),
            ]),
        };
        let loaded = load_from(&legacy_store).await;

        assert_eq!(loaded.postings.get("alpha").unwrap().0, 1);
        assert!(
            !loaded
                .buckets
                .get(&0)
                .unwrap()
                .tokens
                .contains(&"alpha".to_string())
        );
        assert!(loaded.has_dirty_buckets());

        // The first manifest flush persists the repaired layout and reports
        // the replaced legacy objects as obsolete.
        let mut store = legacy_store.clone();
        let outcome = flush_to(&loaded, &mut store, 2).await;
        assert!(outcome.saved);
        assert!(
            outcome
                .obsolete
                .iter()
                .any(|object| object.bucket_id == 0 && object.generation == 0),
            "the rewritten legacy bucket 0 must be reported obsolete: {outcome:?}"
        );

        let reloaded = load_from(&store).await;
        let results = reloaded.search("alpha", 10, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert!(!reloaded.has_dirty_buckets());
    }

    /// Data persisted by a pre-manifest release (metadata without a manifest,
    /// un-suffixed bucket objects) loads correctly, and the first flush
    /// upgrades the durable layout to the manifest format while retiring the
    /// rewritten legacy objects.
    #[tokio::test]
    async fn test_legacy_format_loads_and_upgrades_on_first_flush() {
        let config = BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 64,
        };
        let index = BM25Index::new(
            "legacy_upgrade".to_string(),
            default_tokenizer(),
            Some(config),
        );
        index.insert(1, "alpha bravo charlie", 0).unwrap();
        index
            .insert(2, "delta echo foxtrot golf hotel india", 0)
            .unwrap();
        index
            .insert(3, "juliet kilo lima mike november oscar", 0)
            .unwrap();
        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;
        assert!(
            store.buckets.len() > 1,
            "scenario must span several buckets"
        );

        // Transform the store into the legacy layout: strip the manifest from
        // the metadata and re-key every bucket object to generation 0.
        let mut legacy_meta = index.metadata();
        legacy_meta.buckets = BTreeMap::new();
        let mut metadata_buf = Vec::new();
        cbor2::to_writer(
            &BM25IndexRef {
                metadata: &legacy_meta,
            },
            &mut metadata_buf,
        )
        .unwrap();
        let legacy_store = MemStore {
            metadata: metadata_buf,
            buckets: store
                .buckets
                .iter()
                .map(|(object, data)| {
                    (
                        BucketObject {
                            bucket_id: object.bucket_id,
                            generation: 0,
                        },
                        data.clone(),
                    )
                })
                .collect(),
        };

        // Legacy data loads through the bucket-id-scan path.
        let loaded = load_from(&legacy_store).await;
        assert_eq!(loaded.len(), 3);
        for (doc, term) in [(1, "alpha"), (2, "hotel"), (3, "oscar")] {
            assert!(
                loaded
                    .search(term, 10, None)
                    .iter()
                    .any(|(id, _)| *id == doc),
                "doc {doc} must be found via '{term}' from the legacy layout"
            );
        }

        // Mutate and flush: the durable metadata upgrades to the manifest
        // format; rewritten legacy objects are reported obsolete.
        loaded.insert(4, "papa quebec romeo", 2).unwrap();
        let mut upgraded = legacy_store.clone();
        let outcome = flush_to(&loaded, &mut upgraded, 3).await;
        assert!(outcome.saved);
        for object in &outcome.obsolete {
            assert_eq!(
                object.generation, 0,
                "only replaced legacy objects may be obsolete here"
            );
        }

        let upgraded_meta = BM25Index::load_metadata(default_tokenizer(), &upgraded.metadata[..])
            .unwrap()
            .metadata();
        assert!(
            !upgraded_meta.buckets.is_empty(),
            "the first flush must commit a manifest"
        );

        let reloaded = load_from(&upgraded).await;
        assert_eq!(reloaded.len(), 4);
        for (doc, term) in [(1, "alpha"), (2, "hotel"), (3, "oscar"), (4, "papa")] {
            assert!(
                reloaded
                    .search(term, 10, None)
                    .iter()
                    .any(|(id, _)| *id == doc),
                "doc {doc} must be found via '{term}' after the format upgrade"
            );
        }
    }

    #[test]
    fn test_remove_with_wrong_text_does_not_leak_into_search() {
        let index = create_test_index();

        // remove() currently relies on caller providing the original text.
        // Even if postings are not fully cleaned, search must not return deleted documents.
        let removed = index.remove(2, "totally different text", 0);
        assert!(removed);
        assert_eq!(index.len(), 3);

        let results = index.search("fox", 10, None);
        assert!(!results.iter().any(|(id, _)| *id == 2));
    }

    #[tokio::test]
    async fn test_remove_with_wrong_text_does_not_resurrect_after_reload() {
        let config = BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 64,
        };
        let index = BM25Index::new(
            "remove_wrong_text_reload".to_string(),
            default_tokenizer(),
            Some(config),
        );
        let terms = [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
            "juliet", "kilo", "lima",
        ];
        let text = terms.join(" ");
        index.insert(1, &text, 0).unwrap();
        assert!(index.stats().max_bucket_id > 0);

        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;

        assert!(index.remove(1, "wrong text", 2));
        flush_to(&index, &mut store, 3).await;

        let loaded_index = load_from(&store).await;

        assert_eq!(loaded_index.len(), 0);
        for term in terms {
            assert!(
                loaded_index.search(term, 10, None).is_empty(),
                "removed document was found after reload for term '{term}'"
            );
        }

        // Loading prunes stale posting entries of deleted documents entirely,
        // and the resulting cleanup is flushed on the next store.
        assert!(
            loaded_index.postings.is_empty(),
            "stale postings must be pruned on load"
        );
        assert!(loaded_index.has_dirty_buckets());

        flush_to(&loaded_index, &mut store, 4).await;
        for data in store.buckets.values() {
            let bucket: BucketOwned = cbor2::from_reader(&data[..]).unwrap();
            assert!(bucket.postings.is_empty());
            assert!(bucket.doc_tokens.is_empty());
        }
    }

    #[test]
    fn test_search() {
        let index = create_test_index();

        // 测试基本搜索功能
        let results = index.search("fox", 10, None);
        assert_eq!(results.len(), 3); // 应该找到3个包含"fox"的文档

        // 检查结果排序 - 文档1和2应该排在前面，因为它们都包含"fox"
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 2));
        assert!(results.iter().any(|(id, _)| *id == 4));

        // 测试多词搜索
        let results = index.search("quick fox dog", 10, None);
        assert!(results[0].0 == 1); // 文档1应该排在最前面，因为它同时包含"quick", "fox", "dog"

        // 测试top_k限制
        let results = index.search("dog", 2, None);
        assert_eq!(results.len(), 2); // 应该只返回2个结果，尽管有3个文档包含"dog"

        // 测试空查询
        let results = index.search("", 10, None);
        assert_eq!(results.len(), 0);

        // 测试无匹配查询
        let results = index.search("elephant giraffe", 10, None);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_search_top_k_zero_returns_empty() {
        let index = create_test_index();

        let basic = index.search("fox", 0, None);
        assert!(basic.is_empty());

        let advanced = index.search_advanced("fox OR dog", 0, None);
        assert!(advanced.is_empty());
    }

    #[test]
    fn test_empty_index() {
        let tokenizer = default_tokenizer();
        let index = BM25Index::new("anda_db_tfs_bm25".to_string(), tokenizer, None);

        assert_eq!(index.len(), 0);
        assert!(index.is_empty());

        // 测试空索引的搜索
        let results = index.search("test", 10, None);
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_serialization() {
        let index = create_test_index();

        // 保存索引
        let mut store = MemStore::default();
        flush_to(&index, &mut store, 0).await;

        // 加载索引
        let loaded_index = load_from(&store).await;

        // 验证加载的索引
        assert_eq!(loaded_index.len(), index.len());

        // 验证搜索结果
        let mut original_results = index.search("fox", 10, None);
        let mut loaded_results = loaded_index.search("fox", 10, None);

        assert_eq!(original_results.len(), loaded_results.len());
        original_results.sort_by_key(|a| a.0);
        loaded_results.sort_by_key(|a| a.0);
        // 比较文档ID和分数（允许浮点数有小误差）
        for i in 0..original_results.len() {
            assert_eq!(original_results[i].0, loaded_results[i].0);
            assert!((original_results[i].1 - loaded_results[i].1).abs() < 0.001);
        }
    }

    /// A dirty bucket always forces a manifest commit — bucket objects are
    /// unreachable until the metadata references them. This covers the
    /// load-time repair path, which marks buckets dirty without bumping the
    /// stats version.
    #[tokio::test]
    async fn test_flush_commits_manifest_even_if_metadata_version_unchanged() {
        let index = create_test_index();
        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;
        assert!(!index.has_pending_metadata_flush());
        assert!(!index.has_dirty_buckets());

        // Simulate a load-time repair: dirty bucket, no version bump.
        index
            .buckets
            .get_mut(&0)
            .expect("bucket 0 exists")
            .mark_dirty();
        assert!(!index.has_pending_metadata_flush());
        assert!(index.has_dirty_buckets());

        let before = store.clone();
        let outcome = flush_to(&index, &mut store, 2).await;
        assert!(outcome.saved);
        assert!(!index.has_dirty_buckets());
        assert!(!index.has_pending_metadata_flush());
        assert_ne!(
            before.metadata, store.metadata,
            "the manifest commit must rewrite the metadata"
        );

        let reloaded = load_from(&store).await;
        assert_eq!(reloaded.len(), index.len());
    }

    #[tokio::test]
    async fn test_flush_does_not_commit_metadata_when_bucket_write_fails() {
        let index = create_test_index();
        assert!(index.has_pending_metadata_flush());
        assert!(index.has_dirty_buckets());

        // Bucket persistence fails: flush must NOT have committed the
        // manifest (buckets are written before the metadata commit).
        let mut metadata_buf = Vec::new();
        let err = index
            .flush(&mut metadata_buf, 1, async |_, _| {
                Err::<(), BoxError>("bucket write failed".into())
            })
            .await
            .unwrap_err();
        assert!(matches!(err, BM25Error::Generic { .. }));
        assert!(metadata_buf.is_empty());
        assert!(index.has_pending_metadata_flush());
        assert!(index.has_dirty_buckets());

        // The next flush retries both buckets and metadata.
        let mut metadata_buf = Vec::new();
        assert!(
            index
                .flush(&mut metadata_buf, 2, async |_, _| Ok(()))
                .await
                .unwrap()
                .saved
        );
        assert!(!metadata_buf.is_empty());
        assert!(!index.has_pending_metadata_flush());
        assert!(!index.has_dirty_buckets());
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FlushEvent {
        Bucket(u32),
        Metadata,
    }

    /// Builds the token-migration scenario used by the crash-window tests:
    /// doc 1 is fully persisted, then doc 2 both dirties doc 1's bucket (via
    /// the shared token "alpha") and overflows it so new buckets are
    /// allocated. Returns the index (holding unflushed doc 2) and the durable
    /// store holding the committed doc-1-only snapshot.
    async fn build_migration_scenario() -> (BM25Index<TokenizerChain>, MemStore) {
        let config = BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 64,
        };
        let index = BM25Index::new(
            "manifest_migration".to_string(),
            default_tokenizer(),
            Some(config),
        );
        index.insert(1, "alpha bravo", 0).unwrap();

        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;
        let old_max = index.stats().max_bucket_id;

        index
            .insert(
                2,
                "alpha xray yankee zulu whiskey victor uniform tango sierra",
                2,
            )
            .unwrap();
        assert!(
            index.stats().max_bucket_id > old_max,
            "doc 2 must overflow into freshly-allocated buckets"
        );

        (index, store)
    }

    /// The manifest commit must be the last write of a flush: every dirty
    /// bucket object precedes the metadata, and each is written to a fresh
    /// generation-suffixed object.
    #[tokio::test]
    async fn test_flush_writes_all_buckets_before_manifest_commit() {
        let (index, _store) = build_migration_scenario().await;

        let events = std::cell::RefCell::new(Vec::<FlushEvent>::new());
        index
            .flush_with(
                3,
                |_data| {
                    events.borrow_mut().push(FlushEvent::Metadata);
                    std::future::ready(Ok(()))
                },
                |object, _data| {
                    assert!(
                        object.generation > 0,
                        "bucket writes must target generation-suffixed objects"
                    );
                    events
                        .borrow_mut()
                        .push(FlushEvent::Bucket(object.bucket_id));
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap();

        let events = events.into_inner();
        assert!(
            events.len() > 2,
            "expected several bucket writes: {events:?}"
        );
        assert_eq!(
            events.last(),
            Some(&FlushEvent::Metadata),
            "the manifest commit must come last: {events:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| **e == FlushEvent::Metadata)
                .count(),
            1
        );
    }

    /// A crash after every new-generation bucket object is written but before
    /// the manifest commit must leave the previous snapshot fully intact —
    /// the new objects are unreferenced garbage.
    #[tokio::test]
    async fn test_flush_crash_before_manifest_commit_keeps_previous_snapshot() {
        let (index, store) = build_migration_scenario().await;

        // Crash window: bucket objects reach the store, the manifest doesn't.
        let mut crashed = store.clone();
        {
            let buckets = &mut crashed.buckets;
            let err = index
                .flush_with(
                    3,
                    |_| std::future::ready(Err::<(), BoxError>("crash before manifest".into())),
                    |object, data| {
                        buckets.insert(object, data);
                        std::future::ready(Ok(()))
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(err, BM25Error::Generic { .. }));
        }

        // Reload from the old manifest plus the orphaned new objects: the
        // orphans are invisible and the previous snapshot is complete.
        let loaded = load_from(&crashed).await;
        assert_eq!(loaded.len(), 1);
        for term in ["alpha", "bravo"] {
            assert!(
                loaded.search(term, 10, None).iter().any(|(id, _)| *id == 1),
                "doc 1 must still be found via '{term}' after the crash"
            );
        }
        assert!(
            loaded.search("zulu", 10, None).is_empty(),
            "uncommitted doc 2 must stay invisible"
        );

        // The interrupted flush retries cleanly and converges.
        let mut recovered_store = crashed;
        assert!(flush_to(&index, &mut recovered_store, 4).await.saved);
        let recovered = load_from(&recovered_store).await;
        assert_eq!(recovered.len(), 2);
        for (doc, term) in [(1, "alpha"), (1, "bravo"), (2, "alpha"), (2, "zulu")] {
            assert!(
                recovered
                    .search(term, 10, None)
                    .iter()
                    .any(|(id, _)| *id == doc),
                "doc {doc} must be found via '{term}' after recovery"
            );
        }
    }

    /// Cancellation (the flush future is dropped at an await point) before
    /// the manifest commit must leave everything retryable: no metadata
    /// version is claimed, no bucket is marked clean, and a retry followed by
    /// a reload converges on the complete new snapshot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancelled_flush_before_commit_stays_retryable() {
        let (index, store) = build_migration_scenario().await;
        let index = Arc::new(index);
        let persisted = Arc::new(std::sync::Mutex::new(store));
        let metadata_entered = Arc::new(tokio::sync::Notify::new());

        let flushing = {
            let index = index.clone();
            let persisted = persisted.clone();
            let metadata_entered = metadata_entered.clone();
            tokio::spawn(async move {
                index
                    .flush_with(
                        3,
                        move |_data| {
                            let metadata_entered = metadata_entered.clone();
                            async move {
                                metadata_entered.notify_one();
                                std::future::pending::<Result<(), BoxError>>().await
                            }
                        },
                        move |object, data| {
                            persisted.lock().unwrap().buckets.insert(object, data);
                            std::future::ready(Ok(()))
                        },
                    )
                    .await
            })
        };

        metadata_entered.notified().await;
        flushing.abort();
        assert!(
            flushing
                .await
                .expect_err("flush should be cancelled")
                .is_cancelled()
        );
        assert!(
            index.has_pending_metadata_flush(),
            "cancellation before the commit must not claim the metadata version"
        );
        assert!(
            index.has_dirty_buckets(),
            "cancellation before the commit must keep every bucket dirty"
        );

        // Before the retry, the durable store still loads the old snapshot.
        let crashed = persisted.lock().unwrap().clone();
        let loaded = load_from(&crashed).await;
        assert_eq!(loaded.len(), 1);

        // The retry persists one complete new snapshot.
        let mut retry_store = crashed;
        assert!(flush_to(&index, &mut retry_store, 4).await.saved);
        let reopened = load_from(&retry_store).await;
        for (doc, term) in [(1, "alpha"), (1, "bravo"), (2, "alpha"), (2, "zulu")] {
            assert!(
                reopened
                    .search(term, 10, None)
                    .iter()
                    .any(|(id, _)| *id == doc),
                "doc {doc} must be found via '{term}' after abort and retry"
            );
        }
    }

    /// After the manifest commit, the replaced objects are garbage. A crash
    /// (or plain failure) before they are deleted must not affect reloads:
    /// the manifest never references them.
    #[tokio::test]
    async fn test_reload_unaffected_when_obsolete_deletion_fails() {
        let (index, mut store) = build_migration_scenario().await;

        // Flush, but "crash" between the manifest commit and the cleanup:
        // keep every obsolete object in the store.
        let outcome;
        let mut meta_buf: Vec<u8> = Vec::new();
        {
            let buckets = &mut store.buckets;
            outcome = index
                .flush_with(
                    3,
                    |data| {
                        meta_buf = data;
                        std::future::ready(Ok(()))
                    },
                    |object, data| {
                        buckets.insert(object, data);
                        std::future::ready(Ok(()))
                    },
                )
                .await
                .unwrap();
        }
        assert!(outcome.saved);
        assert!(
            !outcome.obsolete.is_empty(),
            "the rewritten bucket's previous object must be reported obsolete"
        );
        for object in &outcome.obsolete {
            assert!(
                store.buckets.contains_key(object),
                "test setup: obsolete {object:?} must still exist in the store"
            );
        }
        store.metadata = meta_buf;

        // Reload with the leaked garbage still present: invisible.
        let loaded = load_from(&store).await;
        assert_eq!(loaded.len(), 2);
        for (doc, term) in [(1, "alpha"), (1, "bravo"), (2, "alpha"), (2, "zulu")] {
            assert!(
                loaded
                    .search(term, 10, None)
                    .iter()
                    .any(|(id, _)| *id == doc),
                "doc {doc} must be found via '{term}' with garbage objects present"
            );
        }
        assert!(
            !loaded.has_dirty_buckets(),
            "leaked garbage must not dirty anything on load"
        );
    }

    #[test]
    fn test_remove_replay_cleans_postings_after_doc_tokens_are_already_gone() {
        let index = BM25Index::new("idempotent_remove".to_string(), default_tokenizer(), None);
        let text = "alpha bravo charlie";
        index.insert(42, text, 1).unwrap();

        // Model a crash after the logical document membership was removed but
        // before the original-text postings were all cleaned.
        assert!(index.remove(42, "unrelated text", 2));
        assert!(
            index
                .postings
                .iter()
                .any(|posting| { posting.1.iter().any(|(doc_id, _)| *doc_id == 42) })
        );
        let after_first = index.stats();

        // Recovery replays the correct historical text. The logical removal
        // reports false and does not advance statistics twice, but it still
        // purges every stale posting and bucket doc-id reference.
        assert!(!index.remove(42, text, 3));
        let after_replay = index.stats();
        assert_eq!(after_replay.version, after_first.version);
        assert_eq!(after_replay.delete_count, after_first.delete_count);
        assert_eq!(after_replay.last_deleted, after_first.last_deleted);
        assert!(
            index
                .postings
                .iter()
                .all(|posting| { posting.1.iter().all(|(doc_id, _)| *doc_id != 42) })
        );
        assert!(
            index
                .buckets
                .iter()
                .all(|bucket| !bucket.doc_ids.contains(&42))
        );
    }

    /// Compaction needs no special write ordering under the manifest
    /// protocol: the repacked layout becomes visible atomically with the
    /// manifest commit, every pre-compaction object is reported obsolete,
    /// and a reload sees the identical index.
    #[tokio::test]
    async fn test_compaction_flush_commits_atomically_and_reports_obsolete() {
        // Fragmented index: tiny limit creates many buckets.
        let small_config = BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 50,
        };
        let index = BM25Index::new(
            "compact_manifest".to_string(),
            default_tokenizer(),
            Some(small_config),
        );
        let docs = [
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a fast brown fox runs past the lazy dog"),
            (3, "the lazy dog sleeps all day long"),
            (4, "quick brown foxes are rare in the wild"),
        ];
        for (id, text) in &docs {
            index.insert(*id, text, 0).unwrap();
        }

        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;
        let objects_before: Vec<BucketObject> = store.buckets.keys().copied().collect();
        assert!(objects_before.len() > 3, "scenario must be fragmented");

        // Reload with a large limit and compact.
        let mut loaded =
            BM25Index::load_metadata(default_tokenizer(), &store.metadata[..]).unwrap();
        loaded.config.bucket_overload_size = 1024 * 512;
        loaded.metadata.write().config.bucket_overload_size = 1024 * 512;
        loaded
            .load_buckets(async |object| Ok(store.buckets.get(&object).cloned()))
            .await
            .unwrap();
        let results_before = loaded.search("fox", 20, None);
        let (old_count, new_count) = loaded.compact_buckets();
        assert!(new_count < old_count);

        let outcome = flush_to(&loaded, &mut store, 2).await;
        assert!(outcome.saved);
        for object in &objects_before {
            assert!(
                outcome.obsolete.contains(object),
                "pre-compaction {object:?} must be reported obsolete"
            );
            assert!(
                !store.buckets.contains_key(object),
                "pre-compaction {object:?} must be deleted from the store"
            );
        }

        // The compacted layout reloads and searches identically.
        let reloaded = load_from(&store).await;
        let mut before_ids: Vec<u64> = results_before.iter().map(|(id, _)| *id).collect();
        let mut after_ids: Vec<u64> = reloaded
            .search("fox", 20, None)
            .iter()
            .map(|(id, _)| *id)
            .collect();
        before_ids.sort_unstable();
        after_ids.sort_unstable();
        assert_eq!(before_ids, after_ids);
    }

    #[test]
    fn test_search_count_only_counts_scored_queries() {
        let index = create_test_index();
        assert_eq!(index.stats().search_count, 0);

        // top_k == 0 short-circuits and is not counted.
        assert!(index.search("fox", 0, None).is_empty());
        assert!(
            index
                .try_search_advanced("fox", 0, None)
                .unwrap()
                .is_empty()
        );
        assert_eq!(index.stats().search_count, 0);

        // Parse failures are not counted.
        let flood = format!("x{}", ")".repeat(100));
        assert!(index.try_search_advanced(&flood, 10, None).is_err());
        assert_eq!(index.stats().search_count, 0);

        // Successful searches are counted.
        assert!(!index.search("fox", 10, None).is_empty());
        assert!(!index.search_advanced("fox AND lazy", 10, None).is_empty());
        assert_eq!(index.stats().search_count, 2);
    }

    #[test]
    fn test_token_counters_stay_consistent_under_concurrent_insert_remove() {
        use std::thread;

        let index = Arc::new(BM25Index::new(
            "avg_convergence".to_string(),
            default_tokenizer(),
            None,
        ));
        // A stable base corpus so the index is never empty.
        index
            .insert(1, "base document alpha bravo charlie", 0)
            .unwrap();
        index.insert(2, "base document delta echo", 0).unwrap();

        const ITERS: usize = 300;
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let index = index.clone();
            handles.push(thread::spawn(move || {
                let id = 100 + t;
                let text = format!("churn document number {id} with token payload");
                for _ in 0..ITERS {
                    index.insert(id, &text, 0).unwrap();
                    assert!(index.remove(id, &text, 0));
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // Once all writers drained, `total_tokens` must account for exactly
        // the documents still present, and the reported average must be the
        // quotient of the two (it is derived from them, never cached).
        let total = index.total_tokens.load(Ordering::Relaxed);
        let live_tokens: usize = index.doc_tokens.iter().map(|entry| *entry.value()).sum();
        assert_eq!(
            total, live_tokens as u64,
            "total_tokens {total} != sum of live doc_tokens {live_tokens}"
        );
        let expected = total as f32 / index.doc_tokens.len() as f32;
        assert_eq!(index.stats().avg_doc_tokens, expected);
    }

    #[test]
    fn test_bm25_params() {
        // 使用默认参数
        let default_index = create_test_index();

        // 搜索相同的查询
        let default_results = default_index.search("fox", 10, None);
        let custom_results = default_index.search("fox", 10, Some(BM25Params { k1: 1.5, b: 0.75 }));

        // 验证结果数量相同但分数不同
        assert_eq!(default_results.len(), custom_results.len());

        // 至少有一个文档的分数应该不同
        let mut scores_different = false;
        for i in 0..default_results.len() {
            if (default_results[i].1 - custom_results[i].1).abs() > 0.001 {
                scores_different = true;
                break;
            }
        }
        assert!(scores_different);
    }

    #[test]
    fn test_invalid_bm25_params_do_not_produce_non_finite_scores() {
        let index = create_test_index();
        // A document longer than average that mentions the query term twice:
        // the only shape for which an unclamped `k1` overflows *both* sides of
        // the ratio and yields `NaN` rather than a harmless `0.0`.
        index
            .insert(
                5,
                "The fox and the other fox watched the quick brown fox run past the lazy dog again",
                0,
            )
            .unwrap();

        // `f32::MAX` (and any other large-but-finite `k1`) passes an
        // `is_finite` guard, yet overflows the unclamped formula to
        // `inf / inf = NaN`; `b` outside `[0, 1]` distorts the length
        // normalization the same way.
        let hostile = [
            BM25Params {
                k1: f32::NAN,
                b: f32::INFINITY,
            },
            BM25Params {
                k1: f32::MAX,
                b: 1.0,
            },
            BM25Params {
                k1: f32::MAX,
                b: f32::MAX,
            },
            BM25Params { k1: 1e30, b: 1.0 },
            BM25Params { k1: -1e30, b: -5.0 },
        ];

        for params in hostile {
            let results = index.search("fox", 10, Some(params.clone()));
            assert!(!results.is_empty(), "no results for {params:?}");
            assert!(
                results.iter().all(|(_, score)| score.is_finite()),
                "non-finite score for {params:?}: {results:?}"
            );
        }
    }

    /// Regression: a hostile-but-finite `k1` used to make `score_term` emit
    /// `NaN` for every document longer than average, which then fed
    /// `select_nth_unstable_by` a non-transitive comparator (`NaN` compared
    /// equal to everything, so ordering fell back to id order against it while
    /// the remaining pairs stayed score-ordered). That can panic with
    /// "user-provided comparison function does not correctly implement a total
    /// order" and otherwise returns arbitrary, partly non-finite rankings.
    ///
    /// The corpus is large enough for `top_k_results` to take the partial-sort
    /// path, and the documents deliberately straddle the average length with
    /// term frequencies `>= 2`.
    #[test]
    fn test_large_finite_bm25_params_keep_ranking_total_and_finite() {
        let index = BM25Index::new("hostile_params".to_string(), default_tokenizer(), None);
        for id in 0..2_000u64 {
            // Half the corpus is long with a repeated query term (tf >= 2 and
            // doc_len > avg), the other half is short.
            let text = if id % 2 == 0 {
                format!("alpha alpha beta gamma delta epsilon zeta doc{id} padding padding padding")
            } else {
                format!("alpha doc{id}")
            };
            index.insert(id, &text, 0).unwrap();
        }

        let results = index.search(
            "alpha",
            1_000,
            Some(BM25Params {
                k1: f32::MAX,
                b: 1.0,
            }),
        );

        assert_eq!(results.len(), 1_000);
        assert!(
            results.iter().all(|(_, score)| score.is_finite()),
            "hostile k1 produced non-finite scores: {} of {}",
            results.iter().filter(|(_, s)| !s.is_finite()).count(),
            results.len()
        );
        assert!(
            results.windows(2).all(|w| w[0].1 >= w[1].1),
            "results are not sorted by descending score"
        );
    }

    /// `compare_scored_docs` must be a total order for *every* input, so no
    /// future scoring change can trip the standard-library sorts. The triple
    /// below is the exact cycle the old comparator produced: `cmp(x, n)` and
    /// `cmp(n, y)` both said `Less` while `cmp(x, y)` said `Greater`.
    #[test]
    fn test_compare_scored_docs_is_a_total_order_with_nan() {
        let x = (1u64, 1.0f32);
        let n = (2u64, f32::NAN);
        let y = (3u64, 2.0f32);
        let entries = [
            x,
            n,
            y,
            (4, f32::INFINITY),
            (5, f32::NEG_INFINITY),
            (6, 1.0),
        ];

        for a in entries {
            for b in entries {
                for c in entries {
                    let ab = BM25Index::<TokenizerChain>::compare_scored_docs(&a, &b);
                    let bc = BM25Index::<TokenizerChain>::compare_scored_docs(&b, &c);
                    let ac = BM25Index::<TokenizerChain>::compare_scored_docs(&a, &c);
                    // Antisymmetry.
                    assert_eq!(
                        ab.reverse(),
                        BM25Index::<TokenizerChain>::compare_scored_docs(&b, &a),
                        "not antisymmetric for {a:?} / {b:?}"
                    );
                    // Transitivity.
                    if ab != std::cmp::Ordering::Greater && bc != std::cmp::Ordering::Greater {
                        assert_ne!(
                            ac,
                            std::cmp::Ordering::Greater,
                            "cycle: {a:?} <= {b:?} <= {c:?} but {a:?} > {c:?}"
                        );
                    }
                }
            }
        }

        // NaN scores rank last, never first.
        let mut entries = [n, x, y];
        entries.sort_unstable_by(BM25Index::<TokenizerChain>::compare_scored_docs);
        assert_eq!(entries[0].0, 3);
        assert_eq!(entries[2].0, 2);
    }

    #[test]
    fn test_search_advanced() {
        let index = create_test_index();

        // 测试简单的 Term 查询
        let results = index.search_advanced("fox", 10, None);
        assert_eq!(results.len(), 3); // 应该找到3个包含"fox"的文档

        // 测试 AND 查询
        let results = index.search_advanced("fox AND lazy", 10, None);
        assert_eq!(results.len(), 2); // 文档1和2同时包含"fox"和"lazy"
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 2));

        // 测试 OR 查询
        let results = index.search_advanced("quick OR fast", 10, None);
        assert_eq!(results.len(), 3); // 文档1包含"quick"，文档2包含"fast"，文档4包含"quick"
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 2));
        assert!(results.iter().any(|(id, _)| *id == 4));

        // 测试 NOT 查询
        let results = index.search_advanced("dog AND NOT lazy", 10, None);
        assert_eq!(results.len(), 0); // 所有包含"dog"的文档也包含"lazy"

        // 测试复杂的嵌套查询
        let results = index.search_advanced("(quick OR fast) AND fox", 10, None);
        assert_eq!(results.len(), 3); // 文档1、2和4

        // 测试更复杂的嵌套查询
        let results = index.search_advanced("(brown AND fox) AND NOT (rare OR sleeps)", 10, None);
        assert_eq!(results.len(), 2); // 文档1和2，排除了包含"rare"的文档4和包含"sleeps"的文档3
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 2));

        // 测试空查询
        let results = index.search_advanced("", 10, None);
        assert_eq!(results.len(), 0);

        // 测试无匹配查询
        let results = index.search_advanced("elephant AND giraffe", 10, None);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_search_advanced_with_parentheses() {
        let index = create_test_index();

        // 测试带括号的复杂查询
        let results = index.search_advanced("(fox AND quick) OR (dog AND sleeps)", 10, None);
        assert_eq!(results.len(), 3); // 文档1, 3, 4
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 3));
        assert!(results.iter().any(|(id, _)| *id == 4));

        // 测试多层嵌套括号
        let results = index.search_advanced(
            "((brown AND fox) OR (lazy AND sleeps)) AND NOT rare",
            10,
            None,
        );
        assert_eq!(results.len(), 3); // 文档1、2和3，排除了包含"rare"的文档4
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 2));
        assert!(results.iter().any(|(id, _)| *id == 3));

        // 测试带括号的 NOT 查询
        let results = index.search_advanced("dog AND NOT (quick OR fast)", 10, None);
        assert_eq!(results.len(), 1); // 只有文档3，因为它包含"dog"但不包含"quick"或"fast"
        assert_eq!(results[0].0, 3);
    }

    #[test]
    fn test_search_advanced_score_ordering() {
        let index = create_test_index();

        // 测试分数排序 - 包含更多匹配词的文档应该排在前面
        let results = index.search_advanced("quick OR fox OR dog", 10, None);
        assert!(results.len() >= 3);

        // 文档1应该排在最前面，因为它同时包含所有三个词
        assert_eq!(results[0].0, 1);

        // 测试 top_k 限制
        let results = index.search_advanced("dog", 2, None);
        assert_eq!(results.len(), 2); // 应该只返回2个结果，尽管有3个文档包含"dog"
    }

    #[test]
    fn test_search_vs_search_advanced() {
        let index = create_test_index();

        // 对于简单查询，search 和 search_advanced 应该返回相似的结果
        let simple_results = index.search("fox", 10, None);
        let advanced_results = index.search_advanced("fox", 10, None);

        assert_eq!(simple_results.len(), advanced_results.len());

        // 检查文档ID是否匹配（不检查分数，因为实现可能略有不同）
        let simple_ids: Vec<u64> = simple_results.iter().map(|(id, _)| *id).collect();
        let advanced_ids: Vec<u64> = advanced_results.iter().map(|(id, _)| *id).collect();

        assert_eq!(simple_ids.len(), advanced_ids.len());
        for id in simple_ids {
            assert!(advanced_ids.contains(&id));
        }

        // 测试多词查询 - search 将它们视为 OR，search_advanced 也应该如此
        let simple_results = index.search("quick fox", 10, None);
        let advanced_results = index.search_advanced("quick OR fox", 10, None);

        // 检查文档ID是否匹配
        let simple_ids: Vec<u64> = simple_results.iter().map(|(id, _)| *id).collect();
        let advanced_ids: Vec<u64> = advanced_results.iter().map(|(id, _)| *id).collect();

        assert_eq!(simple_ids.len(), advanced_ids.len());
        for id in simple_ids {
            assert!(advanced_ids.contains(&id));
        }
    }

    #[test]
    fn test_search_not_alone() {
        let index = create_test_index();
        // NOT fox => 返回所有不含 fox 的文档 (文档3)
        let results = index.search_advanced("NOT fox", 10, None);
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![3]);
    }

    #[test]
    fn test_double_negation_inside_and() {
        let index = create_test_index();

        // dog AND NOT (NOT lazy) === dog AND lazy => 文档1、2、3
        let results = index.search_advanced("dog AND NOT (NOT lazy)", 10, None);
        let mut ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3]);

        // NOT (NOT fox) === fox => 文档1、2、4
        let results = index.search_advanced("NOT (NOT fox)", 10, None);
        let mut ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 4]);
    }

    #[test]
    fn test_nested_not_complement_guard_inside_and_filter() {
        let index = BM25Index::new("nested_not_guard".to_string(), default_tokenizer(), None);
        for id in 0..=MAX_NOT_COMPLEMENT_DOCS as u64 {
            index.insert(id, "hello world", 0).unwrap();
        }

        let err = index
            .try_search_advanced("hello AND NOT (NOT world)", 10, None)
            .unwrap_err();
        assert!(err.to_string().contains("logical NOT complement"));
    }

    #[test]
    fn test_not_first_in_and_matches_not_last() {
        let index = create_test_index();

        // NOT lazy AND fox === fox AND NOT lazy => 只有文档4
        let a = index.search_advanced("NOT lazy AND fox", 10, None);
        let b = index.search_advanced("fox AND NOT lazy", 10, None);
        let mut ids_a: Vec<u64> = a.iter().map(|(id, _)| *id).collect();
        let mut ids_b: Vec<u64> = b.iter().map(|(id, _)| *id).collect();
        ids_a.sort_unstable();
        ids_b.sort_unstable();
        assert_eq!(ids_a, vec![4]);
        assert_eq!(ids_a, ids_b);
    }

    #[test]
    fn test_and_with_only_not_subqueries() {
        let index = create_test_index();

        // NOT fox AND NOT rare => 不含 fox 也不含 rare 的文档 (文档3)
        let results = index.search_advanced("NOT fox AND NOT rare", 10, None);
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![3]);
    }

    #[test]
    fn test_reinsert_after_remove_with_wrong_text() {
        let index = BM25Index::new("reinsert".to_string(), default_tokenizer(), None);
        index.insert(1, "dog dog cat", 0).unwrap();

        // Remove with non-original text: the "dog"/"cat" postings keep stale entries.
        assert!(index.remove(1, "bird", 0));

        // Re-insert the same id with a different "dog" frequency; the stale
        // posting entry must not be double-counted nor inflate df.
        index.insert(1, "dog dog dog mouse", 0).unwrap();

        let results = index.search("dog", 10, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert!(
            results[0].1.is_finite() && results[0].1 > 0.0,
            "score must be positive, got {}",
            results[0].1
        );

        // Removing with the correct text must clear all entries for the doc,
        // including stale duplicates, and drop the now-empty posting.
        assert!(index.remove(1, "dog dog dog mouse", 0));
        assert!(index.search("dog", 10, None).is_empty());
        assert!(index.postings.get("dog").is_none());
    }

    #[test]
    fn test_concurrent_insert_remove_shared_token() {
        use std::thread;

        // Regression test: remove() must not drop a posting that a concurrent
        // insert just appended to (the empty-check and the removal must be
        // atomic). Two writers share the token "shared"; the reader-side
        // assertion in thread B would fail if the posting got lost.
        let index = Arc::new(BM25Index::new(
            "concurrent_shared".to_string(),
            default_tokenizer(),
            None,
        ));

        const ITERS: usize = 500;
        let a = {
            let index = index.clone();
            thread::spawn(move || {
                for _ in 0..ITERS {
                    index.insert(2, "shared alpha", 0).unwrap();
                    assert!(index.remove(2, "shared alpha", 0));
                }
            })
        };
        let b = {
            let index = index.clone();
            thread::spawn(move || {
                for _ in 0..ITERS {
                    index.insert(3, "shared beta", 0).unwrap();
                    let results = index.search("shared", 10, None);
                    assert!(
                        results.iter().any(|(id, _)| *id == 3),
                        "doc 3 must stay searchable while it exists"
                    );
                    assert!(index.remove(3, "shared beta", 0));
                }
            })
        };

        a.join().unwrap();
        b.join().unwrap();

        // After all churn the index must still accept and find new documents.
        index.insert(10, "shared final", 0).unwrap();
        let results = index.search("shared", 10, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 10);
    }

    /// Regression: `remove()` strips a token from its bucket based on the
    /// `removed_postings` snapshot taken a few lines earlier. When a
    /// concurrent `insert` re-created that posting in the *same* bucket in
    /// between, the token must stay listed — `serialize_bucket` only walks
    /// `bucket.tokens`, so a live posting that no bucket lists is never
    /// persisted and the term is silently lost on the next flush + reload.
    ///
    /// The interleaving is forced, never raced: `remove()` drops the emptied
    /// posting from `postings` *before* it touches `buckets`, so holding
    /// bucket 0's shard guard parks it precisely between those two steps. The
    /// guard is taken before the remover starts, and the posting vanishing
    /// from `postings` is the observable end of the first step — no sleeps and
    /// no timing-dependent assertions.
    #[tokio::test]
    async fn test_remove_keeps_bucket_token_recreated_by_concurrent_insert() {
        use std::thread;
        use std::time::{Duration, Instant};

        let index = Arc::new(BM25Index::new(
            "recreated_posting".to_string(),
            default_tokenizer(),
            None,
        ));
        index.insert(1, "alpha", 0).unwrap();
        assert_eq!(index.postings.get("alpha").unwrap().0, 0);

        let remover = {
            // Parks `remove()` at its bucket-update phase.
            let mut bucket0 = index.buckets.get_mut(&0).unwrap();
            let remover = {
                let index = index.clone();
                thread::spawn(move || index.remove(1, "alpha", 0))
            };

            let deadline = Instant::now() + Duration::from_secs(30);
            while index.postings.contains_key("alpha") {
                assert!(
                    Instant::now() < deadline,
                    "remove() never dropped the emptied posting"
                );
                thread::yield_now();
            }

            // What the concurrent `insert`'s phase 1 does: doc 2 re-creates
            // the posting in bucket 0, which still lists the token, so the
            // insert's phase 2 would only bump this bucket's accounting.
            index
                .postings
                .insert("alpha".to_string(), (0, vec![(2u64, 1usize)].into()));
            index.doc_tokens.insert(2, 1);
            index.total_tokens.fetch_add(1, Ordering::Relaxed);
            bucket0.doc_ids.insert(2);
            bucket0.mark_dirty();
            assert!(bucket0.tokens.contains("alpha"));
            remover
        };
        assert!(remover.join().unwrap());

        // The invariant: every live posting is listed by exactly the bucket it
        // names, otherwise no bucket would ever serialize it.
        let posting = index.postings.get("alpha").expect("posting must survive");
        assert_eq!(posting.0, 0);
        drop(posting);
        assert!(
            index.buckets.get(&0).unwrap().tokens.contains("alpha"),
            "the owning bucket must keep listing a posting a concurrent insert re-created"
        );

        // ... and that bucket carries the term through a flush + reload.
        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;
        let reloaded = load_from(&store).await;
        let found: Vec<u64> = reloaded
            .search("alpha", 10, None)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(
            found,
            vec![2],
            "the re-created term must stay searchable after a flush + reload"
        );
    }

    #[tokio::test]
    async fn test_serialization_with_buckets() {
        // 创建一个带有小桶大小的索引，强制触发分桶
        let tokenizer = default_tokenizer();
        let config = BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 100, // 非常小的桶大小，强制分桶
        };
        let index = BM25Index::new(
            "test_bucket_serialization".to_string(),
            tokenizer,
            Some(config),
        );

        // 添加大量文档，确保触发分桶
        let test_docs = vec![
            (
                1,
                "The quick brown fox jumps over the lazy dog in the forest",
            ),
            (2, "A fast brown fox runs past the lazy dog near the river"),
            (3, "The lazy dog sleeps all day under the warm sun"),
            (4, "Quick brown foxes are rare in the wild mountain regions"),
            (5, "Many foxes hunt at night when the moon is bright"),
            (6, "Dogs and cats are common pets in modern households"),
            (7, "Wild animals like foxes and wolves roam the countryside"),
            (8, "The forest is home to many different species of animals"),
            (9, "Lazy afternoon naps are enjoyed by both dogs and cats"),
            (
                10,
                "Quick movements help foxes catch their prey efficiently",
            ),
        ];

        for (id, text) in test_docs {
            index.insert(id, text, 0).unwrap();
        }

        // 验证确实创建了多个桶
        let original_stats = index.stats();
        println!(
            "Original index has {} buckets",
            original_stats.max_bucket_id + 1
        );
        assert!(original_stats.max_bucket_id > 0, "应该创建了多个桶");

        // 保存索引
        let mut store = MemStore::default();
        flush_to(&index, &mut store, 100).await;

        // 验证保存了正确数量的桶
        println!("Saved {} document buckets", store.buckets.len());
        assert!(store.buckets.len() > 1, "应该保存了多个文档桶");

        // 验证每个桶的内容
        for (object, data) in &store.buckets {
            let bucket: BucketOwned = cbor2::from_reader(&data[..]).unwrap();
            println!("Document bucket {object:?} {:?}", bucket.doc_tokens);
            assert!(!bucket.postings.is_empty());

            // 验证倒排索引结构
            for (term, (bucket_ref, doc_list)) in bucket.postings {
                assert_eq!(
                    bucket_ref, object.bucket_id,
                    "术语 {} 的桶引用应该指向当前桶",
                    term
                );
                assert!(!doc_list.is_empty(), "术语 {} 的文档列表不应该为空", term);

                for (doc_id, freq) in doc_list.iter() {
                    assert!(*freq > 0, "文档 {} 中术语 {} 的频率应该大于0", doc_id, term);
                }
            }

            // 验证文档token数量的合理性
            for (doc_id, token_count) in bucket.doc_tokens {
                assert!(token_count > 0, "文档 {} 的token数量应该大于0", doc_id);
            }
        }

        // 加载索引
        let loaded_index = load_from(&store).await;

        // 验证加载的索引基本信息
        assert_eq!(loaded_index.len(), index.len(), "文档数量应该一致");

        let loaded_stats = loaded_index.stats();
        assert_eq!(
            loaded_stats.max_bucket_id, original_stats.max_bucket_id,
            "最大桶ID应该一致"
        );
        assert_eq!(
            loaded_stats.max_document_id, original_stats.max_document_id,
            "最大文档ID应该一致"
        );
        assert!(
            (loaded_stats.avg_doc_tokens - original_stats.avg_doc_tokens).abs() < 0.01,
            "平均文档token数应该基本一致"
        );

        // 验证每个文档的token数量
        for i in 1..=10 {
            let original_tokens = index.get_doc_tokens(i);
            let loaded_tokens = loaded_index.get_doc_tokens(i);
            assert_eq!(
                original_tokens, loaded_tokens,
                "文档 {} 的token数量应该一致",
                i
            );
        }

        // 验证多种搜索查询的结果一致性
        let test_queries = vec![
            "fox",
            "dog",
            "lazy",
            "quick brown",
            "fox AND dog",
            "brown OR lazy",
            "fox AND NOT lazy",
            "(quick OR fast) AND fox",
        ];

        for query in test_queries {
            println!("Testing query: {}", query);

            let original_results =
                if query.contains("AND") || query.contains("OR") || query.contains("NOT") {
                    index.search_advanced(query, 10, None)
                } else {
                    index.search(query, 10, None)
                };

            let loaded_results =
                if query.contains("AND") || query.contains("OR") || query.contains("NOT") {
                    loaded_index.search_advanced(query, 10, None)
                } else {
                    loaded_index.search(query, 10, None)
                };

            assert_eq!(
                original_results.len(),
                loaded_results.len(),
                "查询 '{}' 的结果数量应该一致",
                query
            );

            // 按文档ID排序后比较
            let mut orig_sorted = original_results.clone();
            let mut loaded_sorted = loaded_results.clone();
            orig_sorted.sort_by_key(|a| a.0);
            loaded_sorted.sort_by_key(|a| a.0);

            for i in 0..orig_sorted.len() {
                assert_eq!(
                    orig_sorted[i].0, loaded_sorted[i].0,
                    "查询 '{}' 的第 {} 个结果文档ID应该一致",
                    query, i
                );
                assert!(
                    (orig_sorted[i].1 - loaded_sorted[i].1).abs() < 0.001,
                    "查询 '{}' 的第 {} 个结果分数应该基本一致，原始: {}, 加载: {}",
                    query,
                    i,
                    orig_sorted[i].1,
                    loaded_sorted[i].1
                );
            }
        }

        // 验证倒排索引的完整性 - 检查一些关键词的倒排列表
        let key_terms = vec!["fox", "dog", "lazy", "brown", "quick"];
        for term in key_terms {
            let original_postings = index.postings.get(term);
            let loaded_postings = loaded_index.postings.get(term);

            match (original_postings, loaded_postings) {
                (Some(orig), Some(loaded)) => {
                    // 比较倒排列表内容
                    assert_eq!(
                        orig.1.len(),
                        loaded.1.len(),
                        "术语 '{}' 的倒排列表长度应该一致",
                        term
                    );

                    let mut orig_docs: Vec<_> = orig.1.iter().collect();
                    let mut loaded_docs: Vec<_> = loaded.1.iter().collect();
                    orig_docs.sort();
                    loaded_docs.sort();

                    for i in 0..orig_docs.len() {
                        assert_eq!(
                            orig_docs[i], loaded_docs[i],
                            "术语 '{}' 的第 {} 个倒排项应该一致",
                            term, i
                        );
                    }
                }
                (None, None) => {
                    // 都没有该术语，正常
                }
                _ => {
                    panic!("术语 '{}' 在原始索引和加载索引中的存在性不一致", term);
                }
            }
        }

        println!("所有分桶序列化测试通过！");

        {
            // 测试只加载部分桶的情况（只读部分加载）
            let tokenizer = default_tokenizer();
            let partial_index =
                BM25Index::load_all(tokenizer, &store.metadata[..], async |object| {
                    // 只加载桶0的文档
                    if object.bucket_id == 0 {
                        Ok(store.buckets.get(&object).cloned())
                    } else {
                        Ok(None)
                    }
                })
                .await
                .unwrap();

            // 部分加载会载入桶0 posting 需要的文档长度；如果桶0包含高频词，
            // 它可能覆盖全部文档，但搜索结果仍不应超过完整索引。
            assert!(partial_index.len() <= index.len());

            // 验证部分搜索结果
            let partial_results = partial_index.search("fox", 10, None);
            let full_results = index.search("fox", 10, None);

            // 部分结果应该是完整结果的子集
            assert!(partial_results.len() <= full_results.len());

            for (doc_id, _) in partial_results {
                assert!(
                    full_results.iter().any(|(id, _)| *id == doc_id),
                    "部分加载结果中的文档 {} 应该存在于完整结果中",
                    doc_id
                );
            }

            println!("加载部分分桶测试通过！");
        }
    }

    #[tokio::test]
    async fn test_partial_load_keeps_doc_tokens_with_existing_token_bucket() {
        let config = BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 64,
        };
        let index = BM25Index::new(
            "partial_load_doc_tokens".to_string(),
            default_tokenizer(),
            Some(config),
        );

        index.insert(1, "alpha bravo", 0).unwrap();
        let alpha_bucket = index.postings.get("alpha").unwrap().0;

        let filler_docs = [
            (2, "charlie delta echo foxtrot"),
            (3, "golf hotel india juliet"),
            (4, "kilo lima mike november"),
            (5, "oscar papa quebec romeo"),
            (6, "sierra tango uniform victor"),
            (7, "whiskey xray yankee zulu"),
        ];
        for (id, text) in filler_docs {
            index.insert(id, text, 0).unwrap();
        }
        assert!(index.stats().max_bucket_id > alpha_bucket);

        index.insert(99, "alpha alpha", 0).unwrap();
        assert_eq!(index.postings.get("alpha").unwrap().0, alpha_bucket);

        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;

        let partial_index =
            BM25Index::load_all(default_tokenizer(), &store.metadata[..], async |object| {
                if object.bucket_id == alpha_bucket {
                    Ok(store.buckets.get(&object).cloned())
                } else {
                    Ok(None)
                }
            })
            .await
            .unwrap();

        assert_eq!(partial_index.get_doc_tokens(99), Some(2));
        let results = partial_index.search("alpha", 10, None);
        assert!(results.iter().any(|(id, _)| *id == 99));
    }

    #[test]
    fn test_no_excessive_small_buckets() {
        // Regression test: existing tokens in a bucket must NOT trigger migration,
        // otherwise each insert after the bucket reaches the limit creates many
        // tiny new buckets.
        let tokenizer = default_tokenizer();
        let config = BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 200, // small limit to trigger splits quickly
        };
        let index = BM25Index::new("small_bucket_test".to_string(), tokenizer, Some(config));

        // Insert many documents sharing common tokens
        let docs = vec![
            (1, "the quick brown fox"),
            (2, "the lazy brown dog"),
            (3, "the quick red cat"),
            (4, "a lazy brown fox jumps"),
            (5, "the brown dog runs fast"),
            (6, "a quick fox hunts at night"),
            (7, "the lazy cat sleeps all day"),
            (8, "brown dogs and brown cats"),
            (9, "quick movements help foxes"),
            (10, "the fast dog chases the fox"),
            (11, "lazy afternoons with brown dogs"),
            (12, "quick brown fox returns again"),
            (13, "the old brown dog rests"),
            (14, "a new quick fox appears"),
            (15, "brown and lazy describe the dog"),
        ];

        for (id, text) in &docs {
            index.insert(*id, text, 0).unwrap();
        }

        let stats = index.stats();
        let num_buckets = stats.max_bucket_id + 1;
        println!(
            "docs={}, buckets={}, max_bucket_id={}",
            docs.len(),
            num_buckets,
            stats.max_bucket_id
        );

        // With 15 short documents and 200-byte limit, we expect a modest number
        // of buckets — certainly not one per insert.
        assert!(
            (num_buckets as usize) < docs.len(),
            "Too many buckets ({num_buckets}) for {} documents — \
             existing tokens are likely being migrated incorrectly",
            docs.len()
        );

        // Verify all documents are still searchable
        for (id, text) in &docs {
            let first_word = text.split_whitespace().find(|w| w.len() > 2).unwrap();
            let results = index.search(first_word, 20, None);
            assert!(
                results.iter().any(|(rid, _)| *rid == *id),
                "doc {} not found when searching for '{}'",
                id,
                first_word
            );
        }
    }

    #[tokio::test]
    async fn test_compact_buckets() {
        // Simulate the real-world scenario: the configured limit is large, but the old
        // bucket-splitting bug created many tiny buckets anyway.
        // We build the index with a tiny limit (to generate fragmentation), then
        // serialize, reload with the correct large limit, and compact.
        let tokenizer = default_tokenizer();
        let small_config = BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 50, // tiny limit to force many buckets
        };
        let index = BM25Index::new("compact_test".to_string(), tokenizer, Some(small_config));

        let docs = vec![
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a fast brown fox runs past the lazy dog"),
            (3, "the lazy dog sleeps all day long"),
            (4, "quick brown foxes are rare in the wild"),
            (5, "many foxes hunt at night when the moon is bright"),
            (6, "dogs and cats are common pets in modern households"),
            (7, "wild animals like foxes and wolves roam the countryside"),
            (8, "the forest is home to many different species of animals"),
        ];

        for (id, text) in &docs {
            index.insert(*id, text, 0).unwrap();
        }

        let bucket_count_before = index.stats().max_bucket_id + 1;
        println!("Before compact: {} buckets", bucket_count_before);
        assert!(
            bucket_count_before > 3,
            "should have many fragmented buckets"
        );

        // Serialize fragmented index
        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;

        // Reload with the correct (large) bucket limit
        let mut loaded =
            BM25Index::load_metadata(default_tokenizer(), &store.metadata[..]).unwrap();
        loaded.config.bucket_overload_size = 1024 * 512;
        loaded.metadata.write().config.bucket_overload_size = 1024 * 512;
        loaded
            .load_buckets(async |object| Ok(store.buckets.get(&object).cloned()))
            .await
            .unwrap();

        let bucket_count_loaded = loaded.stats().max_bucket_id + 1;
        assert_eq!(bucket_count_loaded, bucket_count_before);

        // Capture search results before compaction
        let queries = ["fox", "dog", "lazy brown", "quick OR fast"];
        let results_before: Vec<Vec<(u64, f32)>> = queries
            .iter()
            .map(|q| {
                if q.contains("OR") {
                    loaded.search_advanced(q, 20, None)
                } else {
                    loaded.search(q, 20, None)
                }
            })
            .collect();

        // Compact!
        let (old, new) = loaded.compact_buckets();
        println!("Compacted: {} -> {} buckets", old, new);
        assert!(
            new < old,
            "compaction should reduce bucket count significantly"
        );
        assert!(
            new <= 3,
            "with 512K limit all postings should fit in very few buckets, got {}",
            new,
        );

        // Verify search results are unchanged
        for (i, q) in queries.iter().enumerate() {
            let results_after = if q.contains("OR") {
                loaded.search_advanced(q, 20, None)
            } else {
                loaded.search(q, 20, None)
            };
            assert_eq!(
                results_before[i].len(),
                results_after.len(),
                "query '{}' result count changed after compaction",
                q
            );

            let mut before_sorted = results_before[i].clone();
            let mut after_sorted = results_after.clone();
            before_sorted.sort_by_key(|a| a.0);
            after_sorted.sort_by_key(|a| a.0);
            for j in 0..before_sorted.len() {
                assert_eq!(before_sorted[j].0, after_sorted[j].0);
                assert!(
                    (before_sorted[j].1 - after_sorted[j].1).abs() < 0.001,
                    "query '{}' scores diverged for doc {}",
                    q,
                    before_sorted[j].0
                );
            }
        }

        // Verify flush + reload works after compaction
        let mut store2 = store.clone();
        flush_to(&loaded, &mut store2, 200).await;

        let final_loaded = load_from(&store2).await;
        assert_eq!(final_loaded.len(), loaded.len());

        for q in &queries {
            let orig = if q.contains("OR") {
                loaded.search_advanced(q, 20, None)
            } else {
                loaded.search(q, 20, None)
            };
            let reloaded = if q.contains("OR") {
                final_loaded.search_advanced(q, 20, None)
            } else {
                final_loaded.search(q, 20, None)
            };
            assert_eq!(
                orig.len(),
                reloaded.len(),
                "query '{}' mismatch after reload",
                q
            );
        }
    }

    /// Regression (twin of the B-Tree case): `compact_buckets` snapshots
    /// `postings`, clears `buckets` and re-bins the snapshot. A posting created
    /// by a concurrent `insert` after the snapshot ended up in no bucket at
    /// all — and only bucket contents are serialized — so `insert` returned
    /// `Ok` while the term silently vanished from the durable index on the
    /// next flush. `anda_db` drives compaction under the same *shared*
    /// operation lease as `add`/`update`/`remove`, so the exclusion has to
    /// live here.
    #[tokio::test]
    async fn test_compaction_never_loses_concurrent_inserts() {
        use std::thread;

        let index = Arc::new(BM25Index::new(
            "compact_concurrent_insert".to_string(),
            default_tokenizer(),
            Some(BM25Config {
                bm25: BM25Params::default(),
                bucket_overload_size: 64,
            }),
        ));
        // Seed enough tokens that each compaction has real work to do.
        for id in 0..64u64 {
            index.insert(id, &format!("seed{id:04}"), 0).unwrap();
        }

        const WRITES: u64 = 400;
        let writer_index = index.clone();
        let writer = thread::spawn(move || {
            for id in 0..WRITES {
                writer_index
                    .insert(1_000 + id, &format!("live{id:04}"), 0)
                    .unwrap();
            }
        });
        let mut compactions = 0usize;
        while !writer.is_finished() {
            index.compact_buckets();
            compactions += 1;
        }
        writer.join().unwrap();
        assert!(compactions > 0, "no compaction overlapped the writer");

        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;
        let reloaded = load_from(&store).await;
        let missing: Vec<String> = (0..WRITES)
            .map(|id| format!("live{id:04}"))
            .filter(|token| reloaded.search(token, 1, None).is_empty())
            .collect();
        assert!(
            missing.is_empty(),
            "{} concurrently inserted terms were lost, e.g. {:?}",
            missing.len(),
            &missing[..missing.len().min(5)]
        );
        assert_eq!(reloaded.len(), index.len());
    }

    /// The corpus used by the `purge_ids` tests. Terms deliberately overlap so
    /// that purging hits three different posting shapes: lists that disappear
    /// entirely (`unicorn`), lists that merely shrink (`shared`), and lists the
    /// purge must not touch at all (`walrus`).
    const PURGE_DOCS: [(u64, &str); 6] = [
        (1, "shared alpha unicorn"),
        (2, "shared beta narwhal"),
        (3, "shared gamma walrus"),
        (4, "shared delta walrus"),
        (5, "shared epsilon quokka"),
        (6, "shared zeta quokka"),
    ];

    fn build_purge_index(name: &str, config: Option<BM25Config>) -> BM25Index<TokenizerChain> {
        let index = BM25Index::new(name.to_string(), default_tokenizer(), config);
        for (id, text) in PURGE_DOCS {
            index.insert(id, text, 1).unwrap();
        }
        index
    }

    /// Normalizes the inverted index into a comparable shape: entry order
    /// inside a posting list is an implementation detail (`swap_remove`
    /// reorders), and so is the bucket a token happens to live in.
    fn posting_snapshot(index: &BM25Index<TokenizerChain>) -> BTreeMap<String, Vec<(u64, usize)>> {
        index
            .postings
            .iter()
            .map(|entry| {
                let mut docs: Vec<(u64, usize)> = entry.value().1.iter().copied().collect();
                docs.sort_unstable();
                (entry.key().clone(), docs)
            })
            .collect()
    }

    fn doc_token_snapshot(index: &BM25Index<TokenizerChain>) -> BTreeMap<u64, usize> {
        index
            .doc_tokens
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }

    /// Asserts that `purged` is indistinguishable from an index that only ever
    /// saw the surviving documents.
    fn assert_matches_reference(purged: &BM25Index<TokenizerChain>, survivors: &[u64]) {
        let reference = BM25Index::new(
            purged.name().to_string(),
            default_tokenizer(),
            Some(purged.config.clone()),
        );
        for (id, text) in PURGE_DOCS {
            if survivors.contains(&id) {
                reference.insert(id, text, 1).unwrap();
            }
        }

        assert_eq!(posting_snapshot(purged), posting_snapshot(&reference));
        assert_eq!(doc_token_snapshot(purged), doc_token_snapshot(&reference));
        assert_eq!(
            purged.total_tokens.load(Ordering::Relaxed),
            reference.total_tokens.load(Ordering::Relaxed),
        );
        assert_eq!(purged.len(), survivors.len());
        // `avg_doc_tokens` is no longer cached; it must fall out of the two
        // counters above rather than out of a stale field.
        assert_eq!(
            purged.stats().avg_doc_tokens,
            reference.stats().avg_doc_tokens
        );
        assert_eq!(
            purged.stats().avg_doc_tokens,
            purged.total_tokens.load(Ordering::Relaxed) as f32 / survivors.len() as f32,
        );
        assert_eq!(purged.stats().num_elements, survivors.len() as u64);
    }

    /// `purge_ids` erases documents whose text is unrecoverable: every posting
    /// entry goes, the counters land exactly where a survivors-only index would
    /// have put them, and the repair survives a flush + reload.
    #[tokio::test]
    async fn test_purge_ids_erases_documents_without_their_text() {
        let index = build_purge_index("purge_ids", None);
        let dead: BTreeSet<u64> = [2, 4].into_iter().collect();

        // Persist first, so every bucket is *clean* when the purge runs: only
        // the purge's own dirty marks can make the repair durable below.
        let mut store = MemStore::default();
        assert!(flush_to(&index, &mut store, 10).await.saved);
        assert!(!index.has_dirty_buckets());

        // Sanity: the dead documents are findable before the purge.
        assert_eq!(index.search("narwhal", 10, None).len(), 1);
        assert_eq!(index.search("shared", 10, None).len(), 6);

        let purged = index.purge_ids(&dead, 42);
        assert_eq!(purged, 2);
        // Re-purging is a no-op: nothing is left to remove.
        assert_eq!(index.purge_ids(&dead, 43), 0);

        // No posting list mentions a purged id anywhere.
        for entry in index.postings.iter() {
            for (doc_id, _) in entry.value().1.iter() {
                assert!(
                    !dead.contains(doc_id),
                    "token {:?} still lists purged doc {doc_id}",
                    entry.key(),
                );
            }
        }
        // Tokens that only the purged documents carried are gone entirely.
        assert!(!index.postings.contains_key("narwhal"));
        assert!(!index.postings.contains_key("delta"));
        // Tokens shared with survivors stay, minus the purged entries.
        assert_eq!(index.postings.get("walrus").unwrap().1.len(), 1);
        assert!(index.search("narwhal", 10, None).is_empty());
        assert!(index.search("delta", 10, None).is_empty());
        assert_eq!(index.search("shared", 10, None).len(), 4);
        assert_eq!(index.stats().last_deleted, 42);

        let survivors = [1, 3, 5, 6];
        assert_matches_reference(&index, &survivors);

        // The purge must be durable: a bucket left clean would resurrect the
        // ids from its stale serialized `doc_tokens` on the next load.
        assert!(index.has_dirty_buckets(), "the purge dirtied nothing");
        assert!(flush_to(&index, &mut store, 100).await.saved);
        let reloaded = load_from(&store).await;
        assert_matches_reference(&reloaded, &survivors);
        assert!(reloaded.search("narwhal", 10, None).is_empty());
        assert_eq!(reloaded.search("shared", 10, None).len(), 4);
        assert!(
            !reloaded.has_dirty_buckets(),
            "reload had to repair postings the purge should have already fixed",
        );
    }

    /// The same repair across a multi-bucket layout: only the buckets that
    /// actually referenced a purged id may be rewritten, and the survivors'
    /// buckets must stay byte-identical.
    #[tokio::test]
    async fn test_purge_ids_dirties_only_affected_buckets() {
        // A tiny overload size forces one token per bucket or so.
        let index = build_purge_index(
            "purge_ids_buckets",
            Some(BM25Config {
                bucket_overload_size: 48,
                ..Default::default()
            }),
        );
        assert!(index.buckets.len() > 1, "expected a multi-bucket layout");

        let mut store = MemStore::default();
        assert!(flush_to(&index, &mut store, 1).await.saved);
        let before = store.buckets.clone();

        let dead: BTreeSet<u64> = [2, 4].into_iter().collect();
        assert_eq!(index.purge_ids(&dead, 2), 2);

        // Buckets that never referenced a purged id must not be rewritten.
        let untouched: Vec<u32> = index
            .buckets
            .iter()
            .filter(|bucket| !bucket.is_dirty())
            .map(|bucket| *bucket.key())
            .collect();
        assert!(!untouched.is_empty(), "the purge dirtied every bucket");
        for bucket_id in &untouched {
            let object = BucketObject {
                bucket_id: *bucket_id,
                generation: index.metadata().buckets[bucket_id],
            };
            let bucket: BucketOwned = cbor2::from_reader(&before[&object][..]).unwrap();
            for id in &dead {
                assert!(!bucket.doc_tokens.contains_key(id));
                assert!(
                    bucket
                        .postings
                        .values()
                        .all(|posting| posting.1.iter().all(|(doc, _)| doc != id)),
                );
            }
        }

        assert!(flush_to(&index, &mut store, 3).await.saved);
        // Nothing durable mentions a purged id any more.
        for data in store.buckets.values() {
            let bucket: BucketOwned = cbor2::from_reader(&data[..]).unwrap();
            for id in &dead {
                assert!(!bucket.doc_tokens.contains_key(id), "stale doc_tokens");
                for (token, posting) in &bucket.postings {
                    assert!(
                        posting.1.iter().all(|(doc, _)| doc != id),
                        "token {token:?} still lists purged doc {id}",
                    );
                }
            }
        }

        let reloaded = load_from(&store).await;
        assert_matches_reference(&reloaded, &[1, 3, 5, 6]);
        assert!(!reloaded.has_dirty_buckets());
    }
}
