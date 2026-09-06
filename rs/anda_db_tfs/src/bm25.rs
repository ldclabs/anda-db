//! # BM25 index implementation
//!
//! This module contains [`BM25Index`], the concurrent, bucket-sharded BM25
//! index that backs the crate. See the crate-level documentation for a
//! high-level overview.

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
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
const DOC_LOCK_STRIPES: usize = 128;

#[derive(Clone, Copy, PartialEq, Eq)]
enum LoadState {
    MetadataOnly,
    Partial,
    Complete,
}

/// Longest document prefix copied into [`BM25Error::TokenizeFailed`].
const MAX_ERROR_TEXT_BYTES: usize = 256;

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

/// Estimated serialized size of one `doc_tokens` entry of a bucket object.
///
/// A bucket object carries the token count of every document its postings
/// reference (see [`BucketRef`]), so the entry is charged to a bucket the
/// first time a document lands in it and refunded when the document leaves.
fn doc_entry_size(doc_id: u64, token_count: usize) -> usize {
    cbor_serialized_size(&(doc_id, token_count))
}

/// Copies at most [`MAX_ERROR_TEXT_BYTES`] of `text` into an error value,
/// cutting at a char boundary and noting the original length.
fn truncate_error_text(text: &str) -> String {
    if text.len() <= MAX_ERROR_TEXT_BYTES {
        return text.to_string();
    }
    let mut end = MAX_ERROR_TEXT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… [{} bytes total]", &text[..end], text.len())
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
///   with a soft placement target set by [`BM25Config::bucket_overload_size`].
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
pub struct BM25Index<T: Tokenizer> {
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
    /// Lock order: mutation gate, document stripes in ascending order, maps.
    /// A stripe spans membership publication, postings and bucket accounting.
    doc_locks: [Mutex<()>; DOC_LOCK_STRIPES],
    load_state: LoadState,
    /// Manifest entries still on disk, also retained across incremental loads.
    unloaded_buckets: BTreeSet<u32>,
}

#[derive(Default)]
struct Bucket {
    /// Version counter incremented on each modification
    dirty_version: u64,
    /// Version that was last successfully persisted
    saved_version: u64,
    /// Estimated serialized size of the bucket object: postings plus the
    /// per-document token counts it carries. Accumulated from estimates
    /// between flushes; exact right after a flush or a reload.
    size: usize,
    /// Tokens whose posting this bucket owns and serializes.
    tokens: FxHashSet<String>,
    /// Documents whose token count the bucket's durable object may carry: a
    /// superset hint used to mark the bucket dirty when one of them is
    /// removed. Made exact by every flush.
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
///
/// The limit is enforced only when a token is *placed*: a token stays in the
/// bucket that first received it, and that bucket keeps growing by one
/// posting entry for every later document containing the token. Very
/// frequent terms therefore make their buckets grow with the corpus (a term
/// present in every document costs about ten bytes per document), and each
/// of those buckets is rewritten whole by every flush that touches it. With
/// a tokenizer that keeps stop words this is the dominant flush cost on large
/// corpora; stop-word filtering in the tokenizer chain is the effective
/// remedy, and [`BM25Index::compact_buckets`] repacks whole tokens but never
/// splits one.
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
///
/// The list is a plain `Vec`: one entry per `insert`, with no uniqueness
/// enforced on it. A document can appear more than once only after a
/// [`BM25Index::remove`] with non-original text followed by a re-insert of
/// the same id; scoring keys by document id so such a duplicate is scored
/// once, [`BM25Index::remove`] drops every entry of the id, and a reload
/// prunes entries whose document is gone.
///
/// Duplicates of a *live* document are not pruned, though: repeating that
/// cycle appends one entry per round, for good. Nothing de-duplicates them
/// because the only cheap place to do so is the `insert` hot path, where the
/// scan would be linear in the posting length — the cost `UniqueVec` used to
/// pay, in storage, on every posting. Callers that cannot supply the original
/// text should use [`BM25Index::purge_ids`], which needs none.
pub type PostingValue = (u32, Vec<(u64, usize)>);

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
#[derive(Serialize, Deserialize)]
struct BM25IndexOwned {
    metadata: BM25Metadata,
}

#[derive(Serialize)]
struct BM25IndexRef<'a> {
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

mod compact;
mod mutation;
mod persistence;
#[cfg(test)]
mod regression_tests;
mod search;
#[cfg(test)]
mod tests;

impl<T: Tokenizer> BM25Index<T> {
    /// Reinstalls the tokenizer used to build this index when opening it.
    /// Changing tokenization policy requires rebuilding existing postings.
    pub fn set_tokenizer(&mut self, tokenizer: T) {
        self.tokenizer = tokenizer;
    }

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
            doc_locks: std::array::from_fn(|_| Mutex::new(())),
            load_state: LoadState::Complete,
            unloaded_buckets: BTreeSet::new(),
        }
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

    /// Returns metadata with live statistics, or the persisted statistics for
    /// a metadata-only shell. `len()` always counts loaded documents.
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
        // A metadata-only shell intentionally has no live document tables, so
        // preserve their persisted statistics while still exposing counters
        // (such as search_count) that can change without loading buckets.
        if self.load_state == LoadState::MetadataOnly {
            return;
        }
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

    /// Gets the number of tokens for a document by its ID
    pub fn get_doc_tokens(&self, id: u64) -> Option<usize> {
        self.doc_tokens.get(&id).map(|v| *v)
    }

    /// Whether all referenced buckets are loaded and mutations are allowed.
    pub fn is_fully_loaded(&self) -> bool {
        self.load_state == LoadState::Complete
    }

    fn require_loaded(&self) -> Result<(), BM25Error> {
        if self.is_fully_loaded() {
            Ok(())
        } else {
            Err(BM25Error::Generic {
                name: self.name.clone(),
                source: "index is not fully loaded; load all referenced buckets before writing"
                    .into(),
            })
        }
    }

    fn doc_stripe(id: u64) -> usize {
        id as usize % DOC_LOCK_STRIPES
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
