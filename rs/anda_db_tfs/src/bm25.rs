//! # Anda-DB BM25 Full-Text Search Library

use anda_db_utils::{UniqueVec, estimate_cbor_size};
use dashmap::DashMap;
use parking_lot::RwLock;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use crate::error::*;
use crate::query::*;
use crate::tokenizer::*;

/// BM25 search index with customizable tokenization
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

    /// Average number of tokens per document
    avg_doc_tokens: RwLock<f32>,

    /// Total number of tokens indexed.
    total_tokens: AtomicU64,

    /// Number of search operations performed.
    search_count: AtomicU64,

    /// Last saved version of the index
    last_saved_version: AtomicU64,
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

/// Parameters for the BM25 ranking algorithm
///
/// - `k1`: Controls term frequency saturation. Higher values give more weight to term frequency.
/// - `b`: Controls document length normalization. 0.0 means no normalization, 1.0 means full normalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BM25Params {
    pub k1: f32,
    pub b: f32,
}

impl Default for BM25Params {
    /// Returns default BM25 parameters (k1=1.2, b=0.75) which work well for most use cases
    fn default() -> Self {
        BM25Params { k1: 1.2, b: 0.75 }
    }
}

/// Configuration parameters for the BM25 index
///
/// - `bm25`: BM25 algorithm parameters
/// - `bucket_overload_size`: Maximum size of a bucket before creating a new one (in bytes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BM25Config {
    pub bm25: BM25Params,
    /// Maximum size of a bucket before creating a new one
    /// When a bucket's stored data exceeds this size,
    /// a new bucket should be created for new data
    pub bucket_overload_size: usize,
}

impl Default for BM25Config {
    /// Returns default BM25 parameters (k1=1.2, b=0.75) which work well for most use cases
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
            buckets: DashMap::from_iter(vec![(0, Bucket::default())]),
            metadata: RwLock::new(BM25Metadata {
                name,
                config,
                stats,
            }),
            max_bucket_id: AtomicU32::new(0),
            max_document_id: AtomicU64::new(0),
            avg_doc_tokens: RwLock::new(0.0),
            total_tokens: AtomicU64::new(0),
            search_count: AtomicU64::new(0),
            last_saved_version: AtomicU64::new(0),
        }
    }

    /// Loads an index from metadata reader and closure for loading documents and postings.
    ///
    /// # Arguments
    ///
    /// * `tokenizer` - Tokenizer to use for processing text
    /// * `metadata` - Metadata reader
    /// * `f1` - Closure for loading documents
    /// * `f2` - Closure for loading postings
    ///
    /// # Returns
    ///
    /// * `Result<Self, HnswError>` - Loaded index or error.
    pub async fn load_all<R: Read, F>(tokenizer: T, metadata: R, f: F) -> Result<Self, BM25Error>
    where
        F: AsyncFnMut(u32) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut index = Self::load_metadata(tokenizer, metadata)?;
        index.load_buckets(f).await?;
        Ok(index)
    }

    /// Loads an index from a reader
    /// This only loads metadata, you need to call [`Self::load_buckets`] to load the actual posting data.
    ///
    /// # Arguments
    ///
    /// * `tokenizer` - Tokenizer to use with the loaded index
    /// * `r` - Any type implementing the [`Read`] trait
    ///
    /// # Returns
    ///
    /// * `Result<(), BM25Error>` - Success or error.
    pub fn load_metadata<R: Read>(tokenizer: T, r: R) -> Result<Self, BM25Error> {
        let index: BM25IndexOwned =
            ciborium::from_reader(r).map_err(|err| BM25Error::Serialization {
                name: "unknown".to_string(),
                source: err.into(),
            })?;
        let max_bucket_id = AtomicU32::new(index.metadata.stats.max_bucket_id);
        let max_document_id = AtomicU64::new(index.metadata.stats.max_document_id);
        let search_count = AtomicU64::new(index.metadata.stats.search_count);
        let avg_doc_tokens = RwLock::new(index.metadata.stats.avg_doc_tokens);
        let last_saved_version = AtomicU64::new(index.metadata.stats.version);

        Ok(BM25Index {
            name: index.metadata.name.clone(),
            tokenizer,
            config: index.metadata.config.clone(),
            doc_tokens: DashMap::new(),
            postings: DashMap::new(),
            buckets: DashMap::from_iter(vec![(0, Bucket::default())]),
            metadata: RwLock::new(index.metadata),
            max_bucket_id,
            max_document_id,
            avg_doc_tokens,
            search_count,
            last_saved_version,
            total_tokens: AtomicU64::new(0),
        })
    }

    /// Loads data from buckets using the provided async function
    /// This function should be called during database startup to load all document data
    /// and form a complete document index
    ///
    /// # Arguments
    ///
    /// * `f` - Async function that reads posting data from a specified bucket.
    ///   `F: AsyncFn(u64) -> Result<Option<Vec<u8>>, BTreeError>`
    ///   The function should take a bucket ID as input and return a vector of bytes
    ///   containing the serialized bucket data. If the bucket does not exist,
    ///   it should return `Ok(None)`.
    ///
    /// # Returns
    ///
    /// * `Result<(), BTreeError>` - Success or error
    pub async fn load_buckets<F>(&mut self, mut f: F) -> Result<(), BM25Error>
    where
        F: AsyncFnMut(u32) -> Result<Option<Vec<u8>>, BoxError>,
    {
        for i in 0..=self.max_bucket_id.load(Ordering::Relaxed) {
            let data = f(i).await.map_err(|err| BM25Error::Generic {
                name: self.name.clone(),
                source: err,
            })?;
            if let Some(data) = data {
                let bucket: BucketOwned =
                    ciborium::from_reader(&data[..]).map_err(|err| BM25Error::Serialization {
                        name: self.name.clone(),
                        source: err.into(),
                    })?;

                let mut b = Bucket {
                    size: data.len(),
                    ..Default::default()
                };
                if !bucket.doc_tokens.is_empty() {
                    b.doc_ids = bucket.doc_tokens.keys().cloned().collect();
                    self.doc_tokens.extend(bucket.doc_tokens);
                }

                if !bucket.postings.is_empty() {
                    b.tokens = bucket.postings.keys().cloned().collect();
                    self.postings.extend(bucket.postings);
                }

                self.buckets.insert(i, b);
            }
        }

        let total_tokens: usize = self.doc_tokens.iter().map(|r| *r.value()).sum();
        self.total_tokens
            .store(total_tokens as u64, Ordering::Relaxed);

        let doc_count = self.doc_tokens.len();
        let avg = if doc_count == 0 {
            0.0
        } else {
            total_tokens as f32 / doc_count as f32
        };
        *self.avg_doc_tokens.write() = avg;

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
        metadata.stats.search_count = self.search_count.load(Ordering::Relaxed);
        metadata.stats.num_elements = self.doc_tokens.len() as u64;
        metadata.stats.max_bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
        metadata.stats.max_document_id = self.max_document_id.load(Ordering::Relaxed);
        metadata.stats.avg_doc_tokens = *self.avg_doc_tokens.read();
        metadata
    }

    /// Gets current statistics about the index
    ///
    /// # Returns
    ///
    /// * `IndexStats` - Current statistics
    pub fn stats(&self) -> BM25Stats {
        let mut stats = { self.metadata.read().stats.clone() };
        stats.search_count = self.search_count.load(Ordering::Relaxed);
        stats.num_elements = self.doc_tokens.len() as u64;
        stats.max_bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
        stats.max_document_id = self.max_document_id.load(Ordering::Relaxed);
        stats.avg_doc_tokens = *self.avg_doc_tokens.read();
        stats
    }

    /// Inserts a document to the index
    ///
    /// # Arguments
    ///
    /// * `id` - Unique document identifier
    /// * `text` - Segment text content
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the document was successfully added
    /// * `Err(BM25Error)` if failed
    pub fn insert(&self, id: u64, text: &str, now_ms: u64) -> Result<(), BM25Error> {
        if self.doc_tokens.contains_key(&id) {
            return Err(BM25Error::AlreadyExists {
                name: self.name.clone(),
                id,
            });
        }

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

        let _ = self.max_document_id.fetch_max(id, Ordering::Relaxed);

        // Phase 1: Update the postings collection
        let bucket_id = self.max_bucket_id.load(Ordering::Acquire);
        let prev_docs = self.doc_tokens.len();
        let tokens: usize = token_freqs.values().sum();
        // buckets_to_update: BTreeMap<bucketid, FxHashMap<token, size_increase>>
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

                {
                    // Calculate new average document length
                    let prev_total = self
                        .total_tokens
                        .fetch_add(tokens as u64, Ordering::Relaxed);
                    let new_avg = (prev_total + tokens as u64) as f32 / (prev_docs + 1) as f32;
                    *self.avg_doc_tokens.write() = new_avg;
                }

                // Update inverted index
                for (token, freq) in token_freqs {
                    match self.postings.entry(token.clone()) {
                        dashmap::Entry::Occupied(mut entry) => {
                            let val = (id, freq);
                            let size_increase = estimate_cbor_size(&val) + 2;
                            let e = entry.get_mut();
                            e.1.push(val);
                            let b = buckets_to_update.entry(e.0).or_default();
                            b.insert(token, size_increase);
                        }
                        dashmap::Entry::Vacant(entry) => {
                            // Create new posting
                            let val = (bucket_id, vec![(id, freq)].into());
                            let size_increase =
                                estimate_cbor_size(&(&token, (bucket_id, &[(id, freq)]))) + 2;
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
            for (token, size) in val {
                if bucket.tokens.contains(&token) {
                    // Token already tracked in this bucket; just account for the new posting entry.
                    bucket.size += size;
                } else if bucket.tokens.is_empty()
                    || bucket.size + size < self.config.bucket_overload_size
                {
                    bucket.tokens.push(token);
                    bucket.size += size;
                } else {
                    tokens_to_migrate.push((bid, token, size));
                }
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
                }
            }

            self.buckets
                .entry(next_bucket_id)
                .or_default()
                .doc_ids
                .insert(id);
        } else {
            let mut b = self.buckets.entry(bucket_id).or_default();
            b.mark_dirty();
            b.doc_ids.insert(id);
        }

        self.update_metadata(|m| {
            m.stats.version += 1;
            m.stats.last_inserted = now_ms;
            m.stats.insert_count += 1;
        });

        Ok(())
    }

    /// Removes a document from the index
    ///
    /// # Arguments
    ///
    /// * `id` - Segment identifier to remove
    /// * `text` - Original document text (needed to identify tokens to remove)
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `true` if the document was found and removed
    /// * `false` if the document was not found
    pub fn remove(&self, id: u64, text: &str, now_ms: u64) -> bool {
        let removed_tokens = match self.doc_tokens.remove(&id) {
            Some((_k, v)) => v,
            None => return false,
        };

        {
            // Recalculate average document length
            let prev_total = self
                .total_tokens
                .fetch_sub(removed_tokens as u64, Ordering::Relaxed);
            let new_total = prev_total.saturating_sub(removed_tokens as u64);
            let remaining = self.doc_tokens.len();
            let new_avg = if remaining == 0 {
                0.0
            } else {
                new_total as f32 / remaining as f32
            };
            *self.avg_doc_tokens.write() = new_avg;
        }

        // Tokenize the document
        let token_freqs = {
            let mut tokenizer = self.tokenizer.clone();
            collect_tokens(&mut tokenizer, text, None)
        };

        // buckets_to_update: FxHashMap<bucketid, FxHashMap<token, size_decrease>>
        let mut buckets_to_update: FxHashMap<u32, FxHashMap<String, usize>> = FxHashMap::default();
        // Remove from inverted index
        let mut tokens_to_remove: FxHashSet<String> = FxHashSet::default();
        for (token, _) in token_freqs {
            if let Some(mut posting) = self.postings.get_mut(&token) {
                // Remove document from postings list
                if let Some(val) = posting.1.swap_remove_if(|&(idx, _)| idx == id) {
                    let mut size_decrease = estimate_cbor_size(&val) + 2;
                    if posting.1.is_empty() {
                        size_decrease =
                            estimate_cbor_size(&(&token, (posting.0, &[(val.0, val.1)]))) + 2;
                        tokens_to_remove.insert(token.clone());
                    }
                    let b = buckets_to_update.entry(posting.0).or_default();
                    b.insert(token, size_decrease);
                }
            }
        }

        for token in tokens_to_remove.iter() {
            self.postings.remove(token);
        }

        let mut removed_id = false;
        for (bucket_id, val) in buckets_to_update {
            if let Some(mut b) = self.buckets.get_mut(&bucket_id) {
                // Mark as dirty, needs to be persisted
                b.mark_dirty();
                for (token, size_decrease) in val {
                    b.size = b.size.saturating_sub(size_decrease);
                    if tokens_to_remove.contains(&token) {
                        b.tokens.swap_remove_if(|k| &token == k);
                    }
                }
                removed_id = removed_id || b.doc_ids.remove(&id);
            }
        }

        if !removed_id {
            for mut bucket in self.buckets.iter_mut() {
                if bucket.doc_ids.remove(&id) {
                    bucket.mark_dirty();
                    break;
                }
            }
        }

        self.update_metadata(|m| {
            m.stats.version += 1;
            m.stats.last_deleted = now_ms;
            m.stats.delete_count += 1;
        });

        true
    }

    /// Searches the index for documents matching the query
    ///
    /// # Arguments
    ///
    /// * `query` - Search query text
    /// * `top_k` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A vector of (document_id, score) pairs, sorted by descending score
    pub fn search(&self, query: &str, top_k: usize, params: Option<BM25Params>) -> Vec<(u64, f32)> {
        let params = params.as_ref().unwrap_or(&self.config.bm25);
        let scored_docs = self.score_term(query.trim(), params);

        self.search_count.fetch_add(1, Ordering::Relaxed);
        Self::top_k_results(scored_docs, top_k)
    }

    /// Searches the index for documents matching the query expression
    ///
    /// # Arguments
    ///
    /// * `query` - Search query text, which can include boolean operators (OR, AND, NOT), example:
    ///   `(hello AND world) OR (rust AND NOT java)`
    /// * `top_k` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A vector of (document_id, score) pairs, sorted by descending score
    pub fn search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Vec<(u64, f32)> {
        let query_expr = QueryType::parse(query);
        let params = params.as_ref().unwrap_or(&self.config.bm25);
        let scored_docs = self.execute_query(&query_expr, params, false);

        self.search_count.fetch_add(1, Ordering::Relaxed);
        Self::top_k_results(scored_docs, top_k)
    }

    /// Extracts the top-k results from scored documents using partial sorting.
    /// Uses `select_nth_unstable_by` for O(n + k·log(k)) instead of O(n·log(n)).
    fn top_k_results(scored_docs: FxHashMap<u64, f32>, top_k: usize) -> Vec<(u64, f32)> {
        if top_k == 0 || scored_docs.is_empty() {
            return Vec::new();
        }

        let mut results: Vec<(u64, f32)> = scored_docs.into_iter().collect();
        if results.len() > top_k {
            results.select_nth_unstable_by(top_k - 1, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(top_k);
        }
        results.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
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
        let k1 = params.k1.max(0.0);
        let b = params.b.clamp(0.0, 1.0);

        let mut tokenizer = self.tokenizer.clone();
        let query_terms = collect_tokens(&mut tokenizer, term, None);
        if query_terms.is_empty() {
            return FxHashMap::default();
        }

        let mut scores: FxHashMap<u64, f32> =
            FxHashMap::with_capacity_and_hasher(self.doc_tokens.len().min(1000), FxBuildHasher);
        let doc_count = self.doc_tokens.len() as f32;
        let avg_doc_tokens = *self.avg_doc_tokens.read();
        let avg_doc_tokens = avg_doc_tokens.max(1.0);

        for query_token in query_terms.keys() {
            if let Some(postings) = self.postings.get(query_token) {
                // Two-pass approach: first count valid docs for IDF, then compute scores.
                // This avoids allocating an intermediate Vec per query term.
                // Filter out deleted / not-loaded documents.
                // `remove()` depends on the caller providing original text; if they don't,
                // postings can become stale. Also, when only part of buckets are loaded,
                // postings might contain docs missing in `doc_tokens`.
                let mut doc_freq: usize = 0;
                for (doc_id, _) in postings.1.iter() {
                    if self.doc_tokens.contains_key(doc_id) {
                        doc_freq += 1;
                    }
                }

                if doc_freq == 0 || doc_count == 0.0 {
                    continue;
                }

                // Classic Okapi BM25: ln(1 + (N - df + 0.5)/(df + 0.5))
                let df = doc_freq as f32;
                let idf = ((doc_count - df + 0.5) / (df + 0.5) + 1.0).ln();

                // Compute BM25 score for each valid document
                for (doc_id, token_freq) in postings.1.iter() {
                    if let Some(doc_len) = self.doc_tokens.get(doc_id).map(|v| *v as f32) {
                        let tf = *token_freq as f32;
                        let tf_component = (tf * (k1 + 1.0))
                            / (tf + k1 * (1.0 - b + b * doc_len / avg_doc_tokens));
                        *scores.entry(*doc_id).or_default() += idf * tf_component;
                    }
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

        // Execute the first subquery
        let mut result = self.execute_query(&subqueries[0], params, false);
        if result.is_empty() {
            return FxHashMap::default();
        }

        // Execute the remaining subqueries and intersect the results
        for subquery in &subqueries[1..] {
            let sub_result = self.execute_query(subquery, params, true);
            if matches!(subquery.as_ref(), QueryType::Not(_)) {
                // handle NOT query, remove it from the result
                for doc_id in sub_result.keys() {
                    result.remove(doc_id);
                }
                continue;
            }

            // Retain only documents that are in both results
            result.retain(|k, _| sub_result.contains_key(k));
            if result.is_empty() {
                return FxHashMap::default();
            }

            // Merge scores
            for (doc_id, score) in sub_result {
                result.entry(doc_id).and_modify(|s| *s += score);
            }
        }

        result
    }

    /// Scores a NOT query
    fn score_not(
        &self,
        subquery: &QueryType,
        params: &BM25Params,
        negated_not: bool,
    ) -> FxHashMap<u64, f32> {
        let exclude = self.execute_query(subquery, params, negated_not);
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

    /// Stores the index metadata, IDs and nodes to persistent storage.
    pub async fn flush<W: Write, F>(
        &self,
        metadata: W,
        now_ms: u64,
        f: F,
    ) -> Result<bool, BM25Error>
    where
        F: AsyncFnMut(u32, &[u8]) -> Result<bool, BoxError>,
    {
        let meta_saved = self.store_metadata(metadata, now_ms)?;
        let has_dirty = self.has_dirty_buckets();
        if !meta_saved && !has_dirty {
            return Ok(false);
        }

        self.store_dirty_buckets(f).await?;
        Ok(meta_saved || has_dirty)
    }

    /// Returns whether there are dirty buckets pending persistence.
    pub fn has_dirty_buckets(&self) -> bool {
        self.buckets.iter().any(|b| b.is_dirty())
    }

    /// Compacts fragmented buckets by re-binning all tokens into fewer, properly-sized
    /// buckets using a first-fit-decreasing bin-packing strategy.
    ///
    /// This is intended as a one-time repair after the bucket-splitting bug that created
    /// many tiny buckets. After compaction all buckets are marked dirty and will be
    /// persisted on the next [`flush`](Self::flush) call.
    ///
    /// # Returns
    ///
    /// `(old_bucket_count, new_bucket_count)`
    pub fn compact_buckets(&self) -> (usize, usize) {
        let old_count = self.buckets.len();
        if old_count <= 1 {
            return (old_count, old_count);
        }

        // Step 1: Estimate each token's serialized contribution.
        let mut token_sizes: Vec<(String, usize)> = self
            .postings
            .iter()
            .map(|entry| {
                let size = estimate_cbor_size(&(entry.key(), entry.value())) + 2;
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
        token_sizes.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        // Step 3: First-fit-decreasing bin packing.
        let limit = self.config.bucket_overload_size;
        // Each bin: (accumulated_size, tokens)
        let mut bins: Vec<(usize, Vec<String>)> = Vec::new();

        for (token, size) in token_sizes {
            if let Some(bin) = bins.iter_mut().find(|b| b.0 + size < limit) {
                bin.0 += size;
                bin.1.push(token);
            } else {
                bins.push((size, vec![token]));
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

    /// Stores the index metadata to a writer in CBOR format.
    ///
    /// # Arguments
    ///
    /// * `w` - Any type implementing the [`Write`] trait
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `Result<bool, BM25Error>` - true if the metadata was saved, false if the version was not updated
    pub fn store_metadata<W: Write>(&self, w: W, now_ms: u64) -> Result<bool, BM25Error> {
        let current_version = { self.metadata.read().stats.version };
        if self.last_saved_version.load(Ordering::Relaxed) >= current_version {
            return Ok(false);
        }

        let mut meta = self.metadata();
        let prev_saved_version = self
            .last_saved_version
            .fetch_max(meta.stats.version, Ordering::Relaxed);
        if prev_saved_version >= meta.stats.version {
            // No need to save if the version is not updated
            return Ok(false);
        }

        meta.stats.last_saved = now_ms.max(meta.stats.last_saved);

        if let Err(err) = ciborium::into_writer(&BM25IndexRef { metadata: &meta }, w) {
            // Serialization failed: revert only if this call still owns the claimed version.
            let _ = self.last_saved_version.compare_exchange(
                meta.stats.version,
                prev_saved_version,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            return Err(BM25Error::Serialization {
                name: self.name.clone(),
                source: err.into(),
            });
        }

        self.update_metadata(|m| {
            m.stats.last_saved = meta.stats.last_saved.max(m.stats.last_saved);
        });

        Ok(true)
    }

    /// Stores dirty buckets to persistent storage using the provided async function.
    /// Serializes each dirty bucket synchronously and releases all DashMap locks
    /// before making async persistence calls to minimize lock contention.
    pub async fn store_dirty_buckets<F>(&self, mut f: F) -> Result<(), BM25Error>
    where
        F: AsyncFnMut(u32, &[u8]) -> Result<bool, BoxError>,
    {
        // Collect dirty bucket IDs to avoid holding iter locks during async calls
        let dirty_buckets: Vec<(u32, u64)> = self
            .buckets
            .iter()
            .filter(|b| b.is_dirty())
            .map(|b| (*b.key(), b.dirty_version))
            .collect();

        let mut buf = Vec::with_capacity(4096);
        for (bucket_id, snapshot_version) in dirty_buckets {
            // Serialize within a scoped block to release all DashMap locks before async call
            {
                let bucket = match self.buckets.get(&bucket_id) {
                    Some(b) if b.is_dirty() => b,
                    _ => continue,
                };

                let postings: FxHashMap<_, _> = bucket
                    .tokens
                    .iter()
                    .filter_map(|k| self.postings.get(k).map(|v| (k, v)))
                    .collect();

                let doc_tokens: FxHashMap<_, _> = bucket
                    .doc_ids
                    .iter()
                    .filter_map(|id| self.doc_tokens.get(id).map(|v| (*id, *v)))
                    .collect();

                buf.clear();
                ciborium::into_writer(
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
            } // All DashMap Ref/RefMut guards dropped here

            let conti = f(bucket_id, &buf).await.map_err(|err| BM25Error::Generic {
                name: self.name.clone(),
                source: err,
            })?;

            // Use version-based dirty tracking: only mark as saved up to the snapshot version.
            // If another write incremented dirty_version after our snapshot, the bucket
            // will remain dirty and be re-persisted on the next flush.
            if let Some(mut b) = self.buckets.get_mut(&bucket_id) {
                b.saved_version = b.saved_version.max(snapshot_version);
            }

            if !conti {
                return Ok(());
            }
        }

        Ok(())
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
    use std::sync::Arc;
    use tokio::sync::Mutex;

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

        // 创建临时文件
        let mut metadata: Vec<u8> = Vec::new();
        let mut buckets: HashMap<u32, Vec<u8>> = HashMap::new();

        // 保存索引
        index
            .flush(&mut metadata, 0, async |id: u32, data: &[u8]| {
                buckets.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap();

        // 加载索引
        let tokenizer = default_tokenizer();
        let loaded_index = BM25Index::load_all(tokenizer, &metadata[..], async |id| {
            Ok(buckets.get(&id).cloned())
        })
        .await
        .unwrap();

        // 验证加载的索引
        assert_eq!(loaded_index.len(), index.len());

        // 验证搜索结果
        let mut original_results = index.search("fox", 10, None);
        let mut loaded_results = loaded_index.search("fox", 10, None);

        assert_eq!(original_results.len(), loaded_results.len());
        original_results.sort_by(|a, b| a.0.cmp(&b.0));
        loaded_results.sort_by(|a, b| a.0.cmp(&b.0));
        // 比较文档ID和分数（允许浮点数有小误差）
        for i in 0..original_results.len() {
            assert_eq!(original_results[i].0, loaded_results[i].0);
            assert!((original_results[i].1 - loaded_results[i].1).abs() < 0.001);
        }
    }

    #[tokio::test]
    async fn test_flush_persists_dirty_buckets_even_if_metadata_unchanged() {
        let index = create_test_index();
        index.insert(99, "new fox document", 1).unwrap();

        let mut metadata_buf = Vec::new();
        assert!(index.store_metadata(&mut metadata_buf, 2).unwrap());
        assert!(index.has_dirty_buckets());

        let writes = Arc::new(Mutex::new(0usize));
        let writes_clone = writes.clone();
        let mut metadata_buf2 = Vec::new();
        let saved = index
            .flush(&mut metadata_buf2, 3, async move |_, _| {
                let mut g = writes_clone.lock().await;
                *g += 1;
                Ok(true)
            })
            .await
            .unwrap();

        assert!(saved);
        assert!(*writes.lock().await > 0);
        assert!(!index.has_dirty_buckets());
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

        // 创建存储映射
        let mut metadata: Vec<u8> = Vec::new();
        let mut buckets: HashMap<u32, Vec<u8>> = HashMap::new();

        // 保存索引
        index
            .flush(&mut metadata, 100, async |id: u32, data: &[u8]| {
                println!("Saving bucket {}, size: {}", id, data.len());
                buckets.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap();

        // 验证保存了正确数量的桶
        println!("Saved {} document buckets", buckets.len());
        assert!(buckets.len() > 1, "应该保存了多个文档桶");

        // 验证每个桶的内容
        for (bucket_id, data) in &buckets {
            let bucket: BucketOwned = ciborium::from_reader(&data[..]).unwrap();
            println!("Document bucket {bucket_id} {:?}", bucket.doc_tokens);
            assert!(!bucket.postings.is_empty());

            // 验证倒排索引结构
            for (term, (bucket_ref, doc_list)) in bucket.postings {
                assert_eq!(
                    bucket_ref, *bucket_id,
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
        let tokenizer2 = default_tokenizer();
        let loaded_index = BM25Index::load_all(tokenizer2, &metadata[..], async |id| {
            println!("Loading for bucket {}", id);
            Ok(buckets.get(&id).cloned())
        })
        .await
        .unwrap();

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
            orig_sorted.sort_by(|a, b| a.0.cmp(&b.0));
            loaded_sorted.sort_by(|a, b| a.0.cmp(&b.0));

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
            // 测试只加载部分桶的情况
            let tokenizer = default_tokenizer();
            let partial_index = BM25Index::load_all(tokenizer, &metadata[..], async |id| {
                // 只加载桶0的文档
                if id == 0 {
                    Ok(buckets.get(&id).cloned())
                } else {
                    Ok(None)
                }
            })
            .await
            .unwrap();

            // 部分加载的索引应该只包含桶0中的文档
            assert!(partial_index.len() < index.len());

            // 验证部分搜索结果
            let partial_results = partial_index.search("fox", 10, None);
            let full_results = index.search("fox", 10, None);

            // 部分结果应该是完整结果的子集
            assert!(partial_results.len() < full_results.len());

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
        let mut metadata_buf = Vec::new();
        let mut bucket_data: HashMap<u32, Vec<u8>> = HashMap::new();
        index
            .flush(&mut metadata_buf, 1, async |id: u32, data: &[u8]| {
                bucket_data.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap();

        // Reload with the correct (large) bucket limit
        let mut loaded = BM25Index::load_metadata(default_tokenizer(), &metadata_buf[..]).unwrap();
        loaded.config.bucket_overload_size = 1024 * 512;
        loaded.metadata.write().config.bucket_overload_size = 1024 * 512;
        loaded
            .load_buckets(async |id| Ok(bucket_data.get(&id).cloned()))
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
            before_sorted.sort_by(|a, b| a.0.cmp(&b.0));
            after_sorted.sort_by(|a, b| a.0.cmp(&b.0));
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
        let mut metadata_buf2 = Vec::new();
        let mut bucket_data2: HashMap<u32, Vec<u8>> = HashMap::new();
        loaded
            .flush(&mut metadata_buf2, 200, async |id: u32, data: &[u8]| {
                bucket_data2.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap();

        let final_loaded =
            BM25Index::load_all(default_tokenizer(), &metadata_buf2[..], async |id| {
                Ok(bucket_data2.get(&id).cloned())
            })
            .await
            .unwrap();
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
}
