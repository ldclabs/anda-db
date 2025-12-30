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
    // Indicates if the bucket has new data that needs to be persisted
    is_dirty: bool,
    // Current size of the bucket in bytes
    size: usize,
    // List of tokens stored in this bucket
    tokens: UniqueVec<String>,
    // Set of document IDs associated with this bucket
    doc_ids: FxHashSet<u64>,
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
        for (id, val) in buckets_to_update {
            let mut bucket = self.buckets.entry(id).or_default();
            // Mark as dirty, needs to be persisted
            bucket.is_dirty = true;
            for (token, size) in val {
                if bucket.tokens.is_empty() || bucket.size + size < self.config.bucket_overload_size
                {
                    if bucket.tokens.push(token) {
                        bucket.size += size;
                    }
                } else {
                    tokens_to_migrate.push((id, token, size));
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
                    ob.is_dirty = true;
                }

                let mut next_new_bucket = false;
                {
                    let mut nb = self.buckets.entry(next_bucket_id).or_default();

                    if nb.tokens.is_empty() || nb.size + size < self.config.bucket_overload_size {
                        // Bucket has enough space, update directly
                        nb.is_dirty = true;
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
                    nb.is_dirty = true;
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
            b.is_dirty = true;
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
                b.is_dirty = true;
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
                    bucket.is_dirty = true;
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
        let mut sorted_scores: Vec<(u64, f32)> = scored_docs.into_iter().collect();
        // Convert to vector and sort by score (descending)
        sorted_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        sorted_scores.truncate(top_k);
        sorted_scores
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
        // Convert to vector and sort by score (descending)
        let mut results: Vec<(u64, f32)> = scored_docs.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(top_k);

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

    /// Scores a single term
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
        let avg_doc_tokens = self.avg_doc_tokens.read().max(1.0);
        let term_scores: Vec<FxHashMap<u64, f32>> = query_terms
            .iter()
            .filter_map(|(term, _)| {
                self.postings.get(term).map(|postings| {
                    // Filter out deleted / not-loaded documents.
                    // `remove()` depends on the caller providing original text; if they don't,
                    // postings can become stale. Also, when only part of buckets are loaded,
                    // postings might contain docs missing in `doc_tokens`.
                    let mut docs: Vec<(u64, usize, f32)> = Vec::with_capacity(postings.1.len());
                    for (doc_id, token_freq) in postings.1.iter() {
                        if let Some(tokens) = self.doc_tokens.get(doc_id).map(|v| *v as f32) {
                            docs.push((*doc_id, *token_freq, tokens));
                        }
                    }

                    let doc_freq = docs.len() as f32;
                    if doc_freq == 0.0 || doc_count == 0.0 {
                        return FxHashMap::default();
                    }

                    // Classic Okapi BM25: ln(1 + (N - df + 0.5)/(df + 0.5))
                    let idf_raw = (doc_count - doc_freq + 0.5) / (doc_freq + 0.5);
                    let idf = (idf_raw + 1.0).ln();

                    // compute BM25 score for each document
                    let mut term_scores = FxHashMap::default();
                    for (doc_id, token_freq, tokens) in docs {
                        let token_freq_f = token_freq as f32;
                        let tf_component = (token_freq_f * (k1 + 1.0))
                            / (token_freq_f + k1 * (1.0 - b + b * tokens / avg_doc_tokens));

                        term_scores.insert(doc_id, idf * tf_component);
                    }
                    term_scores
                })
            })
            .collect();

        // merge term scores into a single score for each document
        for term_score in term_scores {
            for (doc_id, score) in term_score {
                *scores.entry(doc_id).or_default() += score;
            }
        }

        scores
    }

    /// Scores an OR query
    fn score_or(&self, subqueries: &[Box<QueryType>], params: &BM25Params) -> FxHashMap<u64, f32> {
        let mut result = FxHashMap::default();
        if subqueries.is_empty() {
            return result;
        }

        // Execute all subqueries and merge results
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
        if !self.store_metadata(metadata, now_ms)? {
            return Ok(false);
        }

        self.store_dirty_buckets(f).await?;
        Ok(true)
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
        let mut meta = self.metadata();
        let prev_saved_version = self
            .last_saved_version
            .fetch_max(meta.stats.version, Ordering::Release);
        if prev_saved_version >= meta.stats.version {
            // No need to save if the version is not updated
            return Ok(false);
        }

        meta.stats.last_saved = now_ms.max(meta.stats.last_saved);

        ciborium::into_writer(&BM25IndexRef { metadata: &meta }, w).map_err(|err| {
            BM25Error::Serialization {
                name: self.name.clone(),
                source: err.into(),
            }
        })?;

        self.update_metadata(|m| {
            m.stats.last_saved = meta.stats.last_saved.max(m.stats.last_saved);
        });

        Ok(true)
    }

    /// Stores dirty buckets to persistent storage using the provided async function
    pub async fn store_dirty_buckets<F>(&self, mut f: F) -> Result<(), BM25Error>
    where
        F: AsyncFnMut(u32, &[u8]) -> Result<bool, BoxError>,
    {
        let mut buf = Vec::with_capacity(4096);
        for mut bucket in self.buckets.iter_mut() {
            if bucket.is_dirty {
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

                let conti = f(*bucket.key(), &buf)
                    .await
                    .map_err(|err| BM25Error::Generic {
                        name: self.name.clone(),
                        source: err,
                    })?;

                // Only mark as clean if persistence was successful.
                bucket.is_dirty = false;
                if !conti {
                    return Ok(());
                }
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
}
