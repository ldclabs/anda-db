use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

pub use anda_db_btree::RangeQuery;
pub use anda_db_schema::{Fv, bf16};
pub use anda_db_tfs::BM25Params;

/// A query for searching the database.
///
/// This structure defines the parameters for performing searches against the database,
/// including full-text search, vector search, filtering, and result limiting.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Query {
    /// Full-text or vector search configuration.
    ///
    /// When specified, performs a search on the specified field.
    /// The field must have a TFS (Text Field Search) index built.
    pub search: Option<Search>,

    /// Range filtering configuration.
    ///
    /// When specified, filters results based on field values.
    /// The field must have a B-Tree index built.
    pub filter: Option<Filter>,

    /// Maximum number of results to return.
    ///
    /// Defaults to 10 if not specified.
    pub limit: Option<usize>,
}

/// Configuration for full-text and vector search operations.
///
/// Supports both text-based and vector-based search with optional reranking.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Search {
    /// The text query to search for.
    ///
    /// Used for full-text search operations.
    pub text: Option<String>,

    /// The vector query (field, vector) to search for.
    ///
    /// Used for vector similarity search operations.
    pub vector: Option<Vec<f32>>,

    /// Parameters for the BM25 ranking algorithm.
    ///
    /// Customizes the behavior of the full-text search ranking.
    pub bm25_params: Option<BM25Params>,

    /// Configuration for reranking search results.
    ///
    /// When specified, applies the Reciprocal Rank Fusion algorithm
    /// to combine and rerank results from text and vector searches.
    /// Defaults to `RRFReranker` with k=60 if not specified.
    pub reranker: Option<RRFReranker>,

    /// Whether to use logical search operators.
    ///
    /// When true, the search text can include logical operators (AND, OR, NOT).
    pub logical_search: bool,
}

/// Filter conditions for query results.
///
/// Provides a flexible way to define complex filtering logic
/// using field-based conditions and logical operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Filter {
    /// A single field filter condition.
    ///
    /// Filters based on a range query against a specific field.
    /// Format: (btree_index_name, range_query)
    Field((String, RangeQuery<Fv>)),

    /// A logical OR filter.
    ///
    /// Matches documents that satisfy at least one of the contained filters.
    Or(Vec<Box<Filter>>),

    /// A logical AND filter.
    ///
    /// Matches documents that satisfy all of the contained filters.
    And(Vec<Box<Filter>>),

    /// A logical NOT filter.
    ///
    /// Matches documents that do not satisfy the contained filter.
    Not(Box<Filter>),
}

/// Reranks search results using the Reciprocal Rank Fusion (RRF) algorithm.
///
/// This algorithm combines multiple ranked lists (e.g., from text and vector searches)
/// into a single, unified ranking by considering the position of each document
/// across all lists.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RRFReranker {
    /// The constant factor in the RRF formula.
    ///
    /// Higher values reduce the impact of high rankings,
    /// making the algorithm more forgiving to items that
    /// rank poorly in some lists. Must be positive (typically 60).
    pub k: f32,
}

impl Default for RRFReranker {
    fn default() -> Self {
        Self { k: 60.0 }
    }
}

impl RRFReranker {
    /// Creates a new RRFReranker with the specified k value.
    ///
    /// # Arguments
    /// * `k` - The constant factor in the RRF formula. Must be positive.
    ///
    /// # Panics
    /// Panics if k is not positive (k <= 0).
    pub fn new(k: f32) -> Self {
        assert!(k > 0.0, "RRFReranker k must be positive, got {}", k);
        Self { k }
    }

    /// Reranks results using the Reciprocal Rank Fusion algorithm.
    ///
    /// This method combines multiple ranked lists into a single ranking
    /// by assigning scores based on the position of each document in each list,
    /// then aggregating these scores.
    ///
    /// # Arguments
    /// * `ranked_lists` - A slice containing multiple ordered lists, where each list
    ///   is a Vec of document IDs sorted by relevance.
    ///
    /// # Returns
    /// A Vec of (document_id, score) pairs, sorted by descending score.
    pub fn rerank(&self, ranked_lists: &[Vec<u64>]) -> Vec<(u64, f32)> {
        // Ensure k is positive to avoid division by zero or negative scores
        let k = if self.k > 0.0 { self.k } else { 60.0 };

        let mut scores: FxHashMap<u64, f32> = FxHashMap::default();

        for ranked in ranked_lists {
            for (rank, &doc_id) in ranked.iter().enumerate() {
                let score = 1.0 / (k + rank as f32);
                *scores.entry(doc_id).or_insert(0.0) += score;
            }
        }

        let mut results: Vec<(u64, f32)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
}
