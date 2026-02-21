//! # Anda-DB BM25 Full-Text Search Library
//!
//! This library implements a full-text search engine based on the BM25 ranking algorithm.
//! BM25 (Best Matching 25) is a ranking function used by search engines to estimate the relevance
//! of documents to a given search query. It's an extension of the TF-IDF model.
//!
//! ## Features
//!
//! - Segment indexing with BM25 scoring
//! - Segment removal
//! - Query search with top-k results
//! - Serialization and deserialization of indices in CBOR format
//! - Customizable tokenization

mod bm25;
mod error;
mod query;
mod tokenizer;

pub use bm25::*;
pub use error::*;
pub use query::*;
pub use tokenizer::*;

#[cfg(any(test, feature = "tantivy-jieba"))]
mod jieba_tokenizer;

#[cfg(any(test, feature = "tantivy-jieba"))]
pub use jieba_tokenizer::*;
