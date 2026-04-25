//! # Anda-DB BM25 Full-Text Search Library
//!
//! `anda_db_tfs` is a thread-safe, embeddable full-text search engine based on
//! the [Okapi BM25](https://en.wikipedia.org/wiki/Okapi_BM25) ranking algorithm.
//! It is the text-indexing component of AndaDB and is specifically designed to
//! back the long-term textual memory of AI agents.
//!
//! ## Features
//!
//! - **BM25 ranking** with configurable `k1` and `b` parameters.
//! - **Composable tokenization**: plug any [`Tokenizer`] — including chains of
//!   filters — into the same index. Built-in helpers cover Latin/Cyrillic/Arabic
//!   text and Chinese via [jieba](https://github.com/messense/tantivy-jieba).
//! - **Boolean query language** with `AND`, `OR`, `NOT`, and parentheses,
//!   exposed through [`BM25Index::search_advanced`].
//! - **Concurrent reads and writes** powered by [`dashmap`] + atomic counters,
//!   so inserts, removes, and searches can run from multiple threads.
//! - **Incremental persistence**: the inverted index is sharded into *buckets*
//!   of bounded CBOR size; only dirty buckets are re-written on
//!   [`BM25Index::flush`].
//! - **Bucket compaction** via [`BM25Index::compact_buckets`] to repack a
//!   fragmented index into the minimum number of buckets.
//!
//! ## Quick start
//!
//! ```no_run
//! use anda_db_tfs::{BM25Index, default_tokenizer};
//!
//! let index = BM25Index::new("notes".to_string(), default_tokenizer(), None);
//! index.insert(1, "The quick brown fox jumps over the lazy dog", 0).unwrap();
//! index.insert(2, "A fast brown fox runs past the lazy dog", 0).unwrap();
//!
//! let hits = index.search("fox", 10, None);
//! for (doc_id, score) in hits {
//!     println!("doc {doc_id}: {score}");
//! }
//! ```
//!
//! See the [README](https://github.com/ldclabs/anda-db/tree/main/rs/anda_db_tfs)
//! and `docs/anda_db_tfs.md` for a full technical overview.

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
