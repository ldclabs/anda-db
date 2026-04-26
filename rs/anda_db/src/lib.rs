//! Core embedded database primitives for building durable AI memory systems.
//!
//! `anda_db` combines three retrieval modes behind a single collection model:
//!
//! - B-Tree indexes for exact match and range filters
//! - BM25 indexes for full-text retrieval
//! - HNSW indexes for vector similarity search
//!
//! The crate is designed for agent memory workloads where data must be:
//!
//! - schema-validated on write
//! - incrementally searchable through multiple index types
//! - persisted to a generic `object_store` backend
//! - recoverable after partial flushes or process restarts
//!
//! Typical usage:
//!
//! 1. Create or connect to an [`database::AndaDB`]
//! 2. Create or open a [`collection::Collection`]
//! 3. Define a schema via [`schema`] or `AndaDBSchema`
//! 4. Add B-Tree, BM25, and/or HNSW indexes
//! 5. Insert documents and query them through [`query`]
//!
//! Feature flags:
//!
//! - `full`: enables full-text search integrations exposed by the workspace setup
//! - `tantivy`: enables the Tantivy-backed text search dependency
//! - `tantivy-jieba`: enables Tantivy plus Jieba tokenization support
//!
//! See also the technical guide in `docs/anda_db.md` for architecture,
//! lifecycle, indexing, and operational guidance.

/// Collection-level document storage, indexing, and query execution.
pub mod collection;
/// Database-level lifecycle and collection management.
pub mod database;
/// Error types returned by the core library.
pub mod error;
/// Index abstractions and concrete B-Tree, BM25, and HNSW integrations.
pub mod index;
/// Query structures for hybrid search, filters, and reranking.
pub mod query;
/// Schema types re-exported from `anda_db_schema`.
pub mod schema;
/// Object-store-backed persistence, compression, and cached I/O.
pub mod storage;

/// Returns the current Unix timestamp in milliseconds.
///
/// The crate uses millisecond timestamps for metadata bookkeeping,
/// collection statistics, and periodic flush coordination.
#[inline]
pub fn unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch");
    ts.as_millis() as u64
}
