//! # Anda-DB HNSW Vector Search Library
//!
//! A high-performance implementation of Hierarchical Navigable Small World (HNSW) algorithm
//! for approximate nearest neighbor search in high-dimensional spaces.
//!
//! HNSW is a graph-based indexing algorithm that creates a multi-layered structure
//! to enable fast and accurate nearest neighbor search in high-dimensional spaces.
//!
//! ## Features
//!
//! - Fast approximate nearest neighbor search;
//! - Multiple distance metrics (Euclidean, Cosine, Inner Product, Manhattan);
//! - Configurable index parameters;
//! - Thread-safe implementation with concurrent read/write operations;
//! - Serialization and deserialization support;
//! - Support for bf16 (brain floating point) vector storage for memory efficiency.
//!

mod config;
mod distance;
mod error;
mod hnsw;
mod node;

pub use config::*;
pub use distance::*;
pub use error::*;
pub use hnsw::*;
pub use node::{HnswNode, serialize_node};
