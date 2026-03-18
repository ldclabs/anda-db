//! Library entry point for the Anda DB shard proxy.
//!
//! This crate exposes the reusable building blocks behind the shard proxy
//! binary so the proxy can also be embedded into integration tests or other
//! Rust services.
//!
//! The main responsibilities are split into four modules:
//! - [`handler`]: Axum management and proxy router construction.
//! - [`proxy`]: Reverse-proxy state and request forwarding logic.
//! - [`router`]: Database-name and shard-id extraction rules.
//! - [`store`]: PostgreSQL-backed routing metadata with in-memory caches.

pub mod handler;
pub mod proxy;
pub mod router;
pub mod store;

// Re-export key types for library usage.
pub use proxy::{AppState, DbShardExtractor};
pub use store::ShardStore;
