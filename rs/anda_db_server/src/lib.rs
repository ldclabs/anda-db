//! # Anda DB Server
//!
//! An HTTP service wrapping the embedded [`anda_db`] engine. The API is a
//! lightweight RPC over HTTP POST with **CBOR as the primary encoding**
//! (JSON is supported for debugging and non-CBOR clients).
//!
//! ## Routes
//!
//! | Route | Description |
//! |-------|-------------|
//! | `GET /` | Unauthenticated health/info (name, version) |
//! | `POST /` | Root-scope methods (server info, database lifecycle) |
//! | `POST /{db_name}` | Database-scoped methods (`db.*`, `collection.*`, `doc.*`) |
//!
//! Routing a database by the first path segment keeps the server compatible
//! with `anda_db_shard_proxy` deployments.
//!
//! ## Wire protocol
//!
//! Request body: `{"method": "...", "params": {...}}`
//!
//! Success response: `{"result": ...}` (HTTP 200)
//!
//! Error response: `{"error": {"code": "...", "message": "..."}}` with a
//! meaningful HTTP status (400/401/404/408/409/413/500/503). Client-safe
//! input/query/conflict errors are classified at the HTTP boundary; engine
//! failures are logged and return a generic response without storage paths or
//! nested source details.
//!
//! Encoding negotiation:
//! - Request body format follows `Content-Type` (`application/cbor` default).
//! - Response format follows `Accept` when present, otherwise mirrors the
//!   request `Content-Type`, otherwise CBOR.
//!
//! ## Methods
//!
//! Root scope (`POST /`): `info`, `db.list`, `db.create`, `db.open`,
//! `db.connect`, `db.close`.
//!
//! Database scope (`POST /{db_name}`):
//! - `info`, `db.metadata`, `db.stats`, `db.flush`, `db.set_read_only`,
//!   `db.get_extension`, `db.save_extension`, `db.remove_extension`
//! - `collection.list`, `collection.create`, `collection.ensure`,
//!   `collection.metadata`, `collection.stats`, `collection.delete`,
//!   `collection.flush`, `collection.set_read_only`,
//!   `collection.get_extension`, `collection.save_extension`,
//!   `collection.remove_extension`
//! - `doc.add`, `doc.add_many`, `doc.get`, `doc.get_many`, `doc.update`,
//!   `doc.remove`, `doc.exists`, `doc.count`, `doc.search`, `doc.search_ids`,
//!   `doc.query_ids`
//!
//! See the crate README for parameter shapes and examples.

use axum::{Router, extract::DefaultBodyLimit, middleware, routing};

pub mod api;
pub mod encoding;
pub mod error;
pub mod state;

pub use error::{ApiError, ClientError};
pub use state::{AppState, OpenMode, ServerInfo, ServerOptions};

/// Builds the axum [`Router`] for the server.
///
/// `GET /` is unauthenticated; the RPC endpoints check the configured
/// API key (if any) on every request. The request body size limit comes
/// from [`ServerOptions::max_body_size`] and over-limit requests receive
/// the RPC error envelope (`payload_too_large`) instead of axum's plain
/// text 413.
///
/// An outermost timeout layer ([`api::total_timeout`], 2× the request
/// timeout) bounds the whole request including reading the body, so a
/// slow-transmitting client cannot hold a connection open indefinitely;
/// the per-request timeout applied to the dispatched operation itself
/// lives in the RPC handlers. Read-only RPCs are cancellation-safe; mutating
/// RPCs run under bounded concurrency and remain tracked through shutdown.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", routing::get(api::get_info).post(api::rpc_root))
        .route("/{db_name}", routing::post(api::rpc_db))
        .layer(DefaultBodyLimit::max(state.max_body_size()))
        .layer(middleware::from_fn(api::normalize_rejections))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            api::total_timeout,
        ))
        .with_state(state)
}
