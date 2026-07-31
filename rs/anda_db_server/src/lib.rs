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
//! - Request body format follows `Content-Type`, which must be present and
//!   recognized (`application/cbor` or `application/json`); anything else is
//!   refused with 415.
//! - Response format follows `Accept` when present, otherwise mirrors the
//!   request `Content-Type`, otherwise CBOR.
//!
//! ## Authorization
//!
//! Two key tiers, both presented as `Authorization: Bearer <key>`:
//!
//! - The **admin key** (`--api-key` / `API_KEY`) authorizes the root scope
//!   and every database scope.
//! - A **per-database key** authorizes `POST /{its_db}` only. Its SHA3-256
//!   hash is persisted in the primary database, provisioned with
//!   `db.create {api_key}` and rotated with `db.set_api_key`.
//!
//! A database with no key of its own falls back to the admin key, so an
//! instance that never provisions one behaves exactly as it did before this
//! existed — including the unauthenticated loopback mode when no admin key is
//! configured. Unauthorized database-scope requests always return the same
//! `401`, whether or not the named database exists, and authorization runs
//! before the request body is read, so an anonymous caller can neither make
//! the server buffer its body nor observe the body-limit `413`. See [`auth`]
//! for the full precedence rules.
//!
//! ## Methods
//!
//! Root scope (`POST /`, admin key only): `info`, `db.list`, `db.create`,
//! `db.open`, `db.connect`, `db.close`, `db.set_api_key`,
//! `db.remove_api_key`.
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
pub mod auth;
pub mod encoding;
pub mod error;
pub mod state;

pub use auth::{ApiKeyHash, Principal, Scope};
pub use error::{ApiError, ClientError};
pub use state::{AppState, OpenMode, ServerInfo, ServerOptions};

/// Builds the axum [`Router`] for the server.
///
/// `GET /` is unauthenticated; the RPC endpoints authorize every request
/// against the scope it addresses (see [`auth`]) **before the body is
/// read**: [`api::require_auth`] runs as a route layer ahead of the
/// handlers' body extractor, so an anonymous oversized request is answered
/// `401`, never `413`, and never buffered. The request body size limit comes
/// from [`ServerOptions::max_body_size`] and over-limit (authorized)
/// requests receive the RPC error envelope (`payload_too_large`) instead of
/// axum's plain text 413.
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
        // A *route* layer, innermost: it runs after routing (so the matched
        // path captures are available for scope derivation) but before any
        // handler extractor touches the body, and never for the 404
        // fallback. See [`api::require_auth`].
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            api::require_auth,
        ))
        .layer(DefaultBodyLimit::max(state.max_body_size()))
        .layer(middleware::from_fn(api::normalize_rejections))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            api::total_timeout,
        ))
        .with_state(state)
}
