//! # Anda DB Server
//!
//! A HTTP server for Anda DB that uses a lightweight RPC protocol and supports
//! both JSON and CBOR data interchange formats.
//!
//! This library module exposes the router and handler types so they can be
//! reused in integration tests or embedded in other applications.

pub mod handler;
pub mod payload;

use axum::{Router, middleware, routing};

use handler::*;

/// Builds the axum [`Router`] with RPC endpoints and auth middleware.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", routing::post(handle_root_rpc))
        .route("/{db_name}", routing::post(handle_db_rpc))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state)
}

/// Authentication middleware that checks for a valid API key.
pub(crate) async fn auth_middleware(
    axum::extract::State(app): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: middleware::Next,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    if let Some(ref expected_api_key) = app.api_key {
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let provided = provided.trim_start_matches("Bearer ");
        if provided != expected_api_key {
            return Err(axum::http::StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(request).await)
}
