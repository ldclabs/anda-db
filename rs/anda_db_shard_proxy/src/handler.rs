//! Axum router construction and administrative HTTP handlers.
//!
//! The management API lets operators inspect and mutate routing metadata while
//! the proxy fallback forwards normal database traffic to the resolved shard.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing,
};
use serde::{Deserialize, Serialize};

use crate::proxy::{AppState, proxy_handler};
use crate::store::ShardBackend;

// ── Management API request/response types ───────────────────────────────────

/// Minimal JSON response envelope used by management endpoints.
#[derive(Debug, Default, Serialize)]
pub struct RpcResponse<T> {
    /// Successful payload when the operation completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,

    /// Error message when the operation failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Reserved pagination cursor for future list-style endpoints.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl<T> RpcResponse<T> {
    /// Create a successful RPC response.
    pub fn ok(result: T) -> Self {
        Self {
            result: Some(result),
            error: None,
            next_cursor: None,
        }
    }

    /// Create an error RPC response.
    pub fn err(error: impl Into<String>) -> Self {
        Self {
            result: None,
            error: Some(error.into()),
            next_cursor: None,
        }
    }
}

/// Request body for creating a permanent database-to-shard assignment.
#[derive(Deserialize)]
pub struct AssignDbRequest {
    /// Logical database name exposed to clients.
    pub db_name: String,
    /// Shard that should own the database.
    pub shard_id: u32,
}

/// Request body for removing a database-to-shard assignment.
#[derive(Deserialize)]
pub struct UnassignDbRequest {
    /// Database name to remove from the routing table.
    pub db_name: String,
}

/// Request body for creating or updating a shard backend endpoint.
#[derive(Deserialize)]
pub struct UpsertBackendRequest {
    /// Shard whose backend should be updated.
    pub shard_id: u32,
    /// Backend base URL such as `http://10.0.0.12:8080`.
    pub backend_addr: String,
    /// Whether the backend should be advertised as read-only.
    #[serde(default)]
    pub read_only: bool,
}

/// Request body for deleting a shard backend entry.
#[derive(Deserialize)]
pub struct DeleteBackendRequest {
    /// Shard whose backend entry should be deleted.
    pub shard_id: u32,
}

// ── db_shards handlers ──────────────────────────────────────────────────────

async fn get_db_shard(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
) -> impl IntoResponse {
    let rt = state.store.get_db_shard(&db_name).await;
    Json(RpcResponse::ok(rt))
}

async fn assign_db(
    State(state): State<AppState>,
    Json(req): Json<AssignDbRequest>,
) -> Result<impl IntoResponse, (StatusCode, impl IntoResponse)> {
    state
        .store
        .assign_db(&req.db_name, req.shard_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RpcResponse::<()>::err(e.to_string())),
            )
        })?;
    Ok(Json(RpcResponse::ok("db assigned to shard")))
}

async fn unassign_db(
    State(state): State<AppState>,
    Json(req): Json<UnassignDbRequest>,
) -> Result<impl IntoResponse, (StatusCode, impl IntoResponse)> {
    let deleted = state.store.unassign_db(&req.db_name).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RpcResponse::<()>::err(e.to_string())),
        )
    })?;
    if deleted {
        Ok(Json(RpcResponse::ok("db unassigned")))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(RpcResponse::<()>::err("db shard binding not found")),
        ))
    }
}

// ── shard_backends handlers ─────────────────────────────────────────────────

async fn list_shard_backends(State(state): State<AppState>) -> Json<Vec<ShardBackend>> {
    Json(state.store.list_shard_backends())
}

async fn upsert_backend(
    State(state): State<AppState>,
    Json(req): Json<UpsertBackendRequest>,
) -> Result<impl IntoResponse, (StatusCode, impl IntoResponse)> {
    let backend = ShardBackend {
        shard_id: req.shard_id,
        backend_addr: req.backend_addr,
        read_only: req.read_only,
    };
    state.store.upsert_backend(&backend).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RpcResponse::<()>::err(e.to_string())),
        )
    })?;
    Ok(Json(RpcResponse::ok("shard backend upserted")))
}

async fn delete_backend(
    State(state): State<AppState>,
    Json(req): Json<DeleteBackendRequest>,
) -> Result<impl IntoResponse, (StatusCode, impl IntoResponse)> {
    let deleted = state
        .store
        .delete_backend(req.shard_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RpcResponse::<()>::err(e.to_string())),
            )
        })?;
    if deleted {
        Ok(Json(RpcResponse::ok("shard backend deleted")))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(RpcResponse::<()>::err("shard backend not found")),
        ))
    }
}

// ── Auth middleware ─────────────────────────────────────────────────────────

async fn auth_middleware(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Result<axum::response::Response, StatusCode> {
    if let Some(expected) = state.api_key.as_deref() {
        // The admin API accepts a conventional Bearer token and stays open when
        // no API key is configured, which is convenient for local development.
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let provided = provided.trim_start_matches("Bearer ");
        if provided != expected {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(request).await)
}

// ── Router construction ─────────────────────────────────────────────────────

/// Build the full axum router:
///
/// - `/_admin/db_shards`       – db → shard mappings (auth-protected)
/// - `/_admin/shard_backends`  – shard → backend mappings (auth-protected)
/// - `/*`                      – reverse proxy to database shard backends
pub fn build_router(state: AppState) -> Router {
    // Management routes (require auth)
    let admin = Router::new()
        .route("/db_shards/{db_name}", routing::get(get_db_shard))
        .route("/db_shards", routing::put(assign_db))
        .route("/db_shards", routing::delete(unassign_db))
        .route("/shard_backends", routing::get(list_shard_backends))
        .route("/shard_backends", routing::put(upsert_backend))
        .route("/shard_backends", routing::delete(delete_backend))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    Router::new()
        .nest("/_admin", admin)
        .fallback(proxy_handler)
        .with_state(state)
}
