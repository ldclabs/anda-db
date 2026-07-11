//! HTTP handlers and RPC method dispatch.
//!
//! Routes:
//! - `GET /` — unauthenticated server health/info
//! - `POST /` — root-scope methods (server info, database lifecycle)
//! - `POST /{db_name}` — database-scoped methods (`db.*`, `collection.*`, `doc.*`)

use axum::{
    body::{Body, Bytes},
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::Response,
};
use serde::Serialize;
use std::future::Future;

use crate::{
    encoding::{Encoding, RpcRequest},
    error::ApiError,
    state::{AppState, OpenMode},
};

mod collection;
mod db;
mod document;
mod root;

pub use collection::{CollectionParams, CreateCollectionParams, HnswIndexParams};
pub use document::{
    AddManyParams, AddParams, DocumentIdParams, DocumentIdsParams, QueryIdsParams, SearchParams,
    UpdateParams,
};
pub use root::DatabaseParams;

/// `GET /` — unauthenticated health/info endpoint.
///
/// Returns only the server name and version; the database list requires
/// authentication via the `info` RPC method. Defaults to JSON so that
/// load balancers and browsers get a readable payload.
pub async fn get_info(State(state): State<AppState>, headers: HeaderMap) -> Response {
    #[derive(Serialize)]
    struct Health<'a> {
        name: &'a str,
        version: &'a str,
    }

    let enc = Encoding::negotiate_or(&headers, Encoding::Json);
    let info = state.info().await;
    enc.reply(&Health {
        name: &info.name,
        version: &info.version,
    })
}

/// `POST /` — root-scope RPC endpoint.
pub async fn rpc_root(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let enc = Encoding::negotiate(&headers);
    let result = execute_rpc(&state, enc, &headers, &body, |state, enc, req| async move {
        dispatch_root(&state, enc, req).await
    })
    .await;
    result.unwrap_or_else(|err| err.respond(enc))
}

/// `POST /{db_name}` — database-scoped RPC endpoint.
pub async fn rpc_db(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let enc = Encoding::negotiate(&headers);
    let result = execute_rpc(&state, enc, &headers, &body, |state, enc, req| async move {
        dispatch_db(&state, &db_name, enc, req).await
    })
    .await;
    result.unwrap_or_else(|err| err.respond(enc))
}

/// Authorizes and parses the request inline (cheap and side-effect free),
/// then runs `dispatch` on its own spawned task with the configured request
/// timeout applied to the [`tokio::task::JoinHandle`].
///
/// Mutating RPC methods (`db.close`, `db.create`, `doc.add`, ...) are not
/// cancel-safe: dropping them at an arbitrary await point — which is exactly
/// what wrapping the whole future in `tokio::time::timeout` used to do —
/// can break invariants such as "a database removed from the registry always
/// has its flush task cancelled". Spawning decouples the operation from the
/// response: on timeout the client receives 408, the `JoinHandle` is dropped
/// (which *detaches* the task rather than aborting it), and the operation
/// runs to completion in the background, so a retry observes a consistent
/// state instead of racing a half-executed predecessor. The same reasoning
/// protects the operation from a client disconnect or the route-level
/// [`total_timeout`], both of which only drop this handler's future.
async fn execute_rpc<F, Fut>(
    state: &AppState,
    enc: Encoding,
    headers: &HeaderMap,
    body: &Bytes,
    dispatch: F,
) -> Result<Response, ApiError>
where
    F: FnOnce(AppState, Encoding, RpcRequest) -> Fut,
    Fut: Future<Output = Result<Response, ApiError>> + Send + 'static,
{
    if state.is_shutting_down() {
        return Err(ApiError::unavailable());
    }
    authorize(state, headers)?;
    let req = RpcRequest::parse(headers, body)?;
    let task = tokio::spawn(dispatch(state.clone(), enc, req));
    match tokio::time::timeout(state.request_timeout(), task).await {
        Ok(Ok(result)) => result,
        Ok(Err(err)) => {
            // The dispatch task panicked (nothing aborts these tasks).
            log::error!(action = "execute_rpc"; "rpc dispatch task failed: {err:?}");
            Err(ApiError::internal("internal server error"))
        }
        Err(_elapsed) => Err(ApiError::timeout()),
    }
}

/// Route-level timeout covering the *entire* request, including reading the
/// body — the per-request timeout in [`execute_rpc`] only starts after the
/// `Bytes` extractor has buffered the body, so without this layer a client
/// trickling a request body could hold the connection (and its task) open
/// forever.
///
/// The deadline is twice the configured request timeout so that once
/// dispatch has started, the inner timeout (which produces the precise 408
/// for the operation) always fires first; this layer only triggers when the
/// transport itself stalls. Cancelling here is safe: during body reading no
/// state has been touched yet, and once dispatch has started the operation
/// lives on its own spawned task ([`execute_rpc`]) that dropping this future
/// cannot cancel.
pub async fn total_timeout(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let enc = Encoding::negotiate(req.headers());
    let deadline = state.request_timeout().saturating_mul(2);
    match tokio::time::timeout(deadline, next.run(req)).await {
        Ok(resp) => resp,
        Err(_elapsed) => ApiError::timeout().respond(enc),
    }
}

/// Rewrites extractor-level rejections (e.g. the body-limit 413, which axum
/// emits as plain text) into the RPC error envelope in the negotiated
/// encoding. Responses that already carry a CBOR/JSON body pass through.
pub async fn normalize_rejections(req: Request<Body>, next: Next) -> Response {
    let enc = Encoding::negotiate(req.headers());
    let resp = next.run(req).await;
    if resp.status() == StatusCode::PAYLOAD_TOO_LARGE
        && Encoding::from_content_type(resp.headers()).is_none()
    {
        return ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request body exceeds the configured size limit",
        )
        .respond(enc);
    }
    resp
}

/// Verifies the `Authorization: Bearer <key>` header when an API key is set.
fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    if let Some(expected) = state.api_key() {
        if expected.trim().is_empty() {
            return Err(ApiError::unauthorized());
        }

        let Some(provided) = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
        else {
            return Err(ApiError::unauthorized());
        };

        if !constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
            return Err(ApiError::unauthorized());
        }
    }
    Ok(())
}

/// Constant-time byte comparison to avoid a timing side channel on the API
/// key. Only the length may leak, which is not considered secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn dispatch_root(
    state: &AppState,
    enc: Encoding,
    req: RpcRequest,
) -> Result<Response, ApiError> {
    let RpcRequest { method, params } = req;
    let resp = match method.as_str() {
        "info" => enc.reply(&state.info().await),
        "db.list" => enc.reply(&state.db_names().await),
        "db.create" => enc.reply(&root::register(state, OpenMode::Create, params.decode()?).await?),
        "db.open" => enc.reply(&root::register(state, OpenMode::Open, params.decode()?).await?),
        "db.connect" => {
            enc.reply(&root::register(state, OpenMode::Connect, params.decode()?).await?)
        }
        "db.close" => enc.reply(&root::close(state, params.decode()?).await?),
        _ => return Err(ApiError::method_not_found(&method)),
    };
    Ok(resp)
}

async fn dispatch_db(
    state: &AppState,
    db_name: &str,
    enc: Encoding,
    req: RpcRequest,
) -> Result<Response, ApiError> {
    let db = state.get_db(db_name).await?;
    let RpcRequest { method, params } = req;
    let resp = match method.as_str() {
        "info" => enc.reply(&state.info().await),

        // ─── database ────────────────────────────────────────────────
        "db.metadata" => enc.reply(&db.metadata()),
        "db.stats" => enc.reply(&db.stats()),
        "db.flush" => enc.reply(&db::flush(&db).await?),
        "db.set_read_only" => enc.reply(&db::set_read_only(&db, params.decode()?)),
        "db.get_extension" => enc.reply(&db::get_extension(&db, params.decode()?)),
        "db.save_extension" => enc.reply(&db::save_extension(&db, params.decode()?).await?),
        "db.remove_extension" => enc.reply(&db::remove_extension(&db, params.decode()?).await?),

        // ─── collections ─────────────────────────────────────────────
        "collection.list" => enc.reply(&db.metadata().collections),
        "collection.create" => enc.reply(&collection::create(&db, params.decode()?).await?),
        "collection.ensure" => enc.reply(&collection::ensure(&db, params.decode()?).await?),
        "collection.metadata" => enc.reply(&collection::metadata(&db, params.decode()?).await?),
        "collection.stats" => enc.reply(&collection::stats(&db, params.decode()?).await?),
        "collection.delete" => enc.reply(&collection::delete(&db, params.decode()?).await?),
        "collection.flush" => enc.reply(&collection::flush(&db, params.decode()?).await?),
        "collection.set_read_only" => {
            enc.reply(&collection::set_read_only(&db, params.decode()?).await?)
        }
        "collection.get_extension" => {
            enc.reply(&collection::get_extension(&db, params.decode()?).await?)
        }
        "collection.save_extension" => {
            enc.reply(&collection::save_extension(&db, params.decode()?).await?)
        }
        "collection.remove_extension" => {
            enc.reply(&collection::remove_extension(&db, params.decode()?).await?)
        }

        // ─── documents ───────────────────────────────────────────────
        "doc.add" => enc.reply(&document::add(&db, params.decode()?).await?),
        "doc.add_many" => enc.reply(&document::add_many(&db, params.decode()?).await?),
        "doc.get" => enc.reply(&document::get(&db, params.decode()?).await?),
        "doc.get_many" => enc.reply(&document::get_many(&db, params.decode()?).await?),
        "doc.update" => enc.reply(&document::update(&db, params.decode()?).await?),
        "doc.remove" => enc.reply(&document::remove(&db, params.decode()?).await?),
        "doc.exists" => enc.reply(&document::exists(&db, params.decode()?).await?),
        "doc.count" => enc.reply(&document::count(&db, params.decode()?).await?),
        "doc.search" => enc.reply(&document::search(&db, params.decode()?).await?),
        "doc.search_ids" => enc.reply(&document::search_ids(&db, params.decode()?).await?),
        "doc.query_ids" => enc.reply(&document::query_ids(&db, params.decode()?).await?),

        _ => return Err(ApiError::method_not_found(&method)),
    };
    Ok(resp)
}
