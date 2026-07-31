//! HTTP handlers and RPC method dispatch.
//!
//! Routes:
//! - `GET /` — unauthenticated server health/info
//! - `POST /` — root-scope methods (server info, database lifecycle), admin key only
//! - `POST /{db_name}` — database-scoped methods (`db.*`, `collection.*`, `doc.*`),
//!   admin key or the key bound to that database
//!
//! Every RPC request is authorized against the scope it addresses before its
//! body is even read: [`require_auth`] runs as a route layer ahead of the
//! handlers' `Bytes` extractor, and [`execute_rpc`] authorizes again as
//! defense in depth before the body is parsed and before the database
//! registry is consulted. See [`crate::auth`] for the two key tiers and
//! their precedence.

use axum::{
    body::{Body, Bytes},
    extract::{Path, RawPathParams, Request, State, rejection::RawPathParamsRejection},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::Next,
    response::Response,
};
use serde::Serialize;
use std::future::Future;

use crate::{
    auth::{Principal, Scope},
    encoding::{Encoding, RpcParams, RpcRequest},
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
pub use root::{ApiKeyParams, ApiKeyResult, CreateDatabaseParams, DatabaseParams};

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

/// `POST /` — root-scope RPC endpoint. Requires the admin key.
pub async fn rpc_root(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let enc = Encoding::negotiate(&headers);
    let result = execute_rpc(
        &state,
        Scope::Root,
        enc,
        &headers,
        &body,
        RootMethod::parse,
        |state, enc, method, params, _principal| async move {
            dispatch_root(&state, enc, method, params).await
        },
    )
    .await;
    result.unwrap_or_else(|err| err.respond(enc))
}

/// `POST /{db_name}` — database-scoped RPC endpoint. Requires the admin key
/// or the key bound to `db_name`.
pub async fn rpc_db(
    State(state): State<AppState>,
    Path(db_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let enc = Encoding::negotiate(&headers);
    // The scope borrows a separate copy: `db_name` itself moves into the
    // dispatch future, which must be `'static`.
    let scope_name = db_name.clone();
    let result = execute_rpc(
        &state,
        Scope::Database(&scope_name),
        enc,
        &headers,
        &body,
        DbMethod::parse,
        move |state, enc, method, params, principal| async move {
            dispatch_db(&state, &db_name, principal, enc, method, params).await
        },
    )
    .await;
    result.unwrap_or_else(|err| err.respond(enc))
}

/// Authorizes and parses the request inline, then applies cancellation policy
/// according to the method's side effects.
///
/// Authorization happens first and is keyed by `scope`, so every entry point
/// is forced to declare what the request addresses and the resulting
/// [`Principal`] is handed to the dispatcher — a database-scoped caller can
/// then be told apart from an admin inside the method handlers.
///
/// Read-only methods run directly in the HTTP handler. Their future is
/// cancelled on timeout, disconnect, or shutdown, preventing abandoned
/// searches from consuming resources after their response is gone.
///
/// Mutating methods are not generally cancel-safe, so they acquire a bounded
/// semaphore slot and run in the state's mutation tracker. A timed-out or
/// disconnected response drops only its `JoinHandle`; shutdown closes
/// admission and drains the tracked task before closing databases.
async fn execute_rpc<M, F, Fut>(
    state: &AppState,
    scope: Scope<'_>,
    enc: Encoding,
    headers: &HeaderMap,
    body: &Bytes,
    parse_method: fn(&str) -> Option<(M, MethodEffect)>,
    dispatch: F,
) -> Result<Response, ApiError>
where
    M: Send + 'static,
    F: FnOnce(AppState, Encoding, M, RpcParams, Principal) -> Fut,
    Fut: Future<Output = Result<Response, ApiError>> + Send + 'static,
{
    // [`require_auth`] already rejected unauthenticated requests before the
    // body was buffered. This second check is deliberate defense in depth:
    // it is cheap, and it keeps `execute_rpc` safe on its own even if a
    // future route is wired up without the middleware.
    let principal = state.authorize(scope, bearer_token(headers))?;
    let RpcRequest { method, params } = RpcRequest::parse(headers, body)?;
    // Resolving the method and its side-effect class in one step is what
    // keeps the cancellation policy below in sync with the dispatch table.
    let (method, effect) =
        parse_method(&method).ok_or_else(|| ApiError::method_not_found(&method))?;
    let deadline = tokio::time::Instant::now() + state.request_timeout();

    if effect == MethodEffect::Mutating {
        let mutation = dispatch(state.clone(), enc, method, params, principal);
        let task = tokio::time::timeout_at(deadline, state.spawn_mutation(mutation))
            .await
            .map_err(|_| ApiError::timeout())??;
        match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(err)) if err.is_cancelled() && state.is_shutting_down() => {
                Err(ApiError::unavailable())
            }
            Ok(Err(err)) => {
                log::error!(action = "execute_rpc"; "rpc mutation task failed: {err:?}");
                Err(ApiError::internal("internal server error"))
            }
            Err(_elapsed) => Err(ApiError::timeout()),
        }
    } else {
        let cancel = state.admit_read()?;
        let read = dispatch(state.clone(), enc, method, params, principal);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(ApiError::unavailable()),
            result = tokio::time::timeout_at(deadline, read) => {
                result.map_err(|_| ApiError::timeout())?
            }
        }
    }
}

/// Side-effect class of an RPC method. It selects the cancellation policy in
/// [`execute_rpc`], which is a durability decision: a mutating future dropped
/// at an arbitrary await can poison a collection handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MethodEffect {
    /// The handler only reads. Its future may be dropped on timeout, client
    /// disconnect, or shutdown.
    Read,
    /// The handler may modify server, database, collection, index, or
    /// document state and must not be dropped at an arbitrary await.
    Mutating,
}

/// Root-scope methods (`POST /`).
///
/// The classification lives in [`RootMethod::parse`], the same table that
/// resolves the name, and the dispatch match below is exhaustive over this
/// enum. Adding a method therefore cannot compile until it is both
/// classified and handled — a hand-maintained list of method names parallel
/// to the dispatch match let a new mutating method silently fall onto the
/// cancellable path, which the compiler could not catch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootMethod {
    Info,
    DbList,
    DbCreate,
    DbOpen,
    DbConnect,
    DbClose,
    DbSetApiKey,
    DbRemoveApiKey,
}

impl RootMethod {
    /// Resolves a method name to its handler selector *and* its side-effect
    /// class. This is the only place either is declared.
    fn parse(method: &str) -> Option<(Self, MethodEffect)> {
        use MethodEffect::{Mutating, Read};
        Some(match method {
            "info" => (Self::Info, Read),
            "db.list" => (Self::DbList, Read),
            "db.create" => (Self::DbCreate, Mutating),
            "db.open" => (Self::DbOpen, Mutating),
            "db.connect" => (Self::DbConnect, Mutating),
            "db.close" => (Self::DbClose, Mutating),
            "db.set_api_key" => (Self::DbSetApiKey, Mutating),
            "db.remove_api_key" => (Self::DbRemoveApiKey, Mutating),
            _ => return None,
        })
    }
}

/// Database-scope methods (`POST /{db_name}`). See [`RootMethod`] for why the
/// side-effect class is declared inside [`DbMethod::parse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbMethod {
    Info,
    DbMetadata,
    DbStats,
    DbFlush,
    DbSetReadOnly,
    DbGetExtension,
    DbSaveExtension,
    DbRemoveExtension,
    CollectionList,
    CollectionCreate,
    CollectionEnsure,
    CollectionMetadata,
    CollectionStats,
    CollectionDelete,
    CollectionFlush,
    CollectionSetReadOnly,
    CollectionGetExtension,
    CollectionSaveExtension,
    CollectionRemoveExtension,
    DocAdd,
    DocAddMany,
    DocGet,
    DocGetMany,
    DocUpdate,
    DocRemove,
    DocExists,
    DocCount,
    DocSearch,
    DocSearchIds,
    DocQueryIds,
}

impl DbMethod {
    /// Resolves a method name to its handler selector *and* its side-effect
    /// class. This is the only place either is declared.
    fn parse(method: &str) -> Option<(Self, MethodEffect)> {
        use MethodEffect::{Mutating, Read};
        Some(match method {
            "info" => (Self::Info, Read),

            // ─── database ────────────────────────────────────────────────
            "db.metadata" => (Self::DbMetadata, Read),
            "db.stats" => (Self::DbStats, Read),
            "db.flush" => (Self::DbFlush, Mutating),
            "db.set_read_only" => (Self::DbSetReadOnly, Mutating),
            "db.get_extension" => (Self::DbGetExtension, Read),
            "db.save_extension" => (Self::DbSaveExtension, Mutating),
            "db.remove_extension" => (Self::DbRemoveExtension, Mutating),

            // ─── collections ─────────────────────────────────────────────
            "collection.list" => (Self::CollectionList, Read),
            "collection.create" => (Self::CollectionCreate, Mutating),
            "collection.ensure" => (Self::CollectionEnsure, Mutating),
            "collection.metadata" => (Self::CollectionMetadata, Read),
            "collection.stats" => (Self::CollectionStats, Read),
            "collection.delete" => (Self::CollectionDelete, Mutating),
            "collection.flush" => (Self::CollectionFlush, Mutating),
            "collection.set_read_only" => (Self::CollectionSetReadOnly, Mutating),
            "collection.get_extension" => (Self::CollectionGetExtension, Read),
            "collection.save_extension" => (Self::CollectionSaveExtension, Mutating),
            "collection.remove_extension" => (Self::CollectionRemoveExtension, Mutating),

            // ─── documents ───────────────────────────────────────────────
            "doc.add" => (Self::DocAdd, Mutating),
            "doc.add_many" => (Self::DocAddMany, Mutating),
            "doc.get" => (Self::DocGet, Read),
            "doc.get_many" => (Self::DocGetMany, Read),
            "doc.update" => (Self::DocUpdate, Mutating),
            "doc.remove" => (Self::DocRemove, Mutating),
            "doc.exists" => (Self::DocExists, Read),
            "doc.count" => (Self::DocCount, Read),
            "doc.search" => (Self::DocSearch, Read),
            "doc.search_ids" => (Self::DocSearchIds, Read),
            "doc.query_ids" => (Self::DocQueryIds, Read),

            _ => return None,
        })
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
/// transport itself stalls. Cancelling this middleware cancels read-only
/// dispatch directly. A mutation that has already acquired admission lives
/// in the state tracker and is drained during shutdown.
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

/// Rejects an unauthorized RPC request before any handler extractor runs.
///
/// Authorizing only inside [`execute_rpc`] let the `Bytes` extractor buffer
/// the request body (up to [`crate::state::ServerOptions::max_body_size`])
/// first, so an anonymous caller could make the server hold that memory per
/// request and could tell the body-limit `413` apart from the `401`. This
/// route layer answers from the headers and the matched path alone; the
/// in-handler check remains as defense in depth.
///
/// Only `POST` — the method every RPC endpoint uses — is checked: `GET /`
/// (the health endpoint) deliberately stays unauthenticated, and other
/// methods keep the router's own 405 answers.
pub async fn require_auth(
    State(state): State<AppState>,
    params: Result<RawPathParams, RawPathParamsRejection>,
    req: Request,
    next: Next,
) -> Response {
    if req.method() != Method::POST {
        return next.run(req).await;
    }
    // Invalid UTF-8 in a percent-encoded path segment: fall through and let
    // the handler's `Path` extractor answer the same 400 it does today —
    // still before the body is read, and still behind the in-handler
    // authorization.
    let Ok(params) = params else {
        return next.run(req).await;
    };
    if let Err(err) = state.authorize(scope_from_params(&params), bearer_token(req.headers())) {
        return err.respond(Encoding::negotiate(req.headers()));
    }
    next.run(req).await
}

/// Derives the scope a matched RPC route addresses, mirroring the handler
/// entry points exactly: `POST /` carries no `db_name` capture and addresses
/// [`Scope::Root`]; `POST /{db_name}` addresses that database. The capture
/// value is percent-decoded, like the `Path` extractor [`rpc_db`] uses.
fn scope_from_params(params: &RawPathParams) -> Scope<'_> {
    params
        .iter()
        .find(|(name, _)| *name == "db_name")
        .map(|(_, value)| Scope::Database(value))
        .unwrap_or(Scope::Root)
}

/// Extracts the credential from an `Authorization: Bearer <key>` header.
///
/// A malformed or absent header yields `None`, which every authorization
/// path treats exactly like a wrong key.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Constant-time byte comparison to avoid a timing side channel on the API
/// key. Callers compare fixed-length hashes (see [`crate::auth::ApiKeyHash`]),
/// so not even the key length leaks.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Root-scope dispatch. Reached only by [`Principal::Admin`], so every method
/// here may see and change server-level state.
///
/// The match is exhaustive over [`RootMethod`]: a new variant does not
/// compile until it is dispatched here and classified in
/// [`RootMethod::parse`].
async fn dispatch_root(
    state: &AppState,
    enc: Encoding,
    method: RootMethod,
    params: RpcParams,
) -> Result<Response, ApiError> {
    let resp = match method {
        RootMethod::Info => enc.reply(&state.info().await),
        RootMethod::DbList => enc.reply(&state.db_names().await),
        RootMethod::DbCreate => enc.reply(&root::create(state, params.decode()?).await?),
        RootMethod::DbOpen => {
            enc.reply(&root::register(state, OpenMode::Open, params.decode()?).await?)
        }
        RootMethod::DbConnect => {
            enc.reply(&root::register(state, OpenMode::Connect, params.decode()?).await?)
        }
        RootMethod::DbClose => enc.reply(&root::close(state, params.decode()?).await?),
        RootMethod::DbSetApiKey => enc.reply(&root::set_api_key(state, params.decode()?).await?),
        RootMethod::DbRemoveApiKey => {
            enc.reply(&root::remove_api_key(state, params.decode()?).await?)
        }
    };
    Ok(resp)
}

/// Database-scope dispatch, entered only after `db_name` was authorized for
/// `principal`. Every method here is confined to that one database; the
/// `principal` is needed only by `info`, which must not enumerate the
/// instance for a per-database caller.
///
/// The match is exhaustive over [`DbMethod`]: a new variant does not compile
/// until it is dispatched here and classified in [`DbMethod::parse`].
async fn dispatch_db(
    state: &AppState,
    db_name: &str,
    principal: Principal,
    enc: Encoding,
    method: DbMethod,
    params: RpcParams,
) -> Result<Response, ApiError> {
    let db = state.get_db(db_name).await?;
    let resp = match method {
        DbMethod::Info => enc.reply(&state.scoped_info(principal, db_name).await),

        // ─── database ────────────────────────────────────────────────
        DbMethod::DbMetadata => enc.reply(&db.metadata()),
        DbMethod::DbStats => enc.reply(&db.stats()),
        DbMethod::DbFlush => enc.reply(&db::flush(&db).await?),
        DbMethod::DbSetReadOnly => enc.reply(&db::set_read_only(&db, params.decode()?)),
        DbMethod::DbGetExtension => enc.reply(&db::get_extension(&db, params.decode()?)),
        DbMethod::DbSaveExtension => enc.reply(&db::save_extension(&db, params.decode()?).await?),
        DbMethod::DbRemoveExtension => {
            enc.reply(&db::remove_extension(&db, params.decode()?).await?)
        }

        // ─── collections ─────────────────────────────────────────────
        DbMethod::CollectionList => enc.reply(&db.metadata().collections),
        DbMethod::CollectionCreate => enc.reply(&collection::create(&db, params.decode()?).await?),
        DbMethod::CollectionEnsure => enc.reply(&collection::ensure(&db, params.decode()?).await?),
        DbMethod::CollectionMetadata => {
            enc.reply(&collection::metadata(&db, params.decode()?).await?)
        }
        DbMethod::CollectionStats => enc.reply(&collection::stats(&db, params.decode()?).await?),
        DbMethod::CollectionDelete => enc.reply(&collection::delete(&db, params.decode()?).await?),
        DbMethod::CollectionFlush => enc.reply(&collection::flush(&db, params.decode()?).await?),
        DbMethod::CollectionSetReadOnly => {
            enc.reply(&collection::set_read_only(&db, params.decode()?).await?)
        }
        DbMethod::CollectionGetExtension => {
            enc.reply(&collection::get_extension(&db, params.decode()?).await?)
        }
        DbMethod::CollectionSaveExtension => {
            enc.reply(&collection::save_extension(&db, params.decode()?).await?)
        }
        DbMethod::CollectionRemoveExtension => {
            enc.reply(&collection::remove_extension(&db, params.decode()?).await?)
        }

        // ─── documents ───────────────────────────────────────────────
        DbMethod::DocAdd => enc.reply(&document::add(&db, params.decode()?).await?),
        DbMethod::DocAddMany => enc.reply(&document::add_many(&db, params.decode()?).await?),
        DbMethod::DocGet => enc.reply(&document::get(&db, params.decode()?).await?),
        DbMethod::DocGetMany => enc.reply(&document::get_many(&db, params.decode()?).await?),
        DbMethod::DocUpdate => enc.reply(&document::update(&db, params.decode()?).await?),
        DbMethod::DocRemove => enc.reply(&document::remove(&db, params.decode()?).await?),
        DbMethod::DocExists => enc.reply(&document::exists(&db, params.decode()?).await?),
        DbMethod::DocCount => enc.reply(&document::count(&db, params.decode()?).await?),
        DbMethod::DocSearch => enc.reply(&document::search(&db, params.decode()?).await?),
        DbMethod::DocSearchIds => enc.reply(&document::search_ids(&db, params.decode()?).await?),
        DbMethod::DocQueryIds => enc.reply(&document::query_ids(&db, params.decode()?).await?),
    };
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ServerOptions;
    use axum::http::HeaderValue;
    use object_store::memory::InMemory;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::sync::Semaphore;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    fn json_request(method: &str) -> (HeaderMap, Bytes) {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        (headers, Bytes::from(format!(r#"{{"method":"{method}"}}"#)))
    }

    async fn test_state(mut options: ServerOptions) -> AppState {
        options.primary_db = "rpc_admission_test".to_string();
        AppState::connect(Arc::new(InMemory::new()), options)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn begin_shutdown_cancels_an_admitted_read_future() {
        let state = test_state(ServerOptions::default()).await;
        let (headers, body) = json_request("info");
        let entered = Arc::new(Semaphore::new(0));
        let dropped = Arc::new(AtomicBool::new(false));

        let request = tokio::spawn({
            let state = state.clone();
            let entered = entered.clone();
            let dropped = dropped.clone();
            async move {
                execute_rpc(
                    &state,
                    Scope::Root,
                    Encoding::Json,
                    &headers,
                    &body,
                    RootMethod::parse,
                    move |_state, enc, _method, _params, _principal| async move {
                        let _drop_flag = DropFlag(dropped);
                        entered.add_permits(1);
                        std::future::pending::<()>().await;
                        Ok(enc.reply(&()))
                    },
                )
                .await
            }
        });
        entered.acquire().await.unwrap().forget();

        state.begin_shutdown();
        let err = request.await.unwrap().unwrap_err();
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(dropped.load(Ordering::Acquire));
        state.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_rejects_a_mutation_waiting_for_the_bounded_slot() {
        let options = ServerOptions {
            max_concurrent_mutations: 1,
            ..Default::default()
        };
        let state = test_state(options).await;
        let first_entered = Arc::new(Semaphore::new(0));
        let first_release = Arc::new(Semaphore::new(0));

        let first = tokio::spawn({
            let state = state.clone();
            let entered = first_entered.clone();
            let release = first_release.clone();
            let (headers, body) = json_request("db.flush");
            async move {
                execute_rpc(
                    &state,
                    Scope::Root,
                    Encoding::Json,
                    &headers,
                    &body,
                    DbMethod::parse,
                    move |_state, enc, _method, _params, _principal| async move {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        Ok(enc.reply(&()))
                    },
                )
                .await
            }
        });
        first_entered.acquire().await.unwrap().forget();

        let second_polled = Arc::new(AtomicBool::new(false));
        let second = tokio::spawn({
            let state = state.clone();
            let second_polled = second_polled.clone();
            let (headers, body) = json_request("db.flush");
            async move {
                execute_rpc(
                    &state,
                    Scope::Root,
                    Encoding::Json,
                    &headers,
                    &body,
                    DbMethod::parse,
                    move |_state, enc, _method, _params, _principal| async move {
                        second_polled.store(true, Ordering::Release);
                        Ok(enc.reply(&()))
                    },
                )
                .await
            }
        });
        // Let the second request reach the semaphore wait before closing it.
        tokio::task::yield_now().await;
        state.begin_shutdown();

        let err = second.await.unwrap().unwrap_err();
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!second_polled.load(Ordering::Acquire));

        first_release.add_permits(1);
        assert!(first.await.unwrap().is_ok());
        state.shutdown().await;
    }
}
