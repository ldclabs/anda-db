//! HTTP handlers and RPC method dispatch.
//!
//! Routes:
//! - `GET /` — unauthenticated server health/info
//! - `POST /` — root-scope methods (server info, database lifecycle)
//! - `POST /{db_name}` — database-scoped methods (`db.*`, `collection.*`, `doc.*`)

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, header},
    response::Response,
};
use serde::Serialize;

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
    let result = async {
        authorize(&state, &headers)?;
        let req = RpcRequest::parse(&headers, &body)?;
        dispatch_root(&state, enc, req).await
    }
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
    let result = async {
        authorize(&state, &headers)?;
        let req = RpcRequest::parse(&headers, &body)?;
        dispatch_db(&state, &db_name, enc, req).await
    }
    .await;
    result.unwrap_or_else(|err| err.respond(enc))
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

        if provided != expected {
            return Err(ApiError::unauthorized());
        }
    }
    Ok(())
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
