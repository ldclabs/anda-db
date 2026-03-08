//! RPC handlers for Anda DB Server.
//!
//! Endpoints:
//! - `POST /` for server-level methods (e.g. create database)
//! - `POST /{db_name}` for database-scoped operations

use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    index::HnswConfig,
    query::{Filter, Query},
    schema::{DocumentId, Fv, Schema},
    storage::StorageConfig,
};
use anda_db_schema::Cbor;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
    response::IntoResponse,
};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

use crate::payload::{
    ALREADY_EXISTS, Accept, AppResponse, CborRpcRequest, CborRpcResponse, ContentType,
    INTERNAL_ERROR, INVALID_PARAMS, JsonRpcRequest, JsonRpcResponse, METHOD_NOT_FOUND, NOT_FOUND,
    PARSE_ERROR, RpcError,
};

#[derive(Clone)]
pub struct AppState {
    pub databases: Arc<RwLock<BTreeMap<String, Arc<AndaDB>>>>,
    pub object_store: Arc<dyn ObjectStore>,
    pub storage: StorageConfig,
    pub name: String,
    pub version: String,
    pub api_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCollectionParams {
    pub config: CollectionConfig,
    pub schema: Schema,
    #[serde(default)]
    pub btree_indexes: Vec<Vec<String>>,
    #[serde(default)]
    pub bm25_indexes: Vec<String>,
    #[serde(default)]
    pub hnsw_indexes: Vec<HnswIndexDef>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HnswIndexDef {
    pub field: String,
    pub config: HnswConfig,
}

#[derive(Debug, Deserialize)]
pub struct CollectionParams {
    pub collection: String,
}

#[derive(Debug, Deserialize)]
pub struct AddDocumentParams {
    pub collection: String,
    pub document: BTreeMap<String, Fv>,
}

#[derive(Debug, Deserialize)]
pub struct DocumentParams {
    pub collection: String,
    pub _id: DocumentId,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDocumentParams {
    pub collection: String,
    pub _id: DocumentId,
    pub fields: BTreeMap<String, Fv>,
}

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub collection: String,
    pub query: Query,
}

#[derive(Debug, Deserialize)]
pub struct QueryIdsParams {
    pub collection: String,
    pub filter: Filter,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct CreateDatabaseParams {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
struct InformationResult<'a> {
    name: &'a str,
    version: &'a str,
}

#[derive(Debug, Serialize)]
struct StatusResult {
    result: &'static str,
}

#[derive(Debug, Serialize)]
struct AddDocumentResult {
    _id: DocumentId,
}

pub async fn handle_root_rpc(
    Accept(ct): Accept,
    headers: HeaderMap,
    State(app): State<AppState>,
    body: Bytes,
) -> impl IntoResponse {
    handle_rpc_impl(ct, headers, body, |method, params| {
        let app = app.clone();
        async move { dispatch_root(&app, &method, params).await }
    })
    .await
}

pub async fn handle_db_rpc(
    Path(db_name): Path<String>,
    Accept(ct): Accept,
    headers: HeaderMap,
    State(app): State<AppState>,
    body: Bytes,
) -> impl IntoResponse {
    handle_rpc_impl(ct, headers, body, |method, params| {
        let app = app.clone();
        let db_name = db_name.clone();
        async move { dispatch_db(&app, &db_name, &method, params).await }
    })
    .await
}

async fn handle_rpc_impl<F, Fut>(
    ct: ContentType,
    headers: HeaderMap,
    body: Bytes,
    dispatch: F,
) -> axum::response::Response
where
    F: FnOnce(String, Fv) -> Fut,
    Fut: std::future::Future<Output = Result<Fv, RpcError>>,
{
    let req_ct = ContentType::from_content_type(&headers);
    let result = match parse_request(&body, req_ct) {
        Ok((method, params)) => dispatch(method, params).await,
        Err(err) => Err(err),
    };
    build_response(result, ct)
}

fn build_response(result: Result<Fv, RpcError>, ct: ContentType) -> axum::response::Response {
    match ct {
        ContentType::Json => {
            let resp = match result {
                Ok(value) => match serde_json::to_value(value) {
                    Ok(v) => JsonRpcResponse::success(v),
                    Err(e) => JsonRpcResponse::error(RpcError::new(
                        INTERNAL_ERROR,
                        format!("failed to serialize response: {e}"),
                    )),
                },
                Err(err) => JsonRpcResponse::error(err),
            };
            AppResponse::new(resp, ct).into_response()
        }
        ContentType::Cbor => {
            let resp = match result {
                Ok(value) => CborRpcResponse::success(value.into()),
                Err(err) => CborRpcResponse::error(err),
            };
            AppResponse::new(resp, ct).into_response()
        }
    }
}

fn parse_request(body: &[u8], ct: ContentType) -> Result<(String, Fv), RpcError> {
    match ct {
        ContentType::Json => {
            let req: JsonRpcRequest = serde_json::from_slice(body)
                .map_err(|e| RpcError::new(PARSE_ERROR, format!("parse JSON error: {e}")))?;
            let cbor = json_to_cbor(&req.params.unwrap_or(JsonValue::Null))?;
            let fv = Fv::try_from(cbor).map_err(|err| {
                RpcError::new(INVALID_PARAMS, format!("invalid CBOR params: {err}"))
            })?;
            Ok((req.method, fv))
        }
        ContentType::Cbor => {
            let req: CborRpcRequest = ciborium::de::from_reader(body)
                .map_err(|e| RpcError::new(PARSE_ERROR, format!("parse CBOR error: {e}")))?;
            let fv = Fv::try_from(req.params.unwrap_or(Cbor::Null)).map_err(|err| {
                RpcError::new(INVALID_PARAMS, format!("invalid CBOR params: {err}"))
            })?;
            Ok((req.method, fv))
        }
    }
}

fn json_to_cbor(value: &JsonValue) -> Result<Cbor, RpcError> {
    Cbor::serialized(value).map_err(|e| {
        RpcError::new(
            INTERNAL_ERROR,
            format!("failed to build CBOR response: {e}"),
        )
    })
}

fn serialize_result<T>(value: &T) -> Result<Fv, RpcError>
where
    T: Serialize,
{
    Fv::serialized(value, None).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn dispatch_root(app: &AppState, method: &str, params: Fv) -> Result<Fv, RpcError> {
    match method {
        "get_information" => get_information(app),
        "create_database" => create_database(app, Fv::deserialized(params)?).await,
        "list_databases" => list_databases(app).await,
        _ => Err(RpcError::new(
            METHOD_NOT_FOUND,
            format!("method not found: {method}"),
        )),
    }
}

async fn dispatch_db(
    app: &AppState,
    db_name: &str,
    method: &str,
    params: Fv,
) -> Result<Fv, RpcError> {
    let db = get_database(app, db_name).await?;

    match method {
        "get_information" => get_information(app),
        "get_db_metadata" => get_db_metadata(&db),
        "flush_db" => flush_db(&db).await,
        "create_collection" => create_collection(&db, Fv::deserialized(params)?).await,
        "get_collection_metadata" => get_collection_metadata(&db, Fv::deserialized(params)?).await,
        "delete_collection" => delete_collection(&db, Fv::deserialized(params)?).await,
        "add_document" => add_document(&db, Fv::deserialized(params)?).await,
        "get_document" => get_document(&db, Fv::deserialized(params)?).await,
        "update_document" => update_document(&db, Fv::deserialized(params)?).await,
        "remove_document" => remove_document(&db, Fv::deserialized(params)?).await,
        "search_documents" => search_documents(&db, Fv::deserialized(params)?).await,
        "search_document_ids" => search_document_ids(&db, Fv::deserialized(params)?).await,
        "query_document_ids" => query_document_ids(&db, Fv::deserialized(params)?).await,
        _ => Err(RpcError::new(
            METHOD_NOT_FOUND,
            format!("method not found: {method}"),
        )),
    }
}

fn get_information(app: &AppState) -> Result<Fv, RpcError> {
    serialize_result(&InformationResult {
        name: &app.name,
        version: &app.version,
    })
}

async fn create_database(app: &AppState, req: CreateDatabaseParams) -> Result<Fv, RpcError> {
    if req.name.trim().is_empty() {
        return Err(RpcError::new(
            INVALID_PARAMS,
            "database name cannot be empty",
        ));
    }

    {
        let dbs = app.databases.read().await;
        if dbs.contains_key(&req.name) {
            return Err(RpcError::new(
                ALREADY_EXISTS,
                format!("database exists: {}", req.name),
            ));
        }
    }

    let cfg = DBConfig {
        name: req.name.clone(),
        description: req.description.unwrap_or_else(|| req.name.clone()),
        storage: app.storage.clone(),
        lock: None,
    };

    let db = Arc::new(AndaDB::connect(app.object_store.clone(), cfg).await?);

    {
        let mut dbs = app.databases.write().await;
        if dbs.contains_key(&req.name) {
            return Err(RpcError::new(
                ALREADY_EXISTS,
                format!("database exists: {}", req.name),
            ));
        }
        dbs.insert(req.name.clone(), db.clone());
    }

    serialize_result(&db.metadata())
}

async fn list_databases(app: &AppState) -> Result<Fv, RpcError> {
    let dbs = app.databases.read().await;
    let names: Vec<String> = dbs.keys().cloned().collect();
    serialize_result(&names)
}

async fn get_database(app: &AppState, db_name: &str) -> Result<Arc<AndaDB>, RpcError> {
    let dbs = app.databases.read().await;
    dbs.get(db_name)
        .cloned()
        .ok_or_else(|| RpcError::new(NOT_FOUND, format!("database not found: {db_name}")))
}

fn get_db_metadata(db: &AndaDB) -> Result<Fv, RpcError> {
    serialize_result(&db.metadata())
}

async fn flush_db(db: &AndaDB) -> Result<Fv, RpcError> {
    db.flush().await?;
    serialize_result(&StatusResult { result: "flushed" })
}

async fn create_collection(db: &AndaDB, req: CreateCollectionParams) -> Result<Fv, RpcError> {
    let btree_indexes = req.btree_indexes;
    let bm25_indexes = req.bm25_indexes;
    let hnsw_indexes = req.hnsw_indexes;

    let col = db
        .create_collection(req.schema, req.config, async |col| {
            for fields in &btree_indexes {
                let fields: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                col.create_btree_index_nx(&fields).await?;
            }
            if !bm25_indexes.is_empty() {
                col.create_bm25_index_nx(
                    &bm25_indexes
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>(),
                )
                .await?;
            }
            for hnsw in &hnsw_indexes {
                col.create_hnsw_index_nx(&hnsw.field, hnsw.config.clone())
                    .await?;
            }
            Ok(())
        })
        .await?;

    serialize_result(&col.metadata())
}

async fn get_collection_metadata(db: &AndaDB, req: CollectionParams) -> Result<Fv, RpcError> {
    let col = open_collection(db, &req.collection).await?;
    serialize_result(&col.metadata())
}

async fn delete_collection(db: &AndaDB, req: CollectionParams) -> Result<Fv, RpcError> {
    db.delete_collection(&req.collection).await?;
    serialize_result(&StatusResult { result: "deleted" })
}

async fn add_document(db: &AndaDB, mut req: AddDocumentParams) -> Result<Fv, RpcError> {
    req.document.insert("_id".to_string(), 0u64.into());
    let col = open_collection(db, &req.collection).await?;
    let id = col.add_from(&req.document).await?;
    let _ = col.flush(anda_db::unix_ms()).await;
    serialize_result(&AddDocumentResult { _id: id })
}

async fn get_document(db: &AndaDB, req: DocumentParams) -> Result<Fv, RpcError> {
    let col = open_collection(db, &req.collection).await?;
    Ok(col.get_as(req._id).await?)
}

async fn update_document(db: &AndaDB, req: UpdateDocumentParams) -> Result<Fv, RpcError> {
    let col = open_collection(db, &req.collection).await?;
    let doc = col.update(req._id, req.fields).await?;
    let _ = col.flush(anda_db::unix_ms()).await;
    doc.try_into().map_err(|e| {
        RpcError::new(
            INTERNAL_ERROR,
            format!("failed to serialize updated document: {e}"),
        )
    })
}

async fn remove_document(db: &AndaDB, req: DocumentParams) -> Result<Fv, RpcError> {
    let col = open_collection(db, &req.collection).await?;
    let doc = col.remove(req._id).await?;
    let _ = col.flush(anda_db::unix_ms()).await;
    match doc {
        Some(doc) => {
            let rt: Fv = doc.try_into().map_err(|e| {
                RpcError::new(
                    INTERNAL_ERROR,
                    format!("failed to serialize removed document: {e}"),
                )
            })?;
            Ok(rt)
        }
        None => Ok(Fv::Null),
    }
}

async fn search_documents(db: &AndaDB, req: SearchParams) -> Result<Fv, RpcError> {
    let col = open_collection(db, &req.collection).await?;
    let docs: Vec<Fv> = col.search_as(req.query).await?;
    serialize_result(&docs)
}

async fn search_document_ids(db: &AndaDB, req: SearchParams) -> Result<Fv, RpcError> {
    let col = open_collection(db, &req.collection).await?;
    let ids = col.search_ids(req.query).await?;
    serialize_result(&ids)
}

async fn query_document_ids(db: &AndaDB, req: QueryIdsParams) -> Result<Fv, RpcError> {
    let col = open_collection(db, &req.collection).await?;
    let ids = col.query_ids(req.filter, req.limit).await?;
    serialize_result(&ids)
}

async fn open_collection(
    db: &AndaDB,
    name: &str,
) -> Result<Arc<anda_db::collection::Collection>, RpcError> {
    Ok(db
        .open_collection(name.to_string(), async |_| Ok(()))
        .await?)
}
