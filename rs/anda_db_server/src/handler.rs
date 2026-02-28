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
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
    response::IntoResponse,
};
use ciborium::Value as Cbor;
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
    pub bm25_indexes: Vec<Vec<String>>,
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
    pub document: JsonValue,
}

#[derive(Debug, Deserialize)]
pub struct DocumentParams {
    pub collection: String,
    pub id: DocumentId,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDocumentParams {
    pub collection: String,
    pub id: DocumentId,
    pub fields: JsonValue,
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
    F: Fn(String, JsonValue) -> Fut,
    Fut: std::future::Future<Output = Result<JsonValue, RpcError>>,
{
    let req_ct = ContentType::from_content_type(&headers);
    match parse_request(&body, req_ct) {
        Ok(ParsedRpcRequest::Json(req)) => {
            let params = req.params.unwrap_or(JsonValue::Null);
            match dispatch(req.method, params).await {
                Ok(value) => AppResponse::new(JsonRpcResponse::success(value), ct).into_response(),
                Err(err) => AppResponse::new(JsonRpcResponse::error(err), ct).into_response(),
            }
        }
        Ok(ParsedRpcRequest::Cbor(req)) => {
            let params = match req.params {
                Some(v) => match cbor_to_json(v) {
                    Ok(value) => value,
                    Err(err) => {
                        return AppResponse::new(CborRpcResponse::error(err), ct).into_response();
                    }
                },
                None => JsonValue::Null,
            };

            match dispatch(req.method, params).await {
                Ok(value) => match json_to_cbor(&value) {
                    Ok(cbor_value) => {
                        AppResponse::new(CborRpcResponse::success(cbor_value), ct).into_response()
                    }
                    Err(err) => AppResponse::new(CborRpcResponse::error(err), ct).into_response(),
                },
                Err(err) => AppResponse::new(CborRpcResponse::error(err), ct).into_response(),
            }
        }
        Err(err) => match ct {
            ContentType::Json => AppResponse::new(JsonRpcResponse::error(err), ct).into_response(),
            ContentType::Cbor => AppResponse::new(CborRpcResponse::error(err), ct).into_response(),
        },
    }
}

enum ParsedRpcRequest {
    Json(JsonRpcRequest),
    Cbor(CborRpcRequest),
}

fn parse_request(body: &[u8], ct: ContentType) -> Result<ParsedRpcRequest, RpcError> {
    match ct {
        ContentType::Json => serde_json::from_slice(body)
            .map(ParsedRpcRequest::Json)
            .map_err(|e| RpcError::new(PARSE_ERROR, format!("parse error: {e}"))),
        ContentType::Cbor => ciborium::de::from_reader(body)
            .map(ParsedRpcRequest::Cbor)
            .map_err(|e| RpcError::new(PARSE_ERROR, format!("parse error: {e}"))),
    }
}

fn cbor_to_json(value: Cbor) -> Result<JsonValue, RpcError> {
    serde_json::to_value(value)
        .map_err(|e| RpcError::new(INVALID_PARAMS, format!("invalid CBOR params: {e}")))
}

fn json_to_cbor(value: &JsonValue) -> Result<Cbor, RpcError> {
    Cbor::serialized(value).map_err(|e| {
        RpcError::new(
            INTERNAL_ERROR,
            format!("failed to build CBOR response: {e}"),
        )
    })
}

async fn dispatch_root(
    app: &AppState,
    method: &str,
    params: JsonValue,
) -> Result<JsonValue, RpcError> {
    match method {
        "get_information" => get_information(app),
        "create_database" => create_database(app, params).await,
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
    params: JsonValue,
) -> Result<JsonValue, RpcError> {
    let db = get_database(app, db_name).await?;

    match method {
        "get_information" => get_information(app),
        "get_db_metadata" => get_db_metadata(&db),
        "flush_db" => flush_db(&db).await,
        "create_collection" => create_collection(&db, params).await,
        "get_collection_metadata" => get_collection_metadata(&db, params).await,
        "delete_collection" => delete_collection(&db, params).await,
        "add_document" => add_document(&db, params).await,
        "get_document" => get_document(&db, params).await,
        "update_document" => update_document(&db, params).await,
        "remove_document" => remove_document(&db, params).await,
        "search_documents" => search_documents(&db, params).await,
        "search_document_ids" => search_document_ids(&db, params).await,
        "query_document_ids" => query_document_ids(&db, params).await,
        _ => Err(RpcError::new(
            METHOD_NOT_FOUND,
            format!("method not found: {method}"),
        )),
    }
}

fn get_information(app: &AppState) -> Result<JsonValue, RpcError> {
    Ok(serde_json::json!({
        "name": app.name,
        "version": app.version,
    }))
}

async fn create_database(app: &AppState, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: CreateDatabaseParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;

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

    let db = Arc::new(
        AndaDB::connect(app.object_store.clone(), cfg)
            .await
            .map_err(RpcError::from)?,
    );

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

    serde_json::to_value(db.metadata()).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn list_databases(app: &AppState) -> Result<JsonValue, RpcError> {
    let dbs = app.databases.read().await;
    let names: Vec<String> = dbs.keys().cloned().collect();
    serde_json::to_value(names).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn get_database(app: &AppState, db_name: &str) -> Result<Arc<AndaDB>, RpcError> {
    let dbs = app.databases.read().await;
    dbs.get(db_name)
        .cloned()
        .ok_or_else(|| RpcError::new(NOT_FOUND, format!("database not found: {db_name}")))
}

fn get_db_metadata(db: &AndaDB) -> Result<JsonValue, RpcError> {
    let metadata = db.metadata();
    serde_json::to_value(metadata).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn flush_db(db: &AndaDB) -> Result<JsonValue, RpcError> {
    db.flush().await.map_err(RpcError::from)?;
    Ok(serde_json::json!({"result": "flushed"}))
}

async fn create_collection(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: CreateCollectionParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;

    let btree_indexes = req.btree_indexes;
    let bm25_indexes = req.bm25_indexes;
    let hnsw_indexes = req.hnsw_indexes;

    let col = db
        .create_collection(req.schema, req.config, async |col| {
            for fields in &btree_indexes {
                let fields: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                col.create_btree_index_nx(&fields).await?;
            }
            for fields in &bm25_indexes {
                let fields: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                col.create_bm25_index_nx(&fields).await?;
            }
            for hnsw in &hnsw_indexes {
                col.create_hnsw_index_nx(&hnsw.field, hnsw.config.clone())
                    .await?;
            }
            Ok(())
        })
        .await
        .map_err(RpcError::from)?;

    serde_json::to_value(col.metadata()).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn get_collection_metadata(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: CollectionParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;
    let col = open_collection(db, &req.collection).await?;
    serde_json::to_value(col.metadata()).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn delete_collection(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: CollectionParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;
    db.delete_collection(&req.collection)
        .await
        .map_err(RpcError::from)?;
    Ok(serde_json::json!({"result": "deleted"}))
}

async fn add_document(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let mut req: AddDocumentParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;

    let col = open_collection(db, &req.collection).await?;
    if let Some(obj) = req.document.as_object_mut() {
        obj.entry("_id").or_insert(JsonValue::from(0u64));
    }

    let id = col.add_from(&req.document).await.map_err(RpcError::from)?;
    let _ = col.flush(anda_db::unix_ms()).await;
    Ok(serde_json::json!({"_id": id}))
}

async fn get_document(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: DocumentParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;
    let col = open_collection(db, &req.collection).await?;
    let doc: JsonValue = col.get_as(req.id).await.map_err(RpcError::from)?;
    Ok(doc)
}

async fn update_document(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: UpdateDocumentParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;
    let col = open_collection(db, &req.collection).await?;
    let fields = value_to_update_fields(col.schema().as_ref(), &req.fields)?;
    let doc = col.update(req.id, fields).await.map_err(RpcError::from)?;
    let doc: JsonValue = doc.try_into().map_err(RpcError::from)?;
    let _ = col.flush(anda_db::unix_ms()).await;
    Ok(doc)
}

async fn remove_document(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: DocumentParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;
    let col = open_collection(db, &req.collection).await?;
    let doc = col.remove(req.id).await.map_err(RpcError::from)?;
    let result: JsonValue = match doc {
        Some(doc) => doc.try_into().map_err(RpcError::from)?,
        None => JsonValue::Null,
    };
    let _ = col.flush(anda_db::unix_ms()).await;
    Ok(result)
}

async fn search_documents(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: SearchParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;
    let col = open_collection(db, &req.collection).await?;
    let docs: Vec<JsonValue> = col.search_as(req.query).await.map_err(RpcError::from)?;
    serde_json::to_value(docs).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn search_document_ids(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: SearchParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;
    let col = open_collection(db, &req.collection).await?;
    let ids = col.search_ids(req.query).await.map_err(RpcError::from)?;
    serde_json::to_value(ids).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn query_document_ids(db: &AndaDB, params: JsonValue) -> Result<JsonValue, RpcError> {
    let req: QueryIdsParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;
    let col = open_collection(db, &req.collection).await?;
    let ids = col
        .query_ids(req.filter, req.limit)
        .await
        .map_err(RpcError::from)?;
    serde_json::to_value(ids).map_err(|e| RpcError::new(INTERNAL_ERROR, e.to_string()))
}

async fn open_collection(
    db: &AndaDB,
    name: &str,
) -> Result<Arc<anda_db::collection::Collection>, RpcError> {
    db.open_collection(name.to_string(), async |_| Ok(()))
        .await
        .map_err(RpcError::from)
}

fn value_to_update_fields(
    schema: &anda_db_schema::Schema,
    value: &JsonValue,
) -> Result<BTreeMap<String, Fv>, RpcError> {
    let cbor = Cbor::serialized(value)
        .map_err(|e| RpcError::new(INVALID_PARAMS, format!("failed to convert value: {e}")))?;

    let map = cbor
        .into_map()
        .map_err(|e| RpcError::new(INVALID_PARAMS, format!("expected object/map, got: {e:?}")))?;

    let mut fields = BTreeMap::new();
    for (k, v) in map {
        let name = k.into_text().map_err(|e| {
            RpcError::new(INVALID_PARAMS, format!("expected string key, got: {e:?}"))
        })?;

        if name == anda_db_schema::Schema::ID_KEY {
            continue;
        }

        let field = schema
            .get_field_or_err(&name)
            .map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;

        let fv = field
            .extract(v, false)
            .map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;

        fields.insert(name, fv);
    }

    Ok(fields)
}
