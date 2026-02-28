//! Integration tests for anda_db_server RPC API.
//!
//! Routing model:
//! - `POST /` for root-level methods (`create_database`, `list_databases`)
//! - `POST /{db_name}` for database-scoped methods.

use anda_db::{
    database::{AndaDB, DBConfig},
    storage::StorageConfig,
};
use anda_db_server::{build_router, handler::AppState};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use object_store::{ObjectStore, memory::InMemory};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;
use tower::ServiceExt;

const PARSE_ERROR: i64 = -32700;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const NOT_FOUND: i64 = -32001;
const ALREADY_EXISTS: i64 = -32002;

async fn test_app() -> Router {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let storage = StorageConfig {
        cache_max_capacity: 10000,
        compress_level: 0,
        object_chunk_size: 256 * 1024,
        bucket_overload_size: 1024 * 1024,
        max_small_object_size: 1024 * 1024 * 10,
    };

    let db_cfg = DBConfig {
        name: "test_db".to_string(),
        description: "Test DB".to_string(),
        storage: storage.clone(),
        lock: None,
    };
    let db = Arc::new(AndaDB::connect(object_store.clone(), db_cfg).await.unwrap());

    let mut databases = BTreeMap::new();
    databases.insert("test_db".to_string(), db);

    let state = AppState {
        databases: Arc::new(RwLock::new(databases)),
        object_store,
        storage,
        name: "test".to_string(),
        version: "0.0.0".to_string(),
        api_key: None,
    };
    build_router(state)
}

async fn test_app_with_auth() -> Router {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let storage = StorageConfig {
        cache_max_capacity: 10000,
        compress_level: 0,
        object_chunk_size: 256 * 1024,
        bucket_overload_size: 1024 * 1024,
        max_small_object_size: 1024 * 1024 * 10,
    };

    let db_cfg = DBConfig {
        name: "test_db".to_string(),
        description: "Test DB".to_string(),
        storage: storage.clone(),
        lock: None,
    };
    let db = Arc::new(AndaDB::connect(object_store.clone(), db_cfg).await.unwrap());

    let mut databases = BTreeMap::new();
    databases.insert("test_db".to_string(), db);

    let state = AppState {
        databases: Arc::new(RwLock::new(databases)),
        object_store,
        storage,
        name: "test".to_string(),
        version: "0.0.0".to_string(),
        api_key: Some("test-secret".to_string()),
    };
    build_router(state)
}

async fn body_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes().to_vec();
    serde_json::from_slice(&bytes).unwrap()
}

async fn body_cbor_to_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes().to_vec();
    ciborium::de::from_reader(&bytes[..]).unwrap()
}

async fn rpc_call_path(app: &Router, path: &str, method: &str, params: Option<Value>) -> Value {
    let req = json!({
        "method": method,
        "params": params,
    });

    let resp = app
        .clone()
        .oneshot(
            Request::post(path)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    body_json(resp.into_body()).await
}

async fn rpc_call_root(app: &Router, method: &str, params: Option<Value>) -> Value {
    rpc_call_path(app, "/", method, params).await
}

async fn rpc_call_db(app: &Router, db_name: &str, method: &str, params: Option<Value>) -> Value {
    let path = format!("/{db_name}");
    rpc_call_path(app, &path, method, params).await
}

fn rpc_result(resp: &Value) -> &Value {
    assert!(resp.get("error").is_none());
    resp.get("result").unwrap()
}

fn rpc_error(resp: &Value) -> &Value {
    assert!(resp.get("result").is_none());
    resp.get("error").unwrap()
}

fn articles_schema_json() -> Value {
    json!({
        "fields": [
            {"n": "_id", "d": "", "t": "U64", "u": true, "i": 0},
            {"n": "title", "d": "Article title", "t": "Text", "u": false, "i": 1},
            {"n": "body", "d": "Article content", "t": "Text", "u": false, "i": 2},
            {"n": "score", "d": "Relevance score", "t": {"Option": "U64"}, "u": false, "i": 3}
        ]
    })
}

fn create_articles_params() -> Value {
    json!({
        "config": {
            "name": "articles",
            "description": "Articles collection"
        },
        "schema": articles_schema_json(),
        "btree_indexes": [["score"]],
        "bm25_indexes": [],
        "hnsw_indexes": []
    })
}

async fn setup_collection(app: &Router, db_name: &str) {
    let resp = rpc_call_db(
        app,
        db_name,
        "create_collection",
        Some(create_articles_params()),
    )
    .await;
    assert_eq!(rpc_result(&resp)["config"]["name"], "articles");
}

#[tokio::test]
async fn test_root_create_and_list_databases() {
    let app = test_app().await;

    let list = rpc_call_root(&app, "list_databases", None).await;
    let names = rpc_result(&list).as_array().unwrap();
    assert!(names.iter().any(|v| v == "test_db"));

    let create = rpc_call_root(
        &app,
        "create_database",
        Some(json!({"name": "tenant_a", "description": "Tenant A"})),
    )
    .await;
    assert_eq!(rpc_result(&create)["config"]["name"], "tenant_a");

    let dup = rpc_call_root(&app, "create_database", Some(json!({"name": "tenant_a"}))).await;
    assert_eq!(rpc_error(&dup)["code"], ALREADY_EXISTS);
}

#[tokio::test]
async fn test_db_scoped_metadata_and_flush() {
    let app = test_app().await;

    let meta = rpc_call_db(&app, "test_db", "get_db_metadata", None).await;
    assert_eq!(rpc_result(&meta)["config"]["name"], "test_db");

    let flush = rpc_call_db(&app, "test_db", "flush_db", None).await;
    assert_eq!(rpc_result(&flush)["result"], "flushed");

    let missing = rpc_call_db(&app, "not_exists", "get_db_metadata", None).await;
    assert_eq!(rpc_error(&missing)["code"], NOT_FOUND);
}

#[tokio::test]
async fn test_collection_and_document_crud_on_db_path() {
    let app = test_app().await;
    setup_collection(&app, "test_db").await;

    let add = rpc_call_db(
        &app,
        "test_db",
        "add_document",
        Some(json!({
            "collection": "articles",
            "document": {
                "title": "Hello World",
                "body": "This is a test article.",
                "score": 42
            }
        })),
    )
    .await;
    let doc_id = rpc_result(&add)["_id"].as_u64().unwrap();

    let get = rpc_call_db(
        &app,
        "test_db",
        "get_document",
        Some(json!({"collection": "articles", "id": doc_id})),
    )
    .await;
    assert_eq!(rpc_result(&get)["title"], "Hello World");

    let update = rpc_call_db(
        &app,
        "test_db",
        "update_document",
        Some(json!({
            "collection": "articles",
            "id": doc_id,
            "fields": {"title": "Updated"}
        })),
    )
    .await;
    assert_eq!(rpc_result(&update)["title"], "Updated");

    let remove = rpc_call_db(
        &app,
        "test_db",
        "remove_document",
        Some(json!({"collection": "articles", "id": doc_id})),
    )
    .await;
    assert_eq!(rpc_result(&remove)["title"], "Updated");
}

#[tokio::test]
async fn test_search_and_query_ids_on_db_path() {
    let app = test_app().await;
    setup_collection(&app, "test_db").await;

    for i in 0..3 {
        let _ = rpc_call_db(
            &app,
            "test_db",
            "add_document",
            Some(json!({
                "collection": "articles",
                "document": {
                    "title": format!("Article {i}"),
                    "body": format!("Content {i}"),
                    "score": i * 10
                }
            })),
        )
        .await;
    }

    let search_docs = rpc_call_db(
        &app,
        "test_db",
        "search_documents",
        Some(json!({
            "collection": "articles",
            "query": {
                "filter": {"Field": ["score", {"Ge": 0}]},
                "limit": 2
            }
        })),
    )
    .await;
    assert_eq!(rpc_result(&search_docs).as_array().unwrap().len(), 2);

    let query_ids = rpc_call_db(
        &app,
        "test_db",
        "query_document_ids",
        Some(json!({
            "collection": "articles",
            "filter": {"Field": ["score", {"Gt": 15}]},
            "limit": 10
        })),
    )
    .await;
    assert_eq!(rpc_result(&query_ids).as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_database_isolation_across_db_name_paths() {
    let app = test_app().await;

    let create_db = rpc_call_root(
        &app,
        "create_database",
        Some(json!({"name": "tenant_b", "description": "Tenant B"})),
    )
    .await;
    assert_eq!(rpc_result(&create_db)["config"]["name"], "tenant_b");

    setup_collection(&app, "test_db").await;
    setup_collection(&app, "tenant_b").await;

    let _ = rpc_call_db(
        &app,
        "test_db",
        "add_document",
        Some(json!({
            "collection": "articles",
            "document": {
                "title": "Only In Test DB",
                "body": "A",
                "score": 10
            }
        })),
    )
    .await;

    let _ = rpc_call_db(
        &app,
        "tenant_b",
        "add_document",
        Some(json!({
            "collection": "articles",
            "document": {
                "title": "Only In Tenant B",
                "body": "B",
                "score": 20
            }
        })),
    )
    .await;

    let test_db_docs = rpc_call_db(
        &app,
        "test_db",
        "search_documents",
        Some(json!({
            "collection": "articles",
            "query": {
                "filter": {"Field": ["score", {"Ge": 0}]},
                "limit": 10
            }
        })),
    )
    .await;
    let test_db_docs = rpc_result(&test_db_docs).as_array().unwrap();
    assert_eq!(test_db_docs.len(), 1);
    assert_eq!(test_db_docs[0]["title"], "Only In Test DB");

    let tenant_docs = rpc_call_db(
        &app,
        "tenant_b",
        "search_documents",
        Some(json!({
            "collection": "articles",
            "query": {
                "filter": {"Field": ["score", {"Ge": 0}]},
                "limit": 10
            }
        })),
    )
    .await;
    let tenant_docs = rpc_result(&tenant_docs).as_array().unwrap();
    assert_eq!(tenant_docs.len(), 1);
    assert_eq!(tenant_docs[0]["title"], "Only In Tenant B");
}

#[tokio::test]
async fn test_cross_database_document_access_returns_not_found() {
    let app = test_app().await;

    let create_db = rpc_call_root(
        &app,
        "create_database",
        Some(json!({"name": "tenant_c", "description": "Tenant C"})),
    )
    .await;
    assert_eq!(rpc_result(&create_db)["config"]["name"], "tenant_c");

    setup_collection(&app, "test_db").await;
    setup_collection(&app, "tenant_c").await;

    let add = rpc_call_db(
        &app,
        "test_db",
        "add_document",
        Some(json!({
            "collection": "articles",
            "document": {
                "title": "Doc In Test DB",
                "body": "Only here",
                "score": 7
            }
        })),
    )
    .await;
    let doc_id = rpc_result(&add)["_id"].as_u64().unwrap();

    let wrong_db_get = rpc_call_db(
        &app,
        "tenant_c",
        "get_document",
        Some(json!({"collection": "articles", "id": doc_id})),
    )
    .await;
    assert_eq!(rpc_error(&wrong_db_get)["code"], NOT_FOUND);

    let right_db_get = rpc_call_db(
        &app,
        "test_db",
        "get_document",
        Some(json!({"collection": "articles", "id": doc_id})),
    )
    .await;
    assert_eq!(rpc_result(&right_db_get)["title"], "Doc In Test DB");
}

#[tokio::test]
async fn test_auth_middleware_on_db_path() {
    let app = test_app_with_auth().await;

    let req = json!({"method": "get_db_metadata"});

    let resp = app
        .clone()
        .oneshot(
            Request::post("/test_db")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = app
        .clone()
        .oneshot(
            Request::post("/test_db")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", "Bearer wrong-key")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = app
        .clone()
        .oneshot(
            Request::post("/test_db")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", "Bearer test-secret")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_cbor_request_response_on_db_path() {
    let app = test_app().await;

    let req = json!({
        "method": "get_db_metadata",
    });
    let mut body = Vec::new();
    ciborium::ser::into_writer(&req, &mut body).unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::post("/test_db")
                .header(header::CONTENT_TYPE, "application/cbor")
                .header(header::ACCEPT, "application/cbor")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/cbor"
    );

    let decoded = body_cbor_to_json(resp.into_body()).await;
    assert_eq!(rpc_result(&decoded)["config"]["name"], "test_db");
}

#[tokio::test]
async fn test_rpc_parse_and_method_errors() {
    let app = test_app().await;

    let resp = app
        .clone()
        .oneshot(
            Request::post("/")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("not valid json {{{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed = body_json(resp.into_body()).await;
    assert_eq!(rpc_error(&parsed)["code"], PARSE_ERROR);

    let method_missing = rpc_call_root(&app, "unknown_method", None).await;
    assert_eq!(rpc_error(&method_missing)["code"], METHOD_NOT_FOUND);

    let bad_params = rpc_call_db(
        &app,
        "test_db",
        "get_document",
        Some(json!({"collection": "articles"})),
    )
    .await;
    assert_eq!(rpc_error(&bad_params)["code"], INVALID_PARAMS);
}

#[tokio::test]
async fn test_cbor_request_uses_cbor_value_params() {
    let app = test_app().await;

    let req = ciborium::Value::Map(vec![
        (
            ciborium::Value::Text("method".to_string()),
            ciborium::Value::Text("create_database".to_string()),
        ),
        (
            ciborium::Value::Text("params".to_string()),
            ciborium::Value::Map(vec![
                (
                    ciborium::Value::Text("name".to_string()),
                    ciborium::Value::Text("tenant_cbor".to_string()),
                ),
                (
                    ciborium::Value::Text("description".to_string()),
                    ciborium::Value::Text("From CBOR".to_string()),
                ),
            ]),
        ),
    ]);

    let mut body = Vec::new();
    ciborium::ser::into_writer(&req, &mut body).unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::post("/")
                .header(header::CONTENT_TYPE, "application/cbor")
                .header(header::ACCEPT, "application/cbor")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let decoded = body_cbor_to_json(resp.into_body()).await;
    assert_eq!(rpc_result(&decoded)["config"]["name"], "tenant_cbor");
}

#[tokio::test]
async fn test_mixed_protocol_cbor_request_json_response() {
    let app = test_app().await;

    let req = ciborium::Value::Map(vec![
        (
            ciborium::Value::Text("method".to_string()),
            ciborium::Value::Text("create_database".to_string()),
        ),
        (
            ciborium::Value::Text("params".to_string()),
            ciborium::Value::Map(vec![(
                ciborium::Value::Text("name".to_string()),
                ciborium::Value::Text("tenant_mix_1".to_string()),
            )]),
        ),
    ]);

    let mut body = Vec::new();
    ciborium::ser::into_writer(&req, &mut body).unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::post("/")
                .header(header::CONTENT_TYPE, "application/cbor")
                .header(header::ACCEPT, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let decoded = body_json(resp.into_body()).await;
    assert_eq!(rpc_result(&decoded)["config"]["name"], "tenant_mix_1");
}

#[tokio::test]
async fn test_mixed_protocol_json_request_cbor_response() {
    let app = test_app().await;

    let req = json!({"method": "get_db_metadata"});
    let resp = app
        .clone()
        .oneshot(
            Request::post("/test_db")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::ACCEPT, "application/cbor")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/cbor"
    );
    let decoded = body_cbor_to_json(resp.into_body()).await;
    assert_eq!(rpc_result(&decoded)["config"]["name"], "test_db");
}
