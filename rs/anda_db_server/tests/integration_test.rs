//! Integration tests for the Anda DB Server RPC API.
//!
//! The wire protocol is CBOR-first: most tests send CBOR request bodies and
//! decode CBOR responses, transcoding through `serde_json::Value` only for
//! easy assertions. JSON round-trips and encoding negotiation are covered
//! separately.

use anda_db_server::{AppState, ServerOptions, build_router};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use object_store::{ObjectStore, memory::InMemory};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

const PRIMARY_DB: &str = "test_db";

fn test_options(api_key: Option<String>) -> ServerOptions {
    ServerOptions {
        name: "test".to_string(),
        version: "0.0.0".to_string(),
        primary_db: PRIMARY_DB.to_string(),
        description: "Test server".to_string(),
        api_key,
        flush_interval: Duration::from_secs(60),
        ..Default::default()
    }
}

async fn test_state(object_store: Arc<dyn ObjectStore>, api_key: Option<String>) -> AppState {
    AppState::connect(object_store, test_options(api_key))
        .await
        .expect("failed to connect AppState")
}

async fn test_app() -> Router {
    build_router(test_state(Arc::new(InMemory::new()), None).await)
}

/// Sends an RPC request encoded as CBOR and decodes the CBOR response.
/// Returns the HTTP status and the response body transcoded to JSON.
async fn rpc_cbor(app: &Router, path: &str, method: &str, params: Value) -> (StatusCode, Value) {
    let req = json!({"method": method, "params": params});
    let mut body = Vec::new();
    ciborium::ser::into_writer(&req, &mut body).unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::post(path)
                .header(header::CONTENT_TYPE, "application/cbor")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = resp.status();
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/cbor"
    );
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = ciborium::de::from_reader(&bytes[..]).unwrap();
    (status, value)
}

/// Like [`rpc_cbor`] but asserts HTTP 200 and unwraps `result`.
async fn rpc_ok(app: &Router, path: &str, method: &str, params: Value) -> Value {
    let (status, resp) = rpc_cbor(app, path, method, params).await;
    assert_eq!(status, StatusCode::OK, "unexpected response: {resp:?}");
    resp.get("result")
        .unwrap_or_else(|| panic!("missing result: {resp:?}"))
        .clone()
}

/// Like [`rpc_cbor`] but asserts an error status and unwraps `error`.
async fn rpc_err(
    app: &Router,
    path: &str,
    method: &str,
    params: Value,
    status: StatusCode,
) -> Value {
    let (got, resp) = rpc_cbor(app, path, method, params).await;
    assert_eq!(got, status, "unexpected response: {resp:?}");
    resp.get("error")
        .unwrap_or_else(|| panic!("missing error: {resp:?}"))
        .clone()
}

fn articles_schema() -> Value {
    json!({
        "fields": [
            {"name": "_id", "description": "", "type": "U64", "unique": true, "index": 0},
            {"name": "title", "description": "Article title", "type": "Text", "unique": false, "index": 1},
            {"name": "body", "description": "Article content", "type": "Text", "unique": false, "index": 2},
            {"name": "score", "description": "Relevance score", "type": {"Option": "U64"}, "unique": false, "index": 3}
        ]
    })
}

fn create_articles_params() -> Value {
    json!({
        "config": {"name": "articles", "description": "Articles collection"},
        "schema": articles_schema(),
        "btree_indexes": [["score"]],
        "bm25_indexes": ["title", "body"]
    })
}

async fn setup_articles(app: &Router, db: &str) {
    let meta = rpc_ok(
        app,
        &format!("/{db}"),
        "collection.create",
        create_articles_params(),
    )
    .await;
    assert_eq!(meta["config"]["name"], "articles");
}

async fn add_article(app: &Router, db: &str, title: &str, body: &str, score: u64) -> u64 {
    let added = rpc_ok(
        app,
        &format!("/{db}"),
        "doc.add",
        json!({
            "collection": "articles",
            "doc": {"title": title, "body": body, "score": score}
        }),
    )
    .await;
    added["_id"].as_u64().unwrap()
}

#[tokio::test]
async fn test_health_endpoint_is_unauthenticated_json() {
    let app = build_router(test_state(Arc::new(InMemory::new()), Some("secret".to_string())).await);

    let resp = app
        .clone()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["result"]["name"], "test");
    assert_eq!(value["result"]["version"], "0.0.0");
    // The health endpoint must not leak database names.
    assert!(value["result"].get("databases").is_none());
}

#[tokio::test]
async fn test_root_info_and_database_lifecycle() {
    let app = test_app().await;

    let info = rpc_ok(&app, "/", "info", Value::Null).await;
    assert_eq!(info["primary_db"], PRIMARY_DB);
    assert_eq!(info["databases"], json!([PRIMARY_DB]));

    let created = rpc_ok(&app, "/", "db.create", json!({"name": "tenant_a"})).await;
    assert_eq!(created["config"]["name"], "tenant_a");

    let names = rpc_ok(&app, "/", "db.list", Value::Null).await;
    assert_eq!(names, json!(["tenant_a", PRIMARY_DB]));

    // Duplicate creation conflicts.
    let err = rpc_err(
        &app,
        "/",
        "db.create",
        json!({"name": "tenant_a"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(err["code"], "already_exists");

    // `db.connect` on an existing database is a no-op returning its metadata.
    let connected = rpc_ok(&app, "/", "db.connect", json!({"name": "tenant_a"})).await;
    assert_eq!(connected["config"]["name"], "tenant_a");

    // Close, then reopen.
    rpc_ok(&app, "/", "db.close", json!({"name": "tenant_a"})).await;
    let names = rpc_ok(&app, "/", "db.list", Value::Null).await;
    assert_eq!(names, json!([PRIMARY_DB]));

    let reopened = rpc_ok(&app, "/", "db.open", json!({"name": "tenant_a"})).await;
    assert_eq!(reopened["config"]["name"], "tenant_a");

    // Opening a database that was never created fails with 404.
    let err = rpc_err(
        &app,
        "/",
        "db.open",
        json!({"name": "nope"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(err["code"], "not_found");

    // The primary database cannot be closed.
    let err = rpc_err(
        &app,
        "/",
        "db.close",
        json!({"name": PRIMARY_DB}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "bad_request");

    // Invalid database names are rejected before touching storage.
    let err = rpc_err(
        &app,
        "/",
        "db.create",
        json!({"name": "Bad-Name"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "bad_request");
}

#[tokio::test]
async fn test_database_registry_survives_restart() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let state = test_state(object_store.clone(), None).await;
    let app = build_router(state.clone());
    rpc_ok(&app, "/", "db.create", json!({"name": "tenant_a"})).await;
    rpc_ok(&app, "/", "db.create", json!({"name": "tenant_b"})).await;
    rpc_ok(&app, "/", "db.close", json!({"name": "tenant_b"})).await;
    state.shutdown().await;

    // A new server over the same object store reopens registered databases;
    // tenant_b was closed and must stay closed.
    let app = build_router(test_state(object_store, None).await);
    let names = rpc_ok(&app, "/", "db.list", Value::Null).await;
    assert_eq!(names, json!(["tenant_a", PRIMARY_DB]));
}

#[tokio::test]
async fn test_collection_lifecycle() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");

    setup_articles(&app, PRIMARY_DB).await;

    let names = rpc_ok(&app, &path, "collection.list", Value::Null).await;
    assert_eq!(names, json!(["articles"]));

    let meta = rpc_ok(
        &app,
        &path,
        "collection.metadata",
        json!({"collection": "articles"}),
    )
    .await;
    assert_eq!(meta["config"]["name"], "articles");
    assert_eq!(meta["btree_indexes"].as_object().unwrap().len(), 1);
    assert_eq!(meta["bm25_indexes"].as_object().unwrap().len(), 1);

    let stats = rpc_ok(
        &app,
        &path,
        "collection.stats",
        json!({"collection": "articles"}),
    )
    .await;
    assert_eq!(stats["num_documents"], 0);

    // Duplicate creation conflicts, while ensure is idempotent.
    let err = rpc_err(
        &app,
        &path,
        "collection.create",
        create_articles_params(),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(err["code"], "already_exists");
    let meta = rpc_ok(&app, &path, "collection.ensure", create_articles_params()).await;
    assert_eq!(meta["config"]["name"], "articles");

    rpc_ok(
        &app,
        &path,
        "collection.flush",
        json!({"collection": "articles"}),
    )
    .await;
    rpc_ok(
        &app,
        &path,
        "collection.delete",
        json!({"collection": "articles"}),
    )
    .await;
    let names = rpc_ok(&app, &path, "collection.list", Value::Null).await;
    assert_eq!(names, json!([]));
}

#[tokio::test]
async fn test_document_crud() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");
    setup_articles(&app, PRIMARY_DB).await;

    let id = add_article(&app, PRIMARY_DB, "Hello World", "This is a test.", 42).await;

    let doc = rpc_ok(
        &app,
        &path,
        "doc.get",
        json!({"collection": "articles", "_id": id}),
    )
    .await;
    assert_eq!(doc["title"], "Hello World");
    assert_eq!(doc["score"], 42);

    let exists = rpc_ok(
        &app,
        &path,
        "doc.exists",
        json!({"collection": "articles", "_id": id}),
    )
    .await;
    assert_eq!(exists, json!(true));

    let count = rpc_ok(&app, &path, "doc.count", json!({"collection": "articles"})).await;
    assert_eq!(count, json!(1));

    let updated = rpc_ok(
        &app,
        &path,
        "doc.update",
        json!({"collection": "articles", "_id": id, "fields": {"title": "Updated"}}),
    )
    .await;
    assert_eq!(updated["title"], "Updated");

    let removed = rpc_ok(
        &app,
        &path,
        "doc.remove",
        json!({"collection": "articles", "_id": id}),
    )
    .await;
    assert_eq!(removed["title"], "Updated");

    // Removing again returns null; getting returns 404.
    let removed = rpc_ok(
        &app,
        &path,
        "doc.remove",
        json!({"collection": "articles", "_id": id}),
    )
    .await;
    assert_eq!(removed, Value::Null);

    let err = rpc_err(
        &app,
        &path,
        "doc.get",
        json!({"collection": "articles", "_id": id}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(err["code"], "not_found");
}

#[tokio::test]
async fn test_document_batch_operations() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");
    setup_articles(&app, PRIMARY_DB).await;

    let added = rpc_ok(
        &app,
        &path,
        "doc.add_many",
        json!({
            "collection": "articles",
            "docs": [
                {"title": "A", "body": "first", "score": 1},
                {"title": "B", "body": "second", "score": 2},
                {"title": "C", "body": "third", "score": 3}
            ]
        }),
    )
    .await;
    let ids: Vec<u64> = added
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["_id"].as_u64().unwrap())
        .collect();
    assert_eq!(ids.len(), 3);

    let docs = rpc_ok(
        &app,
        &path,
        "doc.get_many",
        json!({"collection": "articles", "_ids": [ids[0], 9999, ids[2]]}),
    )
    .await;
    let docs = docs.as_array().unwrap();
    assert_eq!(docs[0]["title"], "A");
    assert_eq!(docs[1], Value::Null);
    assert_eq!(docs[2]["title"], "C");
}

#[tokio::test]
async fn test_search_and_query_ids() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");
    setup_articles(&app, PRIMARY_DB).await;

    for i in 0u64..3 {
        add_article(
            &app,
            PRIMARY_DB,
            &format!("Article {i}"),
            &format!("Anda DB content number {i}"),
            i * 10,
        )
        .await;
    }

    // B-Tree filter search.
    let docs = rpc_ok(
        &app,
        &path,
        "doc.search",
        json!({
            "collection": "articles",
            "query": {"filter": {"Field": ["score", {"Ge": 10}]}, "limit": 10}
        }),
    )
    .await;
    assert_eq!(docs.as_array().unwrap().len(), 2);

    // Full-text search through the BM25 index.
    let docs = rpc_ok(
        &app,
        &path,
        "doc.search",
        json!({
            "collection": "articles",
            "query": {"search": {"text": "Anda"}, "limit": 10}
        }),
    )
    .await;
    assert_eq!(docs.as_array().unwrap().len(), 3);

    let ids = rpc_ok(
        &app,
        &path,
        "doc.search_ids",
        json!({
            "collection": "articles",
            "query": {"filter": {"Field": ["score", {"Ge": 0}]}, "limit": 2}
        }),
    )
    .await;
    assert_eq!(ids.as_array().unwrap().len(), 2);

    let ids = rpc_ok(
        &app,
        &path,
        "doc.query_ids",
        json!({
            "collection": "articles",
            "filter": {"Field": ["score", {"Gt": 15}]},
            "limit": 10
        }),
    )
    .await;
    assert_eq!(ids.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_database_isolation() {
    let app = test_app().await;

    rpc_ok(&app, "/", "db.create", json!({"name": "tenant_b"})).await;
    setup_articles(&app, PRIMARY_DB).await;
    setup_articles(&app, "tenant_b").await;

    // Document IDs are assigned per collection, so insert two documents into
    // the primary database to get an ID that does not exist in tenant_b.
    add_article(&app, PRIMARY_DB, "Only In Primary", "A", 10).await;
    let id = add_article(&app, PRIMARY_DB, "Also Only In Primary", "A2", 11).await;
    add_article(&app, "tenant_b", "Only In Tenant B", "B", 20).await;
    assert_eq!(id, 2);

    let docs = rpc_ok(
        &app,
        "/tenant_b",
        "doc.search",
        json!({
            "collection": "articles",
            "query": {"filter": {"Field": ["score", {"Ge": 0}]}, "limit": 10}
        }),
    )
    .await;
    let docs = docs.as_array().unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["title"], "Only In Tenant B");

    // A document ID from one database does not resolve in another.
    let err = rpc_err(
        &app,
        "/tenant_b",
        "doc.get",
        json!({"collection": "articles", "_id": id}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(err["code"], "not_found");
}

#[tokio::test]
async fn test_db_and_collection_extensions() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");
    setup_articles(&app, PRIMARY_DB).await;

    rpc_ok(
        &app,
        &path,
        "db.save_extension",
        json!({"key": "owner", "value": "alice"}),
    )
    .await;
    let value = rpc_ok(&app, &path, "db.get_extension", json!({"key": "owner"})).await;
    assert_eq!(value, json!("alice"));
    let old = rpc_ok(&app, &path, "db.remove_extension", json!({"key": "owner"})).await;
    assert_eq!(old, json!("alice"));
    let value = rpc_ok(&app, &path, "db.get_extension", json!({"key": "owner"})).await;
    assert_eq!(value, Value::Null);

    rpc_ok(
        &app,
        &path,
        "collection.save_extension",
        json!({"collection": "articles", "key": "cursor", "value": 7}),
    )
    .await;
    let value = rpc_ok(
        &app,
        &path,
        "collection.get_extension",
        json!({"collection": "articles", "key": "cursor"}),
    )
    .await;
    assert_eq!(value, json!(7));
}

#[tokio::test]
async fn test_db_metadata_stats_and_read_only() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");
    setup_articles(&app, PRIMARY_DB).await;

    let meta = rpc_ok(&app, &path, "db.metadata", Value::Null).await;
    assert_eq!(meta["config"]["name"], PRIMARY_DB);
    assert_eq!(meta["collections"], json!(["articles"]));

    let stats = rpc_ok(&app, &path, "db.stats", Value::Null).await;
    assert!(stats["total_put_count"].as_u64().unwrap() > 0);

    rpc_ok(&app, &path, "db.flush", Value::Null).await;

    // Writes fail while read-only, succeed after re-enabling.
    rpc_ok(&app, &path, "db.set_read_only", json!({"read_only": true})).await;
    let err = rpc_err(
        &app,
        &path,
        "doc.add",
        json!({"collection": "articles", "doc": {"title": "x", "body": "y", "score": 0}}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(err["message"].as_str().unwrap().contains("read-only"));

    rpc_ok(&app, &path, "db.set_read_only", json!({"read_only": false})).await;
    add_article(&app, PRIMARY_DB, "x", "y", 0).await;
}

#[tokio::test]
async fn test_auth() {
    let app =
        build_router(test_state(Arc::new(InMemory::new()), Some("test-secret".to_string())).await);

    let req = json!({"method": "info"});
    let mut body = Vec::new();
    ciborium::ser::into_writer(&req, &mut body).unwrap();

    for token in [None, Some("Bearer wrong")] {
        let mut builder = Request::post(format!("/{PRIMARY_DB}"))
            .header(header::CONTENT_TYPE, "application/cbor");
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, token);
        }
        let resp = app
            .clone()
            .oneshot(builder.body(Body::from(body.clone())).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    let resp = app
        .clone()
        .oneshot(
            Request::post(format!("/{PRIMARY_DB}"))
                .header(header::CONTENT_TYPE, "application/cbor")
                .header(header::AUTHORIZATION, "Bearer test-secret")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_encoding_negotiation() {
    let app = test_app().await;

    // JSON request -> JSON response (mirrors Content-Type).
    let resp = app
        .clone()
        .oneshot(
            Request::post("/")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"method": "info"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["result"]["name"], "test");

    // JSON request + Accept: application/cbor -> CBOR response.
    let resp = app
        .clone()
        .oneshot(
            Request::post("/")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::ACCEPT, "application/cbor")
                .body(Body::from(r#"{"method": "info"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/cbor"
    );
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = ciborium::de::from_reader(&bytes[..]).unwrap();
    assert_eq!(value["result"]["name"], "test");

    // No Content-Type at all: the body is parsed as CBOR.
    let mut body = Vec::new();
    ciborium::ser::into_writer(&json!({"method": "info"}), &mut body).unwrap();
    let resp = app
        .clone()
        .oneshot(Request::post("/").body(Body::from(body)).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/cbor"
    );
}

#[tokio::test]
async fn test_rpc_errors() {
    let app = test_app().await;

    // Malformed body.
    let resp = app
        .clone()
        .oneshot(
            Request::post("/")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("not json {{{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"]["code"], "bad_request");

    // Unknown method.
    let err = rpc_err(&app, "/", "nope", Value::Null, StatusCode::BAD_REQUEST).await;
    assert_eq!(err["code"], "method_not_found");

    // Unknown database.
    let err = rpc_err(
        &app,
        "/no_such_db",
        "db.metadata",
        Value::Null,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(err["code"], "not_found");

    // Missing params.
    let err = rpc_err(
        &app,
        &format!("/{PRIMARY_DB}"),
        "doc.get",
        Value::Null,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "bad_request");

    // Unknown collection.
    let err = rpc_err(
        &app,
        &format!("/{PRIMARY_DB}"),
        "doc.count",
        json!({"collection": "nope"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(err["code"], "not_found");
}

#[tokio::test]
async fn test_vector_collection_with_hnsw_index() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");

    let meta = rpc_ok(
        &app,
        &path,
        "collection.create",
        json!({
            "config": {"name": "memories", "description": "Vector memories"},
            "schema": {
                "fields": [
                    {"name": "_id", "description": "", "type": "U64", "unique": true, "index": 0},
                    {"name": "text", "description": "", "type": "Text", "unique": false, "index": 1},
                    {"name": "embedding", "description": "", "type": "Vector", "unique": false, "index": 2}
                ]
            },
            "hnsw_indexes": [{
                "field": "embedding",
                "config": {
                    "dimension": 4,
                    "max_layers": 4,
                    "max_connections": 8,
                    "ef_construction": 50,
                    "ef_search": 20,
                    "distance_metric": "Cosine",
                    "select_neighbors_strategy": "Heuristic"
                }
            }]
        }),
    )
    .await;
    assert_eq!(meta["hnsw_indexes"].as_object().unwrap().len(), 1);

    for (text, embedding) in [
        ("alpha", json!([1.0, 0.0, 0.0, 0.0])),
        ("beta", json!([0.0, 1.0, 0.0, 0.0])),
        ("gamma", json!([0.9, 0.1, 0.0, 0.0])),
    ] {
        rpc_ok(
            &app,
            &path,
            "doc.add",
            json!({"collection": "memories", "doc": {"text": text, "embedding": embedding}}),
        )
        .await;
    }

    let docs = rpc_ok(
        &app,
        &path,
        "doc.search",
        json!({
            "collection": "memories",
            "query": {"search": {"vector": [1.0, 0.0, 0.0, 0.0]}, "limit": 2}
        }),
    )
    .await;
    let docs = docs.as_array().unwrap();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0]["text"], "alpha");
    assert_eq!(docs[1]["text"], "gamma");
}
