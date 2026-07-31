//! Integration tests for the Anda DB Server RPC API.
//!
//! The wire protocol is CBOR-first: most tests send CBOR request bodies and
//! decode CBOR responses, transcoding through `serde_json::Value` only for
//! easy assertions. JSON round-trips and encoding negotiation are covered
//! separately.

use anda_db_server::{
    ApiError, AppState, ServerOptions, build_router, state::check_startup_api_key,
};
use anda_object_store::{FaultKind, FaultOp, FaultRule, FaultStore};
use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, Bytes},
    http::{Request, StatusCode, header},
};
use futures::stream::BoxStream;
use http_body_util::BodyExt;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    memory::InMemory,
    path::Path,
    throttle::{ThrottleConfig, ThrottledStore},
};
use serde_json::{Value, json};
use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use tower::ServiceExt;

const PRIMARY_DB: &str = "test_db";

#[derive(Debug)]
struct PutGate {
    armed: AtomicBool,
    blocked: Semaphore,
    release: Semaphore,
    /// Completed `put_opts` calls, so a test can prove that an operation
    /// wrote nothing.
    puts: AtomicUsize,
}

#[derive(Clone, Debug)]
struct PutGateHandle {
    gate: Arc<PutGate>,
}

impl PutGateHandle {
    fn arm(&self) {
        assert!(
            !self.gate.armed.swap(true, Ordering::AcqRel),
            "put gate was already armed"
        );
    }

    async fn wait_until_blocked(&self) {
        self.gate
            .blocked
            .acquire()
            .await
            .expect("put gate blocked semaphore closed")
            .forget();
    }

    fn release(&self) {
        self.gate.release.add_permits(1);
    }

    fn puts(&self) -> usize {
        self.gate.puts.load(Ordering::Acquire)
    }

    /// Waits until at least `expected` puts have completed.
    async fn wait_for_puts(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.puts() < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("storage writes did not complete");
    }
}

#[derive(Debug)]
struct GatedStore {
    inner: Arc<InMemory>,
    gate: Arc<PutGate>,
}

impl GatedStore {
    fn new() -> (Self, PutGateHandle) {
        let gate = Arc::new(PutGate {
            armed: AtomicBool::new(false),
            blocked: Semaphore::new(0),
            release: Semaphore::new(0),
            puts: AtomicUsize::new(0),
        });
        (
            Self {
                inner: Arc::new(InMemory::new()),
                gate: gate.clone(),
            },
            PutGateHandle { gate },
        )
    }
}

impl fmt::Display for GatedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GatedStore")
    }
}

#[async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.gate.armed.swap(false, Ordering::AcqRel) {
            self.gate.blocked.add_permits(1);
            self.gate
                .release
                .acquire()
                .await
                .expect("put gate release semaphore closed")
                .forget();
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        self.gate.puts.fetch_add(1, Ordering::Release);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

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
    cbor2::ser::to_writer(&req, &mut body).unwrap();

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
    let value: Value = cbor2::de::from_reader(&bytes[..]).unwrap();
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

/// A document with a `Bytes` field, decoded straight from CBOR: a CBOR byte
/// string has no JSON equivalent, so [`rpc_cbor`]'s transcode cannot carry it.
#[derive(Debug, serde::Deserialize)]
struct BlobDoc {
    #[serde(with = "serde_bytes")]
    blob: Vec<u8>,
}

/// Like [`rpc_ok`] but decodes `result` into a typed value without the JSON
/// transcode, for responses carrying binary field values.
async fn rpc_typed<T: serde::de::DeserializeOwned>(
    app: &Router,
    path: &str,
    method: &str,
    params: Value,
) -> T {
    #[derive(serde::Deserialize)]
    struct Envelope<T> {
        result: T,
    }

    let req = json!({"method": method, "params": params});
    let mut body = Vec::new();
    cbor2::ser::to_writer(&req, &mut body).unwrap();

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
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let envelope: Envelope<T> = cbor2::de::from_reader(&bytes[..]).unwrap();
    envelope.result
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
    assert_eq!(err["code"], "invalid_input");

    // Invalid database names are rejected before touching storage.
    let err = rpc_err(
        &app,
        "/",
        "db.create",
        json!({"name": "Bad-Name"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "invalid_input");
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

/// `doc.query_ids` with no `limit` used to return one ID per matching
/// document — an unbounded response body from a one-line request — and
/// `doc.get_many` accepted an unbounded ID list, each ID costing an
/// object-store fetch. Both are now capped like their siblings.
#[tokio::test]
async fn test_batch_read_surfaces_are_capped() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");
    setup_articles(&app, PRIMARY_DB).await;

    for i in 0u64..5 {
        add_article(&app, PRIMARY_DB, &format!("A{i}"), "body", i).await;
    }

    // An omitted limit is the cap, not "everything".
    let ids = rpc_ok(
        &app,
        &path,
        "doc.query_ids",
        json!({"collection": "articles", "filter": {"Field": ["score", {"Ge": 0}]}}),
    )
    .await;
    assert_eq!(ids.as_array().unwrap().len(), 5);

    // An explicit limit above the cap is a client error rather than a
    // silently-clamped success, so the caller knows the bound exists.
    let err = rpc_err(
        &app,
        &path,
        "doc.query_ids",
        json!({
            "collection": "articles",
            "filter": {"Field": ["score", {"Ge": 0}]},
            "limit": 1_000_001
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "invalid_input");
    assert!(
        err["message"].as_str().unwrap().contains("at most 1000"),
        "unexpected message: {}",
        err["message"]
    );
    // `limit: 0` keeps its "no data requested" meaning.
    let ids = rpc_ok(
        &app,
        &path,
        "doc.query_ids",
        json!({
            "collection": "articles",
            "filter": {"Field": ["score", {"Ge": 0}]},
            "limit": 0
        }),
    )
    .await;
    assert!(ids.as_array().unwrap().is_empty());

    // `doc.get_many` refuses an oversized id list instead of issuing one
    // object-store fetch per id.
    let too_many: Vec<u64> = (0..1_001).collect();
    let err = rpc_err(
        &app,
        &path,
        "doc.get_many",
        json!({"collection": "articles", "_ids": too_many}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "invalid_input");
    assert!(
        err["message"]
            .as_str()
            .unwrap()
            .contains("at most 1000 ids"),
        "unexpected message: {}",
        err["message"]
    );

    // A batch at the cap still works, and duplicates are answered per entry.
    let docs = rpc_ok(
        &app,
        &path,
        "doc.get_many",
        json!({"collection": "articles", "_ids": [0u64, 0u64, 4u64]}),
    )
    .await;
    let docs = docs.as_array().unwrap();
    assert_eq!(docs.len(), 3);
    assert_eq!(docs[0], docs[1]);
}

/// `doc.add` coerces values through the engine's CBOR extraction, so a
/// `Bytes` field accepts `[1, 2, 3]`. `doc.update` used to validate the raw
/// wire value, letting a client create a document it could not then update.
#[tokio::test]
async fn test_update_accepts_every_shape_add_accepts() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");

    rpc_ok(
        &app,
        &path,
        "collection.create",
        json!({
            "config": {"name": "blobs", "description": ""},
            "schema": {
                "fields": [
                    {"name": "_id", "description": "", "type": "U64", "unique": true, "index": 0},
                    {"name": "blob", "description": "", "type": "Bytes", "unique": false, "index": 1}
                ]
            }
        }),
    )
    .await;

    let added = rpc_ok(
        &app,
        &path,
        "doc.add",
        json!({"collection": "blobs", "doc": {"blob": [1, 2, 3]}}),
    )
    .await;
    let id = added["_id"].as_u64().unwrap();

    // The same shape must be accepted by `doc.update`, and stored as bytes.
    let updated: BlobDoc = rpc_typed(
        &app,
        &path,
        "doc.update",
        json!({"collection": "blobs", "_id": id, "fields": {"blob": [4, 5, 6]}}),
    )
    .await;
    assert_eq!(updated.blob.as_slice(), &[4, 5, 6]);

    let doc: BlobDoc = rpc_typed(
        &app,
        &path,
        "doc.get",
        json!({"collection": "blobs", "_id": id}),
    )
    .await;
    assert_eq!(doc.blob.as_slice(), &[4, 5, 6]);

    // A value the field genuinely cannot hold is still a client error.
    let err = rpc_err(
        &app,
        &path,
        "doc.update",
        json!({"collection": "blobs", "_id": id, "fields": {"blob": [1, 999]}}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "invalid_input");
}

/// A one-entry `Map` declared by a nested struct is not a wildcard map, so it
/// cannot key a B-Tree index. The engine rejects it, and the API must reject
/// it at definition time instead of reporting success followed by an
/// undiagnosable failure at the first insert.
#[tokio::test]
async fn test_btree_index_on_a_nested_struct_field_is_rejected_at_definition() {
    let app = test_app().await;
    let path = format!("/{PRIMARY_DB}");

    let nested_schema = json!({
        "fields": [
            {"name": "_id", "description": "", "type": "U64", "unique": true, "index": 0},
            {
                "name": "meta",
                "description": "",
                "type": {"Map": {"owner": "Text"}},
                "unique": false,
                "index": 1
            }
        ]
    });

    for method in ["collection.create", "collection.ensure"] {
        let err = rpc_err(
            &app,
            &path,
            method,
            json!({
                "config": {"name": "nested", "description": ""},
                "schema": nested_schema,
                "btree_indexes": [["meta"]]
            }),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(err["code"], "invalid_input", "method: {method}");
        assert!(
            err["message"]
                .as_str()
                .unwrap()
                .contains("cannot be used by a B-Tree index"),
            "method {method}: unexpected message {}",
            err["message"]
        );
    }

    // The primary key is answered from the collection's id bitmap, so the
    // engine refuses a `_id` B-Tree index; say so as a client error instead
    // of letting it surface as an engine failure at creation time.
    let err = rpc_err(
        &app,
        &path,
        "collection.create",
        json!({
            "config": {"name": "id_indexed", "description": ""},
            "schema": {
                "fields": [
                    {"name": "_id", "description": "", "type": "U64", "unique": true, "index": 0}
                ]
            },
            "btree_indexes": [["_id"]]
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "invalid_input");
    assert!(
        err["message"].as_str().unwrap().contains("_id"),
        "unexpected message: {}",
        err["message"]
    );

    // A wildcard map keys its entries and stays indexable.
    let wildcard_schema = json!({
        "fields": [
            {"name": "_id", "description": "", "type": "U64", "unique": true, "index": 0},
            {
                "name": "tags",
                "description": "",
                "type": {"Map": {"*": "U64"}},
                "unique": false,
                "index": 1
            }
        ]
    });
    let meta = rpc_ok(
        &app,
        &path,
        "collection.create",
        json!({
            "config": {"name": "wildcard", "description": ""},
            "schema": wildcard_schema,
            "btree_indexes": [["tags"]]
        }),
    )
    .await;
    assert_eq!(meta["btree_indexes"].as_object().unwrap().len(), 1);
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
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(err["code"], "conflict");
    assert_eq!(err["message"], "collection \"articles\" is read-only");

    rpc_ok(&app, &path, "db.set_read_only", json!({"read_only": false})).await;
    add_article(&app, PRIMARY_DB, "x", "y", 0).await;
}

#[tokio::test]
async fn test_auth() {
    let app =
        build_router(test_state(Arc::new(InMemory::new()), Some("test-secret".to_string())).await);

    let req = json!({"method": "info"});
    let mut body = Vec::new();
    cbor2::ser::to_writer(&req, &mut body).unwrap();

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
async fn test_empty_api_key_is_rejected_at_startup() {
    let err = match AppState::connect(Arc::new(InMemory::new()), test_options(Some(String::new())))
        .await
    {
        Ok(_) => panic!("empty API key should fail startup"),
        Err(err) => err,
    };
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    assert!(err.message.contains("API key must not be empty"));
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
    let value: Value = cbor2::de::from_reader(&bytes[..]).unwrap();
    assert_eq!(value["result"]["name"], "test");

    // An absent or unrecognized Content-Type is refused. Parsing such a body
    // as CBOR made every RPC endpoint reachable as a browser "simple request"
    // (`text/plain`, no preflight), i.e. a CSRF surface in the supported
    // loopback / `--insecure-no-api-key` modes. Response negotiation is
    // unaffected: the 415 itself still answers in the default encoding.
    let mut body = Vec::new();
    cbor2::ser::to_writer(&json!({"method": "info"}), &mut body).unwrap();
    for content_type in [None, Some("text/plain"), Some("application/octet-stream")] {
        let mut builder = Request::post("/");
        if let Some(content_type) = content_type {
            builder = builder.header(header::CONTENT_TYPE, content_type);
        }
        let resp = app
            .clone()
            .oneshot(builder.body(Body::from(body.clone())).unwrap())
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "content type: {content_type:?}"
        );
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/cbor"
        );
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let value: Value = cbor2::de::from_reader(&bytes[..]).unwrap();
        assert_eq!(value["error"]["code"], "unsupported_media_type");
    }
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
    assert_eq!(err["code"], "invalid_input");

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

    setup_articles(&app, PRIMARY_DB).await;

    // Query misuse is classified at the HTTP boundary without exposing an
    // engine/index error string.
    let err = rpc_err(
        &app,
        &format!("/{PRIMARY_DB}"),
        "doc.query_ids",
        json!({
            "collection": "articles",
            "filter": {"Field": ["missing_index", {"Eq": 1}]}
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "invalid_query");
    assert_eq!(
        err["message"],
        "query requires B-Tree index \"missing_index\", but it does not exist"
    );

    let err = rpc_err(
        &app,
        &format!("/{PRIMARY_DB}"),
        "doc.search",
        json!({
            "collection": "articles",
            "query": {"search": {"vector": [1.0, 2.0]}}
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err["code"], "invalid_query");
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

#[tokio::test]
async fn test_payload_too_large_uses_rpc_error_envelope() {
    let mut options = test_options(None);
    options.max_body_size = 1024;
    let state = AppState::connect(Arc::new(InMemory::new()), options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state);

    let body = format!(
        r#"{{"method": "info", "params": {{"junk": "{}"}}}}"#,
        "x".repeat(4096)
    );
    let resp = app
        .oneshot(
            Request::post("/")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"]["code"], "payload_too_large");
}

#[tokio::test]
async fn test_timeout_returns_408_but_mutation_still_completes() {
    // A store whose throttle can be turned on after startup, so connecting
    // the primary database stays fast.
    let throttled = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let store: Arc<dyn ObjectStore> = throttled.clone();
    let mut options = test_options(None);
    options.request_timeout = Duration::from_millis(100);
    let state = AppState::connect(store, options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state.clone());

    // Every storage put now takes longer than the request timeout, so
    // `db.create` (several puts) cannot finish before the 408.
    throttled.config_mut(|cfg| cfg.wait_put_per_call = Duration::from_millis(300));
    let (status, resp) = rpc_cbor(&app, "/", "db.create", json!({"name": "slowdb"})).await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "timeout");

    // The dispatched operation lives on its own task: it must keep running
    // after the 408 and complete, instead of being dropped halfway (which
    // would leak the database's flush task or lose registry state).
    throttled.config_mut(|cfg| cfg.wait_put_per_call = Duration::ZERO);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let names = rpc_ok(&app, "/", "db.list", Value::Null).await;
        let names: Vec<String> = serde_json::from_value(names).unwrap();
        if names.contains(&"slowdb".to_string()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed-out db.create never completed; open databases: {names:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The completed create left a fully consistent state: the database is
    // usable and can be closed cleanly (flush task present and cancellable).
    rpc_ok(&app, "/slowdb", "db.metadata", Value::Null).await;
    rpc_ok(&app, "/", "db.close", json!({"name": "slowdb"})).await;
    state.shutdown().await;
}

#[tokio::test]
async fn test_slow_create_cannot_register_after_shutdown_closes_admission() {
    let (store, gate) = GatedStore::new();
    let state = test_state(Arc::new(store), None).await;
    let app = build_router(state.clone());

    gate.arm();
    let create = tokio::spawn({
        let app = app.clone();
        async move { rpc_cbor(&app, "/", "db.create", json!({"name": "slowdb"})).await }
    });
    gate.wait_until_blocked().await;

    // The create has passed RPC admission but is still inside object-store
    // I/O. Closing admission before releasing it must prevent its final
    // registry commit.
    state.begin_shutdown();
    gate.release();

    let (status, resp) = tokio::time::timeout(Duration::from_secs(5), create)
        .await
        .expect("slow create did not exit")
        .expect("slow create task panicked");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "unavailable");
    assert_eq!(state.db_names().await, vec![PRIMARY_DB.to_string()]);

    state.shutdown().await;
}

#[tokio::test]
async fn test_shutdown_drains_slow_mutation_before_database_close() {
    let (store, gate) = GatedStore::new();
    let mut options = test_options(None);
    options.request_timeout = Duration::from_millis(100);
    let state = AppState::connect(Arc::new(store), options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state.clone());
    setup_articles(&app, PRIMARY_DB).await;

    let db = state.get_db(PRIMARY_DB).await.unwrap();
    let collection = db
        .open_collection("articles".to_string(), async |_| Ok(()))
        .await
        .unwrap();
    assert!(!collection.metadata().stats.read_only);

    gate.arm();
    let mutation = tokio::spawn({
        let app = app.clone();
        async move {
            rpc_cbor(
                &app,
                &format!("/{PRIMARY_DB}"),
                "doc.add",
                json!({"collection": "articles", "doc": {"title": "slow", "body": "write"}}),
            )
            .await
        }
    });
    gate.wait_until_blocked().await;

    // Let the HTTP response time out and drop its JoinHandle. The mutation is
    // now genuinely detached from the request; shutdown must still find it
    // through the state-owned tracker.
    let (status, resp) = mutation.await.expect("slow mutation task panicked");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "resp: {resp:?}");

    let shutdown = tokio::spawn({
        let state = state.clone();
        async move { state.shutdown().await }
    });
    while !state.is_shutting_down() {
        tokio::task::yield_now().await;
    }

    // `AndaDB::close` sets every collection read-only before flushing. The
    // collection remaining writable here proves shutdown is waiting in the
    // mutation tracker instead of closing concurrently with the blocked add.
    assert!(!collection.metadata().stats.read_only);
    assert!(!shutdown.is_finished());

    gate.release();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("shutdown did not finish after mutation drained")
        .expect("shutdown task panicked");
    assert!(collection.metadata().stats.read_only);
}

#[tokio::test]
async fn test_shutdown_deadline_uses_crash_style_abort_without_database_close() {
    let (store, gate) = GatedStore::new();
    let mut options = test_options(None);
    options.shutdown_timeout = Duration::from_millis(20);
    let state = AppState::connect(Arc::new(store), options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state.clone());
    setup_articles(&app, PRIMARY_DB).await;

    let db = state.get_db(PRIMARY_DB).await.unwrap();
    let collection = db
        .open_collection("articles".to_string(), async |_| Ok(()))
        .await
        .unwrap();

    gate.arm();
    let mutation = tokio::spawn({
        let app = app.clone();
        async move {
            rpc_cbor(
                &app,
                &format!("/{PRIMARY_DB}"),
                "doc.add",
                json!({"collection": "articles", "doc": {"title": "stuck", "body": "write"}}),
            )
            .await
        }
    });
    gate.wait_until_blocked().await;

    tokio::time::timeout(Duration::from_secs(5), state.shutdown())
        .await
        .expect("hard-deadline shutdown did not finish");
    let (status, resp) = tokio::time::timeout(Duration::from_secs(5), mutation)
        .await
        .expect("aborted mutation response did not finish")
        .expect("mutation request task panicked");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "resp: {resp:?}");

    // The forced path joins the aborted mutation and auto-flush owners, but
    // deliberately does not call AndaDB::close: publishing in-memory state
    // after arbitrary cancellation would be less safe than crash recovery.
    assert!(!collection.metadata().stats.read_only);
    assert!(state.db_names().await.is_empty());
    gate.release();
}

/// The registry is bounded: each entry costs a permanent auto-flush task and
/// a name in the primary database's registry extension, so an authorized
/// caller must not be able to grow it without limit.
#[tokio::test]
async fn test_database_registry_is_capped() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut options = test_options(None);
    options.max_databases = 2;
    let state = AppState::connect(store.clone(), options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state.clone());

    rpc_ok(&app, "/", "db.create", json!({"name": "tenant_a"})).await;
    rpc_ok(&app, "/", "db.create", json!({"name": "tenant_b"})).await;

    let err = rpc_err(
        &app,
        "/",
        "db.create",
        json!({"name": "tenant_c"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(err["code"], "limit_exceeded");
    assert!(
        err["message"].as_str().unwrap().contains("maximum of 2"),
        "unexpected message: {}",
        err["message"]
    );

    // Closing one frees a slot.
    rpc_ok(&app, "/", "db.close", json!({"name": "tenant_a"})).await;
    rpc_ok(&app, "/", "db.create", json!({"name": "tenant_c"})).await;
    state.shutdown().await;

    // Lowering the cap below the number of registered databases must not
    // break a restart: the bound applies to registration, not to reopening.
    let mut options = test_options(None);
    options.max_databases = 1;
    let state = AppState::connect(store, options)
        .await
        .expect("a restart must not be blocked by the registry cap");
    let app = build_router(state.clone());
    let names = rpc_ok(&app, "/", "db.list", Value::Null).await;
    let names: Vec<String> = serde_json::from_value(names).unwrap();
    assert!(names.contains(&"tenant_b".to_string()), "names: {names:?}");
    assert!(names.contains(&"tenant_c".to_string()), "names: {names:?}");
    state.shutdown().await;
}

/// A *read* RPC must never be able to poison a collection.
///
/// Reads run on the cancellable path, but opening a collection ends in
/// `Collection::flush`, whose cancel guard poisons the handle when its future
/// is dropped. The first `doc.get` on a cold collection that hits the request
/// timeout therefore used to poison a healthy collection, after which every
/// in-flight and subsequent operation failed with a generic 500.
#[tokio::test]
async fn test_a_timed_out_read_does_not_poison_a_cold_collection() {
    let (gated, gate) = GatedStore::new();
    let store: Arc<dyn ObjectStore> = Arc::new(gated);
    let path = format!("/{PRIMARY_DB}");

    // Leave the collection with unflushed state: shutdown hits its drain
    // deadline and takes the crash-style path, so nothing is flushed and the
    // mutation intents stay on disk. The next open must therefore replay
    // them and write a checkpoint — the flush whose cancel guard poisons.
    let mut options = test_options(None);
    options.request_timeout = Duration::from_millis(100);
    options.shutdown_timeout = Duration::from_millis(20);
    let state = AppState::connect(store.clone(), options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state.clone());
    setup_articles(&app, PRIMARY_DB).await;
    let id = add_article(&app, PRIMARY_DB, "cold", "body", 1).await;

    gate.arm();
    let stuck = tokio::spawn({
        let app = app.clone();
        let path = path.clone();
        async move {
            rpc_cbor(
                &app,
                &path,
                "doc.add",
                json!({"collection": "articles", "doc": {"title": "stuck", "body": "b"}}),
            )
            .await
        }
    });
    gate.wait_until_blocked().await;
    tokio::time::timeout(Duration::from_secs(5), state.shutdown())
        .await
        .expect("crash-style shutdown did not finish");
    let _ = tokio::time::timeout(Duration::from_secs(5), stuck).await;
    // The blocked put was aborted with the mutation, so the gate is idle
    // again and must not be released: a stray permit would let the next
    // armed put through.

    // A fresh server over the same storage: the collection is not in memory
    // and its first open has real work to write.
    let mut options = test_options(None);
    options.request_timeout = Duration::from_millis(100);
    let state = AppState::connect(store, options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state.clone());

    gate.arm();
    let read = tokio::spawn({
        let app = app.clone();
        let path = path.clone();
        async move {
            rpc_cbor(
                &app,
                &path,
                "doc.get",
                json!({"collection": "articles", "_id": id}),
            )
            .await
        }
    });
    gate.wait_until_blocked().await;

    // The client gives up while the open is still writing.
    let blocked_puts = gate.puts();
    let (status, resp) = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("read did not time out")
        .expect("read task panicked");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "resp: {resp:?}");

    // Let the (detached) open finish the write the client stopped waiting for.
    gate.release();
    gate.wait_for_puts(blocked_puts + 1).await;
    let settled = gate.puts();

    // The open ran to completion, so the collection is registered, active and
    // clean: the next read is served from the open handle and writes nothing.
    // When the cancelled request was allowed to take the open future down with
    // it, the handle was poisoned instead and the next read had to discard it,
    // reload from storage, and flush again.
    let doc = rpc_ok(
        &app,
        &path,
        "doc.get",
        json!({"collection": "articles", "_id": id}),
    )
    .await;
    assert_eq!(doc["title"], "cold");
    assert_eq!(
        gate.puts(),
        settled,
        "a cancelled read left the collection needing a fresh open"
    );

    // And it must still be writable and close cleanly.
    add_article(&app, PRIMARY_DB, "after", "body", 2).await;
    let collection = state
        .get_db(PRIMARY_DB)
        .await
        .unwrap()
        .open_collection("articles".to_string(), async |_| Ok(()))
        .await
        .expect("the collection must still open");
    state.shutdown().await;
    assert!(collection.metadata().stats.read_only);
}

/// A handle poisoned by a cancelled mutation is not read-only, so writes to
/// it used to surface as an opaque `internal` 500. The condition is
/// transient — reopening recovers it — and must be reported as such.
#[tokio::test]
async fn test_a_poisoned_collection_answers_with_a_retryable_status() {
    let (gated, gate) = GatedStore::new();
    let store: Arc<dyn ObjectStore> = Arc::new(gated);
    let path = format!("/{PRIMARY_DB}");

    let mut options = test_options(None);
    options.request_timeout = Duration::from_millis(100);
    options.shutdown_timeout = Duration::from_millis(20);
    let state = AppState::connect(store, options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state.clone());
    setup_articles(&app, PRIMARY_DB).await;

    // Hold the handle the server uses, so the poisoning is observable instead
    // of being repaired by the next open.
    let collection = state
        .get_db(PRIMARY_DB)
        .await
        .unwrap()
        .open_collection("articles".to_string(), async |_| Ok(()))
        .await
        .unwrap();

    // A mutation cancelled mid-write poisons the handle. The crash-style
    // shutdown path aborts it exactly that way.
    gate.arm();
    let stuck = tokio::spawn({
        let app = app.clone();
        let path = path.clone();
        async move {
            rpc_cbor(
                &app,
                &path,
                "doc.add",
                json!({"collection": "articles", "doc": {"title": "stuck", "body": "b"}}),
            )
            .await
        }
    });
    gate.wait_until_blocked().await;
    tokio::time::timeout(Duration::from_secs(5), state.shutdown())
        .await
        .expect("crash-style shutdown did not finish");
    let _ = tokio::time::timeout(Duration::from_secs(5), stuck).await;
    assert!(
        collection.is_poisoned(),
        "the aborted mutation must poison the handle"
    );

    // The pre-flight check every document mutation runs must refuse the
    // handle, with the reason and a retry hint rather than "internal".
    let err = ApiError::from_collection_state(collection.state())
        .expect("a poisoned handle must not be treated as writable");
    assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.code, "collection_unavailable");
    assert!(
        err.message.contains("cancelled"),
        "message: {}",
        err.message
    );
    assert!(err.message.contains("retry"), "message: {}", err.message);

    // And an operation that reaches the engine anyway is classified from the
    // engine's own error, not flattened into an opaque 500.
    let engine_error = collection
        .flush(0)
        .await
        .expect_err("a poisoned handle must reject operations");
    let err = ApiError::from(engine_error);
    assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(err.code, "collection_unavailable");
    assert_ne!(err.message, "internal server error");
}

#[tokio::test]
async fn test_shutdown_rejects_new_requests_with_503() {
    let state = test_state(Arc::new(InMemory::new()), None).await;
    let app = build_router(state.clone());
    state.shutdown().await;

    // Root and database-scoped RPC endpoints refuse new work while (or
    // after) shutting down, so late requests cannot race the database close.
    let (status, resp) = rpc_cbor(&app, "/", "info", Value::Null).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "unavailable");

    let (status, resp) =
        rpc_cbor(&app, &format!("/{PRIMARY_DB}"), "db.metadata", Value::Null).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "unavailable");

    // The unauthenticated health endpoint keeps answering so load balancers
    // can still observe the instance.
    let resp = app
        .clone()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_route_timeout_covers_slow_body_read() {
    let mut options = test_options(None);
    options.request_timeout = Duration::from_millis(100);
    let state = AppState::connect(Arc::new(InMemory::new()), options)
        .await
        .expect("failed to connect AppState");
    let app = build_router(state);

    // A body that sends a partial chunk and then stalls forever; the `Bytes`
    // extractor never completes, so only the route-level timeout can end the
    // request.
    let (mut tx, channel_body) = http_body_util::channel::Channel::<Bytes>::new(4);
    tx.send_data(Bytes::from_static(b"{\"method\":"))
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::post("/")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::new(channel_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"]["code"], "timeout");
    // Keep the sender alive until the response arrived, so the stall (not a
    // closed body) is what the server observed.
    drop(tx);
}

#[test]
fn test_startup_api_key_policy() {
    let loopback: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let loopback_v6: SocketAddr = "[::1]:8080".parse().unwrap();
    let public: SocketAddr = "0.0.0.0:8080".parse().unwrap();

    // A configured key allows any listen address.
    assert!(check_startup_api_key(Some("secret"), &public, false).is_ok());
    // An explicitly empty (or blank) key is always rejected.
    assert!(check_startup_api_key(Some(""), &loopback, false).is_err());
    assert!(check_startup_api_key(Some("  "), &public, true).is_err());
    // No key is fine on loopback only.
    assert!(check_startup_api_key(None, &loopback, false).is_ok());
    assert!(check_startup_api_key(None, &loopback_v6, false).is_ok());
    // No key on a non-loopback address requires the explicit escape hatch.
    assert!(check_startup_api_key(None, &public, false).is_err());
    assert!(check_startup_api_key(None, &public, true).is_ok());
}

#[tokio::test]
async fn test_corrupt_registry_refuses_start() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_state(store.clone(), None).await;
    let app = build_router(state.clone());

    // Corrupt the registry extension in the primary database, then restart.
    rpc_ok(
        &app,
        &format!("/{PRIMARY_DB}"),
        "db.save_extension",
        json!({"key": "server:databases", "value": "not a set"}),
    )
    .await;
    state.shutdown().await;

    let err = match AppState::connect(store, test_options(None)).await {
        Ok(_) => panic!("startup must fail instead of overwriting a corrupt registry"),
        Err(err) => err,
    };
    assert!(
        err.message.contains("registry"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn test_failed_reopen_keeps_database_registered() {
    let (fault_store, handle) = FaultStore::wrap(InMemory::new());
    let store: Arc<dyn ObjectStore> = Arc::new(fault_store);

    // Register an extra database, then stop the server.
    let state = test_state(store.clone(), None).await;
    let app = build_router(state.clone());
    rpc_ok(&app, "/", "db.create", json!({"name": "auxdb"})).await;
    state.shutdown().await;

    // Every read of auxdb fails: the reopen on startup fails, but the
    // registry entry must survive later registry rewrites.
    handle.push_rule(FaultRule {
        op: FaultOp::Get,
        path_contains: Some("auxdb".to_string()),
        skip: 0,
        times: u64::MAX,
        kind: FaultKind::Error,
    });
    let state = test_state(store.clone(), None).await;
    let app = build_router(state.clone());
    let names = rpc_ok(&app, "/", "db.list", Value::Null).await;
    let names: Vec<String> = serde_json::from_value(names).unwrap();
    assert!(!names.contains(&"auxdb".to_string()));

    // Rewrites the registry; auxdb must not be dropped from it.
    rpc_ok(&app, "/", "db.create", json!({"name": "otherdb"})).await;
    state.shutdown().await;

    // Storage recovers: auxdb is reopened automatically on the next start.
    handle.reset();
    let state = test_state(store, None).await;
    let app = build_router(state.clone());
    let names = rpc_ok(&app, "/", "db.list", Value::Null).await;
    let names: Vec<String> = serde_json::from_value(names).unwrap();
    assert!(names.contains(&"auxdb".to_string()), "names: {names:?}");
    assert!(names.contains(&"otherdb".to_string()), "names: {names:?}");
    state.shutdown().await;
}

// ─── authorization ───────────────────────────────────────────────────────
//
// Two key tiers: the process-global admin key (`ServerOptions::api_key`)
// reaches everything, a per-database key reaches only `POST /{its_db}`.

/// Admin key used by the authorization tests.
const ADMIN_KEY: &str = "admin-secret";

/// Sends a CBOR RPC request carrying an optional bearer token.
///
/// Deliberately separate from [`rpc_cbor`] instead of generalizing it, so the
/// pre-existing tests keep exercising the header-free request path verbatim.
async fn rpc_auth(
    app: &Router,
    token: Option<&str>,
    path: &str,
    method: &str,
    params: Value,
) -> (StatusCode, Value) {
    let req = json!({"method": method, "params": params});
    let mut body = Vec::new();
    cbor2::ser::to_writer(&req, &mut body).unwrap();

    let mut builder = Request::post(path).header(header::CONTENT_TYPE, "application/cbor");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let resp = app
        .clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();

    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = cbor2::de::from_reader(&bytes[..]).unwrap();
    (status, value)
}

/// Like [`rpc_auth`] but asserts HTTP 200 and unwraps `result`.
async fn rpc_auth_ok(
    app: &Router,
    token: Option<&str>,
    path: &str,
    method: &str,
    params: Value,
) -> Value {
    let (status, resp) = rpc_auth(app, token, path, method, params).await;
    assert_eq!(status, StatusCode::OK, "unexpected response: {resp:?}");
    resp.get("result")
        .unwrap_or_else(|| panic!("missing result: {resp:?}"))
        .clone()
}

/// Asserts that a request is rejected with the uniform authorization failure.
async fn assert_unauthorized(app: &Router, token: Option<&str>, path: &str, method: &str) {
    let (status, resp) = rpc_auth(app, token, path, method, Value::Null).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "{path} {method} was not rejected: {resp:?}"
    );
    assert_eq!(resp["error"]["code"], "unauthorized");
}

/// A server with an admin key and two tenant databases, each with its own key.
async fn tenants_app(store: Arc<dyn ObjectStore>) -> Router {
    let app = build_router(test_state(store, Some(ADMIN_KEY.to_string())).await);
    for (name, key) in [("tenant_a", "key-a"), ("tenant_b", "key-b")] {
        rpc_auth_ok(
            &app,
            Some(ADMIN_KEY),
            "/",
            "db.create",
            json!({"name": name, "api_key": key}),
        )
        .await;
    }
    app
}

/// Companion to `test_database_isolation`, which covers *data* separation:
/// this one covers *authorization* separation.
#[tokio::test]
async fn test_database_authorization_isolation() {
    let app = tenants_app(Arc::new(InMemory::new())).await;

    // Each key opens its own database ...
    rpc_auth_ok(&app, Some("key-a"), "/tenant_a", "db.metadata", Value::Null).await;
    rpc_auth_ok(&app, Some("key-b"), "/tenant_b", "db.metadata", Value::Null).await;

    // ... and nothing else: not the sibling tenant, not the primary database
    // (which has no key of its own and therefore falls back to the admin key),
    // and not for a write either.
    assert_unauthorized(&app, Some("key-a"), "/tenant_b", "db.metadata").await;
    assert_unauthorized(&app, Some("key-a"), "/tenant_b", "doc.add").await;
    assert_unauthorized(&app, Some("key-a"), "/tenant_b", "collection.delete").await;
    assert_unauthorized(
        &app,
        Some("key-a"),
        &format!("/{PRIMARY_DB}"),
        "db.metadata",
    )
    .await;

    // A per-database key never reaches the root scope, in any of its methods.
    for method in [
        "info",
        "db.list",
        "db.create",
        "db.open",
        "db.connect",
        "db.close",
        "db.set_api_key",
        "db.remove_api_key",
    ] {
        assert_unauthorized(&app, Some("key-a"), "/", method).await;
    }

    // Missing and malformed credentials behave like a wrong key.
    assert_unauthorized(&app, None, "/tenant_a", "db.metadata").await;
    assert_unauthorized(&app, Some(""), "/tenant_a", "db.metadata").await;
    assert_unauthorized(&app, Some("key-a "), "/tenant_a", "db.metadata").await;

    // The admin key reaches every database and the root scope.
    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/tenant_a",
        "db.metadata",
        Value::Null,
    )
    .await;
    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/tenant_b",
        "db.metadata",
        Value::Null,
    )
    .await;
    let names = rpc_auth_ok(&app, Some(ADMIN_KEY), "/", "db.list", Value::Null).await;
    assert_eq!(names, json!(["tenant_a", "tenant_b", PRIMARY_DB]));
}

#[tokio::test]
async fn test_unauthorized_response_hides_database_existence() {
    let app = tenants_app(Arc::new(InMemory::new())).await;

    // Existing-but-forbidden, existing-without-a-key, and non-existent
    // databases must be indistinguishable to an unauthorized caller.
    let forbidden = rpc_auth(&app, Some("key-a"), "/tenant_b", "db.metadata", Value::Null).await;
    let unbound = rpc_auth(
        &app,
        Some("key-a"),
        &format!("/{PRIMARY_DB}"),
        "db.metadata",
        Value::Null,
    )
    .await;
    let missing = rpc_auth(
        &app,
        Some("key-a"),
        "/no_such_db",
        "db.metadata",
        Value::Null,
    )
    .await;
    assert_eq!(forbidden.0, StatusCode::UNAUTHORIZED);
    assert_eq!(forbidden, unbound);
    assert_eq!(forbidden, missing);

    // Only an admin — who is entitled to know — sees the difference.
    let (status, resp) = rpc_auth(
        &app,
        Some(ADMIN_KEY),
        "/no_such_db",
        "db.metadata",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "not_found");
}

#[tokio::test]
async fn test_per_database_key_cannot_enumerate_the_instance() {
    let app = tenants_app(Arc::new(InMemory::new())).await;

    // A tenant sees only itself, and not even the primary database's name.
    let info = rpc_auth_ok(&app, Some("key-a"), "/tenant_a", "info", Value::Null).await;
    assert_eq!(info["databases"], json!(["tenant_a"]));
    assert_eq!(info["primary_db"], Value::Null);

    // An admin keeps the pre-existing view of the whole instance.
    let info = rpc_auth_ok(&app, Some(ADMIN_KEY), "/tenant_a", "info", Value::Null).await;
    assert_eq!(info["primary_db"], PRIMARY_DB);
    assert_eq!(
        info["databases"],
        json!(["tenant_a", "tenant_b", PRIMARY_DB])
    );
}

#[tokio::test]
async fn test_api_key_rotation_and_revocation() {
    let app = tenants_app(Arc::new(InMemory::new())).await;

    // Rotating to a caller-supplied key: the new key works, the old one stops
    // working immediately, and the supplied secret is not echoed back.
    let rotated = rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.set_api_key",
        json!({"name": "tenant_a", "api_key": "key-a2"}),
    )
    .await;
    assert_eq!(rotated["name"], "tenant_a");
    assert_eq!(rotated["api_key"], Value::Null);
    rpc_auth_ok(
        &app,
        Some("key-a2"),
        "/tenant_a",
        "db.metadata",
        Value::Null,
    )
    .await;
    assert_unauthorized(&app, Some("key-a"), "/tenant_a", "db.metadata").await;

    // Rotating without a key: the server generates one and returns it once.
    let rotated = rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.set_api_key",
        json!({"name": "tenant_a"}),
    )
    .await;
    let generated = rotated["api_key"]
        .as_str()
        .expect("generated key")
        .to_string();
    assert_eq!(generated.len(), 64);
    rpc_auth_ok(
        &app,
        Some(&generated),
        "/tenant_a",
        "db.metadata",
        Value::Null,
    )
    .await;
    assert_unauthorized(&app, Some("key-a2"), "/tenant_a", "db.metadata").await;
    // The key is not recoverable afterwards: nothing in the API returns it.
    let info = rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/tenant_a",
        "db.metadata",
        Value::Null,
    )
    .await;
    assert!(
        !serde_json::to_string(&info).unwrap().contains(&generated),
        "database metadata leaked the API key"
    );

    // Revoking returns the database to the admin-key fallback.
    let removed = rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.remove_api_key",
        json!({"name": "tenant_a"}),
    )
    .await;
    assert_eq!(removed, json!(true));
    assert_unauthorized(&app, Some(&generated), "/tenant_a", "db.metadata").await;
    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/tenant_a",
        "db.metadata",
        Value::Null,
    )
    .await;
    let removed = rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.remove_api_key",
        json!({"name": "tenant_a"}),
    )
    .await;
    assert_eq!(removed, json!(false));
}

#[tokio::test]
async fn test_api_key_provisioning_guards() {
    let app = tenants_app(Arc::new(InMemory::new())).await;

    // The primary database holds the registry and the key hashes themselves,
    // so it must never be delegated to a per-database key.
    let (status, resp) = rpc_auth(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.set_api_key",
        json!({"name": PRIMARY_DB, "api_key": "key-p"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "conflict");

    // An empty key would look enabled while accepting a trivial token.
    let (status, resp) = rpc_auth(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.set_api_key",
        json!({"name": "tenant_a", "api_key": "  "}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "invalid_input");

    // Unknown databases are reported as such — only admins get here.
    let (status, resp) = rpc_auth(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.set_api_key",
        json!({"name": "no_such_db", "api_key": "key-x"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "not_found");
}

#[tokio::test]
async fn test_api_keys_survive_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = tenants_app(store.clone()).await;
    rpc_auth_ok(&app, Some("key-a"), "/tenant_a", "db.metadata", Value::Null).await;
    drop(app);

    // Only the hash is persisted, yet the binding is still enforced after a
    // restart — including for a database that was closed and reopened.
    let state = test_state(store.clone(), Some(ADMIN_KEY.to_string())).await;
    let app = build_router(state.clone());
    rpc_auth_ok(&app, Some("key-a"), "/tenant_a", "db.metadata", Value::Null).await;
    assert_unauthorized(&app, Some("key-b"), "/tenant_a", "db.metadata").await;

    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.close",
        json!({"name": "tenant_a"}),
    )
    .await;
    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.open",
        json!({"name": "tenant_a"}),
    )
    .await;
    rpc_auth_ok(&app, Some("key-a"), "/tenant_a", "db.metadata", Value::Null).await;
    assert_unauthorized(&app, Some("key-b"), "/tenant_a", "db.metadata").await;
    state.shutdown().await;

    // Restarting without the admin key would silently downgrade every bound
    // database to "no key at all", so the server refuses to start instead.
    let err = match AppState::connect(store, test_options(None)).await {
        Ok(_) => panic!("startup must fail instead of dropping per-database keys"),
        Err(err) => err,
    };
    assert!(
        err.message.contains("admin API key"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn test_legacy_single_key_deployment_is_unchanged() {
    // Exactly how the server was used before per-database keys existed: one
    // key, no provisioning. It must still open every database.
    let app =
        build_router(test_state(Arc::new(InMemory::new()), Some(ADMIN_KEY.to_string())).await);

    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/",
        "db.create",
        json!({"name": "tenant_a"}),
    )
    .await;
    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        "/tenant_a",
        "db.metadata",
        Value::Null,
    )
    .await;
    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        &format!("/{PRIMARY_DB}"),
        "db.metadata",
        Value::Null,
    )
    .await;

    // The single key is the admin key, so database-scoped `info` still
    // enumerates the instance as it always did.
    let info = rpc_auth_ok(&app, Some(ADMIN_KEY), "/tenant_a", "info", Value::Null).await;
    assert_eq!(info["primary_db"], PRIMARY_DB);
    assert_eq!(info["databases"], json!(["tenant_a", PRIMARY_DB]));

    assert_unauthorized(&app, Some("wrong"), "/tenant_a", "db.metadata").await;
    assert_unauthorized(&app, None, "/tenant_a", "db.metadata").await;
}

#[tokio::test]
async fn test_keyless_mode_stays_open_and_refuses_per_database_keys() {
    let app = test_app().await;

    // No admin key: every scope is open, with or without a bearer token.
    rpc_ok(&app, "/", "db.list", Value::Null).await;
    rpc_auth_ok(
        &app,
        None,
        &format!("/{PRIMARY_DB}"),
        "db.metadata",
        Value::Null,
    )
    .await;
    rpc_auth_ok(
        &app,
        Some("irrelevant"),
        &format!("/{PRIMARY_DB}"),
        "db.metadata",
        Value::Null,
    )
    .await;

    // A per-database key here would be enforced against callers who can
    // simply rotate it away through the open root scope, so it is refused —
    // before the database is created.
    let (status, resp) = rpc_auth(
        &app,
        None,
        "/",
        "db.create",
        json!({"name": "tenant_a", "api_key": "key-a"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "resp: {resp:?}");
    assert_eq!(resp["error"]["code"], "conflict");
    let names = rpc_ok(&app, "/", "db.list", Value::Null).await;
    assert_eq!(names, json!([PRIMARY_DB]));
}

#[tokio::test]
async fn test_corrupt_api_key_map_refuses_start() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_state(store.clone(), Some(ADMIN_KEY.to_string())).await;
    let app = build_router(state.clone());

    // Starting with an unreadable key map and then persisting would drop
    // every binding, downgrading bound databases to the admin-key fallback.
    rpc_auth_ok(
        &app,
        Some(ADMIN_KEY),
        &format!("/{PRIMARY_DB}"),
        "db.save_extension",
        json!({"key": "server:api_keys", "value": "not a map"}),
    )
    .await;
    state.shutdown().await;

    let err = match AppState::connect(store, test_options(Some(ADMIN_KEY.to_string()))).await {
        Ok(_) => panic!("startup must fail instead of overwriting a corrupt key map"),
        Err(err) => err,
    };
    assert!(
        err.message.contains("API key"),
        "unexpected error: {}",
        err.message
    );
}
