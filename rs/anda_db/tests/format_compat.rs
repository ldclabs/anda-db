//! On-disk format compatibility tests.
//!
//! `tests/fixtures/v<MAJOR>_<MINOR>/` holds complete database directories
//! written by past (and the current) versions of `anda_db`, checked into the
//! repository. `all_checked_in_fixtures_remain_readable` loads every fixture
//! into an in-memory store and verifies the database opens and its documents,
//! indexes and extensions are fully intact. Breaking any of these fixtures in
//! a pull request means the change breaks existing users' data — either add a
//! migration path or revert.
//!
//! After an *intentional, backward-compatible* format change, regenerate the
//! current version's fixture once:
//!
//! ```bash
//! cargo test -p anda_db --test format_compat -- --ignored generate
//! git add rs/anda_db/tests/fixtures
//! ```

use anda_db::{
    collection::{Collection, CollectionConfig},
    database::{AndaDB, DBConfig},
    error::DBError,
    index::HnswConfig,
    query::{Filter, Query, RangeQuery, Search},
    schema::{
        AndaDBSchema, FieldEntry, FieldKey, FieldType, Fv, Schema, SchemaError, Vector, bf16,
    },
    storage::StorageConfig,
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as ObjPath};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

const FIXTURES_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, AndaDBSchema)]
struct FixtureDoc {
    _id: u64,
    name: String,
    body: String,
    age: u64,
    active: bool,
    rating: f64,
    note: Option<String>,
    tags: Vec<String>,
    attrs: BTreeMap<String, u64>,
    embedding: Vector,
}

/// The exact contents every fixture is expected to hold. Append-only: never
/// edit existing entries, or old fixtures will (correctly) fail verification.
fn fixture_docs() -> Vec<FixtureDoc> {
    let embed = |values: [f32; 4]| -> Vector { values.into_iter().map(bf16::from_f32).collect() };
    vec![
        FixtureDoc {
            _id: 1,
            name: "alpha".to_string(),
            body: "alpha stores knowledge for agents".to_string(),
            age: 10,
            active: true,
            rating: 1.5,
            note: Some("first".to_string()),
            tags: vec!["rust".to_string(), "db".to_string()],
            attrs: BTreeMap::from([("k1".to_string(), 1)]),
            embedding: embed([1.0, 0.0, 0.0, 0.0]),
        },
        FixtureDoc {
            _id: 2,
            name: "beta".to_string(),
            body: "beta searches vectors quickly".to_string(),
            age: 20,
            active: false,
            rating: -2.25,
            note: None,
            tags: vec!["vector".to_string()],
            attrs: BTreeMap::from([("k2".to_string(), 2)]),
            embedding: embed([0.0, 1.0, 0.0, 0.0]),
        },
        FixtureDoc {
            _id: 3,
            name: "gamma".to_string(),
            body: "gamma ranks text with bm25".to_string(),
            age: 30,
            active: true,
            rating: 0.0,
            note: Some("unicode 标注 ✓".to_string()),
            tags: vec![],
            attrs: BTreeMap::new(),
            embedding: embed([0.0, 0.0, 1.0, 0.0]),
        },
    ]
}

fn db_config() -> DBConfig {
    DBConfig {
        name: "fixturedb".to_string(),
        description: "format compatibility fixture".to_string(),
        storage: StorageConfig::default(),
        lock: None,
    }
}

async fn open_docs_collection(db: &AndaDB) -> Result<Arc<Collection>, DBError> {
    db.open_or_create_collection(
        FixtureDoc::schema()?,
        CollectionConfig {
            name: "docs".to_string(),
            description: "format compatibility docs".to_string(),
        },
        async |collection| {
            collection.create_btree_index_nx(&["age"]).await?;
            collection.create_bm25_index_nx(&["body"]).await?;
            collection
                .create_hnsw_index_nx(
                    "embedding",
                    HnswConfig {
                        dimension: 4,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        },
    )
    .await
}

fn collect_files(dir: &Path, base: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    for entry in fs::read_dir(dir).expect("read fixture dir") {
        let entry = entry.expect("read fixture entry");
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, base, out);
        } else {
            let rel = path
                .strip_prefix(base)
                .expect("fixture file under base")
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.push((rel, fs::read(&path).expect("read fixture file")));
        }
    }
}

/// Loads a fixture directory into a fresh in-memory object store, so the
/// checked-in files are never modified by the test run.
async fn load_fixture(dir: &Path) -> Arc<InMemory> {
    let mut files = Vec::new();
    collect_files(dir, dir, &mut files);
    assert!(
        !files.is_empty(),
        "fixture {} contains no files",
        dir.display()
    );
    let store = Arc::new(InMemory::new());
    for (path, data) in files {
        store
            .put(&ObjPath::from(path), bytes::Bytes::from(data).into())
            .await
            .expect("seed fixture object");
    }
    store
}

async fn verify_fixture(store: Arc<InMemory>, fixture: &str) {
    let db = AndaDB::connect(store, db_config())
        .await
        .unwrap_or_else(|err| panic!("{fixture}: database no longer opens: {err:?}"));
    assert!(
        db.metadata().collections.contains("docs"),
        "{fixture}: collection list lost"
    );

    let collection = open_docs_collection(&db)
        .await
        .unwrap_or_else(|err| panic!("{fixture}: collection no longer opens: {err:?}"));

    // Every document must round-trip byte-exact through the stored format.
    let expected_docs = fixture_docs();
    assert_eq!(
        collection.len(),
        expected_docs.len(),
        "{fixture}: document count diverged"
    );
    for expected in &expected_docs {
        let got: FixtureDoc = collection
            .get_as(expected._id)
            .await
            .unwrap_or_else(|err| panic!("{fixture}: doc {} unreadable: {err:?}", expected._id));
        assert_eq!(&got, expected, "{fixture}: doc {} diverged", expected._id);
    }

    // B-Tree index.
    let ids = collection
        .search_ids(Query {
            filter: Some(Filter::Field((
                "age".to_string(),
                RangeQuery::Eq(Fv::U64(20)),
            ))),
            ..Default::default()
        })
        .await
        .expect("btree query failed");
    assert_eq!(ids, vec![2], "{fixture}: btree index diverged");

    // BM25 index.
    let ids = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("bm25".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("bm25 query failed");
    assert_eq!(ids, vec![3], "{fixture}: bm25 index diverged");

    // HNSW index.
    let ids = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(vec![0.0, 0.9, 0.1, 0.0]),
                ..Default::default()
            }),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .expect("hnsw query failed");
    assert_eq!(ids, vec![2], "{fixture}: hnsw index diverged");

    // Extensions.
    assert_eq!(
        collection.get_extension("format_marker"),
        Some(Fv::Text("anda-db-fixture".to_string())),
        "{fixture}: collection extension diverged"
    );
    assert_eq!(
        db.get_extension("format_marker"),
        Some(Fv::U64(1)),
        "{fixture}: database extension diverged"
    );

    db.close().await.expect("close failed");
}

#[tokio::test]
async fn all_checked_in_fixtures_remain_readable() {
    let root = PathBuf::from(FIXTURES_ROOT);
    let mut fixtures: Vec<PathBuf> = fs::read_dir(&root)
        .unwrap_or_else(|err| panic!("fixtures directory missing at {FIXTURES_ROOT}: {err}"))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with('v'))
        })
        .collect();
    fixtures.sort();
    assert!(
        !fixtures.is_empty(),
        "no format fixtures found; run `cargo test -p anda_db --test format_compat -- --ignored generate`"
    );

    for dir in fixtures {
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        let store = load_fixture(&dir).await;
        verify_fixture(store, &name).await;
    }
}

/// Regenerates the fixture for the *current* crate version. Ignored by
/// default: run manually after an intentional format change and commit the
/// result.
#[tokio::test]
#[ignore = "regenerates the on-disk format fixture; run manually and commit the result"]
async fn generate_fixture_for_current_version() {
    let store = Arc::new(InMemory::new());
    let db = AndaDB::create(store.clone(), db_config())
        .await
        .expect("create db");
    let collection = open_docs_collection(&db).await.expect("create collection");

    for doc in fixture_docs() {
        let id = collection.add_from(&doc).await.expect("add doc");
        assert_eq!(id, doc._id, "fixture docs must keep stable ids");
    }
    collection
        .save_extension(
            "format_marker".to_string(),
            Fv::Text("anda-db-fixture".to_string()),
        )
        .await
        .expect("save collection extension");
    db.set_extension("format_marker".to_string(), Fv::U64(1));
    db.close().await.expect("close db");

    // Dump the object store into the fixture directory.
    let mut version = env!("CARGO_PKG_VERSION").split('.');
    let (major, minor) = (
        version.next().expect("major version"),
        version.next().expect("minor version"),
    );
    let root = PathBuf::from(FIXTURES_ROOT).join(format!("v{major}_{minor}"));
    if root.exists() {
        fs::remove_dir_all(&root).expect("clear old fixture");
    }
    fs::create_dir_all(&root).expect("create fixture dir");

    use futures::TryStreamExt;
    let objects: Vec<_> = store.list(None).try_collect().await.expect("list objects");
    assert!(!objects.is_empty());
    for meta in objects {
        let data = store
            .get(&meta.location)
            .await
            .expect("get object")
            .bytes()
            .await
            .expect("object bytes");
        let file = root.join(meta.location.as_ref());
        fs::create_dir_all(file.parent().expect("object path has parent"))
            .expect("create fixture subdir");
        fs::write(&file, &data).expect("write fixture file");
    }

    // The fresh fixture must pass its own verification.
    let reloaded = load_fixture(&root).await;
    verify_fixture(reloaded, "freshly generated").await;
    println!("fixture written to {}", root.display());
}
