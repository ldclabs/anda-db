use super::*;
use crate::{
    database::{AndaDB, DBConfig},
    error::{CollectionState, DBError},
    index::HnswConfig,
    query::{Filter, Query, RangeQuery, Search},
    schema::{AndaDBSchema, Document, Fv, Json, Schema, Vector},
    storage::{PutMode, StorageConfig},
};
use anda_object_store::{FaultGate, FaultHandle, FaultKind, FaultOp, FaultRule, FaultStore};
use bytes::Bytes;
use ic_auth_types::ByteArrayB64;
use object_store::{
    ObjectStore, ObjectStoreExt, PutOptions, PutPayload, memory::InMemory, path::Path,
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeMap, sync::Arc, time::Duration};

// 测试用的文档结构
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, AndaDBSchema)]
struct TestDoc {
    pub _id: u64,
    pub name: String,
    pub age: u32,
    pub tags: Vec<String>,
    pub metadata: BTreeMap<String, Json>,

    pub data: BTreeMap<ByteArrayB64<4>, u64>,
    pub vector: Vector,
}

// 创建测试数据库和集合的辅助函数
async fn setup_test_db() -> Result<AndaDB, DBError> {
    let object_store = Arc::new(InMemory::new());

    let db_config = DBConfig {
        name: "test_db".to_string(),
        description: "Test database".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };

    let db = AndaDB::connect(object_store, db_config).await?;
    Ok(db)
}

fn simple_upgrade_schema(version: u64, add_fresh: bool) -> Schema {
    let mut builder = Schema::builder();
    builder.with_version(version);
    builder
        .add_field(Fe::new("payload".into(), Ft::Text).unwrap())
        .unwrap();
    if add_fresh {
        builder
            .add_field(Fe::new("fresh".into(), Ft::Option(Box::new(Ft::Text))).unwrap())
            .unwrap();
    }
    builder.build().unwrap()
}

#[tokio::test]
async fn schema_upgrade_recovers_unregistered_documents_and_intent_images() -> Result<(), DBError> {
    let make_schema = |version, resurrect: bool| -> Schema {
        let mut fields = BTreeMap::from([(FieldKey::from("keep"), Ft::Bool)]);
        if resurrect {
            fields.insert("retired".into(), Ft::Option(Box::new(Ft::Text)));
        }
        let mut b = Schema::builder();
        b.with_version(version);
        b.add_field(Fe::new("payload".into(), Ft::Map(fields)).unwrap())
            .unwrap();
        if version > 1 {
            b.add_field(Fe::new("fresh".into(), Ft::Option(Box::new(Ft::Text))).unwrap())
                .unwrap();
        }
        b.build().unwrap()
    };
    let mut wire = serde_json::to_value(make_schema(1, false)).unwrap();
    wire.as_object_mut().unwrap().remove("next_idx");
    wire.as_object_mut().unwrap().remove("history");
    let legacy: Schema = serde_json::from_value(wire).unwrap();
    let db = setup_test_db().await?;
    let mut collection = Collection::create(
        db.clone(),
        legacy.clone(),
        CollectionConfig {
            name: "history".into(),
            ..Default::default()
        },
    )
    .await?;
    let raw = DocumentOwned {
        fields: BTreeMap::from([
            (0, Fv::U64(1)),
            (
                1,
                Fv::Map(BTreeMap::from([
                    ("keep".into(), Fv::Bool(true)),
                    ("retired".into(), Fv::Text("old".into())),
                ])),
            ),
            (9, Fv::Text("removed top field".into())),
        ]),
    };
    collection
        .storage
        .create(&Collection::doc_path(1), &raw)
        .await?;
    assert!(
        collection.ids().is_empty(),
        "scan must include unregistered objects"
    );
    let mut previous = raw.clone();
    previous
        .fields
        .insert(12, Fv::Text("intent-only retired field".into()));
    collection
        .storage
        .create(
            &Collection::mutation_intent_path(1),
            &MutationIntent {
                purge_by_id: false,
                sequence: 1,
                document_id: 1,
                previous: Some(previous),
                proposed: Some(raw.clone()),
            },
        )
        .await?;
    collection.try_upgrade_schema(make_schema(2, false)).await?;
    assert_eq!(collection.schema.get_field("fresh").unwrap().idx(), 13);
    let loaded = Document::try_from_doc(collection.schema(), raw)?;
    assert!(loaded.get_field("fresh").is_none());
    let snapshot = serde_json::to_value(collection.metadata()).unwrap();
    assert!(
        collection
            .try_upgrade_schema(make_schema(3, true))
            .await
            .is_err()
    );
    assert_eq!(
        serde_json::to_value(collection.metadata()).unwrap(),
        snapshot
    );

    let mut broken = Collection::create(
        db.clone(),
        legacy,
        CollectionConfig {
            name: "broken_history".into(),
            ..Default::default()
        },
    )
    .await?;
    broken
        .storage
        .create(&Collection::doc_path(1), &"not a document")
        .await?;
    let snapshot = serde_json::to_value(broken.metadata()).unwrap();
    assert!(
        broken
            .try_upgrade_schema(make_schema(2, false))
            .await
            .is_err()
    );
    assert_eq!(serde_json::to_value(broken.metadata()).unwrap(), snapshot);
    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn schema_upgrade_ignores_undecodable_mutation_intents() -> Result<(), DBError> {
    let mut wire = serde_json::to_value(simple_upgrade_schema(1, false)).unwrap();
    wire.as_object_mut().unwrap().remove("next_idx");
    wire.as_object_mut().unwrap().remove("history");
    let legacy: Schema = serde_json::from_value(wire).unwrap();
    let db = setup_test_db().await?;
    let mut collection = Collection::create(
        db.clone(),
        legacy,
        CollectionConfig {
            name: "undecodable_intent_history".into(),
            ..Default::default()
        },
    )
    .await?;
    collection
        .storage
        .put_bytes(
            &Collection::mutation_intent_path(1),
            Bytes::from_static(b"not a mutation intent"),
            PutMode::Overwrite,
        )
        .await?;

    collection
        .try_upgrade_schema(simple_upgrade_schema(2, true))
        .await?;
    assert_eq!(collection.schema.get_field("fresh").unwrap().idx(), 2);
    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn schema_upgrade_ignores_reserved_and_misplaced_mutation_intents() -> Result<(), DBError> {
    let mut wire = serde_json::to_value(simple_upgrade_schema(1, false)).unwrap();
    wire.as_object_mut().unwrap().remove("next_idx");
    wire.as_object_mut().unwrap().remove("history");
    let legacy: Schema = serde_json::from_value(wire).unwrap();
    let db = setup_test_db().await?;
    let mut collection = Collection::create(
        db.clone(),
        legacy,
        CollectionConfig {
            name: "invalid_intent_history".into(),
            ..Default::default()
        },
    )
    .await?;
    let foreign_image = DocumentOwned {
        fields: BTreeMap::from([
            (0, Fv::U64(1)),
            (60_000, Fv::Text("must not affect schema history".into())),
        ]),
    };
    collection
        .storage
        .create(
            &Collection::mutation_intent_path(2),
            &MutationIntent {
                purge_by_id: false,
                sequence: 2,
                document_id: 0,
                previous: Some(foreign_image.clone()),
                proposed: None,
            },
        )
        .await?;
    collection
        .storage
        .create(
            &Collection::mutation_intent_path(3),
            &MutationIntent {
                purge_by_id: false,
                sequence: 4,
                document_id: 1,
                previous: Some(foreign_image),
                proposed: None,
            },
        )
        .await?;

    collection
        .try_upgrade_schema(simple_upgrade_schema(2, true))
        .await?;
    assert_eq!(collection.schema.get_field("fresh").unwrap().idx(), 2);
    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn schema_upgrade_is_durable_before_the_open_callback_writes() -> Result<(), DBError> {
    let mut wire = serde_json::to_value(simple_upgrade_schema(1, false)).unwrap();
    wire.as_object_mut().unwrap().remove("history");
    let incomplete: Schema = serde_json::from_value(wire).unwrap();
    assert!(incomplete.has_allocation_watermark());
    assert!(!incomplete.has_upgrade_history());

    let db = setup_test_db().await?;
    let name = "upgrade_callback_checkpoint";
    let created = Collection::create(
        db.clone(),
        incomplete,
        CollectionConfig {
            name: name.into(),
            ..Default::default()
        },
    )
    .await?;
    drop(created);

    let upgraded = simple_upgrade_schema(2, true);
    let first_open = Collection::open(
        db.clone(),
        name.into(),
        Some(upgraded),
        async |collection| {
            let mut doc = Document::new(collection.schema());
            doc.set_id(0);
            doc.set_field("payload", Fv::Text("old".into()))?;
            doc.set_field(
                "fresh",
                Fv::Text("written before the callback failed".into()),
            )?;
            collection.add(doc).await?;
            Err(DBError::Generic {
                name: "test".into(),
                source: "stop after writing the document".into(),
            })
        },
    )
    .await;
    assert!(first_open.is_err());

    // Reopen without supplying a schema: the only way this handle can
    // understand index 2 is if the first open persisted its upgrade before
    // invoking the callback.
    let reopened = Collection::open(db.clone(), name.into(), None, async |_| Ok(())).await?;
    assert_eq!(reopened.schema.version(), 2);
    assert_eq!(reopened.schema.get_field("fresh").unwrap().idx(), 2);
    assert_eq!(
        reopened.get(1).await?.get_field("fresh"),
        Some(&Fv::Text("written before the callback failed".into()))
    );
    db.close().await?;
    Ok(())
}

// 创建测试集合的辅助函数
async fn create_test_collection<F>(db: &AndaDB, f: F) -> Result<Arc<Collection>, DBError>
where
    F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
{
    // 创建测试文档的模式
    let schema = TestDoc::schema()?;
    let collection_config = CollectionConfig {
        name: "test_collection".to_string(),
        description: "Test collection".to_string(),
    };

    let collection = db
        .open_or_create_collection(schema, collection_config, f)
        .await?;

    Ok(collection)
}

// 创建测试文档的辅助函数
/// An in-memory store with deterministic fault injection.
fn fault_store() -> (Arc<dyn ObjectStore>, FaultHandle) {
    let (store, faults) = FaultStore::wrap(InMemory::new());
    (Arc::new(store), faults)
}

/// Holds every PUT whose path contains `path` at `kind`'s point until
/// the kind's gate is released.
fn pause_puts(faults: &FaultHandle, path: &str, kind: FaultKind) {
    faults.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: Some(path.to_string()),
        skip: 0,
        times: u64::MAX,
        kind,
    });
}

fn create_test_doc(_id: u64, name: &str, age: u32, tags: Vec<&str>) -> TestDoc {
    TestDoc {
        _id,
        name: name.to_string(),
        age,
        tags: tags.iter().map(|s| s.to_string()).collect(),
        metadata: BTreeMap::new(),
        data: BTreeMap::new(),
        vector: vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
            .into_iter()
            .map(bf16::from_f32)
            .collect(),
    }
}

struct BadArrayBTreeHooks;

impl IndexHooks for BadArrayBTreeHooks {
    fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
        if index.name() == "tags" {
            return Some(Cow::Owned(Fv::Array(vec![
                Fv::Text("valid".to_string()),
                Fv::I64(42),
            ])));
        }

        match index.virtual_field() {
            [] => None,
            [name] => doc.get_field(name).map(Cow::Borrowed),
            _ => None,
        }
    }
}

struct BackfillErrorHooks {
    btree_value: Option<Fv>,
    hnsw_vector: Option<Vector>,
}

impl BackfillErrorHooks {
    fn btree(value: Fv) -> Self {
        Self {
            btree_value: Some(value),
            hnsw_vector: None,
        }
    }

    fn hnsw(vector: Vector) -> Self {
        Self {
            btree_value: None,
            hnsw_vector: Some(vector),
        }
    }
}

impl IndexHooks for BackfillErrorHooks {
    fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
        if let Some(value) = &self.btree_value {
            return Some(Cow::Owned(value.clone()));
        }

        IndexHooks::btree_index_value(&DefaultIndexHooks, index, doc)
    }

    fn hnsw_index_value<'a>(&self, index: &Hnsw, doc: &'a Document) -> Option<Cow<'a, Vector>> {
        if let Some(vector) = &self.hnsw_vector {
            return Some(Cow::Owned(vector.clone()));
        }

        IndexHooks::hnsw_index_value(&DefaultIndexHooks, index, doc)
    }
}

struct UpdateTagsBTreeErrorHooks;

impl IndexHooks for UpdateTagsBTreeErrorHooks {
    fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
        if index.name() == "tags" {
            return match doc.get_field("age") {
                Some(Fv::U64(age)) if *age >= 31 => Some(Cow::Owned(Fv::Array(vec![
                    Fv::Text("new".to_string()),
                    Fv::I64(-1),
                ]))),
                _ => Some(Cow::Owned(Fv::Array(vec![Fv::Text("old".to_string())]))),
            };
        }

        IndexHooks::btree_index_value(&DefaultIndexHooks, index, doc)
    }
}

struct RecoveryCustomHooks;

impl IndexHooks for RecoveryCustomHooks {
    fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
        if index.name() == "name" {
            let name = match doc.get_field("name") {
                Some(Fv::Text(name)) => name,
                _ => return None,
            };
            return Some(Cow::Owned(Fv::Text(format!(
                "hook:{}",
                name.to_lowercase()
            ))));
        }
        IndexHooks::btree_index_value(&DefaultIndexHooks, index, doc)
    }

    fn bm25_index_value<'a>(&self, index: &BM25, doc: &'a Document) -> Option<Cow<'a, str>> {
        if index.name() == "name" && doc.get_field("name").is_some() {
            return Some(Cow::Borrowed("hooktoken"));
        }
        IndexHooks::bm25_index_value(&DefaultIndexHooks, index, doc)
    }
}

#[tokio::test]
async fn test_reconcile_storage_recovers_orphans_and_drops_dead_ids() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    let id1 = collection
        .add_from(&create_test_doc(0, "alice", 30, vec!["a"]))
        .await?;
    let id2 = collection
        .add_from(&create_test_doc(0, "bob", 40, vec!["b"]))
        .await?;
    collection.flush(unix_ms()).await?;

    // Simulate a crash between object deletion and the ids flush: the
    // object is gone but the bitmap still references it.
    collection
        .storage
        .delete(&Collection::doc_path(id2))
        .await?;
    // Simulate an orphan document written but never registered: present
    // on disk, absent from the bitmap (well beyond any repair scan).
    let orphan_id = id2 + 500;
    let mut orphan = Document::new(collection.schema());
    orphan.set_doc(
        Document::try_from(
            collection.schema(),
            &create_test_doc(orphan_id, "carol", 50, vec!["c"]),
        )?
        .into(),
    )?;
    collection
        .storage
        .create(&Collection::doc_path(orphan_id), &orphan)
        .await?;

    let (recovered, dropped) = collection.reconcile_storage().await?;
    assert_eq!(recovered, 1);
    assert_eq!(dropped, 1);
    assert!(collection.contains(id1));
    assert!(!collection.contains(id2));
    assert!(collection.contains(orphan_id));
    // The recovered document is readable and indexed again.
    let doc: TestDoc = collection.get_as(orphan_id).await?;
    assert_eq!(doc.name, "carol");
    assert!(collection.max_document_id() >= orphan_id);

    // Idempotent: a second reconcile finds nothing.
    assert_eq!(collection.reconcile_storage().await?, (0, 0));
    Ok(())
}

#[tokio::test]
async fn test_collection_create() -> Result<(), DBError> {
    let db = setup_test_db().await?;

    let collection = create_test_collection(&db, async |c| {
        c.create_bm25_index_nx(&["name", "tags", "metadata"])
            .await?;
        c.create_hnsw_index_nx("vector", HnswConfig::default())
            .await?;
        Ok(())
    })
    .await?;

    assert_eq!(collection.name(), "test_collection");
    assert_eq!(collection.metadata().config.description, "Test collection");

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_collection_open() -> Result<(), DBError> {
    let db = setup_test_db().await?;

    // 首先创建集合
    {
        let collection = create_test_collection(&db, async |_| Ok(())).await?;
        assert_eq!(collection.name(), "test_collection");

        // 添加一个文档以确保有数据可以在重新打开时加载
        let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
        let doc_obj = Document::try_from(collection.schema(), &doc)?;
        let id = collection.add(doc_obj).await?;
        assert_eq!(id, 1);

        // 刷新以确保数据被持久化
        collection.flush(unix_ms()).await?;
    }

    // 关闭并重新打开数据库
    db.close().await?;
    let db = AndaDB::connect(
        db.object_store(),
        DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;

    // 重新打开集合
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;

    assert_eq!(collection.name(), "test_collection");
    assert_eq!(collection.metadata().stats.num_documents, 1);

    // 验证文档是否正确加载
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "_id".to_string(),
                RangeQuery::Eq(Fv::U64(1)),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Alice");

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_document_operations() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    // 添加文档
    let doc1 = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
    let doc_obj1 = Document::try_from(collection.schema(), &doc1)?;
    let id1 = collection.add(doc_obj1).await?;
    assert_eq!(id1, 1);

    let doc2 = create_test_doc(0, "Bob", 25, vec!["tall", "quiet"]);
    let doc_obj2 = Document::try_from(collection.schema(), &doc2)?;
    let id2 = collection.add(doc_obj2).await?;
    assert_eq!(id2, 2);

    // 获取文档
    let result: TestDoc = collection.get_as(id1).await?;
    assert_eq!(result.name, "Alice");
    assert_eq!(result.age, 30);

    // 删除文档
    collection.remove(id2).await?;

    // 验证删除
    let result = collection.get(id2).await;
    assert!(result.is_err());

    // 验证集合统计信息
    let stats = collection.stats();
    assert_eq!(stats.num_documents, 1);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_remove_rolls_back_indexes_when_storage_delete_fails() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let db_config = DBConfig {
        name: "test_db".to_string(),
        description: "Test database".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), db_config).await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["name"]).await?;
        collection
            .create_bm25_index_nx(&["name", "tags", "metadata"])
            .await?;
        collection
            .create_hnsw_index_nx(
                "vector",
                HnswConfig {
                    dimension: 10,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    })
    .await?;

    let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
    let doc_obj = Document::try_from(collection.schema(), &doc)?;
    let id = collection.add(doc_obj).await?;
    assert_eq!(id, 1);

    faults.push_rule(FaultRule::fail_once(FaultOp::Delete, "data/1.cbor"));
    let err = collection.remove(id).await.unwrap_err();
    assert!(matches!(err, DBError::Storage { .. }));

    assert!(collection.contains(id));
    let stored: TestDoc = collection.get_as(id).await?;
    assert_eq!(stored.name, "Alice");

    let btree_ids = collection
        .query_ids(
            Filter::Field(("name".to_string(), RangeQuery::Eq(Fv::Text("Alice".into())))),
            Some(10),
        )
        .await?;
    assert_eq!(btree_ids, vec![id]);

    let bm25_ids = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("Alice".to_string()),
                ..Default::default()
            }),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert!(bm25_ids.contains(&id));

    let hnsw_ids = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(
                    doc.vector
                        .iter()
                        .map(|value| value.to_f32())
                        .collect::<Vec<_>>(),
                ),
                ..Default::default()
            }),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert!(hnsw_ids.contains(&id));

    // The DELETE outcome was unknown, so the handle is poisoned: reads
    // above still serve the rolled-back in-memory state, but mutations
    // are rejected until the collection is reopened.
    assert!(collection.is_poisoned());
    assert!(collection.remove(id).await.is_err());

    // Reopening replays the retained remove intent against storage: the
    // object still exists (the injected failure happened before erasing
    // it), so the document survives fully indexed.
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert!(collection.contains(id));
    let stored: TestDoc = collection.get_as(id).await?;
    assert_eq!(stored.name, "Alice");

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_create_indexes_backfills_existing_documents() -> Result<(), DBError> {
    let object_store = Arc::new(InMemory::new());
    let db_config = DBConfig {
        name: "test_db".to_string(),
        description: "Test database".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };

    let db = AndaDB::connect(object_store.clone(), db_config.clone()).await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    let alice = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
    let alice_id = collection
        .add(Document::try_from(collection.schema(), &alice)?)
        .await?;
    let bob = create_test_doc(0, "Bob", 42, vec!["careful", "focused"]);
    let bob_id = collection
        .add(Document::try_from(collection.schema(), &bob)?)
        .await?;
    assert_eq!((alice_id, bob_id), (1, 2));
    db.close().await?;

    let db = AndaDB::connect(object_store.clone(), db_config).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |collection| {
            collection.create_btree_index_nx(&["name"]).await?;
            collection
                .create_bm25_index_nx(&["name", "tags", "metadata"])
                .await?;
            collection
                .create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

    let btree_ids = collection
        .query_ids(
            Filter::Field(("name".to_string(), RangeQuery::Eq(Fv::Text("Alice".into())))),
            Some(10),
        )
        .await?;
    assert_eq!(btree_ids, vec![alice_id]);

    let bm25_ids = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("focused".to_string()),
                ..Default::default()
            }),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert!(bm25_ids.contains(&bob_id));

    assert_eq!(collection.get_hnsw_index("vector")?.stats().num_elements, 2);
    let hnsw_ids = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(
                    alice
                        .vector
                        .iter()
                        .map(|value| value.to_f32())
                        .collect::<Vec<_>>(),
                ),
                ..Default::default()
            }),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert!(
        hnsw_ids.contains(&alice_id),
        "HNSW results should contain {alice_id}, got {hnsw_ids:?}",
    );

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_index_operations() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        // 创建索引
        collection.create_btree_index_nx(&["name"]).await?;
        collection.create_btree_index_nx(&["age"]).await?;
        collection.create_btree_index_nx(&["tags"]).await?;

        // 创建搜索索引
        collection
            .create_bm25_index_nx(&["name", "tags", "metadata"])
            .await?;
        collection
            .create_hnsw_index_nx(
                "vector",
                HnswConfig {
                    dimension: 10,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    })
    .await?;

    // 添加测试文档
    for (name, age, tags) in [
        ("Alice", 30, vec!["smart", "friendly"]),
        ("Bob", 25, vec!["tall", "quiet"]),
        ("Charlie", 35, vec!["smart", "tall"]),
        ("David", 40, vec!["friendly", "quiet"]),
    ] {
        let doc = create_test_doc(0, name, age, tags);
        let doc_obj = Document::try_from(collection.schema(), &doc)?;
        collection.add(doc_obj).await?;
    }

    // 刷新以确保索引更新
    collection.flush(unix_ms()).await?;

    // 测试精确匹配查询
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("Alice".to_string())),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Alice");

    // 测试范围查询
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "age".to_string(),
                RangeQuery::Gt(Fv::U64(30)),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 2);
    assert!(result.iter().any(|doc| doc.name == "Charlie"));
    assert!(result.iter().any(|doc| doc.name == "David"));

    // 测试数组字段查询
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "tags".to_string(),
                RangeQuery::Eq(Fv::Text("smart".to_string())),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 2);
    assert!(result.iter().any(|doc| doc.name == "Alice"));
    assert!(result.iter().any(|doc| doc.name == "Charlie"));

    // 测试文本搜索
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            search: Some(Search {
                text: Some("Alice".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Alice");

    // 测试向量搜索
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            search: Some(Search {
                vector: Some(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;

    assert!(!result.is_empty());

    // 测试复合查询
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            search: Some(Search {
                text: Some("tall".to_string()),
                ..Default::default()
            }),
            filter: Some(Filter::Field((
                "age".to_string(),
                RangeQuery::Lt(Fv::U64(30)),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Bob");

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_remove_indexes() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let object_store = db.object_store();

    {
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["name"]).await?;
            collection
                .create_bm25_index_nx(&["name", "tags", "metadata"])
                .await?;
            collection
                .create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

        assert!(collection.metadata().btree_indexes.contains_key("name"));
        assert!(
            collection
                .metadata()
                .bm25_indexes
                .contains_key("name-tags-metadata")
        );
        assert!(collection.metadata().hnsw_indexes.contains_key("vector"));
        assert!(collection.get_btree_index(&["name"]).is_ok());
        assert!(
            collection
                .get_bm25_index(&["name", "tags", "metadata"])
                .is_ok()
        );
        assert!(collection.get_hnsw_index("vector").is_ok());
    }

    db.close().await?;
    let db = AndaDB::connect(
        object_store.clone(),
        DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;

    let collection = db
        .open_collection("test_collection".to_string(), async |collection| {
            assert!(collection.remove_btree_index(&["name"]).await?);
            assert!(
                collection
                    .remove_bm25_index(&["name", "tags", "metadata"])
                    .await?
            );
            assert!(collection.remove_hnsw_index("vector").await?);

            assert!(!collection.remove_btree_index(&["name"]).await?);
            assert!(
                !collection
                    .remove_bm25_index(&["name", "tags", "metadata"])
                    .await?
            );
            assert!(!collection.remove_hnsw_index("vector").await?);

            assert!(collection.get_btree_index(&["name"]).is_err());
            assert!(
                collection
                    .get_bm25_index(&["name", "tags", "metadata"])
                    .is_err()
            );
            assert!(collection.get_hnsw_index("vector").is_err());

            let meta = collection.metadata();
            assert!(!meta.btree_indexes.contains_key("name"));
            assert!(!meta.bm25_indexes.contains_key("name-tags-metadata"));
            assert!(!meta.hnsw_indexes.contains_key("vector"));

            collection.flush(unix_ms()).await?;
            Ok(())
        })
        .await?;

    assert!(collection.get_btree_index(&["name"]).is_err());
    assert!(
        collection
            .get_bm25_index(&["name", "tags", "metadata"])
            .is_err()
    );
    assert!(collection.get_hnsw_index("vector").is_err());
    assert!(!collection.metadata().btree_indexes.contains_key("name"));
    assert!(
        !collection
            .metadata()
            .bm25_indexes
            .contains_key("name-tags-metadata")
    );
    assert!(!collection.metadata().hnsw_indexes.contains_key("vector"));

    db.close().await?;
    let db = AndaDB::connect(
        object_store,
        DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;

    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;

    assert!(collection.get_btree_index(&["name"]).is_err());
    assert!(
        collection
            .get_bm25_index(&["name", "tags", "metadata"])
            .is_err()
    );
    assert!(collection.get_hnsw_index("vector").is_err());
    assert!(!collection.metadata().btree_indexes.contains_key("name"));
    assert!(
        !collection
            .metadata()
            .bm25_indexes
            .contains_key("name-tags-metadata")
    );
    assert!(!collection.metadata().hnsw_indexes.contains_key("vector"));

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_array_btree_index_update_behavior() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["tags"]).await?;
        Ok(())
    })
    .await?;

    // 添加一个包含 ["a", "b"] 标签的文档
    let doc = create_test_doc(0, "Eve", 22, vec!["a", "b"]);
    let id = collection.add_from(&doc).await?;

    // 刷新确保建立索引
    collection.flush(unix_ms()).await?;

    // 查询 tags == "a" 应命中
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "tags".to_string(),
                RangeQuery::Eq(Fv::Text("a".to_string())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Eve");

    // 更新 tags 为 ["b", "c"]，应移除 "a"，新增 "c"
    let mut fields = BTreeMap::new();
    fields.insert(
        "tags".to_string(),
        Fv::Array(vec![Fv::Text("b".to_string()), Fv::Text("c".to_string())]),
    );
    collection.update(id, fields).await?;
    collection.flush(unix_ms()).await?;

    // 查询 tags == "a" 应不命中
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "tags".to_string(),
                RangeQuery::Eq(Fv::Text("a".to_string())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 0);

    // 查询 tags == "c" 应命中
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "tags".to_string(),
                RangeQuery::Eq(Fv::Text("c".to_string())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Eve");

    // 验证 BTree 索引的 keys: 不应包含 "a"，应包含 "b" 和 "c"
    let idx = collection.get_btree_index(&["tags"])?;
    let keys = idx.keys(None, None);
    let keys_text: Vec<String> = keys
        .into_iter()
        .filter_map(|fv| match fv {
            Fv::Text(s) => Some(s),
            _ => None,
        })
        .collect();
    assert!(!keys_text.contains(&"a".to_string()));
    assert!(keys_text.contains(&"b".to_string()));
    assert!(keys_text.contains(&"c".to_string()));

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_map_btree_index_update_behavior() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["metadata"]).await?;
        collection.create_btree_index_nx(&["data"]).await?;
        Ok(())
    })
    .await?;

    let mut doc = create_test_doc(0, "Eve", 22, vec![]);
    doc.metadata.insert("key1".to_string(), "a".into());
    doc.metadata.insert("key2".to_string(), "b".into());
    doc.data.insert([0, 0, 0, 1].into(), 1);
    let id = collection.add_from(&doc).await?;

    // 刷新确保建立索引
    collection.flush(unix_ms()).await?;

    // 查询 metadata.key == "key1" 应命中
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "metadata".to_string(),
                RangeQuery::Eq(Fv::Text("key1".to_string())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Eve");
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "data".to_string(),
                RangeQuery::Eq(Fv::Bytes([0, 0, 0, 1].into())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Eve");

    println!("Initial search tests passed.");

    let mut fields = BTreeMap::new();
    fields.insert(
        "metadata".to_string(),
        Fv::Map(BTreeMap::from([
            ("key2".into(), Fv::Text("b".to_string())),
            ("key3".into(), Fv::Text("c".to_string())),
        ])),
    );
    fields.insert(
        "data".to_string(),
        Fv::Map(BTreeMap::from([
            ([0, 0, 0, 2].into(), Fv::U64(2)),
            ([0, 0, 0, 3].into(), Fv::U64(3)),
        ])),
    );
    collection.update(id, fields).await?;
    collection.flush(unix_ms()).await?;

    // 查询 metadata.key == "key1" 应不命中
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "metadata".to_string(),
                RangeQuery::Eq(Fv::Text("key1".to_string())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 0);
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "data".to_string(),
                RangeQuery::Eq(Fv::Bytes([0, 0, 0, 1].into())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 0);

    // 查询 metadata.key == "key2" 应命中
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "metadata".to_string(),
                RangeQuery::Eq(Fv::Text("key2".to_string())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Eve");
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "data".to_string(),
                RangeQuery::Eq(Fv::Bytes([0, 0, 0, 3].into())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Eve");

    let idx = collection.get_btree_index(&["metadata"])?;
    let keys = idx.keys(None, None);
    let keys_text: Vec<String> = keys
        .into_iter()
        .filter_map(|fv| match fv {
            Fv::Text(s) => Some(s),
            _ => None,
        })
        .collect();
    assert!(!keys_text.contains(&"key1".to_string()));
    assert!(keys_text.contains(&"key2".to_string()));
    assert!(keys_text.contains(&"key3".to_string()));

    let idx = collection.get_btree_index(&["data"])?;
    let keys = idx.keys(None, None);
    let keys_values: Vec<Vec<u8>> = keys
        .into_iter()
        .filter_map(|fv| match fv {
            Fv::Bytes(b) => Some(b),
            _ => None,
        })
        .collect();
    assert!(!keys_values.contains(&[0, 0, 0, 1].to_vec()));
    assert!(keys_values.contains(&[0, 0, 0, 2].to_vec()));
    assert!(keys_values.contains(&[0, 0, 0, 3].to_vec()));

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_compound_btree_index_query() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["name", "age"]).await?;
        Ok(())
    })
    .await?;

    // 添加三条数据
    for (name, age) in [("Alice", 30), ("Alice", 31), ("Bob", 25)] {
        let doc = create_test_doc(0, name, age as u32, vec!["x"]);
        collection.add_from(&doc).await?;
    }

    collection.flush(unix_ms()).await?;

    // 通过虚拟字段值（name-age）做 Eq 查询
    let bytes = crate::index::virtual_field_value(&[
        Some(&Fv::Text("Alice".to_string())),
        Some(&Fv::U64(30)),
    ])
    .expect("virtual_field_value should produce bytes for composite fields");

    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "name-age".to_string(),
                RangeQuery::Eq(bytes),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Alice");
    assert_eq!(result[0].age, 30);

    // 错误的组合应不命中
    let invalid = crate::index::virtual_field_value(&[
        Some(&Fv::Text("Alice".to_string())),
        Some(&Fv::U64(32)),
    ])
    .unwrap();

    let result_none: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "name-age".to_string(),
                RangeQuery::Eq(invalid),
            ))),
            ..Default::default()
        })
        .await?;
    assert!(result_none.is_empty());

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_persistence() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let object_store = db.object_store();

    // 创建集合并添加文档
    {
        let collection = create_test_collection(&db, async |collection| {
            // 创建索引
            collection.create_btree_index_nx(&["name"]).await?;
            collection.create_btree_index_nx(&["age"]).await?;
            collection.create_btree_index_nx(&["tags"]).await?;

            // 创建搜索索引
            collection
                .create_bm25_index_nx(&["name", "tags", "metadata"])
                .await?;
            collection
                .create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

        // 添加文档
        let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
        let doc_obj = Document::try_from(collection.schema(), &doc)?;
        collection.add(doc_obj).await?;

        // 刷新以确保数据被持久化
        // collection.flush(unix_ms()).await?;

        // 关闭集合
        // collection.close().await?;
    }

    // 关闭并持久化数据库
    db.close().await?;

    // 重新打开数据库和集合
    let db = AndaDB::connect(
        object_store.clone(),
        DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;

    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;

    // 验证文档是否正确加载
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("Alice".to_string())),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "Alice");
    assert_eq!(result[0].age, 30);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_read_only_mode() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    // 添加一个文档
    let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
    let doc_obj = Document::try_from(collection.schema(), &doc)?;
    collection.add(doc_obj).await?;

    let mut too_deep = Fv::Text("leaf".to_string());
    for _ in 0..70 {
        too_deep = Fv::Array(vec![too_deep]);
    }
    let err = collection
        .save_extension("too_deep".to_string(), too_deep)
        .await
        .unwrap_err();
    assert!(matches!(err, DBError::Schema { .. }));

    // 设置为只读模式
    collection.set_read_only(true);

    let err = collection
        .save_extension("blocked".to_string(), Fv::Text("value".to_string()))
        .await
        .unwrap_err();
    assert!(matches!(err, DBError::Generic { .. }));

    let err = collection.remove_extension("blocked").await.unwrap_err();
    assert!(matches!(err, DBError::Generic { .. }));

    // 尝试添加另一个文档，应该失败
    let doc2 = create_test_doc(0, "Bob", 25, vec!["tall", "quiet"]);
    let doc_obj2 = Document::try_from(collection.schema(), &doc2)?;
    let result = collection.add(doc_obj2).await;

    assert!(result.is_err());

    // 验证读取操作仍然有效
    let result: TestDoc = collection.get_as(1).await?;
    assert_eq!(result.name, "Alice");

    // 恢复为读写模式
    collection.set_read_only(false);

    // 现在应该可以添加文档
    let doc3 = create_test_doc(0, "Charlie", 35, vec!["smart", "tall"]);
    let doc_obj3 = Document::try_from(collection.schema(), &doc3)?;
    let id = collection.add(doc_obj3).await?;
    assert_eq!(id, 2);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_error_handling() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        // 测试创建已存在的索引
        collection.create_btree_index_nx(&["name"]).await?;
        let result = collection.create_btree_index(&["name"]).await;
        assert!(result.is_err());
        Ok(())
    })
    .await?;

    // 测试获取不存在的文档
    let result = collection.get(999).await;
    assert!(result.is_err());

    // 测试删除不存在的文档
    let result = collection.remove(999).await;
    assert!(result.is_ok());

    // 测试无效的查询
    let result: Result<Vec<TestDoc>, DBError> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "non_existent_field".to_string(),
                RangeQuery::Eq(Fv::Text("value".to_string())),
            ))),
            ..Default::default()
        })
        .await;

    assert!(result.is_err());

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_get_and_search_propagate_corrupt_document_errors() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    let doc = create_test_doc(0, "Alice", 30, vec!["smart"]);
    let id = collection.add_from(&doc).await?;
    collection.flush(unix_ms()).await?;
    collection
        .storage
        .put_bytes(
            &Collection::doc_path(id),
            Bytes::from_static(b"not valid cbor"),
            PutMode::Overwrite,
        )
        .await?;

    let err = collection.get(id).await.unwrap_err();
    assert!(matches!(err, DBError::Serialization { .. }));

    let err = collection
        .search(Query {
            filter: Some(Filter::Field((
                Schema::ID_KEY.to_string(),
                RangeQuery::Eq(Fv::U64(id)),
            ))),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, DBError::Serialization { .. }));

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_btree_array_index_rejects_mismatched_hook_values() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["tags"]).await?;
        collection.set_index_hooks(Arc::new(BadArrayBTreeHooks));
        Ok(())
    })
    .await?;

    let doc = create_test_doc(0, "Alice", 30, vec!["smart"]);
    let doc = Document::try_from(collection.schema(), &doc)?;
    let err = collection.add(doc).await.unwrap_err();
    assert!(matches!(err, DBError::Index { .. }));
    assert!(collection.is_empty());

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_index_backfill_and_update_errors_cleanup_partial_state() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    let doc = create_test_doc(0, "Alice", 30, vec!["smart"]);
    let id = collection.add_from(&doc).await?;
    assert_eq!(id, 1);

    let object_store = db.object_store();
    db.close().await?;
    let db = AndaDB::connect(
        object_store,
        DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;

    let collection = db
        .open_collection("test_collection".to_string(), async |collection| {
            assert!(matches!(
                collection.create_btree_index_nx(&["missing"]).await,
                Err(DBError::Schema { .. })
            ));
            assert!(matches!(
                collection.create_bm25_index_nx(&["missing"]).await,
                Err(DBError::Schema { .. })
            ));
            assert!(matches!(
                collection
                    .create_hnsw_index_nx("age", HnswConfig::default())
                    .await,
                Err(DBError::Schema { .. })
            ));
            assert!(matches!(
                collection
                    .create_hnsw_index_nx(
                        "vector",
                        HnswConfig {
                            dimension: 10,
                            ef_search: HnswConfig::MAX_EF_SEARCH + 1,
                            ..Default::default()
                        },
                    )
                    .await,
                Err(DBError::Index { .. })
            ));

            collection.set_index_hooks(Arc::new(BackfillErrorHooks::btree(Fv::I64(-1))));
            let err = collection.create_btree_index(&["tags"]).await.unwrap_err();
            assert!(matches!(err, DBError::Index { .. }));
            assert!(collection.get_btree_index(&["tags"]).is_err());

            let err = collection
                .create_btree_index(&["name", "age"])
                .await
                .unwrap_err();
            assert!(matches!(err, DBError::Index { .. }));
            assert!(collection.get_btree_index(&["name", "age"]).is_err());

            collection.set_index_hooks(Arc::new(BackfillErrorHooks::hnsw(vec![bf16::from_f32(
                0.1,
            )])));
            let err = collection
                .create_hnsw_index(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(err, DBError::Index { .. }));
            assert!(collection.get_hnsw_index("vector").is_err());

            collection.set_index_hooks(Arc::new(DefaultIndexHooks));
            collection.create_btree_index(&["tags"]).await?;
            collection.set_index_hooks(Arc::new(UpdateTagsBTreeErrorHooks));

            let err = collection
                .update(
                    id,
                    BTreeMap::from([
                        ("age".to_string(), Fv::U64(31)),
                        (
                            "tags".to_string(),
                            Fv::Array(vec![Fv::Text("updated".to_string())]),
                        ),
                    ]),
                )
                .await
                .unwrap_err();
            assert!(matches!(err, DBError::Index { .. }));

            let stored: TestDoc = collection.get_as(id).await?;
            assert_eq!(stored.age, 30);
            assert_eq!(stored.tags, vec!["smart".to_string()]);

            Ok(())
        })
        .await?;

    assert_eq!(collection.len(), 1);
    db.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_operations() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        // 创建索引
        collection.create_btree_index_nx(&["name"]).await?;
        Ok(())
    })
    .await?;

    // 并发添加多个文档
    let mut handles = Vec::new();
    for i in 0..10 {
        let collection_clone = collection.clone();
        let handle = tokio::spawn(async move {
            let doc = create_test_doc(0, &format!("Person{i}"), 20 + i, vec!["tag"]);
            let doc_obj = Document::try_from(collection_clone.schema(), &doc).unwrap();
            collection_clone.add(doc_obj).await
        });
        handles.push(handle);
    }

    // 等待所有任务完成
    let mut ids = Vec::new();
    for handle in handles {
        let id = handle.await.unwrap()?;
        ids.push(id);
    }

    // 验证所有文档都被添加
    assert_eq!(ids.len(), 10);
    // 验证文档数量
    let stats = collection.stats();
    assert_eq!(stats.num_documents, 10);

    // 并发获取文档
    let mut handles = Vec::new();
    for id in ids {
        let collection_clone = collection.clone();
        let handle = tokio::spawn(async move { collection_clone.get_as::<TestDoc>(id).await });
        handles.push(handle);
    }

    // 等待所有任务完成
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_metadata_updates() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    // 记录初始版本
    let initial_version = collection.metadata().stats.version;

    // 添加文档应该更新元数据
    let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
    let doc_obj = Document::try_from(collection.schema(), &doc)?;
    collection.add(doc_obj).await?;

    // 验证版本已更新
    let new_version = collection.metadata().stats.version;
    assert!(new_version > initial_version);

    // 验证统计信息已更新
    let stats = collection.stats();
    assert_eq!(stats.num_documents, 1);
    assert_eq!(stats.insert_count, 1);

    // 删除文档应该更新元数据
    collection.remove(1).await?;

    // 验证统计信息已更新
    let stats = collection.stats();
    assert_eq!(stats.num_documents, 0);
    assert_eq!(stats.delete_count, 1);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_flush_persists_dirty_indexes_even_when_collection_metadata_unchanged()
-> Result<(), DBError> {
    let db = setup_test_db().await?;

    {
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["name"]).await?;
            Ok(())
        })
        .await?;

        let doc = create_test_doc(0, "Alice", 30, vec!["smart"]);
        let id = collection.add_from(&doc).await?;
        assert_eq!(id, 1);

        // First flush to persist the baseline state.
        // Use the same millisecond timestamp as the initial storage metadata save
        // to ensure checkpoint persistence is not skipped by rate limiting.
        let same_ms = collection.storage.stats().last_saved;
        assert!(collection.flush(same_ms).await?);

        // Mutate index-only state directly: remove the mapping from btree index.
        let index = collection.find_btree_index(&["name"])?;
        assert!(index.remove(id, &Fv::Text("Alice".to_string()), unix_ms()));

        // Collection metadata version is unchanged, but index is dirty and must still flush.
        assert!(collection.flush(unix_ms()).await?);
    }

    // Reopen and verify the index-only change is durable.
    db.close().await?;
    let db = AndaDB::connect(
        db.object_store(),
        DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;

    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;

    let ids = collection
        .search_ids(Query {
            filter: Some(Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("Alice".to_string())),
            ))),
            ..Default::default()
        })
        .await?;
    assert!(ids.is_empty());

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_filter_by_field_result_ordering() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    for (name, age) in [("Alice", 30_u32), ("Bob", 25_u32), ("Charlie", 35_u32)] {
        let doc = create_test_doc(0, name, age, vec!["x"]);
        collection.add_from(&doc).await?;
    }

    // candidates 为空：两个方向的结果都按 id 正序返回
    let filter_all = Filter::Field((Schema::ID_KEY.to_string(), RangeQuery::Gt(Fv::U64(0))));
    let ids = collection.filter_by_field(filter_all.clone(), &[], 0, ScanOrder::Ascending)?;
    assert_eq!(ids, vec![1, 2, 3]);
    let ids = collection.filter_by_field(filter_all, &[], 0, ScanOrder::Descending)?;
    assert_eq!(ids, vec![1, 2, 3]);

    // candidates 非空：结果顺序应遵循 candidates 顺序（相关性），
    // 与请求的 id 方向无关
    let filter_subset = Filter::Field((Schema::ID_KEY.to_string(), RangeQuery::Ge(Fv::U64(2))));
    let ids = collection.filter_by_field(filter_subset, &[3, 1, 2], 0, ScanOrder::Ascending)?;
    assert_eq!(ids, vec![3, 2]);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_document_updates() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        // 创建索引以测试更新对索引的影响
        collection.create_btree_index_nx(&["name"]).await?;
        collection.create_btree_index_nx(&["age"]).await?;
        collection.create_btree_index_nx(&["tags"]).await?;
        Ok(())
    })
    .await?;

    // 添加文档
    let doc = create_test_doc(0, "Alice", 30, vec!["smart", "friendly"]);
    let doc_obj = Document::try_from(collection.schema(), &doc)?;
    let id = collection.add(doc_obj).await?;

    // 更新文档
    let mut update_fields = BTreeMap::new();
    update_fields.insert("name".to_string(), Fv::Text("Alice Updated".to_string()));
    update_fields.insert("age".to_string(), Fv::U64(31));
    update_fields.insert(
        "tags".to_string(),
        Fv::Array(vec![
            Fv::Text("smart".to_string()),
            Fv::Text("friendly".to_string()),
            Fv::Text("updated".to_string()),
        ]),
    );

    collection.update(id, update_fields.clone()).await?;

    // 获取并验证更新后的文档
    let updated_doc: TestDoc = collection.get_as(id).await?;
    assert_eq!(updated_doc.name, "Alice Updated");
    assert_eq!(updated_doc.age, 31);
    assert_eq!(updated_doc.tags.len(), 3);
    assert!(updated_doc.tags.contains(&"updated".to_string()));

    // 通过索引验证更新是否生效
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("Alice Updated".to_string())),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].age, 31);

    // 验证原来的值不再能被索引查询到
    let result: Vec<TestDoc> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("Alice".to_string())),
            ))),
            ..Default::default()
        })
        .await?;

    assert_eq!(result.len(), 0);

    // 测试部分更新
    let mut partial_update = BTreeMap::new();
    partial_update.insert("age".to_string(), Fv::U64(32));

    collection.update(id, partial_update).await?;

    let partially_updated: TestDoc = collection.get_as(id).await?;
    assert_eq!(partially_updated.name, "Alice Updated"); // 未更改
    assert_eq!(partially_updated.age, 32); // 已更改

    // 测试更新不存在的文档
    let result = collection.update(999, update_fields.clone()).await;
    assert!(result.is_err());

    // 测试只读模式下的更新失败
    collection.set_read_only(true);
    let result = collection.update(id, update_fields.clone()).await;
    assert!(result.is_err());

    // 恢复读写模式
    collection.set_read_only(false);

    // 测试更新元数据字段
    let mut metadata_update = BTreeMap::new();
    let mut metadata_map = BTreeMap::new();
    metadata_map.insert("key1".into(), Fv::Text("value1".to_string()));
    metadata_map.insert("key2".into(), Fv::U64(42));
    metadata_update.insert("metadata".into(), Fv::Map(metadata_map));

    collection.update(id, metadata_update).await?;

    let doc_with_metadata: TestDoc = collection.get_as(id).await?;
    assert_eq!(doc_with_metadata.metadata.len(), 2);
    assert!(
        matches!(doc_with_metadata.metadata.get("key1"), Some(Json::String(s)) if s == "value1")
    );
    assert!(
        matches!(doc_with_metadata.metadata.get("key2"), Some(Json::Number(n)) if n.as_i64() == Some(42))
    );

    // 验证统计信息已更新
    let stats = collection.stats();
    assert_eq!(stats.update_count, 3); // 初始更新 + 部分更新 + 元数据更新，只读失败不计数

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_extension_get_set_remove() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    // 初始状态：无扩展数据
    assert!(collection.get_extension("key1").is_none());
    assert!(collection.metadata().extensions.is_empty());

    // set_extension：设置后可以 get 到
    collection.set_extension("key1".into(), FieldValue::Text("hello".into()));
    assert_eq!(
        collection.get_extension("key1"),
        Some(FieldValue::Text("hello".into()))
    );

    // 支持不同类型
    collection.set_extension("count".into(), FieldValue::U64(42));
    collection.set_extension("flag".into(), FieldValue::Bool(true));
    assert_eq!(collection.get_extension("count"), Some(FieldValue::U64(42)));
    assert_eq!(
        collection.get_extension("flag"),
        Some(FieldValue::Bool(true))
    );

    // 覆盖已有 key
    collection.set_extension("key1".into(), FieldValue::I64(-1));
    assert_eq!(collection.get_extension("key1"), Some(FieldValue::I64(-1)));

    // metadata() 中也能看到 extensions
    let meta = collection.metadata();
    assert_eq!(meta.extensions.len(), 3);
    assert_eq!(meta.extensions.get("key1"), Some(&FieldValue::I64(-1)));

    // remove_extension：移除存在的 key
    let old = collection.remove_extension("count").await?;
    assert_eq!(old, Some(FieldValue::U64(42)));
    assert!(collection.get_extension("count").is_none());

    // remove_extension：移除不存在的 key 返回 None
    let old = collection.remove_extension("nonexistent").await?;
    assert!(old.is_none());

    db.close().await?;
    Ok(())
}

// Reconnects a fresh AndaDB over the same object store, so tests verify
// what was actually persisted instead of hitting the in-memory collection
// cache of the original AndaDB instance.
async fn reconnect_test_db(db: AndaDB) -> Result<AndaDB, DBError> {
    let object_store = db.object_store();
    db.close().await?;
    drop(db);
    AndaDB::connect(
        object_store,
        DBConfig {
            name: "test_db".to_string(),
            description: "Test database".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await
}

#[tokio::test]
async fn test_vector_get_field_is_canonical_after_reconnect() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;
    let source = create_test_doc(0, "vector", 1, vec![]);
    let expected = source.vector.clone();
    let id = collection.add_from(&source).await?;
    collection.flush(unix_ms()).await?;

    drop(collection);
    let db = reconnect_test_db(db).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    let document = collection.get(id).await?;
    assert_eq!(document.get_field("vector"), Some(&Fv::Vector(expected)));

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_extension_save_and_persist() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    // save_extension 会立即持久化
    collection
        .save_extension("persist_key".into(), FieldValue::Text("persisted".into()))
        .await?;
    assert_eq!(
        collection.get_extension("persist_key"),
        Some(FieldValue::Text("persisted".into()))
    );

    // 验证 last_saved 已被更新（save_extension 调用了 store_metadata）
    let stats = collection.stats();
    assert!(stats.last_saved > 0);

    // 重新连接数据库（绕过 AndaDB 的集合缓存），验证扩展数据真正落盘
    drop(collection);
    let db = reconnect_test_db(db).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;

    assert_eq!(
        collection.get_extension("persist_key"),
        Some(FieldValue::Text("persisted".into()))
    );

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_extension_flush_persist() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    // 使用 set_extension（不立即持久化），再 flush
    collection.set_extension("lazy_key".into(), FieldValue::Bytes(vec![1, 2, 3]));
    collection.flush(unix_ms()).await?;

    // 重新连接数据库（绕过 AndaDB 的集合缓存），验证扩展数据真正落盘。
    // 此前 set_extension 不递增元数据版本，flush 的快路径会跳过写盘，
    // 而旧测试从缓存拿到同一内存实例导致误通过。
    drop(collection);
    let db = reconnect_test_db(db).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;

    assert_eq!(
        collection.get_extension("lazy_key"),
        Some(FieldValue::Bytes(vec![1, 2, 3]))
    );

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_remove_extension_persists() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    collection
        .save_extension("k".into(), FieldValue::U64(7))
        .await?;
    let old = collection.remove_extension("k").await?;
    assert_eq!(old, Some(FieldValue::U64(7)));

    drop(collection);
    let db = reconnect_test_db(db).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert!(collection.get_extension("k").is_none());

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_collection_set_extension_with() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    let key = "test_key".to_string();

    // 1. Initial state: None
    let old = collection.set_extension_with(key.clone(), |val| {
        assert!(val.is_none());
        Some(FieldValue::U64(100))
    });
    assert!(old.is_none());
    assert_eq!(collection.get_extension(&key), Some(FieldValue::U64(100)));

    // 2. Update existing value: 100 -> 200
    let old = collection.set_extension_with(key.clone(), |val| {
        if let Some(FieldValue::U64(v)) = val {
            return Some(FieldValue::U64(v + 100));
        }
        None
    });
    assert_eq!(old, Some(FieldValue::U64(100)));
    assert_eq!(collection.get_extension(&key), Some(FieldValue::U64(200)));

    // 3. Return None: No change
    let old = collection.set_extension_with(key.clone(), |_| None);
    assert!(old.is_none());
    assert_eq!(collection.get_extension(&key), Some(FieldValue::U64(200)));

    db.close().await?;
    Ok(())
}

async fn count_objects(object_store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
    let mut stream = object_store.list(Some(&Path::from(prefix)));
    let mut count = 0;
    while let Some(item) = stream.next().await {
        item.expect("list should succeed");
        count += 1;
    }
    count
}

#[tokio::test]
async fn test_removed_index_can_be_recreated() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let object_store = db.object_store();
    let db_config = || DBConfig {
        name: "test_db".to_string(),
        description: "Test database".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };

    {
        let collection = create_test_collection(&db, async |collection| {
            collection.create_btree_index_nx(&["name"]).await?;
            collection.create_bm25_index_nx(&["name", "tags"]).await?;
            collection
                .create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

        for i in 1..=3u64 {
            let doc = create_test_doc(0, &format!("user_{i}"), 20 + i as u32, vec!["x"]);
            collection.add_from(&doc).await?;
        }
        db.close().await?;
    }

    // Remove the indexes; their storage objects must be deleted.
    let db = AndaDB::connect(object_store.clone(), db_config()).await?;
    let _ = db
        .open_collection("test_collection".to_string(), async |collection| {
            assert!(collection.remove_btree_index(&["name"]).await?);
            assert!(collection.remove_bm25_index(&["name", "tags"]).await?);
            assert!(collection.remove_hnsw_index("vector").await?);
            Ok(())
        })
        .await?;
    assert_eq!(
        count_objects(&object_store, "test_db/test_collection/btree_indexes").await,
        0
    );
    assert_eq!(
        count_objects(&object_store, "test_db/test_collection/bm25_indexes").await,
        0
    );
    assert_eq!(
        count_objects(&object_store, "test_db/test_collection/hnsw_indexes").await,
        0
    );
    db.close().await?;

    // Simulate leftover files from a crashed index creation: a stale meta
    // object must not block re-creation.
    object_store
        .put_opts(
            &Path::from("test_db/test_collection/btree_indexes/name/meta.cbor"),
            PutPayload::from(Bytes::from_static(b"stale")),
            PutOptions::default(),
        )
        .await
        .unwrap();

    // Re-creating the same indexes used to fail with AlreadyExists because
    // the old index files were left behind.
    let db = AndaDB::connect(object_store.clone(), db_config()).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |collection| {
            collection.create_btree_index(&["name"]).await?;
            collection.create_bm25_index(&["name", "tags"]).await?;
            collection
                .create_hnsw_index(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

    // Backfill repopulated the fresh indexes from the existing documents.
    let ids = collection
        .query_ids(
            Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("user_2".to_string())),
            )),
            None,
        )
        .await?;
    assert_eq!(ids.len(), 1);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_query_ids_with_huge_limit_does_not_overallocate() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["age"]).await?;
        Ok(())
    })
    .await?;

    for i in 1..=5u64 {
        let doc = create_test_doc(0, &format!("user_{i}"), 20 + i as u32, vec!["x"]);
        collection.add_from(&doc).await?;
    }

    // A huge limit must not pre-allocate a huge buffer up front (this
    // previously aborted with a capacity overflow via reserve_exact).
    let ids = collection
        .query_ids(
            Filter::Field(("age".to_string(), RangeQuery::Ge(Fv::U64(21)))),
            Some(usize::MAX),
        )
        .await?;
    assert_eq!(ids.len(), 5);

    let ids = collection
        .query_ids(
            Filter::Field((Schema::ID_KEY.to_string(), RangeQuery::Ge(Fv::U64(1)))),
            Some(usize::MAX),
        )
        .await?;
    assert_eq!(ids.len(), 5);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_search_with_lt_filter_keeps_most_relevant() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection
            .create_hnsw_index_nx(
                "vector",
                HnswConfig {
                    dimension: 10,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    })
    .await?;

    // 4 documents at increasing distance from the query vector, so the
    // relevance order of the candidates is deterministic: 1, 2, 3, 4.
    for (i, base) in [0.1f32, 0.2, 0.4, 0.8].into_iter().enumerate() {
        let mut doc = create_test_doc(0, &format!("doc{i}"), 20 + i as u32, vec!["x"]);
        doc.vector = std::iter::repeat_n(bf16::from_f32(base), 10).collect();
        collection.add_from(&doc).await?;
    }

    // Hybrid search + Lt filter matching everything, with more hits than
    // the limit: the retained results must be the MOST relevant ones
    // (head of the relevance-ordered candidates), not the tail.
    let ids = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(vec![0.1; 10]),
                ..Default::default()
            }),
            filter: Some(Filter::Field((
                Schema::ID_KEY.to_string(),
                RangeQuery::Lt(Fv::U64(100)),
            ))),
            limit: Some(2),
        })
        .await?;
    assert_eq!(ids, vec![1, 2]);

    // Pure filter queries keep the smallest ids, like `query_ids`: the end
    // is the method's contract, not something `Lt` decides.
    let ids = collection
        .search_ids(Query {
            filter: Some(Filter::Field((
                Schema::ID_KEY.to_string(),
                RangeQuery::Lt(Fv::U64(100)),
            ))),
            limit: Some(2),
            ..Default::default()
        })
        .await?;
    assert_eq!(ids, vec![1, 2]);

    // The largest ids are reached by asking for them.
    let ids = collection
        .query_last_ids(
            Filter::Field((Schema::ID_KEY.to_string(), RangeQuery::Lt(Fv::U64(100)))),
            Some(2),
        )
        .await?;
    assert_eq!(ids, vec![3, 4]);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_query_ids_equivalent_filters_return_the_same_page() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["age"]).await?;
        Ok(())
    })
    .await?;

    // age == _id == 1..=20
    for i in 1..=20u64 {
        let doc = create_test_doc(0, &format!("user_{i}"), i as u32, vec!["x"]);
        assert_eq!(collection.add_from(&doc).await?, i);
    }

    // 三种等价写法（5 <= x < 15），分别走 BTree 索引路径和 _id 快路径。
    // 截断端过去是从过滤条件反推的，导致 And 被误判为“保留尾部”，
    // 同一 limit 下三者返回互不相交的结果。
    for field in [Schema::ID_KEY, "age"] {
        let filters = [
            Filter::And(vec![
                Box::new(Filter::Field((
                    field.to_string(),
                    RangeQuery::Ge(Fv::U64(5)),
                ))),
                Box::new(Filter::Field((
                    field.to_string(),
                    RangeQuery::Lt(Fv::U64(15)),
                ))),
            ]),
            Filter::Field((
                field.to_string(),
                RangeQuery::Between(Fv::U64(5), Fv::U64(14)),
            )),
            Filter::Field((
                field.to_string(),
                RangeQuery::And(vec![
                    Box::new(RangeQuery::Ge(Fv::U64(5))),
                    Box::new(RangeQuery::Lt(Fv::U64(15))),
                ]),
            )),
        ];

        let full: Vec<DocumentId> = (5..=14).collect();
        for filter in filters {
            let ids = collection.query_ids(filter.clone(), Some(3)).await?;
            assert_eq!(ids, vec![5, 6, 7], "field {field}, filter {filter:?}");

            let ids = collection.query_ids(filter.clone(), None).await?;
            assert_eq!(ids, full, "field {field}, filter {filter:?}");

            // 纯过滤的 search_ids 走同一条路径，结果必须一致
            let ids = collection
                .search_ids(Query {
                    filter: Some(filter.clone()),
                    limit: Some(3),
                    ..Default::default()
                })
                .await?;
            assert_eq!(ids, vec![5, 6, 7], "field {field}, filter {filter:?}");
        }
    }

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_query_ids_or_pages_are_operand_order_independent() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["age"]).await?;
        Ok(())
    })
    .await?;

    // age == _id == 1..=20
    for i in 1..=20u64 {
        let doc = create_test_doc(0, &format!("user_{i}"), i as u32, vec!["x"]);
        assert_eq!(collection.add_from(&doc).await?, i);
    }

    // Or 各分支必须完整求值：过去 `_id` 快路径在并集超过 limit 时提前
    // break，同一布尔集合的两种操作数顺序返回不同的页面。
    for field in [Schema::ID_KEY, "age"] {
        let pairs = [
            (
                Filter::Field((
                    field.to_string(),
                    RangeQuery::Or(vec![
                        Box::new(RangeQuery::Gt(Fv::U64(10))),
                        Box::new(RangeQuery::Eq(Fv::U64(1))),
                    ]),
                )),
                Filter::Field((
                    field.to_string(),
                    RangeQuery::Or(vec![
                        Box::new(RangeQuery::Eq(Fv::U64(1))),
                        Box::new(RangeQuery::Gt(Fv::U64(10))),
                    ]),
                )),
            ),
            (
                Filter::Or(vec![
                    Box::new(Filter::Field((
                        field.to_string(),
                        RangeQuery::Gt(Fv::U64(10)),
                    ))),
                    Box::new(Filter::Field((
                        field.to_string(),
                        RangeQuery::Eq(Fv::U64(1)),
                    ))),
                ]),
                Filter::Or(vec![
                    Box::new(Filter::Field((
                        field.to_string(),
                        RangeQuery::Eq(Fv::U64(1)),
                    ))),
                    Box::new(Filter::Field((
                        field.to_string(),
                        RangeQuery::Gt(Fv::U64(10)),
                    ))),
                ]),
            ),
        ];
        for (a, b) in pairs {
            let ids_a = collection.query_ids(a.clone(), Some(2)).await?;
            let ids_b = collection.query_ids(b.clone(), Some(2)).await?;
            assert_eq!(ids_a, vec![1, 11], "field {field}, filter {a:?}");
            assert_eq!(ids_b, vec![1, 11], "field {field}, filter {b:?}");
        }
    }

    // Filter::Or 的分支过去以调用方 limit 求值：Le 分支的 B-tree 扫描
    // 是降序的，收集到的是最大键，升序截断后两端的匹配都会被丢掉
    // （此例中错误结果是 [8, 9, 10]）。
    let ids = collection
        .query_ids(
            Filter::Or(vec![
                Box::new(Filter::Field((
                    "age".to_string(),
                    RangeQuery::Le(Fv::U64(10)),
                ))),
                Box::new(Filter::Field((
                    "age".to_string(),
                    RangeQuery::Eq(Fv::U64(19)),
                ))),
            ]),
            Some(3),
        )
        .await?;
    assert_eq!(ids, vec![1, 2, 3]);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_filter_by_id_inverted_between_is_empty() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    for i in 1..=5u64 {
        let doc = create_test_doc(0, &format!("user_{i}"), i as u32, vec!["x"]);
        assert_eq!(collection.add_from(&doc).await?, i);
    }

    // 反转区间匹配空集（此前 BTreeSet::range 会直接 panic）
    let inverted = Filter::Field((
        Schema::ID_KEY.to_string(),
        RangeQuery::Between(Fv::U64(4), Fv::U64(2)),
    ));
    assert!(
        collection
            .query_ids(inverted.clone(), None)
            .await?
            .is_empty()
    );

    // 嵌套在复合查询里同样不能 panic
    let nested = Filter::Field((
        Schema::ID_KEY.to_string(),
        RangeQuery::Or(vec![
            Box::new(RangeQuery::Between(Fv::U64(4), Fv::U64(2))),
            Box::new(RangeQuery::Eq(Fv::U64(1))),
        ]),
    ));
    assert_eq!(collection.query_ids(nested, None).await?, vec![1]);

    let ids = collection
        .search_ids(Query {
            filter: Some(inverted),
            ..Default::default()
        })
        .await?;
    assert!(ids.is_empty());

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_filter_type_mismatch_returns_error() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["age"]).await?;
        Ok(())
    })
    .await?;
    collection
        .add_from(&create_test_doc(0, "Alice", 30, vec!["x"]))
        .await?;

    // A filter value whose type does not match the index key type is a
    // caller bug and must surface as an error, not an empty result.
    let err = collection
        .query_ids(
            Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::Text("30".into())))),
            Some(10),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DBError::Index { .. }));

    db.close().await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct DualVectorDoc {
    pub _id: u64,
    pub name: String,
    pub vec_a: Vector,
    pub vec_b: Vector,
}

#[tokio::test]
async fn test_search_with_multiple_hnsw_dimensions_and_missing_indexes() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = db
        .open_or_create_collection(
            DualVectorDoc::schema()?,
            CollectionConfig {
                name: "dual_vec".to_string(),
                description: "two vector fields".to_string(),
            },
            async |c| {
                c.create_hnsw_index_nx(
                    "vec_a",
                    HnswConfig {
                        dimension: 4,
                        ..Default::default()
                    },
                )
                .await?;
                c.create_hnsw_index_nx(
                    "vec_b",
                    HnswConfig {
                        dimension: 8,
                        ..Default::default()
                    },
                )
                .await?;
                Ok(())
            },
        )
        .await?;

    let id = collection
        .add_from(&DualVectorDoc {
            _id: 0,
            name: "a".into(),
            vec_a: std::iter::repeat_n(bf16::from_f32(0.1), 4).collect(),
            vec_b: std::iter::repeat_n(bf16::from_f32(0.2), 8).collect(),
        })
        .await?;

    // A 4-dim query searches only the matching index and succeeds even
    // though another index has a different dimension.
    let ids = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(vec![0.1; 4]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    assert_eq!(ids, vec![id]);

    // An 8-dim query hits the other index.
    let ids = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(vec![0.2; 8]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    assert_eq!(ids, vec![id]);

    // A query dimension matching no index is an error, not silence.
    let err = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(vec![0.3; 5]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, DBError::Index { .. }));

    // Text search without any BM25 index is an error, not silence.
    let err = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("a".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, DBError::Index { .. }));

    db.close().await?;
    Ok(())
}

/// Regression (#20): a hybrid text+vector query whose vector dimension
/// matches no HNSW index degrades to text-only results instead of
/// failing the whole query; a pure vector query still errors.
#[tokio::test]
async fn test_hybrid_search_degrades_when_vector_matches_no_index() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_bm25_index_nx(&["name"]).await?;
        collection
            .create_hnsw_index_nx(
                "vector",
                HnswConfig {
                    dimension: 10,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    })
    .await?;
    let id = collection
        .add_from(&create_test_doc(0, "alpha beta", 30, vec!["x"]))
        .await?;

    // Hybrid: the BM25 hit is kept, the mismatched vector part is
    // dropped with a warning.
    let ids = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("alpha".to_string()),
                vector: Some(vec![0.1; 5]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    assert_eq!(ids, vec![id]);

    // Pure vector misuse still surfaces as an error.
    let err = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(vec![0.1; 5]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, DBError::Index { .. }));

    db.close().await?;
    Ok(())
}

// Document with a signed counter, for the I64/U64 read-back regressions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, AndaDBSchema)]
struct CounterDoc {
    pub _id: u64,
    pub name: String,
    pub count: i64,
}

fn counter_config() -> CollectionConfig {
    CollectionConfig {
        name: "counters".to_string(),
        description: "signed counters".to_string(),
    }
}

/// Regression (#1/#9, cases a+b): a non-negative i64 field reads back
/// from storage as U64. `update` must retire the old B-tree entry (no
/// phantom match on the old value) and `remove` must not leak entries.
#[tokio::test]
async fn test_i64_btree_index_update_and_remove_after_read_back() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = db
        .open_or_create_collection(CounterDoc::schema()?, counter_config(), async |c| {
            c.create_btree_index_nx(&["count"]).await?;
            Ok(())
        })
        .await?;

    let id = collection
        .add_from(&CounterDoc {
            _id: 0,
            name: "a".to_string(),
            count: 5,
        })
        .await?;

    // (a) update: the old document is materialized from storage where
    // `count: 5` reads back as U64(5); the old index entry must still be
    // retired.
    collection
        .update(id, BTreeMap::from([("count".to_string(), Fv::I64(7))]))
        .await?;
    let stale = collection
        .query_ids(
            Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(5)))),
            Some(10),
        )
        .await?;
    assert!(
        stale.is_empty(),
        "stale index entry for the old value must be removed, got {stale:?}",
    );
    let current = collection
        .query_ids(
            Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(7)))),
            Some(10),
        )
        .await?;
    assert_eq!(current, vec![id]);

    // (b) remove: the index entry must be dropped, not leaked.
    assert!(collection.remove(id).await?.is_some());
    let leaked = collection
        .query_ids(
            Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(7)))),
            Some(10),
        )
        .await?;
    assert!(leaked.is_empty(), "removed document must leave no entries");
    assert_eq!(
        collection.get_btree_index(&["count"])?.stats().num_elements,
        0,
    );

    db.close().await?;
    Ok(())
}

/// Regression (#1/#9, case c): creating a B-tree index over existing
/// non-negative i64 data backfills from storage, where the values read
/// back as U64 — index creation must succeed and the index must be
/// queryable.
#[tokio::test]
async fn test_i64_btree_index_backfill_over_existing_data() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = db
        .open_or_create_collection(CounterDoc::schema()?, counter_config(), async |_| Ok(()))
        .await?;
    let mut ids = Vec::new();
    for (i, count) in [5i64, -2, 0].into_iter().enumerate() {
        ids.push(
            collection
                .add_from(&CounterDoc {
                    _id: 0,
                    name: format!("doc{i}"),
                    count,
                })
                .await?,
        );
    }
    collection.flush(unix_ms()).await?;
    drop(collection);
    db.close_collection("counters").await?;

    // Reopen from storage and build the index over the existing data.
    let collection = db
        .open_collection("counters".to_string(), async |c| {
            c.create_btree_index(&["count"]).await?;
            Ok(())
        })
        .await?;
    let found = collection
        .query_ids(
            Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(5)))),
            Some(10),
        )
        .await?;
    assert_eq!(found, vec![ids[0]]);
    let found = collection
        .query_ids(
            Filter::Field(("count".to_string(), RangeQuery::Eq(Fv::I64(-2)))),
            Some(10),
        )
        .await?;
    assert_eq!(found, vec![ids[1]]);

    db.close().await?;
    Ok(())
}

/// Regression (#18): `save_extension` / `remove_extension` persist with
/// a single metadata put ("Ok means persisted") and must not advance the
/// flush watermark — the next full flush still runs its complete path.
#[tokio::test]
async fn test_save_extension_single_put_without_claiming_flush() -> Result<(), DBError> {
    let object_store = Arc::new(InMemory::new());
    let db_config = DBConfig {
        name: "ext_db".to_string(),
        description: "extension persistence".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), db_config.clone()).await?;
    let collection = db
        .open_or_create_collection(CounterDoc::schema()?, counter_config(), async |_| Ok(()))
        .await?;

    let puts_before = collection.storage_stats().total_put_count;
    collection
        .save_extension("k".to_string(), Fv::Text("v".to_string()))
        .await?;
    assert_eq!(
        collection.storage_stats().total_put_count,
        puts_before + 1,
        "save_extension must perform exactly one metadata put",
    );

    // Persisted immediately: a second database instance reads it back
    // without any flush on the first one.
    {
        let db2 = AndaDB::connect(object_store.clone(), db_config.clone()).await?;
        let c2 = db2
            .open_collection("counters".to_string(), async |_| Ok(()))
            .await?;
        assert_eq!(c2.get_extension("k"), Some(Fv::Text("v".to_string())));
    }

    // The unclaimed write did not advance `last_saved_version`: the next
    // full flush still persists metadata + ids, and only then does the
    // fast path apply.
    assert!(collection.flush(unix_ms()).await?);
    assert!(!collection.flush(unix_ms()).await?);

    // remove_extension mirrors the single-put behaviour.
    let puts_before = collection.storage_stats().total_put_count;
    let old = collection.remove_extension("k").await?;
    assert_eq!(old, Some(Fv::Text("v".to_string())));
    assert_eq!(
        collection.storage_stats().total_put_count,
        puts_before + 1,
        "remove_extension must perform exactly one metadata put",
    );
    assert!(collection.get_extension("k").is_none());
    // Removing a missing key writes nothing.
    let puts_before = collection.storage_stats().total_put_count;
    assert!(collection.remove_extension("missing").await?.is_none());
    assert_eq!(collection.storage_stats().total_put_count, puts_before);

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_open_repair_scan_skips_unreadable_and_mismatched_documents() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;
    let id1 = collection
        .add_from(&create_test_doc(0, "Alice", 30, vec!["a"]))
        .await?;
    collection.flush(unix_ms()).await?; // persisted checkpoint = 1

    // id 2: corrupt CBOR object inside the repair scan window.
    collection
        .storage
        .put_bytes(
            &Collection::doc_path(2),
            Bytes::from_static(b"not valid cbor"),
            PutMode::Overwrite,
        )
        .await?;
    // id 3: valid CBOR that does not match the collection schema.
    let bad_doc = DocumentOwned {
        fields: BTreeMap::from([(0usize, Fv::Text("not an id".to_string()))]),
    };
    collection
        .storage
        .create(&Collection::doc_path(3), &bad_doc)
        .await?;
    // id 4: a valid orphan document, recoverable.
    let orphan = Document::try_from(
        collection.schema(),
        &create_test_doc(4, "Carol", 40, vec!["c"]),
    )?;
    collection
        .storage
        .create(&Collection::doc_path(4), &orphan)
        .await?;

    // Reopening must not fail on the corrupt or mismatched objects
    // (previously a schema mismatch bricked the whole collection open),
    // and the valid orphan behind them must still be recovered.
    drop(collection);
    let db = reconnect_test_db(db).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert!(collection.contains(id1));
    assert!(!collection.contains(2));
    assert!(!collection.contains(3));
    assert!(collection.contains(4));
    let recovered: TestDoc = collection.get_as(4).await?;
    assert_eq!(recovered.name, "Carol");

    db.close().await?;
    Ok(())
}

/// Regression (P0-04): F1 is held after serializing its old ids snapshot
/// but before the conditional ids PUT completes. An add and F2 queue
/// behind the collection-wide gate in that order. Once released, F1 must
/// publish its version, the add completes, and only then may F2 take the
/// new snapshot/version and advance the checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_flushes_bind_checkpoint_to_serialized_ids_generation()
-> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let gate = FaultGate::new();
    let gated = "test_collection/ids.cbor";
    let db_config = || DBConfig {
        name: "test_db".to_string(),
        description: String::new(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), db_config()).await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;
    assert_eq!(
        collection
            .add_from(&create_test_doc(0, "one", 20, vec!["one"]))
            .await?,
        1
    );

    pause_puts(&faults, gated, FaultKind::PauseBefore(gate.clone()));
    let first_flush = {
        let collection = collection.clone();
        tokio::spawn(async move { collection.flush(unix_ms()).await })
    };
    gate.wait_entered().await;

    // Queue the mutation before F2. Tokio's fair RwLock makes F2 observe
    // the completed mutation instead of overtaking it with a stale ids
    // snapshot and then treating a CAS conflict as success.
    let adding = {
        let collection = collection.clone();
        tokio::spawn(async move {
            collection
                .add_from(&create_test_doc(0, "two", 21, vec!["two"]))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second_flush = {
        let collection = collection.clone();
        tokio::spawn(async move { collection.flush(unix_ms()).await })
    };
    assert!(!adding.is_finished());
    assert!(!second_flush.is_finished());

    gate.release();
    assert!(first_flush.await.expect("first flush panicked")?);
    assert_eq!(adding.await.expect("add task panicked")?, 2);
    assert!(second_flush.await.expect("second flush panicked")?);
    assert!(collection.storage.stats().check_point >= 2);

    drop(collection);
    drop(db);
    let db = AndaDB::connect(object_store, db_config()).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(collection.ids(), vec![1, 2]);
    db.close().await?;
    Ok(())
}

/// Once collection metadata is durable, a failure in ids.cbor poisons the
/// handle: the checkpoint is a sequence of dependent writes and the
/// in-memory watermarks no longer describe what is durable. Reopening
/// converges from the WAL and the repair scan; no document is lost.
#[tokio::test]
async fn test_failed_ids_phase_poisons_handle_and_reopen_converges() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let config = DBConfig {
        name: "test_db".to_string(),
        description: String::new(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;
    collection
        .add_from(&create_test_doc(0, "one", 20, vec!["one"]))
        .await?;
    collection.flush(unix_ms()).await?;
    collection
        .add_from(&create_test_doc(0, "two", 21, vec!["two"]))
        .await?;

    let same_ms = unix_ms();
    faults.push_rule(FaultRule::fail_once(
        FaultOp::Put,
        "test_collection/ids.cbor",
    ));
    assert!(collection.flush(same_ms).await.is_err());
    assert!(collection.is_poisoned());
    // Every further operation on the poisoned handle is rejected.
    let err = collection
        .flush(same_ms)
        .await
        .expect_err("poisoned handle must reject flush");
    assert!(err.is_poisoned(), "{err:?}");
    assert_eq!(err.collection_state(), Some(CollectionState::Poisoned));
    assert!(
        collection
            .add_from(&create_test_doc(0, "three", 22, vec!["three"]))
            .await
            .is_err()
    );

    // Reopening through the same database discards the poisoned handle
    // (without flushing it) and loads a fresh generation. The WAL replay
    // recovers document 2 and the flush inside `open_collection` already
    // persists the converged checkpoint and retires the WAL.
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(collection.ids(), vec![1, 2]);
    assert!(collection.pending_mutations.lock().is_empty());
    let mut intents = collection
        .storage
        .list_meta(Some(Collection::MUTATION_INTENT_PREFIX), None);
    assert!(intents.next().await.is_none(), "WAL retired after reopen");
    assert!(collection.storage.stats().check_point >= 2);

    drop(collection);
    db.close().await?;
    let db = AndaDB::connect(object_store, config).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(collection.ids(), vec![1, 2]);
    db.close().await?;
    Ok(())
}

/// A conditional metadata PUT can commit before its future reports
/// success. Aborting at that boundary poisons the handle (cancellation is
/// treated as a crash); reopening loads the durable state and the
/// retained WAL converges ids and indexes without losing the document.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancelled_metadata_put_poisons_handle_and_reopen_converges() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let gate = FaultGate::new();
    let gated = "test_collection/meta.cbor";
    let config = DBConfig {
        name: "test_db".to_string(),
        description: String::new(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["name"]).await
    })
    .await?;
    let id = collection
        .add_from(&create_test_doc(0, "one", 20, vec!["one"]))
        .await?;
    assert_eq!(id, 1);

    pause_puts(&faults, gated, FaultKind::PauseAfter(gate.clone()));
    let first_now = unix_ms();
    let flushing = {
        let collection = collection.clone();
        tokio::spawn(async move { collection.flush(first_now).await })
    };
    gate.wait_entered().await;

    flushing.abort();
    assert!(
        flushing
            .await
            .expect_err("flush should be cancelled")
            .is_cancelled()
    );
    gate.release();
    assert!(collection.is_poisoned());

    // Every further operation on the poisoned handle is rejected.
    let err = collection
        .save_extension("after_cancel".to_string(), Fv::Text("durable".to_string()))
        .await
        .expect_err("poisoned handle must reject writes");
    assert!(err.is_poisoned(), "{err:?}");
    assert_eq!(err.collection_state(), Some(CollectionState::Poisoned));

    // Reopening through the same database discards the poisoned handle
    // without flushing it. The watermark-bounded repair scan recovers the
    // committed document; a full flush then completes a checkpoint.
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(collection.ids(), vec![id]);
    collection.flush(unix_ms()).await?;
    let mut intents = collection
        .storage
        .list_meta(Some(Collection::MUTATION_INTENT_PREFIX), None);
    assert!(intents.next().await.is_none(), "WAL retired after reopen");
    assert!(collection.storage.stats().check_point >= id);

    drop(collection);
    db.close().await?;
    let db = AndaDB::connect(object_store, config).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(collection.ids(), vec![id]);
    let reopened: TestDoc = collection.get_as(id).await?;
    assert_eq!(reopened.name, "one");
    assert!(collection.storage.stats().check_point >= id);
    assert!(collection.pending_mutations.lock().is_empty());
    db.close().await?;
    Ok(())
}

/// An immediate metadata-only write has the same post-commit cancellation
/// window as a full flush: aborting it poisons the handle. The committed
/// extension write is durable and visible after a reopen; the unclaimed
/// write never publishes the full-flush watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancelled_unclaimed_metadata_put_poisons_handle() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let gate = FaultGate::new();
    let gated = "test_collection/meta.cbor";
    let config = DBConfig {
        name: "test_db".to_string(),
        description: String::new(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;
    let saved_before = collection.last_saved_version.load(Ordering::Acquire);

    pause_puts(&faults, gated, FaultKind::PauseAfter(gate.clone()));
    let saving = {
        let collection = collection.clone();
        tokio::spawn(async move {
            collection
                .save_extension("before_cancel".to_string(), Fv::Text("durable".to_string()))
                .await
        })
    };
    gate.wait_entered().await;
    saving.abort();
    assert!(
        saving
            .await
            .expect_err("metadata-only writer should be cancelled after commit")
            .is_cancelled()
    );
    gate.release();
    assert!(collection.is_poisoned());
    assert_eq!(
        collection.last_saved_version.load(Ordering::Acquire),
        saved_before,
        "an unclaimed generation must not publish the full-flush watermark",
    );
    assert!(
        collection
            .add_from(&create_test_doc(0, "after", 21, vec!["after"]))
            .await
            .is_err(),
        "poisoned handle must reject mutations",
    );

    // Reopen through the same database: the committed extension write is
    // durable and visible in the fresh generation, which stays writable.
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(
        collection.get_extension("before_cancel"),
        Some(Fv::Text("durable".to_string()))
    );
    let id = collection
        .add_from(&create_test_doc(0, "after", 21, vec!["after"]))
        .await?;
    assert!(collection.flush(unix_ms()).await?);
    assert!(collection.pending_mutations.lock().is_empty());
    assert!(collection.storage.stats().check_point >= id);

    drop(collection);
    db.close().await?;
    let db = AndaDB::connect(object_store, config).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(collection.ids(), vec![id]);
    assert_eq!(
        collection.get_extension("before_cancel"),
        Some(Fv::Text("durable".to_string()))
    );
    let reopened: TestDoc = collection.get_as(id).await?;
    assert_eq!(reopened.name, "after");
    assert!(collection.storage.stats().check_point >= id);
    db.close().await?;
    Ok(())
}

/// A second writer replacing the metadata object makes the next flush
/// fail with `Precondition` and poisons the handle: single-writer
/// violations are never reconciled in place.
#[tokio::test]
async fn test_foreign_metadata_writer_poisons_handle() -> Result<(), DBError> {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: "conflict_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;
    collection
        .add_from(&create_test_doc(0, "one", 20, vec!["one"]))
        .await?;
    collection.flush(unix_ms()).await?;

    // Simulate a second writer bumping the durable metadata object.
    let (mut foreign, _) = collection
        .storage
        .fetch::<CollectionMetadata>(Collection::METADATA_PATH)
        .await?;
    foreign.config.description = "foreign writer".to_string();
    collection
        .storage
        .put(Collection::METADATA_PATH, &foreign, None)
        .await?;

    collection
        .add_from(&create_test_doc(0, "two", 21, vec!["two"]))
        .await?;
    let err = collection
        .flush(unix_ms())
        .await
        .expect_err("stale CAS token must remain a conflict");
    assert!(matches!(err, DBError::Precondition { .. }));
    assert!(collection.is_poisoned());
    Ok(())
}

/// An object-store PUT may commit before the caller observes its result.
/// Cancelling `add` in that interval poisons the handle; the durable WAL
/// intent makes the reopened generation recover the committed document
/// instead of skipping it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancelled_add_poisons_handle_and_reopen_recovers_document() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let gate = FaultGate::new();
    pause_puts(&faults, "data/1.cbor", FaultKind::PauseAfter(gate.clone()));
    let config = DBConfig {
        name: "test_db".to_string(),
        description: String::new(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["name"]).await
    })
    .await?;

    let adding = {
        let collection = collection.clone();
        tokio::spawn(async move {
            collection
                .add_from(&create_test_doc(0, "committed", 20, vec!["x"]))
                .await
        })
    };
    gate.wait_entered().await;
    adding.abort();
    assert!(
        adding
            .await
            .expect_err("add should be cancelled")
            .is_cancelled()
    );
    gate.release();
    assert!(collection.is_poisoned());
    assert!(
        collection
            .add_from(&create_test_doc(0, "second", 21, vec!["y"]))
            .await
            .is_err(),
        "poisoned handle must reject mutations",
    );

    // Reopen through the same database: WAL replay recovers the committed
    // document and the id allocator, so the next add gets a fresh id.
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(
        collection
            .add_from(&create_test_doc(0, "second", 21, vec!["y"]))
            .await?,
        2
    );
    collection.flush(unix_ms()).await?;
    assert_eq!(collection.ids(), vec![1, 2]);
    assert_eq!(
        collection
            .query_ids(
                Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("committed".to_string())),
                )),
                Some(10),
            )
            .await?,
        vec![1]
    );

    drop(collection);
    drop(db);
    let db = AndaDB::connect(object_store, config).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert_eq!(collection.ids(), vec![1, 2]);
    assert_eq!(collection.get_as::<TestDoc>(1).await?.name, "committed");
    db.close().await?;
    Ok(())
}

/// A cancelled update can leave its proposed in-memory index value
/// applied while the document PUT is still blocked. The handle is
/// poisoned; the WAL stores both before/after values so the reopened
/// generation removes that phantom.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancelled_update_poisons_handle_and_reopen_removes_phantom() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let gate = FaultGate::new();
    let gated = "data/1.cbor";
    let db = AndaDB::connect(
        object_store,
        DBConfig {
            name: "test_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;
    let collection = create_test_collection(&db, async |collection| {
        collection.create_btree_index_nx(&["name"]).await
    })
    .await?;
    let id = collection
        .add_from(&create_test_doc(0, "before", 20, vec!["x"]))
        .await?;
    collection.flush(unix_ms()).await?;

    pause_puts(&faults, gated, FaultKind::PauseBefore(gate.clone()));
    let updating = {
        let collection = collection.clone();
        tokio::spawn(async move {
            collection
                .update(
                    id,
                    BTreeMap::from([("name".to_string(), Fv::Text("cancelled".to_string()))]),
                )
                .await
        })
    };
    gate.wait_entered().await;
    updating.abort();
    assert!(
        updating
            .await
            .expect_err("update should be cancelled")
            .is_cancelled()
    );
    gate.release();
    assert!(collection.is_poisoned());
    assert!(
        collection.flush(unix_ms()).await.is_err(),
        "poisoned handle must reject flush",
    );

    // The reopened generation replays the WAL: both historical values are
    // removed and the document still present in storage is re-indexed.
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    collection.flush(unix_ms()).await?;
    let before = collection
        .query_ids(
            Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("before".to_string())),
            )),
            Some(10),
        )
        .await?;
    let cancelled = collection
        .query_ids(
            Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("cancelled".to_string())),
            )),
            Some(10),
        )
        .await?;
    assert_eq!(before, vec![id]);
    assert!(cancelled.is_empty());
    db.close().await?;
    Ok(())
}

/// Recovery must wait until the open callback installs custom hooks.
/// Replaying once with default derivation would persist raw `After`
/// B-tree/BM25 entries that the custom hook cannot subsequently identify.
#[tokio::test]
async fn test_mutation_replay_uses_custom_hooks_before_clearing_intent() -> Result<(), DBError> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = DBConfig {
        name: "custom_replay_db".to_string(),
        description: String::new(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), config.clone()).await?;
    let collection = db
        .create_collection(
            TestDoc::schema()?,
            CollectionConfig {
                name: "documents".to_string(),
                description: String::new(),
            },
            async |collection| {
                collection.set_index_hooks(Arc::new(RecoveryCustomHooks));
                collection.create_btree_index_nx(&["name"]).await?;
                collection.create_bm25_index_nx(&["name"]).await?;
                Ok(())
            },
        )
        .await?;
    let id = collection
        .add_from(&create_test_doc(0, "Before", 20, vec!["x"]))
        .await?;
    collection.flush(unix_ms()).await?;
    collection
        .update(
            id,
            BTreeMap::from([("name".to_string(), Fv::Text("After".to_string()))]),
        )
        .await?;
    drop(collection);
    drop(db);

    let db = AndaDB::connect(object_store, config).await?;
    let collection = db
        .open_collection("documents".to_string(), async |collection| {
            collection.set_index_hooks(Arc::new(RecoveryCustomHooks));
            Ok(())
        })
        .await?;
    let raw = collection
        .query_ids(
            Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("After".to_string())),
            )),
            Some(10),
        )
        .await?;
    let hooked = collection
        .query_ids(
            Filter::Field((
                "name".to_string(),
                RangeQuery::Eq(Fv::Text("hook:after".to_string())),
            )),
            Some(10),
        )
        .await?;
    assert!(raw.is_empty(), "default-hook B-tree value must not survive");
    assert_eq!(hooked, vec![id]);

    let raw_text = collection
        .get_bm25_index(&["name"])?
        .search("after", 10, None);
    let hooked_text = collection
        .get_bm25_index(&["name"])?
        .search("hooktoken", 10, None);
    assert!(raw_text.iter().all(|(found, _)| *found != id));
    assert!(hooked_text.iter().any(|(found, _)| *found == id));
    db.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_flush_drains_in_flight_add_before_checkpoint() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let gate = FaultGate::new();
    pause_puts(&faults, "data/2.cbor", FaultKind::PauseBefore(gate.clone()));
    let db_config = || DBConfig {
        name: "test_db".to_string(),
        description: "Test database".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store.clone(), db_config()).await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;

    let id1 = collection
        .add_from(&create_test_doc(0, "a", 20, vec!["x"]))
        .await?;
    assert_eq!(id1, 1);
    collection.flush(unix_ms()).await?;

    // Start an add whose document write (data/2.cbor) is blocked,
    // simulating an add still in flight while a flush runs.
    let blocked = {
        let collection = collection.clone();
        tokio::spawn(async move {
            collection
                .add_from(&create_test_doc(0, "b", 21, vec!["y"]))
                .await
        })
    };
    // Wait until the blocked add has allocated its id.
    while collection.max_document_id() < 2 {
        tokio::task::yield_now().await;
    }

    // A third add completes fully and bumps the metadata version so the
    // flush below has something to persist.
    let id3 = collection
        .add_from(&create_test_doc(0, "c", 22, vec!["z"]))
        .await?;
    assert_eq!(id3, 3);

    // A complete flush now owns the collection operation gate
    // exclusively. It must wait for add(2), rather than checkpointing a
    // bitmap/index snapshot while that mutation is only half complete.
    let flushing = {
        let collection = collection.clone();
        tokio::spawn(async move { collection.flush(unix_ms()).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!flushing.is_finished(), "flush must drain the active add");

    // Unblock the in-flight add, let the serialized flush checkpoint the
    // complete state, then simulate a crash and reopen.
    gate.release();
    let id2 = blocked.await.expect("add task panicked")?;
    assert_eq!(id2, 2);
    assert!(flushing.await.expect("flush task panicked")?);
    assert!(collection.storage.stats().check_point >= 3);
    drop(collection);
    drop(db);

    let db = AndaDB::connect(object_store, db_config()).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert!(collection.contains(1));
    assert!(
        collection.contains(2),
        "document written by the in-flight add must be recovered by the repair scan"
    );
    assert!(collection.contains(3));

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_mutation_intents_replay_update_and_remove_after_crash() -> Result<(), DBError> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db_config = || DBConfig {
        name: "mutation_replay_db".to_string(),
        description: String::new(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };

    let db = AndaDB::connect(object_store.clone(), db_config()).await?;
    let collection = db
        .create_collection(
            TestDoc::schema()?,
            CollectionConfig {
                name: "documents".to_string(),
                description: String::new(),
            },
            async |c| {
                c.create_btree_index_nx(&["name"]).await?;
                c.create_bm25_index_nx(&["name"]).await?;
                c.create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
                Ok(())
            },
        )
        .await?;

    let mut first = create_test_doc(0, "before", 20, vec!["old"]);
    first.vector = vec![bf16::from_f32(0.0); 10];
    let first_id = collection.add_from(&first).await?;
    let mut second = create_test_doc(0, "other", 21, vec!["other"]);
    second.vector = vec![bf16::from_f32(10.0); 10];
    let second_id = collection.add_from(&second).await?;
    collection.flush(unix_ms()).await?;

    // The document PUT succeeds, but no index flush follows before the
    // process disappears. The retained intent must make reopen converge
    // every index to the new document rather than the old checkpoint.
    collection
        .update(
            first_id,
            BTreeMap::from([
                ("name".to_string(), Fv::Text("middle".to_string())),
                (
                    "vector".to_string(),
                    Fv::Vector(vec![bf16::from_f32(15.0); 10]),
                ),
            ]),
        )
        .await?;
    collection
        .update(
            first_id,
            BTreeMap::from([
                ("name".to_string(), Fv::Text("after".to_string())),
                (
                    "vector".to_string(),
                    Fv::Vector(vec![bf16::from_f32(20.0); 10]),
                ),
            ]),
        )
        .await?;
    assert!(!collection.pending_mutations.lock().is_empty());
    drop(collection);
    drop(db);

    let db = AndaDB::connect(object_store.clone(), db_config()).await?;
    let collection = db
        .open_collection("documents".to_string(), async |_| Ok(()))
        .await?;

    let by_name = |name: &str| Query {
        filter: Some(Filter::Field((
            "name".to_string(),
            RangeQuery::Eq(Fv::Text(name.to_string())),
        ))),
        ..Default::default()
    };
    assert!(collection.search_ids(by_name("before")).await?.is_empty());
    assert!(collection.search_ids(by_name("middle")).await?.is_empty());
    assert_eq!(
        collection.search_ids(by_name("after")).await?,
        vec![first_id]
    );

    let old_text = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("before".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    assert!(!old_text.contains(&first_id));
    let new_text = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("after".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    assert!(new_text.contains(&first_id));

    let hnsw = collection.get_hnsw_index("vector")?;
    assert_eq!(hnsw.try_search(&[0.0; 10], 1)?[0].0, second_id);
    assert_eq!(hnsw.try_search(&[20.0; 10], 1)?[0].0, first_id);
    assert!(collection.pending_mutations.lock().is_empty());

    // Exercise the other terminal state: a remove whose object delete is
    // durable while the derived indexes are not yet flushed.
    collection.remove(first_id).await?;
    assert!(!collection.pending_mutations.lock().is_empty());
    drop(collection);
    drop(db);

    let db = AndaDB::connect(object_store, db_config()).await?;
    let collection = db
        .open_collection("documents".to_string(), async |_| Ok(()))
        .await?;
    assert!(!collection.contains(first_id));
    assert!(collection.search_ids(by_name("after")).await?.is_empty());
    let removed_text = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("after".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    assert!(!removed_text.contains(&first_id));
    assert!(
        !collection
            .get_hnsw_index("vector")?
            .try_search(&[20.0; 10], 2)?
            .iter()
            .any(|(id, _)| *id == first_id)
    );
    assert!(collection.pending_mutations.lock().is_empty());

    db.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_close_drains_update_and_old_handle_cannot_reenable() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let gate = FaultGate::new();
    let gated = "data/1.cbor";
    let config = DBConfig {
        name: "close_drain_db".to_string(),
        description: String::new(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = AndaDB::connect(object_store, config).await?;
    let old = db
        .create_collection(
            TestDoc::schema()?,
            CollectionConfig {
                name: "documents".to_string(),
                description: String::new(),
            },
            async |_| Ok(()),
        )
        .await?;

    let id = old
        .add_from(&create_test_doc(0, "before", 20, vec!["x"]))
        .await?;
    pause_puts(&faults, gated, FaultKind::PauseBefore(gate.clone()));
    let updating = {
        let collection = old.clone();
        tokio::spawn(async move {
            collection
                .update(
                    id,
                    BTreeMap::from([("name".to_string(), Fv::Text("after".to_string()))]),
                )
                .await
        })
    };
    gate.wait_entered().await;

    let closing = {
        let db = db.clone();
        tokio::spawn(async move { db.close_collection("documents").await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !closing.is_finished(),
        "close must wait for admitted update"
    );

    // Cancelling close must not create an empty registry slot. Opening the
    // same name takes over the retiring handle and remains blocked until
    // its admitted update is drained and flushed.
    closing.abort();
    assert!(
        closing
            .await
            .expect_err("close should be cancelled")
            .is_cancelled()
    );
    let opening = {
        let db = db.clone();
        tokio::spawn(async move {
            db.open_or_create_collection(
                TestDoc::schema()?,
                CollectionConfig {
                    name: "documents".to_string(),
                    description: String::new(),
                },
                async |_| Ok(()),
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !opening.is_finished(),
        "open must finish the cancelled close before loading a fresh handle"
    );

    gate.release();
    let updated = updating.await.expect("update task panicked")?;
    assert_eq!(updated.get_field("name"), Some(&Fv::Text("after".into())));
    let fresh = opening.await.expect("open task panicked")?;
    assert_eq!(fresh.len(), 1);
    let persisted: TestDoc = fresh.get_as(id).await?;
    assert_eq!(persisted.name, "after");
    assert!(!Arc::ptr_eq(&old, &fresh));

    // The user-controlled read-only flag is reversible only while the
    // handle's lifecycle lease is active. The retired Arc must never
    // become a second writer over the same prefix.
    old.set_read_only(false);
    assert!(
        old.add_from(&create_test_doc(0, "zombie", 21, vec!["z"]))
            .await
            .is_err()
    );
    assert!(
        old.save_extension("zombie".to_string(), Fv::Bool(true))
            .await
            .is_err()
    );
    old.set_extension("zombie".to_string(), Fv::Bool(true));
    assert!(old.get_extension("zombie").is_none());
    assert!(old.flush(unix_ms()).await.is_err());

    db.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_drains_add_before_prefix_removal() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();
    let gate = FaultGate::new();
    pause_puts(&faults, "data/1.cbor", FaultKind::PauseBefore(gate.clone()));
    let db = AndaDB::connect(
        object_store,
        DBConfig {
            name: "delete_drain_db".to_string(),
            description: String::new(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await?;
    let old = db
        .create_collection(
            TestDoc::schema()?,
            CollectionConfig {
                name: "documents".to_string(),
                description: String::new(),
            },
            async |_| Ok(()),
        )
        .await?;
    let adding = {
        let collection = old.clone();
        tokio::spawn(async move {
            collection
                .add_from(&create_test_doc(0, "deleted", 20, vec!["x"]))
                .await
        })
    };
    while old.max_document_id() == 0 {
        tokio::task::yield_now().await;
    }

    let deleting = {
        let db = db.clone();
        tokio::spawn(async move { db.delete_collection("documents").await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !deleting.is_finished(),
        "delete must drain the active add before listing the prefix"
    );
    deleting.abort();
    assert!(
        deleting
            .await
            .expect_err("delete should be cancelled")
            .is_cancelled()
    );
    assert!(
        db.open_collection("documents".to_string(), async |_| Ok(()))
            .await
            .is_err(),
        "the deletion tombstone must block open after cancellation"
    );
    let deleting = {
        let db = db.clone();
        tokio::spawn(async move { db.delete_collection("documents").await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !deleting.is_finished(),
        "retry must take over and continue draining the retained handle"
    );
    gate.release();
    adding.await.expect("add task panicked")?;
    deleting.await.expect("delete task panicked")?;

    let fresh = db
        .create_collection(
            TestDoc::schema()?,
            CollectionConfig {
                name: "documents".to_string(),
                description: String::new(),
            },
            async |_| Ok(()),
        )
        .await?;
    assert!(
        fresh.is_empty(),
        "deleted add must leave no residual object"
    );
    old.set_read_only(false);
    assert!(
        old.add_from(&create_test_doc(0, "zombie", 21, vec!["z"]))
            .await
            .is_err()
    );

    db.close().await?;
    Ok(())
}

// ---------------------------------------------------------------------
// Regression tests for audited defects
// ---------------------------------------------------------------------

fn test_db_config() -> DBConfig {
    DBConfig {
        name: "test_db".to_string(),
        description: "Test database".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    }
}

async fn connect_test_db(object_store: Arc<dyn ObjectStore>) -> Result<AndaDB, DBError> {
    AndaDB::connect(object_store, test_db_config()).await
}

/// A newly created index must be durable **before** the collection
/// metadata that registers it. Publishing the registration first left the
/// index permanently empty on the next start: bootstrap loaded the empty
/// durable index, `create_bm25_index_nx` swallowed `AlreadyExists`, and the
/// repair scan only covers ids above the storage checkpoint.
#[tokio::test]
async fn test_flush_persists_index_before_registering_it() -> Result<(), DBError> {
    let (object_store, faults) = fault_store();

    let db = connect_test_db(object_store.clone()).await?;
    let collection = create_test_collection(&db, async |_| Ok(())).await?;
    for i in 0..20u32 {
        collection
            .add_from(&create_test_doc(0, &format!("doc-{i}"), 20 + i, vec!["t"]))
            .await?;
    }
    assert_eq!(collection.len(), 20);
    db.close().await?;
    drop(db);

    // Reopen and create the BM25 index: the backfill only exists in
    // memory until a flush persists it. The index bucket write fails.
    faults.push_rule(FaultRule {
        times: u64::MAX,
        ..FaultRule::fail_once(FaultOp::Put, "bm25_indexes/name/b_")
    });
    let db = connect_test_db(object_store.clone()).await?;
    let opened = db
        .open_collection("test_collection".to_string(), async |c| {
            c.create_bm25_index_nx(&["name"]).await?;
            Ok(())
        })
        .await;
    assert!(
        opened.is_err(),
        "the failing index flush must fail the open that created it"
    );
    drop(db);

    // Next start: whatever is durable must not claim to be a usable index.
    faults.reset();
    let db = connect_test_db(object_store.clone()).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |c| {
            c.create_bm25_index_nx(&["name"]).await?;
            Ok(())
        })
        .await?;
    assert_eq!(collection.len(), 20);
    let hits = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("doc-3".to_string()),
                ..Default::default()
            }),
            limit: Some(20),
            ..Default::default()
        })
        .await?;
    assert!(
        !hits.is_empty(),
        "the BM25 index must be backfilled instead of silently staying empty"
    );

    db.close().await?;
    Ok(())
}

/// A single unusable retained intent must not make the collection
/// permanently unopenable: nothing clears it, so every reopen would fail
/// identically with no operator escape hatch.
#[tokio::test]
async fn test_open_tolerates_unusable_mutation_intents() -> Result<(), DBError> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = connect_test_db(object_store.clone()).await?;
    let collection = create_test_collection(&db, async |c| {
        c.create_btree_index_nx(&["age"]).await?;
        Ok(())
    })
    .await?;
    let id = collection
        .add_from(&create_test_doc(0, "alice", 30, vec!["a"]))
        .await?;
    collection.flush(unix_ms()).await?;

    // (a) An intent object that no longer decodes at all.
    collection
        .storage
        .put_bytes(
            &Collection::mutation_intent_path(1),
            Bytes::from_static(b"not a mutation intent"),
            PutMode::Overwrite,
        )
        .await?;

    // (b) A decodable intent whose recorded pre-image no longer satisfies
    // the schema — what a crash plus a later schema change produces.
    let name_idx = collection
        .schema()
        .get_field("name")
        .expect("name field")
        .idx();
    let mut previous: DocumentOwned = Document::try_from(
        collection.schema(),
        &create_test_doc(id, "alice", 30, vec!["a"]),
    )?
    .into();
    previous.fields.insert(name_idx, Fv::U64(7));
    assert!(
        Document::try_from_doc(collection.schema(), previous.clone()).is_err(),
        "the pre-image must be undecodable for this test to mean anything"
    );
    let intent = MutationIntent {
        purge_by_id: false,
        sequence: 2,
        document_id: id,
        previous: Some(previous),
        proposed: None,
    };
    collection
        .storage
        .create(&Collection::mutation_intent_path(2), &intent)
        .await?;

    // (c) A decodable intent that targets the reserved document id.
    let reserved = MutationIntent {
        purge_by_id: false,
        sequence: 3,
        document_id: 0,
        previous: None,
        proposed: None,
    };
    collection
        .storage
        .create(&Collection::mutation_intent_path(3), &reserved)
        .await?;

    // (d) A phantom posting a partial flush left behind under a key only
    // the (undecodable) pre-image knew about. Value-keyed removal cannot
    // reach it; the replay must sweep it out by id.
    for index in &collection.btree_indexes {
        if index.name() == "age" {
            assert!(index.insert(id, &Fv::U64(99), unix_ms())?);
        }
    }

    db.close().await?;
    drop(db);

    let db = connect_test_db(object_store.clone()).await?;
    let collection = db
        .open_collection("test_collection".to_string(), async |_| Ok(()))
        .await?;
    assert!(
        collection.stale_mutation_intents.lock().is_empty(),
        "the successful open checkpoint must retire unusable records"
    );
    assert_eq!(collection.len(), 1);
    let doc: TestDoc = collection.get_as(id).await?;
    assert_eq!(doc.name, "alice");
    assert_eq!(
        collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::U64(30)))),
                None,
            )
            .await?,
        vec![id]
    );
    // The phantom posting from (d) must be gone: the replay swept the id
    // out of every index before re-indexing the stored document.
    assert_eq!(
        collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::U64(99)))),
                None,
            )
            .await?,
        Vec::<DocumentId>::new()
    );

    db.close().await?;
    assert!(collection.stale_mutation_intents.lock().is_empty());
    let mut objects = object_store.list(None);
    while let Some(meta) = objects.next().await {
        let meta = meta.map_err(DBError::from)?;
        assert!(
            !meta
                .location
                .as_ref()
                .contains(Collection::MUTATION_INTENT_PREFIX),
            "stale mutation intent survived a successful checkpoint: {}",
            meta.location
        );
    }
    Ok(())
}

/// Which end of the match set a page keeps must depend only on the method
/// called, never on the filter's shape. 0.11.0 derived it from the scan
/// direction, so `_id Lt cursor` returned the newest ids while the same
/// predicate inside an `And` returned the oldest — cursor pagination built
/// on the bare form silently walked backwards off the first page as soon
/// as a second condition was added.
#[tokio::test]
async fn test_page_end_is_the_methods_contract_not_the_filters_shape() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |c| {
        c.create_btree_index_nx(&["name"]).await?;
        c.create_btree_index_nx(&["age"]).await?;
        Ok(())
    })
    .await?;

    // ids 1..=10, all sharing one `name` so the composite filter matches
    // exactly what the bare `_id` filter matches.
    for i in 1..=10u64 {
        let doc = create_test_doc(0, "alice", 20 + i as u32, vec!["x"]);
        assert_eq!(collection.add_from(&doc).await?, i);
    }

    let bare = Filter::Field((Schema::ID_KEY.to_string(), RangeQuery::Lt(Fv::U64(100))));
    let composite = Filter::And(vec![
        Box::new(Filter::Field((
            "name".to_string(),
            RangeQuery::Eq(Fv::Text("alice".to_string())),
        ))),
        Box::new(bare.clone()),
    ]);

    // Same predicate, two shapes, same page — in both directions.
    for filter in [bare.clone(), composite.clone()] {
        assert_eq!(
            collection.query_ids(filter.clone(), Some(3)).await?,
            vec![1, 2, 3],
            "query_ids must keep the smallest ids for {filter:?}"
        );
        assert_eq!(
            collection.query_last_ids(filter.clone(), Some(3)).await?,
            vec![8, 9, 10],
            "query_last_ids must keep the largest ids for {filter:?}"
        );
    }

    // Newest-first cursor pagination walks the whole set without gaps or
    // repeats, which is what the shape-dependent end broke downstream.
    let mut seen = Vec::new();
    let mut cursor = collection.max_document_id() + 1;
    loop {
        let page = collection
            .query_last_ids(
                Filter::And(vec![
                    Box::new(Filter::Field((
                        "name".to_string(),
                        RangeQuery::Eq(Fv::Text("alice".to_string())),
                    ))),
                    Box::new(Filter::Field((
                        Schema::ID_KEY.to_string(),
                        RangeQuery::Lt(Fv::U64(cursor)),
                    ))),
                ]),
                Some(4),
            )
            .await?;
        if page.is_empty() {
            break;
        }
        cursor = page[0];
        seen.extend(page.iter().rev().copied());
    }
    assert_eq!(seen, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);

    // A bounded `Not` also honors the requested end.
    let complement = Filter::Not(Box::new(Filter::Field((
        "age".to_string(),
        RangeQuery::Between(Fv::U64(21), Fv::U64(28)),
    ))));
    assert_eq!(
        collection.query_ids(complement.clone(), Some(1)).await?,
        vec![9]
    );
    assert_eq!(
        collection.query_last_ids(complement, Some(1)).await?,
        vec![10]
    );

    db.close().await?;
    Ok(())
}

/// `query_all_ids` must return every match: in-process callers (cascade
/// deletion, link re-pointing, `NOT` evaluation) corrupt data when a
/// result is silently truncated, which is exactly what `query_ids`'s
/// `MAX_SEARCH_LIMIT` clamp does to them.
#[tokio::test]
async fn test_query_all_ids_is_unbounded() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |c| {
        c.create_btree_index_nx(&["age"]).await?;
        Ok(())
    })
    .await?;

    let n = Collection::MAX_SEARCH_LIMIT + 100;
    for i in 0..n {
        collection
            .add_from(&create_test_doc(0, &format!("u{i}"), 1, vec![]))
            .await?;
    }

    let filter = Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::U64(1))));
    assert_eq!(
        collection.query_ids(filter.clone(), None).await?.len(),
        Collection::MAX_SEARCH_LIMIT
    );
    assert_eq!(collection.query_all_ids(filter).await?.len(), n);

    db.close().await?;
    Ok(())
}

/// The unbounded field scan collects into a sorted vector: an id posted
/// under several keys of an array index must still come back once.
#[tokio::test]
async fn test_query_all_ids_dedups_array_postings() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection =
        create_test_collection(&db, async |c| c.create_btree_index_nx(&["tags"]).await).await?;
    let a = collection
        .add_from(&create_test_doc(0, "a", 1, vec!["x", "y", "z"]))
        .await?;
    let b = collection
        .add_from(&create_test_doc(0, "b", 1, vec!["y"]))
        .await?;

    let filter = Filter::Field(("tags".to_string(), RangeQuery::Ge(Fv::Text("x".into()))));
    assert_eq!(collection.query_all_ids(filter.clone()).await?, vec![a, b]);
    assert_eq!(
        collection.query_ids(filter.clone(), Some(1)).await?,
        vec![a]
    );
    assert_eq!(collection.query_last_ids(filter, Some(1)).await?, vec![b]);

    db.close().await?;
    Ok(())
}

/// Intents are written from borrowed documents and read back as
/// `MutationIntent`; both forms must produce identical bytes.
#[test]
fn test_mutation_intent_ref_encodes_like_the_owned_intent() {
    let doc = Document::try_from(
        Arc::new(TestDoc::schema().unwrap()),
        &create_test_doc(7, "a", 1, vec!["x"]),
    )
    .unwrap();
    for (previous, proposed) in [(Some(&doc), Some(&doc)), (Some(&doc), None), (None, None)] {
        let purge_by_id = previous.is_none() && proposed.is_none();
        let owned = MutationIntent {
            sequence: 3,
            document_id: 7,
            previous: previous.map(|d| d.clone().into()),
            proposed: proposed.map(|d| d.clone().into()),
            purge_by_id,
        };
        let borrowed = MutationIntentRef {
            sequence: 3,
            document_id: 7,
            previous,
            proposed,
            purge_by_id,
        };
        let (mut a, mut b) = (Vec::new(), Vec::new());
        cbor2::to_writer(&owned, &mut a).unwrap();
        cbor2::to_writer(&borrowed, &mut b).unwrap();
        assert_eq!(a, b);
    }
}

/// Legacy data a later validation tightening rejects must stay
/// manageable in-band: `search` skips such a document instead of failing
/// the whole batch, and `remove` deletes it via an id sweep even though
/// its values cannot drive value-keyed index cleanup. `get` stays strict.
#[tokio::test]
async fn test_schema_invalid_stored_document_is_skippable_and_removable() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |c| {
        c.create_btree_index_nx(&["age"]).await?;
        Ok(())
    })
    .await?;

    let id_ok = collection
        .add_from(&create_test_doc(0, "alice", 30, vec!["a"]))
        .await?;
    let id_bad = collection
        .add_from(&create_test_doc(0, "bob", 40, vec!["b"]))
        .await?;

    // Overwrite the stored object with a raw image whose `name` is not
    // Text — the shape 0.10 write paths could persist before validation
    // covered it.
    let name_idx = collection
        .schema()
        .get_field("name")
        .expect("name field")
        .idx();
    let mut raw: DocumentOwned = Document::try_from(
        collection.schema(),
        &create_test_doc(id_bad, "bob", 40, vec!["b"]),
    )?
    .into();
    raw.fields.insert(name_idx, Fv::U64(7));
    collection
        .storage
        .put(&Collection::doc_path(id_bad), &raw, None)
        .await?;

    // Reads of the poisoned id stay strict.
    assert!(collection.get_as::<TestDoc>(id_bad).await.is_err());

    // A search matching both documents returns the healthy one instead
    // of failing the whole batch.
    let docs = collection
        .search(Query {
            filter: Some(Filter::Field((
                "age".to_string(),
                RangeQuery::Ge(Fv::U64(0)),
            ))),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].id(), id_ok);

    // `remove` deletes it (no recoverable pre-image, so it reports None)
    // and sweeps its postings, so the id stops matching queries.
    assert!(collection.remove(id_bad).await?.is_none());
    assert_eq!(collection.len(), 1);
    assert_eq!(
        collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::U64(40)))),
                None,
            )
            .await?,
        Vec::<DocumentId>::new()
    );

    db.close().await?;
    Ok(())
}

/// Neither a multi-key posting scan nor a caller-supplied `Include` list
/// may emit the same document id twice: duplicates make `search` return
/// the same document repeatedly and consume the caller's `limit`.
#[tokio::test]
async fn test_filter_results_are_deduplicated() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |c| {
        c.create_btree_index_nx(&["tags"]).await?;
        Ok(())
    })
    .await?;

    let id = collection
        .add_from(&create_test_doc(0, "alice", 30, vec!["x", "y", "z"]))
        .await?;

    // The document is indexed under three keys; a range scan visits all
    // three postings.
    let tags_filter = Filter::Field((
        "tags".to_string(),
        RangeQuery::Between(Fv::Text("a".to_string()), Fv::Text("zz".to_string())),
    ));
    assert_eq!(
        collection.query_ids(tags_filter.clone(), None).await?,
        vec![id]
    );
    let docs = collection
        .search(Query {
            filter: Some(tags_filter.clone()),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert_eq!(docs.len(), 1);

    // A `limit` must count distinct documents, not postings.
    let second = collection
        .add_from(&create_test_doc(0, "bob", 31, vec!["x", "y", "z"]))
        .await?;
    assert_eq!(
        collection.query_ids(tags_filter, Some(2)).await?,
        vec![id, second]
    );

    // Duplicated keys in an `Include` list yield one hit each, matching
    // `anda_db_btree`'s own `Include` semantics.
    assert_eq!(
        collection
            .query_ids(
                Filter::Field((
                    "_id".to_string(),
                    RangeQuery::Include(vec![
                        Fv::U64(id),
                        Fv::U64(id),
                        Fv::U64(second),
                        Fv::U64(id),
                    ]),
                )),
                None,
            )
            .await?,
        vec![id, second]
    );

    db.close().await?;
    Ok(())
}

/// `create_hnsw_index_nx` must not silently keep an index whose persisted
/// configuration differs from the requested one.
#[tokio::test]
async fn test_create_hnsw_index_nx_rejects_changed_config() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = create_test_collection(&db, async |c| {
        c.create_hnsw_index_nx(
            "vector",
            HnswConfig {
                dimension: 10,
                ..Default::default()
            },
        )
        .await?;
        // Same configuration: still idempotent.
        c.create_hnsw_index_nx(
            "vector",
            HnswConfig {
                dimension: 10,
                ..Default::default()
            },
        )
        .await?;

        let err = c
            .create_hnsw_index_nx(
                "vector",
                HnswConfig {
                    dimension: 4,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a changed configuration must not be discarded");
        assert!(matches!(err, DBError::Index { .. }), "{err:?}");

        // A non-dimension change must be reported too.
        let err = c
            .create_hnsw_index_nx(
                "vector",
                HnswConfig {
                    dimension: 10,
                    ef_search: 999,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a changed configuration must not be discarded");
        assert!(matches!(err, DBError::Index { .. }), "{err:?}");
        Ok(())
    })
    .await?;

    // The existing index is untouched and still accepts its own vectors.
    assert_eq!(collection.get_hnsw_index("vector")?.dimension(), 10);
    collection
        .add_from(&create_test_doc(0, "alice", 30, vec!["a"]))
        .await?;

    db.close().await?;
    Ok(())
}

/// `PutMode::Create` reporting `AlreadyExists` means this add wrote
/// nothing and the object belongs to someone else. The compensating
/// delete must not run, or it destroys a committed document.
#[tokio::test]
async fn test_add_conflict_does_not_delete_the_existing_document() -> Result<(), DBError> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let db_a = connect_test_db(object_store.clone()).await?;
    let collection_a = create_test_collection(&db_a, async |_| Ok(())).await?;
    // Publish the collection so the second writer can open it while its
    // own `max_document_id` is still 0.
    db_a.flush().await?;

    let db_b = connect_test_db(object_store.clone()).await?;
    let collection_b = create_test_collection(&db_b, async |_| Ok(())).await?;
    assert_eq!(collection_b.max_document_id(), 0);

    let id = collection_a
        .add_from(&create_test_doc(0, "alice", 30, vec!["a"]))
        .await?;
    assert_eq!(id, 1);
    collection_a.flush(unix_ms()).await?;

    // The second writer allocates the same id and loses the create race.
    let err = collection_b
        .add_from(&create_test_doc(0, "bob", 31, vec!["b"]))
        .await
        .expect_err("the conflicting add must fail");
    assert!(matches!(err, DBError::AlreadyExists { .. }), "{err:?}");

    // Writer A's document must still be there.
    let stored = object_store
        .head(&Path::from(format!(
            "test_db/test_collection/{}",
            Collection::doc_path(id)
        )))
        .await;
    assert!(
        stored.is_ok(),
        "the losing add must not delete the winner's document: {stored:?}"
    );
    let doc: TestDoc = collection_a.get_as(id).await?;
    assert_eq!(doc.name, "alice");

    db_a.close().await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, AndaDBSchema)]
struct UniqueNameDoc {
    pub _id: u64,
    #[unique]
    pub name: String,
    pub body: String,
    pub vector: Vector,
}

fn unique_name_doc(name: &str, vector: [f32; 4]) -> UniqueNameDoc {
    UniqueNameDoc {
        _id: 0,
        name: name.to_string(),
        body: format!("{name} keeps a journal"),
        vector: vector.into_iter().map(bf16::from_f32).collect(),
    }
}

/// Dropping a dead id from the bitmap is only half the repair: the index
/// postings survive, so a unique key stays occupied forever and queries
/// keep returning the dead id.
#[tokio::test]
async fn test_reconcile_storage_purges_index_postings_of_dead_ids() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let collection = db
        .open_or_create_collection(
            UniqueNameDoc::schema()?,
            CollectionConfig {
                name: "unique_docs".to_string(),
                description: String::new(),
            },
            async |c| {
                c.create_btree_index_nx(&["name"]).await?;
                c.create_bm25_index_nx(&["body"]).await?;
                c.create_hnsw_index_nx(
                    "vector",
                    HnswConfig {
                        dimension: 4,
                        ..Default::default()
                    },
                )
                .await?;
                Ok(())
            },
        )
        .await?;

    let alice = collection
        .add_from(&unique_name_doc("alice", [1.0, 0.0, 0.0, 0.0]))
        .await?;
    collection
        .add_from(&unique_name_doc("bob", [0.0, 1.0, 0.0, 0.0]))
        .await?;
    collection.flush(unix_ms()).await?;

    // Simulate a crash that lost the document object.
    collection
        .storage
        .delete(&Collection::doc_path(alice))
        .await?;

    let (recovered, dropped) = collection.reconcile_storage().await?;
    assert_eq!((recovered, dropped), (0, 1));
    assert!(!collection.contains(alice));

    // The B-tree posting must be gone: queries no longer return the dead
    // id, and the unique key is free again.
    assert!(
        collection
            .query_ids(
                Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("alice".to_string())),
                )),
                None,
            )
            .await?
            .is_empty()
    );

    // The BM25 postings must be gone too. They are keyed by the document's
    // tokens, which the lost body no longer supplies, so the index has to
    // be swept by id — otherwise a full-text search keeps resurrecting the
    // dead id (`search_ids` returns BM25 hits unfiltered).
    let text_hits = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("alice".to_string()),
                ..Default::default()
            }),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert!(text_hits.is_empty(), "{text_hits:?}");

    let reborn = collection
        .add_from(&unique_name_doc("alice", [1.0, 0.0, 0.0, 0.0]))
        .await?;
    assert_ne!(reborn, alice);

    // The reborn document is the only text hit; the dead id stays gone.
    let text_hits = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("alice".to_string()),
                ..Default::default()
            }),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert_eq!(text_hits, vec![reborn]);

    // The HNSW posting must be gone as well.
    let hits = collection
        .search_ids(Query {
            search: Some(Search {
                vector: Some(vec![1.0, 0.0, 0.0, 0.0]),
                ..Default::default()
            }),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert!(!hits.contains(&alice), "{hits:?}");

    // Surviving documents keep their postings.
    assert_eq!(
        collection
            .query_ids(
                Filter::Field((
                    "name".to_string(),
                    RangeQuery::Eq(Fv::Text("bob".to_string())),
                )),
                None,
            )
            .await?
            .len(),
        1
    );
    let bob_hits = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("bob".to_string()),
                ..Default::default()
            }),
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert_eq!(bob_hits.len(), 1, "{bob_hits:?}");

    db.close().await?;
    Ok(())
}

/// A B-tree index on the primary key can never answer a query: the filter
/// dispatcher serves `_id` from the collection's own id index.
#[tokio::test]
async fn test_btree_index_on_primary_key_is_rejected() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    create_test_collection(&db, async |c| {
        let err = c
            .create_btree_index(&["_id"])
            .await
            .expect_err("an unreachable index must be rejected");
        assert!(matches!(err, DBError::Schema { .. }), "{err:?}");
        // `_nx` must not swallow it either.
        assert!(c.create_btree_index_nx(&["_id"]).await.is_err());
        Ok(())
    })
    .await?;

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn oversized_legacy_history_can_reindex_and_update_status_but_not_be_newly_written()
-> Result<(), DBError> {
    #[derive(Clone, Serialize, Deserialize, AndaDBSchema)]
    struct History {
        _id: u64,
        status: String,
        messages: Json,
    }
    let db = setup_test_db().await?;
    let config = CollectionConfig {
        name: "legacy_history".into(),
        description: String::new(),
    };
    let collection = db
        .open_or_create_collection(History::schema()?, config.clone(), async |_| Ok(()))
        .await?;
    let id = collection
        .add_from(&History {
            _id: 0,
            status: "running".into(),
            messages: serde_json::json!([]),
        })
        .await?;
    collection.flush(unix_ms()).await?;
    let messages = serde_json::json!(vec![vec!["original history"; 50]; 400]);
    let mut stored: DocumentOwned = collection.get(id).await?.into();
    stored.fields.insert(
        collection.schema().get_field("messages").unwrap().idx(),
        Fv::Json(messages.clone()),
    );
    // Reproduce bytes an older writer accepted; do not relax current admission.
    collection
        .storage
        .put(&Collection::doc_path(id), &stored, None)
        .await?;
    db.close_collection("legacy_history").await?;
    let mut schema = History::schema()?;
    schema.with_version(1);
    let collection = db
        .open_or_create_collection(schema, config, async |c| {
            c.create_btree_index_nx(&["status"]).await?;
            Ok(())
        })
        .await?;
    let read: History = collection.get_as(id).await?;
    assert_eq!(read.messages, messages);
    collection
        .update(
            id,
            BTreeMap::from([("status".into(), Fv::Text("completed".into()))]),
        )
        .await?;
    assert_eq!(
        collection
            .query_all_ids(Filter::Field((
                "status".into(),
                RangeQuery::Eq(Fv::Text("completed".into()))
            )))
            .await?,
        vec![id]
    );
    assert!(collection.add_from(&read).await.is_err());
    assert!(collection.add(collection.get(id).await?).await.is_err());
    assert!(
        collection
            .update(
                id,
                BTreeMap::from([("messages".into(), Fv::Json(messages.clone()))])
            )
            .await
            .is_err()
    );
    db.close_collection("legacy_history").await?;
    let collection = db
        .open_collection("legacy_history".into(), async |_| Ok(()))
        .await?;
    let read: History = collection.get_as(id).await?;
    assert_eq!(read.status, "completed");
    assert_eq!(read.messages, messages);
    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn nested_multi_index_pages_remain_ordered_after_deletion() -> Result<(), DBError> {
    let db = setup_test_db().await?;
    let c = create_test_collection(&db, async |c| {
        c.create_btree_index_nx(&["age"]).await?;
        c.create_btree_index_nx(&["name"]).await
    })
    .await?;
    let mut ids = Vec::new();
    for _ in 0..20 {
        ids.push(c.add_from(&create_test_doc(0, "same", 30, vec![])).await?);
    }
    let page = || {
        Filter::And(vec![
            Box::new(Filter::And(vec![
                Box::new(Filter::Field(("age".into(), RangeQuery::Eq(Fv::U64(30))))),
                Box::new(Filter::Field((
                    "name".into(),
                    RangeQuery::Eq(Fv::Text("same".into())),
                ))),
            ])),
            Box::new(Filter::Field((
                "_id".into(),
                RangeQuery::Gt(Fv::U64(ids[4])),
            ))),
        ])
    };
    assert_eq!(c.query_ids(page(), Some(3)).await?, ids[5..8]);
    let (_, stats) = c.query_ids_with_stats(page(), Some(3)).await?;
    assert_eq!(stats.posting_ids + stats.bitmap_ids, 3);
    assert_eq!(stats.membership_probes, 6);
    assert_eq!(stats.ordered_snapshot_ids, 0);
    assert_eq!(stats.returned_ids, 3);
    c.remove(ids[5]).await?;
    assert_eq!(c.query_ids(page(), Some(3)).await?, ids[6..9]);
    assert_eq!(c.query_last_ids(page(), Some(3)).await?, ids[17..20]);
    assert!(
        c.query_ids(
            Filter::And(vec![Box::new(page()), Box::new(Filter::And(vec![]))]),
            Some(3)
        )
        .await?
        .is_empty()
    );
    c.flush(unix_ms()).await?;
    db.close().await?;
    Ok(())
}
