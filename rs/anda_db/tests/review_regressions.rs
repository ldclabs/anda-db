//! Regression coverage for the 2026-09-06 core review.
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::{AndaDB, DBConfig},
    error::{CollectionState, DBError},
    index::{HnswConfig, jieba_tokenizer, virtual_searchable_text},
    query::{Filter, Query, RangeQuery, Search},
    schema::{AndaDBSchema, Document, Fv, Vector, vector_from_f32},
    storage::{Storage, StorageConfig},
    unix_ms,
};
use anda_object_store::{FaultGate, FaultKind, FaultOp, FaultRule, FaultStore};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Doc {
    _id: u64,
    #[unique]
    key: String,
    body: String,
    embedding: Vector,
}
fn doc(key: &str, body: &str) -> Doc {
    Doc {
        _id: 0,
        key: key.into(),
        body: body.into(),
        embedding: vector_from_f32(vec![1., 2.]),
    }
}
fn cfg() -> DBConfig {
    DBConfig {
        name: "reviewdb".into(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn cc() -> CollectionConfig {
    CollectionConfig {
        name: "docs".into(),
        ..Default::default()
    }
}
fn eq(key: &str) -> Filter {
    Filter::Field(("key".into(), RangeQuery::Eq(Fv::Text(key.into()))))
}
fn fail_callback() -> DBError {
    DBError::Generic {
        name: "review".into(),
        source: "callback failed after initialization".into(),
    }
}
async fn plain(db: &AndaDB) -> Arc<Collection> {
    db.open_or_create_collection(Doc::schema().unwrap(), cc(), async |_| Ok(()))
        .await
        .unwrap()
}

#[tokio::test]
async fn unique_key_is_reserved_until_document_commit() {
    let (store, faults) = FaultStore::wrap(InMemory::new());
    let db = AndaDB::connect(Arc::new(store), cfg()).await.unwrap();
    let c = db
        .open_or_create_collection(Doc::schema().unwrap(), cc(), async |c| {
            c.create_btree_index(&["key"]).await
        })
        .await
        .unwrap();
    let a = c.add_from(&doc("x", "first")).await.unwrap();
    c.flush(unix_ms()).await.unwrap();
    let gate = FaultGate::new();
    faults.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: Some("data/1.cbor".into()),
        skip: 0,
        times: 1,
        kind: FaultKind::PauseBefore(gate.clone()),
    });
    let writer = c.clone();
    let updating = tokio::spawn(async move {
        writer
            .update(a, BTreeMap::from([("key".into(), Fv::Text("y".into()))]))
            .await
    });
    gate.wait_entered().await;
    let writer = c.clone();
    let mut adding = tokio::spawn(async move { writer.add_from(&doc("x", "second")).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut adding)
            .await
            .is_err(),
        "the old key must stay reserved while its document PUT is pending"
    );
    updating.abort();
    assert!(updating.await.unwrap_err().is_cancelled());
    assert!(adding.await.unwrap().unwrap_err().is_poisoned());
    let fresh = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(fresh.ids(), vec![a]);
    assert_eq!(fresh.query_all_ids(eq("x")).await.unwrap(), vec![a]);
    fresh
        .update(a, BTreeMap::from([("key".into(), Fv::Text("y".into()))]))
        .await
        .unwrap();
    assert!(fresh.add_from(&doc("x", "second")).await.is_ok());
}

#[tokio::test]
async fn regression_extension_publishes_unflushed_new_index() {
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = plain(&db).await;
    let id = c.add_from(&doc("x", "text")).await.unwrap();
    db.close_collection("docs").await.unwrap();
    let result = db
        .open_collection("docs".into(), async |c| {
            c.create_btree_index(&["key"]).await?;
            c.save_extension("initialized".into(), Fv::Bool(true))
                .await?;
            Err(fail_callback())
        })
        .await;
    assert!(result.is_err());
    let c = db
        .open_collection("docs".into(), async |c| {
            c.create_btree_index_nx(&["key"]).await
        })
        .await
        .unwrap();
    assert!(c.contains(id));
    assert_eq!(c.query_all_ids(eq("x")).await.unwrap(), vec![id]);
}

#[tokio::test]
async fn btree_nx_propagates_backfill_conflicts() {
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = plain(&db).await;
    c.add_from(&doc("same", "one")).await.unwrap();
    c.add_from(&doc("same", "two")).await.unwrap();
    db.close_collection("docs").await.unwrap();
    let result = db
        .open_collection("docs".into(), async |c| {
            c.create_btree_index_nx(&["key"]).await
        })
        .await;
    assert!(matches!(result, Err(DBError::AlreadyExists { .. })));
}

#[tokio::test]
async fn recovery_read_failure_does_not_advance_checkpoint() {
    let (store, faults) = FaultStore::wrap(InMemory::new());
    let store = Arc::new(store);
    {
        let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
        let c = plain(&db).await;
        c.add_from(&doc("a", "one")).await.unwrap();
        c.add_from(&doc("b", "two")).await.unwrap();
    }
    faults.push_rule(FaultRule::fail_once(FaultOp::Get, "data/1.cbor"));
    let db = AndaDB::connect(store, cfg()).await.unwrap();
    assert!(
        db.open_collection("docs".into(), async |_| Ok(()))
            .await
            .is_err()
    );
    let c = plain(&db).await;
    assert_eq!(c.ids(), vec![1, 2]);
    assert_eq!(c.reconcile_storage().await.unwrap(), (0, 0));
}

#[tokio::test]
async fn accepted_large_document_can_be_updated_and_recovered() {
    let store = Arc::new(InMemory::new());
    {
        let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
        let c = plain(&db).await;
        let id = c.add_from(&doc("a", &"z".repeat(1_100_000))).await.unwrap();
        c.flush(unix_ms()).await.unwrap();
        c.update(id, BTreeMap::from([("key".into(), Fv::Text("b".into()))]))
            .await
            .unwrap();
    }
    let db = AndaDB::connect(store, cfg()).await.unwrap();
    let c = plain(&db).await;
    assert_eq!(c.get_as::<Doc>(1).await.unwrap().key, "b");
    assert!(c.remove(1).await.unwrap().is_some());
}

#[tokio::test]
async fn regression_custom_tokenizer_not_restored_to_loaded_bm25() {
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = db
        .open_or_create_collection(Doc::schema().unwrap(), cc(), async |c| {
            c.set_tokenizer(jieba_tokenizer());
            c.create_bm25_index_nx(&["body"]).await
        })
        .await
        .unwrap();
    c.add_from(&doc("a", "南京市长江大桥")).await.unwrap();
    let q = Query {
        search: Some(Search {
            text: Some("南京市长江大桥".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(c.search_ids(q.clone()).await.unwrap().len(), 1);
    db.close_collection("docs").await.unwrap();
    let c = db
        .open_collection("docs".into(), async |c| {
            c.set_tokenizer(jieba_tokenizer());
            c.create_bm25_index_nx(&["body"]).await
        })
        .await
        .unwrap();
    assert!(c.tokenize("南京市长江大桥").len() > 1);
    assert_eq!(c.search_ids(q).await.unwrap(), vec![1]);
    c.update(
        1,
        BTreeMap::from([("body".into(), Fv::Text("上海交通大学".into()))]),
    )
    .await
    .unwrap();
    assert_eq!(
        c.search_ids(Query {
            search: Some(Search {
                text: Some("上海交通大学".into()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap(),
        vec![1]
    );
    c.remove(1).await.unwrap();
    assert!(
        c.get_bm25_index(&["body"])
            .unwrap()
            .search("上海交通大学", 10, None)
            .is_empty()
    );
}

#[tokio::test]
async fn regression_open_callback_add_runs_before_allocator_recovery() {
    let store = Arc::new(InMemory::new());
    {
        let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
        plain(&db).await.add_from(&doc("a", "one")).await.unwrap();
    }
    let db = AndaDB::connect(store, cfg()).await.unwrap();
    let result = db
        .open_collection("docs".into(), async |c| {
            c.add_from(&doc("b", "two")).await?;
            Ok(())
        })
        .await;
    let c = result.unwrap();
    assert_eq!(c.ids(), vec![1, 2]);
    assert_eq!(c.get_as::<Doc>(1).await.unwrap().key, "a");
    assert_eq!(c.get_as::<Doc>(2).await.unwrap().key, "b");
}

#[tokio::test]
async fn regression_dead_id_removal_has_no_replay_record() {
    let store = Arc::new(InMemory::new());
    {
        let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
        let c = db
            .open_or_create_collection(Doc::schema().unwrap(), cc(), async |c| {
                c.create_btree_index(&["key"]).await
            })
            .await
            .unwrap();
        c.add_from(&doc("a", "one")).await.unwrap();
        c.flush(unix_ms()).await.unwrap();
        store
            .delete(&Path::from("reviewdb/docs/data/1.cbor"))
            .await
            .unwrap();
        assert!(c.remove(1).await.unwrap().is_none());
        assert!(!c.contains(1));
        // No flush: the successful maintenance removal needs crash recovery.
    }
    let db = AndaDB::connect(store, cfg()).await.unwrap();
    let c = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    assert!(c.query_all_ids(eq("a")).await.unwrap().is_empty());
    assert!(c.add_from(&doc("a", "replacement")).await.is_ok());
}

#[tokio::test]
async fn regression_read_only_open_deletes_orphan_hnsw_blobs() {
    let store = Arc::new(InMemory::new());
    let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
    db.open_or_create_collection(Doc::schema().unwrap(), cc(), async |c| {
        c.create_hnsw_index(
            "embedding",
            HnswConfig {
                dimension: 2,
                ..Default::default()
            },
        )
        .await
    })
    .await
    .unwrap();
    db.close_collection("docs").await.unwrap();
    let orphan = Path::from("reviewdb/docs/hnsw_indexes/embedding/n_999.cbor");
    store.put(&orphan, vec![0u8].into()).await.unwrap();
    db.set_read_only(true);
    db.open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    assert!(
        store.head(&orphan).await.is_ok(),
        "read-only open must preserve orphan storage objects"
    );
}

#[tokio::test]
async fn regression_unknown_extension_commit_does_not_poison() {
    let (store, faults) = FaultStore::wrap(InMemory::new());
    let db = AndaDB::connect(Arc::new(store), cfg()).await.unwrap();
    let c = plain(&db).await;
    faults.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: Some("docs/meta.cbor".into()),
        skip: 0,
        times: 1,
        kind: FaultKind::ErrorAfter,
    });
    assert!(c.save_extension("x".into(), Fv::Bool(true)).await.is_err());
    assert_eq!(c.state(), CollectionState::Poisoned);
    let same = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    assert!(
        !Arc::ptr_eq(&c, &same),
        "reopen must replace the poisoned generation"
    );
    same.save_extension("y".into(), Fv::Bool(true))
        .await
        .unwrap();
}

#[tokio::test]
async fn regression_stream_writer_reader_rejects_compressible_roundtrip() {
    let storage = Storage::connect(
        "stream_review".into(),
        Arc::new(InMemory::new()),
        StorageConfig {
            compress_level: 3,
            max_small_object_size: 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut writer = storage.stream_writer("data");
    writer.write_all(&vec![b'x'; 64 * 1024]).await.unwrap();
    writer.shutdown().await.unwrap();
    let mut reader = storage.stream_reader("data").await.unwrap();
    let mut output = Vec::new();
    reader.read_to_end(&mut output).await.unwrap();
    assert_eq!(output, vec![b'x'; 64 * 1024]);
}

#[tokio::test]
async fn regression_bm25_compaction_same_bucket_count_does_not_flush() {
    use anda_db::index::{BM25, default_tokenizer};
    let storage = Storage::connect(
        "compact_review".into(),
        Arc::new(InMemory::new()),
        StorageConfig {
            compress_level: 0,
            bucket_overload_size: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let index = BM25::new(vec!["body".into()], default_tokenizer(), storage.clone(), 1)
        .await
        .unwrap();
    for (id, text) in ["aaaa", "bbbb", "cccc", "dddd"].into_iter().enumerate() {
        index.insert(id as u64 + 1, text, 2).unwrap();
    }
    index.flush(3).await.unwrap();
    let before = index.metadata();
    assert_eq!(before.buckets.len(), 4);
    index.compact_index().await.unwrap();
    assert_eq!(index.metadata().buckets.len(), 4);
    assert!(
        !index.has_pending_flush(),
        "compaction must persist its changed layout"
    );
    let durable = BM25::bootstrap("body".into(), default_tokenizer(), storage)
        .await
        .unwrap();
    assert_eq!(
        durable.metadata().stats.version,
        index.metadata().stats.version
    );
    let stats = index.metadata();
    index.compact_index().await.unwrap();
    assert_eq!(stats.stats.version, index.metadata().stats.version);
}

#[tokio::test]
async fn regression_stream_reader_small_chunk_does_not_sniff_zstd() {
    let storage = Storage::connect(
        "tiny_chunk".into(),
        Arc::new(InMemory::new()),
        StorageConfig {
            compress_level: 3,
            object_chunk_size: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let payload = vec![b'x'; 1024];
    storage
        .put_bytes(
            "data",
            payload.clone().into(),
            anda_db::storage::PutMode::Overwrite,
        )
        .await
        .unwrap();
    assert_eq!(
        storage.fetch_bytes("data").await.unwrap().0.as_ref(),
        payload.as_slice()
    );
    let mut reader = storage.stream_reader("data").await.unwrap();
    let mut output = Vec::new();
    reader.read_to_end(&mut output).await.unwrap();
    assert_eq!(output, payload);
}

#[tokio::test]
async fn regression_relocated_database_uses_original_collection_prefix() {
    use futures::TryStreamExt;
    let store = Arc::new(InMemory::new());
    {
        let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
        plain(&db)
            .await
            .add_from(&doc("old", "original"))
            .await
            .unwrap();
        db.close().await.unwrap();
    }
    let objects: Vec<_> = store
        .list(Some(&Path::from("reviewdb")))
        .try_collect()
        .await
        .unwrap();
    for object in objects {
        let target = Path::from(
            object
                .location
                .as_ref()
                .replacen("reviewdb/", "restored/", 1),
        );
        store.copy(&object.location, &target).await.unwrap();
    }
    let mut relocated_cfg = cfg();
    relocated_cfg.name = "restored".into();
    let db = AndaDB::connect(store.clone(), relocated_cfg).await.unwrap();
    assert_eq!(
        db.name(),
        "restored",
        "the opened prefix must be authoritative"
    );
    let c = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    c.add_from(&doc("new", "should target restored copy"))
        .await
        .unwrap();
    assert!(
        store
            .head(&Path::from("reviewdb/docs/data/2.cbor"))
            .await
            .is_err()
    );
    assert!(
        store
            .head(&Path::from("restored/docs/data/2.cbor"))
            .await
            .is_ok()
    );
}

#[test]
fn regression_json_text_extraction_depends_on_first_array_element() {
    let value = Fv::Json(serde_json::json!([0, "searchable", ["nested"]]));
    assert_eq!(
        virtual_searchable_text(&[Some(&value)]).as_deref(),
        Some("searchable\nnested")
    );
}

#[tokio::test]
async fn regression_foreign_document_schema_yields_wrong_index_values() {
    use anda_db::schema::{Fe, Ft, Schema};
    fn schema(reverse: bool) -> Schema {
        let mut s = Schema::builder();
        let names = if reverse { ["b", "a"] } else { ["a", "b"] };
        for name in names {
            s.add_field(Fe::new(name.into(), Ft::Text).unwrap())
                .unwrap();
        }
        s.build().unwrap()
    }
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = db
        .create_collection(schema(false), cc(), async |c| {
            c.create_btree_index(&["a"]).await
        })
        .await
        .unwrap();
    let mut foreign = Document::new(Arc::new(schema(true)));
    foreign.set_id(0);
    foreign.set_field("a", Fv::Text("A".into())).unwrap();
    foreign.set_field("b", Fv::Text("B".into())).unwrap();
    assert!(matches!(c.add(foreign).await, Err(DBError::Schema { .. })));
    assert!(c.is_empty());
    let mut equivalent = Document::new(Arc::new(schema(false)));
    equivalent.set_id(0);
    equivalent.set_field("a", Fv::Text("A".into())).unwrap();
    equivalent.set_field("b", Fv::Text("B".into())).unwrap();
    let id = c.add(equivalent).await.unwrap();
    assert_eq!(
        c.get(id).await.unwrap().get_field("a"),
        Some(&Fv::Text("A".into()))
    );
}

#[tokio::test]
async fn bounded_boolean_pages_match_a_reference_set() {
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = db
        .create_collection(Doc::schema().unwrap(), cc(), async |c| {
            c.create_btree_index(&["body"]).await
        })
        .await
        .unwrap();
    for i in 0..150 {
        c.add_from(&doc(&format!("key{i}"), &format!("group{}", i % 7)))
            .await
            .unwrap();
    }
    fn id_range(q: RangeQuery<Fv>) -> Filter {
        Filter::Field(("_id".into(), q))
    }
    for seed in 0..30u64 {
        let a = id_range(RangeQuery::Between(Fv::U64(seed + 1), Fv::U64(seed + 100)));
        let b = Filter::Field((
            "body".into(),
            RangeQuery::Eq(Fv::Text(format!("group{}", seed % 7))),
        ));
        let forms = [
            Filter::Or(vec![Box::new(a.clone()), Box::new(b.clone())]),
            Filter::Or(vec![Box::new(b.clone()), Box::new(a.clone())]),
            Filter::And(vec![Box::new(a.clone()), Box::new(b.clone())]),
            Filter::And(vec![Box::new(b.clone()), Box::new(a.clone())]),
            Filter::And(vec![
                Box::new(a.clone()),
                Box::new(Filter::Not(Box::new(b.clone()))),
            ]),
        ];
        for (which, filter) in forms.into_iter().enumerate() {
            let expected: Vec<_> = (1..=150u64)
                .filter(|id| {
                    let inside = (seed + 1..=seed + 100).contains(id);
                    let group = (id - 1) % 7 == seed % 7;
                    match which {
                        0 | 1 => inside || group,
                        2 | 3 => inside && group,
                        _ => inside && !group,
                    }
                })
                .collect();
            assert_eq!(c.query_all_ids(filter.clone()).await.unwrap(), expected);
            for limit in [0, 1, 7, 20, 1000] {
                assert_eq!(
                    c.query_ids(filter.clone(), Some(limit)).await.unwrap(),
                    expected.iter().take(limit).copied().collect::<Vec<_>>()
                );
                assert_eq!(
                    c.query_last_ids(filter.clone(), Some(limit)).await.unwrap(),
                    expected[expected.len().saturating_sub(limit)..].to_vec()
                );
            }
        }
    }
}

#[tokio::test]
async fn selective_search_finds_matches_beyond_the_global_candidate_window() {
    use anda_db::query::SearchOptions;
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = db
        .create_collection(Doc::schema().unwrap(), cc(), async |c| {
            c.create_btree_index(&["key"]).await?;
            c.create_bm25_index(&["body"]).await?;
            c.create_hnsw_index(
                "embedding",
                HnswConfig {
                    dimension: 2,
                    ..Default::default()
                },
            )
            .await
        })
        .await
        .unwrap();
    for i in 0..120 {
        c.add_from(&doc(&format!("key{i}"), "memory"))
            .await
            .unwrap();
    }
    for search in [
        Search {
            text: Some("memory".into()),
            ..Default::default()
        },
        Search {
            vector: Some(vec![1., 2.]),
            ..Default::default()
        },
        Search {
            text: Some("memory".into()),
            vector: Some(vec![1., 2.]),
            ..Default::default()
        },
    ] {
        let query = Query {
            search: Some(search),
            filter: Some(eq("key119")),
            limit: Some(1),
        };
        assert_eq!(c.search_ids(query).await.unwrap(), vec![120]);
    }
    let query = Query {
        search: Some(Search {
            text: Some("memory".into()),
            ..Default::default()
        }),
        filter: Some(eq("key119")),
        limit: Some(1),
    };
    assert!(
        c.search_ids_with_options(
            query.clone(),
            SearchOptions {
                prefilter_limit: 0,
                adaptive: false,
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        c.search_ids_with_options(
            query,
            SearchOptions {
                prefilter_limit: 0,
                adaptive: true,
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        vec![120]
    );
}

#[tokio::test]
async fn stream_budgets_are_explicit_and_symmetric() {
    let store = Arc::new(InMemory::new());
    let storage = Storage::connect("limits".into(), store.clone(), StorageConfig::default())
        .await
        .unwrap();
    for compressed in [false, true] {
        let storage = if compressed {
            storage.clone()
        } else {
            Storage::connect(
                "plain_limits".into(),
                store.clone(),
                StorageConfig {
                    compress_level: 0,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        };
        let mut writer = storage.stream_writer_with_limit("data", 1024);
        writer.write_all(&vec![b'x'; 1024]).await.unwrap();
        writer.shutdown().await.unwrap();
        let mut reader = storage
            .stream_reader_with_limit("data", 1024)
            .await
            .unwrap();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out.len(), 1024);
        let mut reader = storage
            .stream_reader_with_limit("data", 1023)
            .await
            .unwrap();
        assert!(reader.read_to_end(&mut Vec::new()).await.is_err());
        let mut writer = storage.stream_writer_with_limit("too_large", 1023);
        assert!(writer.write_all(&vec![b'x'; 1024]).await.is_err());
    }
    assert!(
        Storage::connect(
            "zero".into(),
            store,
            StorageConfig {
                object_chunk_size: 0,
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn deleting_another_index_does_not_publish_a_staged_index() {
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = db
        .create_collection(Doc::schema().unwrap(), cc(), async |c| {
            c.create_bm25_index(&["body"]).await
        })
        .await
        .unwrap();
    c.add_from(&doc("key", "memory")).await.unwrap();
    db.close_collection("docs").await.unwrap();
    assert!(
        db.open_collection("docs".into(), async |c| {
            c.create_hnsw_index(
                "embedding",
                HnswConfig {
                    dimension: 2,
                    ..Default::default()
                },
            )
            .await?;
            c.remove_bm25_index(&["body"]).await?;
            Err(fail_callback())
        })
        .await
        .is_err()
    );
    let c = db
        .open_collection("docs".into(), async |c| {
            c.create_hnsw_index_nx(
                "embedding",
                HnswConfig {
                    dimension: 2,
                    ..Default::default()
                },
            )
            .await
        })
        .await
        .unwrap();
    assert_eq!(
        c.search_ids(Query {
            search: Some(Search {
                vector: Some(vec![1., 2.]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap(),
        vec![1]
    );
}

#[tokio::test]
async fn unique_key_recovery_covers_update_and_delete_before_and_after_commit() {
    for deleting in [false, true] {
        for after in [false, true] {
            let (store, faults) = FaultStore::wrap(InMemory::new());
            let db = AndaDB::connect(Arc::new(store), cfg()).await.unwrap();
            let c = db
                .create_collection(Doc::schema().unwrap(), cc(), async |c| {
                    c.create_btree_index(&["key"]).await
                })
                .await
                .unwrap();
            c.add_from(&doc("x", "first")).await.unwrap();
            c.flush(unix_ms()).await.unwrap();
            let gate = FaultGate::new();
            faults.push_rule(FaultRule {
                op: if deleting {
                    FaultOp::Delete
                } else {
                    FaultOp::Put
                },
                path_contains: Some("data/1.cbor".into()),
                skip: 0,
                times: 1,
                kind: if after {
                    FaultKind::PauseAfter(gate.clone())
                } else {
                    FaultKind::PauseBefore(gate.clone())
                },
            });
            let owner = c.clone();
            let mutation = tokio::spawn(async move {
                if deleting {
                    owner.remove(1).await.map(|_| ())
                } else {
                    owner
                        .update(1, BTreeMap::from([("key".into(), Fv::Text("y".into()))]))
                        .await
                        .map(|_| ())
                }
            });
            gate.wait_entered().await;
            let waiter = c.clone();
            let mut waiter =
                tokio::spawn(async move { waiter.add_from(&doc("x", "second")).await });
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiter)
                    .await
                    .is_err()
            );
            mutation.abort();
            assert!(mutation.await.unwrap_err().is_cancelled());
            assert!(waiter.await.unwrap().unwrap_err().is_poisoned());
            let fresh = db
                .open_collection("docs".into(), async |_| Ok(()))
                .await
                .unwrap();
            let added = fresh.add_from(&doc("x", "second")).await;
            assert_eq!(added.is_ok(), after);
            if !after {
                assert_eq!(fresh.query_all_ids(eq("x")).await.unwrap(), vec![1]);
            } else {
                assert_eq!(
                    fresh.query_all_ids(eq("x")).await.unwrap(),
                    vec![added.unwrap()]
                );
            }
        }
    }
}

#[tokio::test]
async fn unrelated_unique_keys_can_commit_concurrently() {
    use std::hash::{Hash, Hasher};
    fn stripe(value: &str) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        "key".hash(&mut h);
        value.hash(&mut h);
        h.finish() % 256
    }
    let other = (0..1000)
        .map(|i| format!("independent{i}"))
        .find(|key| stripe(key) != stripe("x") && stripe(key) != stripe("y"))
        .unwrap();
    let (store, faults) = FaultStore::wrap(InMemory::new());
    let db = AndaDB::connect(Arc::new(store), cfg()).await.unwrap();
    let c = db
        .create_collection(Doc::schema().unwrap(), cc(), async |c| {
            c.create_btree_index(&["key"]).await
        })
        .await
        .unwrap();
    c.add_from(&doc("x", "first")).await.unwrap();
    let gate = FaultGate::new();
    faults.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: Some("data/1.cbor".into()),
        skip: 0,
        times: 1,
        kind: FaultKind::PauseBefore(gate.clone()),
    });
    let owner = c.clone();
    let updating = tokio::spawn(async move {
        owner
            .update(1, BTreeMap::from([("key".into(), Fv::Text("y".into()))]))
            .await
    });
    gate.wait_entered().await;
    let owner = c.clone();
    let mut independent =
        tokio::spawn(async move { owner.add_from(&doc(&other, "independent")).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), &mut independent)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    gate.release();
    updating.await.unwrap().unwrap();
    assert!(c.add_from(&doc("x", "replacement")).await.is_ok());
}

#[tokio::test]
async fn document_mapping_ignores_non_encoding_schema_metadata() {
    use anda_db::schema::{Fe, Ft, Schema};
    let mut original = Schema::builder();
    original
        .add_field(Fe::new("value".into(), Ft::Text).unwrap())
        .unwrap();
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = db
        .create_collection(original.build().unwrap(), cc(), async |_| Ok(()))
        .await
        .unwrap();
    let mut described = Schema::builder();
    described.with_version(2);
    described
        .add_field(
            Fe::new("value".into(), Ft::Text)
                .unwrap()
                .with_description("updated documentation".into()),
        )
        .unwrap();
    let mut document = Document::new(Arc::new(described.build().unwrap()));
    document.set_id(0);
    document
        .set_field("value", Fv::Text("same bytes".into()))
        .unwrap();
    let id = c.add(document).await.unwrap();
    assert_eq!(
        c.get(id).await.unwrap().get_field("value"),
        Some(&Fv::Text("same bytes".into()))
    );
}
