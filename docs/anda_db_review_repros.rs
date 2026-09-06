// Historical anomaly probes for the pre-fix 2026-09-06 review snapshot.
// These are intentionally not the current test suite. The permanent fixed-
// behavior assertions live in rs/anda_db/tests/review_regressions.rs.
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
async fn repro_unique_key_released_before_document_commit() {
    let (store, faults) = FaultStore::wrap(InMemory::new());
    let store = Arc::new(store);
    let db = AndaDB::connect(store, cfg()).await.unwrap();
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
    let old = c.clone();
    let updating = tokio::spawn(async move {
        old.update(a, BTreeMap::from([("key".into(), Fv::Text("y".into()))]))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_entered())
        .await
        .expect("update did not reach the reviewed commit boundary");
    let adding_collection = c.clone();
    let mut adding =
        tokio::spawn(async move { adding_collection.add_from(&doc("x", "second")).await });
    // Wait on the task handle, not on a mutating future. A future fix may
    // correctly block this key, in which case the probe should fail promptly.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), &mut adding).await;
    if outcome.is_err() {
        updating.abort();
        adding.abort();
        gate.release();
        panic!("unique-key insertion now blocks; rewrite this probe as a fixed-behavior test");
    }
    let b = outcome.unwrap().unwrap().unwrap();
    updating.abort();
    assert!(updating.await.unwrap_err().is_cancelled());
    assert!(c.is_poisoned());
    let fresh = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    let first: Doc = fresh.get_as(a).await.unwrap();
    let second: Doc = fresh.get_as(b).await.unwrap();
    assert_eq!(
        first.key, second.key,
        "current bug: two durable documents own unique x"
    );
    assert_eq!(
        fresh.query_all_ids(eq("x")).await.unwrap().len(),
        1,
        "only one of two documents is searchable by x"
    );
}

#[tokio::test]
async fn repro_extension_publishes_unflushed_new_index() {
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
    assert!(
        c.query_all_ids(eq("x")).await.unwrap().is_empty(),
        "current bug: registered index is empty forever"
    );
}

#[tokio::test]
async fn repro_create_btree_index_nx_swallows_duplicate_backfill_failure() {
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = plain(&db).await;
    c.add_from(&doc("same", "one")).await.unwrap();
    c.add_from(&doc("same", "two")).await.unwrap();
    db.close_collection("docs").await.unwrap();
    let c = db
        .open_collection("docs".into(), async |c| {
            // This returns Ok even though unique-key backfill fails.
            c.create_btree_index_nx(&["key"]).await
        })
        .await
        .unwrap();
    assert!(c.get_btree_index(&["key"]).is_err());
    assert!(c.add_from(&doc("same", "three")).await.is_ok());
}

#[tokio::test]
async fn repro_transient_recovery_read_failure_is_checkpointed_past() {
    let (store, faults) = FaultStore::wrap(InMemory::new());
    let store = Arc::new(store);
    {
        let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
        let c = plain(&db).await;
        c.add_from(&doc("a", "one")).await.unwrap();
        c.add_from(&doc("b", "two")).await.unwrap();
        // Drop all handles without a checkpoint, modeling process termination.
    }
    faults.push_rule(FaultRule::fail_once(FaultOp::Get, "data/1.cbor"));
    {
        let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
        let c = plain(&db).await;
        assert_eq!(c.ids(), vec![2]);
        assert_eq!(c.storage_stats().check_point, 2);
    }
    let db = AndaDB::connect(store.clone(), cfg()).await.unwrap();
    let c = plain(&db).await;
    assert_eq!(
        c.ids(),
        vec![2],
        "current bug: retry no longer probes document 1"
    );
    assert!(
        store
            .head(&Path::from("reviewdb/docs/data/1.cbor"))
            .await
            .is_ok()
    );
    assert_eq!(c.reconcile_storage().await.unwrap(), (1, 0));
}

#[tokio::test]
async fn repro_accepted_large_document_cannot_be_updated() {
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg())
        .await
        .unwrap();
    let c = plain(&db).await;
    let id = c.add_from(&doc("a", &"z".repeat(1_100_000))).await.unwrap();
    let err = c
        .update(id, BTreeMap::from([("key".into(), Fv::Text("b".into()))]))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DBError::PayloadTooLarge { ref path, .. } if path.contains("mutation_intents"))
    );
}

#[tokio::test]
async fn repro_custom_tokenizer_not_restored_to_loaded_bm25() {
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
    assert!(
        c.search_ids(q).await.unwrap().is_empty(),
        "current bug: loaded BM25 still uses default tokenizer"
    );
}

#[tokio::test]
async fn repro_open_callback_add_runs_before_allocator_recovery() {
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
    assert!(
        matches!(result, Err(DBError::AlreadyExists { ref path, .. }) if path.ends_with("data/1.cbor"))
    );
}

#[tokio::test]
async fn repro_dead_id_removal_has_no_replay_record() {
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
    assert_eq!(c.query_all_ids(eq("a")).await.unwrap(), vec![1]);
    assert!(c.add_from(&doc("a", "replacement")).await.is_err());
}

#[tokio::test]
async fn repro_read_only_open_deletes_orphan_hnsw_blobs() {
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
        store.head(&orphan).await.is_err(),
        "current bug: opening a read-only collection deleted storage"
    );
}

#[tokio::test]
async fn repro_unknown_extension_commit_does_not_poison() {
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
    assert_eq!(c.state(), CollectionState::Active);
    let same = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    assert!(
        Arc::ptr_eq(&c, &same),
        "reopen silently returns unrecovered stale handle"
    );
    assert!(matches!(
        c.save_extension("y".into(), Fv::Bool(true)).await,
        Err(DBError::Precondition { .. })
    ));
}

#[tokio::test]
async fn repro_stream_writer_reader_rejects_compressible_roundtrip() {
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
    assert!(reader.read_to_end(&mut output).await.is_err());
    assert_eq!(output.len(), 16 * 1024);
}

#[tokio::test]
async fn repro_bm25_compaction_same_bucket_count_does_not_flush() {
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
        index.has_pending_flush(),
        "current bug: compact returned Ok with dirty rewritten layout"
    );
    let durable = BM25::bootstrap("body".into(), default_tokenizer(), storage)
        .await
        .unwrap();
    assert!(durable.metadata().stats.version < index.metadata().stats.version);
}

#[tokio::test]
async fn repro_stream_reader_small_chunk_does_not_sniff_zstd() {
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
    assert!(output.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]));
    assert_ne!(
        output, payload,
        "current bug: only one magic byte was inspected"
    );
}

#[tokio::test]
async fn repro_relocated_database_uses_original_collection_prefix() {
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
        "reviewdb",
        "current bug: old name overrides caller path"
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
            .is_ok()
    );
    assert!(
        store
            .head(&Path::from("restored/docs/data/2.cbor"))
            .await
            .is_err()
    );
}

#[test]
fn repro_json_text_extraction_depends_on_first_array_element() {
    let value = Fv::Json(serde_json::json!([0, "searchable", ["nested"]]));
    assert!(virtual_searchable_text(&[Some(&value)]).is_none());
}

#[tokio::test]
async fn repro_foreign_document_schema_yields_wrong_index_values() {
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
    let id = c.add(foreign).await.unwrap();
    assert_eq!(
        c.get(id).await.unwrap().get_field("a"),
        Some(&Fv::Text("B".into()))
    );
    let ids = c
        .query_all_ids(Filter::Field((
            "a".into(),
            RangeQuery::Eq(Fv::Text("A".into())),
        )))
        .await
        .unwrap();
    assert_eq!(ids, vec![id]);
}
