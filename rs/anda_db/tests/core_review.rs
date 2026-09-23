//! Regression coverage for the September 2026 collection review.
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::{AndaDB, DBConfig},
    error::{CollectionState, DBError},
    index::{BM25, HnswConfig, IndexHooks, default_tokenizer},
    query::{Filter, Query, RangeQuery, Search},
    schema::{Document, Fe, Ft, Fv, Schema, vector_from_f32},
    storage::{Storage, StorageConfig},
    unix_ms,
};
use anda_object_store::{FaultGate, FaultKind, FaultOp, FaultRule, FaultStore};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::{borrow::Cow, collections::BTreeMap, sync::Arc, time::Duration};

fn schema(fields: &[(&str, Ft)], version: u64) -> Schema {
    let mut b = Schema::builder();
    b.with_version(version);
    for (name, ft) in fields {
        b.add_field(Fe::new((*name).into(), ft.clone()).unwrap())
            .unwrap();
    }
    b.build().unwrap()
}
fn cc() -> CollectionConfig {
    CollectionConfig {
        name: "docs".into(),
        ..Default::default()
    }
}
fn cfg(limit: usize, compression: i32) -> DBConfig {
    DBConfig {
        name: "core_review".into(),
        storage: StorageConfig {
            compress_level: compression,
            max_small_object_size: limit,
            bucket_overload_size: limit / 2,
            ..Default::default()
        },
        ..Default::default()
    }
}
async fn db(limit: usize) -> AndaDB {
    AndaDB::connect(Arc::new(InMemory::new()), cfg(limit, 0))
        .await
        .unwrap()
}
fn body_doc(c: &Collection, body: &str) -> Document {
    let mut doc = c.new_document();
    doc.set_id(0);
    doc.set_field("body", Fv::Text(body.into())).unwrap();
    doc
}
fn text_query() -> Query {
    Query {
        search: Some(Search {
            text: Some("memory".into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn popular_postings_can_exceed_the_document_limit_and_reopen() {
    for compression in [0, 3] {
        for kind in ["btree", "bm25"] {
            let store = Arc::new(InMemory::new());
            let config = cfg(4096, compression);
            let db = AndaDB::connect(store.clone(), config.clone())
                .await
                .unwrap();
            let c = db
                .create_collection(schema(&[("body", Ft::Text)], 1), cc(), async |c| {
                    if kind == "btree" {
                        c.create_btree_index(&["body"]).await
                    } else {
                        c.create_bm25_index(&["body"]).await
                    }
                })
                .await
                .unwrap();
            for _ in 0..1600 {
                c.add(body_doc(&c, "memory")).await.unwrap();
            }
            db.close().await.unwrap();
            drop(c);
            drop(db);
            let db = AndaDB::open(store, config).await.unwrap();
            let c = db
                .open_collection("docs".into(), async |_| Ok(()))
                .await
                .unwrap();
            assert_eq!(c.len(), 1600);
            c.add(body_doc(&c, "memory")).await.unwrap();
            c.flush(unix_ms()).await.unwrap();
            if kind == "btree" {
                assert_eq!(
                    c.query_all_ids(Filter::Field((
                        "body".into(),
                        RangeQuery::Eq(Fv::Text("memory".into()))
                    )))
                    .await
                    .unwrap(),
                    c.ids()
                );
            } else {
                assert_eq!(
                    c.get_bm25_index(&["body"])
                        .unwrap()
                        .search("memory", 2000, None)
                        .len(),
                    1601
                );
            }
        }
    }
}

#[tokio::test]
async fn default_bm25_capacity_handles_one_hundred_thousand_common_texts() {
    let storage = Storage::connect(
        "default_capacity".into(),
        Arc::new(InMemory::new()),
        StorageConfig::default(),
    )
    .await
    .unwrap();
    let index = BM25::new(vec!["body".into()], default_tokenizer(), storage.clone(), 1)
        .await
        .unwrap();
    for id in 1..=100_000 {
        index.insert(id, "memory knowledge database", 2).unwrap();
    }
    index.flush(3).await.unwrap();
    let index = BM25::bootstrap("body".into(), default_tokenizer(), storage)
        .await
        .unwrap();
    assert_eq!(index.stats().num_elements, 100_000);
    assert_eq!(index.search("knowledge", 10, None).len(), 10);
    index
        .insert(100_001, "memory knowledge database", 4)
        .unwrap();
    index.flush(5).await.unwrap();
}

#[tokio::test]
async fn schema_cannot_retire_fields_referenced_by_any_index() {
    for kind in ["btree", "compound", "bm25", "hnsw"] {
        let db = db(2_048_000).await;
        let original = schema(
            &[
                ("body", Ft::Text),
                ("owner", Ft::U64),
                ("embedding", Ft::Vector),
            ],
            1,
        );
        let c = db
            .create_collection(original, cc(), async |c| match kind {
                "btree" => c.create_btree_index(&["body"]).await,
                "compound" => c.create_btree_index(&["body", "owner"]).await,
                "bm25" => c.create_bm25_index(&["body", "owner"]).await,
                _ => {
                    c.create_hnsw_index(
                        "embedding",
                        HnswConfig {
                            dimension: 2,
                            ..Default::default()
                        },
                    )
                    .await
                }
            })
            .await
            .unwrap();
        let mut doc = body_doc(&c, "memory");
        doc.set_field("owner", Fv::U64(1)).unwrap();
        doc.set_field("embedding", Fv::Vector(vector_from_f32(vec![1., 2.])))
            .unwrap();
        let id = c.add(doc).await.unwrap();
        db.close_collection("docs").await.unwrap();
        let path = Path::from("core_review/docs/meta.cbor");
        let before = db
            .object_store()
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let err = db
            .open_or_create_collection(schema(&[], 2), cc(), async |_| {
                panic!("invalid upgrade must fail before the callback")
            })
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Schema { .. }), "{err}");
        assert!(err.to_string().contains("index"), "{err}");
        assert_eq!(
            db.object_store()
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            before
        );
        let c = db
            .open_collection("docs".into(), async |c| {
                match kind {
                    "btree" => {
                        c.remove_btree_index(&["body"]).await?;
                    }
                    "compound" => {
                        c.remove_btree_index(&["body", "owner"]).await?;
                    }
                    "bm25" => {
                        c.remove_bm25_index(&["body", "owner"]).await?;
                    }
                    _ => {
                        c.remove_hnsw_index("embedding").await?;
                    }
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            c.get(id).await.unwrap().get_field("body"),
            Some(&Fv::Text("memory".into()))
        );
        db.close_collection("docs").await.unwrap();
        let c = db
            .open_or_create_collection(schema(&[], 2), cc(), async |_| Ok(()))
            .await
            .unwrap();
        c.remove(id).await.unwrap();
        db.close_collection("docs").await.unwrap();
        let c = db
            .open_collection("docs".into(), async |_| Ok(()))
            .await
            .unwrap();
        assert!(c.is_empty());
        assert!(c.metadata().btree_indexes.is_empty());
        assert!(c.metadata().bm25_indexes.is_empty());
        assert!(c.metadata().hnsw_indexes.is_empty());
    }
}

#[tokio::test]
async fn optional_vectors_support_backfill_later_updates_and_reopen() {
    let db = db(2_048_000).await;
    let c = db
        .create_collection(schema(&[("body", Ft::Text)], 1), cc(), async |_| Ok(()))
        .await
        .unwrap();
    let id = c.add(body_doc(&c, "memory")).await.unwrap();
    db.close_collection("docs").await.unwrap();
    let c = db
        .open_or_create_collection(
            schema(
                &[
                    ("body", Ft::Text),
                    ("embedding", Ft::Option(Box::new(Ft::Vector))),
                ],
                2,
            ),
            cc(),
            async |c| {
                c.create_hnsw_index(
                    "embedding",
                    HnswConfig {
                        dimension: 2,
                        ..Default::default()
                    },
                )
                .await
            },
        )
        .await
        .unwrap();
    c.add(body_doc(&c, "without vector")).await.unwrap();
    let q = || Query {
        search: Some(Search {
            vector: Some(vec![1., 2.]),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(c.search_ids(q()).await.unwrap().is_empty());
    c.update(
        id,
        BTreeMap::from([(
            "embedding".into(),
            Fv::Vector(vector_from_f32(vec![1., 2.])),
        )]),
    )
    .await
    .unwrap();
    assert_eq!(c.search_ids(q()).await.unwrap(), vec![id]);
    db.close_collection("docs").await.unwrap();
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
    assert_eq!(c.search_ids(q()).await.unwrap(), vec![id]);
    c.update(id, BTreeMap::from([("embedding".into(), Fv::Null)]))
        .await
        .unwrap();
    db.close_collection("docs").await.unwrap();
    let c = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    assert!(c.search_ids(q()).await.unwrap().is_empty());
}

#[tokio::test]
async fn oversized_update_is_rejected_before_any_persistent_or_index_change() {
    let db = db(4096).await;
    let c = db
        .create_collection(schema(&[("body", Ft::Text)], 1), cc(), async |c| {
            c.create_btree_index(&["body"]).await?;
            c.create_bm25_index(&["body"]).await
        })
        .await
        .unwrap();
    let id = c.add(body_doc(&c, "memory")).await.unwrap();
    c.flush(unix_ms()).await.unwrap();
    let before = c.storage_stats();
    let index_before = c.get_bm25_index(&["body"]).unwrap().stats().version;
    let err = c
        .update(
            id,
            BTreeMap::from([("body".into(), Fv::Text("a".repeat(4500)))]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DBError::PayloadTooLarge { .. }));
    assert_eq!(c.state(), CollectionState::Active);
    assert_eq!(c.storage_stats().total_put_count, before.total_put_count);
    assert_eq!(
        c.get_bm25_index(&["body"]).unwrap().stats().version,
        index_before
    );
    assert_eq!(c.search_ids(text_query()).await.unwrap(), vec![id]);
    assert_eq!(
        c.get(id).await.unwrap().get_field("body"),
        Some(&Fv::Text("memory".into()))
    );
    c.update(
        id,
        BTreeMap::from([("body".into(), Fv::Text("updated".into()))]),
    )
    .await
    .unwrap();
    db.close_collection("docs").await.unwrap();
    let c = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(
        c.get(id).await.unwrap().get_field("body"),
        Some(&Fv::Text("updated".into()))
    );
}

#[tokio::test]
async fn new_document_allocates_its_id_without_a_caller_placeholder() {
    let db = db(2_048_000).await;
    let c = db
        .create_collection(schema(&[("body", Ft::Text)], 1), cc(), async |_| Ok(()))
        .await
        .unwrap();
    assert!(c.add(c.new_document()).await.is_err());
    let mut doc = c.new_document();
    doc.set_field("body", Fv::Text("memory".into())).unwrap();
    let id = c.add(doc).await.unwrap();
    assert!(id > 0);
    assert_eq!(c.get(id).await.unwrap().id(), id);
}

#[tokio::test]
async fn normalized_same_value_updates_do_not_write_or_reindex() {
    let db = db(2_048_000).await;
    let c = db
        .create_collection(
            schema(
                &[
                    ("body", Ft::Text),
                    ("embedding", Ft::Vector),
                    ("signed", Ft::I64),
                ],
                1,
            ),
            cc(),
            async |c| {
                c.create_bm25_index(&["body"]).await?;
                c.create_hnsw_index(
                    "embedding",
                    HnswConfig {
                        dimension: 2,
                        ..Default::default()
                    },
                )
                .await
            },
        )
        .await
        .unwrap();
    let mut doc = body_doc(&c, "memory");
    doc.set_field("embedding", Fv::Vector(vector_from_f32(vec![1., 2.])))
        .unwrap();
    doc.set_field("signed", Fv::I64(1)).unwrap();
    let id = c.add(doc).await.unwrap();
    c.flush(unix_ms()).await.unwrap();
    let before = c.storage_stats();
    let version = c.stats().version;
    let bm25 = c.get_bm25_index(&["body"]).unwrap().stats().version;
    let hnsw = c.get_hnsw_index("embedding").unwrap().stats().version;
    c.update(
        id,
        BTreeMap::from([
            ("body".into(), Fv::Text("memory".into())),
            (
                "embedding".into(),
                Fv::Vector(vector_from_f32(vec![1., 2.])),
            ),
            ("signed".into(), Fv::U64(1)),
        ]),
    )
    .await
    .unwrap();
    assert!(!c.flush(unix_ms()).await.unwrap());
    assert_eq!(c.stats().version, version);
    assert_eq!(c.storage_stats().total_put_count, before.total_put_count);
    assert_eq!(c.get_bm25_index(&["body"]).unwrap().stats().version, bm25);
    assert_eq!(c.get_hnsw_index("embedding").unwrap().stats().version, hnsw);
    // An unchanged vector supplied with an actual text change stays untouched.
    c.update(
        id,
        BTreeMap::from([
            ("body".into(), Fv::Text("changed".into())),
            (
                "embedding".into(),
                Fv::Vector(vector_from_f32(vec![1., 2.])),
            ),
        ]),
    )
    .await
    .unwrap();
    assert_eq!(c.get_hnsw_index("embedding").unwrap().stats().version, hnsw);
    assert!(c.search_ids(text_query()).await.unwrap().is_empty());
}

struct StableText;
impl IndexHooks for StableText {
    fn bm25_index_value<'a>(&self, _: &BM25, _: &'a Document) -> Option<Cow<'a, str>> {
        Some(Cow::Borrowed("memory"))
    }
}

#[tokio::test]
async fn unchanged_hook_outputs_are_not_reindexed() {
    let db = db(2_048_000).await;
    let c = db
        .create_collection(schema(&[("body", Ft::Text)], 1), cc(), async |c| {
            c.set_index_hooks(Arc::new(StableText));
            c.create_bm25_index(&["body"]).await
        })
        .await
        .unwrap();
    let id = c.add(body_doc(&c, "original")).await.unwrap();
    let version = c.get_bm25_index(&["body"]).unwrap().stats().version;
    c.update(
        id,
        BTreeMap::from([("body".into(), Fv::Text("changed".into()))]),
    )
    .await
    .unwrap();
    assert_eq!(
        c.get_bm25_index(&["body"]).unwrap().stats().version,
        version
    );
    assert_eq!(c.search_ids(text_query()).await.unwrap(), vec![id]);
}

#[tokio::test]
async fn only_membership_changes_rewrite_ids() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    let db = AndaDB::connect(Arc::new(store), cfg(2_048_000, 0))
        .await
        .unwrap();
    let c = db
        .create_collection(schema(&[("body", Ft::Text)], 1), cc(), async |_| Ok(()))
        .await
        .unwrap();
    let id = c.add(body_doc(&c, "memory")).await.unwrap();
    c.flush(unix_ms()).await.unwrap();
    handle.reset();
    c.update(
        id,
        BTreeMap::from([("body".into(), Fv::Text("changed".into()))]),
    )
    .await
    .unwrap();
    c.save_extension("saved".into(), Fv::U64(1)).await.unwrap();
    c.set_extension("staged".into(), Fv::U64(2));
    c.flush(unix_ms()).await.unwrap();
    assert!(
        !handle
            .mutation_log()
            .iter()
            .any(|(op, path)| *op == FaultOp::Put && path.ends_with("/ids.cbor"))
    );
    c.add(body_doc(&c, "new")).await.unwrap();
    c.flush(unix_ms()).await.unwrap();
    assert!(
        handle
            .mutation_log()
            .iter()
            .any(|(op, path)| *op == FaultOp::Put && path.ends_with("/ids.cbor"))
    );
    handle.reset();
    c.remove(id).await.unwrap();
    c.flush(unix_ms()).await.unwrap();
    assert!(
        handle
            .mutation_log()
            .iter()
            .any(|(op, path)| *op == FaultOp::Put && path.ends_with("/ids.cbor"))
    );
    db.close_collection("docs").await.unwrap();
    let c = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(c.ids(), vec![2]);
}

#[tokio::test]
async fn replay_prefetches_distinct_documents_with_a_bounded_window() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    let store = Arc::new(store);
    {
        let db = AndaDB::connect(store.clone(), cfg(2_048_000, 0))
            .await
            .unwrap();
        let c = db
            .create_collection(schema(&[("body", Ft::Text)], 1), cc(), async |_| Ok(()))
            .await
            .unwrap();
        for _ in 0..6 {
            c.add(body_doc(&c, "before")).await.unwrap();
        }
        c.flush(unix_ms()).await.unwrap();
        for id in 1..=6 {
            c.update(
                id,
                BTreeMap::from([("body".into(), Fv::Text("after".into()))]),
            )
            .await
            .unwrap();
        }
    }
    let first = FaultGate::new();
    let second = FaultGate::new();
    let third = FaultGate::new();
    for (id, gate) in [(1, &first), (2, &second), (3, &third)] {
        handle.push_rule(FaultRule {
            op: FaultOp::Get,
            path_contains: Some(format!("/data/{id}.cbor")),
            skip: 0,
            times: 1,
            kind: FaultKind::PauseBefore(gate.clone()),
        });
    }
    let db = AndaDB::open(store, cfg(2_048_000, 0)).await.unwrap();
    let open = tokio::spawn(async move {
        db.open_collection("docs".into(), async |c| c.set_io_concurrency(2))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), first.wait_entered())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), second.wait_entered())
        .await
        .unwrap();
    // Both admitted reads are still suspended: a third must not start yet.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), third.wait_entered())
            .await
            .is_err()
    );
    first.release();
    second.release();
    tokio::time::timeout(Duration::from_secs(5), third.wait_entered())
        .await
        .unwrap();
    third.release();
    let c = open.await.unwrap().unwrap();
    assert_eq!(c.len(), 6);
    for id in 1..=6 {
        assert_eq!(
            c.get(id).await.unwrap().get_field("body"),
            Some(&Fv::Text("after".into()))
        );
    }
}

#[tokio::test]
async fn indexed_cursor_pages_match_document_history_after_posting_reordering() {
    let db = db(2_048_000).await;
    let c = db
        .create_collection(schema(&[("body", Ft::Text)], 1), cc(), async |c| {
            c.create_btree_index(&["body"]).await
        })
        .await
        .unwrap();
    let mut model = BTreeMap::new();
    for i in 0..300 {
        let body = if i % 3 == 0 { "memory" } else { "other" };
        let id = c.add(body_doc(&c, body)).await.unwrap();
        model.insert(id, body);
    }
    for id in [1, 7, 10, 20, 100, 250] {
        c.remove(id).await.unwrap();
        model.remove(&id);
    }
    for id in [3, 8, 13, 19, 60, 200] {
        c.update(
            id,
            BTreeMap::from([("body".into(), Fv::Text("memory".into()))]),
        )
        .await
        .unwrap();
        model.insert(id, "memory");
    }
    for cursor in [0, 21, 140, 301] {
        let indexed = Filter::Field(("body".into(), RangeQuery::Eq(Fv::Text("memory".into()))));
        let excluded = Filter::Field((
            "_id".into(),
            RangeQuery::Include(vec![Fv::U64(13), Fv::U64(13), Fv::U64(19)]),
        ));
        let id_filter = Filter::And(vec![
            Box::new(Filter::Field((
                "_id".into(),
                RangeQuery::Lt(Fv::U64(cursor)),
            ))),
            Box::new(Filter::Not(Box::new(excluded))),
        ]);
        let expected: Vec<_> = model
            .iter()
            .filter(|(id, body)| **id < cursor && ![13, 19].contains(id) && **body == "memory")
            .map(|(id, _)| *id)
            .collect();
        for branches in [
            vec![Box::new(indexed.clone()), Box::new(id_filter.clone())],
            vec![Box::new(id_filter.clone()), Box::new(indexed.clone())],
        ] {
            let filter = Filter::And(branches);
            for limit in [1, 17, 1000] {
                assert_eq!(
                    c.query_ids(filter.clone(), Some(limit)).await.unwrap(),
                    expected.iter().take(limit).copied().collect::<Vec<_>>()
                );
                assert_eq!(
                    c.query_last_ids(filter.clone(), Some(limit)).await.unwrap(),
                    expected[expected.len().saturating_sub(limit)..]
                );
            }
        }
    }
    for (name, key, id_value) in [
        ("missing", Fv::Text("memory".into()), Fv::U64(100)),
        ("body", Fv::U64(10), Fv::U64(100)),
        (
            "body",
            Fv::Text("memory".into()),
            Fv::Text("invalid".into()),
        ),
    ] {
        let f = Filter::And(vec![
            Box::new(Filter::Field((name.into(), RangeQuery::Eq(key)))),
            Box::new(Filter::Field(("_id".into(), RangeQuery::Lt(id_value)))),
        ]);
        assert!(c.query_ids(f, Some(10)).await.is_err());
    }
}
