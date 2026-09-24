use super::*;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("writer failed"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// In-memory model of the durable store: one metadata object plus one
/// object per `(bucket_id, generation)`, mirroring the production
/// object-store layout.
#[derive(Default, Clone)]
struct MemStore {
    metadata: Vec<u8>,
    buckets: HashMap<BucketObject, Vec<u8>>,
}

/// Flushes `index` into `store` the way the production adapter does:
/// buckets first, then the manifest commit, then best-effort deletion of
/// the obsolete objects.
async fn flush_to(
    index: &BM25Index<TokenizerChain>,
    store: &mut MemStore,
    now_ms: u64,
) -> FlushOutcome {
    let mut meta_buf: Vec<u8> = Vec::new();
    let buckets = &mut store.buckets;
    let outcome = index
        .flush_with(
            now_ms,
            |data| {
                meta_buf = data;
                std::future::ready(Ok(()))
            },
            |object, data| {
                buckets.insert(object, data);
                std::future::ready(Ok(()))
            },
        )
        .await
        .unwrap();
    if outcome.saved {
        store.metadata = meta_buf;
        for object in &outcome.obsolete {
            store.buckets.remove(object);
        }
    }
    outcome
}

/// Loads a complete index from `store`.
async fn load_from(store: &MemStore) -> BM25Index<TokenizerChain> {
    BM25Index::load_all(default_tokenizer(), &store.metadata[..], async |object| {
        Ok(store.buckets.get(&object).cloned())
    })
    .await
    .unwrap()
}

// 创建一个简单的测试索引
fn create_test_index() -> BM25Index<TokenizerChain> {
    let index = BM25Index::new("anda_db_tfs_bm25".to_string(), default_tokenizer(), None);

    // 添加一些测试文档
    index
        .insert(1, "The quick brown fox jumps over the lazy dog", 0)
        .unwrap();
    index
        .insert(2, "A fast brown fox runs past the lazy dog", 0)
        .unwrap();
    index.insert(3, "The lazy dog sleeps all day", 0).unwrap();
    index
        .insert(4, "Quick brown foxes are rare in the wild", 0)
        .unwrap();

    index
}

fn encode_bucket_owned(
    postings: FxHashMap<String, PostingValue>,
    doc_tokens: FxHashMap<u64, usize>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    cbor2::to_writer(
        &BucketOwned {
            postings,
            doc_tokens,
        },
        &mut buf,
    )
    .unwrap();
    buf
}

#[test]
fn test_insert() {
    let index = create_test_index();
    assert_eq!(index.len(), 4);

    // 测试添加新文档
    index
        .insert(5, "A new document about cats and dogs", 0)
        .unwrap();
    assert_eq!(index.len(), 5);

    // 测试添加已存在的文档ID
    let result = index.insert(3, "This should fail", 0);
    assert!(matches!(
        result,
        Err(BM25Error::AlreadyExists { id: 3, .. })
    ));

    // 测试添加空文档
    let result = index.insert(6, "", 0);
    assert!(matches!(
        result,
        Err(BM25Error::TokenizeFailed { id: 6, .. })
    ));
}

#[tokio::test]
async fn test_metadata_accessors_empty_compaction_and_writer_error_paths() {
    let load_result: Result<BM25Index<_>, _> =
        BM25Index::load_metadata(default_tokenizer(), &b"not cbor"[..]);
    assert!(matches!(load_result, Err(BM25Error::Serialization { .. })));

    let index = BM25Index::new("empty_bm25".to_string(), default_tokenizer(), None);
    assert_eq!(index.name(), "empty_bm25");
    assert_eq!(index.len(), 0);
    assert!(index.is_empty());
    assert!(index.has_pending_metadata_flush());
    assert_eq!(index.metadata().name, "empty_bm25");

    index.buckets.insert(1, Bucket::default());
    let (old_count, new_count) = index.compact_buckets();
    assert_eq!((old_count, new_count), (2, 1));
    assert_eq!(index.max_bucket_id.load(Ordering::Relaxed), 0);
    assert!(index.has_dirty_buckets());

    // A failing metadata writer surfaces as an error and commits nothing.
    let mut writer = FailingWriter;
    let err = index
        .flush(&mut writer, 123, |_, _| std::future::ready(Ok(())))
        .await
        .unwrap_err();
    assert!(matches!(err, BM25Error::Generic { .. }));
    assert!(index.has_pending_metadata_flush());
    assert!(index.has_dirty_buckets());
}

#[test]
fn test_remove() {
    let index = create_test_index();
    assert_eq!(index.len(), 4);

    // 测试移除存在的文档
    let removed = index.remove(2, "A fast brown fox runs past the lazy dog", 0);
    assert!(removed);
    assert_eq!(index.len(), 3);

    // 测试移除不存在的文档
    let removed = index.remove(99, "This document doesn't exist", 0);
    assert!(!removed);
    assert_eq!(index.len(), 3);
}

/// Legacy (pre-manifest) data may contain the same token in two bucket
/// objects — a leftover of the old multi-phase flush protocol. The legacy
/// loader must keep the copy in the highest-numbered bucket, mark the
/// stale bucket dirty, and the first manifest flush must persist the
/// repaired layout.
#[tokio::test]
async fn test_legacy_load_reconciles_duplicate_token_bucket_ownership() {
    let index = BM25Index::new(
        "duplicate_token_load".to_string(),
        default_tokenizer(),
        Some(BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 64,
        }),
    );
    index.insert(1, "alpha", 0).unwrap();

    // Craft a legacy layout by hand: metadata without a manifest, bucket
    // objects at generation 0, and the token duplicated in buckets 0 and 1.
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let stale_bucket0 = store
        .buckets
        .values()
        .next()
        .expect("bucket 0 must be persisted")
        .clone();

    let mut metadata = index.metadata();
    metadata.stats.version += 1;
    metadata.stats.max_bucket_id = 1;
    metadata.buckets = BTreeMap::new(); // legacy: no manifest
    let mut metadata_buf = Vec::new();
    cbor2::to_writer(
        &BM25IndexRef {
            metadata: &metadata,
        },
        &mut metadata_buf,
    )
    .unwrap();

    let mut newer_postings = FxHashMap::default();
    newer_postings.insert("alpha".to_string(), (1, vec![(1, 1)]));
    let newer_bucket1 = encode_bucket_owned(newer_postings, FxHashMap::from_iter([(1, 1)]));

    let legacy_store = MemStore {
        metadata: metadata_buf,
        buckets: HashMap::from_iter([
            (
                BucketObject {
                    bucket_id: 0,
                    generation: 0,
                },
                stale_bucket0,
            ),
            (
                BucketObject {
                    bucket_id: 1,
                    generation: 0,
                },
                newer_bucket1,
            ),
        ]),
    };
    let loaded = load_from(&legacy_store).await;

    assert_eq!(loaded.postings.get("alpha").unwrap().0, 1);
    assert!(!loaded.buckets.get(&0).unwrap().tokens.contains("alpha"));
    assert!(loaded.has_dirty_buckets());

    // The first manifest flush persists the repaired layout and reports
    // the replaced legacy objects as obsolete.
    let mut store = legacy_store.clone();
    let outcome = flush_to(&loaded, &mut store, 2).await;
    assert!(outcome.saved);
    assert!(
        outcome
            .obsolete
            .iter()
            .any(|object| object.bucket_id == 0 && object.generation == 0),
        "the rewritten legacy bucket 0 must be reported obsolete: {outcome:?}"
    );

    let reloaded = load_from(&store).await;
    let results = reloaded.search("alpha", 10, None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1);
    assert!(!reloaded.has_dirty_buckets());
}

/// Data persisted by a pre-manifest release (metadata without a manifest,
/// un-suffixed bucket objects) loads correctly, and the first flush
/// upgrades the durable layout to the manifest format while retiring the
/// rewritten legacy objects.
#[tokio::test]
async fn test_legacy_format_loads_and_upgrades_on_first_flush() {
    let config = BM25Config {
        bm25: BM25Params::default(),
        bucket_overload_size: 64,
    };
    let index = BM25Index::new(
        "legacy_upgrade".to_string(),
        default_tokenizer(),
        Some(config),
    );
    index.insert(1, "alpha bravo charlie", 0).unwrap();
    index
        .insert(2, "delta echo foxtrot golf hotel india", 0)
        .unwrap();
    index
        .insert(3, "juliet kilo lima mike november oscar", 0)
        .unwrap();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    assert!(
        store.buckets.len() > 1,
        "scenario must span several buckets"
    );

    // Transform the store into the legacy layout: strip the manifest from
    // the metadata and re-key every bucket object to generation 0.
    let mut legacy_meta = index.metadata();
    legacy_meta.buckets = BTreeMap::new();
    let mut metadata_buf = Vec::new();
    cbor2::to_writer(
        &BM25IndexRef {
            metadata: &legacy_meta,
        },
        &mut metadata_buf,
    )
    .unwrap();
    let legacy_store = MemStore {
        metadata: metadata_buf,
        buckets: store
            .buckets
            .iter()
            .map(|(object, data)| {
                (
                    BucketObject {
                        bucket_id: object.bucket_id,
                        generation: 0,
                    },
                    data.clone(),
                )
            })
            .collect(),
    };

    // Legacy data loads through the bucket-id-scan path.
    let loaded = load_from(&legacy_store).await;
    assert_eq!(loaded.len(), 3);
    for (doc, term) in [(1, "alpha"), (2, "hotel"), (3, "oscar")] {
        assert!(
            loaded
                .search(term, 10, None)
                .iter()
                .any(|(id, _)| *id == doc),
            "doc {doc} must be found via '{term}' from the legacy layout"
        );
    }

    // Mutate and flush: the durable metadata upgrades to the manifest
    // format; rewritten legacy objects are reported obsolete.
    loaded.insert(4, "papa quebec romeo", 2).unwrap();
    let mut upgraded = legacy_store.clone();
    let outcome = flush_to(&loaded, &mut upgraded, 3).await;
    assert!(outcome.saved);
    for object in &outcome.obsolete {
        assert_eq!(
            object.generation, 0,
            "only replaced legacy objects may be obsolete here"
        );
    }

    let upgraded_meta = BM25Index::load_metadata(default_tokenizer(), &upgraded.metadata[..])
        .unwrap()
        .metadata();
    assert!(
        !upgraded_meta.buckets.is_empty(),
        "the first flush must commit a manifest"
    );

    let reloaded = load_from(&upgraded).await;
    assert_eq!(reloaded.len(), 4);
    for (doc, term) in [(1, "alpha"), (2, "hotel"), (3, "oscar"), (4, "papa")] {
        assert!(
            reloaded
                .search(term, 10, None)
                .iter()
                .any(|(id, _)| *id == doc),
            "doc {doc} must be found via '{term}' after the format upgrade"
        );
    }
}

#[test]
fn test_remove_with_wrong_text_does_not_leak_into_search() {
    let index = create_test_index();

    // remove() currently relies on caller providing the original text.
    // Even if postings are not fully cleaned, search must not return deleted documents.
    let removed = index.remove(2, "totally different text", 0);
    assert!(removed);
    assert_eq!(index.len(), 3);

    let results = index.search("fox", 10, None);
    assert!(!results.iter().any(|(id, _)| *id == 2));
}

#[tokio::test]
async fn test_remove_with_wrong_text_does_not_resurrect_after_reload() {
    let config = BM25Config {
        bm25: BM25Params::default(),
        bucket_overload_size: 64,
    };
    let index = BM25Index::new(
        "remove_wrong_text_reload".to_string(),
        default_tokenizer(),
        Some(config),
    );
    let terms = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima",
    ];
    let text = terms.join(" ");
    index.insert(1, &text, 0).unwrap();
    assert!(index.stats().max_bucket_id > 0);

    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;

    // The text accounts for none of the entries, so remove sweeps them by id.
    assert!(index.remove(1, "wrong text", 2));
    assert!(index.postings.is_empty());
    assert!(index.buckets.iter().all(|bucket| bucket.tokens.is_empty()));
    flush_to(&index, &mut store, 3).await;

    let loaded_index = load_from(&store).await;

    assert_eq!(loaded_index.len(), 0);
    for term in terms {
        assert!(
            loaded_index.search(term, 10, None).is_empty(),
            "removed document was found after reload for term '{term}'"
        );
    }
    assert!(loaded_index.postings.is_empty());
    assert!(!loaded_index.has_dirty_buckets());

    // Emptied buckets left the manifest; only the empty tail remains.
    assert_eq!(store.buckets.len(), 1);
    for data in store.buckets.values() {
        let bucket: BucketOwned = cbor2::from_reader(&data[..]).unwrap();
        assert!(bucket.postings.is_empty());
        assert!(bucket.doc_tokens.is_empty());
    }
}

#[test]
fn test_search() {
    let index = create_test_index();

    // 测试基本搜索功能
    let results = index.search("fox", 10, None);
    assert_eq!(results.len(), 3); // 应该找到3个包含"fox"的文档

    // 检查结果排序 - 文档1和2应该排在前面，因为它们都包含"fox"
    assert!(results.iter().any(|(id, _)| *id == 1));
    assert!(results.iter().any(|(id, _)| *id == 2));
    assert!(results.iter().any(|(id, _)| *id == 4));

    // 测试多词搜索
    let results = index.search("quick fox dog", 10, None);
    assert!(results[0].0 == 1); // 文档1应该排在最前面，因为它同时包含"quick", "fox", "dog"

    // 测试top_k限制
    let results = index.search("dog", 2, None);
    assert_eq!(results.len(), 2); // 应该只返回2个结果，尽管有3个文档包含"dog"

    // 测试空查询
    let results = index.search("", 10, None);
    assert_eq!(results.len(), 0);

    // 测试无匹配查询
    let results = index.search("elephant giraffe", 10, None);
    assert_eq!(results.len(), 0);
}

#[test]
fn test_search_top_k_zero_returns_empty() {
    let index = create_test_index();

    let basic = index.search("fox", 0, None);
    assert!(basic.is_empty());

    let advanced = index.search_advanced("fox OR dog", 0, None);
    assert!(advanced.is_empty());
}

#[test]
fn test_empty_index() {
    let tokenizer = default_tokenizer();
    let index = BM25Index::new("anda_db_tfs_bm25".to_string(), tokenizer, None);

    assert_eq!(index.len(), 0);
    assert!(index.is_empty());

    // 测试空索引的搜索
    let results = index.search("test", 10, None);
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_serialization() {
    let index = create_test_index();

    // 保存索引
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 0).await;

    // 加载索引
    let loaded_index = load_from(&store).await;

    // 验证加载的索引
    assert_eq!(loaded_index.len(), index.len());

    // 验证搜索结果
    let mut original_results = index.search("fox", 10, None);
    let mut loaded_results = loaded_index.search("fox", 10, None);

    assert_eq!(original_results.len(), loaded_results.len());
    original_results.sort_by_key(|a| a.0);
    loaded_results.sort_by_key(|a| a.0);
    // 比较文档ID和分数（允许浮点数有小误差）
    for i in 0..original_results.len() {
        assert_eq!(original_results[i].0, loaded_results[i].0);
        assert!((original_results[i].1 - loaded_results[i].1).abs() < 0.001);
    }
}

/// A dirty bucket always forces a manifest commit — bucket objects are
/// unreachable until the metadata references them. This covers the
/// load-time repair path, which marks buckets dirty without bumping the
/// stats version.
#[tokio::test]
async fn test_flush_commits_manifest_even_if_metadata_version_unchanged() {
    let index = create_test_index();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    assert!(!index.has_pending_metadata_flush());
    assert!(!index.has_dirty_buckets());

    // Simulate a load-time repair: dirty bucket, no version bump.
    index
        .buckets
        .get_mut(&0)
        .expect("bucket 0 exists")
        .mark_dirty();
    assert!(!index.has_pending_metadata_flush());
    assert!(index.has_dirty_buckets());

    let before = store.clone();
    let outcome = flush_to(&index, &mut store, 2).await;
    assert!(outcome.saved);
    assert!(!index.has_dirty_buckets());
    assert!(!index.has_pending_metadata_flush());
    assert_ne!(
        before.metadata, store.metadata,
        "the manifest commit must rewrite the metadata"
    );

    let reloaded = load_from(&store).await;
    assert_eq!(reloaded.len(), index.len());
}

#[tokio::test]
async fn test_flush_does_not_commit_metadata_when_bucket_write_fails() {
    let index = create_test_index();
    assert!(index.has_pending_metadata_flush());
    assert!(index.has_dirty_buckets());

    // Bucket persistence fails: flush must NOT have committed the
    // manifest (buckets are written before the metadata commit).
    let mut metadata_buf = Vec::new();
    let err = index
        .flush(&mut metadata_buf, 1, async |_, _| {
            Err::<(), BoxError>("bucket write failed".into())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, BM25Error::Generic { .. }));
    assert!(metadata_buf.is_empty());
    assert!(index.has_pending_metadata_flush());
    assert!(index.has_dirty_buckets());

    // The next flush retries both buckets and metadata.
    let mut metadata_buf = Vec::new();
    assert!(
        index
            .flush(&mut metadata_buf, 2, async |_, _| Ok(()))
            .await
            .unwrap()
            .saved
    );
    assert!(!metadata_buf.is_empty());
    assert!(!index.has_pending_metadata_flush());
    assert!(!index.has_dirty_buckets());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushEvent {
    Bucket(u32),
    Metadata,
}

/// Builds the token-migration scenario used by the crash-window tests:
/// doc 1 is fully persisted, then doc 2 both dirties doc 1's bucket (via
/// the shared token "alpha") and overflows it so new buckets are
/// allocated. Returns the index (holding unflushed doc 2) and the durable
/// store holding the committed doc-1-only snapshot.
async fn build_migration_scenario() -> (BM25Index<TokenizerChain>, MemStore) {
    let config = BM25Config {
        bm25: BM25Params::default(),
        bucket_overload_size: 64,
    };
    let index = BM25Index::new(
        "manifest_migration".to_string(),
        default_tokenizer(),
        Some(config),
    );
    index.insert(1, "alpha bravo", 0).unwrap();

    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let old_max = index.stats().max_bucket_id;

    index
        .insert(
            2,
            "alpha xray yankee zulu whiskey victor uniform tango sierra",
            2,
        )
        .unwrap();
    assert!(
        index.stats().max_bucket_id > old_max,
        "doc 2 must overflow into freshly-allocated buckets"
    );

    (index, store)
}

/// The manifest commit must be the last write of a flush: every dirty
/// bucket object precedes the metadata, and each is written to a fresh
/// generation-suffixed object.
#[tokio::test]
async fn test_flush_writes_all_buckets_before_manifest_commit() {
    let (index, _store) = build_migration_scenario().await;

    let events = std::cell::RefCell::new(Vec::<FlushEvent>::new());
    index
        .flush_with(
            3,
            |_data| {
                events.borrow_mut().push(FlushEvent::Metadata);
                std::future::ready(Ok(()))
            },
            |object, _data| {
                assert!(
                    object.generation > 0,
                    "bucket writes must target generation-suffixed objects"
                );
                events
                    .borrow_mut()
                    .push(FlushEvent::Bucket(object.bucket_id));
                std::future::ready(Ok(()))
            },
        )
        .await
        .unwrap();

    let events = events.into_inner();
    assert!(
        events.len() > 2,
        "expected several bucket writes: {events:?}"
    );
    assert_eq!(
        events.last(),
        Some(&FlushEvent::Metadata),
        "the manifest commit must come last: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| **e == FlushEvent::Metadata)
            .count(),
        1
    );
}

/// A crash after every new-generation bucket object is written but before
/// the manifest commit must leave the previous snapshot fully intact —
/// the new objects are unreferenced garbage.
#[tokio::test]
async fn test_flush_crash_before_manifest_commit_keeps_previous_snapshot() {
    let (index, store) = build_migration_scenario().await;

    // Crash window: bucket objects reach the store, the manifest doesn't.
    let mut crashed = store.clone();
    {
        let buckets = &mut crashed.buckets;
        let err = index
            .flush_with(
                3,
                |_| std::future::ready(Err::<(), BoxError>("crash before manifest".into())),
                |object, data| {
                    buckets.insert(object, data);
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BM25Error::Generic { .. }));
    }

    // Reload from the old manifest plus the orphaned new objects: the
    // orphans are invisible and the previous snapshot is complete.
    let loaded = load_from(&crashed).await;
    assert_eq!(loaded.len(), 1);
    for term in ["alpha", "bravo"] {
        assert!(
            loaded.search(term, 10, None).iter().any(|(id, _)| *id == 1),
            "doc 1 must still be found via '{term}' after the crash"
        );
    }
    assert!(
        loaded.search("zulu", 10, None).is_empty(),
        "uncommitted doc 2 must stay invisible"
    );

    // The interrupted flush retries cleanly and converges.
    let mut recovered_store = crashed;
    assert!(flush_to(&index, &mut recovered_store, 4).await.saved);
    let recovered = load_from(&recovered_store).await;
    assert_eq!(recovered.len(), 2);
    for (doc, term) in [(1, "alpha"), (1, "bravo"), (2, "alpha"), (2, "zulu")] {
        assert!(
            recovered
                .search(term, 10, None)
                .iter()
                .any(|(id, _)| *id == doc),
            "doc {doc} must be found via '{term}' after recovery"
        );
    }
}

/// Cancellation (the flush future is dropped at an await point) before
/// the manifest commit must leave everything retryable: no metadata
/// version is claimed, no bucket is marked clean, and a retry followed by
/// a reload converges on the complete new snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancelled_flush_before_commit_stays_retryable() {
    let (index, store) = build_migration_scenario().await;
    let index = Arc::new(index);
    let persisted = Arc::new(std::sync::Mutex::new(store));
    let metadata_entered = Arc::new(tokio::sync::Notify::new());

    let flushing = {
        let index = index.clone();
        let persisted = persisted.clone();
        let metadata_entered = metadata_entered.clone();
        tokio::spawn(async move {
            index
                .flush_with(
                    3,
                    move |_data| {
                        let metadata_entered = metadata_entered.clone();
                        async move {
                            metadata_entered.notify_one();
                            std::future::pending::<Result<(), BoxError>>().await
                        }
                    },
                    move |object, data| {
                        persisted.lock().unwrap().buckets.insert(object, data);
                        std::future::ready(Ok(()))
                    },
                )
                .await
        })
    };

    metadata_entered.notified().await;
    flushing.abort();
    assert!(
        flushing
            .await
            .expect_err("flush should be cancelled")
            .is_cancelled()
    );
    assert!(
        index.has_pending_metadata_flush(),
        "cancellation before the commit must not claim the metadata version"
    );
    assert!(
        index.has_dirty_buckets(),
        "cancellation before the commit must keep every bucket dirty"
    );

    // Before the retry, the durable store still loads the old snapshot.
    let crashed = persisted.lock().unwrap().clone();
    let loaded = load_from(&crashed).await;
    assert_eq!(loaded.len(), 1);

    // The retry persists one complete new snapshot.
    let mut retry_store = crashed;
    assert!(flush_to(&index, &mut retry_store, 4).await.saved);
    let reopened = load_from(&retry_store).await;
    for (doc, term) in [(1, "alpha"), (1, "bravo"), (2, "alpha"), (2, "zulu")] {
        assert!(
            reopened
                .search(term, 10, None)
                .iter()
                .any(|(id, _)| *id == doc),
            "doc {doc} must be found via '{term}' after abort and retry"
        );
    }
}

/// After the manifest commit, the replaced objects are garbage. A crash
/// (or plain failure) before they are deleted must not affect reloads:
/// the manifest never references them.
#[tokio::test]
async fn test_reload_unaffected_when_obsolete_deletion_fails() {
    let (index, mut store) = build_migration_scenario().await;

    // Flush, but "crash" between the manifest commit and the cleanup:
    // keep every obsolete object in the store.
    let outcome;
    let mut meta_buf: Vec<u8> = Vec::new();
    {
        let buckets = &mut store.buckets;
        outcome = index
            .flush_with(
                3,
                |data| {
                    meta_buf = data;
                    std::future::ready(Ok(()))
                },
                |object, data| {
                    buckets.insert(object, data);
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap();
    }
    assert!(outcome.saved);
    assert!(
        !outcome.obsolete.is_empty(),
        "the rewritten bucket's previous object must be reported obsolete"
    );
    for object in &outcome.obsolete {
        assert!(
            store.buckets.contains_key(object),
            "test setup: obsolete {object:?} must still exist in the store"
        );
    }
    store.metadata = meta_buf;

    // Reload with the leaked garbage still present: invisible.
    let loaded = load_from(&store).await;
    assert_eq!(loaded.len(), 2);
    for (doc, term) in [(1, "alpha"), (1, "bravo"), (2, "alpha"), (2, "zulu")] {
        assert!(
            loaded
                .search(term, 10, None)
                .iter()
                .any(|(id, _)| *id == doc),
            "doc {doc} must be found via '{term}' with garbage objects present"
        );
    }
    assert!(
        !loaded.has_dirty_buckets(),
        "leaked garbage must not dirty anything on load"
    );
}

#[test]
fn test_remove_replay_cleans_postings_after_doc_tokens_are_already_gone() {
    let index = BM25Index::new("idempotent_remove".to_string(), default_tokenizer(), None);
    let text = "alpha bravo charlie";
    index.insert(42, text, 1).unwrap();

    // Model a crash after the logical document membership was removed but
    // before the original-text postings were cleaned.
    let tokens = index.doc_tokens.remove(&42).unwrap().1;
    index
        .total_tokens
        .fetch_sub(tokens as u64, Ordering::Relaxed);
    assert!(
        index
            .postings
            .iter()
            .any(|posting| { posting.1.iter().any(|(doc_id, _)| *doc_id == 42) })
    );
    let after_first = index.stats();

    // Recovery replays the correct historical text. The logical removal
    // reports false and does not advance statistics twice, but it still
    // purges every stale posting and bucket doc-id reference.
    assert!(!index.remove(42, text, 3));
    let after_replay = index.stats();
    assert_eq!(after_replay.version, after_first.version);
    assert_eq!(after_replay.delete_count, after_first.delete_count);
    assert_eq!(after_replay.last_deleted, after_first.last_deleted);
    assert!(
        index
            .postings
            .iter()
            .all(|posting| { posting.1.iter().all(|(doc_id, _)| *doc_id != 42) })
    );
    assert!(
        index
            .buckets
            .iter()
            .all(|bucket| !bucket.doc_ids.contains(&42))
    );
}

/// Buckets without tokens other than the tail leave the manifest, including
/// empty objects written by earlier releases; a metadata-only shell, whose
/// placeholders have no tokens yet, keeps them.
#[tokio::test]
async fn test_flush_drops_empty_non_tail_buckets_but_shell_keeps_placeholders() {
    let index = BM25Index::new(
        "empty_buckets".to_string(),
        default_tokenizer(),
        Some(BM25Config {
            bucket_overload_size: 1,
            ..Default::default()
        }),
    );
    index.insert(1, "alpha", 0).unwrap();
    index.insert(2, "bravo", 0).unwrap();
    index.insert(3, "charlie", 0).unwrap();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    assert_eq!(store.buckets.len(), 3);

    // An earlier release removed document 1 and wrote bucket 0 back empty.
    let bucket0 = *store.buckets.keys().find(|o| o.bucket_id == 0).unwrap();
    store.buckets.insert(
        bucket0,
        encode_bucket_owned(FxHashMap::default(), FxHashMap::default()),
    );

    // A shell flushing only metadata keeps every committed object.
    let shell = BM25Index::load_metadata(default_tokenizer(), &store.metadata[..]).unwrap();
    shell.update_metadata(|m| m.stats.version += 1);
    let outcome = flush_to(&shell, &mut store, 2).await;
    assert!(outcome.saved && outcome.obsolete.is_empty());
    assert_eq!(store.buckets.len(), 3);

    // A fully loaded index drops the empty bucket at its next flush.
    let loaded = load_from(&store).await;
    assert_eq!(loaded.len(), 2);
    loaded.insert(4, "charlie delta", 3).unwrap();
    let outcome = flush_to(&loaded, &mut store, 4).await;
    assert!(outcome.obsolete.contains(&bucket0));
    assert!(!loaded.buckets.contains_key(&0));
    assert!(store.buckets.keys().all(|o| o.bucket_id != 0));

    // Emptying a non-tail bucket drops it; the tail stays even when empty.
    let tail = loaded.max_bucket_id.load(Ordering::Relaxed);
    assert!(loaded.remove(2, "bravo", 5));
    assert!(loaded.remove(4, "charlie delta", 5));
    flush_to(&loaded, &mut store, 6).await;
    let reloaded = load_from(&store).await;
    assert_eq!(reloaded.len(), 1);
    assert_eq!(reloaded.search("charlie", 10, None).len(), 1);
    let ids: BTreeSet<u32> = store.buckets.keys().map(|o| o.bucket_id).collect();
    assert!(ids.contains(&tail));
    assert!(!ids.contains(&1));
    reloaded.insert(5, "echo", 7).unwrap();
    assert_eq!(reloaded.search("echo", 10, None).len(), 1);
}

#[test]
fn test_remove_with_wrong_text_sweeps_so_reinsert_does_not_duplicate() {
    let index = BM25Index::new("sweep".to_string(), default_tokenizer(), None);
    let text = "alpha bravo charlie";
    index.insert(7, text, 1).unwrap();
    index.insert(8, "alpha delta", 1).unwrap();
    let expected = index.search("alpha bravo", 10, None);

    // Partly matching text: `alpha` is removed by text, the rest by sweep.
    assert!(index.remove(7, "alpha", 2));
    assert!(
        index
            .postings
            .iter()
            .all(|posting| posting.1.iter().all(|(doc_id, _)| *doc_id != 7))
    );
    assert!(
        index
            .buckets
            .iter()
            .all(|bucket| !bucket.doc_ids.contains(&7))
    );

    index.insert(7, text, 3).unwrap();
    assert_eq!(index.postings.get("alpha").unwrap().1.len(), 2);
    assert_eq!(index.search("alpha bravo", 10, None), expected);
}

/// Compaction needs no special write ordering under the manifest
/// protocol: the repacked layout becomes visible atomically with the
/// manifest commit, every pre-compaction object is reported obsolete,
/// and a reload sees the identical index.
#[tokio::test]
async fn test_compaction_flush_commits_atomically_and_reports_obsolete() {
    // Fragmented index: tiny limit creates many buckets.
    let small_config = BM25Config {
        bm25: BM25Params::default(),
        bucket_overload_size: 50,
    };
    let index = BM25Index::new(
        "compact_manifest".to_string(),
        default_tokenizer(),
        Some(small_config),
    );
    let docs = [
        (1, "the quick brown fox jumps over the lazy dog"),
        (2, "a fast brown fox runs past the lazy dog"),
        (3, "the lazy dog sleeps all day long"),
        (4, "quick brown foxes are rare in the wild"),
    ];
    for (id, text) in &docs {
        index.insert(*id, text, 0).unwrap();
    }

    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let objects_before: Vec<BucketObject> = store.buckets.keys().copied().collect();
    assert!(objects_before.len() > 3, "scenario must be fragmented");

    // Reload with a large limit and compact.
    let mut loaded = BM25Index::load_metadata(default_tokenizer(), &store.metadata[..]).unwrap();
    loaded.config.bucket_overload_size = 1024 * 512;
    loaded.metadata.write().config.bucket_overload_size = 1024 * 512;
    loaded
        .load_buckets(async |object| Ok(store.buckets.get(&object).cloned()))
        .await
        .unwrap();
    let results_before = loaded.search("fox", 20, None);
    let (old_count, new_count) = loaded.compact_buckets();
    assert!(new_count < old_count);

    let outcome = flush_to(&loaded, &mut store, 2).await;
    assert!(outcome.saved);
    for object in &objects_before {
        assert!(
            outcome.obsolete.contains(object),
            "pre-compaction {object:?} must be reported obsolete"
        );
        assert!(
            !store.buckets.contains_key(object),
            "pre-compaction {object:?} must be deleted from the store"
        );
    }

    // The compacted layout reloads and searches identically.
    let reloaded = load_from(&store).await;
    let mut before_ids: Vec<u64> = results_before.iter().map(|(id, _)| *id).collect();
    let mut after_ids: Vec<u64> = reloaded
        .search("fox", 20, None)
        .iter()
        .map(|(id, _)| *id)
        .collect();
    before_ids.sort_unstable();
    after_ids.sort_unstable();
    assert_eq!(before_ids, after_ids);
}

#[test]
fn test_search_count_only_counts_scored_queries() {
    let index = create_test_index();
    assert_eq!(index.stats().search_count, 0);

    // top_k == 0 short-circuits and is not counted.
    assert!(index.search("fox", 0, None).is_empty());
    assert!(
        index
            .try_search_advanced("fox", 0, None)
            .unwrap()
            .is_empty()
    );
    assert_eq!(index.stats().search_count, 0);

    // Parse failures are not counted.
    let flood = format!("x{}", ")".repeat(100));
    assert!(index.try_search_advanced(&flood, 10, None).is_err());
    assert_eq!(index.stats().search_count, 0);

    // Successful searches are counted.
    assert!(!index.search("fox", 10, None).is_empty());
    assert!(!index.search_advanced("fox AND lazy", 10, None).is_empty());
    assert_eq!(index.stats().search_count, 2);
}

#[test]
fn test_token_counters_stay_consistent_under_concurrent_insert_remove() {
    use std::thread;

    let index = Arc::new(BM25Index::new(
        "avg_convergence".to_string(),
        default_tokenizer(),
        None,
    ));
    // A stable base corpus so the index is never empty.
    index
        .insert(1, "base document alpha bravo charlie", 0)
        .unwrap();
    index.insert(2, "base document delta echo", 0).unwrap();

    const ITERS: usize = 300;
    let mut handles = Vec::new();
    for t in 0..4u64 {
        let index = index.clone();
        handles.push(thread::spawn(move || {
            let id = 100 + t;
            let text = format!("churn document number {id} with token payload");
            for _ in 0..ITERS {
                index.insert(id, &text, 0).unwrap();
                assert!(index.remove(id, &text, 0));
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    // Once all writers drained, `total_tokens` must account for exactly
    // the documents still present, and the reported average must be the
    // quotient of the two (it is derived from them, never cached).
    let total = index.total_tokens.load(Ordering::Relaxed);
    let live_tokens: usize = index.doc_tokens.iter().map(|entry| *entry.value()).sum();
    assert_eq!(
        total, live_tokens as u64,
        "total_tokens {total} != sum of live doc_tokens {live_tokens}"
    );
    let expected = total as f32 / index.doc_tokens.len() as f32;
    assert_eq!(index.stats().avg_doc_tokens, expected);
}

#[test]
fn test_bm25_params() {
    // 使用默认参数
    let default_index = create_test_index();

    // 搜索相同的查询
    let default_results = default_index.search("fox", 10, None);
    let custom_results = default_index.search("fox", 10, Some(BM25Params { k1: 1.5, b: 0.75 }));

    // 验证结果数量相同但分数不同
    assert_eq!(default_results.len(), custom_results.len());

    // 至少有一个文档的分数应该不同
    let mut scores_different = false;
    for i in 0..default_results.len() {
        if (default_results[i].1 - custom_results[i].1).abs() > 0.001 {
            scores_different = true;
            break;
        }
    }
    assert!(scores_different);
}

#[test]
fn test_invalid_bm25_params_do_not_produce_non_finite_scores() {
    let index = create_test_index();
    // A document longer than average that mentions the query term twice:
    // the only shape for which an unclamped `k1` overflows *both* sides of
    // the ratio and yields `NaN` rather than a harmless `0.0`.
    index
        .insert(
            5,
            "The fox and the other fox watched the quick brown fox run past the lazy dog again",
            0,
        )
        .unwrap();

    // `f32::MAX` (and any other large-but-finite `k1`) passes an
    // `is_finite` guard, yet overflows the unclamped formula to
    // `inf / inf = NaN`; `b` outside `[0, 1]` distorts the length
    // normalization the same way.
    let hostile = [
        BM25Params {
            k1: f32::NAN,
            b: f32::INFINITY,
        },
        BM25Params {
            k1: f32::MAX,
            b: 1.0,
        },
        BM25Params {
            k1: f32::MAX,
            b: f32::MAX,
        },
        BM25Params { k1: 1e30, b: 1.0 },
        BM25Params { k1: -1e30, b: -5.0 },
    ];

    for params in hostile {
        let results = index.search("fox", 10, Some(params.clone()));
        assert!(!results.is_empty(), "no results for {params:?}");
        assert!(
            results.iter().all(|(_, score)| score.is_finite()),
            "non-finite score for {params:?}: {results:?}"
        );
    }
}

/// Regression: a hostile-but-finite `k1` used to make `score_term` emit
/// `NaN` for every document longer than average, which then fed
/// `select_nth_unstable_by` a non-transitive comparator (`NaN` compared
/// equal to everything, so ordering fell back to id order against it while
/// the remaining pairs stayed score-ordered). That can panic with
/// "user-provided comparison function does not correctly implement a total
/// order" and otherwise returns arbitrary, partly non-finite rankings.
///
/// The corpus is large enough for `top_k_results` to take the partial-sort
/// path, and the documents deliberately straddle the average length with
/// term frequencies `>= 2`.
#[test]
fn test_large_finite_bm25_params_keep_ranking_total_and_finite() {
    let index = BM25Index::new("hostile_params".to_string(), default_tokenizer(), None);
    for id in 0..2_000u64 {
        // Half the corpus is long with a repeated query term (tf >= 2 and
        // doc_len > avg), the other half is short.
        let text = if id % 2 == 0 {
            format!("alpha alpha beta gamma delta epsilon zeta doc{id} padding padding padding")
        } else {
            format!("alpha doc{id}")
        };
        index.insert(id, &text, 0).unwrap();
    }

    let results = index.search(
        "alpha",
        1_000,
        Some(BM25Params {
            k1: f32::MAX,
            b: 1.0,
        }),
    );

    assert_eq!(results.len(), 1_000);
    assert!(
        results.iter().all(|(_, score)| score.is_finite()),
        "hostile k1 produced non-finite scores: {} of {}",
        results.iter().filter(|(_, s)| !s.is_finite()).count(),
        results.len()
    );
    assert!(
        results.windows(2).all(|w| w[0].1 >= w[1].1),
        "results are not sorted by descending score"
    );
}

/// `compare_scored_docs` must be a total order for *every* input, so no
/// future scoring change can trip the standard-library sorts. The triple
/// below is the exact cycle the old comparator produced: `cmp(x, n)` and
/// `cmp(n, y)` both said `Less` while `cmp(x, y)` said `Greater`.
#[test]
fn test_compare_scored_docs_is_a_total_order_with_nan() {
    let x = (1u64, 1.0f32);
    let n = (2u64, f32::NAN);
    let y = (3u64, 2.0f32);
    let entries = [
        x,
        n,
        y,
        (4, f32::INFINITY),
        (5, f32::NEG_INFINITY),
        (6, 1.0),
    ];

    for a in entries {
        for b in entries {
            for c in entries {
                let ab = BM25Index::<TokenizerChain>::compare_scored_docs(&a, &b);
                let bc = BM25Index::<TokenizerChain>::compare_scored_docs(&b, &c);
                let ac = BM25Index::<TokenizerChain>::compare_scored_docs(&a, &c);
                // Antisymmetry.
                assert_eq!(
                    ab.reverse(),
                    BM25Index::<TokenizerChain>::compare_scored_docs(&b, &a),
                    "not antisymmetric for {a:?} / {b:?}"
                );
                // Transitivity.
                if ab != std::cmp::Ordering::Greater && bc != std::cmp::Ordering::Greater {
                    assert_ne!(
                        ac,
                        std::cmp::Ordering::Greater,
                        "cycle: {a:?} <= {b:?} <= {c:?} but {a:?} > {c:?}"
                    );
                }
            }
        }
    }

    // NaN scores rank last, never first.
    let mut entries = [n, x, y];
    entries.sort_unstable_by(BM25Index::<TokenizerChain>::compare_scored_docs);
    assert_eq!(entries[0].0, 3);
    assert_eq!(entries[2].0, 2);
}

#[test]
fn test_search_advanced() {
    let index = create_test_index();

    // 测试简单的 Term 查询
    let results = index.search_advanced("fox", 10, None);
    assert_eq!(results.len(), 3); // 应该找到3个包含"fox"的文档

    // 测试 AND 查询
    let results = index.search_advanced("fox AND lazy", 10, None);
    assert_eq!(results.len(), 2); // 文档1和2同时包含"fox"和"lazy"
    assert!(results.iter().any(|(id, _)| *id == 1));
    assert!(results.iter().any(|(id, _)| *id == 2));

    // 测试 OR 查询
    let results = index.search_advanced("quick OR fast", 10, None);
    assert_eq!(results.len(), 3); // 文档1包含"quick"，文档2包含"fast"，文档4包含"quick"
    assert!(results.iter().any(|(id, _)| *id == 1));
    assert!(results.iter().any(|(id, _)| *id == 2));
    assert!(results.iter().any(|(id, _)| *id == 4));

    // 测试 NOT 查询
    let results = index.search_advanced("dog AND NOT lazy", 10, None);
    assert_eq!(results.len(), 0); // 所有包含"dog"的文档也包含"lazy"

    // 测试复杂的嵌套查询
    let results = index.search_advanced("(quick OR fast) AND fox", 10, None);
    assert_eq!(results.len(), 3); // 文档1、2和4

    // 测试更复杂的嵌套查询
    let results = index.search_advanced("(brown AND fox) AND NOT (rare OR sleeps)", 10, None);
    assert_eq!(results.len(), 2); // 文档1和2，排除了包含"rare"的文档4和包含"sleeps"的文档3
    assert!(results.iter().any(|(id, _)| *id == 1));
    assert!(results.iter().any(|(id, _)| *id == 2));

    // 测试空查询
    let results = index.search_advanced("", 10, None);
    assert_eq!(results.len(), 0);

    // 测试无匹配查询
    let results = index.search_advanced("elephant AND giraffe", 10, None);
    assert_eq!(results.len(), 0);
}

#[test]
fn test_search_advanced_with_parentheses() {
    let index = create_test_index();

    // 测试带括号的复杂查询
    let results = index.search_advanced("(fox AND quick) OR (dog AND sleeps)", 10, None);
    assert_eq!(results.len(), 3); // 文档1, 3, 4
    assert!(results.iter().any(|(id, _)| *id == 1));
    assert!(results.iter().any(|(id, _)| *id == 3));
    assert!(results.iter().any(|(id, _)| *id == 4));

    // 测试多层嵌套括号
    let results = index.search_advanced(
        "((brown AND fox) OR (lazy AND sleeps)) AND NOT rare",
        10,
        None,
    );
    assert_eq!(results.len(), 3); // 文档1、2和3，排除了包含"rare"的文档4
    assert!(results.iter().any(|(id, _)| *id == 1));
    assert!(results.iter().any(|(id, _)| *id == 2));
    assert!(results.iter().any(|(id, _)| *id == 3));

    // 测试带括号的 NOT 查询
    let results = index.search_advanced("dog AND NOT (quick OR fast)", 10, None);
    assert_eq!(results.len(), 1); // 只有文档3，因为它包含"dog"但不包含"quick"或"fast"
    assert_eq!(results[0].0, 3);
}

#[test]
fn test_search_advanced_score_ordering() {
    let index = create_test_index();

    // 测试分数排序 - 包含更多匹配词的文档应该排在前面
    let results = index.search_advanced("quick OR fox OR dog", 10, None);
    assert!(results.len() >= 3);

    // 文档1应该排在最前面，因为它同时包含所有三个词
    assert_eq!(results[0].0, 1);

    // 测试 top_k 限制
    let results = index.search_advanced("dog", 2, None);
    assert_eq!(results.len(), 2); // 应该只返回2个结果，尽管有3个文档包含"dog"
}

#[test]
fn test_search_vs_search_advanced() {
    let index = create_test_index();

    // 对于简单查询，search 和 search_advanced 应该返回相似的结果
    let simple_results = index.search("fox", 10, None);
    let advanced_results = index.search_advanced("fox", 10, None);

    assert_eq!(simple_results.len(), advanced_results.len());

    // 检查文档ID是否匹配（不检查分数，因为实现可能略有不同）
    let simple_ids: Vec<u64> = simple_results.iter().map(|(id, _)| *id).collect();
    let advanced_ids: Vec<u64> = advanced_results.iter().map(|(id, _)| *id).collect();

    assert_eq!(simple_ids.len(), advanced_ids.len());
    for id in simple_ids {
        assert!(advanced_ids.contains(&id));
    }

    // 测试多词查询 - search 将它们视为 OR，search_advanced 也应该如此
    let simple_results = index.search("quick fox", 10, None);
    let advanced_results = index.search_advanced("quick OR fox", 10, None);

    // 检查文档ID是否匹配
    let simple_ids: Vec<u64> = simple_results.iter().map(|(id, _)| *id).collect();
    let advanced_ids: Vec<u64> = advanced_results.iter().map(|(id, _)| *id).collect();

    assert_eq!(simple_ids.len(), advanced_ids.len());
    for id in simple_ids {
        assert!(advanced_ids.contains(&id));
    }
}

#[test]
fn test_search_not_alone() {
    let index = create_test_index();
    // NOT fox => 返回所有不含 fox 的文档 (文档3)
    let results = index.search_advanced("NOT fox", 10, None);
    let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![3]);
}

#[test]
fn test_double_negation_inside_and() {
    let index = create_test_index();

    // dog AND NOT (NOT lazy) === dog AND lazy => 文档1、2、3
    let results = index.search_advanced("dog AND NOT (NOT lazy)", 10, None);
    let mut ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);

    // NOT (NOT fox) === fox => 文档1、2、4
    let results = index.search_advanced("NOT (NOT fox)", 10, None);
    let mut ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 4]);
}

#[test]
fn test_nested_not_complement_guard_inside_and_filter() {
    let index = BM25Index::new("nested_not_guard".to_string(), default_tokenizer(), None);
    for id in 0..=MAX_NOT_COMPLEMENT_DOCS as u64 {
        index.insert(id, "hello world", 0).unwrap();
    }

    let matched = index
        .try_search_advanced("hello AND NOT (NOT world)", 10, None)
        .unwrap();
    assert_eq!(matched.len(), 10);
    assert!(index.try_search_advanced("NOT world", 10, None).is_err());
}

#[test]
fn test_not_first_in_and_matches_not_last() {
    let index = create_test_index();

    // NOT lazy AND fox === fox AND NOT lazy => 只有文档4
    let a = index.search_advanced("NOT lazy AND fox", 10, None);
    let b = index.search_advanced("fox AND NOT lazy", 10, None);
    let mut ids_a: Vec<u64> = a.iter().map(|(id, _)| *id).collect();
    let mut ids_b: Vec<u64> = b.iter().map(|(id, _)| *id).collect();
    ids_a.sort_unstable();
    ids_b.sort_unstable();
    assert_eq!(ids_a, vec![4]);
    assert_eq!(ids_a, ids_b);
}

#[test]
fn test_and_with_only_not_subqueries() {
    let index = create_test_index();

    // NOT fox AND NOT rare => 不含 fox 也不含 rare 的文档 (文档3)
    let results = index.search_advanced("NOT fox AND NOT rare", 10, None);
    let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![3]);
}

#[test]
fn test_reinsert_after_remove_with_wrong_text() {
    let index = BM25Index::new("reinsert".to_string(), default_tokenizer(), None);
    index.insert(1, "dog dog cat", 0).unwrap();

    // Remove with non-original text: the "dog"/"cat" postings keep stale entries.
    assert!(index.remove(1, "bird", 0));

    // Re-insert the same id with a different "dog" frequency; the stale
    // posting entry must not be double-counted nor inflate df.
    index.insert(1, "dog dog dog mouse", 0).unwrap();

    let results = index.search("dog", 10, None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1);
    assert!(
        results[0].1.is_finite() && results[0].1 > 0.0,
        "score must be positive, got {}",
        results[0].1
    );

    // Removing with the correct text must clear all entries for the doc,
    // including stale duplicates, and drop the now-empty posting.
    assert!(index.remove(1, "dog dog dog mouse", 0));
    assert!(index.search("dog", 10, None).is_empty());
    assert!(index.postings.get("dog").is_none());
}

#[test]
fn test_concurrent_insert_remove_shared_token() {
    use std::thread;

    // Regression test: remove() must not drop a posting that a concurrent
    // insert just appended to (the empty-check and the removal must be
    // atomic). Two writers share the token "shared"; the reader-side
    // assertion in thread B would fail if the posting got lost.
    let index = Arc::new(BM25Index::new(
        "concurrent_shared".to_string(),
        default_tokenizer(),
        None,
    ));

    const ITERS: usize = 500;
    let a = {
        let index = index.clone();
        thread::spawn(move || {
            for _ in 0..ITERS {
                index.insert(2, "shared alpha", 0).unwrap();
                assert!(index.remove(2, "shared alpha", 0));
            }
        })
    };
    let b = {
        let index = index.clone();
        thread::spawn(move || {
            for _ in 0..ITERS {
                index.insert(3, "shared beta", 0).unwrap();
                let results = index.search("shared", 10, None);
                assert!(
                    results.iter().any(|(id, _)| *id == 3),
                    "doc 3 must stay searchable while it exists"
                );
                assert!(index.remove(3, "shared beta", 0));
            }
        })
    };

    a.join().unwrap();
    b.join().unwrap();

    // After all churn the index must still accept and find new documents.
    index.insert(10, "shared final", 0).unwrap();
    let results = index.search("shared", 10, None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 10);
}

/// Regression: `remove()` strips a token from its bucket based on the
/// `removed_postings` snapshot taken a few lines earlier. When a
/// concurrent `insert` re-created that posting in the *same* bucket in
/// between, the token must stay listed — `serialize_bucket` only walks
/// `bucket.tokens`, so a live posting that no bucket lists is never
/// persisted and the term is silently lost on the next flush + reload.
///
/// The interleaving is forced, never raced: `remove()` drops the emptied
/// posting from `postings` *before* it touches `buckets`, so holding
/// bucket 0's shard guard parks it precisely between those two steps. The
/// guard is taken before the remover starts, and the posting vanishing
/// from `postings` is the observable end of the first step — no sleeps and
/// no timing-dependent assertions.
#[tokio::test]
async fn test_remove_keeps_bucket_token_recreated_by_concurrent_insert() {
    use std::thread;
    use std::time::{Duration, Instant};

    let index = Arc::new(BM25Index::new(
        "recreated_posting".to_string(),
        default_tokenizer(),
        None,
    ));
    index.insert(1, "alpha", 0).unwrap();
    assert_eq!(index.postings.get("alpha").unwrap().0, 0);

    let remover = {
        // Parks `remove()` at its bucket-update phase.
        let mut bucket0 = index.buckets.get_mut(&0).unwrap();
        let remover = {
            let index = index.clone();
            thread::spawn(move || index.remove(1, "alpha", 0))
        };

        let deadline = Instant::now() + Duration::from_secs(30);
        while index.postings.contains_key("alpha") {
            assert!(
                Instant::now() < deadline,
                "remove() never dropped the emptied posting"
            );
            thread::yield_now();
        }

        // What the concurrent `insert`'s phase 1 does: doc 2 re-creates
        // the posting in bucket 0, which still lists the token, so the
        // insert's phase 2 would only bump this bucket's accounting.
        index
            .postings
            .insert("alpha".to_string(), (0, vec![(2u64, 1usize)]));
        index.doc_tokens.insert(2, 1);
        index.total_tokens.fetch_add(1, Ordering::Relaxed);
        bucket0.doc_ids.insert(2);
        bucket0.mark_dirty();
        assert!(bucket0.tokens.contains("alpha"));
        remover
    };
    assert!(remover.join().unwrap());

    // The invariant: every live posting is listed by exactly the bucket it
    // names, otherwise no bucket would ever serialize it.
    let posting = index.postings.get("alpha").expect("posting must survive");
    assert_eq!(posting.0, 0);
    drop(posting);
    assert!(
        index.buckets.get(&0).unwrap().tokens.contains("alpha"),
        "the owning bucket must keep listing a posting a concurrent insert re-created"
    );

    // ... and that bucket carries the term through a flush + reload.
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let reloaded = load_from(&store).await;
    let found: Vec<u64> = reloaded
        .search("alpha", 10, None)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        found,
        vec![2],
        "the re-created term must stay searchable after a flush + reload"
    );
}

#[tokio::test]
async fn test_serialization_with_buckets() {
    // 创建一个带有小桶大小的索引，强制触发分桶
    let tokenizer = default_tokenizer();
    let config = BM25Config {
        bm25: BM25Params::default(),
        bucket_overload_size: 100, // 非常小的桶大小，强制分桶
    };
    let index = BM25Index::new(
        "test_bucket_serialization".to_string(),
        tokenizer,
        Some(config),
    );

    // 添加大量文档，确保触发分桶
    let test_docs = vec![
        (
            1,
            "The quick brown fox jumps over the lazy dog in the forest",
        ),
        (2, "A fast brown fox runs past the lazy dog near the river"),
        (3, "The lazy dog sleeps all day under the warm sun"),
        (4, "Quick brown foxes are rare in the wild mountain regions"),
        (5, "Many foxes hunt at night when the moon is bright"),
        (6, "Dogs and cats are common pets in modern households"),
        (7, "Wild animals like foxes and wolves roam the countryside"),
        (8, "The forest is home to many different species of animals"),
        (9, "Lazy afternoon naps are enjoyed by both dogs and cats"),
        (
            10,
            "Quick movements help foxes catch their prey efficiently",
        ),
    ];

    for (id, text) in test_docs {
        index.insert(id, text, 0).unwrap();
    }

    // 验证确实创建了多个桶
    let original_stats = index.stats();
    println!(
        "Original index has {} buckets",
        original_stats.max_bucket_id + 1
    );
    assert!(original_stats.max_bucket_id > 0, "应该创建了多个桶");

    // 保存索引
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 100).await;

    // 验证保存了正确数量的桶
    println!("Saved {} document buckets", store.buckets.len());
    assert!(store.buckets.len() > 1, "应该保存了多个文档桶");

    // 验证每个桶的内容
    for (object, data) in &store.buckets {
        let bucket: BucketOwned = cbor2::from_reader(&data[..]).unwrap();
        println!("Document bucket {object:?} {:?}", bucket.doc_tokens);
        assert!(!bucket.postings.is_empty());

        // 验证倒排索引结构
        for (term, (bucket_ref, doc_list)) in bucket.postings {
            assert_eq!(
                bucket_ref, object.bucket_id,
                "术语 {} 的桶引用应该指向当前桶",
                term
            );
            assert!(!doc_list.is_empty(), "术语 {} 的文档列表不应该为空", term);

            for (doc_id, freq) in doc_list.iter() {
                assert!(*freq > 0, "文档 {} 中术语 {} 的频率应该大于0", doc_id, term);
            }
        }

        // 验证文档token数量的合理性
        for (doc_id, token_count) in bucket.doc_tokens {
            assert!(token_count > 0, "文档 {} 的token数量应该大于0", doc_id);
        }
    }

    // 加载索引
    let loaded_index = load_from(&store).await;

    // 验证加载的索引基本信息
    assert_eq!(loaded_index.len(), index.len(), "文档数量应该一致");

    let loaded_stats = loaded_index.stats();
    assert_eq!(
        loaded_stats.max_bucket_id, original_stats.max_bucket_id,
        "最大桶ID应该一致"
    );
    assert_eq!(
        loaded_stats.max_document_id, original_stats.max_document_id,
        "最大文档ID应该一致"
    );
    assert!(
        (loaded_stats.avg_doc_tokens - original_stats.avg_doc_tokens).abs() < 0.01,
        "平均文档token数应该基本一致"
    );

    // 验证每个文档的token数量
    for i in 1..=10 {
        let original_tokens = index.get_doc_tokens(i);
        let loaded_tokens = loaded_index.get_doc_tokens(i);
        assert_eq!(
            original_tokens, loaded_tokens,
            "文档 {} 的token数量应该一致",
            i
        );
    }

    // 验证多种搜索查询的结果一致性
    let test_queries = vec![
        "fox",
        "dog",
        "lazy",
        "quick brown",
        "fox AND dog",
        "brown OR lazy",
        "fox AND NOT lazy",
        "(quick OR fast) AND fox",
    ];

    for query in test_queries {
        println!("Testing query: {}", query);

        let original_results =
            if query.contains("AND") || query.contains("OR") || query.contains("NOT") {
                index.search_advanced(query, 10, None)
            } else {
                index.search(query, 10, None)
            };

        let loaded_results =
            if query.contains("AND") || query.contains("OR") || query.contains("NOT") {
                loaded_index.search_advanced(query, 10, None)
            } else {
                loaded_index.search(query, 10, None)
            };

        assert_eq!(
            original_results.len(),
            loaded_results.len(),
            "查询 '{}' 的结果数量应该一致",
            query
        );

        // 按文档ID排序后比较
        let mut orig_sorted = original_results.clone();
        let mut loaded_sorted = loaded_results.clone();
        orig_sorted.sort_by_key(|a| a.0);
        loaded_sorted.sort_by_key(|a| a.0);

        for i in 0..orig_sorted.len() {
            assert_eq!(
                orig_sorted[i].0, loaded_sorted[i].0,
                "查询 '{}' 的第 {} 个结果文档ID应该一致",
                query, i
            );
            assert!(
                (orig_sorted[i].1 - loaded_sorted[i].1).abs() < 0.001,
                "查询 '{}' 的第 {} 个结果分数应该基本一致，原始: {}, 加载: {}",
                query,
                i,
                orig_sorted[i].1,
                loaded_sorted[i].1
            );
        }
    }

    // 验证倒排索引的完整性 - 检查一些关键词的倒排列表
    let key_terms = vec!["fox", "dog", "lazy", "brown", "quick"];
    for term in key_terms {
        let original_postings = index.postings.get(term);
        let loaded_postings = loaded_index.postings.get(term);

        match (original_postings, loaded_postings) {
            (Some(orig), Some(loaded)) => {
                // 比较倒排列表内容
                assert_eq!(
                    orig.1.len(),
                    loaded.1.len(),
                    "术语 '{}' 的倒排列表长度应该一致",
                    term
                );

                let mut orig_docs: Vec<_> = orig.1.iter().collect();
                let mut loaded_docs: Vec<_> = loaded.1.iter().collect();
                orig_docs.sort();
                loaded_docs.sort();

                for i in 0..orig_docs.len() {
                    assert_eq!(
                        orig_docs[i], loaded_docs[i],
                        "术语 '{}' 的第 {} 个倒排项应该一致",
                        term, i
                    );
                }
            }
            (None, None) => {
                // 都没有该术语，正常
            }
            _ => {
                panic!("术语 '{}' 在原始索引和加载索引中的存在性不一致", term);
            }
        }
    }

    println!("所有分桶序列化测试通过！");

    {
        // 测试只加载部分桶的情况（只读部分加载）
        let tokenizer = default_tokenizer();
        let partial_index = BM25Index::load_all(tokenizer, &store.metadata[..], async |object| {
            // 只加载桶0的文档
            if object.bucket_id == 0 {
                Ok(store.buckets.get(&object).cloned())
            } else {
                Ok(None)
            }
        })
        .await
        .unwrap();

        // 部分加载会载入桶0 posting 需要的文档长度；如果桶0包含高频词，
        // 它可能覆盖全部文档，但搜索结果仍不应超过完整索引。
        assert!(partial_index.len() <= index.len());

        // 验证部分搜索结果
        let partial_results = partial_index.search("fox", 10, None);
        let full_results = index.search("fox", 10, None);

        // 部分结果应该是完整结果的子集
        assert!(partial_results.len() <= full_results.len());

        for (doc_id, _) in partial_results {
            assert!(
                full_results.iter().any(|(id, _)| *id == doc_id),
                "部分加载结果中的文档 {} 应该存在于完整结果中",
                doc_id
            );
        }

        println!("加载部分分桶测试通过！");
    }
}

#[tokio::test]
async fn test_partial_load_keeps_doc_tokens_with_existing_token_bucket() {
    let config = BM25Config {
        bm25: BM25Params::default(),
        bucket_overload_size: 64,
    };
    let index = BM25Index::new(
        "partial_load_doc_tokens".to_string(),
        default_tokenizer(),
        Some(config),
    );

    index.insert(1, "alpha bravo", 0).unwrap();
    let alpha_bucket = index.postings.get("alpha").unwrap().0;

    let filler_docs = [
        (2, "charlie delta echo foxtrot"),
        (3, "golf hotel india juliet"),
        (4, "kilo lima mike november"),
        (5, "oscar papa quebec romeo"),
        (6, "sierra tango uniform victor"),
        (7, "whiskey xray yankee zulu"),
    ];
    for (id, text) in filler_docs {
        index.insert(id, text, 0).unwrap();
    }
    assert!(index.stats().max_bucket_id > alpha_bucket);

    index.insert(99, "alpha alpha", 0).unwrap();
    assert_eq!(index.postings.get("alpha").unwrap().0, alpha_bucket);

    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;

    let partial_index =
        BM25Index::load_all(default_tokenizer(), &store.metadata[..], async |object| {
            if object.bucket_id == alpha_bucket {
                Ok(store.buckets.get(&object).cloned())
            } else {
                Ok(None)
            }
        })
        .await
        .unwrap();

    assert_eq!(partial_index.get_doc_tokens(99), Some(2));
    let results = partial_index.search("alpha", 10, None);
    assert!(results.iter().any(|(id, _)| *id == 99));
}

#[test]
fn test_no_excessive_small_buckets() {
    // Regression test: existing tokens in a bucket must NOT trigger migration,
    // otherwise each insert after the bucket reaches the limit creates many
    // tiny new buckets.
    let tokenizer = default_tokenizer();
    let config = BM25Config {
        bm25: BM25Params::default(),
        bucket_overload_size: 200, // small limit to trigger splits quickly
    };
    let index = BM25Index::new("small_bucket_test".to_string(), tokenizer, Some(config));

    // Insert many documents sharing common tokens
    let docs = vec![
        (1, "the quick brown fox"),
        (2, "the lazy brown dog"),
        (3, "the quick red cat"),
        (4, "a lazy brown fox jumps"),
        (5, "the brown dog runs fast"),
        (6, "a quick fox hunts at night"),
        (7, "the lazy cat sleeps all day"),
        (8, "brown dogs and brown cats"),
        (9, "quick movements help foxes"),
        (10, "the fast dog chases the fox"),
        (11, "lazy afternoons with brown dogs"),
        (12, "quick brown fox returns again"),
        (13, "the old brown dog rests"),
        (14, "a new quick fox appears"),
        (15, "brown and lazy describe the dog"),
    ];

    for (id, text) in &docs {
        index.insert(*id, text, 0).unwrap();
    }

    let stats = index.stats();
    let num_buckets = stats.max_bucket_id + 1;
    println!(
        "docs={}, buckets={}, max_bucket_id={}",
        docs.len(),
        num_buckets,
        stats.max_bucket_id
    );

    // With 15 short documents and 200-byte limit, we expect a modest number
    // of buckets — certainly not one per insert.
    assert!(
        (num_buckets as usize) < docs.len(),
        "Too many buckets ({num_buckets}) for {} documents — \
         existing tokens are likely being migrated incorrectly",
        docs.len()
    );

    // Verify all documents are still searchable
    for (id, text) in &docs {
        let first_word = text.split_whitespace().find(|w| w.len() > 2).unwrap();
        let results = index.search(first_word, 20, None);
        assert!(
            results.iter().any(|(rid, _)| *rid == *id),
            "doc {} not found when searching for '{}'",
            id,
            first_word
        );
    }
}

#[tokio::test]
async fn test_compact_buckets() {
    // Simulate the real-world scenario: the configured limit is large, but the old
    // bucket-splitting bug created many tiny buckets anyway.
    // We build the index with a tiny limit (to generate fragmentation), then
    // serialize, reload with the correct large limit, and compact.
    let tokenizer = default_tokenizer();
    let small_config = BM25Config {
        bm25: BM25Params::default(),
        bucket_overload_size: 50, // tiny limit to force many buckets
    };
    let index = BM25Index::new("compact_test".to_string(), tokenizer, Some(small_config));

    let docs = vec![
        (1, "the quick brown fox jumps over the lazy dog"),
        (2, "a fast brown fox runs past the lazy dog"),
        (3, "the lazy dog sleeps all day long"),
        (4, "quick brown foxes are rare in the wild"),
        (5, "many foxes hunt at night when the moon is bright"),
        (6, "dogs and cats are common pets in modern households"),
        (7, "wild animals like foxes and wolves roam the countryside"),
        (8, "the forest is home to many different species of animals"),
    ];

    for (id, text) in &docs {
        index.insert(*id, text, 0).unwrap();
    }

    let bucket_count_before = index.stats().max_bucket_id + 1;
    println!("Before compact: {} buckets", bucket_count_before);
    assert!(
        bucket_count_before > 3,
        "should have many fragmented buckets"
    );

    // Serialize fragmented index
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;

    // Reload with the correct (large) bucket limit
    let mut loaded = BM25Index::load_metadata(default_tokenizer(), &store.metadata[..]).unwrap();
    loaded.config.bucket_overload_size = 1024 * 512;
    loaded.metadata.write().config.bucket_overload_size = 1024 * 512;
    loaded
        .load_buckets(async |object| Ok(store.buckets.get(&object).cloned()))
        .await
        .unwrap();

    let bucket_count_loaded = loaded.stats().max_bucket_id + 1;
    assert_eq!(bucket_count_loaded, bucket_count_before);

    // Capture search results before compaction
    let queries = ["fox", "dog", "lazy brown", "quick OR fast"];
    let results_before: Vec<Vec<(u64, f32)>> = queries
        .iter()
        .map(|q| {
            if q.contains("OR") {
                loaded.search_advanced(q, 20, None)
            } else {
                loaded.search(q, 20, None)
            }
        })
        .collect();

    // Compact!
    let (old, new) = loaded.compact_buckets();
    println!("Compacted: {} -> {} buckets", old, new);
    assert!(
        new < old,
        "compaction should reduce bucket count significantly"
    );
    assert!(
        new <= 3,
        "with 512K limit all postings should fit in very few buckets, got {}",
        new,
    );

    // Verify search results are unchanged
    for (i, q) in queries.iter().enumerate() {
        let results_after = if q.contains("OR") {
            loaded.search_advanced(q, 20, None)
        } else {
            loaded.search(q, 20, None)
        };
        assert_eq!(
            results_before[i].len(),
            results_after.len(),
            "query '{}' result count changed after compaction",
            q
        );

        let mut before_sorted = results_before[i].clone();
        let mut after_sorted = results_after.clone();
        before_sorted.sort_by_key(|a| a.0);
        after_sorted.sort_by_key(|a| a.0);
        for j in 0..before_sorted.len() {
            assert_eq!(before_sorted[j].0, after_sorted[j].0);
            assert!(
                (before_sorted[j].1 - after_sorted[j].1).abs() < 0.001,
                "query '{}' scores diverged for doc {}",
                q,
                before_sorted[j].0
            );
        }
    }

    // Verify flush + reload works after compaction
    let mut store2 = store.clone();
    flush_to(&loaded, &mut store2, 200).await;

    let final_loaded = load_from(&store2).await;
    assert_eq!(final_loaded.len(), loaded.len());

    for q in &queries {
        let orig = if q.contains("OR") {
            loaded.search_advanced(q, 20, None)
        } else {
            loaded.search(q, 20, None)
        };
        let reloaded = if q.contains("OR") {
            final_loaded.search_advanced(q, 20, None)
        } else {
            final_loaded.search(q, 20, None)
        };
        assert_eq!(
            orig.len(),
            reloaded.len(),
            "query '{}' mismatch after reload",
            q
        );
    }
}

/// Regression (twin of the B-Tree case): `compact_buckets` snapshots
/// `postings`, clears `buckets` and re-bins the snapshot. A posting created
/// by a concurrent `insert` after the snapshot ended up in no bucket at
/// all — and only bucket contents are serialized — so `insert` returned
/// `Ok` while the term silently vanished from the durable index on the
/// next flush. `anda_db` drives compaction under the same *shared*
/// operation lease as `add`/`update`/`remove`, so the exclusion has to
/// live here.
#[tokio::test]
async fn test_compaction_never_loses_concurrent_inserts() {
    use std::thread;

    let index = Arc::new(BM25Index::new(
        "compact_concurrent_insert".to_string(),
        default_tokenizer(),
        Some(BM25Config {
            bm25: BM25Params::default(),
            bucket_overload_size: 64,
        }),
    ));
    // Seed enough tokens that each compaction has real work to do.
    for id in 0..64u64 {
        index.insert(id, &format!("seed{id:04}"), 0).unwrap();
    }

    const WRITES: u64 = 400;
    let writer_index = index.clone();
    let writer = thread::spawn(move || {
        for id in 0..WRITES {
            writer_index
                .insert(1_000 + id, &format!("live{id:04}"), 0)
                .unwrap();
        }
    });
    let mut compactions = 0usize;
    while !writer.is_finished() {
        index.compact_buckets();
        compactions += 1;
    }
    writer.join().unwrap();
    assert!(compactions > 0, "no compaction overlapped the writer");

    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let reloaded = load_from(&store).await;
    let missing: Vec<String> = (0..WRITES)
        .map(|id| format!("live{id:04}"))
        .filter(|token| reloaded.search(token, 1, None).is_empty())
        .collect();
    assert!(
        missing.is_empty(),
        "{} concurrently inserted terms were lost, e.g. {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
    assert_eq!(reloaded.len(), index.len());
}

/// The corpus used by the `purge_ids` tests. Terms deliberately overlap so
/// that purging hits three different posting shapes: lists that disappear
/// entirely (`unicorn`), lists that merely shrink (`shared`), and lists the
/// purge must not touch at all (`walrus`).
const PURGE_DOCS: [(u64, &str); 6] = [
    (1, "shared alpha unicorn"),
    (2, "shared beta narwhal"),
    (3, "shared gamma walrus"),
    (4, "shared delta walrus"),
    (5, "shared epsilon quokka"),
    (6, "shared zeta quokka"),
];

fn build_purge_index(name: &str, config: Option<BM25Config>) -> BM25Index<TokenizerChain> {
    let index = BM25Index::new(name.to_string(), default_tokenizer(), config);
    for (id, text) in PURGE_DOCS {
        index.insert(id, text, 1).unwrap();
    }
    index
}

/// Normalizes the inverted index into a comparable shape: entry order
/// inside a posting list is an implementation detail (`swap_remove`
/// reorders), and so is the bucket a token happens to live in.
fn posting_snapshot(index: &BM25Index<TokenizerChain>) -> BTreeMap<String, Vec<(u64, usize)>> {
    index
        .postings
        .iter()
        .map(|entry| {
            let mut docs: Vec<(u64, usize)> = entry.value().1.to_vec();
            docs.sort_unstable();
            (entry.key().clone(), docs)
        })
        .collect()
}

fn doc_token_snapshot(index: &BM25Index<TokenizerChain>) -> BTreeMap<u64, usize> {
    index
        .doc_tokens
        .iter()
        .map(|entry| (*entry.key(), *entry.value()))
        .collect()
}

/// Asserts that `purged` is indistinguishable from an index that only ever
/// saw the surviving documents.
fn assert_matches_reference(purged: &BM25Index<TokenizerChain>, survivors: &[u64]) {
    let reference = BM25Index::new(
        purged.name().to_string(),
        default_tokenizer(),
        Some(purged.config.clone()),
    );
    for (id, text) in PURGE_DOCS {
        if survivors.contains(&id) {
            reference.insert(id, text, 1).unwrap();
        }
    }

    assert_eq!(posting_snapshot(purged), posting_snapshot(&reference));
    assert_eq!(doc_token_snapshot(purged), doc_token_snapshot(&reference));
    assert_eq!(
        purged.total_tokens.load(Ordering::Relaxed),
        reference.total_tokens.load(Ordering::Relaxed),
    );
    assert_eq!(purged.len(), survivors.len());
    // `avg_doc_tokens` is no longer cached; it must fall out of the two
    // counters above rather than out of a stale field.
    assert_eq!(
        purged.stats().avg_doc_tokens,
        reference.stats().avg_doc_tokens
    );
    assert_eq!(
        purged.stats().avg_doc_tokens,
        purged.total_tokens.load(Ordering::Relaxed) as f32 / survivors.len() as f32,
    );
    assert_eq!(purged.stats().num_elements, survivors.len() as u64);
}

/// `purge_ids` erases documents whose text is unrecoverable: every posting
/// entry goes, the counters land exactly where a survivors-only index would
/// have put them, and the repair survives a flush + reload.
#[tokio::test]
async fn test_purge_ids_erases_documents_without_their_text() {
    let index = build_purge_index("purge_ids", None);
    let dead: BTreeSet<u64> = [2, 4].into_iter().collect();

    // Persist first, so every bucket is *clean* when the purge runs: only
    // the purge's own dirty marks can make the repair durable below.
    let mut store = MemStore::default();
    assert!(flush_to(&index, &mut store, 10).await.saved);
    assert!(!index.has_dirty_buckets());

    // Sanity: the dead documents are findable before the purge.
    assert_eq!(index.search("narwhal", 10, None).len(), 1);
    assert_eq!(index.search("shared", 10, None).len(), 6);

    let purged = index.purge_ids(&dead, 42);
    assert_eq!(purged, 2);
    // Re-purging is a no-op: nothing is left to remove.
    assert_eq!(index.purge_ids(&dead, 43), 0);

    // No posting list mentions a purged id anywhere.
    for entry in index.postings.iter() {
        for (doc_id, _) in entry.value().1.iter() {
            assert!(
                !dead.contains(doc_id),
                "token {:?} still lists purged doc {doc_id}",
                entry.key(),
            );
        }
    }
    // Tokens that only the purged documents carried are gone entirely.
    assert!(!index.postings.contains_key("narwhal"));
    assert!(!index.postings.contains_key("delta"));
    // Tokens shared with survivors stay, minus the purged entries.
    assert_eq!(index.postings.get("walrus").unwrap().1.len(), 1);
    assert!(index.search("narwhal", 10, None).is_empty());
    assert!(index.search("delta", 10, None).is_empty());
    assert_eq!(index.search("shared", 10, None).len(), 4);
    assert_eq!(index.stats().last_deleted, 42);

    let survivors = [1, 3, 5, 6];
    assert_matches_reference(&index, &survivors);

    // The purge must be durable: a bucket left clean would resurrect the
    // ids from its stale serialized `doc_tokens` on the next load.
    assert!(index.has_dirty_buckets(), "the purge dirtied nothing");
    assert!(flush_to(&index, &mut store, 100).await.saved);
    let reloaded = load_from(&store).await;
    assert_matches_reference(&reloaded, &survivors);
    assert!(reloaded.search("narwhal", 10, None).is_empty());
    assert_eq!(reloaded.search("shared", 10, None).len(), 4);
    assert!(
        !reloaded.has_dirty_buckets(),
        "reload had to repair postings the purge should have already fixed",
    );
}

/// The same repair across a multi-bucket layout: only the buckets that
/// actually referenced a purged id may be rewritten, and the survivors'
/// buckets must stay byte-identical.
#[tokio::test]
async fn test_purge_ids_dirties_only_affected_buckets() {
    // A tiny overload size forces one token per bucket or so.
    let index = build_purge_index(
        "purge_ids_buckets",
        Some(BM25Config {
            bucket_overload_size: 48,
            ..Default::default()
        }),
    );
    assert!(index.buckets.len() > 1, "expected a multi-bucket layout");

    let mut store = MemStore::default();
    assert!(flush_to(&index, &mut store, 1).await.saved);
    let before = store.buckets.clone();

    let dead: BTreeSet<u64> = [2, 4].into_iter().collect();
    assert_eq!(index.purge_ids(&dead, 2), 2);

    // Buckets that never referenced a purged id must not be rewritten.
    let untouched: Vec<u32> = index
        .buckets
        .iter()
        .filter(|bucket| !bucket.is_dirty())
        .map(|bucket| *bucket.key())
        .collect();
    assert!(!untouched.is_empty(), "the purge dirtied every bucket");
    for bucket_id in &untouched {
        let object = BucketObject {
            bucket_id: *bucket_id,
            generation: index.metadata().buckets[bucket_id],
        };
        let bucket: BucketOwned = cbor2::from_reader(&before[&object][..]).unwrap();
        for id in &dead {
            assert!(!bucket.doc_tokens.contains_key(id));
            assert!(
                bucket
                    .postings
                    .values()
                    .all(|posting| posting.1.iter().all(|(doc, _)| doc != id)),
            );
        }
    }

    assert!(flush_to(&index, &mut store, 3).await.saved);
    // Nothing durable mentions a purged id any more.
    for data in store.buckets.values() {
        let bucket: BucketOwned = cbor2::from_reader(&data[..]).unwrap();
        for id in &dead {
            assert!(!bucket.doc_tokens.contains_key(id), "stale doc_tokens");
            for (token, posting) in &bucket.postings {
                assert!(
                    posting.1.iter().all(|(doc, _)| doc != id),
                    "token {token:?} still lists purged doc {id}",
                );
            }
        }
    }

    let reloaded = load_from(&store).await;
    assert_matches_reference(&reloaded, &[1, 3, 5, 6]);
    assert!(!reloaded.has_dirty_buckets());
}
#[tokio::test]
async fn test_metadata_only_shell_flush_keeps_manifest() {
    let index = create_test_index();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let committed = index.metadata().buckets;
    assert!(!committed.is_empty());

    // A shell that only loaded the metadata must not drop the durable
    // buckets when flushed: the manifest is carried forward unchanged.
    let shell = BM25Index::load_metadata(default_tokenizer(), &store.metadata[..]).unwrap();
    assert_eq!(shell.metadata().buckets, committed);
    let outcome = flush_to(&shell, &mut store, 2).await;
    assert!(!outcome.saved, "nothing is dirty in a freshly loaded shell");

    // Even a forced metadata commit keeps every committed bucket.
    shell.update_metadata(|m| m.stats.version += 1);
    let outcome = flush_to(&shell, &mut store, 3).await;
    assert!(outcome.saved);
    assert!(outcome.obsolete.is_empty());
    let reloaded = load_from(&store).await;
    assert_eq!(reloaded.len(), 4);
    assert_eq!(reloaded.search("fox", 10, None).len(), 3);
}

#[tokio::test]
async fn test_metadata_only_shell_refuses_destructive_writes() {
    // A small overload size so the corpus spreads over several buckets:
    // with a single one, `compact_buckets`'s `old_count <= 1` early
    // return would hide the guard under test.
    let index = BM25Index::new(
        "shell".to_string(),
        default_tokenizer(),
        Some(BM25Config {
            bucket_overload_size: 64,
            ..Default::default()
        }),
    );
    index
        .insert(1, "The quick brown fox jumps over the lazy dog", 0)
        .unwrap();
    index
        .insert(2, "A fast brown fox runs past the lazy dog", 0)
        .unwrap();
    index.insert(3, "The lazy dog sleeps all day", 0).unwrap();
    index
        .insert(4, "Quick brown foxes are rare in the wild", 0)
        .unwrap();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let committed = index.metadata().buckets;
    assert!(committed.len() > 1, "need several buckets to exercise this");

    // Compaction rebuilds the bucket map from `postings`, which a shell
    // has not loaded: without the guard it would clear every placeholder
    // and the next flush would retire every committed object.
    let shell = BM25Index::load_metadata(default_tokenizer(), &store.metadata[..]).unwrap();
    assert_eq!(
        shell.compact_buckets(),
        (committed.len(), committed.len()),
        "compaction must be a no-op on a shell"
    );
    assert_eq!(shell.metadata().buckets, committed);

    // A mutation on a shell writes a bucket object holding only that
    // mutation over one whose content was never loaded, so the flush is
    // refused rather than silently destructive.
    let shell = BM25Index::load_metadata(default_tokenizer(), &store.metadata[..]).unwrap();
    let err = shell.insert(99, "a brand new document", 0).unwrap_err();
    assert!(err.to_string().contains("not fully loaded"), "{err}");
    assert!(!shell.remove(1, "The quick brown fox jumps over the lazy dog", 0));
    assert_eq!(shell.purge_ids(&BTreeSet::from([1]), 0), 0);

    // The durable index is untouched.
    let reloaded = load_from(&store).await;
    assert_eq!(reloaded.len(), 4);
    assert_eq!(reloaded.search("fox", 10, None).len(), 3);
}

#[tokio::test]
async fn test_flush_makes_bucket_size_and_doc_ids_exact() {
    let index = create_test_index();
    // A wrong-text removal leaves doc 3 behind in the postings.
    assert!(index.remove(3, "nothing here", 0));
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    for (bucket_id, generation) in index.metadata().buckets {
        let data = &store.buckets[&BucketObject {
            bucket_id,
            generation,
        }];
        let bucket = index.buckets.get(&bucket_id).unwrap();
        assert_eq!(bucket.size, data.len(), "bucket {bucket_id} size");
        let owned: BucketOwned = cbor2::from_reader(&data[..]).unwrap();
        let expected: FxHashSet<u64> = owned.doc_tokens.keys().copied().collect();
        assert_eq!(bucket.doc_ids, expected, "bucket {bucket_id} doc_ids");
        assert!(!bucket.doc_ids.contains(&3));
    }

    // Without stale entries to prune, a reload sees the same sizes the
    // writer had, so the estimate does not jump across a restart.
    let clean = create_test_index();
    let mut store = MemStore::default();
    flush_to(&clean, &mut store, 1).await;
    let reloaded = load_from(&store).await;
    for (bucket_id, _) in clean.metadata().buckets {
        assert_eq!(
            reloaded.buckets.get(&bucket_id).unwrap().size,
            clean.buckets.get(&bucket_id).unwrap().size
        );
    }
}

#[test]
fn test_bare_word_or_scores_like_search() {
    let index = create_test_index();
    assert_eq!(
        index.search_advanced("quick fox dog", 10, None),
        index.search("quick fox dog", 10, None)
    );
    // A repeated word is scored once, as `search` does.
    assert_eq!(
        index.search_advanced("fox fox", 10, None),
        index.search("fox", 10, None)
    );
    // A mixed OR still merges per subquery.
    let mixed = index.search_advanced("sleeps OR (fox AND dog)", 10, None);
    let mut ids: Vec<u64> = mixed.into_iter().map(|(id, _)| id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn test_top_level_double_negation() {
    let index = create_test_index();
    let mut ids: Vec<u64> = index
        .search_advanced("NOT NOT fox", 10, None)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 4]);
}

#[test]
fn test_tokenize_failed_truncates_text() {
    let index = BM25Index::new("truncate".to_string(), default_tokenizer(), None);
    // Single-byte tokens are dropped, so this text yields no token.
    let text = "a ".repeat(1000);
    match index.insert(1, &text, 0) {
        Err(BM25Error::TokenizeFailed { text: kept, .. }) => {
            assert!(kept.len() < 320, "{}", kept.len());
            assert!(kept.starts_with("a a a "));
            assert!(kept.ends_with("[2000 bytes total]"), "{kept}");
        }
        other => panic!("unexpected: {other:?}"),
    }
}
