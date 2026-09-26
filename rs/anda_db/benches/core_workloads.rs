//! Reproducible core workloads; run with `cargo bench -p anda_db --bench core_workloads`.
//! ANDA_BENCH_DOCS and ANDA_BENCH_ITERATIONS control scale. JSON lines are artifacts.
use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    query::{Filter, Query, RangeQuery, Search},
    schema::{AndaDBSchema, Fv},
    storage::{Storage, StorageConfig},
    unix_ms,
};
use object_store::memory::InMemory;
use serde::Serialize;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

mod support;

struct TrackedAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for TrackedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let n = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, layout) };
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, layout, new_size) };
        if !q.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            let n = LIVE.fetch_add(new_size, Ordering::Relaxed) + new_size;
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        q
    }
}
#[global_allocator]
static ALLOC: TrackedAllocator = TrackedAllocator;
#[derive(Serialize, AndaDBSchema)]
struct Row {
    _id: u64,
    owner: u64,
    body: String,
}
fn count(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}
fn id(q: RangeQuery<Fv>) -> Filter {
    Filter::Field(("_id".into(), q))
}
async fn measure<F, Fut>(name: &str, n: usize, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let start = Instant::now();
        f().await;
        times.push(start.elapsed().as_secs_f64() * 1000.);
    }
    times.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({"workload":name,"iterations":n,"p50_ms":times[n/2],"p95_ms":times[(n*95/100).min(n-1)],"p99_ms":times[(n*99/100).min(n-1)],"total_ms":times.iter().sum::<f64>(),"peak_additional_heap_bytes":PEAK.load(Ordering::Relaxed).saturating_sub(live)})
    );
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if std::env::var_os("ANDA_BENCH_QUERY_SCALE").is_some() {
        query_scale_bench().await;
        return;
    }
    if std::env::var_os("ANDA_BENCH_REVIEW").is_some() {
        review_bench().await;
        return;
    }
    if std::env::var_os("ANDA_BENCH_IO").is_some() {
        io_bench().await;
        return;
    }
    if std::env::var_os("ANDA_BENCH_CACHE").is_some() {
        cache_bench().await;
        return;
    }
    if std::env::var_os("ANDA_BENCH_SCHEDULER").is_some() {
        scheduler_bench().await;
        return;
    }
    let n = count("ANDA_BENCH_DOCS", 10_000).max(10);
    let iterations = count("ANDA_BENCH_ITERATIONS", 100).max(1);
    #[cfg(feature = "full")]
    let directory = std::env::temp_dir().join(format!(
        "anda-core-bench-{}-{}",
        std::process::id(),
        unix_ms()
    ));
    let store: Arc<dyn object_store::ObjectStore> =
        if std::env::var("ANDA_BENCH_BACKEND").as_deref() == Ok("fs") {
            #[cfg(feature = "full")]
            {
                std::fs::create_dir_all(&directory).unwrap();
                Arc::new(
                    anda_object_store::MetaStoreBuilder::new(
                        object_store::local::LocalFileSystem::new_with_prefix(&directory)
                            .unwrap()
                            .with_fsync(true),
                        10000,
                    )
                    .build(),
                )
            }
            #[cfg(not(feature = "full"))]
            {
                panic!("filesystem benchmark requires --features full");
            }
        } else {
            Arc::new(InMemory::new())
        };
    let cfg = DBConfig {
        name: "bench".into(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = AndaDB::connect(store.clone(), cfg.clone()).await.unwrap();
    let c = db
        .create_collection(
            Row::schema().unwrap(),
            CollectionConfig {
                name: "docs".into(),
                ..Default::default()
            },
            async |c| {
                c.create_btree_index(&["owner"]).await?;
                c.create_bm25_index(&["body"]).await
            },
        )
        .await
        .unwrap();
    let start = Instant::now();
    for i in 0..n {
        c.add_from(&Row {
            _id: 0,
            owner: (i % 100) as u64,
            body: format!("memory knowledge topic{}", i % 101),
        })
        .await
        .unwrap();
    }
    c.flush(unix_ms()).await.unwrap();
    println!(
        "{}",
        serde_json::json!({"workload":"seed","documents":n,"elapsed_ms":start.elapsed().as_secs_f64()*1000.})
    );
    measure("bounded_or", iterations, || async {
        let f = Filter::Or(vec![
            Box::new(id(RangeQuery::Gt(Fv::U64(0)))),
            Box::new(id(RangeQuery::Lt(Fv::U64(n as u64)))),
        ]);
        assert_eq!(c.query_ids(f, Some(10)).await.unwrap().len(), 10);
    })
    .await;
    measure("bounded_and", iterations, || async {
        let f = Filter::And(vec![
            Box::new(id(RangeQuery::Gt(Fv::U64(0)))),
            Box::new(Filter::Field(("owner".into(), RangeQuery::Eq(Fv::U64(1))))),
        ]);
        assert_eq!(
            c.query_ids(f, Some(10)).await.unwrap().len(),
            (n / 100).min(10)
        );
    })
    .await;
    measure("single_bm25", iterations, || async {
        assert!(
            !c.search_ids(Query {
                search: Some(Search {
                    text: Some("memory".into()),
                    ..Default::default()
                }),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .unwrap()
            .is_empty()
        );
    })
    .await;
    measure("selective_bm25", iterations, || async {
        let hits = c
            .search_ids(Query {
                search: Some(Search {
                    text: Some("memory".into()),
                    ..Default::default()
                }),
                filter: Some(Filter::Field(("owner".into(), RangeQuery::Eq(Fv::U64(99))))),
                limit: Some(10),
            })
            .await
            .unwrap();
        std::hint::black_box(hits);
    })
    .await;
    let filtered = c
        .search_ids(Query {
            search: Some(Search {
                text: Some("memory".into()),
                ..Default::default()
            }),
            filter: Some(Filter::Field(("owner".into(), RangeQuery::Eq(Fv::U64(99))))),
            limit: Some(10),
        })
        .await
        .unwrap();
    println!(
        "{}",
        serde_json::json!({"workload":"selective_hits","returned":filtered.len(),"expected":(n/100).min(10)})
    );
    let large = "a".repeat(100_000);
    c.update(
        1,
        BTreeMap::from([("body".into(), Fv::Text(large.clone()))]),
    )
    .await
    .unwrap();
    c.flush(unix_ms()).await.unwrap();
    let live = LIVE.load(Ordering::Relaxed);
    for i in 0..100 {
        c.update(1, BTreeMap::from([("owner".into(), Fv::U64(i))]))
            .await
            .unwrap();
    }
    println!(
        "{}",
        serde_json::json!({"workload":"pending_intent_memory","updates":100,"additional_heap_bytes":LIVE.load(Ordering::Relaxed).saturating_sub(live)})
    );
    measure("clear_100_intents", 1, || async {
        c.flush(unix_ms()).await.unwrap();
    })
    .await;
    db.close_collection("docs").await.unwrap();
    measure("reopen", 5, || async {
        db.open_collection("docs".into(), async |_| Ok(()))
            .await
            .unwrap();
        db.close_collection("docs").await.unwrap();
    })
    .await;
    let storage = Storage::connect(
        "codec".into(),
        Arc::new(InMemory::new()),
        StorageConfig::default(),
    )
    .await
    .unwrap();
    let payload = vec![b'x'; 1_100_000];
    measure("large_codec", 20, || async {
        storage
            .put_bytes(
                "data",
                payload.clone().into(),
                anda_db::storage::PutMode::Overwrite,
            )
            .await
            .unwrap();
        assert_eq!(
            storage.fetch_bytes("data").await.unwrap().0.len(),
            payload.len()
        );
    })
    .await;
    #[cfg(feature = "full")]
    if directory.exists() {
        drop(c);
        drop(db);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

/// Local comparison of dense pages, unchanged/changed writes, and recovery of
/// many different documents. Uses the same public API before and after fixes.
async fn review_bench() {
    use anda_db::{
        index::HnswConfig,
        schema::{Vector, vector_from_f32},
    };
    let n = count("ANDA_BENCH_DOCS", 100_000).max(100);
    let iterations = count("ANDA_BENCH_ITERATIONS", 30).max(1);
    let cfg = DBConfig {
        name: "review_bench".into(),
        storage: StorageConfig {
            compress_level: 0,
            cache_max_capacity: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = AndaDB::connect(Arc::new(InMemory::new()), cfg.clone())
        .await
        .unwrap();
    let dense = db
        .create_collection(
            Row::schema().unwrap(),
            CollectionConfig {
                name: "dense".into(),
                ..Default::default()
            },
            async |c| c.create_btree_index(&["owner"]).await,
        )
        .await
        .unwrap();
    for _ in 0..n {
        dense
            .add_from(&Row {
                _id: 0,
                owner: 1,
                body: "memory".into(),
            })
            .await
            .unwrap();
    }
    dense.flush(unix_ms()).await.unwrap();
    measure("dense_and_page", iterations, || async {
        let f = Filter::And(vec![
            Box::new(Filter::Field(("owner".into(), RangeQuery::Eq(Fv::U64(1))))),
            Box::new(id(RangeQuery::Lt(Fv::U64((n / 2) as u64)))),
        ]);
        let result = dense.query_last_ids(f, Some(20)).await.unwrap();
        assert_eq!(
            result,
            ((n / 2 - 20) as u64..(n / 2) as u64).collect::<Vec<_>>()
        );
    })
    .await;

    #[derive(Serialize, AndaDBSchema)]
    struct IndexedRow {
        _id: u64,
        body: String,
        embedding: Vector,
    }
    let indexed = db
        .create_collection(
            IndexedRow::schema().unwrap(),
            CollectionConfig {
                name: "indexed".into(),
                ..Default::default()
            },
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
    indexed
        .add_from(&IndexedRow {
            _id: 0,
            body: "memory".into(),
            embedding: vector_from_f32(vec![1., 2.]),
        })
        .await
        .unwrap();
    indexed.flush(unix_ms()).await.unwrap();
    let before = indexed.storage_stats();
    measure("unchanged_update_flush", iterations, || async {
        indexed
            .update(
                1,
                BTreeMap::from([
                    ("body".into(), Fv::Text("memory".into())),
                    (
                        "embedding".into(),
                        Fv::Vector(vector_from_f32(vec![1., 2.])),
                    ),
                ]),
            )
            .await
            .unwrap();
        indexed.flush(unix_ms()).await.unwrap();
    })
    .await;
    println!(
        "{}",
        serde_json::json!({"workload":"unchanged_update_io", "puts":indexed.storage_stats().total_put_count-before.total_put_count})
    );

    let before = dense.storage_stats();
    let serial = AtomicUsize::new(0);
    measure("changed_update_flush", iterations, || async {
        dense
            .update(
                1,
                BTreeMap::from([(
                    "body".into(),
                    Fv::Text(format!(
                        "updated {}",
                        serial.fetch_add(1, Ordering::Relaxed)
                    )),
                )]),
            )
            .await
            .unwrap();
        dense.flush(unix_ms()).await.unwrap();
    })
    .await;
    println!(
        "{}",
        serde_json::json!({"workload":"changed_update_io", "puts":dense.storage_stats().total_put_count-before.total_put_count})
    );
    db.close().await.unwrap();

    let recovery_docs = count("ANDA_BENCH_RECOVERY_DOCS", 128).max(1);
    let backend = Arc::new(support::InstrumentedStore::default());
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(support::Store(backend.clone()));
    {
        let db = AndaDB::connect(store.clone(), cfg.clone()).await.unwrap();
        let c = db
            .create_collection(
                Row::schema().unwrap(),
                CollectionConfig {
                    name: "docs".into(),
                    ..Default::default()
                },
                async |_| Ok(()),
            )
            .await
            .unwrap();
        for _ in 0..recovery_docs {
            c.add_from(&Row {
                _id: 0,
                owner: 1,
                body: "before".into(),
            })
            .await
            .unwrap();
        }
        c.flush(unix_ms()).await.unwrap();
        for id in 1..=recovery_docs as u64 {
            c.update(
                id,
                BTreeMap::from([("body".into(), Fv::Text("after".into()))]),
            )
            .await
            .unwrap();
        }
    }
    backend.reset(5);
    let start = Instant::now();
    let db = AndaDB::connect(store, cfg).await.unwrap();
    let c = db
        .open_collection("docs".into(), async |_| Ok(()))
        .await
        .unwrap();
    let elapsed = start.elapsed().as_secs_f64() * 1000.;
    let calls = backend.counts();
    assert_eq!(c.len(), recovery_docs);
    assert_eq!(
        c.get(recovery_docs as u64).await.unwrap().get_field("body"),
        Some(&Fv::Text("after".into()))
    );
    println!(
        "{}",
        serde_json::json!({"workload":"distinct_document_recovery", "documents":recovery_docs, "latency_ms":5, "elapsed_ms":elapsed, "get_put_delete_list":calls})
    );
}

async fn io_bench() {
    for latency in [1, 10, 50] {
        let backend = Arc::new(support::InstrumentedStore::default());
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(support::Store(backend.clone()));
        let cfg = DBConfig {
            name: "io_bench".into(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        {
            let db = AndaDB::connect(store.clone(), cfg.clone()).await.unwrap();
            let c = db
                .create_collection(
                    Row::schema().unwrap(),
                    CollectionConfig {
                        name: "docs".into(),
                        ..Default::default()
                    },
                    async |_| Ok(()),
                )
                .await
                .unwrap();
            c.add_from(&Row {
                _id: 0,
                owner: 0,
                body: "memory".into(),
            })
            .await
            .unwrap();
            c.flush(unix_ms()).await.unwrap();
            for i in 0..100 {
                c.update(1, BTreeMap::from([("owner".into(), Fv::U64(i))]))
                    .await
                    .unwrap();
            }
        }
        backend.reset(latency);
        let start = Instant::now();
        let db = AndaDB::connect(store, cfg).await.unwrap();
        let c = db
            .open_collection("docs".into(), async |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(
            c.get(1).await.unwrap().get_field("owner"),
            Some(&Fv::U64(99))
        );
        println!(
            "{}",
            serde_json::json!({"workload":"recover_100_intents","latency_ms":latency,"elapsed_ms":start.elapsed().as_secs_f64()*1000.,"get_put_delete_list":backend.counts()})
        );
    }
}
async fn cache_bench() {
    let storage = Storage::connect(
        "cache".into(),
        Arc::new(InMemory::new()),
        StorageConfig {
            compress_level: 0,
            cache_max_bytes: Some(1024 * 1024),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for i in 0..64 {
        let path = format!("hot{i}");
        storage.put(&path, &i, None).await.unwrap();
        storage.get::<i32>(&path).await.unwrap();
    }
    let before = storage.stats();
    let start = Instant::now();
    for i in 0..1000 {
        storage.put(&format!("cold{i}"), &i, None).await.unwrap();
        for j in 0..64 {
            storage.get::<i32>(&format!("hot{j}")).await.unwrap();
        }
    }
    let after = storage.stats();
    println!(
        "{}",
        serde_json::json!({"workload":"cache_mixed","reads":64000,"cold_writes":1000,"backend_gets":after.total_fetch_count-before.total_fetch_count,"elapsed_ms":start.elapsed().as_secs_f64()*1000.})
    );
}
async fn scheduler_bench() {
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let active = Arc::new(AtomicUsize::new(2));
    let mut workers = Vec::new();
    for i in 0..2 {
        let barrier = barrier.clone();
        let active = active.clone();
        workers.push(tokio::spawn(async move {
            let storage = Storage::connect(
                format!("worker{i}"),
                Arc::new(InMemory::new()),
                StorageConfig::default(),
            )
            .await
            .unwrap();
            let data = vec![b'x'; 1_100_000];
            barrier.wait().await;
            for _ in 0..100 {
                storage
                    .put_bytes(
                        "data",
                        data.clone().into(),
                        anda_db::storage::PutMode::Overwrite,
                    )
                    .await
                    .unwrap();
                storage.fetch_bytes("data").await.unwrap();
            }
            active.fetch_sub(1, Ordering::Release);
        }));
    }
    let started = Instant::now();
    // Sample throughout the CPU workload, not just an initial burst of yields.
    let heartbeat = tokio::spawn(async move {
        // Start the clock before releasing the workers. A fully starved
        // heartbeat may first resume only after both workers have finished.
        let mut tick_start = Instant::now();
        barrier.wait().await;
        let mut ticks = Vec::new();
        loop {
            tokio::time::sleep_until(
                tokio::time::Instant::from_std(tick_start) + std::time::Duration::from_millis(1),
            )
            .await;
            ticks.push(tick_start.elapsed().as_secs_f64() * 1000.);
            if active.load(Ordering::Acquire) == 0 {
                break;
            }
            tick_start = Instant::now();
        }
        ticks.sort_by(f64::total_cmp);
        ticks
    });
    let ticks = heartbeat.await.unwrap();
    for worker in workers {
        worker.await.unwrap();
    }
    assert!(!ticks.is_empty());
    println!(
        "{}",
        serde_json::json!({"workload":"codec_scheduler","harness_version":3,"requested_tick_ms":1,"tick_samples":ticks.len(),"maximum_tick_ms":ticks.last().unwrap(),"p95_tick_ms":ticks[(ticks.len()*95/100).min(ticks.len()-1)],"elapsed_ms":started.elapsed().as_secs_f64()*1000.})
    );
}

/// Uses only public APIs present in the baseline, so the same harness can
/// compare checkouts without timing fixture construction or compilation.
async fn query_scale_bench() {
    #[derive(Serialize, AndaDBSchema)]
    struct ScaleRow {
        _id: u64,
        tenant: u64,
        status: u64,
        needle: u64,
    }
    let n = count("ANDA_BENCH_DOCS", 1_000_001).max(100);
    let samples = count("ANDA_BENCH_ITERATIONS", 15).max(1);
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: "query_scale".into(),
            storage: StorageConfig {
                compress_level: 0,
                cache_max_capacity: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let c = db
        .create_collection(
            ScaleRow::schema().unwrap(),
            CollectionConfig {
                name: "rows".into(),
                ..Default::default()
            },
            async |c| {
                c.create_btree_index(&["tenant"]).await?;
                c.create_btree_index(&["status"]).await?;
                c.create_btree_index(&["needle"]).await
            },
        )
        .await
        .unwrap();
    for i in 0..n {
        c.add_from(&ScaleRow {
            _id: 0,
            tenant: 1,
            status: 1,
            needle: u64::from(i < 10),
        })
        .await
        .unwrap();
    }
    let eq = |field: &str, value| Filter::Field((field.into(), RangeQuery::Eq(Fv::U64(value))));
    let cases = [
        (
            "rare_and_dense",
            Filter::And(vec![
                Box::new(eq("needle", 1)),
                Box::new(eq("tenant", 1)),
                Box::new(eq("status", 1)),
            ]),
            (1..=10).collect::<Vec<_>>(),
        ),
        (
            "empty_and_dense",
            Filter::And(vec![
                Box::new(eq("needle", 99)),
                Box::new(eq("tenant", 1)),
                Box::new(eq("status", 1)),
            ]),
            vec![],
        ),
        (
            "dense_first_page",
            Filter::And(vec![Box::new(eq("tenant", 1)), Box::new(eq("status", 1))]),
            (1..=50).collect::<Vec<_>>(),
        ),
        (
            "dense_deep_page",
            Filter::And(vec![
                Box::new(eq("tenant", 1)),
                Box::new(eq("status", 1)),
                Box::new(id(RangeQuery::Gt(Fv::U64(n as u64 - 100)))),
            ]),
            ((n as u64 - 99)..=(n as u64 - 50)).collect::<Vec<_>>(),
        ),
    ];
    println!(
        "{}",
        serde_json::json!({"workload":"query_scale_environment", "documents":n, "samples":samples, "backend":"InMemory", "seed_timed":false})
    );
    for (name, filter, expected) in cases {
        let start = Instant::now();
        assert_eq!(
            c.query_ids(filter.clone(), Some(50)).await.unwrap(),
            expected
        );
        println!(
            "{}",
            serde_json::json!({"workload":format!("{name}_cold_posting"), "elapsed_ms":start.elapsed().as_secs_f64()*1000.})
        );
        for _ in 0..2 {
            assert_eq!(
                c.query_ids(filter.clone(), Some(50)).await.unwrap(),
                expected
            );
        }
        measure(name, samples, || async {
            assert_eq!(
                c.query_ids(filter.clone(), Some(50)).await.unwrap(),
                expected
            );
        })
        .await;
    }
    // The all-ID API is a complete answer, and is not silently page-clamped.
    let filters = Filter::And(vec![Box::new(eq("tenant", 1)), Box::new(eq("status", 1))]);
    measure("dense_all_ids", samples.min(5), || async {
        assert_eq!(c.query_all_ids(filters.clone()).await.unwrap().len(), n);
    })
    .await;
    db.close().await.unwrap();
}
