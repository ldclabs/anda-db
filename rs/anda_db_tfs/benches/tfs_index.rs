//! Fixed corpora keep before/after comparisons independent of README changes.
use anda_db_tfs::{
    BM25Config, BM25Index, BucketObject, TokenizerChain, default_tokenizer, jieba_tokenizer,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group};
use futures::executor::block_on;
use std::{
    collections::{BTreeMap, BTreeSet},
    hint::black_box,
    time::{Duration, Instant},
};

type Index = BM25Index<TokenizerChain>;

#[derive(Default, Clone)]
struct Store {
    metadata: Vec<u8>,
    buckets: BTreeMap<BucketObject, Vec<u8>>,
}

fn corpus(count: usize, chinese: bool) -> Vec<String> {
    (0..count)
        .map(|id| {
            format!(
                "common common anchor group{:02} term{id:06} {}",
                id % 100,
                if chinese {
                    "数据库支持全文搜索。人工智能需要长期记忆。"
                } else {
                    "database memory search document text indexing retrieval storage"
                }
            )
        })
        .collect()
}

fn build(docs: &[String], limit: usize, chinese: bool) -> Index {
    let index = BM25Index::new(
        "bench".into(),
        if chinese {
            jieba_tokenizer()
        } else {
            default_tokenizer()
        },
        Some(BM25Config {
            bucket_overload_size: limit,
            ..Default::default()
        }),
    );
    for (id, text) in docs.iter().enumerate() {
        index.insert(id as u64, text, 0).unwrap();
    }
    index
}

fn save(index: &Index, store: &mut Store) -> (usize, usize) {
    let mut bytes = 0;
    let mut objects = 0;
    let mut metadata = Vec::new();
    let outcome = block_on(index.flush(&mut metadata, 1, |object, data| {
        bytes += data.len();
        objects += 1;
        store.buckets.insert(object, data);
        std::future::ready(Ok(()))
    }))
    .unwrap();
    if outcome.saved {
        store.metadata = metadata;
        for object in outcome.obsolete {
            store.buckets.remove(&object);
        }
    }
    (objects, bytes)
}

fn benchmark(c: &mut Criterion) {
    let sizes = if std::env::var_os("ANDA_TFS_BENCH_LARGE").is_some() {
        vec![1_000, 10_000, 100_000]
    } else {
        vec![1_000, 10_000]
    };
    let mut search = c.benchmark_group("bm25_search");
    search.sample_size(20);
    for n in sizes {
        let docs = corpus(n, false);
        let index = build(&docs, 8192, false);
        for (name, query, advanced) in [
            ("missing", "absent", false),
            ("rare", "term000099", false),
            ("common", "common", false),
            ("and", "common AND term000099", true),
            ("not", "group01 AND NOT term000001", true),
            ("or", "group01 OR group02 OR group03", true),
        ] {
            search.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| {
                    black_box(if advanced {
                        index.search_advanced(black_box(query), 10, None)
                    } else {
                        index.search(black_box(query), 10, None)
                    })
                })
            });
        }
        for count in [0, 1, 100] {
            let ids: Vec<_> = (0..count).collect();
            search.bench_function(BenchmarkId::new(format!("candidates_{count}"), n), |b| {
                b.iter(|| {
                    black_box(
                        index
                            .try_search_in_ids("common", 10, None, black_box(&ids), false)
                            .unwrap(),
                    )
                })
            });
        }
        search.bench_function(BenchmarkId::new("mixed_or", n), |b| {
            b.iter(|| {
                black_box(index.search_advanced(
                    "group01 OR group01 OR (group02 AND common)",
                    10,
                    None,
                ))
            })
        });
        let many_absent_negatives = format!(
            "common{}",
            (0..64)
                .map(|id| format!(" AND NOT absent{id}"))
                .collect::<String>()
        );
        search.bench_function(BenchmarkId::new("not_many_absent", n), |b| {
            b.iter(|| black_box(index.search_advanced(black_box(&many_absent_negatives), 10, None)))
        });
        if std::env::var_os("ANDA_TFS_METRICS").is_some() {
            for (name, query, advanced) in [
                ("common", "common", false),
                ("and", "common AND term000099", true),
            ] {
                let mut samples = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    let start = Instant::now();
                    black_box(if advanced {
                        index.search_advanced(query, 10, None)
                    } else {
                        index.search(query, 10, None)
                    });
                    samples.push(start.elapsed().as_nanos());
                }
                samples.sort_unstable();
                eprintln!(
                    "BM25_LATENCY docs={n} query={name} p50_ns={} p95_ns={}",
                    samples[500], samples[950]
                );
            }
        }
        // Measure write amplification for one additional document with common terms.
        let mut store = Store::default();
        let initial = save(&index, &mut store);
        index.insert(n as u64, &docs[0], 1).unwrap();
        let incremental = save(&index, &mut store);
        eprintln!(
            "BM25_IO docs={n} initial_objects={} initial_bytes={} one_insert_objects={} one_insert_bytes={} largest_bucket={}",
            initial.0,
            initial.1,
            incremental.0,
            incremental.1,
            store.buckets.values().map(Vec::len).max().unwrap_or(0)
        );
    }
    search.finish();

    let mut mutations = c.benchmark_group("bm25_mutation");
    mutations.sample_size(20);
    for n in [256, 512, 1024] {
        let text = (0..n)
            .map(|i| format!("unique{i:06}"))
            .collect::<Vec<_>>()
            .join(" ");
        mutations.throughput(Throughput::Elements(n as u64));
        mutations.bench_function(BenchmarkId::new("new_terms", n), |b| {
            b.iter_batched(
                || build(&[], 1, false),
                |index| {
                    index.insert(1, black_box(&text), 0).unwrap();
                    black_box(index);
                },
                BatchSize::SmallInput,
            )
        });
    }
    let docs = corpus(1000, false);
    mutations.throughput(Throughput::Elements(100));
    mutations.bench_function("remove_100", |b| {
        b.iter_batched(
            || build(&docs, 8192, false),
            |index| {
                for (id, text) in docs.iter().take(100).enumerate() {
                    assert!(index.remove(id as u64, text, 1));
                }
            },
            BatchSize::SmallInput,
        )
    });
    let dead = (0..100).collect::<BTreeSet<u64>>();
    mutations.bench_function("purge_100", |b| {
        b.iter_batched(
            || build(&docs, 8192, false),
            |index| {
                assert_eq!(index.purge_ids(black_box(&dead), 1), 100);
            },
            BatchSize::SmallInput,
        )
    });
    mutations.finish();

    let mut persistence = c.benchmark_group("bm25_persistence");
    persistence.sample_size(20);
    persistence.bench_function("flush_1000", |b| {
        b.iter_batched(
            || build(&docs, 8192, false),
            |index| black_box(save(&index, &mut Store::default())),
            BatchSize::SmallInput,
        )
    });
    let mut store = Store::default();
    save(&build(&docs, 8192, false), &mut store);
    persistence.bench_function("load_1000", |b| {
        b.iter(|| {
            black_box(
                block_on(BM25Index::load_all(
                    default_tokenizer(),
                    store.metadata.as_slice(),
                    async |object| Ok(store.buckets.get(&object).cloned()),
                ))
                .unwrap(),
            )
        })
    });
    persistence.bench_function("compact_1000", |b| {
        b.iter_batched(
            || build(&docs, 512, false),
            |index| black_box(index.compact_buckets()),
            BatchSize::SmallInput,
        )
    });
    let compacted = build(&corpus(20_000, false), 8192, false);
    compacted.compact_buckets();
    let mut compacted_store = Store::default();
    save(&compacted, &mut compacted_store);
    persistence.bench_function("compact_unchanged_20000", |b| {
        b.iter(|| {
            compacted.compact_buckets();
            black_box(save(&compacted, &mut compacted_store))
        })
    });
    let deleted = (100..1000).collect::<BTreeSet<_>>();
    persistence.bench_function("compact_after_purge_90_percent", |b| {
        b.iter_batched(
            || {
                let index = build(&docs, 8192, false);
                index.purge_ids(&deleted, 1);
                index
            },
            |index| black_box(index.compact_buckets()),
            BatchSize::SmallInput,
        )
    });
    persistence.finish();

    let chinese = corpus(1000, true);
    let index = build(&chinese, 8192, true);
    c.bench_function("bm25_chinese/search_1000", |b| {
        b.iter(|| black_box(index.search("人工智能 数据库", 10, None)))
    });
}

criterion_group! { name = benches; config = Criterion::default().warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(1)); targets = benchmark }
fn main() {
    if std::env::var_os("ANDA_TFS_MEMORY_PROFILE").is_some() {
        // Run /usr/bin/time -l (macOS) or -v (Linux) on the built executable.
        // Discard uploads so the peak excludes an in-memory object store.
        let count = std::env::var("ANDA_TFS_PROFILE_DOCS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(20_000);
        let index = build(&[], 8192, false);
        for id in 0..count {
            let text = (0..8)
                .map(|term| format!("unique{id:08}word{term}"))
                .collect::<Vec<_>>()
                .join(" ");
            index.insert(id as u64, &text, 0).unwrap();
        }
        let start = Instant::now();
        let (mut objects, mut bytes, mut largest) = (0, 0, 0);
        block_on(index.flush_with(
            1,
            |_| std::future::ready(Ok(())),
            |_, payload| {
                objects += 1;
                bytes += payload.len();
                largest = largest.max(payload.len());
                std::future::ready(Ok(()))
            },
        ))
        .unwrap();
        eprintln!(
            "BM25_MEMORY docs={count} objects={objects} total_bytes={bytes} max_payload_bytes={largest} flush_ms={}",
            start.elapsed().as_millis()
        );
        return;
    }
    benches();
    Criterion::default().configure_from_args().final_summary();
}
