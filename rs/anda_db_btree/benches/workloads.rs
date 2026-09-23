//! Instrumented workload benchmark. Run with `ANDA_BTREE_RUN_BENCH=1`.
//! ANDA_BTREE_BENCH_SAMPLES controls the default 100 samples. Output is JSON.
use anda_db_btree::{BTreeConfig, BTreeIndex, RangeQuery};
use futures::executor::block_on;
use serde::Serialize;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
struct Meter;
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
fn added(size: usize) {
    CALLS.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(size as u64, Ordering::Relaxed);
    let live = LIVE.fetch_add(size as u64, Ordering::Relaxed) + size as u64;
    PEAK.fetch_max(live, Ordering::Relaxed);
}
// Forward exact pointer/layout contracts to System. Never reset LIVE: existing
// allocations must still be accounted for when their destructors run.
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            added(l.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            added(l.size());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let out = unsafe { System.realloc(p, l, n) };
        if !out.is_null() {
            LIVE.fetch_sub(l.size() as u64, Ordering::Relaxed);
            added(n);
        }
        out
    }
}
#[global_allocator]
static ALLOCATOR: Meter = Meter;
#[derive(Serialize)]
struct Measurement {
    case: String,
    samples: usize,
    operations_per_sample: u64,
    median_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    operations_per_second: f64,
    allocations_per_sample: f64,
    allocated_bytes_per_sample: f64,
    peak_extra_heap_bytes: u64,
}
fn summarize(
    name: &str,
    mut times: Vec<u64>,
    ops: u64,
    calls: u64,
    bytes: u64,
    peak: u64,
) -> Measurement {
    let n = times.len();
    let total: u64 = times.iter().sum();
    times.sort_unstable();
    Measurement {
        case: name.into(),
        samples: n,
        operations_per_sample: ops,
        median_ns: times[n / 2],
        p95_ns: times[(n * 95 / 100).min(n - 1)],
        p99_ns: times[(n * 99 / 100).min(n - 1)],
        operations_per_second: ops as f64 * n as f64 * 1e9 / total.max(1) as f64,
        allocations_per_sample: calls as f64 / n as f64,
        allocated_bytes_per_sample: bytes as f64 / n as f64,
        peak_extra_heap_bytes: peak,
    }
}
fn measure(name: &str, n: usize, ops: u64, mut run: impl FnMut()) -> Measurement {
    let mut times = Vec::with_capacity(n);
    let calls = CALLS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    for _ in 0..n {
        let start = Instant::now();
        run();
        times.push(start.elapsed().as_nanos() as u64);
    }
    summarize(
        name,
        times,
        ops,
        CALLS.load(Ordering::Relaxed) - calls,
        BYTES.load(Ordering::Relaxed) - bytes,
        PEAK.load(Ordering::Relaxed).saturating_sub(live),
    )
}
fn seeded(n: u64) -> BTreeIndex<u64, u64> {
    let index = BTreeIndex::new("bench".into(), None);
    for key in 0..n {
        index.insert(key, key, 1).unwrap();
    }
    index
}
fn main() {
    // A harness-free bench is also executed by `cargo test --release
    // --all-targets`, without a distinguishing argument. Keep expensive work
    // explicit so test commands only perform tests.
    if std::env::var_os("ANDA_BTREE_RUN_BENCH").is_none() {
        return;
    }
    // Warm CPU execution before comparing sub-microsecond paths. Keep the
    // synthetic-latency suite optional for alternating CPU-only A/B passes.
    let warm = Instant::now();
    let mut state = 1u64;
    while warm.elapsed() < Duration::from_millis(100) {
        for _ in 0..4096 {
            state = black_box(state)
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1);
        }
    }
    let no_delay = std::env::var_os("ANDA_BTREE_BENCH_NO_IO_DELAY").is_some();
    let samples = std::env::var("ANDA_BTREE_BENCH_SAMPLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100usize)
        .max(10);
    let mut results = Vec::new();
    let index = seeded(10_000);
    // Batch tiny queries so timer quantization does not dominate point paths.
    for mode in 0..3 {
        results.push(measure(
            [
                "query/point/batched",
                "query/eq/batched",
                "query/ge/batched",
            ][mode],
            samples,
            1_000,
            || {
                for key in 0..1_000 {
                    if mode == 0 {
                        black_box(index.query_with(&key, |ids| Some(ids.len())));
                    } else {
                        let query = if mode == 1 {
                            RangeQuery::Eq(key)
                        } else {
                            RangeQuery::Ge(key)
                        };
                        black_box(index.range_query_with(query, |_, ids| {
                            black_box(ids.len());
                            (false, Vec::<u64>::new())
                        }));
                    }
                }
            },
        ));
    }
    for n in [8, 128, 4_096] {
        results.push(measure(
            &format!("query/include/{n}/first"),
            samples,
            1,
            || {
                let keys = (0..n).rev().flat_map(|key| [key, key]).collect();
                black_box(
                    index.range_query_with(RangeQuery::Include(keys), |key, _| (false, vec![*key])),
                );
            },
        ));
    }
    for mode in 0..5 {
        results.push(measure(
            [
                "query/ge/first",
                "query/and/first",
                "query/or/first",
                "query/not/last",
                "query/and/last",
            ][mode],
            samples,
            1,
            || {
                let query = match mode {
                    0 => RangeQuery::Ge(0),
                    1 => RangeQuery::And(vec![Box::new(RangeQuery::Ge(0))]),
                    2 => RangeQuery::Or(vec![Box::new(RangeQuery::Ge(0))]),
                    3 => RangeQuery::Not(Box::new(RangeQuery::Lt(9_999))),
                    _ => RangeQuery::And(vec![
                        Box::new(RangeQuery::Ge(0)),
                        Box::new(RangeQuery::Le(9_999)),
                    ]),
                };
                let callback = |key: &u64, _: &Vec<u64>| (false, vec![*key]);
                if mode == 4 {
                    black_box(index.range_query_rev_with(query, callback));
                } else {
                    black_box(index.range_query_with(query, callback));
                }
            },
        ));
    }
    for n in [1_000u64, 10_000, 100_000] {
        let hot = BTreeIndex::new("hot".into(), None);
        for id in 0..n {
            hot.insert(id, 0u64, 1).unwrap();
        }
        results.push(measure(&format!("delete/{n}/miss"), samples, 1, || {
            assert!(!black_box(hot.remove(n + 1, 0, 2)));
        }));
        let mut id = n;
        results.push(measure(
            &format!("delete/{n}/tail"),
            samples.min(n as usize / 2),
            1,
            || {
                id -= 1;
                assert!(black_box(hot.remove(id, 0, 2)));
            },
        ));
    }
    for low in [false, true] {
        results.push(measure(
            if low {
                "write/low_cardinality"
            } else {
                "write/high_cardinality"
            },
            samples.min(20),
            2_000,
            || {
                let tree = BTreeIndex::new("writes".into(), None);
                for id in 0..2_000u64 {
                    tree.insert(id, if low { id % 8 } else { id }, 1).unwrap();
                }
                black_box(tree);
            },
        ));
    }
    let retention = {
        let tree = BTreeIndex::new("retention".into(), None);
        let baseline = LIVE.load(Ordering::Relaxed);
        for id in 0..100_000u64 {
            tree.insert(id, 0u64, 1).unwrap();
        }
        let full = LIVE.load(Ordering::Relaxed) - baseline;
        let start = Instant::now();
        for id in 5..100_000u64 {
            assert!(tree.remove(id, 0, 2));
        }
        let delete_ns = start.elapsed().as_nanos() as u64;
        let remaining_five = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        let capacity_five = tree.query_with(&0, |ids| Some(ids.capacity())).unwrap();
        assert!(tree.remove(4, 0, 2));
        let remaining_four = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        serde_json::json!({"initial_ids":100_000,"full_heap_bytes":full,
            "remaining_5_heap_bytes":remaining_five,"remaining_5_vector_capacity":capacity_five,
            "remaining_4_heap_bytes":remaining_four,"delete_99995_ns":delete_ns})
    };
    let mut compaction = Vec::new();
    for n in [32, 300] {
        let tree = BTreeIndex::new(
            "compact".into(),
            Some(BTreeConfig {
                bucket_overload_size: 64,
                allow_duplicates: true,
            }),
        );
        for key in (0..n).rev() {
            tree.insert(1u64, format!("{key:04}-{}", "x".repeat(100)), 1)
                .unwrap();
        }
        tree.compact_buckets();
        block_on(tree.flush(Vec::new(), 1, |_, _| std::future::ready(Ok(())))).unwrap();
        let mut changed = 0;
        let mut writes = 0;
        results.push(measure(
            &format!("compact/{n}/repeat"),
            samples.min(20),
            1,
            || {
                changed += usize::from(tree.compact_buckets_with_outcome().changed);
                block_on(tree.flush(Vec::new(), 2, |_, _| {
                    writes += 1;
                    std::future::ready(Ok(()))
                }))
                .unwrap();
            },
        ));
        compaction
            .push(serde_json::json!({"buckets":n,"changed_rounds":changed,"bucket_writes":writes}));
    }
    let shared = Arc::new(seeded(1_000));
    for threads in [1u64, 4] {
        results.push(measure(
            &format!("point_read/{threads}_threads"),
            samples.min(20),
            threads * 5_000,
            || {
                std::thread::scope(|s| {
                    for worker in 0..threads {
                        let index = shared.clone();
                        s.spawn(move || {
                            for i in 0..5_000u64 {
                                black_box(
                                    index
                                        .query_with(&((i + worker) % 1_000), |ids| Some(ids.len())),
                                );
                            }
                        });
                    }
                });
            },
        ));
        results.push(measure(
            &format!("write/{threads}_threads"),
            samples.min(20),
            threads * 500,
            || {
                let tree = Arc::new(BTreeIndex::new("concurrent".into(), None));
                std::thread::scope(|s| {
                    for worker in 0..threads {
                        let tree = tree.clone();
                        s.spawn(move || {
                            for i in 0..500u64 {
                                let id = worker * 500 + i;
                                tree.insert(id, id, 1).unwrap();
                            }
                        });
                    }
                });
                black_box(tree);
            },
        ));
    }
    let mut details = Vec::new();
    for percent in [10u64, 100] {
        for latency in [0u64, 1] {
            if no_delay && latency != 0 {
                continue;
            }
            let tree = BTreeIndex::new(
                "flush".into(),
                Some(BTreeConfig {
                    bucket_overload_size: 8_192,
                    allow_duplicates: true,
                }),
            );
            let keys: Vec<_> = (0..200u64)
                .map(|i| format!("{i:04}-{}", "x".repeat(4096)))
                .collect();
            for (i, k) in keys.iter().enumerate() {
                tree.insert(i as u64, k.clone(), 1).unwrap();
            }
            block_on(tree.flush(Vec::new(), 1, |_, _| std::future::ready(Ok(())))).unwrap();
            assert_eq!(tree.metadata().buckets.len(), keys.len());
            let mut times = Vec::new();
            let (mut max_peak, mut calls, mut bytes) = (0, 0, 0);
            let (mut payload, mut bucket_writes) = (0, 0);
            for round in 0..samples.min(20) {
                for k in keys
                    .iter()
                    .take((keys.len() as u64 * percent / 100) as usize)
                {
                    tree.insert(10_000 + round as u64, k.clone(), 2).unwrap();
                }
                let live = LIVE.load(Ordering::Relaxed);
                PEAK.store(live, Ordering::Relaxed);
                let c = CALLS.load(Ordering::Relaxed);
                let b = BYTES.load(Ordering::Relaxed);
                payload = 0;
                bucket_writes = 0;
                let start = Instant::now();
                block_on(tree.flush(Vec::new(), 3, |_, data| {
                    payload += data.len() as u64;
                    bucket_writes += 1;
                    if latency > 0 {
                        std::thread::sleep(Duration::from_millis(latency));
                    }
                    std::future::ready(Ok(()))
                }))
                .unwrap();
                times.push(start.elapsed().as_nanos() as u64);
                assert_eq!(bucket_writes, keys.len() as u64 * percent / 100);
                max_peak = max_peak.max(PEAK.load(Ordering::Relaxed).saturating_sub(live));
                calls += CALLS.load(Ordering::Relaxed) - c;
                bytes += BYTES.load(Ordering::Relaxed) - b;
            }
            results.push(summarize(
                &format!("flush/{percent}_percent/{latency}_ms_per_bucket"),
                times,
                1,
                calls,
                bytes,
                max_peak,
            ));
            details.push(serde_json::json!({"dirty_percent":percent,"last_bucket_writes":bucket_writes,"last_payload_bytes":payload,"latency_ms":latency}));
        }
    }
    println!("{}",serde_json::to_string_pretty(&serde_json::json!({"profile":"workspace bench/release",
        "memory":"instrumented heap; fixture setup excluded for query/delete/flush", "measurements":results,"flush_details":details,
        "posting_retention":retention,"repeated_compaction":compaction})).unwrap());
}
