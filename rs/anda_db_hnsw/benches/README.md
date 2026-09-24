# HNSW benchmark guide

The benchmark is opt-in so cargo test --all-targets does not run long workloads.
It uses fixed corpus and graph seeds and prints one CSV row per phase.

```sh
CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_db_hnsw --bench hnsw_index -- \
  --run --n 1000 --dim 128 --queries 100 --recall-queries 20 --seed 42
```

Phases: public mixed distance, build, query, initial flush/load, query after load,
20% deletion, query after deletion, reinsertion of changed vectors, final query.

Columns include operations, total milliseconds, latency P50/P95/P99, operations
per second, allocation calls, change in live requested heap bytes, peak live
requested heap bytes, serialized bytes and recall@10. For flush/load, latency
quantiles refer to the whole pass; operations/second counts node objects.
NaN means that metric does not apply to that phase.

The counting allocator adds measurement overhead to both implementations.
It measures requested heap sizes, not allocator size classes or RSS. For OS peak
RSS, run the built benchmark executable under /usr/bin/time -l on macOS or
/usr/bin/time -v on Linux. This excludes the compiler's memory/time from the run.

## Parameters

| Flag | Default / supported values |
| --- | --- |
| --n | 1000 |
| --dim | 128 |
| --queries | 100 |
| --recall-queries | 20; use 0 to skip expensive exact ground truth |
| --seed | 42 |
| --metric | euclidean / cosine / inner / manhattan |
| --strategy | heuristic / simple |
| --distribution | uniform / clustered / duplicates |
| --m | 32 |
| --ef | 50 |
| --ef-construction | 200 |
| --layers | 16; 1 isolates base-layer costs without random-layer effects |
| --reconnect | false / true |
| --upload-concurrency | 8, in 1–64 |
| --io-ms | 0; optional simulated per-node I/O latency |
| --data | Local CSV or whitespace-separated f32 embedding rows; overrides N/D |

Stored data is rounded through bf16; queries stay f32. Exact ground truth is
computed independently in f64. Boundary tolerance is abs(kth)*0.001+1e-6,
which also works for negative inner-product distances. Nonuniform/real-data
queries are selected data rows with deterministic small perturbations.

## Suggested matrix

Run N=1k/10k/100k and D=128/384/768/1536 as hardware permits. Cross representative
cases with M=8/16/32/64, all four distances, both neighbor strategies and both
deletion modes. Include uniform, clustered and duplicate-heavy corpora before
using an application-owned embedding corpus:

```sh
CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_db_hnsw --bench hnsw_index -- \
  --run --data /absolute/path/to/embeddings.csv --metric cosine --queries 200

CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_db_hnsw --bench hnsw_index -- \
  --run --n 10000 --dim 384 --distribution clustered --reconnect true

CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_db_hnsw --bench hnsw_index -- \
  --run --n 1000 --dim 128 --io-ms 1 --upload-concurrency 8
```

For upload comparisons repeat the last case with concurrency 1 and 32. Compare
the current size-oriented publishing profile separately using
CARGO_PROFILE_BENCH_OPT_LEVEL=z. These overrides do not edit the workspace
release profile. Run repeated trials on an otherwise idle machine, holding
parameters, seeds and dependency versions constant.

Record hardware, Rust version, profile, workload and seeds with results. Report
recall together with throughput/latency and heap/RSS: a faster graph with worse
recall is a different tradeoff, not an unconditional optimization. Large matrix
coverage is a runnable capability, not a claim that every case was executed.

## September 23, 2026 comparison

[Raw phase measurements](results/review-20260923.csv) and
[environment / parameters](results/review-20260923.json) compare the HNSW source
at `975b16a7` with duplicate-aware selection, fresh f32 pruning scores, cached
cosine norms, reusable writer scratch and streamlined loading. Both binaries use
opt-level 3, LTO, the same dependencies and the counting allocator. The table
shows medians of three trials, each with 1,000 uniform vectors, M=32,
efConstruction=200, efSearch=50, 16 maximum layers and seed 42. There are 100
queries per phase and 40 exact recall queries. Only the 768-dimensional case
enables deletion reconnection. Deletion removes 199 nodes.

| Case | Build ms, before → after | Load ms, before → after | Delete ms, before → after | Build allocations, before → after |
| --- | ---: | ---: | ---: | ---: |
| Cosine, D=384 | 1563.256 → 994.543 | 11.330 → 9.098 | 12.542 → 10.775 | 398,657 → 279,224 |
| Cosine, D=768, reconnect | 2727.119 → 1763.915 | 15.790 → 13.716 | 7687.198 → 4473.675 | 398,345 → 278,826 |
| Euclidean, D=128 | 539.761 → 507.508 | 7.671 → 6.375 | 10.827 → 11.317 | 397,903 → 278,368 |

Load allocations fell by about 29% in all three cases. Cosine D=768 deletion
allocations fell from 292,007 to 55,456 with scratch reuse. Cosine recall@10
was unchanged: 1.0 at D=384 and 0.9975 at D=768, both initially and after
reinsertion. Euclidean initial recall increased from 0.9975 to 1.0; after
reinsertion it remained 0.9975. Euclidean query elapsed time increased from
12.532 to 13.190 ms initially and from 12.944 to 13.925 ms after reinsertion
(5–8%). Fresh pruning scores change graph topology, so this is a measured
quality/performance tradeoff, not an across-the-board query speedup.

Scratch reuse retains memory between mutations: the build phase's net live
requested heap grew by about 0.5 MB relative to the baseline in these cases.
For D=768 with reconnection, the deletion phase's live-byte delta changed from
−0.79 MB to +2.12 MB as its pair-distance cache grew. This is allocation reuse,
not a reduction in retained heap. Search and pair-distance caches release
capacities above 131,072 entries after an operation.

These are synthetic, non-isolated machine measurements with allocator
instrumentation. The original and loaded graphs coexist during the load phase;
its process-wide peak includes both graphs and serialized data. The table is
not a standalone index RSS estimate. Wider datasets and production hardware
need separate measurements.

## September 24, 2026 comparison

[Raw phase measurements](results/review-20260924.csv) and
[environment / parameters](results/review-20260924.json) compare the HNSW source
at `16e90f4` with branchless bf16 widening in the distance kernels, no
per-query distance map, FxHash for the node map and an extended (not rebuilt)
dirty-node set. Both binaries use opt-level 3, the same dependencies and the
counting allocator; debug line tables were kept for profiling, so LTO was off.
The table shows medians of three alternating trials with M=32,
efConstruction=200, efSearch=50, 16 maximum layers, seed 42 and 40 exact recall
queries. Deletion removes 20% of the nodes.

| Case | Build ms | Query ms | Query P50 µs | Delete ms | Reinsert ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Euclidean, N=5,000, D=128, 3,000 queries | 5727.1 → 2536.2 | 981.4 → 421.3 | 300.2 → 132.0 | 78.2 → 58.8 | 1290.4 → 591.7 |
| Cosine, N=3,000, D=384, 2,000 queries | 6182.8 → 2107.5 | 1016.1 → 384.7 | 483.3 → 178.1 | 43.3 → 28.8 | 1036.5 → 385.4 |

Recall@10 was identical in every phase (Euclidean 0.965 initially, Cosine
0.9375): finite distances are bit-identical, so both binaries build the same
graph. Flush and load times, allocation counts and live heap deltas were
unchanged within noise.

Sampling showed the `half` crate's NaN-quieting branch in `bf16::to_f32`
blocked vectorization. Before the change, distance kernels took about 60% of
build and 67% of query samples; afterwards they took about 30% of build
samples. The largest remaining build cost is the per-layer visited set (about
28%), which is inherent to sparse u64 node ids.
The machine was not isolated (load average about 8–12), so compare ratios
rather than absolute times.
