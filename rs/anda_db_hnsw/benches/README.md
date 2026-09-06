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
