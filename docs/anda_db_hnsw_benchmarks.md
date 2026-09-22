# HNSW Post-Fix Benchmark Measurements

[中文版](anda_db_hnsw_benchmarks.zh.md)

Dated 2026-09-06. Evaluated on the same machine, Rust 1.97.1, `opt-level=3`; baseline reflects the pre-modification HNSW source snapshot. Comparative scenarios used single-layer graphs with a fixed random seed (42) to eliminate variances from legacy builds lacking injectable layer RNG. The 1,000-node comparison reports the median of 3 runs; remaining cases represent single-run scale/functional measurements.

Benchmarks utilize an allocation-counting allocator, introducing measurement overhead to both baselines; the host machine was not isolated for exclusive execution. Consequently, these metrics represent reproducible engineering benchmarks rather than production capacity guarantees.

[Full CSV](../rs/anda_db_hnsw/benches/results/review-20260906.csv) · [Environment and Notes](../rs/anda_db_hnsw/benches/results/review-20260906.json) · [Reproduction Instructions](../rs/anda_db_hnsw/benches/README.md)

## Comparison Under Default Connectivity

N=1,000, D=128, M=32, efConstruction=200, efSearch=50, uniform synthetic data. Query duration reports total wall-clock time across 100 queries; recall is computed against an independent f64 brute-force oracle across 20 sample queries. Storage backend is in-memory with single-stream upload and zero injected I/O latency.

| Metric | Euclidean: Old → New | Cosine: Old → New |
| --- | --- | --- |
| Build 1,000 nodes | 454.247 → 512.234 ms | 667.705 → 580.309 ms |
| Execute 100 queries | 12.549 → 11.044 ms | 16.306 → 11.728 ms |
| Query allocation count | 1,761 → 100 | 1,800 → 100 |
| recall@10 | 1.000 → 0.995 | 1.000 → 1.000 |
| Save duration | 1.511 → 1.444 ms | 1.433 → 1.427 ms |
| Peak requested heap bytes during save | 9,158,392 → 4,858,884 | 9,189,539 → 4,648,798 |
| Load duration | 4.369 → 7.993 ms | 4.473 → 7.887 ms |
| Delete 199 nodes | 5.790 → 11.076 ms | 6.628 → 11.243 ms |

Verified improvements: The Cosine workload achieved identical recall while total query latency decreased by ~28%, query allocations dropped by ~94%, and peak requested heap during save dropped by ~49%.

Documented tradeoffs: Euclidean build latency increased by ~13%; comprehensive inbound edge cleanup adds overhead to deletions on dense graphs; loading incurs extra latency to handle generational tags, structural checks, and reverse reference tracking. Euclidean recall with identical parameters shifted from 200/200 hits to 199/200; this timing change cannot be framed as a strictly lossless performance increase at identical recall.

The public `DistanceMetric::compute_mixed` function added comprehensive input validation: 10,000 Euclidean calculations increased from 0.801 ms to 2.166 ms, and Cosine increased from 1.065 ms to 2.597 ms. Hot index search paths operate on pre-validated vectors and pre-processed queries to avoid redundant public boundary checks.

## Scale Verification for Deletion Scans

N=10,000, D=128, M=2, efConstruction=16, efSearch=16, single-layer graph; deleting 1,999 non-entry nodes:

| Metric | Prior | New |
| --- | --- | --- |
| Delete duration | 94.052 ms | 7.017 ms |
| Save duration | 19.082 ms | 7.613 ms |
| Load duration | 26.349 ms | 26.424 ms |
| Peak requested heap bytes during save | 66,120,932 | 16,881,599 |

Deleting non-entry nodes no longer triggers a full graph scan, running ~13.4x faster under this sparse configuration. Note that recall is intentionally low here (0.070 / 0.055) as this configuration is designed solely to isolate scan and capacity overhead, not as a recommended retrieval configuration.

## Node Uploads Under Simulated Latency

New implementation, N=1,000, D=128, with 1 ms simulated I/O latency per node:

| Upload Concurrency | Total Save Duration |
| --- | --- |
| 1 | 2,273.961 ms |
| 8 | 287.556 ms |

This scenario runs ~7.9x faster; real-world benefits depend on backend concurrency limits, rate limiting, and network latency. Regression tests independently verify that IDs and metadata are committed only after all node uploads succeed, subject to configured concurrency and byte budget bounds.

## 100,000-Node Scale Test

New implementation, N=100,000, D=128, M=32, efConstruction=200, efSearch=100, 16 max layers, uniform synthetic data. 50 queries executed, exact recall sampled across 5 queries:

| Metric | Result |
| --- | --- |
| Graph build | 516.208 s (~194 nodes/s) |
| Query latency P50 / P95 / P99 | 2.904 / 3.483 / 3.708 ms |
| Initial recall@10 | 0.740 |
| Save duration | 188.240 ms (94,080,629 serialized bytes) |
| Load duration | 1,106.599 ms |
| Post-reopen recall@10 | 0.740 |
| Delete 19,999 nodes | 2,472.523 ms |
| Post-delete recall@10 | 0.680 |
| Re-insert modified vectors | 144.496 s |
| Post-reinsert recall@10 | 0.640 |
| Peak requested heap bytes during load | 756,758,941 |

This test validates scale, persistence, and search paths, **without claiming this parameter set satisfies high-recall production requirements**. High-dimensional uniform synthetic data is challenging; default deletion without edge reconnection also degrades graph connectivity and recall. A 5-query sample is insufficient for production quality conclusions; production deployments should sample domain-specific vector distributions, increase `efSearch`, and evaluate edge reconnection or periodic re-indexing.

Additional runs covering 1,000x384 clustered vectors under InnerProduct/Manhattan and reconnect-on-delete modes are recorded in the CSV. Duplicate vectors, dual neighbor selection heuristics, and deletion modes are validated in regression tests.

## Measurement Methodologies

- `peak_live_bytes` and `live_byte_delta` reflect heap volume observed by the counting allocator, inclusive of other objects surviving in the process, and do not equal the minimal resident memory of a single index.
- `maxrss_raw` records raw peak RSS via `getrusage` per child process, preserving OS-specific units without conflating process RSS with heap bytes.
- Load measurements concurrently retain the source index, serialized graph bytes, and reopened index, resulting in higher peak memory than starting a single standalone index.
- Legacy deletions left dangling inbound references; the repaired implementation performs full cleanup. Changes in deletion latency must be evaluated against the corrected semantics.
- The release profile retains `opt-level='z'`; speed benchmarks override this via environment variables without altering the workspace-wide release profile.
