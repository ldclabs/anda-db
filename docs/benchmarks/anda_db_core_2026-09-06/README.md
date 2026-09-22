# AndaDB Core Remediation Performance Benchmarks (2026-09-06)

[中文版](README.zh.md)

This report documents comparative directional performance validations on identical hardware under identical workloads; microbenchmark speedups should not be extrapolated as overall database speedup. The default release build retains `opt-level=z, lto=true`; an optional `release-speed` build (`opt-level=3`) was also measured.

Environment: Rust 1.97.1, arm64, macOS 26.6.2. Main in-memory dataset: 10,000 documents with 100 queries; local filesystem dataset: 1,000 documents with 20 queries. Filesystem backend: LocalFileSystem + MetaStore with fsync enabled. Simulated storage latencies were injected using an InMemory wrapper.

**Default Build: Pre- vs Post-Remediation**

| Workload | Pre-Fix | Post-Fix | Notes |
| --- | ---: | ---: | --- |
| OR pagination p50 | 0.608375 ms | 0.001167 ms | 521.32× |
| AND pagination p50 | 0.248833 ms | 0.012125 ms | 20.52× |
| Single-term BM25 p50 | 0.895375 ms | 0.935333 ms | Neutral (4.5% increase) |
| Selective BM25 p50 | 0.902833 ms | 0.401792 ms | 2.25× |
| 1.1 MB serial codec p50 | 0.304000 ms | 0.328333 ms | Neutral (8.0% increase) |
| Filesystem backend, additional Rust heap for 100 updates | 19.513 MiB | 0.243 MiB | 98.8% reduction |
| InMemory, additional Rust heap for 100 updates | 38.772 MiB | 19.599 MiB | Backend itself retains log objects in RAM |
| Additional GETs across 64,000 hot reads + 1,000 cold writes | 273 | 20 | 92.7% reduction; stripes increased from 256 to 4096 |
| Max observed timer delay under 2 concurrent codec write tasks (1 ms timer) | 46.595 ms | 2.913 ms | Reflects scheduler latency under load; not production p99 |
| Fetch 10 results satisfying owner filter | 1 | 10 | Target dataset had sufficient matches; subset calculation filled results |

Memory figures track allocations via Rust's global allocator, excluding C-library internal mallocs and process RSS. On-disk logs continue to store full before/after images, while in-memory collection handles track only sequence numbers pending sweep. Because InMemory retained log objects contribute to heap measurements, heap reductions on InMemory are smaller than on the filesystem backend.

**Recovery I/O: 100 Pending Mutation Intents**

| Injected Per-Operation Latency | Pre-Fix | Post-Fix | Speedup |
| --- | ---: | ---: | ---: |
| 1 ms | 0.782 s | 0.112 s | 7.01× |
| 10 ms | 3.633 s | 0.579 s | 6.28× |
| 50 ms | 14.617 s | 2.379 s | 6.14× |

All runs issued identical requests: **172 GET, 3 PUT, 100 DELETE, 1 LIST**. Latency gains stem from bounded concurrency, not truncated reads or premature log truncation. Measured timer wait periods incorporate platform scheduling overhead; the injected 1 ms latency cannot be treated as raw request latency.

**Optional Speed Build**

| 10,000-Document In-Memory Workload | Post-Fix Default (`opt-level=z`) | `release-speed` (`opt-level=3`) |
| --- | ---: | ---: |
| bounded_or p50 | 0.001167 ms | 0.000667 ms |
| bounded_and p50 | 0.012125 ms | 0.005667 ms |
| single_bm25 p50 | 0.935333 ms | 0.473208 ms |
| selective_bm25 p50 | 0.401792 ms | 0.166000 ms |
| reopen p50 | 7.303166 ms | 3.006959 ms |
| large_codec p50 | 0.328333 ms | 0.253875 ms |

Benchmark binary size increased from ~2.90 MiB to 4.79 MiB (+65.3%). Note that this is the benchmark executable size, not overall library size. Default release profiles remain unchanged; throughput-sensitive deployments may opt into `release-speed`.

**Documented Tradeoffs and Boundaries**

- Single-term BM25 under default `z` profiles showed neutral latency; eliminating redundant RRF/deduplication reduces intermediate allocations, but underlying term scoring dominates. The speed profile was noticeably more effective here.
- Purging 100 logs on local filesystem shifted from ~164 ms to 178 ms; bounded concurrency yields negligible benefits under local metadata synchronization costs. Speedups observed under simulated network latency do not apply to local NVMe storage.
- Individual large codec operations incur thread-switching costs; the bounded blocking pool primarily improves timer schedulability under concurrent load. Only owned codec buffers are offloaded, preventing borrowed Collection/index mutations from escaping to background workers.
- Full before/after images are preserved in logs for crash replay and legacy schema recovery. Differential/index-only logging would require new persistence formats; dedicated intent budgets and sequence tracking resolve memory amplification for legitimate large documents without format changes.
- WAL reclamation remains guarded by exclusive lifecycle locks, ensuring `close`, `delete`, and `recreate` operations await completion of prior cleanup; bounded concurrency minimizes lock contention. Background delete loops crossing collection rebuild boundaries were rejected.
- Broad non-PK intersections may still require O(matches) memory. Bounded OR, PK boolean scans, selectivity reordering, and candidate-set NOT operations were optimized, without claiming arbitrary expressions execute within O(limit) memory.
- Selective pre-filtering ranks RRF within candidate subsets. To retain global ranking followed by filtering, set `SearchOptions { prefilter_limit: 0, adaptive: false, ..Default::default() }`.

Timer measurements utilize harness v3: elapsed time measurement begins before releasing CPU worker threads, covering the entire active period to prevent starved timers from recording zero samples. Earlier measurements capturing only initial yield bursts are archived in `scheduler_initial.jsonl` and excluded from these conclusions.

**Reproduction Commands**

```bash
cargo bench -p anda_db --features full --bench core_workloads
ANDA_BENCH_IO=1 cargo bench -p anda_db --features full --bench core_workloads
ANDA_BENCH_CACHE=1 cargo bench -p anda_db --features full --bench core_workloads
ANDA_BENCH_SCHEDULER=1 cargo bench -p anda_db --features full --bench core_workloads
ANDA_BENCH_BACKEND=fs ANDA_BENCH_DOCS=1000 ANDA_BENCH_ITERATIONS=20 cargo bench -p anda_db --features full --bench core_workloads
cargo bench -p anda_db --features full --bench core_workloads --profile release-speed
```

The baseline was compiled from a pre-remediation copy of `anda_db` source using the identical benchmark harness and workspace dependencies. Raw data is archived in `results.jsonl`, phase `.log` files, and `environment.json`; source digests reside in `source_fingerprints.json`. No live cloud or physical power-loss tests are claimed.
