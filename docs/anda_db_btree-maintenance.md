# B-tree Maintenance Checklist and Verification Results

[中文版](anda_db_btree-maintenance.zh.md)

## 2026-09-23 Follow-up

Implemented against `eb950fd623796b47ba56f6ba5db5120117d9cf4f`, without
subagents, new dependencies, public API changes or persisted-format changes.

- [x] Remove the final ID and its posting atomically under one DashMap shard
  lock. Both single and batch deletion use `remove_if_mut`; batch deletion
  accumulates bucket deltas directly without an intermediate removal vector.
  Controlled thread interleavings cover empty-posting visibility, same-ID
  reinsertion, uniqueness enforcement and flush/reload.
- [x] Make compaction independent of the previous bucket-ID encoding width.
  A fixed five-byte ID estimate keeps repeated packing stable. Regressions
  cover 32 and 300 buckets, unchanged flushes and reopening.
- [x] Remove full-shard emptiness checks from range and prefix query paths.
  Query validation still precedes evaluation, including on empty indexes.
- [x] Shrink posting vectors and position maps only after substantial
  deletion, retaining growth slack. A 100,000-ID regression checks both
  allocations, remaining membership, regrowth and non-tail removals.
- [x] Sort/deduplicate owned Include vectors in place, collect boxed query
  conversions directly, and move the prepared manifest into committed state
  instead of cloning it.
- [x] Extend the benchmark with Eq/Ge versus direct point lookup, Include
  sizes, retained heap after deletion and repeated compaction plus flush.

### Measurements

arm64 macOS, 10 available CPUs, rustc 1.98.1, workspace bench profile
(`opt-level=z`, LTO), the same counting allocator and extended harness for
both versions. The baseline uses the pre-fix implementation. CPU-only runs
used 100 samples for queries and 20 for compaction/flush, without concurrent
compilation. Eq/Ge use 1,000 queries per timing sample, stop on the first
match and allocate no result; the table divides batch time by 1,000.

| Scenario | Before | After |
|---|---:|---:|
| Direct point lookup, median ns/query | 29.9 | 29.3 |
| RangeQuery::Eq, median ns/query | 372.1 | 34.0 |
| Ge first match, median ns/query | 454.8 | 114.0 |
| Include 128 distinct keys, median μs/query | 3.833 | 1.583 |
| Include 128 allocations/query (including input/result) | 14 | 2 |
| Include 4,096 allocations/query (including input/result) | 378 | 2 |
| Retained heap: 100,000 IDs reduced to 5 | 3,277,224 B | 3,608 B |
| Retained heap: 100,000 IDs reduced to 4 | 1,048,960 B | 1,392 B |
| Bucket writes over 20 unchanged compactions, 32 buckets | 640 | 0 |
| Bucket writes over 20 unchanged compactions, 300 buckets | 6,000 | 0 |

Retained heap is the live allocation increase relative to the already-created
empty index, not RSS. Vector capacity at five IDs fell from 131,072 to 126.
Deleting 99,995 IDs took 11.29 ms before and 11.70 ms after in this single
retention run; shrinking trades occasional reallocation/rehashing for bounded
retained capacity. Existing 100,000-ID miss/tail deletion medians stayed at
42/125 ns. The manifest change reduced allocations per flush; no flush
latency improvement is claimed. These microbenchmarks do not predict total
application throughput.

Raw results: [before](../rs/anda_db_btree/benches/results/review-2026-09-before.json)
and [after](../rs/anda_db_btree/benches/results/review-2026-09-after.json).

### Validation

All 1,734 workspace tests passed; one fixture-generation test remains ignored
by design. This includes B-tree recovery, crash recovery and historical format
compatibility. The B-tree all-targets suite, including the atomic-file example,
also passed. Workspace all-feature check, all-target/all-feature Clippy with
`-D warnings`, formatting, agent-document consistency and 32 local documentation
links passed. Each new correctness/capacity regression failed against the
pre-fix implementation before passing with the changes.

## 2026-09-06 Maintenance

Dated 2026-09-06; implemented against the review checklist based on commit `f54fd747e3f33d5d32cf8c1686874bfb59da55fc`.
Executed without subagents. No new production dependencies were introduced; CBOR layout of public buckets and metadata was preserved; DashMap serde features enabled only for legacy test serializers were removed.

## Completion Checklist

| Status | Item | Implementation and Evidence |
|---|---|---|
| [x] | B1 Empty manifest vs legacy format confusion | Internal loading DTO distinguishes missing fields from explicit empty maps, rejecting explicit nulls; fresh empty indexes do not probe legacy buckets; clean legacy indexes upgrade on first flush |
| [x] | B2 Incomplete index writability | Full load fails if buckets are missing; `MetadataOnly`/`Partial`/`Ready` state checks; explicit partial load with retry on failure/cancellation; Collection reopen tests with missing buckets |
| [x] | B3 Deep query destructor stack overflow | Iterative validation and deallocation; limits on Serde input depth, node count, and total `Include` clauses; 100,000-level child process tests, empty index, transformations, and reverse query coverage |
| [x] | B4 Metadata premature truncation | Tempfile fsync and atomic rename within commit callbacks; tests for no-change, bucket failure, pre-rename failure, and reopen across legacy/new snapshots |
| [x] | B5 Missing save on same-bucket-count compaction | `CompactionOutcome.changed`; stable sorting, true no-op handling; encapsulated saves verify same-bucket rebuilds and subsequent reopen checks |
| [x] | PERF1 Large posting deletion | Small lists stored inline; larger lists add an ID-to-position index for average O(1) value deletion; array reuse, single-hash append, shrink-to-fit fallback; counter and reload tests |
| [x] | PERF2 Full-candidate allocation on complex pagination | Compiles disjoint intervals from borrowed boundaries; unifies boolean combinations into interval algebra; eliminates FV cloning and linear matching over nested Include candidates; preserves ordering and callback groups |
| [x] | PERF3 Save-time cloning and peak memory | Borrowed serialization without cloning postings or auxiliary tables; bucket-by-bucket encoding and upload; O(bucket_count) manifests with single-bucket data buffers |
| [x] | PERF4 Performance baseline and candidate evaluation | Release benchmark with allocation counting compares old and new versions; full forward measurements and reverse CPU verification; records quantiles, throughput, memory, and serialized bytes |
| [x] | M1 State and code structure | `BucketState`, `Posting`, `Removal`, `LoadState`; unified append, removal, and cleanup primitives for single/batch mutations; clean separation of query/mutation/persistence/compaction/posting/state; independent test suites |
| [x] | M2 Model and fault testing | Sequential array operations, batch replacement, compaction, incremental checkpoint/reopen, bucket/manifest failure injection; sorted group comparisons; concurrency tests with shared FVs, duplicate inputs, and terminal deletions |
| [x] | M3 Documentation and examples | Technical docs rewritten with corrections on loading, uniqueness, complexity, locking, and retry token semantics; enabled and expanded runnable doctests |

Implementations reside primarily in the [B-tree submodule](../rs/anda_db_btree/src/btree/), with regressions in [regressions.rs](../rs/anda_db_btree/tests/regressions.rs), [model tests](../rs/anda_db_btree/tests/proptest_model.rs), [concurrency tests](../rs/anda_db_btree/tests/concurrency.rs), and [Collection recovery tests](../rs/anda_db/tests/btree_recovery.rs).

## Performance Results

Environment: arm64 macOS, rustc 1.97.1; repository bench/release profile (`opt-level=z`, LTO). Legacy source was preserved prior to modification and incorporated into the benchmark harness. Tests were run on a warmed-up CPU with no compilation running concurrently. A full forward pass (legacy -> new) was followed by a reverse pass (new -> legacy) without injected latency.

Regular query/delete scenarios used 100 samples; construction, multi-threading, and save scenarios used 20 samples. The counting allocator introduces measurement overhead; nanosecond-level latencies reflect timer quantization; speedups describe these specific workloads and do not extrapolate directly to total production throughput. JSON results record p95/p99, throughput, allocations, and serialized payload sizes.

Queries evaluated 10,000 keys with a callback retaining 1 result; large postings contained 100,000 IDs. Persistence workloads evaluated 200 buckets with exclusive postings (keys ~4 KiB), verifying that exactly 10% or 100% of buckets were written per round. Memory columns reflect peak additional live heap during the operation, excluding pre-built query/save inputs, and do not represent process RSS.

| Scenario | Prior Median μs | New Median μs | Speedup | Additional Peak Heap Bytes: Before → After |
|---|---:|---:|---:|---:|
| Standard range first item (10k keys) | 0.500 | 0.458 | 1.09× | 40 → 8 |
| And range first item (10k keys) | 235.583 | 0.583 | 404.09× | 233,216 → 80 |
| Or range first item (10k keys) | 630.875 | 0.666 | 947.26× | 327,224 → 200 |
| Not filter last item (10k keys) | 285.333 | 0.583 | 489.42× | 278,568 → 128 |
| And reverse first item (10k keys) | 338.208 | 0.834 | 405.53× | 233,256 → 312 |
| 100k IDs: delete non-existent ID | 31.125 | 0.042 | 741.07× | 0 → 0 |
| 100k IDs: delete trailing ID | 31.250 | 0.125 | 250.00× | 0 → 0 |
| High cardinality: build 2,000 associations | 747.541 | 644.250 | 1.16× | 505,128 → 410,480 |
| Low cardinality: build 2,000 associations | 300.125 | 289.542 | 1.04× | 75,360 → 110,176 |
| 10% dirty bucket save, no latency injection | 127.208 | 114.916 | 1.11× | 179,917 → 19,617 |
| 100% dirty bucket save, no latency injection | 1,311.583 | 1,245.458 | 1.05× | 1,669,133 → 29,123 |
| 100% dirty bucket save, 1 ms injected per bucket | 311,320.083 | 311,497.083 | 1.00× | 1,670,285 → 28,403 |

Deterministic counting regressions confirm: complex first-item pagination eliminates FV cloning; deleting from a 10,000-ID posting avoids 10,000 sequential comparisons; flush avoids 20,000 PK clones on a 10,000-ID list. Standard forward queries also reuse the initial callback result buffer, eliminating a redundant allocation.

In the high-cardinality build scenario, allocations dropped from ~4,661 to 2,659, reducing peak heap by ~19%. In low-cardinality builds, peak heap increased from 75,360 to 110,176 bytes (~46%): storing positional indices incurs an extra array index to guarantee average O(1) value deletion on large lists. This is an explicit memory tradeoff; small postings do not allocate position maps.

For 100% dirty bucket flushes without injected latency, peak heap dropped from ~1.67 MB to ~29 KB (~98% reduction). With injected latency, overall wall-clock time remains dominated by serialized I/O, without claiming throughput improvements for that scenario. The reverse CPU pass confirmed identical trends: ~400–946× on complex first-item pagination, ~249× on trailing deletions from large lists, and ~1.15× on high-cardinality construction, with point queries essentially unchanged.

Raw benchmark outputs: [Full legacy baseline](../rs/anda_db_btree/benches/results/verified-before.json), [Full new implementation](../rs/anda_db_btree/benches/results/verified-after.json), [CPU legacy verification](../rs/anda_db_btree/benches/results/cpu-before.json), and [CPU new verification](../rs/anda_db_btree/benches/results/cpu-after.json).

## Candidate Roadmap Decisions

- Serial I/O callback contracts are retained. Simulated latency confirmed the I/O bottleneck, but production object store metrics are pending; parallel loading involves lending `AsyncFnMut`, callback sequencing, and memory budgets, which should be introduced via optional interfaces rather than concurrently invoking legacy callbacks.
- Mutex-guarded statistics and query counters remain unchanged. Benchmarks showed neutral point queries and improved multi-threaded writes; there is insufficient justification for sharded counter state.
- Compact representations for small postings are adopted. Chunked persistence for large postings would alter storage formats and reclamation protocols, which was deferred to avoid breaking format backward-compatibility. Isolated oversized postings still require a full encoding buffer.

These decisions conclude the scope evaluated under PERF4.

## Verification Commands

```sh
cargo test --workspace --all-features
cargo test -p anda_db_btree -p anda_db --all-features
cargo test -p anda_db_btree --all-targets --all-features
cargo clippy -p anda_db_btree -p anda_db --all-targets --all-features -- -D warnings
cargo fmt -p anda_db_btree -p anda_db -- --check
ANDA_BTREE_RUN_BENCH=1 cargo bench -p anda_db_btree --bench workloads
ANDA_BTREE_RUN_BENCH=1 ANDA_BTREE_BENCH_NO_IO_DELAY=1 cargo bench -p anda_db_btree --bench workloads
```

All 1,512 workspace tests passed (fixture generation ignored by design). Two subsequently added load-boundary regressions and final query allocation optimizations passed clean across both crates. Final module tests, Clippy, and formatting checks passed. No changes were made to TypeScript or generated schemas.

Workspace `cargo fmt --all -- --check` reports pre-existing formatting variance in untouched KIP files (`rs/anda_kip/src/parser.rs`, `rs/anda_kip/src/parser/common.rs`, `rs/anda_kip/src/parser/json.rs`), which were deliberately excluded from this scope.
