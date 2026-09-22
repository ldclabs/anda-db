# anda_db_tfs review follow-up (2026-09-06)

[中文版](anda_db_tfs_review.zh.md)

The correctness checklist F01–F10 is implemented. The existing CBOR metadata and bucket formats are retained. Baseline source: `63973ca276465f7d480a7fc2176d609f22a96fa7`.

## Completed correctness work

| ID | Change | Regression evidence |
| --- | --- | --- |
| F01 | Posting ownership is rechecked under the bucket lock by one shared unlisting helper. New postings and bucket registration are published together. | Recreated-token invariant, concurrent purge/insert of distinct ids, flush/reload. |
| F02 | A document stripe spans membership, postings and accounting; batch purge takes ordered, deduplicated stripes. | Paused removal holds its stripe; concurrent reinsert survives both search and reload. |
| F03 | The generic writer is explicitly flushed before success is published. | Buffered underlying write failure leaves buckets dirty; retry and reload succeed. |
| F04 | The file example uses an atomic staging/sync/rename helper from the metadata callback. | No-op flush and bucket failure preserve old metadata; partial staging failure cleans up its temporary file; crash leftovers are skipped without deleting them. |
| F05 | Strict load APIs reject missing manifest objects; production BM25 bootstrap uses strict loading. Partial/failed loads are read-only and can resume incrementally. | Missing-object integration test, protected partial mutations/compaction/flush, interrupted load retry. |
| F06 | Boolean OR combines separately tokenized operands, not joined raw strings. | RawTokenizer distinguishes `alpha`, `beta`, and `alpha beta`. |
| F07 | Parsing preserves operand case. | Case-sensitive plain and advanced searches agree. |
| F08 | The parser reports combined nesting-budget exhaustion to its strict entry point. | All 4,290 combinations of 0–64 parentheses and 0–65 NOT operators. |
| F09 | Metadata-only accessors preserve persisted statistics; loaded counts remain available through `len()`. | Persisted document count/average survive shell construction. |
| F10 | Optional tokenizer imports/examples are feature-gated and the feature combinations run in CI. | No-default, default, full, doctests, and file-example tests. |

The model-based test now checks BM25 scores against an independent document-based formula, in addition to retrieval sets and persistence. Candidate-restricted scoring retains global document frequency. Every materialized NOT complement, including a candidate-relative one, retains the 10,000-document guard. Direct negative postings and even NOT chains avoid constructing a complement.

### Follow-up review corrections

- Simple negative operands now remove posting ids directly from the score map. A chain such as `common AND NOT absent1 ...` no longer copies and rescans every candidate once per clause.
- Mixed-polarity candidate expressions apply the same complement limit as top-level NOT. Leading NOT pairs are cancelled first, preserving efficient double-negation queries on large result sets.
- Atomic file replacement retries a fresh sequence number after `AlreadyExists`, covering crash leftovers in PID-reusing containers.
- Metadata-only shells preserve persisted document statistics while reporting the live atomic search count.

These follow-up changes were validated after the benchmark table below was captured. A focused run of the new `not_many_absent` benchmark completed 64 absent negative clauses in about 130 µs for 1,000 documents and 865 µs for 10,000 documents on the same development machine; this is close to the cost of scoring the initial common posting rather than 64 full candidate scans.

## Performance and simplification

- O01: Fixed-corpus index and multilingual tokenizer benchmarks now cover search, insertion, removal, purge, flush, load, compaction, latency percentiles and write volume. A separate process mode measures peak memory without retaining uploads in a fake object store.
- O02: Each new term is placed once. A full tail advances for the current term instead of moving every remaining term again.
- O03: Query context samples corpus statistics once, caches IDFs, precomputes length-normalization factors and processes selective AND operands first. NOT filters compute membership only. Dense, overlapping multi-token scoring reuses first-observed document lengths, trading an additional per-query map for fewer shard lookups; single-token scoring does not allocate that cache. Map guards are released before arithmetic scoring.
- O04: Flush keeps dirty ids/versions and metadata, then serializes and uploads one bucket at a time. No bucket is marked clean until the manifest is committed.
- O05: Batch purge uses a hash set for posting membership and probes the smaller side of bucket/id intersections. The maintenance sweep remains one pass over postings.
- O06: Repeated occurrences reuse an existing frequency-map key. The built-in Jieba chain buffers/sorts one SimpleTokenizer span at a time. The public generic merge filter retains its API and global-order behavior; complete token streams are checked for equivalence.
- C01: Mutation, query execution, persistence, compaction and tests are separate private modules. Shared unlisting logic prevents divergence between remove and purge.
- C02: Removed the unused `anda_db_utils` dependency and corrected version, Vec representation, byte-length filtering, concurrency and bin-packing documentation.

## Measurements

Environment: Apple ARM64 macOS, rustc 1.97.1, workspace release profile (`opt-level = z`, LTO, one codegen unit). The default fixed corpus uses 1,000/10,000 documents and an 8 KiB bucket placement target. The table uses a final back-to-back run of the preserved baseline executable and the updated executable (`paired_before` baseline); no compilation ran alongside it. Numbers are local synthetic measurements, not production guarantees. Improvements in one workload do not imply every workload improves.

| Benchmark | Baseline mean (µs) | Updated mean (µs) | Time change |
| --- | ---: | ---: | ---: |
| `bm25_search/rare/1000` | 2.729 | 1.825 | -33.1% |
| `bm25_search/common/1000` | 65.707 | 67.394 | +2.6% |
| `bm25_search/and/1000` | 58.107 | 48.859 | -15.9% |
| `bm25_search/not/1000` | 7.088 | 5.171 | -27.0% |
| `bm25_search/or/1000` | 6.801 | 7.272 | +6.9% |
| `bm25_search/rare/10000` | 2.583 | 1.856 | -28.1% |
| `bm25_search/common/10000` | 782.055 | 854.910 | +9.3% |
| `bm25_search/and/10000` | 730.082 | 485.251 | -33.5% |
| `bm25_search/not/10000` | 15.172 | 13.592 | -10.4% |
| `bm25_search/or/10000` | 27.679 | 24.850 | -10.2% |
| `bm25_mutation/new_terms/256` | 1709.366 | 321.365 | -81.2% |
| `bm25_mutation/new_terms/512` | 6347.192 | 645.203 | -89.8% |
| `bm25_mutation/new_terms/1024` | 24158.317 | 1286.401 | -94.7% |
| `bm25_mutation/remove_100` | 3252.975 | 3279.566 | +0.8% |
| `bm25_mutation/purge_100` | 356.625 | 199.059 | -44.2% |
| `bm25_persistence/flush_1000` | 930.156 | 898.116 | -3.4% |
| `bm25_persistence/load_1000` | 1953.336 | 2064.030 | +5.7% |
| `bm25_persistence/compact_1000` | 1316.066 | 1298.208 | -1.4% |
| `bm25_chinese/search_1000` | 270.149 | 183.672 | -32.0% |

Individual warmed-call latency:

| Documents | Query | p50 (µs) | p95 (µs) |
| ---: | --- | ---: | ---: |
| 1000 | common | 66.083 | 76.333 |
| 1000 | and | 47.916 | 51.791 |
| 10000 | common | 829.042 | 1036.041 |
| 10000 | and | 452.041 | 595.250 |

Same-revision tokenizer comparison (both use the updated frequency collector):

| Corpus | Generic Jieba mean (µs) | Streaming Jieba mean (µs) | Time change |
| --- | ---: | ---: | ---: |
| english | 1037.429 | 1056.308 | +1.8% |
| chinese | 1489.727 | 1253.911 | -15.8% |
| mixed | 2785.557 | 2322.592 | -16.6% |

Some single-term and sparse boolean queries retain overhead; it is reported alongside improvements rather than hidden. Normalizing boolean operands independently is required for custom tokenizer correctness. The document-length cache is enabled only when posting lengths predict substantial overlap, so disjoint OR lists do not pay for an ineffective cache. Loading measurements had substantial baseline variance and should not be interpreted as a demonstrated speedup.

A separate 20,000-document / 160,000-distinct-term process run discarded uploaded payloads. The same driver was linked once against the baseline library and once against the updated library. Child-process `getrusage().ru_maxrss` reported **57,491,456 → 48,693,248 bytes**, approximately **15.3% lower peak RSS**. Both layouts had 650 buckets; total encoded data was about 4.815 MB and the largest bucket 7,427 bytes. The old flush retained all serialized bucket buffers before upload; the new flush retains at most one. RSS also includes the index and allocator overhead, so its reduction is not equal to payload size alone. This was one process run per version, not a statistical latency comparison.

## Evaluated storage tradeoffs (O04, O05, O07)

**High-frequency postings (O07): evaluation completed; segmentation is not enabled in this change.** In the 10,000-document shared-vocabulary case, one additional document rewrote about 580 KB despite the 8 KiB placement target. This confirms the documented soft-limit/write-amplification tradeoff. Bounded in-memory flush buffering does not fix it.

Segmenting one term across durable objects would change the current single-owner posting/bucket invariant, loader reconciliation, deletion bookkeeping, manifest reachability and obsolete-object collection. A future segmented format should version the wire representation explicitly, read existing full postings, maintain per-segment membership/dirty state, and checkpoint/migrate atomically. Its acceptance tests need segment-boundary insert/remove/purge, partial/cancelled writes, missing segments, legacy migration and compaction/reload equality. It should be compared on incremental uploaded bytes and p95 flush latency with the real storage backend. This patch preserves the existing storage format rather than silently introducing that migration.

**Bounded parallel upload (O04): evaluated, not added to the default API.** The measured backend is an in-memory sink, so the data does not demonstrate an upload-latency benefit. Sequential streaming gives the memory bound without changing callback ordering or adding a runtime dependency. A separately configurable concurrent uploader should be evaluated with a latency-bearing backend and keep the manifest commit after all uploads succeed.

**Document reverse index (O05): evaluated, not added.** The batch hash-set optimization already reduces purge cost. A persistent/reconstructed document-to-term/bucket map adds memory and another invariant to every mutation/load path; adopt it only if single-document deletion dominates a representative workload. Keep text-free recovery as a full sweep so it does not depend on bookkeeping it may be repairing.

## Reproduce

```sh
# Capture the baseline using this benchmark source on the baseline revision.
cargo bench -p anda_db_tfs --features full --bench tfs_index -- --save-baseline before
# On the updated revision, measure the same corpus and compare.
ANDA_TFS_METRICS=1 cargo bench -p anda_db_tfs --features full --bench tfs_index -- --baseline before
cargo bench -p anda_db_tfs --features full --bench tfs_tokenizer
# Opt into the larger corpus; its first run needs its own saved baseline.
ANDA_TFS_BENCH_LARGE=1 cargo bench -p anda_db_tfs --features full --bench tfs_index
# Build once, then run the resulting tfs_index executable directly, with
# ANDA_TFS_MEMORY_PROFILE=1 and optionally ANDA_TFS_PROFILE_DOCS=100000.
# Measure its own process RSS, not cargo/rustc's memory.
cargo bench -p anda_db_tfs --features full --bench tfs_index --no-run
```

`ANDA_TFS_METRICS=1` reports individual-call p50/p95 over 1,000 warmed searches, separately from Criterion estimates. The memory mode discards upload payloads rather than retaining a second copy of the index in a memory store. `getrusage(RUSAGE_CHILDREN).ru_maxrss` is bytes on macOS and KiB on Linux; use a fresh parent process per measured child.

## Validation

- `cargo test -p anda_db_tfs --no-default-features`
- `cargo test -p anda_db_tfs`
- `cargo test -p anda_db_tfs --all-features` (69 unit tests, one property test, 10 integration tests, four doctests at this revision)
- `cargo test -p anda_db_tfs --features full --example tfs_demo`
- `cargo clippy -p anda_db_tfs -p anda_db --all-targets --all-features -- -D warnings`

- `cargo test --workspace --all-features`: 1530 tests passed; 1 ignored at this revision. Includes the production missing-bucket bootstrap and stored-format compatibility tests.

The ignored workspace test is the intentional on-disk fixture generator (`generate_fixture_for_current_version`); it is not a skipped correctness test. Existing stored-format compatibility tests passed.
