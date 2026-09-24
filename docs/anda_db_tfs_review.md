# anda_db_tfs review follow-up (2026-09-06)

[中文版](anda_db_tfs_review.zh.md)

> **Superseded in part (2026-09-24):** NOT complements are now limited only
> when they span the whole index; candidate-relative complements are no longer
> guarded, `NOT NOT x` is planned as `x`, and scoring takes DF from the posting
> length. See [anda_db_tfs.md](anda_db_tfs.md) for the current rules.

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

## Follow-up review — 2026-09-23

This follow-up uses `cdc5880` as its implementation baseline. It preserves the
CBOR format and addresses the following ordinary query and maintenance cases:

- Normalize all associative OR operands, including mixed AND/NOT expressions.
  Direct token operands are deduplicated after tokenization, so
  `run OR run OR (beta AND gamma)` and `(run OR run) OR (beta AND gamma)` have
  identical scores and ranking. Complex branches still contribute their scores.
- Make compaction weights independent of previous bucket IDs by reserving a
  fixed maximum u32 CBOR width. Repeated compaction of an unchanged index,
  including after reload and across the 24/256 bucket-ID boundaries, does not
  advance the version or upload objects.
- Short-circuit empty candidate sets before posting access or selectivity
  estimation, while retaining parser errors and successful-query counters.
- Allocate result maps from actual matches. Missing and rare queries no longer
  reserve a thousand entries; common queries avoid repeated result-map growth.
  The existing shared scorer still preserves global DF and last-entry-wins
  handling of historical duplicate postings.
- Tokenize removals before taking mutation/document locks. Share posting pruning
  and size accounting between removal, purge and loading, and share atomic empty
  posting removal/ownership rechecks. This removes nested bookkeeping maps and
  temporary sets without changing lock order or recovery behavior.
- Reclaim oversized posting capacity during compaction, including single-bucket
  indexes. Lists above 64 entries of capacity and below one quarter occupancy
  shrink with room for twice the surviving length. Capacity-only changes do not
  dirty the index.
- Correct the English/Chinese flush concurrency contract and stripe-lock order.

Regression coverage includes mixed OR ranking and stemming, compaction followed
by flush/reload/no-op compaction, a posting-lock test for empty candidate sets,
removal paused inside its actual mutation critical section, and allocation
budgets. The operation model now includes purge and compaction. A separate
recursive boolean model covers truth sets and globally scored candidate subsets.
Benchmarks add missing terms, 0/1/100 candidates, mixed OR, unchanged compaction,
and maintenance after deleting 90% of the corpus.

### Allocation measurements

On Apple ARM64 macOS with rustc 1.98.1, a standalone allocator probe using the
same `default_tokenizer` corpus measured the following cumulative allocation
requests per query. These are allocator request bytes, not RSS or retained heap.

| Case | Before (bytes) | After (bytes) |
| --- | ---: | ---: |
| Missing term, 10,000 documents | 35,271 | 447 |
| One-hit term, 10,000 documents | 35,572 | 824 |
| Common term with an empty candidate set, 10,000 documents | 148,060 | 450 |

The committed allocation test uses `SimpleTokenizer` to isolate index costs:
missing/one-hit queries request 251/622 bytes, and an empty candidate query
requests 250 bytes (448 with boolean parsing), both at 1,000 and 20,000 documents.
A single-bucket corpus reduced from 20,000 to 1,000 documents releases a net
492,288 requested bytes during maintenance, without dirtying the clean index.
The test also verifies insertion after shrinking.

```sh
cargo test -p anda_db_tfs --all-features --test allocations -- --nocapture
```

### Paired timing measurements

The preserved baseline executable and final executable ran consecutively after
all builds/tests completed, with no concurrent compilation. Both use the updated
fixed-corpus benchmark, the workspace size-oriented release profile (`opt-level=z`,
LTO, one codegen unit), 20 samples, 300 ms warmup and one-second target measurement.
This second paired run rechecked small regressions observed in the first pass;
values below are Criterion mean estimates, not production latency guarantees.

| Case | Before (µs) | After (µs) | Change |
| --- | ---: | ---: | ---: |
| Missing term / 10,000 | 1.255 | 1.065 | -15.1% |
| Rare term / 10,000 | 1.643 | 1.417 | -13.8% |
| Common term / 10,000 | 771.887 | 587.376 | -23.9% |
| Empty candidate set / 10,000 | 310.437 | 1.005 | -99.7% |
| One candidate / 10,000 | 330.333 | 326.424 | -1.2% |
| AND / 10,000 | 333.659 | 329.689 | -1.2% |
| Mixed OR / 10,000 | 362.907 | 369.305 | +1.8% |
| Remove 100 | 3187.045 | 3175.205 | -0.4% |
| Purge 100 | 189.755 | 187.503 | -1.2% |
| Load 1,000 | 2158.747 | 2202.623 | +2.0% |
| Unchanged compaction + flush / 20,000 | 63041.229 | 10442.895 | -83.4% |
| Maintenance after 90% deletion / 1,000 | 109.255 | 120.719 | +10.5% |

The repeat run did not establish a substantial AND, mixed-OR, load, or deletion
speed change; it retained those small differences instead of claiming universal
speedups. Maintenance after bulk deletion costs about 11 µs more in this corpus,
including the new capacity-reclamation pass. Unchanged compaction still scans and
packs the index, but avoids rebuilding and uploading the entire layout. A future
single-term scorer was not added: actual-match reservation improves the existing
shared path without duplicating global-DF or historical-posting handling.

To compare revisions, copy the current benchmark file into the baseline checkout,
compile and retain its executable, then run the baseline with `--bench --save-baseline tfs_review_paired` and the final executable with `--bench --baseline tfs_review_paired`.
The allocation-budget command above is independent of Criterion.

### Validation

- `cargo check --workspace --all-features`
- `cargo test --workspace --all-features`: 1,744 passed; one existing fixture
  generator intentionally ignored. Crash-recovery and persisted-format tests pass.
- TFS default, no-default and all-feature tests; `tfs_demo` example tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo fmt --all -- --check`, `make check-agents-doc`, and `git diff --check`.
