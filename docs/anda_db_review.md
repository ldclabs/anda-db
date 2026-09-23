# rs/anda_db Review Checklist and Remediation Report

[中文版](anda_db_review.zh.md)

## Follow-up completed — 2026-09-23

The follow-up review addressed five correctness/API cases, four performance
items and two internal simplifications. Persistent object layouts and public
method signatures are unchanged. Collection schema upgrades now enforce the
existing requirement to remove indexes before retiring their top-level fields.

| Item | Implemented behavior / evidence |
| --- | --- |
| Internal object capacity | Index objects and collection ID bitmaps use a symmetric `max(256 MiB, max_small_object_size)` budget. A default-config BM25 index with 100,000 common texts flushes/reopens; compressed and uncompressed small-limit collection cases cover B-Tree and BM25. |
| Indexed field retirement | Reject before storing the new schema; cover B-Tree, compound B-Tree, multi-field BM25 and HNSW. Removing indexes under the old schema allows the upgrade. |
| Oversized updates | Encode and check document bytes before intents/index changes, then reuse the payload for the conditional PUT. Rejected updates leave the handle active. |
| Optional embeddings | `Option<Vector>` can be indexed, backfilled, populated later and cleared; it survives reopen. |
| Raw document construction | `Collection::new_document()` initializes the `_id` placeholder. Missing business fields still fail validation. |
| Unchanged updates | Compare normalized fields and derived BM25/HNSW values; skip unchanged indexes and all persistence for a document no-op. |
| Dense cursor pages | One indexed equality plus ID predicates uses one posting scan with bounded page memory, preserving ascending result order and both page ends after posting reordering. |
| ID persistence | Track membership dirtiness separately from collection metadata; ordinary updates/extensions do not rewrite `ids.cbor`. |
| Recovery I/O | Prefetch current bodies for distinct intent IDs within the configured concurrency, applying repairs in ID order. A gated test verifies both parallel reads and the concurrency ceiling. |
| ID state | Encapsulate the ordered set, bitmap and dirty flag behind one lock; mutation methods update all three together. |
| Initialization | Creation and reopen share runtime-state construction from their durable inputs. |

Evidence: [collection regressions](../rs/anda_db/tests/core_review.rs), the
storage budget regression in [storage.rs](../rs/anda_db/src/storage.rs), and
[three paired benchmark trials](benchmarks/anda_db_core_2026-09-23/README.md).
The new cases supplement the existing crash-recovery, cancellation and format
fixtures; the old fixtures were not regenerated.

Validation: workspace all-feature check and tests passed (**1,765 passed,
0 failed, 1 existing fixture generator ignored**); workspace all-target,
all-feature Clippy passed with `-D warnings`. Core coverage includes 12 new
regression tests. Formatting, agent-document consistency and local documentation
links were checked. The bundled `db_demo` ran twice against a temporary real
filesystem with MetaStore, covering first creation and reopening with Jieba.

Three-trial median dense-page time fell from 9.939 ms to 3.658 ms, with peak
additional heap from 2,762,184 to 1,176 bytes. Thirty unchanged update/flush
operations went from about 272 PUTs to zero. Distinct-document recovery under
the injected latency model fell from 1,147.834 to 383.431 ms. Ordinary changed
updates save one bitmap PUT per checkpoint, but their p95 did not improve.
Whole postings still have finite capacity and rewrite cost; general composite
filters retain their existing memory behavior.

The September 6 report below is retained as historical validation and design
context, with its original test counts and benchmark results.

**Completion Status (2026-09-06)**

All checklist items — **15 bug fixes, 8 performance enhancements, and 4 architectural simplifications — are resolved**. Executed without subagents.
Existing user modifications were preserved, and parallel HNSW node CAS updates in the workspace remain intact.

| Check | Result |
| --- | --- |
| Workspace `cargo test --workspace --all-features` | **1,621 passed, 0 failed, 1 ignored** (manual fixture generation) |
| Formal review regressions | **25 tests passed**, covering interruptions across update/delete commits, unique key concurrency, boolean pagination reference sets, and schema mappings |
| Strict Clippy across Core, Schema, TFS (`all-targets` / `all-features` / `-D warnings`) | Passed |
| Independent Rust consumer with object_store 0.14 on real filesystem | Create, write, close, reopen, and Jieba search all passed |
| v0_8 / v0_11 storage fixtures | Remain readable without regeneration |
| AGENTS.md / CLAUDE.md | Synchronized and passed consistency checks |

The implementation secures locks by native unique key with stable sorting, releasing them only after final commit; earlier designs serializing all collection writes were rejected.
Metadata snapshots distinguish committed from pending index registrations; transient recovery read errors fail fast; maintenance deletions record per-ID WAL entries.
First operations in open callbacks execute after recovery completes, synchronizing runtime tokenizers to loaded BM25 instances. In-memory intent logs retain only sequence numbers.

New runtime interfaces: `SearchOptions`, `search_with_options`, `search_ids_with_options`,
`set_io_concurrency(1..=64)`, `recovery_issues()`, and streaming `*_with_limit`.
`StorageConfig::with_cache_max_bytes` introduces explicit byte budgets while preserving entry-count semantics.
The default release profile remains `opt-level=z`, with an optional `release-speed` (`opt-level=3`) profile.

`collection.rs` production logic has been decomposed into `src/collection/` while keeping public paths unchanged; embedded test modules moved to dedicated test files.
CRUD operations employ typed undo logs; B-Tree and BM25 share conditional metadata commit and tombstone reclamation routines.

Independent integration validation corrected the S04 local filesystem example: upstream LocalFileSystem 0.14 lacks conditional writes,
so examples wrap it in MetaStore; READMEs, technical docs, CLAUDE/AGENTS, and local skills are aligned.

Benchmark data, raw results, and reproduction commands are documented in the [Benchmark Report](benchmarks/anda_db_core_2026-09-06/README.md).
Bounded OR and primary-key boolean operations, selectivity reordering, candidate pushdowns, and bounded recovery I/O are in place.
No claims are made of universal speedup: single-term BM25 median latency did not change significantly; local filesystem log sweeps remain dominated by filesystem sync costs.
Heavy codec tasks incur thread-switching costs, as detailed in the benchmark serial vs concurrent comparisons.

Usage notes: Default selective prefiltering executes RRF over candidate subsets; to preserve historical "global ranking then filter" semantics,
specify `SearchOptions { prefilter_limit: 0, adaptive: false, ..Default::default() }`.
Broad non-PK range intersections may still allocate O(matches) working memory; result limits are not hard working memory caps across all query plans.
These represent deliberate engineering tradeoffs.

---

**Archived Review Notes (Pre-Fix)**

Review date: 2026-09-06. Evaluated commit `8bc6d8bf743f6f622b8be4ea1715ab7ed01a7235` alongside active workspace changes; `anda_db` was version 0.11.1. Executed without subagents, preserving uncommitted modifications.

Pre-fix summary: 15 bugs or API boundary errors were identified and verified with executable repros (6 designated P1, 9 designated P2), plus 1 BM25 compaction optimization. Top priorities: unique constraints, index publication sequencing, transient recovery error propagation, and prefix isolation.

Priority designations: P1 indicates required resolution prior to production release; P2 indicates deterministic triggers requiring scheduled remediation. Priority does not reflect security vulnerability severity.

**Pre-Fix Scope and Validation**

Audited all 11 Rust files under `src/` (9,910 total lines including comments/docs), checking contracts against unit tests, 5 integration suites, format fixtures, examples, Cargo configurations, and technical docs. Traced schema, B-Tree, BM25, HNSW, and object store dependencies to verify invocation semantics.

| Check | Result |
| --- | --- |
| `cargo test -p anda_db --all-features -- --test-threads=4` | 186 passed, 1 ignored (format fixture generator) |
| `cargo clippy -p anda_db --all-features --all-targets -- -D warnings` | Passed |
| Special repro harness | 16/16 confirmed: 15 bugs, 1 compaction optimization |
| Cloud stores, power loss, end-to-end throughput | Excluded from initial review scope |

The legacy [reproduction probes](anda_db_review_repros.rs) are archived for historical reference. Active verification runs via the permanent [regression suite](../rs/anda_db/tests/review_regressions.rs):

```bash
bash docs/run_anda_db_review_repros.sh --nocapture
```

Passing status confirms post-fix correctness. Detailed issue descriptions and remediation notes follow; checked items indicate completed work.

## Priority 1 Remediations (P1)

- [x] **B01: Retain unique key locks until document commit completes.**

  Location: [crud.rs](../rs/anda_db/src/collection/crud.rs), [btree.rs](../rs/anda_db/src/index/btree.rs). Previously, updates modified indexes before writing documents asynchronously; document-ID striped locks failed to prevent concurrent documents from claiming the prior unique key.

  Reproduction: Document A holds unique key `x`; while updating to `y` before the document PUT executes, Document B successfully inserts `x`. If A's update is interrupted and reopened, both A and B persist `x`, but the unique index returns only one. Poisoned handles and replay logs could not resolve this cross-document conflict.

  Remediation: Implemented unique key reservations spanning old and new keys, held until document write results are acknowledged. Locks are acquired in stable sort order to prevent multi-index deadlocks. Operations re-verify handle validity upon acquiring locks. Replay errors report conflicts explicitly rather than logging warnings while advancing recovery markers.

  Validation: Interleaved updates/removals with concurrent insertions/updates; injected faults before/after storage writes and during dropped responses. Reopened documents must satisfy uniqueness or fail fast with actionable conflict diagnostics. Probe: `repro_unique_key_released_before_document_commit`.

- [x] **B02: Metadata paths must enforce "persist index first, publish references second".**

  Location: [persistence.rs](../rs/anda_db/src/collection/persistence.rs), [extensions.rs](../rs/anda_db/src/collection/extensions.rs), [index_ops.rs](../rs/anda_db/src/collection/index_ops.rs). While `flush_inner` ordered writes correctly, `store_metadata_unclaimed` wrote full metadata containing uncommitted index registrations.

  Reproduction: With existing documents and advanced checkpoints, an open callback creates a B-Tree index, invokes `save_extension`, and returns an error. On subsequent open, the index was registered, `_nx` skipped backfill, but the on-disk index was empty; documents were retrievable via `get` but absent from index queries. Deleting another index followed the same flawed publication path.

  Remediation: Distinguish committed index sets from pending registrations; extension writes serialize only committed index descriptors or flush pending index changes prior to publishing references.

  Validation: B-Tree, BM25, and HNSW tested across "backfill -> extension write/delete another index -> callback error/interruption -> reopen"; all documents remain indexed. Probe: `repro_extension_publishes_unflushed_new_index`.

- [x] **B03: Recovery scans must not advance checkpoints past transient read failures.**

  Location: [recovery.rs](../rs/anda_db/src/collection/recovery.rs). `auto_repair_indexes` caught non-NotFound errors, logged warnings, and continued; subsequent flushes advanced checkpoints past the unread documents.

  Reproduction: Two documents inserted without checkpoints; injecting a single transient GET failure on `data/1.cbor` during recovery resulted in successful open with checkpoint=2; document 1 was lost until an explicit `reconcile_storage` scan was performed.

  Remediation: Distinguish definitive non-existence from data corruption and transient I/O errors. Transient errors abort recovery or persist pending retry lists, preventing checkpoint advancement. Corrupted records record diagnostic metadata for manual repair.

  Validation: Injected transient failures across the recovery window; clearing the fault and retrying recovers all documents automatically without manual storage reconciliation. Probe: `repro_transient_recovery_read_failure_is_checkpointed_past`.

- [x] **B04: Apply user-configured tokenizers to loaded BM25 indexes on reopen.**

  Location: [lifecycle.rs](../rs/anda_db/src/collection/lifecycle.rs), [index_ops.rs](../rs/anda_db/src/collection/index_ops.rs), [collection.rs](../rs/anda_db/src/collection.rs). Indexes bootstrapped with default tokenizers; callback `set_tokenizer` calls updated the Collection field but neglected internal BM25 state.

  Reproduction: Initialized Jieba tokenizer, inserted Chinese text, verified search. Upon restart and re-invoking `set_tokenizer`, queries returned empty results despite `collection.tokenize` emitting valid tokens.

  Remediation: Supply tokenizers prior to bootstrap in open configuration, or update internal BM25 tokenizers via thread-safe runtime setters.

  Validation: Consistent multi-term and CJK queries across restarts, inserts, updates, and deletes. Probe: `repro_custom_tokenizer_not_restored_to_loaded_bm25`.

- [x] **B05: Collection storage paths must follow database relocation paths.**

  Location: [database.rs](../rs/anda_db/src/database.rs), [lifecycle.rs](../rs/anda_db/src/collection/lifecycle.rs). Storage adopted caller-specified paths, but database names were loaded from legacy `db_meta.cbor`, causing collections to format paths against the old name.

  Reproduction: Copying `reviewdb/` to `restored/` and opening `restored` wrote new documents to `reviewdb/docs/data/2.cbor`. If the old path was inaccessible, collections failed to open; if accessible, writes polluted the original database.

  Remediation: Derive storage namespaces strictly from caller paths, decoupling display names from storage prefixes; reconcile database metadata to prevent path redirects.

  Validation: Relocated open scenarios (both with original prefix present and absent) verify that all operations target the new prefix exclusively. Probe: `repro_relocated_database_uses_original_collection_prefix`.

- [x] **B06: `create_btree_index_nx` must only ignore existing matching indexes.**

  Location: [index_ops.rs](../rs/anda_db/src/collection/index_ops.rs). Swallowed all `DBError::AlreadyExists` errors, including unique constraint violations during backfill.

  Reproduction: Pre-inserted duplicate keys, then called `_nx` on a unique index; call succeeded without registering the index, allowing further duplicate keys to be inserted.

  Remediation: Validate index existence upfront, or verify that `AlreadyExists` errors correspond to matching, fully loaded indexes; propagate backfill collision errors with conflicting keys and IDs.

  Validation: Duplicate creation of identical indexes succeeds idempotently; backfills encountering duplicate keys fail explicitly without leaving partial registrations. Probe: `repro_create_btree_index_nx_swallows_duplicate_backfill_failure`.

## Priority 2 Remediations (P2)

- [x] **B07: Decouple document size limits from mutation intent limits.**

  Location: [recovery.rs](../rs/anda_db/src/collection/recovery.rs), [storage.rs](../rs/anda_db/src/storage.rs). Mutation logs retain before and after images of documents, yet enforced `max_small_object_size` (2,048,000 bytes default). A 1.1 MB body could be created, but subsequent updates failed due to log size checks.

  Remediation: Configured independent limits for internal intent records, accounting for CBOR overhead and backward compatibility. Probe: `repro_accepted_large_document_cannot_be_updated`.

- [x] **B08: Recover ID allocators before invoking user open callbacks.**

  Location: [lifecycle.rs](../rs/anda_db/src/collection/lifecycle.rs). Callbacks executed prior to replay/repair; ID allocators started from legacy metadata maximums rather than uncheckpointed storage watermarks.

  Reproduction: Adding document 1 without a checkpoint caused an open callback adding another document to reuse ID 1, failing with `AlreadyExists`.

  Remediation: Advanced allocator watermarks to match persisted storage before invoking callbacks; ensured index hooks activate prior to replay. Probe: `repro_open_callback_add_runs_before_allocator_recovery`.

- [x] **B09: Retain recoverable per-ID records for maintenance deletions.**

  Location: [crud.rs](../rs/anda_db/src/collection/crud.rs). Deletion logs were written only if a Document could be constructed; deletions of dead IDs or unparseable schemas bypassed WAL logging.

  Reproduction: Deleting an unparseable object cleared memory; upon restart, legacy bitmaps and unique keys were resurrected, rejecting replacement documents.

  Remediation: Introduced purge-by-ID intent records independent of pre-images; replay purges index and bitmap entries before checkpointing. Probe: `repro_dead_id_removal_has_no_replay_record`.

- [x] **B10: Invalidate handles on indeterminate extension commits.**

  Location: [persistence.rs](../rs/anda_db/src/collection/persistence.rs), [extensions.rs](../rs/anda_db/src/collection/extensions.rs). Guard wrappers unpoisoned handles on `Err` returns, failing to handle dropped responses or uncertain commits.

  Reproduction: Metadata PUT succeeded on backend but dropped response; handle remained active; subsequent writes failed permanently due to stale CAS tokens.

  Remediation: Categorized errors at metadata boundaries; indeterminate outcomes poison handles, while local pre-validation errors retain healthy state. Probe: `repro_unknown_extension_commit_does_not_poison`.

- [x] **B11: HNSW bootstrap orphan cleanup must honor read-only mode.**

  Location: [hnsw.rs](../rs/anda_db/src/index/hnsw.rs). Bootstrap unconditionally invoked `purge_orphan_node_blobs` during read-only opens.

  Remediation: Shifted cleanup to writable maintenance phases or gated behind read-only flags. Probe: `repro_read_only_open_deletes_orphan_hnsw_blobs`.

- [x] **B12: Ensure readable roundtrips for streamed writes.**

  Location: [storage.rs](../rs/anda_db/src/storage.rs). Decompression bounded reads by `max(compressed_len * 16, small_object_limit * 16)`, while streaming writes lacked corresponding constraints. Highly compressible data could write successfully but fail during read.

  Remediation: Enforced independent uncompressed limits verified during writing. Probe: `repro_stream_writer_reader_rejects_compressible_roundtrip`.

- [x] **B13: Do not rely on 4-byte minimums for zstd magic sniffing.**

  Location: [storage.rs](../rs/anda_db/src/storage.rs). Bounded chunk sizes (1–3 bytes) caused initial buffer fills to fail magic sniffing, returning raw compressed bytes.

  Remediation: Sniffed magic without consuming stream prefixes or enforced minimum chunk sizes. Probe: `repro_stream_reader_small_chunk_does_not_sniff_zstd`.

- [x] **B14: Inspect all elements when traversing JSON arrays for indexing.**

  Location: [index/mod.rs](../rs/anda_db/src/index/mod.rs). Arrays were traversed only if the initial element was a string or object; mixed arrays like `[0, "text"]` were missed.

  Remediation: Traversed all elements using standard stack budgets, skipping non-text entries cleanly. Probe: `repro_json_text_extraction_depends_on_first_array_element`.

- [x] **B15: Validate Document field indices against Collection schema.**

  Location: [crud.rs](../rs/anda_db/src/collection/crud.rs). Collections checked values by internal field indices while hooks inspected names from Document-attached schemas; mismatched registration orders corrupted index values.

  Remediation: Reject mismatched field mappings or convert explicitly by name before validation. Probe: `repro_foreign_document_schema_yields_wrong_index_values`.

## Performance and Simplification Items

| Done | ID | Item | Focus | Verification |
| --- | --- | --- | --- | --- |
| [x] | P01 | Bounded boolean evaluation and candidate pushdown | [query.rs](../rs/anda_db/src/collection/query.rs): Or/And subtrees expanded with limit=0, causing O(N) intermediate allocations. | Validated against reference sets and bidirectional pagination; verified peak memory reductions. |
| [x] | P02 | Bounded concurrency for recovery reads and GC | [recovery.rs](../rs/anda_db/src/collection/recovery.rs): Replaced serialized sequential awaits with configurable concurrency limits. | Tested under 1/10/50 ms latency with 64/1,000 pending items; ensures failures abort cleanly (B03). |
| [x] | P03 | Compact in-memory intent tracking | [recovery.rs](../rs/anda_db/src/collection/recovery.rs): Replaced cloned full documents in memory with sequence identifiers. | Measured 1,000 repeated updates on large documents; confirmed RSS drops and replay correctness. |
| [x] | P04 | BM25 compaction state tracking | [bm25.rs](../rs/anda_db/src/index/bm25.rs): Prevented repeated re-sorting when bucket count is unchanged. | Verified via `repro_bm25_compaction_same_bucket_count_does_not_flush`; second call is a no-op. |
| [x] | P05 | Fast path for single-query retrieval | [query.rs](../rs/anda_db/src/collection/query.rs): Bypassed RRF deduplication maps when querying single indexes. | Verified result ordering and allocation counts across single BM25, single HNSW, and hybrid searches. |
| [x] | P06 | Offload heavy CPU tasks from async runtimes | [crud.rs](../rs/anda_db/src/collection/crud.rs), [storage.rs](../rs/anda_db/src/storage.rs): Bounded blocking threadpools for heavy serialization/compression. | Measured scheduler latency under concurrent reads and writes; benchmarked `opt-level=z` vs `opt-level=3`. |
| [x] | P07 | Byte-based cache budgets and striped eviction | [storage.rs](../rs/anda_db/src/storage.rs): Added `cache_max_bytes` alongside object-count limits. | Benchmarked under mixed workloads; verified eviction patterns and backend GET counts. |
| [x] | P08 | Configurable candidate selection for selective queries | [query.rs](../rs/anda_db/src/collection/query.rs): Added adaptive candidate expansion and pre-filtering options. | Tested under 0.1%, 1%, and 10% selectivity; verified recall improvements. |

- [x] **S01: Decompose Collection into focused modules.**
  Split 9,850 lines of `collection.rs` into lifecycle, recovery, crud, query, index_ops, and extensions while maintaining public APIs and locking invariants.

- [x] **S02: Unified undo logs for CRUD operations.**
  Replaced scattered HashMaps with `Vec<UndoEntry>` for deterministic rollback sequencing and standardized handle poisoning.

- [x] **S03: Explicit metadata state machine.**
  Formalized committed snapshots, pending index changes, and checkpoint states; unified B-Tree and BM25 manifest handling.

- [x] **S04: Documentation and dependency alignment.**
  Corrected documentation on poisoned handle read availability; upgraded README references to `object_store=0.14`; clarified storage reconciliation heuristics.

## Recommended Staging and Gates

| Batch | Items | Acceptance Criteria |
| --- | --- | --- |
| 1: Invariants and Publication | B01, B02, B03, B05, B06 | Unique key interleaving, prefix isolation, and metadata publication fault matrices pass without silent data loss. |
| 2: Reopen and Recovery | B04, B08, B09, B10, B11 | Tokenizer state, intent replay, and read-only behavior match specifications across restarts. |
| 3: Input and Size Boundaries | B07, B12, B13, B14, B15 | Large document CRUD, stream roundtrips, foreign schemas, and nested JSON tests pass. |
| 4: Performance Enhancements | P01, P02, P03, P04, P05 | Verified latencies, RSS, and storage request counts under identical environments. |
| 5: Architectural Refactoring | P06, P07, P08, S01–S04 | Incremental pull requests preserving on-disk compatibility and test stability. |
