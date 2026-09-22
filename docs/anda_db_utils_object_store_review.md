# anda_db_utils / anda_object_store Review Checklist and Implementation Report

[中文版](anda_db_utils_object_store_review.zh.md)

Updated: 2026-09-06. Implements R1–R9, P1–P7, M1–M2, and D1 based on the comprehensive codebase review, supplemented by post-review fixes from `e21d9d2` covering multipart commit, abort handling, overwrite-only storage backends, generation placeholders, and residual buffer deallocation. Work was executed without subagents; unrelated workspace changes were preserved. Initial reproduction scripts have been converted into deterministic regression tests.

## Verification Results

| Check | Result |
| --- | --- |
| Unit, regression, and doc tests across both crates | 47 utils unit, 79 store unit, 32 new integration regressions, 4 doctests passed |
| Strict Clippy across all targets and features | Passed with `-D warnings` |
| cargo check --workspace --all-features --locked | Passed |
| cargo test --workspace --all-features --locked | All passed, 0 failed (1 fixture generation test ignored by design) |
| Strict Rustdoc across both crates | Passed with `RUSTDOCFLAGS="-D warnings"` |
| Formatting and patch whitespace checks | Passed |

Ignored tests correspond to on-disk format fixture generators requiring manual review. No live cloud integration, physical power-loss injection, or TypeScript changes were part of this scope.

## Local Performance Results and Tradeoffs

[Full benchmark report, raw CSVs, resource measurements, and source hashes](benchmarks/anda_libraries_20260906/README.md). Old and new implementations were evaluated in clean, separate build targets against identical workloads. All results, including regressions, are published in full.

In local InMemory microbenchmarks, 16 MiB encrypted put median latency dropped from ~24.9 ms to 5.25 ms; under metadata stress with 4 MiB objects and 1 KiB chunks, hot `head` latency dropped from ~25.3 µs to 1.13 µs. Interleaved range reads achieved ~8.5x speedups. These figures reflect in-memory behavior and do not directly translate to end-to-end network or disk throughput.

Cold metadata reads, partial `From<Vec>` conversions, fully unique `String` construction, and listing operations exhibit increased checking or allocation overhead. Bounded memory consumption and strict validation represent conscious design tradeoffs.

## Checklist Completion Status

- [x] **R1: Unified metadata validation and deletion boundaries.**
  [Sidecar validation](../rs/anda_object_store/src/sidecar.rs). Reads, overwrite sweeps, deletions, and GC uniformly enforce generation structure and metadata authenticity. Invalid documents are conservatively treated as unknown references; explicit repairs do not follow forged paths. Regressions confirm prevention of false deletions in strict mode and prevent parent key pointers from erasing child keys.

- [x] **R2: Cache invalidation on indeterminate commits.**
  [Commit invalidation guard](../rs/anda_object_store/src/sidecar.rs). Once metadata put/delete operations initiate, any error, panic, or future cancellation triggers a synchronous `Drop` guard invalidating shared caches until publication successfully finishes. Conservatively purges shared caches on unexpected errors to prevent stale token persistence. Covers both store wrappers, put/delete operations, errors, cancellations, conditional reads, and listings.

- [x] **R3: Robust multipart lifecycle.**
  [Shared upload state](../rs/anda_object_store/src/upload.rs). Tracks active part futures, preventing commits if any part fails or drops; cancellation during tail or data completion phases voids the upload. Retries metadata-only operations without re-executing internal complete calls once payload writes finish; records commit identities to ensure stale retries cannot overwrite newer commits. Aborts mark terminal state only upon successful backend cleanup; payload materializations delete under key locks only after confirming absence of commit. Completes/aborts release in-flight registrations; duplicate completion calls do not re-publish. Regressions pass on InMemory and LocalFileSystem.

- [x] **R4: Pinned chunk parameters during legacy copy operations.**
  [Encrypted copy](../rs/anda_object_store/src/encryption.rs). Copy and rename operations backfill actual `chunk_size` into legacy document sidecars, preserving original chunk-AAD patterns prior to re-authentication. Legacy objects lacking chunk parameters remain readable after migrating default chunk sizes under strict authentication.

- [x] **R5: Decoupled encryption chunks from transport parts.**
  [Ciphertext transport framing](../rs/anda_object_store/src/upload.rs). Emits fixed 8 MiB transport parts by default (short tail allowed); introduces `with_multipart_part_size` (minimum 5 MiB). Verifies unaligned inputs and concurrent part arrivals on backends enforcing minimum and equal non-final part sizes, eliminating Cloudflare R2 incompatibilities. Residual data copying releases large backing allocations on short tail parts while respecting backend part limits.

- [x] **R6: Comprehensive fault injection coverage.**
  [Fault harness](../rs/anda_object_store/src/fault.rs). Added `MultipartStart`, `Part`, `Complete`, and `Abort` fault hooks, alongside `ErrorAfter`, `PauseBefore`, and `PauseAfter` gates. Budgets span all mutation phases; torn writes fail explicitly on non-standard put operations. Event logs distinguish backend success, response errors, and cancellations. Tested across errors, crashes, dropped responses, and cancellations throughout multipart stages.

- [x] **R7: Exception-safe UniqueVec cleanup.**
  [Unified removal path](../rs/anda_db_utils/src/lib.rs). Employs `set.take` to complete vector adjustments before dropping removed elements. Unifies `remove`, `remove_if`, and `swap_remove_if` through a single entry point with retain guards. Regressions verify set consistency and deduplication under panics within `Hash`, `Eq`, `Clone`, and `Drop`.

- [x] **R8: Removal of redundant Clone bounds.**
  [MetaStore Clone](../rs/anda_object_store/src/lib.rs), [EncryptedStore Clone](../rs/anda_object_store/src/encryption.rs). Manual `Clone` implementations share only `Arc` references and configuration state; backends lacking `Clone` are fully supported. Verified on `FaultStore<InMemory>`, with existing concurrent CAS tests passing clean.

- [x] **R9: Hardened generation IDs and collision resolution.**
  [Generation and ETag generation](../rs/anda_object_store/src/generation.rs). Combines 128-bit random salts with in-process 64-bit monotonic counters, preventing ID reuse under clock skews or PRNG collisions. Discriminates between new and valid legacy 8-bit salt formats. Payloads prefer `Create` with collision retries; copies similarly enforce conditional creation. Backends lacking conditional writes fall back to strong IDs paired with `Overwrite` under the single-writer assumption. Multipart operations avoid zero-byte disk placeholders, eliminating phantom versions on aborted uploads. Covered by regressions spanning clock stops, duplicate salts, pre-existing keys, and overwrite-only storage.

- [x] **P1: Authenticated metadata caching.**
  [Auth context](../rs/anda_object_store/src/encryption.rs). Private tokens bind to verification contexts and paths, preventing derivation from serialized payloads; cloning or re-signing clears tokens. Hot caches bypass full-tag GMAC sweeps. Tests verify that repeated `head`, `get`, and `list` operations avoid redundant authentication, while preventing token inheritance across mismatched keys, paths, or policies.

- [x] **P2: Bounded cache and memory limits.**
  [Resource limits](../rs/anda_object_store/src/limits.rs). Configures a 64 MiB default cache budget alongside item count ceilings; adds builder byte budgets and metadata limits. Validates serialized size, object length, chunk counts, and tag counts against actual received bytes during streaming reads. Custom caches retain custom eviction policies while adhering to authentication and layout boundaries. Budgets represent estimates, not hard process RSS caps.

- [x] **P3: Coalesced and concurrent range reads.**
  [Range planning and decryption](../rs/anda_object_store/src/encryption/ranges.rs). Deduplicates and coalesces overlapping ranges without filling empty gaps; dispatches up to 8 concurrent requests bounded at 8 MiB or one chunk. Reconstructs outputs in requested order without retaining large buffers for small slices. Passes randomized range oracles, cross-boundary slice tests, and request-count regressions.

- [x] **P4: Optimized UniqueVec construction and deserialization.**
  [Builder patterns](../rs/anda_db_utils/src/lib.rs). `FromIterator` and Serde construct via direct deduplication; `From<Vec>` reuses existing buffers and truncates sparse results. Construction employs bounded reservation sampling and lookup thresholds, preserving panic rollbacks on public push APIs. One million identical integers resolve to constant capacity; passes borrowed `&str` deserialization tests. Tradeoffs for unique strings and `From<Vec>` are documented in benchmarks.

- [x] **P5: Elimination of payload ETag hashing.**
  [Commit tokens](../rs/anda_object_store/src/generation.rs). Derives new ETags strictly from domain separators and generation identifiers, removing payload hashers from both uploaders. Legacy tokens continue to parse and compare as opaque strings; copies and renames generate independent tokens. Verified via ABA concurrency tests.

- [x] **P6: Reduced decryption allocations and memory retention.**
  [Streaming decryption](../rs/anda_object_store/src/encryption.rs). Moves chunk AAD to a 52-byte stack array; reuses single-chunk helpers. Streaming decryptors batch small chunks up to 64 KiB while bounding large chunks to single-chunk minimums, eliminating per-chunk allocations and full-object copies. Verifies that reading initial segments avoids buffering complete objects, and enforces verification prior to emitting plaintext.

- [x] **P7: I/O and memory optimizations for GC.**
  [Configurable GC](../rs/anda_object_store/src/sidecar.rs). Concurrent mark phases and grouped lookups by key minimize repeated metadata queries across historical generations. Exposes prefix filters, concurrency parameters, mark budgets, and candidate limits; aborts safely before executing deletions if budgets are exceeded. Re-verifies keys under write locks prior to deletion to preserve in-flight safety. Metadata GET requests for 10 historical generations under a single key dropped from 11 to 2.

- [x] **M1: Code deduplication and module reorganization.**
  [Store tests](../rs/anda_object_store/src/tests.rs), [Utils tests](../rs/anda_db_utils/src/tests.rs). Decoupled inline tests across five source files into dedicated test modules; extracted helpers for generation, limits, upload, and range operations. Refactored `intersect_with` via `retain`; consolidated deletion routines; replaced custom list sorting with ordered stream buffers. Preserves public types and serialization contracts while adding configuration builders.

- [x] **M2: Updated documentation and interface contracts.**
  [Technical reference](anda_object_store.md). Updated documentation to 0.11; clarified ETags, padding, PRNG collision boundaries, legacy behaviors, shared coordination constraints, limits, and upload state transitions. Explicitly rejects version-based addressing; metadata sub-requests propagate supported Extensions across get, put, complete, and delimited list endpoints. Clean Rustdoc validation.

- [x] **D1: Local fsync configuration and documented boundaries.**
  [DB server](../rs/anda_db_server/src/main.rs), [Nexus server](../rs/anda_cognitive_nexus_server/src/main.rs). Enabled `with_fsync(true)` across servers and storage examples, updating local skill templates. Benchmarks report fsync latency impacts. Documented upstream `object_store` 0.14.1 deletion behaviors lacking fsync; tests do not claim power-loss durability for unsupported operations.

## Compatibility and Deployment Considerations

Compatibility pathways are maintained for legacy CBOR fields, legacy ETags, valid historical generations, and unauthenticated legacy encrypted blobs. New writes do not support historical version reads; multi-writer concurrency across distinct instances requires external coordination. Resource limits for metadata, GC, and caches can be tuned via builders; strict authentication migration remains opt-in.

Successful completions, aborts, and failed payload materializations safely release upload registrations. Failed parts should be aborted or dropped to reclaim backend resources; GC does not enumerate or abort incomplete multipart uploads managed internally by cloud providers.

Local fsync is enabled for writes, but upstream deletion and directory metadata limitations remain present. Deployments requiring host-level power-loss guarantees should select backends providing appropriate deletion durability and verify independently.
