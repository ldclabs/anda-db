# Changelog

All notable changes to this workspace are documented in this file.

## [0.10.0] — 2026-07-16

De-complexity release. 0.9.2 was never published; its review-driven hardening
(see "0.9.2 (unpublished)" below) ships here together with a follow-up review
that fixed defects the hardening itself introduced, and a structural
simplification pass that removes the machinery built for extreme edge cases.
The release rests on three explicit contracts:

1. **Single writer per database** is a deployment contract; each metadata
   object keeps one conditional PUT as the last defense against a second
   writer, and a `Precondition` conflict is never reconciled in place.
2. **Cancellation is a crash (poison-on-cancel).** Dropping a mutating future
   mid-operation — or a storage write failing with an unknown outcome —
   poisons the collection handle; every further operation on it errors.
3. **Recovery happens only on reopen**: write-ahead intent replay plus a
   repair scan bounded exactly by the new allocation watermark.

### Migration note: poison-on-cancel

Do not wrap AndaDB mutating calls (`add*`, `update`, `remove`, `flush`,
`close`, extension writes, compactions) in `tokio::select!`/`timeout`.
A cancelled mutation poisons the collection handle; reopen it via
`AndaDB::open_collection`, which discards the poisoned handle and recovers
from storage. The built-in drivers (`auto_flush`, both HTTP servers) never
cancel mutating futures.

### Migration note: on-disk formats roll forward only

Four durable formats gained fields or layouts this release. All of them read
existing data and upgrade it lazily; **binaries older than 0.10 cannot read
data written by 0.10** (do not roll back after writing):

- object store immutable-generation layout (see the dedicated note below),
- BM25/B-Tree bucket manifests (see Changed),
- the collection allocation watermark object (`alloc_watermark.cbor`),
- the schema allocation watermark (`next_idx`, serialized with each schema).

### Migration note: HNSW deletion repair is now opt-in

`HnswConfig::reconnect_on_delete` now defaults to **`false`** for new
configurations *and* for persisted metadata that lacks the field (matching
the behavior those indexes were built with; the unpublished 0.9.2 defaulted
it to `true`). Neighbor repair keeps recall stable under delete-heavy
workloads but runs O(M²·L) distance computations while holding the
structural write lock; enable it explicitly when recall stability matters
more than deletion throughput.

### Migration note: object store immutable-generation layout

`anda_object_store` replaced the mutable dual-object sidecar protocol with an
immutable-generation layout. A logical put now writes the payload to a fresh,
never-overwritten object `gen/<path>/<generation>` and commits by atomically
switching the pointer inside the metadata document `meta/<path>` (a single
backend put). Consequences:

- **Torn states are gone by construction.** A crash before the pointer switch
  leaves the previous version fully readable (previously the old version was
  already overwritten and unrecoverable, and `EncryptedStore` surfaced the
  window as an AES-GCM authentication failure); a crash after it means the
  write took effect.
- **Format rolls forward only.** Pre-0.10 data (payload at `data/<path>`, no
  generation pointer) stays fully readable, and the first overwrite of a key
  migrates it to the new layout. Metadata written by this version carries the
  generation pointer, which older releases do not understand — **do not roll
  back to a pre-0.10 binary after writing with this version.**
- **Garbage collection.** Replaced payloads are deleted best-effort right
  after each commit; crash leftovers are reclaimed by the new explicit
  mark-sweep `MetaStore::collect_garbage` / `EncryptedStore::collect_garbage`
  (run it at open or on a maintenance schedule). The collector reads every
  commit point before deleting anything and re-checks each key immediately
  before each deletion, so referenced payloads are never deleted.
- **Versions are no longer reported.** `PutResult::version` and
  `ObjectMeta::version` are `None` (replaced generations are reclaimed
  eagerly, so version-addressed reads cannot be honoured); conditional
  updates use the content-addressable ETag. Persisted
  `ObjectVersion { version: None }` tokens from `LocalFileSystem` deployments
  keep working unchanged.
- **Conditional semantics unified.** `PutMode::Create` is now arbitrated
  cross-process by a conditional write of the commit point (strictly stronger
  than the old orphan self-heal, which weakened `Create`);
  `PutMode::Update` / `if_match` / `if_none_match` are evaluated against the
  logical ETag on every backend. `EncryptedStore`'s `with_conditional_put()`
  is a retained no-op: those semantics are always on, and `EncryptedStore`
  now always exposes the logical (ciphertext-hash) ETag.
- **Listings enumerate commit points** (`meta/`), so uncommitted payloads and
  crash leftovers are invisible; entries report the logical size and ETag.
  A metadata document that no longer decodes is skipped with a warning in
  compatibility mode (strict `EncryptedStore` mode still fails the listing),
  reads of such a key keep failing loudly, and an overwrite rebuilds it.
- **Removed machinery**: the per-key mutation-lease table, the
  `heal_create` orphan self-heal branches, the conditional-GET rewrite of
  backend ETags, and the listing orphan-tolerance dance are all gone —
  the immutable-generation protocol makes them unnecessary.

### Changed — de-complexity pass

- **Poison-on-cancel recovery model** (`anda_db`; breaking behavior) — The retained-write/read-back state machines (kept payload snapshots, expected-version slots, `Precondition` read-back comparison — four copies across collection metadata, B-Tree, BM25 and HNSW adapters), the pending-checkpoint retry state and the in-flight-add checkpoint clamp are deleted. In their place: a cancelled mutating future or an unknown-outcome storage failure moves the handle to a poisoned lifecycle state, `AndaDB::open_collection` transparently discards a poisoned handle after draining it, and reopening converges from storage (intent replay + repair scan). Handles no longer attempt to "resume in place" after cancellation.
- **`add` writes no per-mutation WAL record** (`anda_db`) — A durable **allocation watermark** (`alloc_watermark.cbor`, published one small PUT per 64 allocations) guarantees every id that may have a document object lies inside `checkpoint+1 ..= max(metadata max, watermark)`. The reopen repair scan probes exactly that window — the consecutive-miss heuristics (and their "more than N consecutive holes can hide documents" failure mode) are gone. Updates and removes keep their durable intents.
- **Flush no longer replays mutation intents** (`anda_db`) — Under the poison contract a live handle only ever holds completed intents, so the normal flush path persists dirty state and retires the intent log without the delete-and-rebuild reconciliation the unpublished 0.9.2 follow-up ran on every flush (which re-inserted every pending document into every index, including re-randomizing HNSW placements). Reconciliation now runs only on reopen.
- **KQL solver rewritten on a row-based solution model** (`anda_cognitive_nexus`) — The parallel representations (columnar entity/predicate bindings, fixed 3+1-slot relation rows, grouped-pair maps, alignment marker sets), the four FIND fallback projection paths and the three FILTER sub-paths are replaced by a single `SolutionTable` (header + rows, columnar layout) with relational operators: natural join, left join (OPTIONAL), padded union (UNION), row-level FILTER, tuple anti-join (NOT), group-aggregate and §3.3 projection dedup. `kql.rs` shrinks from 3,326 to 1,482 lines. Fixed by construction, each with a regression test: concept-only UNION no longer degenerates into a cartesian product; UNION branches with multi-pattern equi-joins no longer lose solutions; NOT anti-joins with 3+ shared variables remove exact tuples only; FILTER after UNION filters both branches; UNION branches are no longer limited to 3 entity / 1 predicate variables.
- **Shard proxy routing caches are invalidate-only** (`anda_db_shard_proxy`) — Administrative writes and NOTIFY events no longer back-fill caches from request payloads (racing back-fills could re-apply an older commit); they only invalidate, and lookups re-resolve against PostgreSQL. The `route_generation` epoch machinery is deleted; the positive route TTL (now 30s) is the convergence bound for the one remaining stale-insert race, and the backend mirror is maintained solely by commit-ordered NOTIFY events.
- **Index-crate concurrency contracts narrowed** (`anda_db_tfs`, `anda_db_btree`, `anda_db_hnsw`) — The internal mutation gates, the hand-written persistence gates (three verbatim copies of an async mutex), BM25/B-Tree empty-posting flush healing and B-Tree's dirty-bucket counter with its drift-healing CAS are deleted. Coordinating mutations against flush/compaction — and flushes against each other — is the caller's responsibility; `anda_db`'s `Collection` already holds an exclusive operation gate across every flush. HNSW additionally drops its per-layer id tracker (entry-point replacement falls back to an O(N) scan on the rare paths that need it).
- **Schema tracks an allocation watermark** (`anda_db_schema`) — `Schema` persists `next_idx`, the exclusive upper bound of every field index the schema lineage ever allocated. This fixes a latent index-reuse bug (removing the highest-indexed field let the next upgrade reallocate its index, so stale values of the removed field could be misread as the new field) and lets `try_from_doc`/`set_doc` distinguish retired-field leftovers (still silently dropped) from indexes the lineage never allocated — which are now **rejected** instead of silently deleted on the next rewrite.
- **`Filter::Or` is order-independent** (`anda_db`) — Every branch is evaluated (each bounded by `limit` on its own) and the union is returned in canonical order; previously evaluation stopped once the union reached `limit`, so equal boolean sets returned different results depending on operand order.
- **Server registry persistence is part of RPC success** (`anda_db_server`) — `db.create`/`db.open` unwind their registration (and close the database) when the registry write fails, and `db.close` reports the failure and stays retryable; previously the registry write was best-effort and a "successful" lifecycle transition could silently not survive a restart.
- **HNSW bootstrap sweeps orphan node blobs** (`anda_db`) — A crash between the ids PUT and the metadata PUT used to leak the removed node's blob forever (no load or purge would ever visit it); bootstrap now deletes node blobs that neither the id set nor the tombstone set references.
- **`EncryptedStore` ETags are seeded with the per-commit nonce** — The logical ETag is now SHA3-256 over the random per-commit nonce followed by the ciphertext. Hashing the bare ciphertext collided for short payloads (a one-byte ciphertext has only 256 possible values), which let a stale CAS token pass a conditional update and silently lose writes once conditional updates switched to logical ETags; found by an OCC stress test that now guards the property.
- **UPSERT self-loop preflight propagates read errors** (`anda_cognitive_nexus`) — Only a definite NotFound degrades to "new concept"; storage or index failures during the preflight now propagate instead of silently skipping the self-loop check it exists to perform.

### Fixed

- **I64 read-back shape no longer corrupts B-Tree indexes** — Stored documents deserialize non-negative `i64` values as `U64` (and `f32` as `F64`); 0.9.2 accepted these shapes at validation but left them un-normalized, so single-field `I64` B-Tree index maintenance silently no-opped: `update` left stale keys behind (queries returned documents by their *old* values), `remove` leaked index entries forever, and creating an index over existing data failed outright. Read-back shapes are now normalized to the declared variant at every document materialization boundary (`try_from_doc`, `set_doc`, `set_field`), and the B-Tree scalar paths additionally tolerate in-range `U64` for `I64` indexes as defense in depth.
- **Collection lifecycle deadlock and double-writer windows** — The global `create_lock` introduced in 0.9.2 was held across the user callback, so creating another collection from inside an `open_or_create_collection` callback deadlocked forever; it also serialized unrelated creations. Replaced with per-name lifecycle locks shared by create / open (slow path) / close / delete: nested creation of a *different* collection works again, `close_collection` no longer races a concurrent `open_collection` into two live writers on one storage prefix (the 0.9.2 ordering removed the handle from the registry *before* flushing, letting an open load a second writable instance whose index writes the closing flush then overwrote), and both create paths re-check `dropping_collections` after acquiring the lock. Lifecycle operations on the *same* name from inside a creation callback are documented as unsupported. `close_collection` failure now puts the (read-only) handle back into the registry so the un-flushed state can be retried instead of being silently dropped.
- **`save_extension` durability window and write amplification** — 0.9.2 routed `save_extension`/`remove_extension` through a full collection flush; a concurrent flusher that had already claimed the metadata version made the call return `Ok` while the winner's write could still fail or be lost to a crash, and every call rewrote the ids bitmap and flushed all indexes. They now perform one unconditional small metadata put (with version precondition and bounded retry) that does not advance the flush watermark: `Ok` means durably written, and the next full flush still persists everything else.
- **Storage cache capacity semantics restored** — 0.9.2 reinterpreted the *persisted* `cache_max_capacity` from entry count to KiB weight, collapsing read-cache hit rates on existing deployments with no way to override (the persisted value wins). `cache_max_capacity` is entry-count again; byte-based bounding is now the separate, optional `cache_max_bytes` (serde-default, backward compatible with existing `storage_meta.cbor`).
- **Hybrid search no longer discards BM25 hits** — A `text`+`vector` query against a collection with no dimension-matched HNSW index degrades to the full-text results with a warning (0.9.2 made the whole query error, dropping valid BM25 hits); a *vector-only* query still errors as intended.
- **F32 JSON read-back** — The 0.9.2 "F64 read-back" acceptance only covered CBOR's exact widening; JSON serializes `f32` via shortest-decimal, so values like `2.71` failed validation on round-trip while `1.25` passed. Both read-back forms are now accepted (and normalized); out-of-range or genuinely lossy `f64` values are still rejected.
- **KIP exact numeric comparison** — 0.9.2's unified `==`/`!=` compared all numbers as `f64`, so integers above 2^53 falsely compared equal (`9007199254740993 == 9007199254740992` was true) and `MIN`/`MAX` could select the wrong element. Integer×integer comparisons now use `i128` (matching `SUM`), integer×float comparisons avoid rounding the integer, and `MIN`/`MAX` are correct at the extremes.
- **KIP loose-equality semantics tightened** — String×string equality is exact again (`"1.10" == "1.1"`, `"1e3" == "1000"`, `"3" == "3.0"` are all false now; datetimes representing the same instant remain equal), while number×string comparisons coerce numerically (`3 == "3.0"` is true). Arrays/objects compare by structural equality again (0.9.2 made `[1] == [1]` false and `[1] != [1]` true — a value unequal to itself). `==`/`!=` are exact negations, exposed as the new public helper `anda_kip::loose_equal`, and the KQL `IN` filter now uses the same equality so `IN(?x, [v])` and `?x == v` cannot disagree. Datetime comparison works across RFC 3339 and RFC 2822 spellings of the same instant.
- **KIP parse-error quality** — `LIMIT 0` (or any invalid LIMIT operand) reports "LIMIT takes a positive integer" at the right position instead of a misleading "unexpected trailing content"; duplicate-key errors point at the duplicated key instead of the start of the object.
- **Derive: bare generic fields with a `FieldTyped` bound compile again** — 0.9.2 unconditionally rejected bare type-parameter fields; the pre-0.9.2 pattern (a `FieldTyped` trait bound supplying `field_type()`) is accepted again, and only unbounded bare generics without `#[field_type]` are compile errors.
- **KQL NOT/grouped aggregation consistency** — The 0.9.2 per-solution anti-join left the old column-wise `ctx.groups` cleanup in place, so grouped `COUNT` over a `NOT`-filtered pattern returned 0 even when members survived; group membership is now narrowed to surviving rows. `NOT` blocks with multiple clauses no longer over-exclude: the excluded tuple set is narrowed by the block's other clauses (`NOT { (?a,"blocked",?b) ?b {type:"Bot"} }` only excludes pairs where `?b` *is* a Bot).
- **KQL dangling-id graceful degradation** — 0.9.2's existence preflight threw `KIP_3002` from *any* execution path; per spec §3.4.7, a dangling `{id:}`/`(id:)` inside `NOT` now makes the clause succeed, inside `OPTIONAL` preserves the outer solution with `null` bindings, and inside a `UNION` branch contributes an empty set — only main-pattern references still fail fast. Storage I/O errors are still propagated everywhere.
- **KQL row-wise UNION with hetero-signature siblings** — A UNION branch's rows were only merged into pattern relations with an *identical* variable signature, so adding a semantically redundant clause that additionally bound a proposition variable silently dropped branch solutions; branch rows now pad into every compatible relation with `null` for unbound slots.
- **KQL solution dedup keys** — Three dedup paths keyed solutions by `|`-joined strings, merging distinct solutions when a predicate contained `|`; keys are structural now.
- **BM25 crash-safe flush ordering (grow-then-shrink)** — 0.9.2's "buckets before metadata" ordering *widened* the crash window for token migration (a token moved to a freshly allocated bucket could vanish with all its postings, and the orphaned file was later overwritten); `flush` now writes migration-target buckets first, then metadata, then rewrites of already-referenced buckets, with the inverse ordering auto-detected after compaction shrinks. The `anda_db` production path now delegates to this single implementation (it had silently kept the old metadata-first order). *Superseded in this release by the bucket-manifest protocol (see Changed), which removes the ordering problem by construction.*
- **HNSW purge persists** — `purge_removed_nodes` now bumps the metadata version so the cleared tombstone set is persisted by the next flush; previously purged deletions were replayed on every reload and the tombstone set grew without bound (a warning now fires past 10k pending tombstones).
- **`UniqueVec` strict panic safety** — The 0.9.2 reordering still had a window (set insertion unwinding after the vec push) that could later admit duplicates, violating the uniqueness invariant B-Tree postings rely on; mutations are now guarded on both sides, including the `remove` variants.
- **B-Tree depth-cap visibility** — Range queries exceeding the depth cap log a warning (index name and depth) instead of only returning a silently empty — and for `Not` roots, semantically inverted — result.
- **Server request timeouts are cancel-safe** — Both `anda_db_server` and `anda_cognitive_nexus_server` wrapped mutating dispatch in `tokio::time::timeout`, truncating non-cancel-safe operations at arbitrary await points; in the worst case a timed-out `db.close` left a detached auto-flush task holding the database open while a retried `db.open` created a second writer. Dispatch now runs on its own spawned task (timeout returns 408, the operation completes in the background), `close_db` cancels the flush token before any await, and `DbEntry` cancels on drop as a backstop.
- **Server shutdown and slow-body hardening** — A shutting-down server rejects new RPCs with 503 instead of racing the database close; an outer route-level timeout (2× request timeout) bounds the whole request including body reading, so a slow-transmitting client cannot hold a connection open indefinitely.
- **Non-loopback startup requires `API_KEY` everywhere** — 0.9.2 added the check only to the shard proxy; `anda_db_server` (which exposes full CRUD) and `anda_cognitive_nexus_server` (arbitrary KML mutation) now also refuse to start on a non-loopback address without an API key unless `INSECURE_NO_API_KEY` explicitly opts in; empty keys are always rejected.
- **Shard proxy negative-cache DoS** — 0.9.2 started caching negative db-route lookups in an unbounded map that only expired logically; unauthenticated requests probing random names could grow proxy memory without limit. The db→shard cache is now a bounded moka cache (100k entries) with per-variant physical TTLs.
- **Shard proxy reconnect resync actually runs** — sqlx's `PgListener::recv()` reconnects transparently and silently drops NOTIFY events, so the 0.9.2 "clear caches on reconnect" never fired for common network blips; the listener now uses `try_recv()` and resyncs the routing caches when it reports a reconnect.
- **Sidecar listings tolerate corrupted metadata** — A sidecar document that exists but fails to decode (torn write before a crash) is treated like a missing one during `list` (entry surfaces with no logical e-tag) instead of failing the whole scan — matching the write path, which already self-heals such objects on the next put. The cross-process weakening of `PutMode::Create` by the orphan self-heal path is now documented as part of the single-writer assumption.
- **`list_logs` limit=0 and `prune_logs` liveness** — `limit=0` returns an empty page (0.9.2 clamped it to 1 and returned data, contradicting KIP's `LIMIT 0` parse error); `prune_logs` exits when a round deletes nothing instead of spinning on a pathological index/document mismatch.

### Changed

- **BM25 & B-Tree bucket persistence replaced by a manifest protocol** (`anda_db_tfs`, `anda_db_btree`; breaking API, rolling on-disk format) — The multi-phase "grow-then-shrink" write ordering, the saved-bucket watermark, and the crash-repair heuristics are gone. Every flush now writes dirty buckets to **fresh immutable objects** keyed by `(bucket_id, generation)` and then commits the metadata, whose new `buckets` manifest (`bucket_id -> generation`) is the loader's single source of truth. The metadata write is the only atomic commit point: a crash before it leaves the previous snapshot fully intact (new objects are unreferenced garbage); after it, the replaced objects are returned as `FlushOutcome::obsolete` for best-effort deletion. Compaction no longer has a special persistence contract — its layout becomes durable atomically with the next manifest commit.
  - **Format rolls forward**: metadata written by earlier releases (no manifest) still loads — the loader falls back to scanning bucket ids `0..=max_bucket_id` at generation `0` (the legacy un-suffixed objects, e.g. `b_3.cbor`) and keeps the legacy higher-bucket-wins/tombstone reconciliation; the first flush upgrades the durable layout to the manifest format and retires rewritten legacy objects. Earlier binaries cannot read data written by this release.
  - **API**: bucket callbacks now receive a `BucketObject { bucket_id, generation }` and return `Result<(), _>` (the cooperative `Ok(false)` stop is gone); `flush`/`flush_with`/`flush_owned_with` return `FlushOutcome { saved, obsolete }` instead of `bool`; `load_all`/`load_buckets` callbacks take `BucketObject`. The low-level `store_metadata`, `store_metadata_with` and `store_dirty_buckets` building blocks are removed — the coordinated flush is the only persistence path. `BTreeIndex::flush_with` (borrowing variant) is folded into `flush_owned_with`.
  - **Concurrency contract narrowed**: both crates no longer serialize flushes internally or defend a flush against concurrent mutations (the internal mutation gate, persistence gate, dirty-bucket counter and empty-posting flush defenses are deleted). Coordinating mutations vs. flush/compaction, and flushes against each other, is the caller's responsibility — `anda_db`'s `Collection` already holds an exclusive operation gate across every flush; a single writer per durable index is a deployment contract.
- **BM25 flush callback signatures** — `BM25Index::flush` / `flush_with` callbacks are now plain `FnMut`/`FnOnce` closures returning a future and receive owned `Vec<u8>` blobs (previously `AsyncFn*` over `&[u8]`); the `AsyncFn*`-over-borrow pattern made every downstream `tokio::spawn` of a flush fail to prove `Send` (rustc "implementation of `Send` is not general enough").
- **HNSW delete-time reconnection is configurable** — New `HnswConfig::reconnect_on_delete` lets recall-sensitive workloads opt into the O(M²·L) under-lock neighbor repair introduced by the unpublished 0.9.2. It now defaults to `false` (see the migration note above).
- **`anda_kip::loose_equal` is public** — The engine's `==`/`!=`/`IN` equality is exposed for downstream executors.

### 0.9.2 (unpublished, 2026-07-10) — folded into this release

Workspace-wide hardening driven by a full per-crate code review. All 13
crates were audited and fixed; ~60 regression tests were added. This version
was never published; entries contradicted by the follow-up review above are
annotated.

### Added

- **`AndaDB::close_collection`** — Closes a collection and releases its handle from the database registry so the collection can be reopened later; previously a closed collection's handle lingered forever and the name could never be reopened in-process.
- **Multi-dimension HNSW routing** — A collection with several HNSW indexes now routes `Search.vector` to the index whose dimension matches the query vector (new `Hnsw::dimension()` accessor); previously any vector search on such a collection always errored.
- **`BTree::try_range_query_ids` and `BTree::flush_with`** — Error-reporting range-query variant (type-mismatched filter values surface `DBError::Index` instead of a silent empty set) and a persist-callback flush that only advances the saved-version watermark after the external write succeeds.
- **Strict metadata authentication mode** — `EncryptedStoreBuilder::with_strict_metadata_auth()` rejects even genuine legacy (pre-auth) metadata; the default mode is now fail-closed against auth stripping (see Fixed) and logs a warning when it accepts legacy metadata.
- **Server operational controls** — `anda_db_server`: `REQUEST_TIMEOUT_SECS` (408 on expiry), `MAX_BODY_SIZE`, `SHUTDOWN_TIMEOUT_SECS` drain deadline. `anda_cognitive_nexus_server`: background auto-flush task (`FLUSH_INTERVAL_SECS`), `kip_logs` retention pruning (`LOG_RETENTION_DAYS`, default off), `SELF_PRINCIPAL_ID`, request timeout and body-limit knobs. Shard proxy: `X-Forwarded-For/-Host/-Proto` injection.
- **RangeQuery depth cap** — `RangeQuery::MAX_DEPTH` (64) with an iterative depth check; hostile deeply-nested filters return an error (or an empty result on the infallible path) instead of overflowing the stack.
- **Derive-macro trybuild UI tests** — Compile-pass and compile-fail coverage for generated code, including shadowed user type names and bare generic fields.

### Changed

- **Search and filter misuse now error instead of returning empty results** — `Search.text` without a BM25 index, `Search.vector` without a dimension-matching HNSW index, and filter values whose type does not match the B-Tree index key now return `DBError::Index`; previously all three silently returned empty result sets.
- **Hybrid search truncation keeps the most-relevant results** — With `Lt`/`Le` filters, result truncation now keeps the head of the RRF-ranked candidates; the tail-keeping strategy only applies to the pure-filter (id-ascending) path. Previously hybrid queries returned the *least* relevant `limit` documents.
- **KQL default result order is deterministic** — Solutions without `ORDER BY` are returned in ascending `EntityID` order (previously clause-insertion order), which also fixes cursor pagination skipping pages over unordered bindings.
- **KIP comparison semantics unified** — `==`/`!=` now use the same loose numeric/datetime comparison as the ordering operators (`3.0 == 3` is true); previously `3.0 == 3` was false while `3.0 <= 3` was true. *The "arrays/objects never compare equal" part was reverted by the follow-up review (structural equality again), and exact big-integer comparison landed there too.*
- **KIP aggregate integer semantics** — `SUM`/`MIN`/`MAX` over all-integer inputs return integers (i128 accumulation, no precision loss above 2^53); `SUM` of an empty set is integer `0`, `AVG` of an empty set is `null`.
- **KIP duplicate keys are parse errors** — Duplicate keys in concept matchers, `SET ATTRIBUTES`/`WITH METADATA` blocks, and nested JSON objects now fail with `KIP_1001` instead of silently keeping the last value; `SEARCH ... LIMIT 0` is likewise rejected at parse time.
- **`UPDATE` response `matched` counts pre-truncation hits** — `updated < matched` now signals `LIMIT` truncation; dangling `{id:}`/`(id:)` references fail fast with `KIP_3002` instead of matching an empty set.
- **Shard proxy refuses insecure exposure** — Listening on a non-loopback address without `API_KEY` now aborts startup unless `INSECURE_NO_API_KEY` is set; `backend_addr` must be an absolute `http://` URI; backend PostgreSQL failures return 503 instead of being misreported as 404, with a 5s negative cache and db-name pre-validation protecting the connection pool.
- **Server 500 responses are generic** — Internal error details go to logs; query-usage errors surfaced by the engine's new `DBError::Index` behavior map to 400.

### Fixed

- **Collection flush checkpoint vs. concurrent adds** — `flush` clamps the crash-recovery checkpoint below the smallest in-flight `add` (ids allocated but not yet in the bitmap), so a crash between id allocation and bitmap update can no longer permanently hide the document from `auto_repair_indexes`. *Superseded in this release by the allocation watermark plus the exclusive flush gate.*
- **`set_extension` persistence** — `set_extension`/`set_extension_with`/`set_extension_from_with` now bump the metadata version so extension data actually persists on the next flush; the prior test passed spuriously against the in-memory cache and has been rewritten to reconnect from storage.
- **Collection lifecycle races** — `delete_collection` vs. `open_collection` can no longer resurrect a zombie collection; `open_or_create_collection` serializes creation and falls back to open on `AlreadyExists`; `inner_drop_prefix` bumps `write_seq` so deleted objects stop being served from cache.
- **Grouped aggregation respects FILTER/NOT narrowing** — Group members are intersected with the narrowed bindings before aggregation, so `COUNT` no longer includes filtered-out members; grouped `FIND` also honors `ORDER BY ?group_var` (previously a silent no-op).
- **Row-wise UNION and sequential pattern joins** — Multi-variable `FIND` no longer keeps only the last covering relation: UNION branches with the same variable signature merge row-wise per KIP §3.4.7.3 (disjoint branches null-pad), and sequential dual patterns over the same variable pair perform a true row-level equi-join instead of an endpoint approximation that produced phantom solutions.
- **Cross-variable NOT anti-join** — `NOT` blocks referencing multiple outer variables prune per solution tuple instead of over-pruning by column; literal-predicate branches retain the `AnyPropositions` constraint.
- **`SET PROPOSITIONS` self-loop preflight** — Self-loops are rejected during validation, before any concept writes, honoring the documented reject-before-write contract; UPSERT preflight was hardened so all detectable failures reject before mutation (true rollback still requires a WAL and remains out of scope).
- **Cascade paths no longer swallow storage errors** — DELETE/UPDATE cascades propagate real storage failures (only "already gone" is tolerated) and removal counts reflect actual deletions; predicates containing `:` now round-trip through `EntityID`.
- **I64/F32 stored-document read-back** — Untyped CBOR deserialization restores non-negative integers as `U64` and `f32` as `F64`; validation and typed conversion now accept these read-back forms (mirroring the existing `Vector` compatibility branch), so such documents no longer fail to load after a flush.
- **JSON prefix escaping inside `FieldValue::Json`** — Strings embedded in `Json` values are `b64:`/`txt:`-escaped symmetrically with deserialization, so values like `"b64:AQID"` survive a human-readable round trip instead of being mangled into `Bytes`.
- **Deep-nesting stack overflow guards** — `FieldValue` recursive conversions enforce `MAX_CONVERSION_DEPTH` (128); the BM25 logical-query parser handles trailing-`)` floods iteratively with a 64-level nesting budget (an 8K-`)` input previously aborted the process); KIP nesting/input limits are now exported as public constants.
- **HNSW deletion repairs the graph** — Removing a node reconnects its former neighbors through neighbor-selection repair, preventing monotonic recall degradation over delete-heavy workloads (recall@10 after deleting 50% of nodes: 0.81 → 0.90 in the new regression test); tombstones persist with metadata so purge survives reload. *The repair now defaults to off and the per-layer tracker was removed again in this release.*
- **BM25 statistics consistency** — `avg_doc_tokens` updates are serialized under one lock so concurrent insert/remove can no longer leave a permanently skewed average; flush persists buckets before advancing the metadata version watermark (HNSW likewise).
- **B-Tree ghost keys after crash** — Empty postings persisted during a remove/flush crash window are treated as tombstones on load (skipped, marked dirty for self-heal), so reloaded trees no longer report keys with no documents; serialization failures during insert return errors instead of panicking.
- **Encrypted metadata auth stripping** — Metadata carrying an auth version but missing `auth_nonce`/`auth_tag` is rejected by default (previously it silently downgraded to legacy verification, allowing cross-path object moves and chunk-boundary truncation by an attacker with storage write access).
- **Sidecar store self-healing** — Corrupted (torn-write) sidecar metadata no longer permanently bricks a key: overwrite puts rebuild it; orphaned data objects (crash between data and meta writes) no longer fail whole `list` scans; `rename(from == to)` no longer deletes the object; concurrent multipart completes serialize per key.
- **`list_logs` remote panic** — `limit=0` (or an empty page) in `anda_cognitive_nexus_server` no longer panics the handler via `rt.last().unwrap()`; limits are clamped and cursors derived safely.
- **Shard proxy stale routing** — The PostgreSQL LISTEN reconnect path clears `db_cache` (missed NOTIFY events could route tenants to shards that no longer own them), with a TTL as a last line of defense; management mutations write and notify inside one transaction.
- **Server registry durability** — `anda_db_server` performs open/create I/O outside the registry lock (a slow S3 open no longer stalls all RPCs), keeps databases that failed to reopen in the registry for retry after restart, and refuses to start on a corrupted registry instead of silently overwriting it.
- **Derive-macro diagnostics and hygiene** — Bare generic fields produce a targeted compile error at the field span (instead of a misleading E0599); generated code uses fully-qualified paths so user types named `FieldType`/`Schema` no longer collide; `#[cbor(key)]` on `AndaDBSchema` top-level fields and container-level `#[unique]` are compile errors.

## [0.9.1] — 2026-07-05

### Changed

- **AES-GCM dependency migrated 0.10 → 0.11** — `anda_object_store` moved to the `aes-gcm` 0.11 `AeadInOut` API; no wire-format change.
- **Workspace dependency alignment** — Internal crate dependency requirements aligned to the `0.9.x` line.

## [0.9.0] — 2026-07-05

### Added

- **KIP v1.0-RC10 specification** — Per-command result shapes (columnar `FIND` result model, solution-set deduplication), `CURSOR` pagination for `EXPORT`, structural `(s, "p", o)` references for higher-order endpoints, `SEARCH PROPOSITION ... WITH TYPE` predicate semantics, `MERGE` source provenance chaining and "already merged" replay self-diagnosis, zero-hop path semantics, bare-variable `ORDER BY` keys, `System`/`Unsorted`/`Archived` operational domains in Genesis bootstrap.
- **EXPORT pagination** — `EXPORT` now supports `CURSOR` with deterministic, idempotent pages; each page is an independently valid capsule.
- **FILTER predicate-variable support** — FILTER expressions over predicate variables (`(?s, ?p, ?o)` patterns) evaluate per-binding without per-iteration cloning.
- **`handle_full_scan_matching`** — Supports the unconstrained `(?s, ?p, ?o)` proposition pattern for memory-metabolism operations (e.g., confidence-decay UPDATE).
- **`compare_order_key`** — Numeric, boolean, datetime-aware string, and null-last ORDER BY semantics matching KIP §3.5.
- **MERGE `diagnose_already_merged`** — Replay of an already-applied MERGE self-diagnoses when the source no longer exists but the target's `_merged_from` already records it.
- **Genesis bootstrap: operational domains** — `Unsorted`, `Archived`, and `System` domains created by Genesis.kip; `Commitment` added to key concept types; `committed_to`/`owed_to` added to key predicates.

### Changed

- **`QueryRelationRow` fields are `Option`** — Enables OPTIONAL left-join padded rows: unbound positions project `null` per KIP §3.4.7.2.
- **`ConceptPK`/`PropositionPK` Display emits valid KIP syntax** — `{id: "C:7"}` instead of `{id: Concept(7)}`, so error messages are directly reusable by self-correcting agents.
- **`$system` type corrected** — From `System` to `Person`, consistent with `$self`.
- **PRIMER degrades gracefully** — A nexus without `$self` still returns a complete domain map; `search_modes` advertises keyword-only capability out-of-band.
- **Multi-hop traversal capped** — Explicit hop bounds beyond 10 return `KIP_4002`; `{m,}` unbounded quantifier is soft-capped.
- **`instance_schema` meta-keys expanded** — Added `item_type` (for arrays), `enum`, and `default_value`; `Person.id` made optional.
- **SleepTask domain** — Moved from CapsuleCreate to Genesis bootstrap; `SleepTask` instances belong to the `System` domain.
- **Lightweight NOT/OPTIONAL sub-context** — `QueryContext::scoped_child` clones only variable bindings and shares the entity cache; `NOT` / `OPTIONAL` no longer clone the (potentially large) relation row sets, groups, or regex cache on every clause.

### Fixed

- **JSON `FieldValue`/`FieldKey` prefix disambiguation (breaking wire change)** — Human-readable serialization now uses explicit prefixes: `Bytes` is `"b64:<url-safe base64>"`, `I64` keys stay `"i64:<n>"`, and text that itself starts with a reserved prefix is escaped as `"txt:<original>"`. The old heuristic promoted *any* Base64-decodable string to `Bytes`, silently corrupting ordinary text like `"test"` on the JSON path (e.g. `anda_db_server` JSON requests). Malformed payloads after a prefix are now hard errors. CBOR encoding is unchanged.
- **Row-based FIND pagination over proposition-less rows** — The relation-row FIND path now paginates with a numeric offset cursor (same convention as the cartesian path). The previous cursor was anchored to the row's proposition id, which multi-hop paths, OPTIONAL-padded rows, and synthetic FILTER relations do not have — a page boundary landing on such a row silently truncated the result with no `next_cursor`.
- **Strict offset-cursor parsing** — The relation-row / cartesian FIND paths and predicate-variable pagination reject a cursor token that is not a plain decimal offset (`KIP_1001`) instead of silently restarting from page one and handing the client duplicate data.
- **Unconstrained `(?s, ?p, ?o)` scan capped** — The full-scan pattern rejects graphs with more propositions than the solution-materialization cap (`KIP_4002`) instead of materializing an unbounded row set; `LIMIT` only bounds projection, not the scan itself.
- **`LIMIT 0` rejected at parse time** — The engine's internal "no limit" sentinel is `0`, so `LIMIT 0` used to silently mean *unlimited* in `FIND`/`EXPORT`; the parser now requires a positive integer (omit `LIMIT` for unlimited).
- **`apply_order_by` JSON pointer construction** — Uses `DotPathVar::to_pointer()` so path components containing `/` or `~` are escaped and a bare-variable key maps to the whole value (`""`) instead of the `""` object key (`"/"`).
- **`reconcile_storage` concurrent-add guard** — The dead-id sweep only considers ids at or below the `max_document_id` snapshot taken before the storage listing, so a document added while the listing runs can no longer be mistaken for a dead id and dropped from the bitmap.
- **Response deserialization** — Custom `Deserialize` dispatches on `error` key presence so a response with both `error` and partial `result` always deserializes as `Err`, not silently as `Ok`.
- **Cross-variable FILTER join semantics** — `FILTER` comparing two different variables (e.g. `FILTER(?a.risk > ?b.risk)`) now evaluates per solution (join) instead of positionally zipping each variable's bindings, which silently returned wrong or empty results. Predicate-variable filters (`FILTER(?p != "…")`) narrow the covering proposition (`?link`) itself, so the memory-metabolism `UPDATE` idiom operates on the correct target set.
- **Multi-variable FIND column alignment** — When no single relation connects all projected variables, the solution set is materialized as their (capped) cartesian product so the columnar `FIND` result stays index-aligned across solutions per KIP §6.2.2; previously the columns could have mismatched lengths and could not be zipped back into rows.
- **Constant FILTER no longer loops** — A variable-free `FILTER` (e.g. `FILTER("a" == "b")`) is evaluated once and clears the solution set on `false`; the previous consume-based evaluator could loop indefinitely on it.
- **Disconnected-solution guard** — Cross-variable `FILTER` and cartesian `FIND` over disconnected variables are capped (`KIP_4002`) rather than materializing an unbounded product.
- **NOT variable scoping** — Only narrows outer-bound variables the NOT block actually references; unrelated variables are preserved intact.
- **OPTIONAL left-join padding** — Outer-bound entities without a match now receive padded rows (projecting `null` for unbound positions) instead of being silently dropped.
- **MERGE provenance chaining** — Source's own `_merged_from` entries are carried forward (duplicates dropped) so provenance survives chains of merges.
- **DELETE PREDICATE cascade** — Higher-order propositions referencing a deleted link are transitively removed; no dangling references.
- **Self-loop proposition error clarity** — Error message now explains the engine storage-model limitation and suggests reification.
- **PRIMER domain type performance** — Populates key schema types in O(#type definitions) instead of O(#members × #domains).
- **Striped document-level concurrency locks** — Added 128-stripe async lock set serializing `update`/`remove` per document id so concurrent mutations of the same document cannot race between index mutations and the versioned storage write, eliminating phantom index entries.
- **`Collection::reconcile_storage` maintenance API** — Full data-directory scan that recovers orphaned documents into the id bitmap and drops dead ids whose objects no longer exist; complements the bounded crash-recovery scan by covering gaps beyond large id discontinuities.
- **HNSW `removed_nodes` tombstone tracking and `purge_removed_nodes`** — Records removed node ids so the persistence layer can delete the corresponding on-disk node blobs during flush cleanup; without this, removed node files accumulated forever.
- **BM25 `store_metadata_with` atomic persistence** — Persist callback variant that only advances the saved-version watermark after the external write succeeds, preventing stale metadata on a failed object-store write.
- **`FieldValue::bytes_from` array coercion** — Accepts CBOR integer arrays in addition to byte strings so `Vec<u8>` and `[u8; N]` struct fields (whose serde serializers emit integer sequences) can populate `FieldType::Bytes` fields.
- **`json_to_cbor` helper** — Replaces the panicking `Cbor::serialized(&obj).expect(...)` path for `FieldValue::Json` with a total conversion function.
- **Derive macro auto-imports schema types** — Added `schema_crate_path()` resolution so `#[derive(AndaDBSchema)]` and `#[derive(FieldTyped)]` import `Schema`, `FieldType`, `FieldKey`, etc. through the correct crate path; callers no longer need explicit `use` statements.
- **CJK tokenizer detection expanded** — `Script::Cjk` now also matches Japanese kana and Hangul syllables alongside Han ideographs, so mixed Japanese/Korean text routes correctly to the CJK tokenizer pipeline.
- **HNSW `ef_search` capped** — `search_layer` caps the user-supplied top-k against `MAX_EF_SEARCH` so a large `Query::limit` cannot force an arbitrarily expensive beam search.
- **BTree bucket size drift** — Unified `posting_entry_size` to account for the field-value key's serialized size in every bucket-size estimate (create, migrate, remove-last, compaction); long string keys previously caused buckets to overshoot `bucket_overload_size`.
- **`FieldValue::deserialized` ownership** — Takes `&self` instead of consuming `self`, allowing reuse of the same value for multiple deserialization targets.
- **Re-insert safety** — HNSW `add` / `insert` clears any pending tombstone for the id so a re-inserted document does not have its new node blob deleted by a stale `removed_nodes` entry.

## [0.8.4] — 2026-06-26

### Added

- **Resource-exhaustion guards** — Added structural complexity validation for runtime field values, query filters, BM25 logical queries, searchable text extraction, KIP parsing input, and HNSW public configuration limits.
- **Authenticated encrypted object metadata** — Added AES-GCM authentication for encrypted object sidecar metadata and chunk associated data so swapped or tampered encrypted payload metadata is rejected, including copy/rename metadata rebinding.

### Changed

- **Workspace crates prepared for 0.8.4** — Bumped `anda_db`, `anda_db_schema`, `anda_db_hnsw`, `anda_db_tfs`, `anda_kip`, `anda_object_store`, and `anda_cognitive_nexus` to `0.8.4` for the new hardening release.
- **Object store dependency updated** — Updated `object_store` from `0.13` to `0.14` and filled default extension metadata in wrapper object-store responses.
- **Server and shard proxy request validation tightened** — Rejected empty API keys, required explicit `Authorization: Bearer <key>` headers, and ignored client-supplied shard headers in favor of server-side/path routing metadata.

### Fixed

- **Read-only extension mutation checks** — Enforced read-only mode for database and collection extension save/remove operations.

## [0.8.3] — 2026-06-19

### Added

- **Integer schema map keys** — Added `FieldKey::I64`, integer wildcard map support via `I64_WILDCARD_KEY`, and signed-integer `Map<I64, T>` / `BTreeMap<i64, T>` schema inference.
- **CBOR integer-keyed nested schemas** — Added `#[cbor(key = N)]` support to `FieldTyped` so nested CBOR-native structs can model integer map labels while keeping schema validation aligned with `cbor2` serialization.

### Changed

- **Workspace crates prepared for 0.8.3** — Bumped `anda_db`, `anda_db_schema`, `anda_db_derive`, and `anda_cognitive_nexus` to `0.8.3`, refreshed schema/derive documentation, and added `cbor2` derive test coverage for integer-keyed CBOR maps.
- **Developer fix target formats first** — Updated `make fix` to run `cargo fmt --all` before applying clippy fixes.

## [0.8.2] — 2026-06-14

### Changed

- **Workspace crates aligned for the 0.8.2 release** — Bumped the published Rust crates to `0.8.2` and kept internal workspace dependency requirements on the matching `0.8` line.
- **Repository metadata normalized** — Updated Cargo package repository and homepage URLs from the old `anda_db` path to the canonical `anda-db` GitHub repository path.
- **Canonical CBOR encoding consolidated on `cbor2`** — Switched index key and virtual-field encoding paths to call `cbor2::to_canonical_vec` directly.
- **Index runtime stats refreshed consistently** — Reused live counter overlays for BM25 and HNSW metadata/stat snapshots so callers observe current element, search, bucket, document, and token statistics.
- **BM25 query execution streamlined** — Reused per-token dedup buffers and merged conjunctive scores during intersection to reduce avoidable allocations and passes.

### Fixed

- **B-Tree posting size accounting** — Reworked existing-posting append and bucket-migration accounting to avoid repeated full-posting measurement while preserving exact source-bucket size updates at CBOR size boundaries.
- **Storage streaming writer sendability** — Made `Storage::stream_writer` return a `Send` async writer so callers can hold it across await points and spawned tasks.
- **Zstd streaming decompression edge cases** — Prevented oversized preallocation for small `max_size` limits and returned an error for truncated frames instead of spinning without progress.
- **HNSW entry-point repair** — Repaired dangling persisted entry points even when the entry id is `0`, treating node id 0 as valid rather than as an unset sentinel.

## [0.8.1] — 2026-06-13

### Added

- **Python binding close API** — Added an idempotent async `PyAndaDB.close()` method so Python clients can explicitly flush and close file-backed Cognitive Nexus stores.
- **CBOR-first Anda DB server RPC API** — Added the `anda_db_server` 0.2.0 RPC surface with root/database-scoped dotted methods, JSON fallback, content negotiation, `GET /` health info, structured HTTP error envelopes, and explicit database lifecycle methods.
- **Server database registry and lifecycle management** — Added multi-database registration, restart-time auto-reopen from primary database extension metadata, per-database background flush tasks, and graceful close/shutdown handling.
- **Quality-assurance test infrastructure** — Added crash-consistency fault injection, on-disk format compatibility fixtures, B-Tree/BM25 property tests, HNSW recall floors, and KIP parser fuzz/proptest coverage.
- **Regression coverage for server RPC and Python binding behavior** — Added HTTP integration and Python tests for server CBOR/JSON/auth/lifecycle behavior, parameter conversion failures, nested parameters, and close idempotency.

### Changed

- **Python binding moved to the 0.3 line** — Bumped `anda_cognitive_nexus_py` to `0.3.0`, updated it to depend on the `0.8` Rust crates, and switched the Python package metadata to derive its version from the binding crate manifest.
- **Python wheel build profile clarified** — Added a `release-py` profile for PyO3 extension wheels and pointed maturin at the binding crate manifest.
- **Anda DB server API modernized** — Replaced the legacy method-name payload handlers with focused `api`, `encoding`, `error`, and `state` modules; updated the README around the new CBOR-first protocol and `local --path` CLI usage.
- **Testing workflow documented and instrumented** — Added testing standards documentation, Makefile coverage targets, and an informational CI coverage job that uploads LCOV artifacts without gating releases.
- **Workspace crate versions aligned for the 0.8 line** — Bumped the supporting database, schema, index, object-store, server, and Cognitive Nexus crates to matching `0.8.x` dependency requirements for the 0.8.1 release train.
- **CBOR stack migrated to `cbor2`** — Replaced direct `ciborium` usage with `cbor2`, updated CBOR encoding/decoding and serialized-size accounting across storage, B-Tree, BM25, HNSW, schema, server, and sidecar code, and updated `ic_auth_types` to the 0.9 line.
- **Developer guidance refreshed for the cbor2-era APIs** — Updated repository agent instructions, docs, README snippets, and the AndaDB skill reference to avoid outdated `ciborium` and removed `cbor_size` examples.

### Fixed

- **Safer Python parameter handling** — Replaced panic-prone JSON string round-tripping with direct JSON-compatible Python value conversion and clear `ValueError` failures for unsupported values, non-finite floats, non-string keys, and excessive nesting.
- **Lossless server parameter decoding** — Kept CBOR and JSON RPC params in their original wire format until typed handler decoding, avoiding lossy cross-format conversion for CBOR-only values such as byte strings.
- **Negotiated server error responses** — Returned authentication, parsing, validation, not-found, conflict, precondition, payload-too-large, and internal failures as structured RPC error envelopes in the negotiated response encoding.
- **Python extension import/build robustness** — Made logger initialization non-fatal when a host process already installed a logger, added PyO3 macOS extension link arguments, and documented the correct module import path in the Python README.

## [0.8.0] — 2026-06-11

### Added

- **KIP mutation primitives** — Added `EXPECT VERSION` optimistic concurrency guards for `UPSERT`, pattern-matched `UPDATE` statements with numeric update expressions, and `MERGE CONCEPT` support for atomic entity consolidation.
- **KIP recall and portability commands** — Extended `SEARCH` with retrieval modes (`keyword`, `semantic`, `hybrid`) and score thresholds, and added `EXPORT` for serializing matched knowledge into idempotent UPSERT capsules.
- **Commitment capsule** — Added the `Commitment.kip` capsule and updated built-in capsule metadata so agents can model durable commitments alongside events and people.
- **Cognitive Nexus KIP execution coverage** — Implemented KML/KQL/META support for the expanded KIP surface, including update execution, merge handling, search scoring, export generation, and version-conflict reporting.
- **Regression coverage for KIP and Cognitive Nexus behavior** — Added parser and executor tests for optimistic concurrency, update expressions, merge semantics, search modes, export capsules, and the split database implementation.

### Changed

- **Workspace crates moved to the 0.8 line** — Bumped `anda_db`, `anda_kip`, and `anda_cognitive_nexus` to `0.8.0`, and updated dependent workspace crates to require the matching `0.8` APIs.
- **Cognitive Nexus database implementation split by responsibility** — Replaced the monolithic `db.rs` with focused modules for KML execution, KQL execution, proposition matching, META commands, shared database setup, and tests.
- **KIP specification and tool schemas refreshed** — Updated the specification, syntax guide, self/system instructions, and function definition JSON files to describe the RC KIP semantics and the new read/write command set.
- **System metadata semantics clarified** — Documented reserved engine-maintained `_` metadata fields, versioning behavior, and protected-scope constraints for write operations.

### Fixed

- **Safer endpoint matching syntax** — Tightened embedded endpoint clause handling so nested concept/proposition endpoints remain unnamed, with explicit guidance for binding endpoints through separate clauses.
- **More robust query and mutation behavior** — Hardened Cognitive Nexus helper/type paths around KIP execution, protected scopes, cache invalidation, and proposition matching while preserving concurrent read and exclusive write semantics.
