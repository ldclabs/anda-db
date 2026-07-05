# Changelog

All notable changes to this workspace are documented in this file.

## [Unreleased]

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
