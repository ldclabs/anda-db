# Changelog

All notable changes to this workspace are documented in this file.

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
