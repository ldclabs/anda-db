# Changelog

All notable changes to this workspace are documented in this file.

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
