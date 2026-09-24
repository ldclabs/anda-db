# AndaDB - AI Agent Database

AndaDB is a modular Rust workspace for embedded AI memory systems. The core
`anda_db` crate is a schema-aware document database with B-Tree, BM25, and HNSW
retrieval, backed by `object_store`. `anda_kip` is the protocol SDK;
`anda_cognitive_nexus` is the stateful KIP executor over AndaDB. The independent
TypeScript engine in `ts/kip-do` uses SQLite-backed Cloudflare Durable Objects.

## Agent Workflow

- Work independently as the current agent. Do not spawn or delegate work to
  subagents.
- Before editing, run `git status --short`, confirm the current branch, and
  inspect existing diffs in the files you intend to change. Preserve the user's
  existing work; do not overwrite or revert unrelated files or changes.
- Use `rg` for search and focused reads before editing. Do not assume module
  boundaries from filenames alone.
- Before committing, review the final diff and stage only the files or hunks
  belonging to the requested task.
- At completion, briefly summarize the changes, the checks actually run and
  their results, and any checks not run or blocked. Never report an unrun check
  as passing. When committing, include the branch and commit ID in the summary.

## Start with the local skill

Before writing Rust code that uses AndaDB, or changing its API examples, read
[skills/anda-db/SKILL.md](skills/anda-db/SKILL.md). It owns the dependency
template and runnable quick start; keep examples there instead of duplicating
them in this document. Read supporting references only as needed:

| Task                                                    | Reference                                                                 |
| ------------------------------------------------------- | ------------------------------------------------------------------------- |
| Lifecycle, CRUD, indexes, search, pagination            | [Core API](skills/anda-db/references/anda_db_quick_ref.md)                |
| Derives, type mapping, schema upgrades, serialization   | [Schemas and CBOR](skills/anda-db/references/schema_and_cbor.md)          |
| Backends, encryption, cache budgets, recovery, shutdown | [Storage and recovery](skills/anda-db/references/storage_and_recovery.md) |
| KIP, Nexus, Brain host integration, cross-engine checks | [KIP and Cognitive Nexus](skills/anda-db/references/kip_and_nexus.md)     |

Use the checked-out implementations and manifests as the authority when prose
or older examples disagree. Update affected documentation alongside public API
or behavior changes.

## Project structure

```text
rs/
├── anda_db/                      # Core embedded database
├── anda_db_schema/               # Schema, FieldType, FieldValue, Document
├── anda_db_derive/               # AndaDBSchema and FieldTyped derives
├── anda_db_btree/                # Exact-match and range index
├── anda_db_tfs/                  # BM25 full-text search
├── anda_db_hnsw/                 # HNSW vector index
├── anda_db_utils/                # Standalone UniqueVec; no workspace users
├── anda_object_store/            # Metadata and encrypted storage wrappers
├── anda_kip/                     # KIP SDK, specs, grammar, wire schemas
├── anda_cognitive_nexus/         # Stateful Rust KIP executor
├── anda_db_server/               # HTTP server for core database APIs
├── anda_cognitive_nexus_server/  # HTTP/JSON-RPC server for Nexus
├── anda_db_shard_proxy/          # Multi-tenant shard proxy
├── anda_kip_wasm/                # Parser test oracle; separate workspace
└── cf-tokenizer/                 # Stateless Jieba HTTP service; separate workspace

ts/kip-do/                       # Independent TypeScript KIP engine
py/anda_cognitive_nexus_py/      # Python binding; not a default workspace member
fixtures/kip-conformance-2.0/    # Shared cross-engine conformance fixtures
skills/anda-db/                 # Agent-facing API usage guidance
docs/                          # Technical and maintenance guides
```

Root `cargo --workspace` commands do not include the separate WASM/tokenizer
workspaces, TypeScript, or the Python binding.

## Dependencies and implementation constraints

The root workspace uses Rust edition 2024 with MSRV **1.95**. Rust crates are
in the **0.14** release family; patch versions can diverge after a release,
so read each `Cargo.toml` instead of assuming a shared patch version. Use workspace
dependencies and existing feature conventions when editing workspace crates.

- `object_store` is on 0.14. `anda_db/full` only enables `object_store/fs`;
  core indexes and Jieba are already available without that feature.
- Use `cbor2` for CBOR, `cbor2::serialized_size` for encoded sizes, and
  `cbor2::to_canonical_vec` where deterministic bytes are required. Do not
  introduce direct `ciborium` usage.
- Stored embeddings use `Vector` and `vector_from_f32`; query vectors use
  `Vec<f32>`. Preserve the embedding model's dimension and metric.
- Share one live writer instance per database namespace. Clone database and
  collection handles for concurrent tasks; `DBConfig::lock` is not a writer
  lease. Await mutations, flushes, and closes to completion; cancellation can
  poison a handle, requiring reopen/recovery through the database.
- Install tokenizers and deterministic index hooks at the start of each fresh
  open callback, before recovery-triggering operations. Active cached handles
  skip callbacks. Use `db.close_collection(name)` before reopening to change
  schema/index configuration.
- Multi-field B-Tree indexes are always unique. Their encoded keys support
  tuple equality, not tuple-ordered ranges. `_id` is queryable without an
  explicit B-Tree index.
- Schema upgrades preserve persisted field indexes and require a higher
  version; new fields must be optional. Storage settings are fixed on first
  initialization and are not changed by passing a different startup config.
  See the references before implementing migrations or storage reconfiguration.

## KIP and Brain boundaries

KIP protocol versions are distinct from Cargo package versions. The repository
implements KIP 2.0 (KIP `3251912`) with the vendored `cognitive-memory@2.0.0`
draft package; use the
[vendored specification](rs/anda_kip/SPECIFICATION.md) and
[syntax reference](rs/anda_kip/KIPSyntax.md).

Protocol timestamps follow §6.5: UTC strings in `YYYY-MM-DDTHH:mm:ss.SSSZ`
form, including `.000Z` for whole seconds. Reject noncanonical inputs;
truncate engine clocks to milliseconds. Use `space_seq` for commit order.

A Proposition is truth-neutral; Assertions record claims and `BELIEF` projects
them under a policy. Governance and Schema Packages have separate control
planes. Hosts construct authenticated session context; user request fields
must not choose their own authority.

Check `DESCRIBE CAPABILITIES` on the relevant engine before relying on optional
behavior. Parsing a command or installing a profile does not install an
embedding model, scheduler, evaluator, or executor. For Watch/wake/dispatch and
learning integration, read the [Brain host contracts](docs/anda-brain-nexus-contracts.zh.md).
For old stores, follow the [KIP 1.x migration guide](docs/kip-v1-migration.md).

## Build and validation

Run commands from the repository root unless stated otherwise. Choose checks
for the changed behavior; documentation-only edits need link/consistency
checks and compilation of changed runnable examples, not unrelated full suites.

| Scope                                        | Command                                                            |
| -------------------------------------------- | ------------------------------------------------------------------ |
| Rust workspace compile                       | `cargo check --workspace --all-features`                           |
| Rust workspace tests                         | `cargo test --workspace --all-features`                            |
| Core database unit and integration tests     | `cargo test -p anda_db --all-features`                             |
| Core crash recovery and format compatibility | `cargo test -p anda_db --test crash_recovery --test format_compat` |
| Schema and derive changes                    | `cargo test -p anda_db_schema -p anda_db_derive`                   |
| KIP SDK and Rust executor                    | `cargo test -p anda_kip -p anda_cognitive_nexus`                   |
| TypeScript typecheck and tests               | `make test-ts`                                                     |
| Formatting and Clippy                        | `make lint`                                                        |

`make lint` runs `cargo fmt` and can change files. For a formatting check
without edits, use `cargo fmt --all -- --check`.

`make test-all` adds format-compatibility checks and KIP fuzzing to Rust tests.
`make test-full` also runs TypeScript checks. Fuzzing requires nightly and
cargo-fuzz; TypeScript checks require installed dependencies and pnpm. The
current TypeScript CI uses Node 24 and pnpm 11; see
[the CI workflow](.github/workflows/test.yml) for the matching setup.

When changing persistence/recovery, verify crash consistency and existing
format fixtures. For index algorithm changes, run the affected index crate's
tests, including its property/recall checks. See [testing guidance](docs/testing.md).
Do not regenerate old fixtures merely to make an incompatible change pass.

The runnable core example is
`cargo run -p anda_db --example db_demo --features full`; it creates data under
`./debug/metastore`. Keep documentation-example test data in a temporary
directory. For Python tests, follow the root `Cargo.toml` / `make test-py`
instructions to enable the binding member and select its supported interpreter.

## Generated files and protocol changes

`ts/kip-do` tests shared conformance fixtures and compares its parser against
the committed Rust WASM oracle. Protocol changes should be checked in both
engines; a default Rust workspace test does not exercise TypeScript.

When generation sources change, run `pnpm run codegen` **from `ts/kip-do`**
and include the generated diff:

- `src/errors.generated.ts`
- `src/meta/capability-names.generated.ts`
- `src/schema/profiles.generated.ts`
- `src/schema/contracts.generated.ts`
- `test/conformance/fixtures.generated.ts`
- `test/oracle/corpus.generated.ts`

When the Rust KIP parser changes, also run `pnpm run build:oracle-wasm` from
`ts/kip-do` and include the updated `vendor/anda_kip_wasm/` artifacts. This
requires wasm-pack and the WASM target toolchain. `pnpm run codegen` does not
rebuild the oracle. Then run `make test-ts` from the repository root. CI
regenerates the TypeScript files and rejects drift.
