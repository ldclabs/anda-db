# KIP and Cognitive Nexus Integration

Use this reference for KIP/Brain work, not ordinary document CRUD. KIP protocol
versions and Cargo package versions are different: the current Rust crates
are in the 0.13 family, implementing KIP 2.0 with the vendored 2.1.0 memory
vocabulary. Check the checked-out specifications and runtime capabilities
rather than inferring support from a package version.

## Choose the layer

| Component | Role and starting point |
| --- | --- |
| `anda_db` | Embedded document storage and B-Tree/BM25/HNSW retrieval; no model-generated embeddings or Brain scheduler |
| `anda_kip` | Protocol SDK: `parse_kip`, executable `Command` AST, request/response envelopes, semantics, errors, `Executor` seam |
| `anda_cognitive_nexus` | Stateful Rust executor over AndaDB: transactions, Schema resolution, Governance, belief projection |
| `anda_cognitive_nexus_server` | HTTP/JSON-RPC transport with a shared system-Principal administrator endpoint |
| `anda_db_server` / `anda_db_shard_proxy` | Core database HTTP APIs / multi-tenant shard routing |
| `ts/kip-do` | Independent SQLite-backed Cloudflare Durable Object KIP engine, not a Rust binding |
| `rs/anda_kip_wasm` | Rust parser compiled to WASM for the TypeScript differential oracle |
| `py/anda_cognitive_nexus_py` | Python binding; excluded from default workspace builds |

Use [the protocol guide](../../../docs/anda_kip.md) and
[SDK README](../../../rs/anda_kip/README.md) for parsing/envelopes. Protocol
authority is the vendored [specification](../../../rs/anda_kip/SPECIFICATION.md),
[syntax guide](../../../rs/anda_kip/KIPSyntax.md),
[grammar](../../../rs/anda_kip/grammar/), and
[wire schemas](../../../rs/anda_kip/schemas/).

For the Rust runtime, start with
[Nexus README](../../../rs/anda_cognitive_nexus/README.md) and
[implemented contracts](../../../docs/anda_cognitive_nexus.md). The current
entry point is `CognitiveNexus::connect(Arc::new(db)).await?`; execution goes
through `session(auth)`. The host constructs `AuthContext` from authenticated
transport state. Embedded `system_session()` uses the engine's system
Principal; do not replace authenticated user sessions with it.

## Data and capability boundaries

KIP §6.5 (upstream `dcde1de`) requires timestamp inputs to be UTC strings
with exactly three fractional digits: `YYYY-MM-DDTHH:mm:ss.SSSZ`. Whole
seconds use `.000Z`. Invalid strings are `ConstraintViolation`; non-strings
are `TypeMismatch`. Null/absence follows each field's contract. Inputs are
never silently normalized. Engine clocks truncate to milliseconds; use
`space_seq`, not timestamps, for per-Space commit order. This applies to
host APIs, Profile timestamps, and time-valued query parameters as well.

KIP 2.0 separates Concept, Proposition, Assertion, Evidence, and Activity.
A Proposition is a truth-neutral tuple. Assertions carry actor, stance,
mode, confidence, and evidence. `BELIEF` is a named-policy projection, not a
truth field to mutate. Governance and versioned Schema Packages belong to
their control planes, not ordinary KML graph edits.

Use `DESCRIBE CAPABILITIES` for the connected engine. Parsing a command or
installing a profile does not mean its execution capability is implemented.
Core AndaDB's vector search does not imply KIP semantic/hybrid `SEARCH`
support or a configured embedding model. Use the
[TypeScript engine guide](../../../ts/kip-do/README.md) for its own boundaries
and host APIs; do not assume Rust/TypeScript host methods are interchangeable.

For Watch, wake, dispatch, contextual trust, and Brain host integration,
read [Anda Brain host contracts](../../../docs/anda-brain-nexus-contracts.zh.md).
The host supplies scheduling, evaluators, executors, and outward delivery.
Nexus persistence/receipts do not imply exactly-once external effects or an
installed background worker.

KIP 1.x data needs the explicit
[migration guide](../../../docs/kip-v1-migration.md). Core schema evolution is
not sufficient to migrate the old graph model; collection replacement is
one-way and should be rehearsed on a backup.

## SDK wire handling

Use `Request::from_json` for raw JSON to retain strict duplicate-key and numeric
source checks. The batch helpers preserve independent/sequence receipts under
`results[].receipt`; the top-level receipt is for atomic execution. Explicit
null ingest payloads and result values are present values, not missing fields.

For transports that classify commands before executing them, use
`PreparedRequest::from_value` after strict raw JSON decoding. Its read-only
`request()` and `operations()` views share the validated envelope and parsed
commands; consuming `execute()` keeps ordinary batch/receipt semantics without
another parse. It does not add atomic execution or authenticated authority.

`CapsuleRecords(Vec<Json>)` preserves array order; `by_kind` borrows a filtered
view. Do not regroup records before computing or verifying a Capsule digest.
The SDK retains delta `changes`, arbitrary handling values and proof members.
See the [SDK migration notes](../../../docs/anda_kip.md#11-cognitive-capsules).

## Cross-engine validation

The two engines share [conformance fixtures](../../../fixtures/kip-conformance-2.0/).
When changing protocol behavior, inspect corresponding Rust and TypeScript
implementations and test both affected paths.

```bash
cargo test -p anda_kip -p anda_cognitive_nexus
make test-ts
```

`make test-ts` runs TypeScript typechecking and tests, including conformance
and the parser oracle. Generated `errors.generated.ts`,
`capability-names.generated.ts`, `profiles.generated.ts`,
`contracts.generated.ts`, `fixtures.generated.ts`, and `corpus.generated.ts`
are committed. If their sources change, run
`pnpm run codegen` from `ts/kip-do` and review the generated diff.

When the Rust KIP parser changes, also run `pnpm run build:oracle-wasm` from
`ts/kip-do` and include the updated `vendor/anda_kip_wasm/` artifacts. This
requires wasm-pack and the WASM target toolchain. `codegen` does not rebuild
the oracle, and TypeScript tests use the committed artifact. Rerun
`make test-ts` after updating it.

`make test-full` combines Rust tests, format-compatibility tests, KIP fuzzing,
and TypeScript checks; it needs pnpm plus cargo-fuzz/nightly. Read
[testing instructions](../../../docs/testing.md) and the [Makefile](../../../Makefile)
for the chosen check. Do not assume a default workspace test includes Python,
the separate WASM/tokenizer workspaces, or TypeScript.
