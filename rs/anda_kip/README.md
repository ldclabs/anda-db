# anda_kip

[![Crates.io](https://img.shields.io/crates/v/anda_kip.svg)](https://crates.io/crates/anda_kip)
[![Docs.rs](https://docs.rs/anda_kip/badge.svg)](https://docs.rs/anda_kip)

Tracks KIP at `11a82ec`, with the draft memory package
`kip://profiles/cognitive-memory@2.0.0` (content digest `sha256:734aa0fd…`). See the
[Cognitive Nexus documentation](../../docs/anda_cognitive_nexus.md) for implemented contracts and capability boundaries.

`anda_kip` is the protocol SDK of the [AndaDB](https://github.com/ldclabs/anda-db)
workspace: the parser, executable AST, runtime envelope, error registry and
executor seam for **KIP 2.0** (Knowledge Interaction Protocol), the cognitive
state protocol between an Agent and a persistent Cognitive Nexus. The KIP
protocol version (`2.0`) is independent of this crate's version.

## What KIP 2.0 changes

KIP 2.0 is not a bigger 1.x. It splits apart what 1.x kept in a single
self-describing graph:

```text
meaning · belief · evidence · provenance · mnemonic state · retention · Governance · Schema
```

Everything else follows from one distinction:

```text
a Proposition existing  ≠  the Proposition being true
```

A **Proposition** is a truth-neutral `(subject, predicate, object)` tuple. An
**Assertion** is one actor's commitment about it — stance, mode, confidence,
Evidence, valid time. What is *currently believed* is projected from those
Assertions under a named policy and is never stored as truth. That is why
correcting a claim records a new Assertion with `SUPERSEDING` instead of
rewriting the old one: the old belief really was held, and erasing it would
erase the audit trail.

## What this crate provides

- **`parser`** — nom parsers for KQL, KML and META, implementing the three
  KIP 2.0 EBNF grammars, with the schema-independent rules enforced as they
  parse: `ASSERT` desugaring, identity selectors, immutable epistemic payload,
  handle resolution, protected engine fields;
- **`ast`** — the executable AST, field-for-field compatible with the reference
  toolkit [`@ldclabs/kip-lang`](https://github.com/ldclabs/KIP/tree/main/packages/kip-lang),
  so a Rust engine and a TypeScript one can be differentially tested;
- **`semantics`** — the Core Package registries (§20.13) and the other rules
  decidable without a Schema Environment: `stance`, `mode`, Assertion
  lifecycle, Evidence roles, SEARCH modes, and the `[0,1]` ranges the protocol
  itself fixes. A misspelled `stance` is refused here rather than half-way
  through an engine's transaction;
- **`draft`** — what a `DEFINE` body may declare for the Space draft
  vocabulary (§20.16);
- **`error`** — the Core Error Registry (§87): 78 stable named codes, each
  with a category, a retry class and a recovery hint;
- **`request`** — the runtime envelope (§71–§85), including ingestion contexts,
  execution modes and receipts;
- **`types`** — the Core data model (§6–§19);
- **`capsule`** — portable Cognitive Capsules (§37–§41), with the canonical
  serialization their digests are taken over (§37.7);
- **`conformance`** — the profile names an implementation declares (§89);
- **`executor`** — the `Executor` trait an engine implements, and the
  runners around it: `execute_kip`, `execute_readonly`, `execute_request`
  and `execute_request_readonly`;
- **`memory`** — the optional Agent-to-Brain Memory Interface binding (see
  below);
- **`cognitive`** — host-side contracts shared by Nexus engines and Brain
  implementations, such as recording repair;
- **`timestamp`** and **`json`** — canonical §6.5 timestamps and strict
  portable JSON decoding;
- bundled agent-facing material: `KIP_SYNTAX`, `SELF_INSTRUCTIONS`,
  `SYSTEM_INSTRUCTIONS`, `COGNITIVE_MEMORY_PROFILE`, and the function-calling
  definitions `KIP_FUNCTION_DEFINITION` (`execute_kip`) and
  `KIP_READONLY_FUNCTION_DEFINITION` (`execute_kip_readonly`).

This crate is protocol-only. Everything that needs state — Schema resolution,
Governance, transactions, projection — belongs to an engine behind `Executor`,
such as [`anda_cognitive_nexus`](../anda_cognitive_nexus).

`pub` means API: `tests/surface.rs` compares every public item with
`tests/fixtures/public_surface.txt`, so widening or narrowing the surface is a
deliberate change to that file.

## Getting started

```toml
[dependencies]
anda_kip = "0.14"
```

```rust
use anda_kip::{Command, parse_kip};

// Raw claims: who said what, truth-neutral.
let read = parse_kip(
    r#"FIND(?a.asserted_by, ?a.confidence)
       WHERE {
           ?p (:alice, "timezone", ?tz)
           ?a ASSERTION {proposition: ?p}
       }"#,
)?;
assert!(matches!(read, Command::Kql(_)));

// What is currently believed: a Projection, computed not stored.
let belief = parse_kip(r#"FIND(?b) WHERE { ?b BELIEF (:alice, "timezone", ?tz) }"#)?;
assert!(matches!(belief, Command::Kql(_)));

// Recording a claim. `by` and `mode` have no safe default: guessing the actor
// would forge attribution, guessing the mode would turn hearsay into observation.
let write = parse_kip(
    r#"ASSERT (:alice, "prefers", :dark_mode) {
        by: :alice, mode: "stated", confidence: 0.9, evidence: :msg
    }"#,
)?;
assert!(write.is_mutation());
# Ok::<(), anda_kip::KipError>(())
```

To run commands, hand an `Executor` to the runners: `execute_kip(&engine,
command, dry_run)` for one command, `execute_readonly` for the read-only path
(which refuses state-changing commands by what they parse as, never by a
label), and `execute_request` for a full request envelope. A batch is not a
transaction: the runners execute `independent` and `sequence` batches and
refuse `atomic` with `UnsupportedCapability` rather than faking it.

## Feature flags

| Feature             | Effect                                                                                      |
| ------------------- | ------------------------------------------------------------------------------------------- |
| `schema-validation` | JSON Schema validation of Memory Interface shapes and `schema_validator` (`jsonschema` 0.58) |

The feature is off by default so the parser's WASM build stays small.
`schema_validator` returns a `jsonschema::Validator`, which is why the
`jsonschema` 0.58 upgrade moved this crate to 0.14.1.

## Timestamp inputs

KIP §6.5 requires valid UTC timestamps with exactly three fractional digits:
`YYYY-MM-DDTHH:mm:ss.SSSZ`, including `.000Z` for whole seconds. Invalid
strings return `ConstraintViolation`; non-string timestamps return
`TypeMismatch`. Optional/null values follow each field's contract. The engines
validate inputs without padding fractions or converting offsets; generated
clock values truncate to milliseconds. Use `space_seq` for commit order.

## SDK compatibility notes

Capsule records retain wire order in `CapsuleRecords(Vec<Json>)`; use `by_kind`
for filtered access. Delta changes, handling values, and proof members survive
round trips. Core lifecycle reference lists accept portable reference objects
as well as native-view IDs. Explicit null payloads/results remain present.
Independent/sequence batch receipts are attached to `results[].receipt`.
See [SDK migration details](../../docs/anda_kip.md#11-cognitive-capsules).

## Memory Interface binding

`anda_kip::memory::binding` types the optional Agent-to-Brain binding of
`Memory-Interface.md` (`schemas/kip-memory.schema.json`): `Request` /
`Response`, receipts and `Progress`, `Briefing` and `Coverage`, and the
`Descriptor` a deployment advertises. `Request::intent` applies the envelope
rules (an idempotency key on every mutation and none on a recall), and
`MemorySession` keeps a host session's outstanding receipts and attention
cursor across restarts. Enable the `schema-validation` feature to validate
requests, responses and descriptors against the vendored schemas. Typing the
wire shapes does not give a Nexus a binding: a Brain declares the levels it
serves.

## Command-line syntax check

`kip-cli` parses every `.kip` file in the given files or directories,
reports each missing, unreadable or invalid one, and exits non-zero if any
failed:

```bash
cargo run -p anda_kip --bin kip-cli -- path/to/commands
```

## Testing, benchmarks and fuzzing

```bash
cargo test -p anda_kip --all-features
cargo bench -p anda_kip --bench protocol --profile release-speed
```

The tests cover property-based parser checks, byte-identical AST parity with
fixtures produced by `@ldclabs/kip-lang` (see
[tests/fixtures](tests/fixtures/README.md)), the wire schemas, the syntax
documents and the public surface.
Benchmark results are recorded in
[docs/benchmarks/anda_kip_2026-09-23](../../docs/benchmarks/anda_kip_2026-09-23/README.md).
Coverage-guided parser fuzzing lives in [`fuzz/`](fuzz/README.md).

KIP command strings in this crate's `src/` and `tests/` feed the TypeScript
engine's parser-oracle corpus. After adding or editing one, run
`pnpm run codegen` in [`ts/kip-do`](../../ts/kip-do); after a parser change,
also rebuild the WASM oracle with `pnpm run build:oracle-wasm`.

## Vendored KIP documents

The Specification, syntax card, companions, grammars, schemas, profile and
`brain/` cards are copies of KIP `11a82ec`, byte for byte except:

- file names drop the `KIP-2.0-` prefix (`KIP-2.0-Memory-Interface.md` →
  `Memory-Interface.md`, `grammar/KIP-2.0-KQL.ebnf` → `grammar/KQL.ebnf`,
  `brain/KIP-2.0-Brain-Runtime.md` → `brain/Brain-Runtime.md`), and relative
  links follow the renames;
- the English/Chinese navigation line is removed;
- links to documents not vendored here (the Architecture, the resolution
  records, `conformance/`, `migration/`) point at the KIP repository.

`Cognitive-Consistency.md` is upstream's redirect table: its contracts moved
into the Specification and the Brain Runtime / Validated Learning companions
(`BRAIN_RUNTIME`, `VALIDATED_LEARNING`). The bundled Schema Package artifacts
live in `anda_cognitive_nexus/profiles/`.

## Technical reference

- [docs/anda_kip.md](../../docs/anda_kip.md)
- [`SPECIFICATION.md`](./SPECIFICATION.md) — the normative KIP 2.0 specification
- [`Capsule-Specification.md`](./Capsule-Specification.md) — its §37–§41 and §95, the Cognitive Capsule, carried in a companion under the same numbering
- [`Optional-Profiles-and-Migration.md`](./Optional-Profiles-and-Migration.md) — its §100, §101, §103 and Appendix I: the optional Historical and High-Assurance profiles, and KIP 1.x migration
- [`Invariants.md`](./Invariants.md) — the invariant registry: the 49 Core invariants and the Cognitive Memory Profile's 49, one list
- [`Memory-Interface.md`](./Memory-Interface.md) — the optional Agent-to-Brain binding
- [`grammar/`](./grammar) and [`schemas/`](./schemas) — the normative EBNF grammars and the request / response / change-envelope wire schemas
- [`KIPSyntax.md`](./KIPSyntax.md) — the LLM-facing syntax reference
- [`SelfInstructions.md`](./SelfInstructions.md) — how an Agent should use its memory
- [`SystemInstructions.md`](./SystemInstructions.md) — what a runtime owes its callers
- [`brain/`](./brain) — the Brain cards and the Brain Runtime / Validated Learning companions

## Related crates

- [`anda_cognitive_nexus`](../anda_cognitive_nexus) — the reference Rust KIP
  engine
- [`anda_kip_wasm`](../anda_kip_wasm) — this parser compiled to WebAssembly,
  the oracle for the TypeScript engine's parser
- [`@ldclabs/kip-do`](../../ts/kip-do) — the independent TypeScript engine

## License

MIT. See [LICENSE](../../LICENSE).
