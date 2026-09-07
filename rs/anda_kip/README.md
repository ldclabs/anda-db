# anda_kip

Tracks KIP v2 at `d6e3a45`, including the 2.1.0 memory vocabulary. See the
[synchronization and compatibility notes](../../docs/kip-v2-d6e3a45-sync.md) for implemented contracts and capability boundaries.

`anda_kip` is the protocol SDK of the AndaDB workspace: the parser, executable
AST, runtime envelope, error registry and executor seam for **KIP 2.0**
(Knowledge Interaction Protocol), the cognitive state protocol between an Agent
and a persistent Cognitive Nexus.

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
- **`error`** — the Core Error Registry (§87): stable named codes with a
  category, a retry class and a recovery hint;
- **`request`** — the runtime envelope (§71–§85), including ingestion contexts,
  execution modes and receipts;
- **`types`** — the Core data model (§6–§19);
- **`capsule`** — portable Cognitive Capsules (§37–§41), with the canonical
  serialization their digests are taken over (§37.7);
- **`conformance`** — the profile names an implementation declares (§89);
- **`executor`** — the trait an engine implements, plus the read-only path;
- bundled agent-facing prompts and function-calling schemas.

This crate is protocol-only. Everything that needs state — Schema resolution,
Governance, transactions, projection — belongs to an engine behind `Executor`.

## Getting started

```toml
[dependencies]
anda_kip = "0.13"
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

// What is currently believed: a Projection, computed not stored.
let belief = parse_kip(r#"FIND(?b) WHERE { ?b BELIEF (:alice, "timezone", ?tz) }"#)?;

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

## Command-line syntax check

```bash
cargo run -p anda_kip --bin kip_cli -- path/to/commands
```

## Technical reference

- [docs/anda_kip.md](../../docs/anda_kip.md)
- [`SPECIFICATION.md`](./SPECIFICATION.md) — the normative KIP 2.0 specification
- [`Capsule-Specification.md`](./Capsule-Specification.md) — its §37–§41 and §95, the Cognitive Capsule, carried in a companion under the same numbering
- [`Optional-Profiles-and-Migration.md`](./Optional-Profiles-and-Migration.md) — its §100, §101, §103 and Appendix I: the optional Historical and High-Assurance profiles, and KIP 1.x migration
- [`Invariants.md`](./Invariants.md) — the invariant registry: the 38 Core invariants and the Cognitive Memory Profile's 35, one list
- [`grammar/`](./grammar) and [`schemas/`](./schemas) — the normative EBNF grammars and the request / response / change-envelope wire schemas
- [`KIPSyntax.md`](./KIPSyntax.md) — the LLM-facing syntax reference
- [`SelfInstructions.md`](./SelfInstructions.md) — how an Agent should use its memory
- [`SystemInstructions.md`](./SystemInstructions.md) — what a runtime owes its callers

## Related crates

- `anda_cognitive_nexus` — the reference KIP executor
- `anda_db` — the embedded storage core the reference backend uses

## License

MIT. See [LICENSE](../../LICENSE).
