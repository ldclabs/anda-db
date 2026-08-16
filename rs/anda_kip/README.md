# anda_kip

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
- **`error`** — the Core Error Registry (§87): stable named codes with a
  category, a retry class and a recovery hint;
- **`request`** — the runtime envelope (§71–§85), including ingestion contexts,
  execution modes and receipts;
- **`types`** — the Core data model (§6–§19);
- **`capsule`** — portable Cognitive Capsules (§37–§41);
- **`executor`** — the trait an engine implements, plus the read-only path;
- bundled agent-facing prompts and function-calling schemas.

This crate is protocol-only. Everything that needs state — Schema resolution,
Governance, transactions, projection — belongs to an engine behind `Executor`.

## Getting started

```toml
[dependencies]
anda_kip = "0.12"
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
- [`KIPSyntax.md`](./KIPSyntax.md) — the LLM-facing syntax reference
- [`SelfInstructions.md`](./SelfInstructions.md) — how an Agent should use its memory
- [`SystemInstructions.md`](./SystemInstructions.md) — what a runtime owes its callers

## Related crates

- `anda_cognitive_nexus` — the reference KIP executor
- `anda_db` — the embedded storage core the reference backend uses

## License

MIT. See [LICENSE](../../LICENSE).
