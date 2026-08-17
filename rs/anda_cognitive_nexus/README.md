# anda_cognitive_nexus

The reference **KIP 2.0** Cognitive Nexus — a persistent memory brain for AI
agents. `anda_kip` parses, classifies and validates; this crate is everything
that needs state, and implements `anda_kip::Executor` on top of `anda_db`.

The KIP 1.x engine that used to live here was **deleted, not ported**. 2.0 is a
different data model — a Proposition existing is not the Proposition being true
— and a renamed 1.x engine would have been a worse lie than an absent one.

## What This Crate Provides

The five Core element kinds — Concept, Proposition, Assertion, Evidence,
Activity — get one `anda_db` collection each. A Proposition is a truth-neutral
tuple; an **Assertion** is one actor's commitment about it, carrying a stance, a
mode, a confidence and its Evidence. What is *currently believed* is projected
from those Assertions under a named policy and never stored — so correcting a
claim writes a new Assertion and a supersession link, not a rewritten row.

- **KML** in real transactions: creation, `ENSURE`, `UPSERT`, `UPDATE`,
  `MERGE CONCEPT`, the Assertion and Evidence lifecycles, retention and removal,
  handles, preconditions, receipts and dry runs;
- **KQL**: element, tuple and structural patterns, hop-quantified traversal,
  `FILTER`, `NOT` / `OPTIONAL` / `UNION`, aggregates, paging, and two time axes
  kept apart: `FOR TIME` (what was true then) and `AS OF` (what this Brain held
  then, reconstructed from the version log rather than approximated);
- the **Epistemic Projection** behind `BELIEF`, under a named versioned policy:
  silence is `insufficient`, not `rejected`, and repetition is not corroboration;
- **META** — `DESCRIBE`, `LIST`, `SEARCH`, `VALIDATE`, `PREVIEW`, `HISTORY`,
  `CHANGES`, `SNAPSHOT` — plus Capsule export and verification;
- **Governance** in a separate control plane — Principals, Grants, Delegations,
  versioned Policies, approvals, audit — authorizing every command and every
  element it touches under default deny, and reachable from no KML clause;
- **Schema Packages**, immutable versioned artifacts resolved through a per-Space
  Schema Environment. In 1.x schema was graph state, so an ordinary write could
  change what a type meant; here every `schema_ref` names an exact version.

## When to Use It

Use it when memory must record who claimed what, on what evidence, and what
changed when, and when disagreement has to stay recordable rather than resolved
by the last writer — and to get the reference KIP engine rather than write an
`Executor` yourself. For documents and retrieval, use `anda_db` directly.

## What It Does Not Do Yet

`DESCRIBE CAPABILITIES` is the machine-readable answer: every gap as structured
data with a reason, so an Agent reads what is missing instead of discovering it
by triggering an error — or, worse, reading an absent feature as an absent fact.
Gaps are refused as `UnsupportedCapability` rather than answered wrongly: atomic
batches; idempotency keys, recorded but not replayed, so a resend re-executes;
grouped aggregation; `STRUCTURAL` over Core reference fields; semantic and hybrid
`SEARCH`, and `SEARCH … AS OF`; Capsule signatures; Space-level retention
defaults. There is no trust model and no evidence-quality evaluation either, so
every corroboration group counts equally — and every projection says so.

## Getting Started

```toml
[dependencies]
anda_cognitive_nexus = "0.12"
anda_kip = "0.12"
anda_db = { version = "0.11", features = ["full"] }
```

```rust
let nexus = CognitiveNexus::connect(Arc::new(db)).await?;
let profile = anda_cognitive_nexus::profiles::COGNITIVE_MEMORY;
nexus.install_and_activate(&[("bundled", profile)], DEFAULT_SPACE).await?;
```

A caller reaches the engine through `CognitiveNexus::session(auth)`, where the
host builds the `AuthContext` from authenticated transport state, never from the
request body — exactly what an Agent under prompt injection controls. Embedded,
it runs as the system Principal that owns the default Space: a real
authorization through the same path, not a bypass.

## Technical Reference

- [docs/anda_cognitive_nexus.md](../../docs/anda_cognitive_nexus.md) — the engine
- [docs/anda_kip.md](../../docs/anda_kip.md) — the protocol layer
- [docs/anda_db.md](../../docs/anda_db.md) — the storage core

`anda_cognitive_nexus_server` serves this crate over HTTP/JSON-RPC. MIT licensed.
