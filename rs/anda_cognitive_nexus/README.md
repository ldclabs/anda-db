# anda_cognitive_nexus

Tracks KIP v2 at `d6e3a45`, including the 2.1.0 memory vocabulary. See the
[Cognitive Nexus documentation](../../docs/anda_cognitive_nexus.md) for implemented contracts and capability boundaries.

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
  kept apart: `FOR TIME` (what was true then) and `AS OF SEQ` (what this Brain
  held then, reconstructed from the version log rather than approximated);
- the **Epistemic Projection** behind `BELIEF`, under a named versioned policy:
  silence is `insufficient`, not `rejected`, and repetition is not corroboration;
- **META** — `DESCRIBE` (including `DESCRIBE SNAPSHOT`, which resolves a
  timestamp to the coordinate an `AS OF SEQ` read can use), `LIST`, `SEARCH`,
  `VALIDATE`, `PREVIEW`, `HISTORY`, `CHANGES` — plus Capsule export and
  verification;
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
`SEARCH`, and `SEARCH … AS OF SEQ`; Capsule signatures; Space-level retention
defaults. There is no trust model and no evidence-quality evaluation either, so
every corroboration group counts equally — and every projection says so.

## Getting Started

```toml
[dependencies]
anda_cognitive_nexus = "0.13"
anda_kip = "0.13"
anda_db = { version = "0.13", features = ["full"] }
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

## Isolated lifecycle simulation

The non-default `simulation` feature exposes a Rust host-only session builder:

```toml
anda_cognitive_nexus = { version = "0.13", features = ["simulation"] }
```

```rust
let simulated = nexus.system_session()
    .with_simulated_lifecycle_time("2030-01-01T00:00:00Z")?;
simulated.expire_lapsed_assertions(DEFAULT_SPACE, 100).await?;
simulated.sweep_expired(DEFAULT_SPACE, RetentionAction::Archive, 100).await?;
```

Only a direct engine system session may set this normalized RFC3339 value.
It is local to that session and its clones, not global Nexus state. It changes
only the eligibility time of these two sweeps, including the Assertion
per-record expiry recheck. Legal holds and all operation permissions remain in
force. Normal KIP requests have no clock-control field; other sessions retain
the real clock.

Use an isolated store: the sweeps still persist real lifecycle changes.
Authentication, policy/Grant validity, task leases, the default KQL/BELIEF
time, transaction timestamps and Governance audit always retain real time.
Queries that mean a simulated world-valid time must explicitly use `FOR TIME`.
Without `simulation` the builder and its session field do not exist, and the
ordinary expiry behavior is unchanged.

The separate `Session::with_simulated_evaluation_time(at)` builder advances only
`EvaluationRecord.cutoff` admissibility for isolated learning experiments. It has
the same direct engine-system restriction, cannot be enabled by request fields,
and does not change sweep eligibility, observer timestamps, audit, authority or
lease clocks. Trial/Attempt ordering, the complete ledger, native replay and CAS
remain enforced. Future-cutoff records are simulation evidence, not production
observations. Ordinary sessions still reject future cutoffs.

## Technical Reference

- [docs/anda_cognitive_nexus.md](../../docs/anda_cognitive_nexus.md) — the engine
- [docs/anda_kip.md](../../docs/anda_kip.md) — the protocol layer
- [docs/anda_db.md](../../docs/anda_db.md) — the storage core

`anda_cognitive_nexus_server` serves this crate over HTTP/JSON-RPC. MIT licensed.

## Upgrading a v1 store

Migration is automatic on first open after host Schema activation. Stop the old
writer, back up and rehearse on a copy first; collection replacement is one-way.
See [migration mapping, checkpoints and limitations](../../docs/kip-v1-migration.md).
