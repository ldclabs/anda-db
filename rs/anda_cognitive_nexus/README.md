# anda_cognitive_nexus

[![Crates.io](https://img.shields.io/crates/v/anda_cognitive_nexus.svg)](https://crates.io/crates/anda_cognitive_nexus)
[![Docs.rs](https://docs.rs/anda_cognitive_nexus/badge.svg)](https://docs.rs/anda_cognitive_nexus)

Tracks KIP at `11a82ec`, with the draft memory package
`kip://profiles/cognitive-memory@2.0.0` (content digest `sha256:734aa0fd…`). See the
[Cognitive Nexus documentation](../../docs/anda_cognitive_nexus.md) for implemented contracts and capability boundaries.

The reference **KIP 2.0** Cognitive Nexus — a persistent memory brain for AI
agents. [`anda_kip`](../anda_kip) parses, classifies and validates; this crate
is everything that needs state, and implements `anda_kip::Executor` on top of
[`anda_db`](../anda_db).

The KIP 1.x engine that used to live here was **deleted, not ported**. 2.0 is a
different data model — a Proposition existing is not the Proposition being true
— and a renamed 1.x engine would have been a worse lie than an absent one.
Existing 1.x stores are migrated on open (see [Upgrading](#upgrading)).

## What this crate provides

The five Core element kinds — Concept, Proposition, Assertion, Evidence,
Activity — get one `anda_db` collection each. A Proposition is a truth-neutral
tuple; an **Assertion** is one actor's commitment about it, carrying a stance, a
mode, a confidence and its Evidence. What is *currently believed* is projected
from those Assertions under a named policy and never stored — so correcting a
claim writes a new Assertion and a supersession link, not a rewritten row.

- **KML** in real transactions: creation, `ENSURE`, `UPSERT`, `UPDATE`,
  `MERGE CONCEPT`, the Assertion and Evidence lifecycles, retention and removal,
  the Space draft vocabulary (`DEFINE`), handles, preconditions, receipts and
  dry runs;
- **KQL**: element, tuple and structural patterns, hop-quantified traversal,
  `FILTER`, `NOT` / `OPTIONAL` / `UNION`, the Search Pattern, aggregates,
  paging, and two time axes kept apart: `FOR TIME` (what was true then) and
  `AS OF SEQ` (what this Brain held then, reconstructed from the version log
  rather than approximated);
- the **Epistemic Projection** behind `BELIEF` and `BELIEF SLOT`, under a named
  versioned policy with an explanation ledger: silence is `insufficient`, not
  `rejected`, and repetition is not corroboration;
- **META** — `DESCRIBE` (including `DESCRIBE SNAPSHOT`, which resolves a
  timestamp to the coordinate an `AS OF SEQ` read can use), `LIST`, `SEARCH`
  (keyword, scoped by authority), `VALIDATE`, `PREVIEW`, `HISTORY`, `CHANGES`
  — plus `EXPORT CAPSULE` / `VERIFY CAPSULE`, with Capsule import as a host API;
- **Governance** in a separate control plane — Principals, groups, Grants,
  Delegations, ActorBindings, versioned Policies, approvals and an
  append-preserving audit — authorizing every command and every element it
  touches under default deny, and reachable from no KML clause;
- **Schema Packages**, immutable versioned artifacts resolved through a per-Space
  Schema Environment. In 1.x schema was graph state, so an ordinary write could
  change what a type meant; here every `schema_ref` names an exact version;
- protected host APIs for Brain runtimes: recording repair, the exposure log,
  durable Watch handoff, wake leases and dispatch, contextual trust and
  calibration (see [Host integration](#host-integration)).

## When to use it

Use it when memory must record who claimed what, on what evidence, and what
changed when, and when disagreement has to stay recordable rather than resolved
by the last writer — and to get the reference KIP engine rather than write an
`Executor` yourself. For documents and retrieval, use `anda_db` directly.

## Getting started

```toml
[dependencies]
anda_cognitive_nexus = "0.14"
anda_kip = "0.14"
anda_db = "0.14"
object_store = "0.14"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

```rust
use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    profiles::{COGNITIVE_MEMORY, GENERAL_DOMAIN},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{TopLevelStatus, execute_kip};
use object_store::memory::InMemory;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: "brain".into(),
            description: "Agent memory".into(),
            ..Default::default()
        },
    )
    .await?;
    let nexus = CognitiveNexus::connect(Arc::new(db)).await?;

    // Core declares no Concept types: put Schema Packages in force first.
    nexus
        .install_and_activate(
            &[("bundled", COGNITIVE_MEMORY), ("bundled", GENERAL_DOMAIN)],
            DEFAULT_SPACE,
        )
        .await?;

    // Record an attributed claim. The Proposition is truth-neutral; the
    // Assertion commits to it with an actor, a mode and a confidence.
    let (_, write) = execute_kip(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?sz { TYPE "Place" NAME "Shenzhen" }
            ASSERT ?a (?alice, "lives_in", ?sz) {
                by: ?alice, mode: "stated", confidence: 0.9
            }
        }"#,
        false,
    )
    .await;
    assert_eq!(write.status, TopLevelStatus::Succeeded, "{write:#?}");

    // What is believed is projected from Assertions under a named policy.
    let (_, read) = execute_kip(
        &nexus,
        r#"FIND(?o.name, ?b.status)
           WHERE {
               ?p PROPOSITION ({type: "Person", name: "Alice"}, "lives_in", ?o)
               ?b BELIEF (?p)
           }"#,
        false,
    )
    .await;
    // [["Shenzhen","accepted"]]
    println!("{}", serde_json::to_string(&read.results[0].result)?);

    nexus.close().await?;
    Ok(())
}
```

For durable storage, connect `AndaDB` to a local or cloud `object_store`
backend as shown in the [anda_db README](../anda_db/README.md).
`anda_kip::execute_request` runs a full request envelope (named Space,
several operations, ingest, idempotency key, preconditions, deadline).

### Bundled packages

`anda_cognitive_nexus::profiles` exposes the Schema Package artifacts:

| Constant                 | Package                                         |
| ------------------------ | ----------------------------------------------- |
| `COGNITIVE_MEMORY`       | `kip://profiles/cognitive-memory@2.0.0` (draft) |
| `GENERAL_DOMAIN`         | `kip://domains/general@1.0.0`: places, organizations, topics and everyday relations |
| `MEMORY_DEFAULT_POLICY`  | the `kip:memory-default` projection policy artifact (§21.13) |
| `STRENGTH_HALF_LIFE_30D` | the `kip:strength-half-life-30d` mnemonic strength policy (§59.1) |

`install_and_activate` puts *exactly* the given packages in force and only
mints a new Schema Environment version when the lock changes.

### Callers and authority

Executing directly against the `CognitiveNexus` runs as the system Principal
that owns the default Space: a real authorization through the same path, not a
bypass. A host serving separately authorized callers authenticates them
itself, builds a `governance::AuthContext` from authenticated transport state —
never from the request body, which an Agent under prompt injection controls —
and executes through `CognitiveNexus::session(auth)`.

## What it does not do yet

`DESCRIBE CAPABILITIES` is the machine-readable answer: every gap as structured
data with a reason, so an Agent reads what is missing instead of discovering it
by triggering an error — or, worse, reading an absent feature as an absent fact.
Gaps are refused as `UnsupportedCapability`:

```text
semantic / hybrid SEARCH         no embedding model
SEARCH ... AS OF SEQ             the index reflects the present only
atomic multi-operation batches   operations[] is a batch, not a transaction
Capsule signatures               nothing is signed
the "restore" import mode        identity continuity is not modelled
Space-level retention defaults   retention is set per element
```

Host methods do not install a scheduler, executor or independent observer, and
installing a Schema Package does not advertise a complete Brain or the optional
Memory Interface: a Brain that serves the binding declares it with
`CognitiveNexus::set_host_capabilities`, and a raw Nexus answers
`memory_interface: false`.

Recording repair (`Session::repair_recording`, §57.8) and the exposure log
(`Session::record_exposures` / `read_exposures`, §66.8) are built. The engine
still claims `KIP-Core` only: the computed GradingState view and lineage fields
are refused on write but not computed on read, and selection dependencies
(`DependencyBasis.queries`, §57.7) are not evaluated.

## Timestamp inputs

KIP §6.5 requires valid UTC timestamps with exactly three fractional digits:
`YYYY-MM-DDTHH:mm:ss.SSSZ`, including `.000Z` for whole seconds. Invalid
strings return `ConstraintViolation`; non-string timestamps return
`TypeMismatch`. Optional/null values follow each field's contract. The engines
validate inputs without padding fractions or converting offsets; generated
clock values truncate to milliseconds. Use `space_seq` for commit order.

## Host integration

These `Session` APIs are for trusted Brain hosts. For Rust/TypeScript
signatures and record ordering, see
[the host-contract guide](../../docs/anda-brain-nexus-contracts.md)
([中文](../../docs/anda-brain-nexus-contracts.zh.md)).

### Durable Watch handoff

`Session::advance_watch` commits the Watch's fired state, a `watch_fire`
Activity and a protected wake record in one redo plan. Delta keys use the actual
matching envelope sequence; silence keys use the generation and normalized
deadline, with a fixed `due_seq` coverage target. Newer traffic cannot extend
that target indefinitely. Identical arm/advance retries return the retained
receipt, including after reopening the database; replay does not re-arm or renew
anything. The `watch_fire:` Activity client-key namespace is reserved for this
native operation.

The response retains `status`, `watch`, `coverage` and `receipt`; fired responses
also contain `fire_key`, `fire_activity_ref` and `wake_ref`. Wake records are
protected runtime state, not SleepTask or LeaseState Facets. Existing SleepTask
APIs remain available.

| Method | Contract |
| --- | --- |
| `arm_watch` / `rearm_watch` | Start a new generation; rearm may replace condition/deadline. Ordinary KML cannot change a retained Watch's deadline/class or protected progress. |
| `set_attention_config` | Requires `manage_policy`; pins host scope/policy/evaluator/binding identities. The instance cannot be replaced in place. Pins do not install executable code or grant permissions. |
| `read_wake` | Requires maintenance authority and current read access to Watch/fire; does not claim work. |
| `claim_wake` / `renew_wake` | Exact record version and fence; real-time lease of at most five minutes. Takeover after expiry increments the fence. |
| `block_wake` / `resume_wake` | Persist reason and retry condition. Timed retries resume when due; `on_change` invokes explicitly registered Rust verifier code outside the write lock, then rechecks the wake. |
| `finish_wake` | Commit a bounded KML output block, continuation wakes and terminal receipt together; reject expired/stale leases and unresolved dispatches. |
| `cancel_wake` | Fence future work and retain its receipt and any unresolved dispatch obligation. |
| `begin_wake_dispatch` | Recheck the live lease, original act/attempt, revision authority, policy and dependencies. Persist intent before returning dispatch; retries return lookup/unknown unless the actual binding supports idempotency. |
| `reconcile_wake_dispatch` | Requires an authorized terminal Outcome for the same attempt; no executor ACK is an Outcome. |
| `list_wakes` | Bounded snapshot pagination with scope/instance/basis cursors; empty intermediate pages remain resumable. Discovery does not claim processing coverage. |
| `set_dispatch_lookup_observer` / `reconcile_wake_lookup` | Pin an authenticated lookup authority before dispatch. Only its current, CAS-guarded NotStarted receipt can reopen the same dispatch identity; Finished is not a success Outcome. |
| `prepare_watch_page` / `read_prepared_watch_page` / `commit_watch_page` | Persist an authorized semantic page, evaluate outside the lock, then verify exact candidates and current generation/basis before atomic advancement. |

Generic `read_control` also checks maintenance and source access for wake and
wake-dispatch records. Prepared pages and operation receipts use their dedicated
read or replay APIs; evaluation material remains subject to source access.

The initial Watch wake uses `anda-brain:attention-v1`; follow-up work uses the
separate `anda-brain:attention-continuation-v1` format and retains its parent.
An armed silence Watch must have a deadline; `rearm_watch` rejects a missing one.
Dispatches that declare outcome lookup require a registered lookup observer.
Completion accepts up to 64 KiB of KML, 128 clauses/outputs and 16 continuations.
Current authorization and the lease fence are checked before the redo commit.

Text and mixed selectors require a pinned evaluator. The two-phase API accepts
up to 200 envelopes, 512 candidates and 512 KiB per prepared page. The returned
candidate IDs must each receive exactly one match/no-match/unknown judgment;
each judgment includes a nonempty rationale of at most 4 KiB. Candidates include
authorized before/after Core views at the event sequence, not current values or
belief projections. There is no caller-authored `complete` flag. Unknown results
retain the original Watch checkpoint. A page prepared before its deadline cannot
later claim deadline coverage merely because model evaluation took time.
Material is retained as a governed artifact, as are judgment rationales (at most
512 KiB per evaluation), so erasure revokes them instead of leaving plaintext in
the transaction replay. Legacy synchronous `advance_watch_with` remains
available; plain `advance_watch` has no semantic evaluator. Hosts supply actual
model calls, global due scheduling and outward delivery.

Evaluation control records contain a material pin readable with `read_artifact`;
their content remains subject to current source permissions and erasure. A
NotStarted lookup must be observed after the latest dispatch intent, so delayed
observations from before a resend cannot reopen it again under a new event key.

Register `WakeResumeVerifier` implementations with
`CognitiveNexus::register_wake_resume_verifier` once per live instance and after
restart. An absent verifier remains unsupported; a supplied boolean or stored
condition digest is not evidence that a condition holds. Cancellation or version
change during an asynchronous check prevents its later result from resuming work.

Scope instances must not be copied into independently executable forks. Keep one
live writer process per database. After uncertain native writes, read/reconcile
the same identity; historical receipts do not confer current execution authority.
Target-system authorization and idempotency must also be enforced by the actual
executor; Nexus atomicity alone does not imply exactly-once external effects.

### Contextual trust and atomic calibration provenance

`trust::TrustConfiguration` retains global actor weights and optionally adds up
to 128 explicit actor/predicate/context rules. Contexts are visible Concept refs;
predicates use exact Schema refs. Rules only apply within their declared scope.
The most specific matching rules win, and conflicting equally specific weights
produce an error rather than depending on array order. BELIEF records the context
and protected trust version; Assertion confidence is never changed.

`Session::set_contextual_trust` explicitly replaces a complete configuration and
requires `manage_trust`. The legacy `set_trust` updates global weights while
retaining scoped rules. Both use version CAS. `apply_trust_calibration` consumes a
governed `TrustCalibrationProposal` artifact with the exact configuration, method
pin, eligible Evidence refs and explicit uncertainty. It commits trust, proposal
references and the native Governance audit together and replays one receipt after
an uncertain response. It needs `manage_trust` and material read access; no broad
cognitive `create` permission is inferred just to write the Governance audit.

These APIs do not estimate causal credit, run calibration or automatically adopt
trust suggestions. The Brain host owns those decisions and must keep them opt-in.

### Isolated lifecycle simulation

The non-default `simulation` feature exposes a Rust host-only session builder:

```toml
anda_cognitive_nexus = { version = "0.14", features = ["simulation"] }
```

```rust
let simulated = nexus.system_session()
    .with_simulated_lifecycle_time("2030-01-01T00:00:00.000Z")?;
simulated.sweep_expired(DEFAULT_SPACE, RetentionAction::Archive, 100).await?;
```

Only a direct engine system session may set this canonical UTC millisecond value.
It is local to that session and its clones, not global Nexus state. It changes
only the eligibility time of the retention sweep. An Assertion's `expired` is
computed from world time at every read and never stored (§14.3), so no clock
moves it. Legal holds and all operation permissions remain in force. Normal KIP
requests have no clock-control field; other sessions retain the real clock.

Use an isolated store: the sweep still persists real lifecycle changes.
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

## Operations

- **One live writer per database.** Share one `CognitiveNexus` (it is
  `Clone`) instead of opening the same store twice.
- **Let opens and writes finish.** Opening a store can recover prepared
  commits, migrate a 1.x layout or upgrade collection schemas; do not cancel
  it. After a host drains cancelled workers, `CognitiveNexus::recover` reopens
  any handle a cancelled operation poisoned.
- **Stack use.** Heavy APIs return boxed futures, so opening a Nexus or running
  a KIP command fits a spawned thread's stack even in debug builds (about
  170–270 KiB); `tests/stack_budget.rs` holds the line.
- **Close** with `nexus.close().await` to flush the underlying database.

## Upgrading

- **From 0.14.0.** The first open by 0.14.1 or newer upgrades the Concept,
  Proposition, Assertion, element-version, Evidence and Activity collection
  schemas and builds the `query_keys` and `lookup_key` indexes over existing
  rows. Do not reopen an upgraded store with 0.14.0. Struct literals of the
  `store::rows` types need the new optional fields or `..Default::default()`,
  and a custom `IndexHooks` that derives a B-Tree key from other columns must
  override `btree_index_depends_on`.
- **From KIP 1.x.** Migration is automatic on first open after host Schema
  activation. Stop the old writer, back up and rehearse on a copy first;
  collection replacement is one-way. Stores migrated by 0.14.2 or earlier are
  repaired once on open so that durable records do not inherit an Event's
  expiry. See the
  [migration mapping, checkpoints and limitations](../../docs/kip-v1-migration.md).
- Spaces activated under the earlier `cognitive-memory` 2.1.0 draft are not
  migrated; start a new Space.

The [changelog](../../CHANGELOG.md) has the details for each release.

## Testing and benchmarks

```bash
cargo test -p anda_kip -p anda_cognitive_nexus
cargo test -p anda_cognitive_nexus --test conformance
cargo test -p anda_cognitive_nexus --test coverage -- --nocapture
ANDA_NEXUS_BENCH=1 cargo bench -p anda_cognitive_nexus --bench query_scale
```

`tests/conformance.rs` runs the shared
[KIP engine suite](../../fixtures/kip-conformance-2.0) that `@ldclabs/kip-do`
also runs, and `tests/coverage.rs` reports the §102 invariant coverage matrix.
KIP command strings in this crate's `src/` and `tests/` feed the TypeScript
parser-oracle corpus: run `pnpm run codegen` in [`ts/kip-do`](../../ts/kip-do)
after adding or editing one. Query-scale measurements are in
[docs/benchmarks](../../docs/benchmarks/anda_query_scale_2026-09-26-rerun/README.md)
and the [million-row report (中文)](../../docs/query-performance-million.zh.md).

## Technical reference

- [docs/anda_cognitive_nexus.md](../../docs/anda_cognitive_nexus.md) — the engine
- [docs/anda_kip.md](../../docs/anda_kip.md) — the protocol layer
- [docs/anda_db.md](../../docs/anda_db.md) — the storage core
- [docs/kip-do-nexus-parity.md](../../docs/kip-do-nexus-parity.md) — parity
  with the TypeScript engine

## Related crates

- [`anda_kip`](../anda_kip): the protocol SDK
- [`anda_cognitive_nexus_server`](../anda_cognitive_nexus_server): this engine
  over HTTP/JSON-RPC
- [`anda_cognitive_nexus_py`](../../py/anda_cognitive_nexus_py): Python binding
- [`@ldclabs/kip-do`](../../ts/kip-do): the independent TypeScript engine

## License

MIT. See [LICENSE](../../LICENSE).
