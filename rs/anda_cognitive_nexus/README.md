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
Gaps are refused as `UnsupportedCapability`. Host methods do not install a
scheduler, executor or independent observer, and installing a Schema Package
does not advertise a complete Brain or the optional Memory Interface.

## Durable Watch handoff (0.13.1)

`Session::advance_watch` now commits the Watch's fired state, a `watch_fire`
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

Trusted hosts use these `Session` methods:

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
belief projections. There is no caller-authored `complete` flag. Unknown results retain the original
Watch checkpoint. A page prepared before its deadline cannot later claim deadline
coverage merely because model evaluation took time. Material is retained as a
governed artifact, as are judgment rationales (at most 512 KiB per evaluation),
so erasure revokes them instead of leaving plaintext in the
transaction replay. Legacy synchronous `advance_watch_with` remains available;
plain `advance_watch` has no semantic evaluator. Hosts supply actual model calls,
global due scheduling and outward delivery.

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

## Contextual trust and atomic calibration provenance

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
For Brain host integration contracts, see
[the host-contract guide](../../docs/anda-brain-nexus-contracts.zh.md).

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
