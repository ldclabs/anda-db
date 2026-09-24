# Anda Brain Nexus Integration: KIP CognitiveMemory 2.1

[中文版](anda-brain-nexus-contracts.zh.md)

This implementation corresponds to KIP commit `597db44`. The protocol version remains KIP 2.0, with standard package `kip://profiles/cognitive-memory@2.0.0` (content digest `sha256:734aa0fd…`; the draft rewrote 2.0.0 in place, so Spaces activated under the earlier 2.1.0 draft are not migrated). Both the Rust and SQLite/Durable Object engines provide the interfaces below. Retrieval policies, scheduling loops, tool adapters, and the 5-intent Memory Interface are integrated by Anda Brain; the database enforces permissions, references, versions, transactions, and record validity.

All protocol timestamp inputs must adhere strictly to `YYYY-MM-DDTHH:mm:ss.SSSZ`, where whole seconds must include `.000Z`. Non-canonical strings (including timezone offsets and invalid calendar dates) yield `ConstraintViolation`; non-string values like numbers return `TypeMismatch`. Omission or `null` is allowed only when permitted by the field contract. Host interfaces, profile fields, and query time arguments follow the exact same rules. Engine-generated timestamps are truncated to milliseconds and never coerce client input; commit ordering is governed by `space_seq` and must not rely on timestamp uniqueness.

## Host Initialization

1. Open `CognitiveNexus` and activate the standard profile package.
2. Register necessary deterministic evaluators at startup, then publish rules, parameters, and `EvaluationPolicy` through a Session holding `manage_policy` permissions. Rule registration is trusted host code and cannot execute arbitrary code from KIP statements or uploaded payloads.
3. Grant Brain only the actual permissions required. Derived writes require `derive`; independent observers typically require `create`, `read`, `derive`, and `record_outcome`. Observer-decision controller relationships are declared in protected `EvaluationPolicy` objects and cannot be inferred merely from differing actor names.
4. All calls must supply host-authenticated `AuthContext`. Never synthesize identity from model-generated request payloads.

The Rust `anda_kip::cognitive` module and TypeScript package root export `ArtifactPin`, `EvaluationPolicy`, `ObserverControl`, `EvaluationInput`, `EvaluationRule`, and `DispatchRequest`. Rust evaluators are `Arc<dyn EvaluationRule>`; pure function closures also implement this trait.

| Operation | Rust Session | TypeScript Session |
| --- | --- | --- |
| Set projection policy | `set_projection_policy(space, name, expected_version, settings)` | `setProjectionPolicy(name, expected, settings, space?)` |
| Set protected trust weights | `set_trust(space, expected, weights, default_weight)` | `setTrust(expected, weights, defaultWeight?, space?)` |
| Store immutable JSON artifact | `put_artifact(space, content, source_refs)` | `putArtifact(content, sourceRefs, space?)` |
| Read artifact | `read_artifact(space, pin)` | `readArtifact(pin, space?)` |
| Publish evaluation policy | `set_evaluation_policy(space, expected, policy)` | `setEvaluationPolicy(expected, policy, space?)` |
| Withdraw identity merge | `withdraw_identity(space, decision_id, expected_identity_version, reason_evidence)` | `withdrawIdentity(decisionId, expectedIdentityVersion, reasonEvidence, space?)` |
| Change page with integrity watermarks | `change_page(space, after, limit)` | `changePage(after, limit?, space?)` |
| Lease, renew, or take over task | `lease_task(space, task_ref, expected_version, expires_at)` | `leaseTask(ref, expected, expiresAt, space?)` |
| Rearm Watch | `arm_watch(space, watch_ref, expected_version)` | `armWatch(ref, expected, space?)` |
| Advance Watch | `advance_watch(space, ref, expected, generation, limit)` | `advanceWatch(ref, expected, generation, limit?, space?, evaluator?)` |
| Enqueue dispatch intent | `enqueue_dispatch(space, request)` | `enqueueDispatch(request, space?)` |
| Pre-dispatch check | `begin_dispatch(space, attempt_id, expected_intent_version, fencing_token)` | `beginDispatch(attemptId, expected, fencingToken, space?)` |
| Validate erasure plan | `validate_erasure_plan(space, plan)` | `validateErasurePlan(plan, space?)` |
| Reconcile dispatch with outcome | `reconcile_dispatch(space, attempt_id, expected, outcome_ref)` | `reconcileDispatch(attemptId, expected, outcomeRef, space?)` |
| Promote a draft symbol (`manage_schema`) | `promote_draft_symbol(space, kind, from, to)` | `promoteDraftSymbol(kind, from, to, space?)` |
| Import a Capsule mapping source draft symbols (Rust only) | `import_capsule_mapped(space, capsule, isolate, symbols)` | — |

Draft vocabulary (§20.16): grant the Brain's agent Principal `propose_schema` and it can run `DEFINE PREDICATE` / `DEFINE CONCEPT TYPE` as standalone commands; each answers `{ref, schema_environment_version}` and a taken name fails `SchemaSymbolConflict` (retry with an idempotency key, never by redefining). `propose_schema` confers no `manage_schema`: queue a `review_schema` SleepTask keyed `review_schema:<kind>:<exact ref>`, and let the owner promote through the host API above. The draft package survives every `ensure_schema` / `activatePackages` call.

The attention/trust host APIs below are implemented in both Rust and kip-do. In the TypeScript table, all methods take an optional trailing `space?`; corresponding Rust methods take `space` as the first argument in `snake_case`.

| Operation | TypeScript Session |
| --- | --- |
| Configure attention scope, policy, evaluators, and dispatch binding digest | `setAttentionConfig(expected, config, space?)` |
| Replace conditions and deadline under a new generation | `rearmWatch(ref, expected, condition, dueAt, space?)` |
| Prepare semantic evaluation page | `prepareWatchPage(ref, expected, generation, limit, preparationKey, space?)` |
| Read and commit evaluation page | `readPreparedWatchPage(ref, space?)` / `commitWatchPage(ref, evaluation, space?)` |
| Read and list wakes | `readWake(ref, space?)` / `listWakes(cursor?, scanLimit?, space?)` |
| Claim, take over, or renew wake | `claimWake(ref, expected, fence, expiresAt, space?)` / `renewWake(...)` |
| Block and resume wake | `blockWake(ref, expected, fence, retry, space?)` / `resumeWake(ref, expected, fence, space?)` |
| Cancel wake | `cancelWake(ref, expected, fence, reason, space?)` |
| Atomically commit output and continuations | `finishWake(ref, expected, fence, command?, parameters?, continuations?, space?)` |
| Pre-dispatch verification for wake | `beginWakeDispatch(ref, expected, fence, attemptRef, supportsIdempotency, supportsOutcomeLookup, space?)` |
| Register directly authenticated lookup observer | `setDispatchLookupObserver(expected, observer, space?)` |
| Reconcile wake lookup and final outcome | `reconcileWakeLookup(ref, expected, observation, space?)` / `reconcileWakeDispatch(ref, expected, outcomeRef, space?)` |
| Configure contextual trust | `setContextualTrust(expected, configuration, space?)` |
| Apply trust calibration proposal with provenance | `applyTrustCalibration(expected, proposalPin, operationKey, space?)` |

Custom evaluators are registered on the Nexus instance: `register_evaluation_rule(artifact, evaluator)` in Rust, `registerEvaluationRule(artifact, evaluator)` in TypeScript. They bind to the SHA-256 digest of canonicalized rule content; identical digests cannot be rebound within the same instance. Registration must be re-executed on restart; historical artifacts do not load code automatically.

## Skills and Learning Records

`Skill` holds a stable identity and `current_revision`; `SkillRevision` stores immutable behavior and `revision_of`. Both entities support bidirectional reference creation within a single `MUTATE` block. The behavior digest covers all fields in `attributes` except `behavior_digest` itself, computed via `kip-jcs-safe-v1` canonical JSON and SHA-256. The digest format is `sha256:<64-hex-lower>`. Rust invokes `content_digest` at the package root; TypeScript calls `contentDigest`.

```text
MUTATE {
  CREATE CONCEPT ?skill {
    TYPE "Skill"
    SET ATTRIBUTES {skill_class: "workflow", summary: "Validate before execution", status: "proposed"}
    SET STRUCTURAL {("current_revision", ?revision)}
  }
  CREATE CONCEPT ?revision {
    TYPE "SkillRevision"
    SET ATTRIBUTES {
      task_family: "deploy/preflight",
      procedure: "run the preflight check",
      behavior_digest: :digest
    }
    SET STRUCTURAL {("revision_of", ?skill)}
  }
}
```

Switching to a new revision requires a CAS commit, resetting status to `proposed` and clearing `current_trial` and `current_evaluation`; `GradingState` is a computed, read-only view of the current evaluation. Non-structural annotations do not reset status. Creating a revision does not mean dependencies are verified; automated execution requires either a production Activity with a `DependencyBasis` or explicit dependency revalidation.

The learning lifecycle commits in the following order:

1. `trial_open` + `TrialRecord`: Freezes revision/bundle, rules, parameters, baseline samples, stratified weights, independent sampling units, quotas, observation windows, missing-data policy, evaluation policy version, and replay artifacts.
2. `action_gate` + `DecisionRecord`: Distinguishes `retrieved`, `used`, and `applied`. Every applied revision must appear in `inputs`. When a `DependencyBasis` is attached, both records must share the identical read basis.
3. `action_attempt` + `AttemptRecord`: Persists a space-unique `attempt_id` prior to executing the action. The trial must precede the decision; trial attribution cannot be backfilled post-observation.
4. `outcome_observation` + `OutcomeRecord`: Deduplicates on `observation_key` and links attempt and decision. Multiple observations for the same attempt/metric/window cannot count as separate independent samples.
5. `lifecycle_verdict` + `EvaluationRecord`: Commits alongside Skill status and caches. All modified attributes, structural links, and facet planes must carry version guards or share an overarching version guard.

`OutcomeRecord` is frozen upon attachment, including omitted optional members; corrections require new observation and correction edges. Imported Skills revert to `proposed`, active trial/grade caches are cleared, and external import sources cannot act as trusted local observers or enter dispatch directly.

Nexus re-evaluates references, trial and revision attribution, independent attempts, observer control, and original vs current policies, invoking registered evaluators to recompute comparisons. Promotion cannot occur via Activity name, manual comparison, or mutable success counters. Only `trialed -> adopted` promotions are valid, and they must satisfy independent sample quotas, non-negative effect thresholds, and rule comparison requirements. Emergency retractions permit zero samples; retaining adopted status under insufficient samples is governed by protected policy.

The built-in rule descriptor is `{"engine":"kip:binary-stratified-v1"}` with parameters `{"alpha":0.05}`. It supports stratified/randomized sampling, pre-declared strata weights, `count_as_failure` handling for missing data, and Hoeffding bounds. Paired, off-policy, or other evaluation algorithms must register deterministic evaluators that validate pairing, correlation, and propensity score contracts. Evaluators operate on verified database artifacts and cannot bypass independent sample quotas, effect minimums, observer permissions, or transactional guards.

## Replay Artifacts

Artifacts are canonical JSON. Do not treat artifact handles as URLs or filesystem paths; engines do not fetch external resources. Reading an artifact re-verifies permissions on all `source_refs`. Publishing rules/parameters without artifact sources requires `manage_policy`; deriving from existing artifacts requires `derive`. Identical content digests cannot be rebound to more permissive sources.

Trial replay content must include `rule`, `parameters`, `basis`, `baseline_attempts`, and `baseline_outcomes`. Evaluation replay content must include `rule`, `parameters`, `trial_record`, `attempts`, and `outcomes`, using reference strings as map keys:

```json
{
  "attempts": {
    "X-12": {"record": "Full AttemptRecord object", "principal_id": "committing_principal"}
  },
  "outcomes": {
    "E-8": {
      "record": "Full OutcomeRecord object",
      "status": "active",
      "corrected_by": [],
      "principal_id": "actual_observer_principal",
      "observed_at": "2026-09-07T00:00:00.000Z"
    }
  }
}
```

The example above illustrates map structure; `record` is a nested object. Both baseline maps in a Trial share this layout. Artifact `source_refs` must encompass all revisions, samples, and (for Evaluation) the parent trial. Concrete examples are maintained in the `nexus_contracts` (Rust) and `nexus-contracts` (TypeScript) integration tests.

## Dependencies and Identity

Production Activities record the actual read versions in their `DependencyBasis`; the engine captures input and final output versions. Reads recursively evaluate `all_of`, `any_of`, and `context` groupings; missing provenance cannot be treated as current. `dependency_validation` must complete and identify exact outputs; it cannot substitute for original premises in older Assertions. Mutated content requires a new version and an associated production Activity.

Every `MERGE` generates an `identity:<tx_id>:<source_id>` decision, readable via protected `read_control`. Retraction requires the current identity version and reason Evidence, returning the set of writes requiring review; historical tuples, Assertions, and resolution trails are retained. References whose intent cannot be recovered remain `needs_review`.

## Persistent Tasks and Change Streams

`SleepTask` is created as `pending` before leasing. Leases track owner, monotonic `fencing_token`, `expires_at`, and `attempt_count`. On task completion, the terminal state and output are committed in the same `MUTATE` using current version guards; expired or superseded leases cannot commit terminal states.

`Watch` is created `disarmed` before arming. Each arm increments `generation`. Advancing a watch commits the observed version and generation, persisting `consumed_seq`, `matched`, and status. Structural selectors combine with `AND`, while array elements in `ops`/`touched` use `OR`. Text and hybrid conditions require fixing the evaluator digest in `AttentionConfig` and supplying a Brain evaluator. Rust uses `advance_watch_with`; TypeScript passes an evaluator to `advanceWatch`.

`change_page` returns coverage even on empty pages. Silence watches fire only when `complete`, `authorization_view` matches, and the silence deadline has elapsed. Changes in permissions, control provenance, or gaps in verifiable control history require resynchronization and rearming. Rust control checkpoints detect interrupted control writes; pre-migration databases cannot synthesize missing historical control records.

Watch firing, a unique `watch_fire` Activity, wake records, and replay results commit in a single transaction. Silence deadlines fix `due_seq` to the last commit at or before the deadline; subsequent traffic does not expand the scan boundary. Asynchronous evaluation uses a 3-step prepare/read/commit protocol and must assess all candidates; `unknown` halts coverage watermark progression. Pages and rationale reside in revocable governance artifacts; once provenance is purged, they cannot be read or applied.

Wake leases have a maximum duration of 5 minutes, tracked using wall-clock time and monotonic fences. Completion output, continuations, and terminal receipts commit atomically. `readWake`, `listWakes`, and generic `readControl` verify `maintain` permission and full source visibility. Paging allows empty intermediate pages; clients must continue while `next_cursor` is present. Resuming `on_change` requires registering handlers after startup via `register_wake_resume_verifier` (Rust) or `registerWakeResumeVerifier` (TS). In TypeScript, `resumeWake` returns a Promise without holding a database transaction across awaits, re-validating state upon return.

Wake dispatch uses `beginWakeDispatch` and requires a registered execution binding. Declaring outcome lookup capabilities requires registering a directly authenticated observer in advance. A lookup result of `not_started` allows retries; `finished` does not synthesize a successful Outcome. All dispatch results must be resolved before completing a wake; reconciliation records remain preserved after cancellation.

Contextual trust rules match on actor, exact predicate schema ref, and context ref. Specific rules override generic rules; weight conflicts at equal specificity reject projection. `setTrust` preserves contextual rules. Calibration proposals require an explicit method, valid Evidence, inherited artifact provenance, and declared uncertainty. Application requires `manage_trust`; configuration, provenance, audit, and receipts commit atomically, without requiring `create` merely for native auditing.

Committed calibration and wake reconciliation requests can be replayed after a one-time approval is consumed. Replay re-checks current permissions and artifact visibility without requiring or consuming an additional approval. New operations remain subject to active policy approvals.

The dispatch sequence for `SleepTask` is: commit `AttemptRecord` -> `enqueue_dispatch` -> `begin_dispatch` -> tool execution. Tool adapter idempotency and lookup flags must come from host configuration, never from model output. Actual dispatch invocations use the returned `attempt_id` as the external idempotency key alongside the active lease token. Allowed action returns are `dispatch`, `lookup`, `outcome_unknown`, and `done`; without idempotency or lookup support, automatic redelivery on lost outcomes is prohibited. Adapter implementations remain responsible for tool permissions, runtime environment, and version checks.

## Memory Interface Host Contracts

`memory_interface`, `durable_brain_runtime` and `receiver_fencing` describe what the host serves around the Nexus, so a raw Nexus answers `false` for each. A Brain that serves them calls `CognitiveNexus::set_host_capabilities` (Rust) / `setHostCapabilities` (TypeScript) before sharing the Nexus. The declaration is process state, never persisted, and is checked against the engine: every advertised level must run on a conformance level the engine claims (both engines claim `KIP-Core`, so only `memory_basic` is accepted today), and `receiver_fencing` requires `durable_brain_runtime`. `DESCRIBE CAPABILITIES` then reports the descriptor as the `memory_interface` registry value, `DESCRIBE PRIMER` carries it under `extensions.memory_interface`, and request `requires` checks answer accordingly. Rust hosts build the descriptor and wire shapes from `anda_kip::memory::binding`, whose `MemorySession` keeps outstanding receipts and the attention cursor; the `schema-validation` feature validates requests, responses and descriptors against the vendored schema.

`Session::repair_recording` / `repairRecording` implements the Memory Interface `misrecorded` route (§57.8). The host passes a `RecordingRepair`: the captured source Evidence, its `content_digest`, a locator into the inline payload (a JSON Pointer or `bytes=<start>-<end>` over the payload text), the wrong Assertions with their current `_system.version`, and optional replacements. The caller needs `repair_recording` on each invalidated Assertion and must be the Principal that recorded it; replacements must already exist as the caller's own Assertions citing the same source. Their `asserted_at` preserves an original invalidated Assertion's claim time no later than the source's `observed_at`, or uses `observed_at` itself. If the original time was also misrecorded, `source_locator` can select the exact canonical timestamp from the source (a JSON string value or a byte range). Without `observed_at`, preserving the original claim time is still allowed. No timestamp is inferred from field names, and the repair's own time is never substituted: a historical message's claim time need not equal its later capture time. An `extraction_error` keeps the canonical actor after identity merges, while an `attribution_error` may change it. The repair commits a terminal `recording_repair` Activity (inputs: source and invalidated Assertions; the `RecordingRepair` Facet names the replacements), a `recording` control change, and a protected invalidation on each wrong Assertion. `_system.recording_validity` reports `{status, repair_ref}` on every Assertion; `repair_ref` and its governance counterpart are `null` unless the reader may currently discover the repair Activity. Projection excludes invalidated extractions, dependent derivations read `needs_review`, and history reads the state at its snapshot. A retry of the same repair returns `replayed: true` without writing. Neither engine maps a misrecording to a correction or a retraction.

`Session::record_exposures` / `recordExposures` is the §66.8 exposure log: the host records `retrieved` for items it returned and `used` (with the decision Activity) for a DecisionRecord's `used_refs`. Entries take no Space sequence, emit no Change Envelope entry and change no element; the engine supplies the Space, time and Principal, and the caller must be able to read each element. `read_exposures` / `readExposures` pages the log under `read_audit`, omitting elements the reader may not discover. Storage reads use batches of at most 256 rows or ids; a continuation is returned only when another visible entry exists. Maintenance folds use into strength only through an explicit, guarded `MnemonicState` write.

## Retention and Erasure

`ErasurePlan` is a Memory Interface reporting structure, not a standard package Facet. Brain invokes `validate_erasure_plan` / `validateErasurePlan` prior to reporting, verifying each target claimed `erased` and its `completed` dependency closure. Validation performs no deletions and does not advance the space sequence. A target with surface `exposure` is erased only when the element has no exposure entries left; purging an element removes them.

When sources are purged via `PURGE` or `PURGE PAYLOAD`, dependent replay content and storage versions are expunged, leaving non-content digest tombstones to prevent re-insertion. Deleting a production Activity retains non-content dependency edges, preventing surviving digests from being misidentified as erased due to missing provenance. `semantic_forgetting` marked `completed` validates the known dependency closure; operations cannot report completion if any targets remain unpurged, held, or unverified. Backup targets require backend verification; model-supplied status is not proof of deletion, and engines cannot recall uncontrolled external exports.

Rust recovers cross-collection cognitive state, protection states, and element governance updates via commit replay logs; SQLite ensures atomicity through native transactions. Writes are persisted before returning success; reboots replay committed logs prior to serving reads. Hosts must honor AndaDB's single-writer process constraint and configure appropriate `object_store` / `fsync` settings for local durability.
