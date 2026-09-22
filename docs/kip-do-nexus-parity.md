# Nexus → kip-do Parity Audit

[中文版](kip-do-nexus-parity.zh.md)

Audit scope: `de2433b^..d9cdeb3`, inclusive of `de2433b`. The verification is based on the Rust implementation, tests, commit diffs, and the TypeScript implementation; no changes were made to the Rust engine, KIP syntax, or protocol version.

## Findings and Remediation

Host capabilities added in `de2433b` had not been synchronized to kip-do alongside the timestamp format and parser corpus updates. The prior Watch implementation only updated `WatchState`, failing to guarantee the durable handoff semantics promised by Rust. This update aligns the following behaviors, using SQLite `transactionSync` to provide the atomic commit guarantees fulfilled by Rust's redo plan.

| Scope | Prior kip-do State | Remediation |
| --- | --- | --- |
| Watch handoff | State updates only, no Activity/wake | Single transaction commits fire identity, Activity, wake, checkpoints, and replay results |
| arm/rearm | No fixed checkpoints; no protected rearm for replacement conditions | Pin generation, conditions, deadlines, configs, and observation provenance; preserve provable legacy arm history |
| silence coverage | Scan target drifted with head | Pin inclusive `due_seq`, maintain watermarks, correctly segment pre-deadline, boundary, and post-deadline mutations |
| Text & hybrid conditions | Evaluator unpinned; mixed text conditions could be skipped | Require pinned evaluator; evaluate text rules after structural match; errors do not consume coverage |
| Asynchronous evaluation | Missing | 3-step prepare/read/commit; complete candidates with before/after history; defer `unknown`; revocable artifacts |
| wake state machine | Missing | Version/fence checks, 5-minute wall-clock lease, block/resume/cancel/finish, idempotent receipts |
| Completion & continuations | Missing | Atomic commit of KML output, up to 16 child wakes, and terminal receipt; entire batch rolls back on failure |
| wake paging & resume verifier | Missing | Bounded snapshot paging; resumption requires registered handlers; re-checks version, permissions, and provenance post-await |
| wake dispatch | SleepTask dispatch only | Reuse Attempt/Decision/dependency checks, validate Watch inputs; verify bindings and observer prerequisites |
| Lookup & terminal reconciliation | Missing | Directly authenticated observer, CAS, observation timestamp and config validation; `not_started` is retryable, `finished` does not imply success |
| Protected reads (`ceefc4f`) | No corresponding attention records | Dedicated reads and general `readControl` protect wake/dispatch/eval artifacts; advancing requires `read_history` |
| Contextual trust | Global weights only | Specificity precedence, reject conflicts at equal specificity, historical projections, preserve rules in `setTrust` |
| Calibration proposal | Missing | Validate scope, method, Evidence, provenance, and uncertainty; atomic commit of config, provenance, audit, and receipts |
| Native transaction classification | Assumed cognitive writes present | Control-only commits classified as service/governance; internal wake records do not alter control provenance |

## Synchronized and Engine-Specific Changes

- The standard millisecond UTC timestamp format, error type classifications, Profile/schema validations, and Governance temporal conditions from `5d0454c` were previously synchronized to kip-do; existing tests were retained and executed.
- Commits `e2a2c05` and `d9cdeb3` primarily updated the parser oracle corpus, with generated files already in place.
- Commits `033a784` and `747c890` involve AndaDB B-Tree index layout, sparse values, and query planner optimizations. Because kip-do uses SQLite, it does not replicate the B-Tree implementation; local compound indexes were added for wake discovery.
- Commits `72bc0b2` and `07fa988` repaired existing wide fields in AndaDB and addressed Rust KIP 1.x migrations (large batches of staged rows, legacy SleepTask states, Commitment completed->fulfilled mapping, and legacy migration fixes). kip-do does not manage these AndaDB data files or Rust migration pathways; no direct porting was necessary, and no new KIP 1.x importer was introduced.

## Verification

- [Watch handoff regressions](../ts/kip-do/test/watch-handoff.test.ts): SQLite failure rollbacks, reconnection, idempotent replay, output/continuation atomicity, lease takeover with terminal checks, permission and asynchronous cancellation races, evaluation completeness, artifact revocation, historical generations, and legacy arm history.
- [Deadlines and dispatch boundaries](../ts/kip-do/test/attention-boundaries.test.ts): Deadline boundaries, new mutations under bounded paging, pinned evaluation time, non-idempotent redelivery prevention, missing binding/observer enforcement, directly authenticated lookups with CAS, and post-cancellation terminal reconciliation.
- [Contextual trust regressions](../ts/kip-do/test/contextual-trust.test.ts): Active and historical `BELIEF`, rule precedence and ambiguity, calibration provenance, audit/configuration failure rollbacks, calibration without `create` permission, and evidence corrections.
- Calibration and both wake reconciliation pathways verify replay after one-time approvals are consumed: replay consumes no pending approvals and does not advance commit sequence; new operations still require approval, and replay is refused if current permissions are denied.
- `make test-ts` executes all conformance suites, parser oracle tests, typechecks, and the regressions noted above; `pnpm run build` verifies distribution entries and type declarations; `pnpm run codegen` checks generated code consistency.

The host environment remains responsible for scheduling, external executors, asynchronous evaluation, and verifier registration. Durable receipts do not guarantee exactly-once external side effects. For API parameter contracts and integration sequencing, see [Brain host contracts](anda-brain-nexus-contracts.md) and [kip-do README](../ts/kip-do/README.md#durable-watch-handoff).
