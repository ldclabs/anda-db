# KIP 2.0 engine suite (vendored)

These fixtures are KIP's executable engine suite —
[`conformance/engine-suite/`](https://github.com/ldclabs/KIP/tree/main/conformance/engine-suite)
in the KIP repository — copied **byte for byte**. KIP owns them; this
directory is a pinned copy, not a second suite. The copy was taken at KIP
`11a82ec`, the protocol baseline too, and `manifest.json` records its
provenance and every revision made on import.

```bash
make sync-kip-conformance KIP_REPO=/path/to/KIP   # copy the suite from a KIP checkout
make check-kip-conformance KIP_REPO=/path/to/KIP  # fail if the copy has drifted
```

Never edit a fixture here. A case that looks wrong is fixed in KIP and synced
back; a local edit would make the two reference engines agree about something
KIP's runner does not check.

Both engines run the same bytes:

- `rs/anda_cognitive_nexus` — `tests/conformance.rs` reads this directory;
- `ts/kip-do` — `test/conformance.test.ts`, via
  `test/conformance/fixtures.generated.ts`, which `pnpm run codegen:fixtures`
  inlines from these files (tests run inside workerd, which has no filesystem).
  Regenerate and commit after a sync.

## How the harnesses read a case

Both harnesses reproduce KIP's runner (`conformance/engine-runner.mjs`):

- one command per case, sent as a single-operation request and flattened to a
  top-level error, the first result's error, or its result;
- a setup step is a command or `{command, params?, capture?}`; `capture` maps
  parameter names to JSON Pointers into that step's raw result, supplied to
  later steps and to every case of the fixture;
- `expect.result` is compared exactly after normalization; `expect.result_contains`
  matches objects member by member and arrays row by row, allowing extras;
  an empty `expect` passes on any result;
- an `UnsupportedCapability` the case did not expect is **SKIP**, never a pass —
  a case that needs an optional capability names it in `envelope.requires`;
- a fixture marked `"status": "pending_engine"` has been verified by no engine
  yet. Its failures are reported but do not fail the run; its passes are new
  evidence. Promoting it is a KIP change.

`manifest.json` is provenance, not a fixture, and both harnesses skip it.

## Normalization

Element ids are engine-assigned, so they are rewritten to `C:<1>`, `P:<2>`, …
in the order a **sorted-key** walk of the answer first reaches them — one
counter across every kind, so inside a change entry the element's own `id` is
numbered before anything under `refs`. Wall-clock timestamps, transaction ids,
content digests, authorization views and search scores are dropped. Everything
else is compared exactly.

## `vectors`, and the coverage matrix

`vectors` names the parent-suite vectors a case pins, declared by the suite's
authors. `rs/anda_cognitive_nexus/tests/coverage.rs` turns the declarations
into the §102 invariant matrix (`Invariants.md` Part A), reports vectors that
pin no Part A invariant, and holds a floor so coverage cannot quietly shrink:

```bash
cargo test -p anda_cognitive_nexus --test coverage -- --nocapture
```
