# KIP 2.0 cross-engine conformance fixtures

Plain data, run by every engine that claims to implement KIP 2.0. Today that is
both of them:

- `rs/anda_cognitive_nexus` — `tests/conformance.rs`, which reads this
  directory directly;
- `ts/kip-do` — `test/conformance.test.ts`, via
  `test/conformance/fixtures.generated.ts`, inlined from these files by
  `pnpm run codegen:fixtures`. Regenerate and commit after changing a fixture;
  CI fails on the drift rather than letting the TypeScript engine quietly test
  an older suite.

The 1.x suite that used to live in `../kip-conformance/` is deleted. It was not
ported: KIP 2.0 is a different data model, so a "ported" 1.x case would assert
1.x semantics in 2.0 clothing.

A case belongs here when a second engine must reproduce it. Behaviour that
follows from one engine's storage layout belongs in that engine's own tests.

## Fixture shape

```jsonc
{
  "name": "core-truth-neutrality",
  "description": "why these cases exist",
  "packages": [ /* extra Schema Package artifacts, installed and activated */ ],
  "setup":    [ "MUTATE { ... }" ],
  "cases": [
    {
      "name": "...",
      "command": "FIND(?x) WHERE { ... }",
      "params":  {"p": "..."},          // optional request parameters
      "expect":  {"result": [...]},     // or {"error": "SchemaSymbolNotFound"}
      "ordered": false,                 // top-level array order is contractual
      "vectors": ["CORE-001"]           // §102 invariant vectors this case pins
    }
  ]
}
```

## `vectors`, and the coverage matrix

`KIP-2.0-Invariants.md` Part A registers the 38 cross-cutting invariants §102
requires, and names the conformance vectors that pin each one. It is blunt
about what that means: *a Core invariant without a vector does not exist.*

These fixtures are this repository's own suite, not the normative one, so a
case covers a normative vector only when someone has read both and decided they
test the same thing — which is what `vectors` records. `rs/anda_cognitive_nexus/tests/coverage.rs`
turns the declarations into the matrix, refuses a name the registry does not
know, and holds a floor so coverage cannot quietly shrink:

```bash
cargo test -p anda_cognitive_nexus --test coverage -- --nocapture
```

Only vectors the registry names are accepted. The suite has 331 vectors and 83
of them pin an invariant; this field is about those 83, because the matrix is
what it feeds. Declare one only where the case really does test what the vector
tests — a wrong claim is worse than a blank, because it reports an invariant as
covered when nothing checks it.

The Cognitive Memory Profile is installed and activated for every fixture.

## What the harness normalizes, and why

Element ids are engine-assigned, so a fixture cannot name them. They are
rewritten to `C:<1>`, `P:<2>`, … by order of first appearance — which still
catches a wrong reference while letting two engines assign different ids.
Wall-clock timestamps, transaction ids, content digests and search scores are
dropped for the same reason: they are engine truth, not behaviour.

Everything else is compared exactly. A fixture that had to be loose about its
expected values would not be pinning anything down.

**The counter is global and the walk is sorted by key.** `C:<1>`, `P:<2>`,
`A:<3>` — one sequence across every kind, in the order a sorted-key walk of the
answer reaches each id. So inside a change entry the element's own `id` is
numbered before anything under `refs`, because `id` sorts before `refs`. Write
the ordinal that walk actually produces: the TypeScript harness compares it
literally, while the Rust one re-numbers a fixture's placeholders through the
same aliasing and so tolerates a wrong ordinal. A fixture that satisfies the
literal reading passes under both; one that does not looks like an engine
divergence when it is an authoring slip.
