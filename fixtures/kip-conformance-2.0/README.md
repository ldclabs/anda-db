# KIP 2.0 cross-engine conformance fixtures

Plain data, run by every engine that claims to implement KIP 2.0. Today that
is `rs/anda_cognitive_nexus` (see `tests/conformance.rs`); `ts/kip-do` adopts
these once it migrates off 1.x.

The 1.x suite in `../kip-conformance/` is kept as-is until then, because
`ts/kip-do` still runs it. The two are not related: KIP 2.0 is a different data
model, so a "ported" 1.x case would assert 1.x semantics in 2.0 clothing.

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
      "ordered": false                  // top-level array order is contractual
    }
  ]
}
```

The Cognitive Memory Profile is installed and activated for every fixture.

## What the harness normalizes, and why

Element ids are engine-assigned, so a fixture cannot name them. They are
rewritten to `C:<1>`, `P:<2>`, … by order of first appearance — which still
catches a wrong reference while letting two engines assign different ids.
Wall-clock timestamps, transaction ids, content digests and search scores are
dropped for the same reason: they are engine truth, not behaviour.

Everything else is compared exactly. A fixture that had to be loose about its
expected values would not be pinning anything down.
