# anda_kip

`anda_kip` is the protocol SDK of the AndaDB workspace. It provides the parser,
AST, request/response model, error vocabulary, and executor interface for the
Knowledge Interaction Protocol (KIP), the model-friendly protocol used by the
Cognitive Nexus memory graph.

## What This Crate Provides

- parsing for KQL, KML, and META commands
- strongly typed AST structures for protocol execution
- `Request` and `Response` envelopes for tool-style integrations
- standardized KIP error codes
- an `Executor` trait for backend implementations
- bundled genesis capsules and function-calling schemas

## When to Use It

Use `anda_kip` when you need to:

- parse KIP commands in Rust
- implement a KIP backend on your own storage system
- expose a model-friendly request/response protocol to LLM tooling
- reuse the same command language across embedded and server deployments

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_kip = "0.7"
```

This crate is protocol-only. If you want the reference persistent backend, pair
it with `anda_cognitive_nexus`.

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_kip.md](../../docs/anda_kip.md)
- [docs/anda_cognitive_nexus.md](../../docs/anda_cognitive_nexus.md)

## Related Crates

- `anda_cognitive_nexus` for the reference KIP executor
- `anda_db` for the embedded storage core used by the reference backend

## License

MIT. See [LICENSE](../../LICENSE).
