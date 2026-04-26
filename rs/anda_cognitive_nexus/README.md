# anda_cognitive_nexus

`anda_cognitive_nexus` is the reference AI-memory runtime in the AndaDB
workspace. It implements the KIP executor on top of `anda_db`, storing concepts
and propositions in persistent collections and exposing a graph-shaped memory
model for AI agents.

## What This Crate Provides

- the reference `Executor` implementation for KIP
- persistent concept and proposition storage on top of `anda_db`
- graph-oriented execution of KQL, KML, and META commands
- bootstrap logic for genesis capsules and protected system entities
- the main reusable runtime behind the Cognitive Nexus server

## When to Use It

Use `anda_cognitive_nexus` when you want:

- a graph-shaped long-term memory system for AI agents
- protocol-driven memory mutation and retrieval through KIP
- the reference backend instead of implementing a KIP executor yourself
- a persistent knowledge runtime that can later be exposed over HTTP

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_cognitive_nexus = "0.7"
anda_kip = "0.7"
anda_db = "0.7"
```

For the higher-level technical model and execution details, start with the
technical reference below.

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_cognitive_nexus.md](../../docs/anda_cognitive_nexus.md)
- [docs/anda_kip.md](../../docs/anda_kip.md)
- [docs/anda_db.md](../../docs/anda_db.md)

## Related Crates

- `anda_kip` for the protocol layer
- `anda_db` for the embedded storage core
- `anda_cognitive_nexus_server` for the HTTP/JSON-RPC deployment layer

## License

MIT. See [LICENSE](../../LICENSE).
