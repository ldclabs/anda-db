# anda_cognitive_nexus_server

`anda_cognitive_nexus_server` is the HTTP/JSON-RPC deployment layer for the
Cognitive Nexus. It exposes KIP execution over the network so non-Rust clients,
agent frameworks, and service integrations can use the reference AI-memory
runtime without embedding the Rust crates directly.

## What This Crate Provides

- an HTTP server for KIP execution
- JSON-RPC-style request handling for `execute_kip` and related operations
- optional bearer-token authentication
- local filesystem and in-memory deployment modes
- a service wrapper around `anda_cognitive_nexus`

## When to Use It

Use `anda_cognitive_nexus_server` when you want:

- KIP over HTTP instead of in-process Rust calls
- a deployable memory service for agent platforms
- a bridge for non-Rust clients into the Cognitive Nexus runtime
- a standalone entrypoint for persistent graph-shaped AI memory

## Quick Start

Run with a local database path:

```bash
cargo run -p anda_cognitive_nexus_server -- local --db ./db
```

Default endpoints:

- `GET /` for service information
- `POST /kip` for KIP request execution and log-related methods

If you set `API_KEY`, clients must send:

```text
Authorization: Bearer <API_KEY>
```

## Related Crates

- `anda_cognitive_nexus` for the reference KIP executor
- `anda_kip` for the protocol model
- `anda_db` for the embedded storage core

## License

MIT. See [LICENSE](../../LICENSE).
