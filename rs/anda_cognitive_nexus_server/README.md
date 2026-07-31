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

The key is checked before the request body is read, so an unauthenticated
caller always gets `401` — never a body-parsing or body-size error.

## Response Statuses

`POST /kip` answers `200` only for a successful execution. A failed KIP
execution keeps the same JSON body (`{"error": {"code": "KIP_...", ...}}`) but
carries a status matching the error class, so load balancers, retry policies,
and 5xx alerting see the failure: `400` for syntax/schema/reference errors and
oversized result sets, `404` for `NotFound`, `409` for `DuplicateExists` /
`VersionConflict`, `403` for `ImmutableTarget`, `408` for `ExecutionTimeout`,
and `500` for internal or unrecognized errors.

## Audit Log

Every `/kip` request appends a durable document to the `kip_logs` collection.
Two bounds keep it from growing without limit:

- `LOG_RETENTION_DAYS` (default `30`) prunes documents older than the window.
  `0` disables pruning entirely and must be chosen explicitly.
- `MAX_LOGGED_REQUEST_BYTES` (default `8192`) caps the request stored in each
  document; a larger request is stored truncated but still parseable. Raise it
  to `MAX_BODY_SIZE` to keep full request bodies.

## Related Crates

- `anda_cognitive_nexus` for the reference KIP executor
- `anda_kip` for the protocol model
- `anda_db` for the embedded storage core

## License

MIT. See [LICENSE](../../LICENSE).
