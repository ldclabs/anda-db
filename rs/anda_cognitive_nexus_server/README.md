# anda_cognitive_nexus_server

`anda_cognitive_nexus_server` is the HTTP/JSON-RPC deployment layer for the
Cognitive Nexus. It exposes KIP execution over the network so non-Rust clients,
agent frameworks, and service integrations can use the reference AI-memory
runtime without embedding the Rust crates directly.

It speaks **KIP 2.0**.

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

`params` for `execute_kip` is the KIP 2.0 request envelope (§71), not a bare
command string:

```bash
curl -sX POST localhost:8080/kip -H 'content-type: application/json' -d '{
  "method": "execute_kip",
  "params": {
    "kip": "2.0",
    "operations": [{"command": "DESCRIBE PRIMER"}]
  }
}'
```

`operations[]` is a batch, not a transaction: a request carrying more than one
operation must declare `execution.mode` as `independent` or `sequence`.
`atomic` is refused with `UnsupportedCapability` rather than silently
downgraded — one transaction and one snapshot across several operations is an
engine property this runtime does not have yet.

If you set `API_KEY`, clients must send:

```text
Authorization: Bearer <API_KEY>
```

The key is checked before the request body is read, so an unauthenticated
caller always gets `401` — never a body-parsing or body-size error.

## Schema Environment

A MemorySpace that has activated nothing resolves the Core package only, and
Core declares no Concept types — so a server without a Schema Environment would
refuse every `CREATE CONCEPT` it was ever sent. The baseline
[cognitive-memory profile](../anda_cognitive_nexus/profiles/) is therefore
installed and activated in the default Space on start.

`SCHEMA_PACKAGE` (repeatable `--schema-package`) installs and activates further
Schema Package artifacts on top of it. The resulting Schema Lock is activated
only when it differs from the one already in force, so a restart does not mint a
new environment version for an unchanged lock.

There is no `$self` genesis node. KIP 1.x seeded one carrying the server's
principal id; in KIP 2.0 a `Person` is explicitly not a Principal, Principals
are Governance state, and this runtime has no Governance plane — so it does not
write an identity into cognitive content to stand in for one.

## Response Statuses

`POST /kip` answers `200` for a successful execution and `207` for a `partial`
batch, where earlier operations committed and a later one failed — reporting
that as an error invites a client to re-issue writes that already landed.

A failed execution keeps the standard KIP response envelope but carries a status
matching the error, so load balancers, retry policies and 5xx alerting see the
failure. The mapping follows the Core Error Registry (§87): `400` for
syntax/protocol/schema/data/resource errors, `401` for `Unauthenticated`, `403`
for the Governance codes, `404` for `NotFoundOrNotVisible`, `409` for version,
precondition, identity and epistemic-revision conflicts, `408` for
`ExecutionTimeout`, `410` for expired cursors and snapshots, `413` for
oversized transactions and artifacts, `429` for `RateLimited`, `501` for
capabilities this runtime declares it does not have, `503` for a temporarily
unavailable index or artifact, and `500` for internal, unknown-outcome, or
unrecognized errors.

`REQUEST_TIMEOUT_SECS` bounds the *response*, not the execution: a KML mutation
that overruns it keeps running rather than being cancelled mid-write. Such a
request answers `408` with `status: "outcome_unknown"` and a retry class of
`outcome_lookup_required` — the write may still commit, so the client must look
the transaction up instead of re-issuing it. A read that overruns is a plain
`ExecutionTimeout` and is safe to re-send.

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
