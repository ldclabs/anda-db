# anda_cognitive_nexus_server

The executable is `anda-cognitive-nexus-server`; the Cargo package name remains `anda_cognitive_nexus_server`.

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

The local directory is created on first use. `local --db memory` is a real
filesystem directory; only omitting the subcommand or using `memory` selects
ephemeral storage. The listener is bound exclusively before storage is opened.
Local mode holds `.anda-nexus-writer.lock` until shutdown, refusing a second
server writer even on another port. The lock is advisory: other applications
opening the same namespace must still honor AndaDB's single-writer contract.
Do not delete the lock file while a server is running.

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

This is a **shared administrator endpoint**: accepted requests execute as the
engine's system Principal, including when authentication is explicitly
disabled. The API key is not a per-caller identity mapping, and `list_logs`
exposes this service's execution logs. Deploy for one trusted owner; a host
serving separately authorized callers must construct authenticated `AuthContext`
values and use `CognitiveNexus::session`. Request body fields never select
transport authority.

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
are Governance state, managed by the engine's separate control plane. The server
does not write an identity into cognitive content to stand in for one.

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

`REQUEST_TIMEOUT_SECS` bounds the execution *response*, not the execution:
a KML mutation that overruns it keeps running rather than being cancelled
mid-write. Such a request answers `408` with `status: "outcome_unknown"` and
`retry.class: "outcome_lookup_required"`. Timeout responses preserve
`request_id` and `execution.idempotency_key` when supplied. Use stable
idempotency keys for writes, and recover with
`DESCRIBE TRANSACTION BY IDEMPOTENCY KEY :key` instead of submitting a fresh
mutation. A read/dry-run timeout is explicitly marked `safe_same_request`.

Body reception has a separate deadline of twice `REQUEST_TIMEOUT_SECS`, and
also stops on shutdown. It cannot overwrite the outcome of a started write.
All admitted requests, including body buffering, parsing, reads, and detached
execution, share the `MAX_CONCURRENT_MUTATIONS` capacity (default `64`; the
legacy option name is retained). Exhaustion returns `429 / RateLimited` with
safe retry advice; shutdown refusal returns `503` and also permits safe retry.
A permit stays with a detached execution until it finishes. Commands are
prepared once and the same parsed operations drive classification and execution.

An execution still running after four times the response timeout initiates
process shutdown without cancelling the write immediately. Shutdown closes
admission, stops retention between completed deletes, drains execution and
periodic flushes, then explicitly closes the database. All phases share
`SHUTDOWN_DRAIN_TIMEOUT_SECS` (default `300`). A forced abort or failed close
returns a nonzero process status and leaves recovery to the next open.

## Cache and Builds

`CACHE_MAX_BYTES` (default `67108864`, 64 MiB; `0` disables caching) sets the
object cache budget for a **new** database. Existing databases retain their
persisted `StorageConfig`; changing this startup option does not resize them.
This is not a total process memory limit: index memory and the metadata wrapper's
separate cache are additional.

The Docker build copies this crate's committed `Cargo.lock` snapshot to the
workspace root and builds with `--locked`. After changing workspace dependencies,
refresh it with `cp Cargo.lock rs/anda_cognitive_nexus_server/Cargo.lock` from
the repository root. The default
release profile favors binary size. For throughput comparisons, build with
`cargo build --locked --profile release-speed -p anda_cognitive_nexus_server`
and use `target/release-speed/anda-cognitive-nexus-server`. Measure your workload
before changing the production build profile.

The preparation benchmark is reproducible with
`cargo bench -p anda_kip --bench protocol --profile release-speed -- server_preparation`.
It compares the previous decode/classify/execute path with prepared execution
using a no-op executor and a 100-clause KML command. Three local macOS runs on
2026-09-23 (30 samples, 1 s warmup, 2 s measurement) gave median estimates of
161.56 → 81.63 µs for the plain envelope and 244.20 → 86.93 µs with a 64 KiB ingest
payload. These isolate SDK preparation/dispatch CPU work, not HTTP throughput,
database writes, or the size-versus-speed build-profile tradeoff.

## Audit Log

Each admitted, valid `execute_kip` envelope attempts an audit append after
execution, including command failures. Authentication, body/envelope failures,
capacity refusals and `list_logs` calls do not append execution logs. Logging is
best-effort: failure is reported in the service log without replacing a committed
KIP result. Successful appends persist the document; periodic flushes checkpoint
indexes and metadata.

Audit `response.operations[]` records `op_id`, `status`, `tx_id` and `error` for
every result, including failed and skipped operations. For compatibility, the
legacy top-level `tx_id` (first receipt) and `errors` summaries remain. Old log
rows without `operations` are still readable. Query result payloads are omitted.

Two settings bound retention and logged request size:

- `LOG_RETENTION_DAYS` (default `30`) prunes documents older than the window.
  `0` disables pruning entirely and must be chosen explicitly.
- `MAX_LOGGED_REQUEST_BYTES` (default `8192`) caps the request stored in each
  document, with a minimum effective cap of 256 bytes. The bound includes JSON
  escaping; a larger request is stored truncated but still deserializable. Raise it
  to `MAX_BODY_SIZE` to keep full request bodies.

## Related Crates

- `anda_cognitive_nexus` for the reference KIP executor
- `anda_kip` for the protocol model
- `anda_db` for the embedded storage core

## License

MIT. See [LICENSE](../../LICENSE).
