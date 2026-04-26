# anda_db_server

`anda_db_server` is the HTTP service layer for the core AndaDB database. It
wraps the embedded `anda_db` engine behind a lightweight RPC-style HTTP API so
applications can expose collections, documents, and search over the network.

## What This Crate Provides

- HTTP endpoints for database and collection operations
- JSON and CBOR request/response support
- optional API-key authentication
- one server process serving multiple databases
- a deployment path for clients that do not embed Rust libraries directly

## When to Use It

Use `anda_db_server` when you want:

- a network entrypoint for the core database layer
- a simple service wrapper over `anda_db`
- JSON or CBOR clients instead of in-process Rust calls
- a backend target for shard-proxy deployments

## Quick Start

Run with a local database path:

```bash
cargo run -p anda_db_server -- local --db ./db
```

Run in memory:

```bash
cargo run -p anda_db_server
```

Useful endpoints:

- `POST /` for root-level methods such as database creation and listing
- `POST /{db_name}` for database-scoped operations such as collection CRUD and search

## Related Crates

- `anda_db` for the embedded database engine
- `anda_db_shard_proxy` for shard routing in multi-tenant deployments

## License

MIT. See [LICENSE](../../LICENSE).
