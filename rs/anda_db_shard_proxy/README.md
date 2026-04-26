# anda_db_shard_proxy

`anda_db_shard_proxy` is the shard-routing service layer of the AndaDB
workspace. It gives multi-tenant deployments a single stable HTTP entrypoint
while resolving logical database names to shard backends through PostgreSQL-
backed routing metadata.

## What This Crate Provides

- reverse proxying for sharded `anda_db_server` deployments
- PostgreSQL-backed routing metadata for `db_name -> shard_id -> backend`
- low-latency in-memory routing caches in each proxy instance
- cache synchronization through PostgreSQL `LISTEN/NOTIFY`
- admin APIs for shard-backend and database-shard assignments

## When to Use It

Use `anda_db_shard_proxy` when you want:

- one ingress layer in front of multiple database backends
- stable tenant routing across backend moves and failover events
- multi-tenant deployments with a shared control plane
- shard-aware forwarding without putting routing logic in clients

## Quick Start

Start the proxy with a PostgreSQL routing database:

```bash
cargo run -p anda_db_shard_proxy -- \
  --database-url postgres://user:pass@localhost/shard_proxy \
  --addr 127.0.0.1:8080
```

Typical admin endpoints:

- `PUT /_admin/shard_backends`
- `PUT /_admin/db_shards`
- `GET /_admin/db_shards/{db_name}`

## Related Crates

- `anda_db_server` for the backend database servers being proxied
- `anda_db` for the embedded database layer behind those services

## License

MIT. See [LICENSE](../../LICENSE).
