# anda_db_shard_proxy

The executable is `anda-db-shard-proxy`; the Cargo package name remains `anda_db_shard_proxy`.

`anda_db_shard_proxy` is the shard-routing service layer of the AndaDB
workspace. It gives multi-tenant deployments one stable HTTP entrypoint in
front of many [`anda_db_server`](../anda_db_server) backends, resolving each
logical database name to a shard backend through PostgreSQL-backed routing
metadata.

```text
 ┌──────────┐       ┌──────────────────┐       ┌────────────────┐
 │  Client  │──────▶│  Shard Proxy (N) │──────▶│  DB Shard 0    │
 └──────────┘       │                  │       ├────────────────┤
                    │  in-memory cache │       │  DB Shard 1    │
                    │        │         │       ├────────────────┤
                    │   PostgreSQL     │       │  DB Shard 2    │
                    │ (LISTEN/NOTIFY)  │       └────────────────┘
                    └──────────────────┘
```

## What this crate provides

- reverse proxying for sharded `anda_db_server` deployments, including
  streamed request and response bodies
- PostgreSQL routing tables for `db_name → shard_id` and
  `shard_id → backend_addr`, created on start if missing
- bounded in-memory routing caches in every proxy instance, kept in sync
  across instances through PostgreSQL `LISTEN/NOTIFY`
- an authenticated `/_admin` API for shard backends and database assignments
- optional fallback to a default backend for databases without a routing row

## When to use it

Use `anda_db_shard_proxy` when you want:

- one ingress layer in front of multiple database backends
- stable tenant routing across backend moves and failover
- multi-tenant deployments with a shared control plane
- shard-aware forwarding without routing logic in clients

## Quick start

```bash
export DATABASE_URL="postgres://user:pass@localhost/shard_proxy"
export API_KEY="my-secret"
cargo run -p anda_db_shard_proxy -- --addr 127.0.0.1:8080

# Register a backend and assign a database to it.
curl -X PUT localhost:8080/_admin/shard_backends \
  -H "Authorization: Bearer $API_KEY" -H 'content-type: application/json' \
  -d '{"shard_id": 1, "backend_addr": "http://10.0.0.1:8080"}'
curl -X PUT localhost:8080/_admin/db_shards \
  -H "Authorization: Bearer $API_KEY" -H 'content-type: application/json' \
  -d '{"db_name": "tenant_a", "shard_id": 1}'

# Requests for /tenant_a are now forwarded to shard 1.
```

## Configuration

Every option is a flag or the matching environment variable (a `.env` file is
read on start):

| Flag / environment variable                                   | Default          | Meaning                                                                  |
| ------------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------ |
| `--addr` / `ADDR`                                             | `127.0.0.1:8080` | Listen address                                                           |
| `--database-url` / `DATABASE_URL`                             | required         | PostgreSQL URL; URL-encode special characters in the password           |
| `--path-prefix` / `PATH_PREFIX`                               | `/`              | Prefix stripped before the database name: `{prefix}{db_name}/...`       |
| `--api-key` / `API_KEY`                                       | —                | Key for the `/_admin` API; required on a non-loopback address           |
| `--insecure-no-api-key` / `INSECURE_NO_API_KEY`               | `false`          | Allow a non-loopback listener without a key (dangerous)                 |
| `--pg-max-connections` / `PG_MAX_CONNECTIONS`                 | `5`              | PostgreSQL pool size                                                     |
| `--proxy-request-timeout` / `PROXY_REQUEST_TIMEOUT`           | `300`            | Seconds until backend response headers; the body stream is not bounded  |
| `--route-resolve-timeout` / `ROUTE_RESOLVE_TIMEOUT`           | `5`              | Seconds for a cold route lookup, including the permit wait              |
| `--route-resolve-max-concurrency` / `ROUTE_RESOLVE_MAX_CONCURRENCY` | `64`       | Concurrent cold route lookups                                            |
| `--trusted-proxy-cidrs` / `TRUSTED_PROXY_CIDRS`               | empty            | Directly connected proxies whose `X-Forwarded-*` chain is trusted       |
| `--default-backend-addr` / `DEFAULT_BACKEND_ADDR`             | —                | Backend for databases that have no routing row yet                      |

## Request routing

The database name is the first path segment after `--path-prefix` and must
match the backend's naming rules (`[a-z0-9_]{1,64}`). Client-supplied shard
headers are ignored; only server-side routing metadata selects a shard.

A request whose path carries no valid database name is answered with `404`
and never forwarded, not even to `--default-backend-addr`: with the default
prefix, `POST /` is the backend's root scope (`db.list`, `db.create`, …), and
a catch-all fallback would expose every database on the shared shard. The
default backend serves only requests that name a database.

Cold lookups are single-flight per name, bounded by
`ROUTE_RESOLVE_MAX_CONCURRENCY` and `ROUTE_RESOLVE_TIMEOUT`. Positive cache
entries are revalidated after 30 seconds and negative ones after 5 seconds,
so routing changes are eventually consistent within those bounds even if a
`NOTIFY` is missed; the cache holds at most 100,000 entries.

The proxy strips only hop-by-hop headers, so the client's `Authorization`
reaches the backend unchanged and `anda_db_server`'s per-database keys are
enforced there. Each shard backend keeps its own key bindings: re-key a
database on its new shard after moving it. The proxy has no read-only flag;
read-only enforcement belongs to the backend (`db.set_read_only`,
`collection.set_read_only`).

## Management API

All `/_admin` routes require `Authorization: Bearer <API_KEY>` (compared in
constant time).

| Method   | Path                            | Body                                                      | Description                   |
| -------- | ------------------------------- | --------------------------------------------------------- | ----------------------------- |
| `GET`    | `/_admin/db_shards/{db_name}`   | –                                                         | Get one database's shard      |
| `PUT`    | `/_admin/db_shards`             | `{"db_name": "mydb", "shard_id": 1}`                      | Assign a database to a shard  |
| `DELETE` | `/_admin/db_shards`             | `{"db_name": "mydb"}`                                     | Remove an assignment          |
| `GET`    | `/_admin/shard_backends`        | –                                                         | List shard backends           |
| `PUT`    | `/_admin/shard_backends`        | `{"shard_id": 1, "backend_addr": "http://10.0.0.1:8080"}` | Add or update a shard backend |
| `DELETE` | `/_admin/shard_backends`        | `{"shard_id": 1}`                                         | Delete a shard backend        |

## Proxy trust

Incoming `X-Forwarded-For`, `X-Forwarded-Host` and `X-Forwarded-Proto`
headers are untrusted by default. The proxy discards them and rebuilds the
values from the immediate socket peer, the original `Host` and its own HTTP
scheme. If this service is deployed directly behind a controlled load
balancer, configure only that balancer's directly connected networks:

```bash
--trusted-proxy-cidrs 10.0.0.0/8,fd00::/8
```

When — and only when — the immediate peer belongs to one of those CIDRs, its
existing forwarding chain is preserved and the peer address is appended. Do
not configure client or public address ranges as trusted proxies.

## Testing

```bash
cargo test -p anda_db_shard_proxy
```

## Related crates

- [`anda_db_server`](../anda_db_server): the backend database servers being
  proxied
- [`anda_db`](../anda_db): the embedded database behind those services

## License

MIT. See [LICENSE](../../LICENSE).
