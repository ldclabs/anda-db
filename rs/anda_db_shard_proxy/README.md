# Anda DB Shard Proxy

`anda_db_shard_proxy` is a lightweight reverse proxy for sharded Anda DB deployments. It keeps a shared routing table in PostgreSQL, maintains low-latency in-memory caches in each proxy instance, and forwards HTTP traffic to the correct shard backend.

This crate is useful when you want to present a single HTTP entrypoint while distributing tenant databases across multiple `anda_db_server` instances.

## What It Does

- Routes requests by logical database name, usually extracted from `/{db_name}/...` or `/v1/{db_name}/...`.
- Supports direct shard routing through the `Shard-ID` or `X-Shard` header.
- Stores routing metadata in PostgreSQL so multiple proxy instances can share the same control plane.
- Uses PostgreSQL `LISTEN/NOTIFY` to keep each instance's in-memory cache synchronized without polling.
- Exposes an admin API for updating database-to-shard assignments and shard-to-backend mappings.

## Routing Model

The proxy separates routing into two layers:

1. `db_shards`
   Maps a logical database name to a stable shard identifier.
2. `shard_backends`
   Maps a shard identifier to the current backend base URL.

This split lets you keep tenant placement stable while still moving a shard between backend instances during failover, migration, or maintenance.

## Architecture

```text
Client
  |
  v
anda_db_shard_proxy
  |  \__ in-memory routing caches (DashMap)
  |  \__ management API
  |  \__ reverse proxy
  |
  +--> PostgreSQL (routing metadata + LISTEN/NOTIFY)
  |
  +--> shard backend A (anda_db_server)
  +--> shard backend B (anda_db_server)
  +--> shard backend C (anda_db_server)
```

## Admin API

If `API_KEY` is configured, all admin endpoints require `Authorization: Bearer <API_KEY>`.

### Database assignments

| Method   | Path                          | Description                            |
| -------- | ----------------------------- | -------------------------------------- |
| `GET`    | `/_admin/db_shards/{db_name}` | Fetch one database-to-shard assignment |
| `PUT`    | `/_admin/db_shards`           | Create a database-to-shard assignment  |
| `DELETE` | `/_admin/db_shards`           | Remove a database-to-shard assignment  |

Create an assignment:

```bash
curl -X PUT http://127.0.0.1:8080/_admin/db_shards \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-secret' \
  -d '{"db_name":"tenant_a","shard_id":1}'
```

### Shard backends

| Method   | Path                     | Description                           |
| -------- | ------------------------ | ------------------------------------- |
| `GET`    | `/_admin/shard_backends` | List cached shard-to-backend mappings |
| `PUT`    | `/_admin/shard_backends` | Create or update a shard backend      |
| `DELETE` | `/_admin/shard_backends` | Delete a shard backend                |

Upsert a backend:

```bash
curl -X PUT http://127.0.0.1:8080/_admin/shard_backends \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-secret' \
  -d '{"shard_id":1,"backend_addr":"http://127.0.0.1:9001","read_only":false}'
```

## Proxy Request Resolution

Requests are resolved in this order:

1. Try `Shard-ID` or `X-Shard`.
2. If no shard id is present, try to extract `db_name` from the request path.
3. Resolve the target shard and backend from the local cache.
4. On a database cache miss, query PostgreSQL and populate the cache.
5. Rewrite the target URI and forward the request.

The proxy strips hop-by-hop headers before forwarding and adds a `Shard-ID` header to both the upstream request and downstream response.

## Configuration

| Variable                | Description                                            | Default          |
| ----------------------- | ------------------------------------------------------ | ---------------- |
| `ADDR`                  | Listen address for the proxy                           | `127.0.0.1:8080` |
| `DATABASE_URL`          | PostgreSQL connection string used for routing metadata | none             |
| `PATH_PREFIX`           | Optional path prefix used to extract `db_name`         | `/`              |
| `API_KEY`               | Optional bearer token for admin endpoints              | none             |
| `PG_MAX_CONNECTIONS`    | Maximum PostgreSQL connections in the shared pool      | `5`              |
| `PROXY_REQUEST_TIMEOUT` | Upstream request timeout in seconds                    | `300`            |
| `DEFAULT_BACKEND_ADDR`  | Optional default backend URL for unmatched requests    | none             |

The same values can also be passed as CLI flags:

```bash
cargo run -p anda_db_shard_proxy -- \
  --addr 0.0.0.0:8080 \
  --database-url postgres://user:pass@localhost/shard_proxy \
  --path-prefix /db/ \
  --api-key my-secret \
  --pg-max-connections 10 \
  --proxy-request-timeout 60
```

## Local Development

1. Start PostgreSQL and create a database for routing metadata.
2. Start one or more backend `anda_db_server` instances.
3. Start the shard proxy.
4. Register shard backends and database assignments through the admin API.
5. Send client traffic through the proxy entrypoint.

Example startup:

```bash
export DATABASE_URL="postgres://user:pass@localhost/shard_proxy"
export API_KEY="my-secret"
export PATH_PREFIX="/db/"

cargo run -p anda_db_server -- --addr 127.0.0.1:9001
cargo run -p anda_db_server -- --addr 127.0.0.1:9002
cargo run -p anda_db_shard_proxy -- --addr 127.0.0.1:8080
```

Then register shard backends and a tenant:

```bash
curl -X PUT http://127.0.0.1:8080/_admin/shard_backends \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-secret' \
  -d '{"shard_id":1,"backend_addr":"http://127.0.0.1:9001"}'

curl -X PUT http://127.0.0.1:8080/_admin/db_shards \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-secret' \
  -d '{"db_name":"tenant_a","shard_id":1}'
```

Now requests such as `POST /tenant_a` or `POST /v1/tenant_a/query` will be forwarded to shard `1`.

## Library Usage

The crate also exposes reusable pieces for embedding the proxy in another Rust application:

- `ShardStore` for PostgreSQL-backed routing metadata
- `AppState` for proxy runtime state
- `DbShardExtractor` for custom request-routing strategies
- `build_router` for constructing the Axum router

## Testing

Run the crate tests with:

```bash
cargo test -p anda_db_shard_proxy
```

Current tests focus on extractor behavior, header sanitization, and cache updates driven by PostgreSQL notification payloads.

## License

MIT