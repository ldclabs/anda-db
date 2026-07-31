# anda_db_server

`anda_db_server` wraps the embedded [`anda_db`](../anda_db) engine behind an
HTTP service with a **CBOR-first** RPC API. JSON is supported as a secondary
format for debugging and non-CBOR clients.

## Features

- Complete database, collection, and document operations over HTTP
- CBOR as the primary wire format (lossless for binary values such as
  `bf16` vectors); JSON supported via content negotiation
- One server process serving multiple databases; databases created at
  runtime are registered and reopened automatically after a restart
- Per-database background flush tasks plus graceful flush-and-close on
  shutdown; tracked mutations drain before database close
- Cancel-safe read RPCs and bounded concurrency for non-cancel-safe mutations
- Structured errors with meaningful HTTP status codes and stable error codes
- Two-tier bearer-token authentication: one admin key plus optional
  per-database keys, so one tenant's key cannot reach another tenant's data
- Compatible with [`anda_db_shard_proxy`](../anda_db_shard_proxy): the first
  path segment is the database name

## Quick Start

```bash
# In-memory storage (data is lost on exit)
cargo run -p anda_db_server

# Local filesystem storage
cargo run -p anda_db_server -- local --path ./debug/db

# S3-compatible storage, configured via AWS_* environment variables
cargo run -p anda_db_server -- s3

# With API key authentication (this is the admin key)
cargo run -p anda_db_server -- --api-key my-secret local --path ./debug/db
```

Options: `--addr` (default `127.0.0.1:8080`), `--api-key`, `--primary-db`
(default `anda_db`), `--flush-interval-secs` (default `30`),
`--request-timeout-secs` (default `300`), `--max-concurrent-mutations`
(default `32`), `--max-body-size` (default `2097152`), `--max-databases`
(default `64`), and `--shutdown-timeout-secs` (default `30`). All options can
also be set through their uppercase environment variables.

`--max-databases` bounds the registry of non-primary databases. Each
registered database keeps a permanent background flush task and a name in the
primary database's registry extension, so registration past the limit is
refused with `409 limit_exceeded`. The bound is not applied when reopening
databases at startup, so lowering it never blocks a restart.

On shutdown, new RPC admission closes immediately, active reads are
cancelled, and admitted mutations drain before databases close. If the
shutdown timeout (measured from admission close, including HTTP drain time)
expires, the server explicitly aborts remaining work and skips the final
database flush, treating the exit as a crash so the normal recovery path can
repair durable state on the next open. The timeout bounds RPC drain; after a
successful drain, the final durable database close is allowed to finish rather
than being cancelled halfway through its flush.

## Wire Protocol

| Route | Description |
|-------|-------------|
| `GET /` | Unauthenticated health/info (name, version), defaults to JSON |
| `POST /` | Root-scope methods (server info, database lifecycle) |
| `POST /{db_name}` | Database-scoped methods (`db.*`, `collection.*`, `doc.*`) |

Request body:

```json
{"method": "doc.add", "params": {"collection": "articles", "doc": {"title": "Hello"}}}
```

Success response (HTTP 200):

```json
{"result": {"_id": 1}}
```

Error response (HTTP 4xx/5xx):

```json
{"error": {"code": "not_found", "message": "database \"demo\" not found"}}
```

Error codes: `bad_request`, `invalid_input`, `invalid_query`,
`method_not_found`, `unauthorized`, `not_found`, `already_exists`, `conflict`,
`timeout`, `payload_too_large`, `unsupported_media_type`, `limit_exceeded`,
`unavailable`, `collection_unavailable`, `gone`, `internal`.

`collection_unavailable` (503) means the collection handle is temporarily
unusable — a cancelled operation invalidated it, or it is closing — and
reopening recovers it, so the request can simply be retried. `gone` (410)
means the collection was deleted and no retry will help.

Only failures positively classified at the HTTP boundary are returned with
client-facing details. Database, storage, serialization, and index failures
are logged server-side and use a generic `internal` response so physical
object paths and nested error sources never cross the API boundary.

### Encoding negotiation

- The request body format follows `Content-Type`, which must be present and
  be `application/cbor` or `application/json`. Anything else is refused with
  `415 unsupported_media_type`; treating an unrecognized type as CBOR would
  make the RPC endpoints reachable as browser "simple requests".
- The response format follows `Accept` when present, otherwise mirrors the
  request `Content-Type`, otherwise CBOR.

### Authentication and authorization

Credentials are always presented as `Authorization: Bearer <key>`. `GET /`
stays open for health checks. There are two tiers:

| Tier | Configured by | Reaches |
|------|---------------|---------|
| **Admin key** | `--api-key` / `API_KEY` (one per process) | `POST /` (all root-scope methods) and every `POST /{db_name}` |
| **Per-database key** | `db.create {api_key}` / `db.set_api_key` | `POST /{that_db_name}` only |

A per-database key is never accepted at the root scope, so it cannot list,
create, open, close, or re-key any database — including its own. Only the
SHA3-256 hash of a key is stored, in the primary database's extension
metadata (`server:api_keys`) next to the database registry, so bindings
survive a restart and a copy of the metadata does not yield working
credentials. Comparison is constant-time over the hashes.

**Precedence.** For `POST /{db_name}`:

1. No admin key configured → the instance is unauthenticated and every
   request is treated as an admin (see the compatibility notes below).
2. The presented key matches the admin key → full access.
3. The named database has a key bound and the presented key matches it →
   access to that database only.
4. Otherwise → `401 unauthorized`. A database with no key of its own falls
   back to rule 2, which is exactly how every database behaved before
   per-database keys existed.

Rule 4 returns the *same* `401` whether the database exists under another
key, exists without a key, or does not exist at all — an unauthorized caller
cannot probe the database namespace. Only an admin ever receives `404
not_found` for a missing database. For the same reason the database-scoped
`info` method returns only the caller's own database (and no primary database
name) when it is answered for a per-database key; admins keep the full view.

### Provisioning and rotating per-database keys

All three methods are root-scope, hence admin-only:

```bash
# Create a database with its own key
curl -H 'Authorization: Bearer $ADMIN_KEY' -H 'Content-Type: application/json' \
  -d '{"method":"db.create","params":{"name":"tenant_a","api_key":"<tenant key>"}}' \
  http://127.0.0.1:8080/

# Rotate it (the previous key stops working immediately)
... '{"method":"db.set_api_key","params":{"name":"tenant_a","api_key":"<new key>"}}'

# Rotate without supplying one: the server generates a 256-bit key with a
# CSPRNG and returns it exactly once, in {"result":{"name":...,"api_key":...}}.
... '{"method":"db.set_api_key","params":{"name":"tenant_a"}}'

# Revoke, returning the database to the admin key only
... '{"method":"db.remove_api_key","params":{"name":"tenant_a"}}'
```

A caller-supplied key is never echoed back, and a generated key is
unrecoverable afterwards — rotate again if it is lost. `db.close` keeps a
binding so that reopening a database cannot silently weaken it; use
`db.remove_api_key` to revoke.

Two bindings are refused: the **primary database** cannot be delegated (its
extension metadata *is* the server's registry and key map), and no
per-database key can be bound while the server runs **without** an admin key,
since the open root scope would let anyone rotate it away. Consequently, if
per-database keys exist in storage and the server is restarted without
`API_KEY`, it refuses to start rather than silently downgrading those
databases to unauthenticated access.

### Upgrade and compatibility

- **Single-key deployments keep working unchanged.** The existing
  `--api-key` becomes the admin key. With no per-database keys provisioned,
  every route behaves exactly as before: that one key opens the root scope
  and every database, `info` still enumerates the instance, and a wrong or
  missing key is still `401`.
- **The keyless loopback/development mode is unchanged.** Without
  `--api-key` the server still accepts every request on a loopback listener
  (or with `--insecure-no-api-key`); per-database keys simply cannot be
  provisioned in that mode.
- **Nothing is required at upgrade time.** Per-database keys are opt-in, one
  database at a time, and can be revoked back to the previous behaviour.
- Deployments fronted by [`anda_db_shard_proxy`](../anda_db_shard_proxy) are
  unaffected: the proxy strips only hop-by-hop headers, so the client's
  `Authorization` reaches the backend unchanged and a per-database key is
  enforced there. The proxy's own `API_KEY` guards its `/_admin/*` API and is
  unrelated to these tiers. Note that each shard backend keeps its own
  bindings, so a database moved to another shard must be re-keyed there.

## Methods

### Root scope (`POST /`)

Admin key only — a per-database key is rejected on `POST /` with `401`.

| Method | Params | Result |
|--------|--------|--------|
| `info` | — | Server name, version, primary database, open databases |
| `db.list` | — | Open database names |
| `db.create` | `{name, description?, api_key?}` | Database metadata; `409` if it exists |
| `db.open` | `{name}` | Database metadata; `404` if missing |
| `db.connect` | `{name, description?}` | Database metadata; creates if missing |
| `db.close` | `{name}` | Flushes, closes, and unregisters the database |
| `db.set_api_key` | `{name, api_key?}` | `{name, api_key}`; `api_key` is set only when generated |
| `db.remove_api_key` | `{name}` | `true` if a key was bound |

Databases created or opened at runtime are recorded in the primary
database's extensions and reopened automatically on the next start;
`db.close` removes a database from that registry. The primary database
cannot be closed.

### Database scope (`POST /{db_name}`)

Admin key, or the key bound to `{db_name}`.

| Method | Params | Result |
|--------|--------|--------|
| `info` | — | Server info; only this database for a per-database key |
| `db.metadata` | — | Database config, collections, extensions |
| `db.stats` | — | Aggregated storage I/O statistics |
| `db.flush` | — | Flushes all collections and metadata |
| `db.set_read_only` | `{read_only}` | Toggles read-only mode |
| `db.get_extension` | `{key}` | Extension value or `null` |
| `db.save_extension` | `{key, value}` | Persists an extension entry |
| `db.remove_extension` | `{key}` | Previous value or `null` |
| `collection.list` | — | Collection names |
| `collection.create` | see below | Collection metadata; `409` if it exists |
| `collection.ensure` | see below | Opens or creates the collection |
| `collection.metadata` | `{collection}` | Config, schema, indexes, stats |
| `collection.stats` | `{collection}` | Collection statistics |
| `collection.delete` | `{collection}` | Deletes the collection and its data |
| `collection.flush` | `{collection}` | `true` if pending changes were written |
| `collection.set_read_only` | `{collection, read_only}` | Toggles read-only mode |
| `collection.get_extension` | `{collection, key}` | Extension value or `null` |
| `collection.save_extension` | `{collection, key, value}` | Persists an extension entry |
| `collection.remove_extension` | `{collection, key}` | Previous value or `null` |
| `doc.add` | `{collection, doc}` | `{_id}` (engine-assigned) |
| `doc.add_many` | `{collection, docs}` | `[{_id}, ...]`; not atomic; at most 10 000 documents |
| `doc.get` | `{collection, _id}` | The document |
| `doc.get_many` | `{collection, _ids}` | One entry per ID, `null` for missing; at most 1 000 IDs |
| `doc.update` | `{collection, _id, fields}` | The updated document |
| `doc.remove` | `{collection, _id}` | The removed document or `null` |
| `doc.exists` | `{collection, _id}` | `true` / `false` |
| `doc.count` | `{collection}` | Number of documents |
| `doc.search` | `{collection, query}` | Matching documents |
| `doc.search_ids` | `{collection, query}` | Matching document IDs |
| `doc.query_ids` | `{collection, filter, limit?}` | IDs matching a B-Tree filter; `limit` defaults to and is capped at 1 000, `0` returns nothing |

### Creating collections

`collection.create` / `collection.ensure` take the collection config, the
document schema, and optional index definitions:

```json
{
  "method": "collection.create",
  "params": {
    "config": {"name": "articles", "description": "Articles"},
    "schema": {
      "fields": [
        {"name": "_id", "description": "", "type": "U64", "unique": true, "index": 0},
        {"name": "title", "description": "", "type": "Text", "unique": false, "index": 1},
        {"name": "embedding", "description": "", "type": "Vector", "unique": false, "index": 2}
      ]
    },
    "btree_indexes": [["title"]],
    "bm25_indexes": ["title"],
    "hnsw_indexes": [{
      "field": "embedding",
      "config": {
        "dimension": 384,
        "max_layers": 16,
        "max_connections": 32,
        "ef_construction": 200,
        "ef_search": 50,
        "distance_metric": "Cosine",
        "select_neighbors_strategy": "Heuristic"
      }
    }]
  }
}
```

The engine only allows index changes while it has exclusive access to a
collection, so indexes are defined at creation time. `collection.ensure` is
idempotent: it opens the collection when it already exists and only applies
the index definitions when it actually creates (or first loads) it. An HNSW
configuration that has drifted from the persisted one is never silently
kept: when the first load detects the difference, `collection.ensure`
answers `409 conflict` naming the field and both configurations — remove
and recreate the index (or the collection) to change it.

### Queries

`doc.search` accepts the engine's `Query` shape — full-text and/or vector
search with optional B-Tree filtering:

```json
{
  "method": "doc.search",
  "params": {
    "collection": "articles",
    "query": {
      "search": {"text": "anda db", "vector": [0.1, 0.2, 0.3]},
      "filter": {"Field": ["score", {"Ge": 10}]},
      "limit": 10
    }
  }
}
```

Filters support `Field`, `And`, `Or`, and `Not` with range operators
(`Eq`, `Gt`, `Ge`, `Lt`, `Le`, `Between`, `Include`, ...) against B-Tree
indexed fields.

### Vector fields

`Vector` fields store `bf16` values. On input the server accepts arrays of
floats (converted to `bf16`) as well as arrays of integers (interpreted as
raw `bf16` bit patterns — the engine's native wire format). Responses always
return vectors as `bf16` bit patterns, so a document read from the server
can be written back unchanged.

### Durability

Writes are persisted to the object store immediately; index and metadata
state is flushed by the per-database background task (`--flush-interval-secs`),
by `db.flush` / `collection.flush`, and on graceful shutdown.

## Example: CBOR client

```python
import cbor2, urllib.request

def rpc(path, method, params=None):
    body = cbor2.dumps({"method": method, "params": params})
    req = urllib.request.Request(
        f"http://127.0.0.1:8080{path}",
        data=body,
        headers={"Content-Type": "application/cbor"},
    )
    with urllib.request.urlopen(req) as resp:
        return cbor2.loads(resp.read())

print(rpc("/", "info"))
print(rpc("/anda_db", "doc.add", {
    "collection": "articles",
    "doc": {"title": "Hello", "embedding": [0.1, 0.2, 0.3]},
}))
```

## Related Crates

- [`anda_db`](../anda_db) — the embedded database engine
- [`anda_db_shard_proxy`](../anda_db_shard_proxy) — shard routing for
  multi-tenant deployments

## License

MIT. See [LICENSE](../../LICENSE).
