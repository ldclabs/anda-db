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
  shutdown
- Structured errors with meaningful HTTP status codes and stable error codes
- Optional bearer-token authentication
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

# With API key authentication
cargo run -p anda_db_server -- --api-key my-secret local --path ./debug/db
```

Options: `--addr` (default `127.0.0.1:8080`), `--api-key`, `--primary-db`
(default `anda_db`), `--flush-interval-secs` (default `30`). All options can
also be set through environment variables (`ADDR`, `API_KEY`, `PRIMARY_DB`,
`FLUSH_INTERVAL_SECS`).

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

Error codes: `bad_request`, `method_not_found`, `unauthorized`, `not_found`,
`already_exists`, `precondition_failed`, `payload_too_large`, `internal`.

### Encoding negotiation

- The request body format follows `Content-Type`: `application/cbor`
  (default when absent) or `application/json`.
- The response format follows `Accept` when present, otherwise mirrors the
  request `Content-Type`, otherwise CBOR.

### Authentication

When the server is started with `--api-key`, every RPC request must carry
`Authorization: Bearer <key>`. `GET /` stays open for health checks.

## Methods

### Root scope (`POST /`)

| Method | Params | Result |
|--------|--------|--------|
| `info` | — | Server name, version, primary database, open databases |
| `db.list` | — | Open database names |
| `db.create` | `{name, description?}` | Database metadata; `409` if it exists |
| `db.open` | `{name}` | Database metadata; `404` if missing |
| `db.connect` | `{name, description?}` | Database metadata; creates if missing |
| `db.close` | `{name}` | Flushes, closes, and unregisters the database |

Databases created or opened at runtime are recorded in the primary
database's extensions and reopened automatically on the next start;
`db.close` removes a database from that registry. The primary database
cannot be closed.

### Database scope (`POST /{db_name}`)

| Method | Params | Result |
|--------|--------|--------|
| `info` | — | Server info |
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
| `doc.add_many` | `{collection, docs}` | `[{_id}, ...]`; not atomic |
| `doc.get` | `{collection, _id}` | The document |
| `doc.get_many` | `{collection, _ids}` | One entry per ID, `null` for missing |
| `doc.update` | `{collection, _id, fields}` | The updated document |
| `doc.remove` | `{collection, _id}` | The removed document or `null` |
| `doc.exists` | `{collection, _id}` | `true` / `false` |
| `doc.count` | `{collection}` | Number of documents |
| `doc.search` | `{collection, query}` | Matching documents |
| `doc.search_ids` | `{collection, query}` | Matching document IDs |
| `doc.query_ids` | `{collection, filter, limit?}` | IDs matching a B-Tree filter |

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
the index definitions when it actually creates (or first loads) it.

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
