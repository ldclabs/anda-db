# Anda DB Server

A HTTP server for [Anda DB](../anda_db/README.md), exposing a lightweight RPC interface (JSON/CBOR).

## Features

- RPC-style API over HTTP (`POST /` and `POST /{db_name}`)
- JSON and CBOR request/response support
- Multiple database instances in one server process
- Full collection/document CRUD and search/query APIs
- Optional API key authentication (`Authorization: Bearer <api-key>`)

## Quick Start

```bash
# Start with local filesystem storage
cargo run -p anda_db_server -- local --db ./debug/db

# Start with in-memory storage
cargo run -p anda_db_server

# Custom address + API key
cargo run -p anda_db_server -- --addr 0.0.0.0:9090 --api-key my-secret local --db ./data
```

### Environment Variables

| Variable        | Description                | Default          |
| --------------- | -------------------------- | ---------------- |
| `ADDR`          | Listen address             | `127.0.0.1:8080` |
| `API_KEY`       | API key for authentication | (none)           |
| `LOCAL_DB_PATH` | Path for local storage     | `./db`           |

## Protocol

This server borrows an RPC-like shape, but is intentionally simplified.

### Request Format

```json
{
  "method": "get_db_metadata",
  "params": {
    "collection": "articles"
  }
}
```

- `method`: required
- `params`: optional

### Response Format

Success:

```json
{
  "result": {...}
}
```

Error:

```json
{
  "error": {
    "code": -32001,
    "message": "database not found: demo"
  }
}
```

## Endpoints

### `POST /` (Root-level methods)

- `get_information`
- `create_database` (`params: { name, description? }`)
- `list_databases`

### `POST /{db_name}` (Database-scoped methods)

- `get_information`
- `get_db_metadata`
- `flush_db`
- `create_collection`
- `get_collection_metadata`
- `delete_collection`
- `add_document`
- `get_document`
- `update_document`
- `remove_document`
- `search_documents`
- `search_document_ids`
- `query_document_ids`

## JSON Example

Create a database:

```bash
curl -X POST http://127.0.0.1:8080/ \
  -H 'Content-Type: application/json' \
  -d '{"method":"create_database","params":{"name":"tenant_a"}}'
```

Get database metadata:

```bash
curl -X POST http://127.0.0.1:8080/tenant_a \
  -H 'Content-Type: application/json' \
  -d '{"method":"get_db_metadata"}'
```

## CBOR Support

- Request format is selected by `Content-Type`:
  - `application/json` → JSON body
  - `application/cbor` → CBOR body
- Response format is selected by `Accept`:
  - `application/json`
  - `application/cbor`

Under CBOR protocol, request `params` and response `result` use native CBOR `Value` encoding.

## License

MIT
