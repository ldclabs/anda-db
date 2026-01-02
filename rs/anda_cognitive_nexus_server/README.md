# `anda_cognitive_nexus_server`

`anda_cognitive_nexus_server` is a high-performance server implementation of the **Knowledge Interaction Protocol (KIP)**, built on top of [Anda DB](../anda_db). It serves as a **Cognitive Nexus**, providing AI agents with a structured, persistent, and verifiable long-term memory system.

## Features

- **KIP Implementation**: Full support for Knowledge Interaction Protocol (KQL/KML) for managing Concept Nodes and Proposition Links.
- **Memory Persistence**: Leverages Anda DB for efficient storage and retrieval of knowledge capsules.
- **JSON-RPC API**: Simple and extensible interface for executing KIP commands and querying logs.
- **Security**: Optional API Key authentication via Bearer tokens.
- **Flexible Storage**: Supports local file system and in-memory storage backends.

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/) (latest stable version)
- [Docker](https://www.docker.com/) (optional, for containerized deployment)

### Running with Cargo

To start the server with a local database:

```bash
cargo run -p anda_cognitive_nexus_server -- local --db ./db
```

### Running with Docker

You can use the official Docker image:

```bash
docker run -d \
  -p 8080:8080 \
  -e API_KEY=your_secret_key \
  -v $(pwd)/db:/app/db \
  ghcr.io/ldclabs/anda_cognitive_nexus_server_amd64:latest \
  local --db /app/db
```

## Configuration

The server can be configured via command-line arguments or environment variables:

| Argument    | Environment Variable | Description                                      | Default          |
| ----------- | -------------------- | ------------------------------------------------ | ---------------- |
| `--addr`    | `ADDR`               | Port to listen on                                | `127.0.0.1:8080` |
| `--api-key` | `API_KEY`            | Optional API key for authentication              | None             |
| `--db`      | `LOCAL_DB_PATH`      | Path to the local database (for `local` command) | `./db`           |

## API Reference

### 1. Get Server Information
**Endpoint:** `GET /`

**Response:**
```json
{
  "name": "anda_cognitive_nexus_server",
  "version": "0.1.0"
}
```

### 2. KIP Interface
**Endpoint:** `POST /kip`

**Headers:**
- `Content-Type: application/json`
- `Authorization: Bearer <API_KEY>` (if configured)

**Request Body (JSON-RPC):**

#### Execute KIP
Executes a KIP request (KQL or KML).

```json
{
  "method": "execute_kip",
  "params": {
    "command": "DESCRIBE PRIMER"
  }
}
```

#### List Logs
Retrieves the history of KIP executions.

```json
{
  "method": "list_logs",
  "params": {
    "cursor": null,
    "limit": 10
  }
}
```

## License
Copyright © 2025 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See the [MIT license][license] for the full license text.

[license]: ./../LICENSE-MIT
