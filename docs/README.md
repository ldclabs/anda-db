# AndaDB Documentation Hub

This directory is the technical documentation hub for the AndaDB workspace.
Each document focuses on one layer of the stack, from the embedded database
engine to schema derivation, index internals, object-store-backed persistence,
the [KIP protocol](https://github.com/ldclabs/kip), and the Cognitive Nexus knowledge graph.

If you are new to the project, start here and then jump into the document that
matches the layer you are working on.

## What This Documentation Covers

The documents in this directory are intended to answer different kinds of
questions:

- What the core embedded database does and how to use it
- How schemas, documents, and derive macros work
- How exact-match, lexical, and vector indexes behave internally
- How persistence is built on top of [`anda_object_store`](./anda_object_store.md)
- How [KIP](https://github.com/ldclabs/kip) is parsed and executed
- How the Cognitive Nexus turns AndaDB into an AI memory graph

This directory is not meant to duplicate every crate README. For service-level
deployment instructions, environment variables, and binary-specific usage,
also check the crate READMEs under `rs/`.

## Recommended Reading Paths

### Embedded database users

If you want to embed AndaDB directly into a Rust application:

1. [anda_db.md](./anda_db.md)
2. [anda_db_schema.md](./anda_db_schema.md)
3. [anda_db_derive.md](./anda_db_derive.md)
4. [anda_db_btree.md](./anda_db_btree.md)
5. [anda_db_tfs.md](./anda_db_tfs.md)
6. [anda_db_hnsw.md](./anda_db_hnsw.md)
7. [anda_object_store.md](./anda_object_store.md)

### Knowledge-graph and agent-memory users

If you are building higher-level AI memory or KIP-powered systems:

1. [anda_kip.md](./anda_kip.md)
2. [anda_cognitive_nexus.md](./anda_cognitive_nexus.md)
3. [anda_db.md](./anda_db.md)
4. [anda_object_store.md](./anda_object_store.md)

### Storage and deployment implementers

If your main concern is persistence, durability, encryption, or portability
across storage backends:

1. [anda_object_store.md](./anda_object_store.md)
2. [anda_db.md](./anda_db.md)
3. [anda_db_btree.md](./anda_db_btree.md)
4. [anda_db_tfs.md](./anda_db_tfs.md)
5. [anda_db_hnsw.md](./anda_db_hnsw.md)

## Documentation Map

| Document                                             | Layer                   | What it Covers                                                                                          | Read It When You Need To                                                  |
| ---------------------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [anda_db.md](./anda_db.md)                           | Core database           | Database lifecycle, collections, indexing model, query model, storage integration, durability, recovery | Understand the main embedded database API and its operational behavior    |
| [anda_db_schema.md](./anda_db_schema.md)             | Type system             | Field types, field values, schemas, documents, resource model, serialization                            | Design schemas, validate document shapes, or inspect on-disk typing rules |
| [anda_db_derive.md](./anda_db_derive.md)             | Code generation         | `AndaDBSchema`, `FieldTyped`, attributes, type inference, `field_type` DSL                              | Generate schemas from Rust structs and understand macro behavior          |
| [anda_db_btree.md](./anda_db_btree.md)               | Exact/range retrieval   | Inverted B-tree design, range queries, bucket persistence, correctness notes                            | Work on filtering, exact lookup, uniqueness, or bucket compaction         |
| [anda_db_tfs.md](./anda_db_tfs.md)                   | Full-text retrieval     | BM25 algorithm, tokenizer pipeline, bucket sharding, persistence layout                                 | Work on lexical search, text ranking, or tokenizer behavior               |
| [anda_db_hnsw.md](./anda_db_hnsw.md)                 | Vector retrieval        | HNSW ANN index, bf16 vectors, insertion pipeline, persistence artifacts                                 | Work on embedding search and vector-index tuning                          |
| [anda_object_store.md](./anda_object_store.md)       | Storage substrate       | `MetaStore`, `EncryptedStore`, portable conditional writes, AES-256-GCM chunked encryption              | Build portable or encrypted object-store-backed persistence               |
| [anda_kip.md](./anda_kip.md)                         | Protocol layer          | KIP parser, AST, request/response model, executor interface, error codes                                | Integrate LLM-facing protocol handling or implement a backend             |
| [anda_cognitive_nexus.md](./anda_cognitive_nexus.md) | Knowledge graph runtime | Reference KIP executor, concept/proposition model, graph execution, bootstrap flow                      | Build or debug the AI memory brain on top of AndaDB                       |

## Layered View of the Stack

```text
Application / Agent Runtime
  -> anda_kip                    protocol, AST, request/response, executor trait
  -> anda_cognitive_nexus        reference KIP executor and knowledge graph
  -> anda_db                     embedded storage and retrieval core
     -> anda_db_schema           schema and document model
     -> anda_db_derive           derive macros for schema generation
     -> anda_db_btree            exact and range index
     -> anda_db_tfs              BM25 full-text index
     -> anda_db_hnsw             HNSW vector index
     -> anda_object_store        portable metadata and encryption wrappers
     -> object_store             backend abstraction for local and cloud storage
```

This structure is deliberate:

- `anda_db` is the embedded storage and retrieval core
- `anda_kip` is the protocol layer for model-friendly knowledge interaction
- `anda_cognitive_nexus` is the reference graph-memory system built on top of both

## Service-Layer Documentation

Some operational components are documented primarily in their own crate
READMEs rather than in this directory.

Use these when you need deployment guidance:

- `rs/anda_db_server/README.md` for the core database HTTP server
- `rs/anda_cognitive_nexus_server/README.md` for the KIP HTTP/JSON-RPC server
- `rs/anda_db_shard_proxy/README.md` for shard routing and multi-tenant proxying

## Practical Entry Points

If you need a fast answer to a concrete task, start with the closest document:

- Adding a collection and hybrid search: [anda_db.md](./anda_db.md)
- Designing field layouts and migration rules: [anda_db_schema.md](./anda_db_schema.md)
- Using derive macros on Rust structs: [anda_db_derive.md](./anda_db_derive.md)
- Debugging range filters: [anda_db_btree.md](./anda_db_btree.md)
- Tuning BM25 or tokenization: [anda_db_tfs.md](./anda_db_tfs.md)
- Tuning vector search recall and memory use: [anda_db_hnsw.md](./anda_db_hnsw.md)
- Understanding portable conditional writes or encryption-at-rest: [anda_object_store.md](./anda_object_store.md)
- Parsing or executing KIP: [anda_kip.md](./anda_kip.md)
- Understanding the AI memory graph runtime: [anda_cognitive_nexus.md](./anda_cognitive_nexus.md)

## Relation to the Project Root README

The project root [README.md](../README.md) is the product-level overview:

- what AndaDB is
- what the workspace crates are for
- how to get started quickly
- where to find the main examples

This `docs/README.md` is the documentation index for deeper technical study.