# Anda DB

[![Build Status](https://github.com/ldclabs/anda-db/actions/workflows/test.yml/badge.svg)](https://github.com/ldclabs/anda-db/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/ldclabs/anda-db/blob/main/LICENSE)

**Anda DB** is a next-generation, embedded database system written in Rust, specifically engineered for AI applications and intelligent agents. It functions as a powerful library for managing complex knowledge and memory, supporting multi-modal data, full-text search, and high-performance vector similarity search.

At its heart, Anda DB powers the **[Anda Cognitive Nexus](./rs/anda_cognitive_nexus/)**, an advanced long-term memory system for AI. By implementing the **KIP (Knowledge Interaction Protocol)**, it enables an AI to build, query, and reason over a persistent and evolving knowledge graph.

## Key Features

-   **Embedded & Performant:** Designed as a Rust library, not a standalone server, for direct integration and high performance within your application.
-   **Multi-Modal Data:** Natively store and index diverse data types, including documents, key-value pairs, and vector embeddings (`bfloat16`).
-   **Pluggable & Secure Storage:** Built on the [`object_store`](https://docs.rs/object_store) crate, allowing for various storage backends (local filesystem, AWS S3, GCS) with optional, transparent AES-256-GCM encryption at rest.
-   **Advanced Indexing:**
    -   **B-Tree Index:** For efficient, exact-match, and range-based queries.
    -   **BM25 Index:** For robust, full-text search capabilities.
    -   **HNSW Index:** For state-of-the-art, approximate nearest neighbor (ANN) vector search.
-   **Hybrid Search:** Intelligently combines full-text (BM25) and vector (HNSW) search results using Reciprocal Rank Fusion (RRF) to deliver more relevant and comprehensive answers.
-   **Knowledge Graph Engine:** Implements the **KIP (Knowledge Interaction Protocol)**, a declarative language for manipulating (KML) and querying (KQL) the database as a graph of concepts and propositions.
-   **Flexible Schema:** A document-oriented model with a derive macro (`AndaDBSchema`) for easily defining and evolving data schemas.
-   **Transactional & Persistent:** Supports incremental index updates and ensures data durability, allowing the entire database state to be saved and loaded efficiently.

## Architecture

Anda DB features a highly modular design, with each crate providing a distinct component of its functionality:

-   `anda_db`: The core database engine that integrates collections, indexing, and query execution.
-   `anda_cognitive_nexus`: The high-level implementation of the KIP-based knowledge graph, providing long-term memory for AI agents.
-   `anda_kip`: A complete parser and execution framework for the KIP language (KQL and KML).
-   `anda_db_schema`: Defines the data structures, types, and schema system.
-   `anda_db_derive`: Provides the procedural macros (`AndaDBSchema`) for convenient schema definition.
-   `anda_object_store`: A wrapper around `object_store` that adds metadata and transparent encryption.
-   `anda_db_btree`: The B-Tree index implementation.
-   `anda_db_tfs`: The full-text search (BM25) implementation.
-   `anda_db_hnsw`: The Hierarchical Navigable Small World (HNSW) vector index implementation.

## Getting Started

Add `anda_db` and its related components to your `Cargo.toml`.

```toml
[dependencies]
anda_db = "0.7"
anda_cognitive_nexus = "0.6"
anda_kip = "0.6"
# Add an object_store backend, e.g., object_store = { version = "0.12", features = ["local"] }
tokio = { version = "1", features = ["full"] }
```

### Example: Basic Database Usage

The following example demonstrates setting up a database, defining a schema, adding documents, and performing a hybrid search query.

```rust
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::{AndaDB, DBConfig},
    error::DBError,
    index::HnswConfig,
    query::{Filter, Query, RangeQuery, Search},
    schema::{AndaDBSchema, FieldType, Fv, Json, Vector, vector_from_f32},
};
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
pub struct Knowledge {
    pub _id: u64,
    pub description: String,
    pub embedding: Vector,
    pub author: String,
}

#[tokio::main]
async fn main() -> Result<(), DBError> {
    // 1. Initialize storage
    let object_store = MetaStoreBuilder::new(LocalFileSystem::new_with_prefix("./db")?, 10000).build();
    let db = AndaDB::connect(Arc::new(object_store), DBConfig::default()).await?;

    // 2. Define a collection and its indexes
    let collection = db
        .open_or_create_collection(Knowledge::schema()?, CollectionConfig::new("knowledge"), |coll| async {
            coll.create_btree_index_nx(&["author"]).await?;
            coll.create_bm25_index_nx(&["description"]).await?;
            coll.create_hnsw_index_nx("embedding", HnswConfig { dimension: 4, ..Default::default() }).await?;
            Ok(())
        })
        .await?;

    // 3. Add documents
    collection.add_from(&Knowledge {
        _id: 0,
        description: "Rust is a systems programming language focused on safety.".to_string(),
        embedding: vector_from_f32(vec![0.1, 0.2, 0.3, 0.4]),
        author: "Graydon".to_string(),
    }).await?;

    collection.add_from(&Knowledge {
        _id: 0,
        description: "A vector database is used for similarity search.".to_string(),
        embedding: vector_from_f32(vec![0.5, 0.6, 0.7, 0.8]),
        author: "Anda".to_string(),
    }).await?;

    collection.flush(anda_db::unix_ms()).await?;

    // 4. Perform a hybrid search
    let query_text = "language";
    let query_vector = vec![0.15, 0.25, 0.35, 0.45];

    let results: Vec<Knowledge> = collection.search_as(Query {
        search: Some(Search {
            text: Some(query_text.to_string()),
            vector: Some(query_vector),
            ..Default::default()
        }),
        ..Default::default()
    }).await?;

    println!("Found {} results for query '{}':", results.len(), query_text);
    for doc in results {
        println!("- ID: {}, Description: {}", doc._id, doc.description);
    }

    db.close().await?;
    Ok(())
}
```

### Example: Using the Cognitive Nexus with KIP

The `anda_cognitive_nexus` provides a higher-level API for interacting with the database as a knowledge graph using KML (Knowledge Manipulation Language) and KQL (Knowledge Query Language).

```rust
use anda_cognitive_nexus::{CognitiveNexus, KipError};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{parse_kml, parse_kql};
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), KipError> {
    // 1. Setup database and connect to the nexus
    let object_store = MetaStoreBuilder::new(LocalFileSystem::new_with_prefix("./nexus_db")?, 10000).build();
    let db = AndaDB::connect(Arc::new(object_store), DBConfig::default()).await?;
    let nexus = CognitiveNexus::connect(Arc::new(db), |_| async { Ok(()) }).await?;

    // 2. Insert knowledge using KML
    let kml_str = r#"
    UPSERT {
        CONCEPT ?rust { {type: "Language", name: "Rust"} }
        CONCEPT ?db { {type: "Field", name: "Database"} }
        PROPOSITION (?rust, "is_good_for", ?db)
    }
    "#;
    nexus.execute_kml(parse_kml(kml_str)?, false).await?;

    // 3. Query the knowledge graph using KQL
    let kql_str = r#"
    FIND(?lang.name)
    WHERE {
        ?lang {type: "Language"}
        (?lang, "is_good_for", {type: "Field", name: "Database"})
    }
    "#;
    let (kql_result, _) = nexus.execute_kql(parse_kql(kql_str)?).await?;

    println!("KQL Query Result: {:#?}", kql_result);

    nexus.close().await?;
    Ok(())
}
```

## Building and Testing

To build the project and run the tests, use the standard Cargo commands:

```bash
# Build in release mode
cargo build --release

# Run all tests
cargo test
```

## License

Anda DB is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

Copyright © 2025 [LDC Labs](https://github.com/ldclabs)