# Anda Cognitive Nexus

[![Crates.io](https://img.shields.io/crates/v/anda_cognitive_nexus.svg)](https://crates.io/crates/anda_cognitive_nexus) [![Docs.rs](https://docs.rs/anda_cognitive_nexus/badge.svg)](https://docs.rs/anda_cognitive_nexus)

**Anda Cognitive Nexus** is a Rust implementation of **KIP (Knowledge Interaction Protocol)** built on top of [anda_db](https://github.com/ldclabs/anda-db/tree/main/rs/anda_db). It provides a persistent, graph-based long-term memory substrate for AI agents (concepts + propositions) with a KIP executor API.

Links:

- **KIP spec**: https://github.com/ldclabs/KIP
- **HTTP server** (optional): https://github.com/ldclabs/anda-db/tree/main/rs/anda_cognitive_nexus_server
- **Database core**: https://github.com/ldclabs/anda-db/tree/main/rs/anda_db

## What is KIP?

**KIP (Knowledge Interaction Protocol)** is a specialized protocol designed for Large Language Models (LLMs). It establishes a standard for efficient, reliable, and bidirectional knowledge exchange between an LLM (the "neural core") and a knowledge graph (the "symbolic core"). This allows AI Agents to build a memory that is not only queryable but also auditable and capable of evolution.

### Key Design Principles

*   **LLM-Friendly**: Declarative syntax that is easy for LLMs/tools to generate.
*   **Graph-Native**: Optimized for knowledge graph patterns.
*   **Auditable**: Queries and mutations can be logged and reviewed as an execution trail.
*   **Lifecycle-aware**: Supports querying, inserting, and evolving knowledge over time.

## Core Concepts

*   **Cognitive Nexus**: The knowledge graph itself, composed of Concept Nodes and Proposition Links.
*   **Concept Node**: An entity or abstract concept (e.g., a `Drug` named "Aspirin"). Each node has a type, a name, attributes, and metadata.
*   **Proposition Link**: A reified fact that connects two nodes in a `(subject, predicate, object)` structure (e.g., `(Aspirin, treats, Headache)`).
*   **Knowledge Capsule**: An atomic unit of knowledge, containing a set of nodes and links, used for transactional updates to the nexus.

## Features

*   **KIP executor**: Runs KQL / KML / META commands via `anda_kip`.
*   **Persistent & performant**: Built on Anda DB for durable storage and indexing.
*   **Self-describing schema**: Concept/proposition types live inside the graph.
*   **Async API**: Designed for modern, non-blocking applications.

## Getting Started

Add dependencies to your `Cargo.toml` (pick a concrete `object_store` backend):

```toml
[dependencies]
anda_cognitive_nexus = "0.6"
anda_kip = "0.6"
tokio = { version = "1", features = ["full"] }

# Provide an ObjectStore implementation
object_store = { version = "0.13", features = ["fs"] }

# Optional (recommended for local filesystem): metadata + conditional put support
anda_object_store = "0.3"
```

### Quickstart (in-memory)

This example boots a Nexus on an in-memory object store, inserts a small capsule via KML, and queries it via KQL.

```rust
use anda_cognitive_nexus::{CognitiveNexus, KipError};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{parse_kml, parse_kql};
use object_store::memory::InMemory;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), KipError> {
    // 1) Set up storage and database
    let db = AndaDB::connect(Arc::new(InMemory::new()), DBConfig::default()).await?;

    // 2) Connect to the Cognitive Nexus
    let nexus = CognitiveNexus::connect(Arc::new(db), async |_nexus| Ok(()) ).await?;
    println!("Connected to Anda Cognitive Nexus: {}", nexus.name());

    // 3) Manipulate Knowledge with KML
    let kml_string = r#"
    UPSERT {
        CONCEPT ?drug_type {
            {type: "$ConceptType", name: "Drug"}
            SET ATTRIBUTES { description: "Pharmaceutical drug concept type" }
        }

        CONCEPT ?symptom_type {
            {type: "$ConceptType", name: "Symptom"}
            SET ATTRIBUTES { description: "Medical symptom concept type" }
        }

        CONCEPT ?treats_relation {
            {type: "$PropositionType", name: "treats"}
            SET ATTRIBUTES { description: "Drug treats symptom relationship" }
        }

        CONCEPT ?headache {
            {type: "Symptom", name: "Headache"}
            SET ATTRIBUTES { severity_scale: "1-10", description: "Pain in the head or neck area" }
        }

        CONCEPT ?aspirin {
            {type: "Drug", name: "Aspirin"}
            SET ATTRIBUTES { molecular_formula: "C9H8O4", risk_level: 1 }
            SET PROPOSITIONS {
                ("treats", {type: "Symptom", name: "Headache"})
            }
        }
    }
    WITH METADATA { source: "Basic Medical Knowledge" }
    "#;

    let kml_commands = parse_kml(kml_string)?;
    let kml_result = nexus.execute_kml(kml_commands, false).await?;
    println!("KML Execution Result: {:#?}", kml_result);

    // 4) Query Knowledge with KQL
    let kql_query = r#"
    FIND(?drug.name, ?drug.attributes.risk_level)
    WHERE {
        ?drug {type: "Drug"}
        (?drug, "treats", {type: "Symptom", name: "Headache"})
    }
    "#;

    let (kql_result, _) = nexus.execute_kql(parse_kql(kql_query)?).await?;
    println!("KQL Query Result: {:#?}", kql_result);

    nexus.close().await?;
    Ok(())
}
```

### LLM-friendly request/response

If you want a single structured interface (handy for function calling), use `anda_kip::Request` / `Response`. This is also what `anda_cognitive_nexus_server` expects under `POST /kip` with `method=execute_kip`.

```rust
use anda_kip::{Request, Response};

let req = Request {
    command: "DESCRIBE PRIMER".to_string(),
    ..Default::default()
};

let (_ty, resp): (_, Response) = req.execute(&nexus).await;
println!("{resp:?}");
```

## Run the Demo

This repository includes a comprehensive demo: https://github.com/ldclabs/anda-db/tree/main/rs/anda_cognitive_nexus/examples/kip_demo.rs

To run it:

```bash
mkdir -p ./debug/metastore
cargo run -p anda_cognitive_nexus --example kip_demo
```

## Related

- `anda_kip`: parser + request/response model for KIP.
- `anda_db`: embedded storage engine.
- `anda_cognitive_nexus_server`: expose KIP over HTTP (JSON-RPC).

## License

Copyright © 2026 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.
```
