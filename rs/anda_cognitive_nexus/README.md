# Anda Cognitive Nexus

[![Crates.io](https://img.shields.io/crates/v/anda_cognitive_nexus.svg)](https://crates.io/crates/anda_cognitive_nexus) [![Docs.rs](https://docs.rs/anda_cognitive_nexus/badge.svg)](https://docs.rs/anda_cognitive_nexus)

**Anda Cognitive Nexus** is a Rust implementation of the **KIP (Knowledge Interaction Protocol)**, built on top of [Anda DB](https://github.com/ldclabs/anda-db/tree/main/rs/anda_db). It provides a powerful, persistent, and graph-based long-term memory system for AI Agents, enabling them to learn, reason, and evolve.

**👉 [Read the full KIP Specification](https://github.com/ldclabs/KIP)**

## What is KIP?

**KIP (Knowledge Interaction Protocol)** is a specialized protocol designed for Large Language Models (LLMs). It establishes a standard for efficient, reliable, and bidirectional knowledge exchange between an LLM (the "neural core") and a knowledge graph (the "symbolic core"). This allows AI Agents to build a memory that is not only queryable but also auditable and capable of evolution.

### Key Design Principles

*   **LLM-Friendly**: A clean, declarative syntax that is easy for LLMs to generate and parse.
*   **Graph-Native**: Optimized for the structure and query patterns of knowledge graphs.
*   **Explainable**: KIP queries and manipulations serve as a transparent, auditable "chain of thought" for an AI's reasoning process.
*   **Comprehensive**: Manages the full lifecycle of knowledge, from initial query to long-term evolution and learning.

## Core Concepts

*   **Cognitive Nexus**: The knowledge graph itself, composed of Concept Nodes and Proposition Links.
*   **Concept Node**: An entity or abstract concept (e.g., a `Drug` named "Aspirin"). Each node has a type, a name, attributes, and metadata.
*   **Proposition Link**: A reified fact that connects two nodes in a `(subject, predicate, object)` structure (e.g., `(Aspirin, treats, Headache)`).
*   **Knowledge Capsule**: An atomic unit of knowledge, containing a set of nodes and links, used for transactional updates to the nexus.

## Features

*   **Full KIP Implementation**: Provides both the **Knowledge Query Language (KQL)** and **Knowledge Manipulation Language (KML)**.
*   **Persistent & Performant**: Built on Anda DB for efficient, durable storage.
*   **Self-Describing Schema**: The types for concepts and propositions are themselves defined within the graph, allowing for a flexible and extensible knowledge structure.
*   **Async API**: Designed for modern, non-blocking applications.

## Getting Started

Add `anda_cognitive_nexus` to your `Cargo.toml`:

```toml
[dependencies]
anda_cognitive_nexus = { version = "0.6" }
```

### Example Usage

Here's a brief example of how to initialize the nexus, insert knowledge using KML, and retrieve it with KQL.

```rust
use anda_cognitive_nexus::{CognitiveNexus, KipError};
use anda_db::{database::{AndaDB, DBConfig}, storage::StorageConfig};
use anda_kip::{parse_kml, parse_kql};
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), KipError> {
    // 1. Set up storage and database
    let object_store = MetaStoreBuilder::new(LocalFileSystem::new_with_prefix("./db")?, 10000).build();
    let db_config = DBConfig::default();
    let db = AndaDB::connect(Arc::new(object_store), db_config).await?;

    // 2. Connect to the Cognitive Nexus
    let nexus = CognitiveNexus::connect(Arc::new(db), |_| async { Ok(()) }).await?;
    println!("Connected to Anda Cognitive Nexus: {}", nexus.name());

    // 3. Manipulate Knowledge with KML (Knowledge Manipulation Language)
    let kml_string = r#"
    UPSERT {
        // Define concept types
        CONCEPT ?drug_type {
            {type: "$ConceptType", name: "Drug"}
            SET ATTRIBUTES {
                description: "Pharmaceutical drug concept type"
            }
        }

        CONCEPT ?symptom_type {
            {type: "$ConceptType", name: "Symptom"}
            SET ATTRIBUTES {
                description: "Medical symptom concept type"
            }
        }

        // Define relation types
        CONCEPT ?treats_relation {
            {type: "$PropositionType", name: "treats"}
            SET ATTRIBUTES {
                description: "Drug treats symptom relationship"
            }
        }

        // Create symptom concepts
        CONCEPT ?headache {
            {type: "Symptom", name: "Headache"}
            SET ATTRIBUTES {
                severity_scale: "1-10",
                description: "Pain in the head or neck area"
            }
        }

        // Create a drug and the symptom it treats
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

    // 4. Query Knowledge with KQL (Knowledge Query Language)
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

## Run the Demo

This repository includes a [comprehensive demo](https://github.com/ldclabs/anda-db/tree/main/rs/anda_cognitive_nexus/examples/kip_demo.rs) that showcases more advanced KML and KQL features. To run it:

```bash
mkdir -p ./debug/metastore
cargo run --example kip_demo
```

## License

Copyright © 2025 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.
```
