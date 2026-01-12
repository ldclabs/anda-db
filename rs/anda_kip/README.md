# Anda KIP

> A Rust SDK of KIP (Knowledge Interaction Protocol) for building sustainable AI knowledge memory systems.

[![Crates.io](https://img.shields.io/crates/v/anda_kip.svg)](https://crates.io/crates/anda_kip)
[![Documentation](https://docs.rs/anda_kip/badge.svg)](https://docs.rs/anda_kip)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

## Overview

**🧬 KIP (Knowledge Interaction Protocol)** is a knowledge memory interaction protocol designed for Large Language Models (LLMs), aimed at building sustainable learning and self-evolving knowledge memory systems for AI Agents.

This crate provides a complete Rust SDK of the KIP specification, offering:

- **Parser**: Full KIP command parsing with comprehensive error handling
- **AST**: Rich Abstract Syntax Tree structures for all KIP command types
- **Executor Framework**: Trait-based execution system for implementing KIP backends
- **Request/Response**: Standardized JSON-based communication structures
- **Type Safety**: Leverages Rust's type system for reliable KIP command processing

## Specification

This implementation follows the official KIP specification. For detailed information about the protocol, syntax, and semantics, please refer to:

**👉 [KIP Specification](https://github.com/ldclabs/KIP)**

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
anda_kip = "0.4"
```

### Basic Usage

```rust
use anda_kip::{parse_kip, Command, Executor, Response, KipError};

// Parse a KQL query
let query = parse_kip(r#"
    FIND(?drug.name, ?drug.attributes.risk_level)
    WHERE {
        ?drug {type: "Drug"}
        ?headache {name: "Headache"}
        (?drug, "treats", ?headache)

        FILTER(?drug.attributes.risk_level < 3)
    }
    ORDER BY ?drug.attributes.risk_level ASC
    LIMIT 10
"#)?;

// Parse a KML statement
let statement = parse_kip(r#"
    UPSERT {
        CONCEPT ?new_drug {
            { type: "Drug", name: "Aspirin" }
            SET ATTRIBUTES {
                molecular_formula: "C9H8O4",
                risk_level: 1
            }
            SET PROPOSITIONS {
                ("treats", { type: "Symptom", name: "Headache" })
                ("is_class_of", { type: "DrugClass", name: "NSAID" })
            }
        }
    }
    WITH METADATA {
        source: "Medical Database v2.1",
        confidence: 0.95
    }
"#)?;

// Parse a META command
let meta = parse_kip("DESCRIBE PRIMER")?;
```

### Implementing an Executor

```rust
use anda_kip::{Executor, Command, Json, KipError, Response};
use async_trait::async_trait;

pub struct MyKnowledgeGraph {
    // Your knowledge graph implementation
}

#[async_trait(?Send)]
impl Executor for MyKnowledgeGraph {
    async fn execute(&self, command: Command, dry_run: bool) -> Response {
        match command {
            Command::Kql(query) => {
                // Execute KQL query against your knowledge graph
                todo!("Implement KQL execution")
            },
            Command::Kml(statement) => {
                // Execute KML statement to modify knowledge graph
                todo!("Implement KML execution")
            },
            Command::Meta(meta_cmd) => {
                // Execute META command for introspection
                todo!("Implement META execution")
            }
        }
    }
}
```

### High-Level Execution

```rust
use anda_kip::{execute_kip, Request, Response};

// Using the high-level execution function
let executor = MyKnowledgeGraph::new();
let response = execute_kip(&executor, "FIND(?x) WHERE { ?x {type: \"Drug\"} }").await?;

// Using structured requests with parameters
let request = Request {
    command: "FIND(?drug) WHERE { ?drug {type: \"Drug\", name: $drug_name} }".to_string(),
    parameters: [("drug_name".to_string(), json!("Aspirin"))].into_iter().collect(),
    dry_run: false,
};

let response = request.execute(&executor).await?;
```

## Architecture

The crate is organized into several key modules:

- **`ast`**: Abstract Syntax Tree definitions for all KIP constructs
- **`capsule`**: KIP Genesis Capsules
- **`error`**: Comprehensive error types and handling
- **`executor`**: Execution framework and traits
- **`parser`**: Nom-based parsers for KQL, KML, and META commands
- **`request`**: Request/Response structures for JSON-based communication
- **`types`**: KIP Entity Types

## Contributing

We welcome contributions! Please feel free to submit issues, feature requests, or pull requests.

## License

Copyright © 2026 [LDC Labs](https://github.com/ldclabs).

`anda_kip` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.
