# Anda KIP

> A Rust SDK of KIP (Knowledge Interaction Protocol) for building sustainable AI knowledge memory systems.

[![Crates.io](https://img.shields.io/crates/v/anda_kip.svg)](https://crates.io/crates/anda_kip)
[![Documentation](https://docs.rs/anda_kip/badge.svg)](https://docs.rs/anda_kip)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

## Overview

**KIP (Knowledge Interaction Protocol)** is a knowledge memory interaction protocol designed for Large Language Models (LLMs), aimed at building sustainable learning and self-evolving knowledge memory systems for AI Agents.

This crate provides a complete Rust SDK of the KIP specification, offering:

- **Parser**: Full KIP command parsing with comprehensive error handling using `nom` combinator library
- **AST**: Rich Abstract Syntax Tree structures for all KIP command types
- **Executor Framework**: Trait-based execution system for implementing KIP backends
- **Request/Response**: Standardized JSON-based communication structures with batch command support
- **Type Safety**: Leverages Rust's type system for reliable KIP command processing
- **Genesis Capsules**: Pre-defined knowledge capsules for bootstrapping cognitive systems

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                              lib.rs                                  │
│  Re-exports: ast, capsule, error, executor, parser, request, types  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
┌───────────────┐        ┌─────────────────┐        ┌─────────────────┐
│     ast.rs     │        │   executor.rs   │        │   request.rs   │
│               │        │                 │        │                │
│ - Command     │        │ - Executor trait│        │ - Request      │
│ - KqlQuery    │◄───────│ - execute_kip   │        │ - CommandItem  │
│ - KmlStatement│        │ - execute_      │        │ - Response     │
│ - MetaCommand │        │   readonly      │        │ - ErrorObject  │
└───────┬───────┘        └─────────────────┘        └────────┬────────┘
        │                                                  │
        │                    ┌─────────────────┐           │
        ▼                    │   error.rs      │           │
┌───────────────┐           │                 │           │
│  types.rs     │           │ - KipError      │           │
│               │           │ - KipErrorCode  │           │
│ - Entity      │           │ - format_       │           │
│ - ConceptNode │           │   nom_error    │           │
│ - PropositionLink          └─────────────────┘           │
└───────────────┘                                           │
        │                                                   │
        ▼                    ┌─────────────────┐            │
┌───────────────┐           │   parser.rs     │            │
│  capsule.rs   │           │                 │            │
│               │           │ - parse_kip     │            │
│ - META_*_TYPE │           │ - parse_kql     │            │
│ - GENESIS_KIP │           │ - parse_kml     │            │
│ - *_KIP       │           │ - parse_meta    │            │
└───────────────┘           │ - parse_json    │            │
                            └────────┬────────┘            │
                                     │                     │
         ┌───────────────────────────┼─────────────────────┤
         │                           │                     │
         ▼                           ▼                     ▼
┌─────────────────┐        ┌─────────────────┐    ┌─────────────────┐
│   parser/       │        │   parser/       │    │   parser/       │
│   common.rs     │        │   kql.rs        │    │   kml.rs        │
│                 │        │                 │    │                 │
│ - identifier    │        │ - FIND clause   │    │ - UPSERT block  │
│ - variable      │        │ - WHERE clause  │    │ - DELETE stmt   │
│ - dot_path_var  │        │ - FILTER expr   │    │ - CONCEPT block │
│ - braced_block  │        │ - ORDER BY      │    │ - PROPOSITION   │
│ - parenthesized │        │ - LIMIT/CURSOR  │    │   block         │
└─────────────────┘        └─────────────────┘    └─────────────────┘
                                     │
                                     ▼
                            ┌─────────────────┐
                            │   parser/       │
                            │   meta.rs       │
                            │                 │
                            │ - DESCRIBE      │
                            │ - SEARCH         │
                            └─────────────────┘
                                     │
                                     ▼
                            ┌─────────────────┐
                            │   parser/       │
                            │   json.rs       │
                            │                 │
                            │ - json_value    │
                            │ - quoted_string │
                            │ - skip_ws_...   │
                            └─────────────────┘
```

## KIP Command Types

KIP defines three main command types for interacting with the Cognitive Nexus:

### KQL - Knowledge Query Language

Used for knowledge retrieval and reasoning. Supports complex graph pattern matching, filtering, aggregation, and result ordering.

```sql
FIND(?drug.name, ?drug.attributes.risk_level)
WHERE {
    ?drug {type: "Drug"}
    ?headache {name: "Headache"}
    (?drug, "treats", ?headache)

    FILTER(?drug.attributes.risk_level < 3)
}
ORDER BY ?drug.attributes.risk_level ASC
LIMIT 10
```

### KML - Knowledge Manipulation Language

Used for knowledge evolution and updates. Supports atomic upsert operations and targeted deletion.

```kml
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
```

### META - Knowledge Exploration

Used for introspection and schema exploration. Fast, metadata-driven commands.

```
DESCRIBE PRIMER
DESCRIBE DOMAINS
DESCRIBE CONCEPT TYPES
DESCRIBE CONCEPT TYPE "Drug"
SEARCH CONCEPT "aspirin" WITH TYPE "Drug" LIMIT 5
```

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

### High-Level Execution with Request/Response

```rust
use anda_kip::{execute_kip, Request, Response};

// Using the high-level execution function
let executor = MyKnowledgeGraph::new();
let response = execute_kip(&executor, "FIND(?x) WHERE { ?x {type: \"Drug\"} }").await?;

// Using structured requests with parameters
let request = Request {
    command: "FIND(?drug) WHERE { ?drug {type: \"Drug\", name: :drug_name} }".to_string(),
    parameters: [("drug_name".to_string(), json!("Aspirin"))].into_iter().collect(),
    dry_run: false,
    ..Default::default()
};

let response = request.execute(&executor).await?;
```

### Batch Command Execution

```rust
use anda_kip::{Request, CommandItem};

let request = Request {
    commands: vec![
        CommandItem::Simple("DESCRIBE PRIMER".to_string()),
        CommandItem::Simple("FIND(?t.name) WHERE { ?t {type: \"$ConceptType\"} } LIMIT 50".to_string()),
        CommandItem::WithParams {
            command: r#"UPSERT { CONCEPT ?e { {type:"Event", name: :name} } }"#.to_string(),
            parameters: [("name".to_string(), json!("MyEvent"))].into_iter().collect(),
        },
    ],
    parameters: [("limit".to_string(), json!(10))].into_iter().collect(),
    dry_run: false,
    readonly: false,
};

let (cmd_type, response) = request.execute(&executor).await?;
```

## Error Handling

KIP defines standard error codes for AI Agent self-correction:

| Code Range | Category | Example |
|-----------|----------|---------|
| 1xxx | Syntax Errors | `KIP_1001`: InvalidSyntax |
| 2xxx | Schema Errors | `KIP_2001`: TypeMismatch |
| 3xxx | Logic/Data Errors | `KIP_3002`: NotFound |
| 4xxx | System Errors | `KIP_4001`: ExecutionTimeout |

```rust
use anda_kip::{KipError, KipErrorCode};

// Parse error
let result = parse_kip("INVALID COMMAND");
if let Err(KipError { code, message, .. }) = result {
    println!("Error {}: {}", code.code(), message);
    println!("Hint: {}", code.hint());
}
```

## Parameter Substitution

KIP supports parameterized queries for safer and more reusable command execution:

```rust
use anda_kip::{Request, CommandItem};
use serde_json::json;

// Single command with parameters
let request = Request {
    command: r#"FIND(?drug) WHERE { ?drug {type: "Drug", name: :name} }"#.to_string(),
    parameters: [
        ("name".to_string(), json!("Aspirin")),
    ].into_iter().collect(),
    dry_run: false,
    ..Default::default()
};

// Parameters are substituted before parsing
// :name becomes "Aspirin"
let command_str = request.to_command();
// "FIND(?drug) WHERE { ?drug {type: \"Drug\", name: \"Aspirin\"} }"
```

## Module Organization

The crate is organized into several key modules:

| Module | Description |
|--------|-------------|
| `ast` | Abstract Syntax Tree definitions for all KIP constructs |
| `capsule` | KIP Genesis Capsules - static knowledge definitions |
| `error` | Comprehensive error types with standard KIP error codes |
| `executor` | Execution framework with async traits for implementing backends |
| `parser` | Nom-based parsers for KQL, KML, and META commands |
| `request` | Request/Response structures for JSON-based communication |
| `types` | KIP Entity Types (ConceptNode, PropositionLink) |

## Specification

This implementation follows the official KIP specification. For detailed information about the protocol, syntax, and semantics, please refer to:

**👉 [KIP Specification](https://github.com/ldclabs/KIP)**

## Contributing

We welcome contributions! Please feel free to submit issues, feature requests, or pull requests.

## License

Copyright © 2026 [LDC Labs](https://github.com/ldclabs).

`anda_kip` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.
