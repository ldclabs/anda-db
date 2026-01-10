//! # Anda KIP - Knowledge Interaction Protocol Implementation
//!
//! A comprehensive Rust implementation of KIP (Knowledge Interaction Protocol) for building
//! sustainable AI knowledge memory systems and enabling sophisticated interactions between
//! Large Language Models (LLMs) and knowledge graphs.
//!
//! ## Overview
//!
//! **🧬 KIP (Knowledge Interaction Protocol)** is a knowledge memory interaction protocol
//! designed specifically for Large Language Models (LLMs), aimed at building sustainable
//! learning and self-evolving knowledge memory systems for AI Agents.
//!
//! This crate provides a complete, type-safe Rust implementation of the KIP specification,
//! offering robust parsing, execution, and communication capabilities for knowledge graph
//! operations.
//!
//! ## Standards Compliance
//!
//! This implementation follows the official KIP specification. For detailed information
//! about the protocol, syntax, and semantics, please refer to:
//!
//! **👉 [KIP Specification](https://github.com/ldclabs/KIP)**
//!
//! ## Architecture
//!
//! The crate is organized into several key modules:
//!
//! - [`ast`]: Abstract Syntax Tree definitions for all KIP language constructs
//! - [`error`]: Comprehensive error types and structured error handling
//! - [`executor`]: Execution framework with async traits for implementing KIP backends
//! - [`parser`]: High-performance nom-based parsers for KQL, KML, and META commands
//! - [`request`]: Standardized JSON-based request/response structures for communication
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use anda_kip::{parse_kip, Command, Executor, Response};
//!
//! // Parse a KQL query
//! let query = parse_kip(r#"
//!     FIND(?drug.name, ?drug.attributes.risk_level)
//!     WHERE {
//!         ?drug {type: "Drug"}
//!         ?headache {name: "Headache"}
//!         (?drug, "treats", ?headache)
//!
//!         FILTER(?drug.attributes.risk_level < 3)
//!     }
//!     ORDER BY ?drug.attributes.risk_level ASC
//!     LIMIT 10
//! "#).unwrap();
//!
//! // Parse a KML statement
//! let statement = parse_kip(r#"
//!     UPSERT {
//!         CONCEPT ?new_drug {
//!             { type: "Drug", name: "Aspirin" }
//!             SET ATTRIBUTES {
//!                 molecular_formula: "C9H8O4",
//!                 risk_level: 1
//!             }
//!         }
//!     }
//! "#).unwrap();
//! ```
//!
//! ## Examples
//!
//! For comprehensive examples and usage patterns, see the individual module documentation
//! and the project's examples directory.

use std::sync::LazyLock;

pub mod ast;
pub mod capsule;
pub mod error;
pub mod executor;
pub mod parser;
pub mod request;
pub mod types;

pub use ast::*;
pub use capsule::*;
pub use error::*;
pub use executor::*;
pub use parser::*;
pub use request::*;
pub use types::*;

pub static SELF_INSTRUCTIONS: &str = include_str!("../SelfInstructions.md");
pub static SYSTEM_INSTRUCTIONS: &str = include_str!("../SystemInstructions.md");

/// JSON schema definition for the `execute_kip` function
pub static KIP_FUNCTION_DEFINITION: LazyLock<Json> =
    LazyLock::new(|| serde_json::from_str(include_str!("../FunctionDefinition.json")).unwrap());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function() {
        let json = KIP_FUNCTION_DEFINITION.clone();
        println!("{json:#?}");
        assert!(json.is_object());
        assert_eq!(
            json.get("name"),
            Some(&Json::String("execute_kip".to_string()))
        );
    }
}
