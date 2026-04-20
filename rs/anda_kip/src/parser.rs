//! # Nom-based parsers for KQL, KML, and META commands
//!
//! This module provides the main parsing functionality for the Knowledge Interaction Protocol (KIP).
//! KIP supports three main command types:
//! - **KQL (Knowledge Query Language)**: For querying knowledge graphs
//! - **KML (Knowledge Manipulation Language)**: For modifying knowledge structures
//! - **META**: For introspection and schema exploration
//!
//! The parser is built using the `nom` parsing combinator library and provides
//! both unified parsing through `parse_kip()` and specialized parsers for each command type.

use nom::{
    Parser,
    branch::alt,
    combinator::{all_consuming, map},
    error::context,
};

use crate::ast::{Command, Json, KmlStatement, KqlQuery, MetaCommand};

// Make sub-modules public within the parser module for internal access
mod common; // Common parsing utilities and helpers
mod json; // JSON value parsing and whitespace handling
mod kml; // Knowledge Manipulation Language parser
mod kql; // Knowledge Query Language parser
mod meta; // META command parser for introspection

use crate::error::{KipError, format_nom_error};

/// The main entry point for parsing any KIP command.
///
/// This function serves as the unified parser that can handle all three types of KIP commands.
/// It attempts to parse the input as KQL, KML, or META in that specific order, returning
/// the first successful match. The parser ensures that the entire input string is consumed,
/// preventing partial parsing that could lead to ambiguous results.
///
/// # Parsing Order
/// 1. **KQL (Knowledge Query Language)**: FIND queries for data retrieval
/// 2. **KML (Knowledge Manipulation Language)**: UPSERT/DELETE operations for data modification
/// 3. **META**: DESCRIBE commands for schema introspection
///
/// # Arguments
///
/// * `input` - The raw KIP command string to be parsed
///
/// # Returns
///
/// A `Result` containing:
/// - `Ok(Command)`: Successfully parsed KIP command wrapped in the appropriate enum variant
/// - `Err(KipError)`: Parsing error with detailed error information
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::parse_kip;
///
/// // Parse a KQL query
/// let kql_result = parse_kip("FIND(?drug) WHERE { ?drug {type: \"Drug\"} }");
///
/// // Parse a KML statement
/// let kml_result = parse_kip("UPSERT { CONCEPT ?drug { { name: \"Aspirin\" } } }");
///
/// // Parse a META command
/// let meta_result = parse_kip("DESCRIBE PRIMER");
/// ```
pub fn parse_kip(input: &str) -> Result<Command, KipError> {
    let rt = all_consuming(json::ws(context(
        "KIP command: FIND (KQL) | UPSERT/DELETE (KML) | DESCRIBE/SEARCH (META)",
        alt((
            map(kql::parse_kql_query, Command::Kql),
            map(kml::parse_kml_statement, Command::Kml),
            map(meta::parse_meta_command, Command::Meta),
        )),
    )))
    .parse(input)
    .map_err(|err| format_nom_error(input, err))?;
    Ok(rt.1)
}

/// Parses a Knowledge Query Language (KQL) command specifically.
///
/// This function is a specialized parser for KQL queries, which are used to retrieve
/// information from the knowledge graph. KQL supports complex graph pattern matching,
/// filtering, aggregation, and result ordering.
///
/// # Arguments
///
/// * `input` - The raw KQL query string
///
/// # Returns
///
/// A `Result` containing the parsed `KqlQuery` AST or a parsing error.
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::parse_kql;
///
/// let query = parse_kql("FIND(?drug.name) WHERE { ?drug {type: \"Drug\"} }");
/// ```
pub fn parse_kql(input: &str) -> Result<KqlQuery, KipError> {
    let rt = all_consuming(json::ws(kql::parse_kql_query))
        .parse(input)
        .map_err(|err| format_nom_error(input, err))?;
    Ok(rt.1)
}

/// Parses a Knowledge Manipulation Language (KML) statement specifically.
///
/// This function handles KML commands that modify the knowledge graph structure,
/// including UPSERT operations for creating/updating concepts and propositions,
/// and DELETE operations for removing knowledge elements.
///
/// # Arguments
///
/// * `input` - The raw KML statement string
///
/// # Returns
///
/// A `Result` containing the parsed `KmlStatement` AST or a parsing error.
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::parse_kml;
///
/// let statement = parse_kml("UPSERT { CONCEPT ?drug { { name: \"Aspirin\" } SET ATTRIBUTES { type: \"NSAID\" } } }");
/// ```
pub fn parse_kml(input: &str) -> Result<KmlStatement, KipError> {
    let rt = all_consuming(json::ws(kml::parse_kml_statement))
        .parse(input)
        .map_err(|err| format_nom_error(input, err))?;
    Ok(rt.1)
}

/// Parses a META command specifically.
///
/// META commands are used for introspection and schema exploration of the knowledge graph.
/// They provide information about the structure, types, and metadata of the knowledge base
/// without performing actual data queries or modifications.
///
/// # Arguments
///
/// * `input` - The raw META command string
///
/// # Returns
///
/// A `Result` containing the parsed `MetaCommand` AST or a parsing error.
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::parse_meta;
///
/// let meta_cmd = parse_meta("DESCRIBE PRIMER");
/// ```
pub fn parse_meta(input: &str) -> Result<MetaCommand, KipError> {
    let rt = all_consuming(json::ws(meta::parse_meta_command))
        .parse(input)
        .map_err(|err| format_nom_error(input, err))?;
    Ok(rt.1)
}

/// Parses a standalone JSON value.
///
/// This utility function parses JSON values that may appear in KIP commands,
/// such as attribute values, metadata, or configuration parameters. It handles
/// all standard JSON types including objects, arrays, strings, numbers, booleans, and null.
///
/// # Arguments
///
/// * `input` - The raw JSON string to parse
///
/// # Returns
///
/// A `Result` containing the parsed `Json` value or a parsing error.
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::parse_json;
///
/// let json_obj = parse_json(r#"{"name": "Aspirin", "dosage": 500}"#);
/// let json_array = parse_json("[1, 2, 3]");
/// let json_string = parse_json("\"hello world\"");
/// ```
pub fn parse_json(input: &str) -> Result<Json, KipError> {
    let rt = all_consuming(json::ws(json::json_value()))
        .parse(input)
        .map_err(|err| format_nom_error(input, err))?;
    Ok(rt.1)
}

/// Converts a string to its JSON-quoted representation.
///
/// This utility function takes a plain string and converts it to a properly
/// JSON-escaped and quoted string. It handles all necessary character escaping
/// including quotes, backslashes, and control characters.
///
/// # Arguments
///
/// * `s` - The input string to quote
///
/// # Returns
///
/// A `String` containing the JSON-quoted representation of the input.
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::quote_str;
///
/// assert_eq!(quote_str("hello"), "\"hello\"");
/// assert_eq!(quote_str("say \"hi\""), "\"say \\\"hi\\\"\"");
/// ```
pub fn quote_str(s: &str) -> String {
    Json::String(s.to_string()).to_string()
}

/// Attempts to unquote a JSON string, returning the inner string value.
///
/// This utility function takes a JSON-quoted string and attempts to parse it,
/// returning the unescaped inner string value. If the input is not a valid
/// JSON string, it returns `None`.
///
/// # Arguments
///
/// * `s` - The JSON-quoted string to unquote
///
/// # Returns
///
/// An `Option<String>` containing:
/// - `Some(String)`: The successfully unquoted string value
/// - `None`: If the input is not a valid JSON string
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::unquote_str;
///
/// assert_eq!(unquote_str("\"hello\""), Some("hello".to_string()));
/// assert_eq!(unquote_str("\"say \\\"hi\\\"\""), Some("say \"hi\"".to_string()));
/// assert_eq!(unquote_str("invalid"), None);
/// ```
pub fn unquote_str(s: &str) -> Option<String> {
    match json::quoted_string(s) {
        Ok(("", value)) => Some(value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::ast;

    use super::*;

    #[test]
    fn test_parse_simple_kml() {
        let input = r#"
UPSERT {
  // First, define the new types we need to properly categorize the new information.
  CONCEPT ?project_type {
    {type: "$ConceptType", name: "SoftwareProject"}
    SET ATTRIBUTES {
        description: "Represents a software development project.",
        display_hint: "💻"
    }
    SET PROPOSITIONS { ("belongs_to_domain", {type: "Domain", name: "CoreSchema"}) }
  }
  CONCEPT ?standard_type {
    {type: "$ConceptType", name: "Standard"}
    SET ATTRIBUTES {
        description: "Represents a technical standard or protocol that governs interaction.",
        display_hint: "📜"
    }
    SET PROPOSITIONS { ("belongs_to_domain", {type: "Domain", name: "CoreSchema"}) }
  }

  // Define the relationship types (predicates).
  CONCEPT ?developer_prop {
    {type: "$PropositionType", name: "is_developer_of"}
    SET ATTRIBUTES {
      description: "Asserts that the subject is a developer of the object.",
      subject_types: ["Person"],
      object_types: ["SoftwareProject"]
    }
    SET PROPOSITIONS { ("belongs_to_domain", {type: "Domain", name: "CoreSchema"}) }
  }
  CONCEPT ?designer_prop {
    {type: "$PropositionType", name: "is_designer_of"}
    SET ATTRIBUTES {
      description: "Asserts that the subject is the designer of the object.",
      subject_types: ["Person"],
      object_types: ["Standard"]
    }
    SET PROPOSITIONS { ("belongs_to_domain", {type: "Domain", name: "CoreSchema"}) }
  }

  // Now, create the actual concepts for ICPanda and KIP.
  CONCEPT ?icpanda {
    {type: "SoftwareProject", name: "ICPanda"}
    SET ATTRIBUTES {
        description: "A software project developed by Yan."
    }
  }
  CONCEPT ?kip {
    {type: "Standard", name: "KIP"}
    SET ATTRIBUTES {
        description: "Knowledge Interaction Protocol. A standard for AI-Knowledge Graph interaction, designed by Yan."
    }
  }

  // Finally, update your Person node with your name and establish the new relationships.
  CONCEPT ?user {
    {type: "Person", name: "nmob2-y6p4k-rp5j7-7x2mo-aqceq-lpie2-fjgw7-nkjdu-bkoe4-zjetd-wae"}
    SET ATTRIBUTES {
      name: "Yan", // Your display name
      person_class: "Human",
      relationship_to_self: "Creator / Designer"
    }
    SET PROPOSITIONS {
      ("is_developer_of", ?icpanda), // atypical trailing comma, generated by Gemini 2.5 Pro
      ("is_designer_of", ?kip)
    }
  }
}
WITH METADATA {
  source: "Direct statement from user during conversation.",
  author: "nmob2-y6p4k-rp5j7-7x2mo-aqceq-lpie2-fjgw7-nkjdu-bkoe4-zjetd-wae",
  confidence: 0.98
}
        "#;
        let result = parse_kml(input);
        println!("{result:#?}");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_kml() {
        let input = r#"
// Knowledge Capsule: cognizin.v1.0
// Description: Defines the novel nootropic drug "Cognizine" and its effects.

UPSERT {
  // Define the main drug concept: Cognizine
  CONCEPT ?cognizine {
    { type: "Drug", name: "Cognizine" }
    SET ATTRIBUTES {
      molecular_formula: "C12H15N5O3",     // Molecular formula of Cognizine
      risk_level: 2,
      description: "A novel nootropic drug designed to enhance cognitive functions."
    }
    SET PROPOSITIONS {
      // Link to an existing concept (Nootropic)
      ("is_class_of", { type: "DrugClass", name: "Nootropic" })

      // Link to an existing concept (Brain Fog)
      ("treats", { type: "Symptom", name: "Brain Fog" })

      // Link to another new concept defined within this capsule (?neural_bloom)
      ("has_side_effect", ?neural_bloom) WITH METADATA {
        // This specific proposition has its own metadata
        confidence: 0.75,
        source: "Preliminary Clinical Trial NCT012345"
      }
    }
  }

  // Define the new side effect concept: Neural Bloom
  CONCEPT ?neural_bloom {
    { type: "Symptom", name: "Neural Bloom" }
    SET ATTRIBUTES {
      description: "A rare side effect characterized by a temporary burst of creative thoughts."
    }
    // This concept has no outgoing propositions in this capsule
  }
}
WITH METADATA {
  // Global metadata for all facts in this capsule
  source: "KnowledgeCapsule:Nootropics_v1.0",
  author: "LDC Labs Research Team",
  confidence: 0.95,
  status: "reviewed"
}
        "#;
        let result = parse_kml(input);
        assert!(result.is_ok());

        let kml_statement = result.unwrap();

        // Verify this is an UPSERT statement
        match kml_statement {
            KmlStatement::Upsert(blocks) => {
                assert_eq!(blocks.len(), 1);
                let ast::UpsertBlock { items, metadata } = &blocks[0];
                // Verify there are two concept operations
                assert_eq!(items.len(), 2);

                // Verify the first concept (?cognizine)
                let cognizine_op = &items[0];
                match cognizine_op {
                    ast::UpsertItem::Concept(ast::ConceptBlock {
                        handle,
                        concept,
                        set_attributes,
                        set_propositions,
                        metadata,
                    }) => {
                        assert_eq!(handle, &Some("cognizine".to_string()));
                        assert_eq!(
                            concept,
                            &ast::ConceptMatcher::Object {
                                r#type: "Drug".to_string(),
                                name: "Cognizine".to_string(),
                            }
                        );
                        assert!(metadata.is_none());
                        assert_eq!(set_attributes.as_ref().unwrap().len(), 3);
                        assert_eq!(set_propositions.as_ref().unwrap().len(), 3);
                    }
                    _ => panic!("Expected Concept operation for first operation"),
                }

                // Verify the second concept (?neural_bloom)
                let neural_bloom_op = &items[1];
                match neural_bloom_op {
                    ast::UpsertItem::Concept(ast::ConceptBlock {
                        handle,
                        concept,
                        set_attributes,
                        set_propositions,
                        metadata,
                    }) => {
                        assert_eq!(handle, &Some("neural_bloom".to_string()));
                        assert_eq!(
                            concept,
                            &ast::ConceptMatcher::Object {
                                r#type: "Symptom".to_string(),
                                name: "Neural Bloom".to_string(),
                            }
                        );
                        assert!(metadata.is_none());
                        assert_eq!(set_attributes.as_ref().unwrap().len(), 1);
                        assert!(set_propositions.is_none());
                    }
                    _ => panic!("Expected Concept operation for second operation"),
                }

                // Verify global metadata
                assert!(metadata.is_some());
                let global_metadata = metadata.as_ref().unwrap();
                assert_eq!(global_metadata.len(), 4);
                assert_eq!(
                    global_metadata.get("source"),
                    Some(&Json::String(
                        "KnowledgeCapsule:Nootropics_v1.0".to_string()
                    ))
                );
                assert_eq!(
                    global_metadata.get("author"),
                    Some(&Json::String("LDC Labs Research Team".to_string()))
                );
                assert_eq!(
                    global_metadata.get("confidence"),
                    Some(&Json::Number(crate::ast::Number::from_f64(0.95).unwrap()))
                );
                assert_eq!(
                    global_metadata.get("status"),
                    Some(&Json::String("reviewed".to_string()))
                );
            }
            _ => panic!("Expected Upsert statement"),
        }
    }

    #[test]
    fn test_quote_str_basic() {
        // Test basic string quoting
        assert_eq!(quote_str("hello"), "\"hello\"");
        assert_eq!(quote_str("world"), "\"world\"");
        assert_eq!(quote_str(""), "\"\"");
    }

    #[test]
    fn test_quote_str_with_quotes() {
        // Test strings containing quotes
        assert_eq!(quote_str("say \"hi\""), "\"say \\\"hi\\\"\"");
        assert_eq!(quote_str("\"quoted\""), "\"\\\"quoted\\\"\"");
        assert_eq!(quote_str("It's \"great\"!"), "\"It's \\\"great\\\"!\"");
    }

    #[test]
    fn test_quote_str_with_backslashes() {
        // Test strings containing backslashes
        assert_eq!(quote_str("path\\to\\file"), "\"path\\\\to\\\\file\"");
        assert_eq!(quote_str("\\n\\t"), "\"\\\\n\\\\t\"");
        assert_eq!(quote_str("C:\\\\Users"), "\"C:\\\\\\\\Users\"");
    }

    #[test]
    fn test_quote_str_with_control_characters() {
        // Test strings containing control characters
        assert_eq!(quote_str("line1\nline2"), "\"line1\\nline2\"");
        assert_eq!(quote_str("tab\there"), "\"tab\\there\"");
        assert_eq!(quote_str("carriage\rreturn"), "\"carriage\\rreturn\"");
        // assert_eq!(quote_str("form\ffeed"), "\"form\\ffeed\"");
        // assert_eq!(quote_str("back\bspace"), "\"back\\bspace\"");
    }

    #[test]
    fn test_quote_str_with_unicode() {
        // Test strings containing Unicode characters
        assert_eq!(quote_str("你好"), "\"你好\"");
        assert_eq!(quote_str("🚀 rocket"), "\"🚀 rocket\"");
        assert_eq!(quote_str("café"), "\"café\"");
    }

    #[test]
    fn test_unquote_str_basic() {
        // Test basic string unquoting
        assert_eq!(unquote_str("\"hello\""), Some("hello".to_string()));
        assert_eq!(unquote_str("\"world\""), Some("world".to_string()));
        assert_eq!(unquote_str("\"\""), Some("".to_string()));
    }

    #[test]
    fn test_unquote_str_with_escaped_quotes() {
        // Test unquoting strings with escaped quotes
        assert_eq!(
            unquote_str("\"say \\\"hi\\\"\""),
            Some("say \"hi\"".to_string())
        );
        assert_eq!(
            unquote_str("\"\\\"quoted\\\"\""),
            Some("\"quoted\"".to_string())
        );
        assert_eq!(
            unquote_str("\"It's \\\"great\\\"!\""),
            Some("It's \"great\"!".to_string())
        );
    }

    #[test]
    fn test_unquote_str_with_escaped_backslashes() {
        // Test unquoting strings with escaped backslashes
        assert_eq!(
            unquote_str("\"path\\\\to\\\\file\""),
            Some("path\\to\\file".to_string())
        );
        assert_eq!(unquote_str("\"\\\\n\\\\t\""), Some("\\n\\t".to_string()));
        assert_eq!(
            unquote_str("\"C:\\\\\\\\Users\""),
            Some("C:\\\\Users".to_string())
        );
    }

    #[test]
    fn test_unquote_str_with_control_characters() {
        // Test unquoting strings with control characters
        assert_eq!(
            unquote_str("\"line1\\nline2\""),
            Some("line1\nline2".to_string())
        );
        assert_eq!(unquote_str("\"tab\\there\""), Some("tab\there".to_string()));
        assert_eq!(
            unquote_str("\"carriage\\rreturn\""),
            Some("carriage\rreturn".to_string())
        );
        // assert_eq!(unquote_str("\"form\\ffeed\""), Some("form\ffeed".to_string()));
        // assert_eq!(unquote_str("\"back\\bspace\""), Some("back\bspace".to_string()));
    }

    #[test]
    fn test_unquote_str_with_unicode() {
        // Test unquoting strings with Unicode characters
        assert_eq!(unquote_str("\"你好\""), Some("你好".to_string()));
        assert_eq!(unquote_str("\"🚀 rocket\""), Some("🚀 rocket".to_string()));
        assert_eq!(unquote_str("\"café\""), Some("café".to_string()));
    }

    #[test]
    fn test_unquote_str_invalid_input() {
        // Test unquoting invalid JSON strings
        assert_eq!(unquote_str("hello"), None); // Missing quotes
        assert_eq!(unquote_str("\"hello"), None); // Missing closing quote
        assert_eq!(unquote_str("hello\""), None); // Missing opening quote
        assert_eq!(unquote_str("'hello'"), None); // Single quotes instead of double
        assert_eq!(unquote_str("\"hello\" world"), None); // Extra content after closing quote
        assert_eq!(unquote_str("\"invalid\\escape\""), None); // Invalid escape sequence
    }

    #[test]
    fn test_quote_unquote_roundtrip() {
        // Test that quote_str and unquote_str are inverse operations
        let test_strings = vec![
            "hello",
            "say \"hi\"",
            "path\\to\\file",
            "line1\nline2\ttab",
            "你好世界",
            "🚀🌟💫",
            "",
            "complex: \"nested\" with \\backslashes\\ and \nnewlines",
        ];

        for original in test_strings {
            let quoted = quote_str(original);
            let unquoted = unquote_str(&quoted);
            assert_eq!(
                unquoted,
                Some(original.to_string()),
                "Roundtrip failed for: {}",
                original
            );
        }
    }

    #[test]
    fn test_quote_str_special_cases() {
        // Test edge cases and special characters
        assert_eq!(quote_str("\0"), "\"\\u0000\""); // Null character
        assert_eq!(quote_str("\x08"), "\"\\b\""); // Backspace
        assert_eq!(quote_str("\x0C"), "\"\\f\""); // Form feed
    }

    #[test]
    fn test_unquote_str_special_escapes() {
        // Test unquoting special escape sequences
        assert_eq!(unquote_str("\"\\u0000\""), Some("\0".to_string()));
        assert_eq!(unquote_str("\"\\b\""), Some("\x08".to_string()));
        assert_eq!(unquote_str("\"\\f\""), Some("\x0C".to_string()));
        assert_eq!(unquote_str("\"\\u4f60\\u597d\""), Some("你好".to_string()));
    }

    #[test]
    fn test_parse_genesis() {
        let input = r#"
// # KIP Genesis Capsule v1.0
// The foundational knowledge that bootstraps the entire Cognitive Nexus.
// It defines what a "Concept Type" and a "Proposition Type" are,
// by creating instances of them that describe themselves.
//
UPSERT {
    // --- STEP 1: THE PRIME MOVER - DEFINE "$ConceptType" ---
    // The absolute root of all knowledge. This node defines what it means to be a "type"
    // of concept. It defines itself, creating the first logical anchor.
    CONCEPT ?concept_type_def {
        {type: "$ConceptType", name: "$ConceptType"}
        SET ATTRIBUTES {
            description: "Defines a class or category of Concept Nodes. It acts as a template for creating new concept instances. Every concept node in the graph must have a 'type' that points to a concept of this type.",
            display_hint: "📦",
            instance_schema: {
                "description": {
                    type: "string",
                    is_required: true,
                    description: "A human-readable explanation of what this concept type represents."
                },
                "display_hint": {
                    type: "string",
                    is_required: false,
                    description: "A suggested icon or visual cue for user interfaces (e.g., an emoji or icon name)."
                },
                "instance_schema": {
                    type: "object",
                    is_required: false,
                    description: "A recommended schema defining the common and core attributes for instances of this concept type. It serves as a 'best practice' guideline for knowledge creation, not a rigid constraint. Keys are attribute names, values are objects defining 'type', 'is_required', and 'description'. Instances SHOULD include required attributes but MAY also include any other attribute not defined in this schema, allowing for knowledge to emerge and evolve freely."
                },
                "key_instances": {
                    type: "array",
                    item_type: "string",
                    is_required: false,
                    description: "A list of names of the most important or representative instances of this type, to help LLMs ground their queries."
                }
            },
            key_instances: [ "$ConceptType", "$PropositionType", "Domain" ]
        }
    }

    // --- STEP 2: DEFINE "$PropositionType" USING "$ConceptType" ---
    // With the ability to define concepts, we now define the concept of a "relation" or "predicate".
    CONCEPT ?proposition_type_def {
        {type: "$ConceptType", name: "$PropositionType"}
        SET ATTRIBUTES {
            description: "Defines a class of Proposition Links (a predicate). It specifies the nature of the relationship between a subject and an object.",
            display_hint: "🔗",
            instance_schema: {
                "description": {
                    type: "string",
                    is_required: true,
                    description: "A human-readable explanation of what this relationship represents."
                },
                "subject_types": {
                    type: "array",
                    item_type: "string",
                    is_required: true,
                    description: "A list of allowed '$ConceptType' names for the subject. Use '*' for any type."
                },
                "object_types": {
                    type: "array",
                    item_type: "string",
                    is_required: true,
                    description: "A list of allowed '$ConceptType' names for the object. Use '*' for any type."
                },
                "is_symmetric": { type: "boolean", is_required: false, default_value: false },
                "is_transitive": { type: "boolean", is_required: false, default_value: false }
            },
            key_instances: [ "belongs_to_domain" ]
        }
    }

    // --- STEP 3: DEFINE THE TOOLS FOR ORGANIZATION ---
    // Now that we can define concepts and propositions, we create the specific
    // concepts needed for organizing the knowledge graph itself.

    // 3a. Define the "Domain" concept type.
    CONCEPT ?domain_type_def {
        {type: "$ConceptType", name: "Domain"}
        SET ATTRIBUTES {
            description: "Defines a high-level container for organizing knowledge. It acts as a primary category for concepts and propositions, enabling modularity and contextual understanding.",
            display_hint: "🗺️",
            instance_schema: {
                "description": {
                    type: "string",
                    is_required: true,
                    description: "A clear, human-readable explanation of what knowledge this domain encompasses."
                },
                "display_hint": {
                    type: "string",
                    is_required: false,
                    description: "A suggested icon or visual cue for this specific domain (e.g., a specific emoji)."
                },
                "scope_note": {
                    type: "string",
                    is_required: false,
                    description: "A more detailed note defining the precise boundaries of the domain, specifying what is included and what is excluded."
                },
                "aliases": {
                    type: "array",
                    item_type: "string",
                    is_required: false,
                    description: "A list of alternative names or synonyms for the domain, to aid in search and natural language understanding."
                },
                "steward": {
                    type: "string",
                    is_required: false,
                    description: "The name of the 'Person' (human or AI) primarily responsible for curating and maintaining the quality of knowledge within this domain."
                }

            },
            key_instances: ["CoreSchema"]
        }
    }

    // 3b. Define the "belongs_to_domain" proposition type.
    CONCEPT ?belongs_to_domain_prop {
        {type: "$PropositionType", name: "belongs_to_domain"}
        SET ATTRIBUTES {
            description: "A fundamental proposition that asserts a concept's membership in a specific knowledge domain.",
            subject_types: ["*"], // Any concept can belong to a domain.
            object_types: ["Domain"] // The object must be a Domain.
        }
    }

    // 3c. Create a dedicated domain "CoreSchema" for meta-definitions.
    // This domain will contain the definitions of all concept types and proposition types.
    CONCEPT ?core_domain {
        {type: "Domain", name: "CoreSchema"}
        SET ATTRIBUTES {
            description: "The foundational domain containing the meta-definitions of the KIP system itself.",
            display_hint: "🧩"
        }
    }
}
WITH METADATA {
    source: "KIP Genesis Capsule v1.0",
    author: "System Architect",
    confidence: 1.0,
    status: "active"
}

// Post-Genesis Housekeeping
UPSERT {
    // Assign all meta-definition concepts to the "CoreSchema" domain.
    CONCEPT ?core_domain {
        {type: "Domain", name: "CoreSchema"}
    }

    CONCEPT ?concept_type_def {
        {type: "$ConceptType", name: "$ConceptType"}
        SET PROPOSITIONS { ("belongs_to_domain", ?core_domain) }
    }
    CONCEPT ?proposition_type_def {
        {type: "$ConceptType", name: "$PropositionType"}
        SET PROPOSITIONS { ("belongs_to_domain", ?core_domain) }
    }
    CONCEPT ?domain_type_def {
        {type: "$ConceptType", name: "Domain"}
        SET PROPOSITIONS { ("belongs_to_domain", ?core_domain) }
    }
    CONCEPT ?belongs_to_domain_prop {
        {type: "$PropositionType", name: "belongs_to_domain"}
        SET PROPOSITIONS { ("belongs_to_domain", ?core_domain) }
    }
}
WITH METADATA {
    source: "System Maintenance",
    author: "System Architect",
    confidence: 1.0,
}

// DEFINE the "Person" concept type ---
UPSERT {
    // The agent itself is a person: `{type: "Person", name: "$self"}`.
    CONCEPT ?person_type_def {
        {type: "$ConceptType", name: "Person"}
        SET ATTRIBUTES {
            description: "Represents an individual actor within the system, which can be an AI, a human, or a group entity. All actors, including the agent itself, are instances of this type.",
            display_hint: "👤",
            instance_schema: {
                "id": {
                    type: "string",
                    is_required: true,
                    description: "A unique identifier for the person, typically a UUID or similar."
                },
                "person_class": {
                    type: "string",
                    is_required: true,
                    description: "The classification of the person, e.g., 'AI', 'Human', 'Organization'."
                },
                "name": {
                    type: "string",
                    is_required: false, // No name for $self at genesis
                    description: "The given or chosen name of the person."
                },
                "handle": {
                    type: "string",
                    is_required: false,
                    description: "A unique handle or username for the person, often used in digital contexts."
                },
                "avatar": {
                    type: "string",
                    is_required: false,
                    description: "A URL or emoji identifier for the person's avatar image, used in user interfaces."
                },
                "persona": {
                    type: "string",
                    is_required: false,
                    description: "For AIs, a self-description of their identity. For humans, it could be a summary of their observed personality or role."
                },
                "core_mission": {
                    type: "string",
                    is_required: false,
                    description: "Primarily for AIs, describing their main objective."
                },
                "capabilities": {
                    type: "array",
                    item_type: "string",
                    is_required: false,
                    description: "Primarily for AIs, a list of key functions they can perform."
                },
                "relationship_to_self": {
                    type: "string",
                    is_required: false,
                    description: "For persons other than '$self', their relationship to the agent (e.g., 'user', 'creator', 'collaborator')."
                },
                "interaction_summary": {
                    type: "object",
                    is_required: false,
                    description: "A dynamically updated summary of interactions, like last_seen, interaction_count, key_topics."
                }
            }
        }

        SET PROPOSITIONS { ("belongs_to_domain", {type: "Domain", name: "CoreSchema"}) }
    }
}
WITH METADATA {
    source: "KIP Capsule Design",
    author: "System Architect",
    confidence: 1.0,
    status: "active"
}
        "#;

        let result = parse_kml(input).unwrap();
        println!("{:?}", result);
        match result {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 3);
            }
            _ => panic!("Expected Upsert"),
        }
    }

    #[test]
    fn test_error_messages_quality() {
        // Helper to extract error message
        fn err_msg(input: &str) -> String {
            parse_kip(input).unwrap_err().message
        }

        // 1. Unterminated string
        let msg = err_msg(r#"UPSERT { CONCEPT ?drug { {type: "Drug, name: "Aspirin"} } }"#);
        println!("=== Unterminated string ===\n{msg}\n");
        assert!(msg.contains("line"), "Should contain line number");
        assert!(msg.contains("column"), "Should contain column number");

        // 2. Missing closing brace
        let msg = err_msg(r#"UPSERT { CONCEPT ?drug { {type: "Drug", name: "Aspirin"} }"#);
        println!("=== Missing closing brace ===\n{msg}\n");
        assert!(msg.contains("line"), "Should contain line number");

        // 3. Wrong keyword (lowercase)
        let msg = err_msg(r#"find(?drug) WHERE { ?drug {type: "Drug"} }"#);
        println!("=== Wrong keyword ===\n{msg}\n");
        assert!(
            msg.contains("FIND")
                || msg.contains("UPSERT")
                || msg.contains("DESCRIBE")
                || msg.contains("KIP"),
            "Should mention valid KIP keywords"
        );

        // 4. Trailing content
        let msg = err_msg(r#"DESCRIBE PRIMER extra_stuff"#);
        println!("=== Trailing content ===\n{msg}\n");
        assert!(
            msg.contains("trailing")
                || msg.contains("extra")
                || msg.contains("Unexpected")
                || msg.contains("Eof"),
            "Should indicate trailing content issue"
        );

        // 5. Missing WHERE
        let msg = err_msg(r#"FIND(?drug) { ?drug {type: "Drug"} }"#);
        println!("=== Missing WHERE ===\n{msg}\n");
        assert!(
            msg.contains("WHERE") || msg.contains("line"),
            "Should mention WHERE or show location"
        );

        // 6. Empty input
        let msg = err_msg("");
        println!("=== Empty input ===\n{msg}\n");
        assert!(
            msg.contains("end of input") || msg.contains("KIP"),
            "Should mention empty/end of input"
        );

        // 7. Invalid JSON escape
        let msg = err_msg(r#"UPSERT { CONCEPT ?drug { {type: "Drug", name: "test\x"} } }"#);
        println!("=== Invalid escape ===\n{msg}\n");
        assert!(msg.contains("line"), "Should contain line number");

        // 8. Unclosed JSON array
        let msg = err_msg(
            r#"UPSERT { CONCEPT ?drug { {type: "Drug", name: "Aspirin"} SET ATTRIBUTES { tags: ["a", "b" } } }"#,
        );
        println!("=== Unclosed array ===\n{msg}\n");
        assert!(msg.contains("line"), "Should contain line number");
    }
}
