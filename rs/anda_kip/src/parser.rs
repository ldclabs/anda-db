//! # KIP Parser Module
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
};

use crate::ast::{Command, Json, KmlStatement, KqlQuery, MetaCommand};

// Make sub-modules public within the parser module for internal access
mod common; // Common parsing utilities and helpers
mod json; // JSON value parsing and whitespace handling
mod kml; // Knowledge Manipulation Language parser
mod kql; // Knowledge Query Language parser
mod meta; // META command parser for introspection

use crate::error::KipError;

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
/// let kql_result = parse_kip("FIND(?drug) WHERE { ?drug(type: \"Drug\") }");
///
/// // Parse a KML statement
/// let kml_result = parse_kip("UPSERT { CONCEPT @drug { ON { name: \"Aspirin\" } } }");
///
/// // Parse a META command
/// let meta_result = parse_kip("DESCRIBE PRIMER");
/// ```
pub fn parse_kip(input: &str) -> Result<Command, KipError> {
    let rt = all_consuming(json::ws(alt((
        map(kql::parse_kql_query, Command::Kql),
        map(kml::parse_kml_statement, Command::Kml),
        map(meta::parse_meta_command, Command::Meta),
    ))))
    .parse(input)
    .map_err(|err| KipError::Parse(format!("{err}")))?;
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
/// let query = parse_kql("FIND(?drug_name) WHERE { ?drug(type: \"Drug\") ATTR(?drug, \"name\", ?drug_name) }");
/// ```
pub fn parse_kql(input: &str) -> Result<KqlQuery, KipError> {
    let rt = all_consuming(json::ws(kql::parse_kql_query))
        .parse(input)
        .map_err(|err| KipError::Parse(format!("{err}")))?;
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
/// let statement = parse_kml("UPSERT { CONCEPT @drug { ON { name: \"Aspirin\" } SET ATTRIBUTES { type: \"NSAID\" } } }");
/// ```
pub fn parse_kml(input: &str) -> Result<KmlStatement, KipError> {
    let rt = all_consuming(json::ws(kml::parse_kml_statement))
        .parse(input)
        .map_err(|err| KipError::Parse(format!("{err}")))?;
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
        .map_err(|err| KipError::Parse(format!("{err}")))?;
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
        .map_err(|err| KipError::Parse(format!("{err}")))?;
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
    fn test_parse_kml() {
        let input = r#"
// Knowledge Capsule: cognizin.v1.0
// Description: Defines the novel nootropic drug "Cognizine" and its effects.

UPSERT {
  // Define the main drug concept: Cognizine
  CONCEPT @cognizine {
    ON { type: "Drug", name: "Cognizine" }
    SET ATTRIBUTES {
      molecular_formula: "C12H15N5O3",     // Molecular formula of Cognizine
      risk_level: 2,
      description: "A novel nootropic drug designed to enhance cognitive functions."
    }
    SET PROPOSITIONS {
      // Link to an existing concept (Nootropic)
      PROP("is_class_of", ON { type: "DrugClass", name: "Nootropic" })

      // Link to an existing concept (Brain Fog)
      PROP("treats", ON { type: "Symptom", name: "Brain Fog" })

      // Link to another new concept defined within this capsule (@neural_bloom)
      PROP("has_side_effect", @neural_bloom) WITH METADATA {
        // This specific proposition has its own metadata
        confidence: 0.75,
        source: "Preliminary Clinical Trial NCT012345"
      }
    }
  }

  // Define the new side effect concept: Neural Bloom
  CONCEPT @neural_bloom {
    ON { type: "Symptom", name: "Neural Bloom" }
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

        // 验证这是一个 UPSERT 语句
        match kml_statement {
            KmlStatement::Upsert(ast::UpsertBlock { items, metadata }) => {
                // 验证有两个概念操作
                assert_eq!(items.len(), 2);

                // 验证第一个概念 (@cognizine)
                let cognizine_op = &items[0];
                match cognizine_op {
                    ast::UpsertItem::Concept(ast::ConceptBlock {
                        handle,
                        on,
                        metadata,
                        set_attributes,
                        set_propositions,
                    }) => {
                        assert_eq!(handle, "cognizine");
                        assert_eq!(on.keys.len(), 2);
                        assert!(metadata.is_none());
                        assert_eq!(set_attributes.as_ref().unwrap().len(), 3);
                        assert_eq!(set_propositions.as_ref().unwrap().len(), 3);
                    }
                    _ => panic!("Expected Concept operation for first operation"),
                }

                // 验证第二个概念 (@neural_bloom)
                let neural_bloom_op = &items[1];
                match neural_bloom_op {
                    ast::UpsertItem::Concept(ast::ConceptBlock {
                        handle,
                        on,
                        metadata,
                        set_attributes,
                        set_propositions,
                    }) => {
                        assert_eq!(handle, "neural_bloom");
                        assert_eq!(on.keys.len(), 2);
                        assert!(metadata.is_none());
                        assert_eq!(set_attributes.as_ref().unwrap().len(), 1);
                        assert!(set_propositions.is_none());
                    }
                    _ => panic!("Expected Concept operation for second operation"),
                }

                // 验证全局元数据
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
}
