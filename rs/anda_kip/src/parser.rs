//! # Nom-based parsers for KQL, KML and META
//!
//! The parser goes straight to the executable AST: the grammar's open-ended
//! positions are closed as they are recognized, and the schema-independent
//! rules — `ASSERT` desugaring, identity selectors, immutable payload, handle
//! resolution — are enforced here rather than left for an engine to discover.
//!
//! Three surfaces, one entry point:
//!
//! - [`parse_kql`] — reads;
//! - [`parse_kml`] — mutations, which are always transactions;
//! - [`parse_meta`] — introspection, grounding, history and export;
//! - [`parse_kip`] — any of the three, classified by what the text actually is.

use nom::{
    Parser,
    branch::alt,
    combinator::{all_consuming, map},
    error::context,
};

use crate::ast::{Command, Json, KmlStatement, KqlQuery, MetaCommand};
use crate::error::{KipError, format_nom_error};

mod common;
mod json;
mod kml;
mod kql;
mod meta;

pub use common::PROTECTED_FIELDS;

/// Maximum accepted length (in bytes) of a single KIP command string.
///
/// Inputs longer than this are rejected by every `parse_*` entry point before
/// any parsing work happens, bounding parser memory and CPU for server-facing
/// deployments.
pub const MAX_KIP_INPUT_LEN: usize = 256 * 1024;

/// Maximum accepted `(`/`{`/`[` nesting depth of a single KIP command string.
///
/// Inputs nested deeper than this are rejected before any parsing work happens,
/// protecting the recursive-descent parser from stack exhaustion.
pub const MAX_KIP_NESTING_DEPTH: usize = 64;

/// Maximum accepted number of operations in a single batch request.
///
/// A request body is bounded by the transport, but the *count* of operations it
/// decodes into is not: a small body yields tens of thousands of items and the
/// result vector pre-allocates from that count.
pub const MAX_KIP_BATCH_COMMANDS: usize = 256;

/// Parses any KIP command, classifying it by what the text actually is.
///
/// A caller-supplied language label cannot downgrade a write into read-only
/// semantics (Spec §73.1), which is why classification happens here and not
/// from the envelope.
///
/// # Examples
///
/// ```rust
/// use anda_kip::{parse_kip, Command};
///
/// let read = parse_kip(r#"FIND(?drug.name) WHERE { ?drug {type: "Drug"} }"#).unwrap();
/// assert!(matches!(read, Command::Kql(_)));
///
/// let write = parse_kip(
///     r#"ASSERT (:alice, "prefers", :dark_mode) { by: :alice, mode: "stated" }"#,
/// )
/// .unwrap();
/// assert!(write.is_mutation());
///
/// let meta = parse_kip("DESCRIBE PRIMER").unwrap();
/// assert!(matches!(meta, Command::Meta(_)));
/// ```
pub fn parse_kip(input: &str) -> Result<Command, KipError> {
    validate_parser_budget(input)?;

    let (_, command) = all_consuming(json::ws(context(
        "a KIP command: FIND (KQL), a mutation (KML), or DESCRIBE/LIST/SEARCH/… (META)",
        alt((
            map(kql::parse_kql_query, Command::Kql),
            map(kml::parse_kml_statement, Command::Kml),
            map(meta::parse_meta_command, Command::Meta),
        )),
    )))
    .parse(input)
    .map_err(|err| format_nom_error(input, err))?;

    validate_command(&command)?;
    Ok(command)
}

/// Re-runs every schema-independent rule the grammar enforces while parsing.
///
/// [`parse_kip`] and friends already ran these, so calling this on their output
/// changes nothing. It exists for a [`Command`] that did **not** come from this
/// parser: an operation may carry a pre-parsed `ast` instead of `command` text
/// (Spec §73), and such a tree has had none of the guards applied — immutable
/// Assertion/Evidence/Proposition payload, engine-owned `_system` state, the
/// frozen `PURGE` confirmation, `UPSERT` identity selectors, handle uniqueness
/// and resolution, and `BELIEF` kept out of mutation and export selections.
///
/// # Examples
///
/// ```rust
/// use anda_kip::{validate_command, Command};
///
/// // A tree that names the same handle twice never parsed from text.
/// let injected: Command = serde_json::from_str(
///     r#"{"Kml":{"explicit_transaction":true,"clauses":[
///          {"CreateConcept":{"handle":"c","type":null,"client_key":null,"name":null,
///           "set_fields":null,"set_attributes":null,"set_facets":[],"set_structural":null}},
///          {"CreateConcept":{"handle":"c","type":null,"client_key":null,"name":null,
///           "set_fields":null,"set_attributes":null,"set_facets":[],"set_structural":null}}]}}"#,
/// )
/// .unwrap();
/// assert!(validate_command(&injected).is_err());
/// ```
pub fn validate_command(command: &Command) -> Result<(), KipError> {
    match command {
        Command::Kml(statement) => kml::validate_plan(statement),
        Command::Meta(MetaCommand::ExportCapsule(export)) => {
            if export.where_clauses.is_empty() {
                return Err(KipError::invalid_syntax(
                    "EXPORT CAPSULE needs at least one selection pattern: an unbounded EXPORT is \
                     not a Capsule",
                ));
            }
            kml::validate_exact_patterns(&export.where_clauses)
        }
        _ => Ok(()),
    }
}

/// Parses a KQL query.
///
/// # Examples
///
/// ```rust
/// use anda_kip::parse_kql;
///
/// let query = parse_kql(
///     r#"FIND(?drug.name) WHERE { ?drug {type: "Drug"} } LIMIT 10"#,
/// )
/// .unwrap();
/// assert_eq!(query.where_clauses.len(), 1);
/// ```
pub fn parse_kql(input: &str) -> Result<KqlQuery, KipError> {
    validate_parser_budget(input)?;

    let (_, query) = all_consuming(json::ws(kql::parse_kql_query))
        .parse(input)
        .map_err(|err| format_nom_error(input, err))?;
    Ok(query)
}

/// Parses a KML statement.
///
/// A statement written on its own is still a one-clause transaction; the
/// `MUTATE { ... }` spelling only records that the source made that explicit.
///
/// # Examples
///
/// ```rust
/// use anda_kip::parse_kml;
///
/// let statement = parse_kml(
///     r#"MUTATE {
///         CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
///         ASSERT (?alice, "prefers", :dark_mode) { by: ?alice, mode: "stated" }
///     }"#,
/// )
/// .unwrap();
/// assert!(statement.explicit_transaction);
/// // ASSERT desugars into ENSURE PROPOSITION + CREATE ASSERTION.
/// assert_eq!(statement.clauses.len(), 3);
/// ```
pub fn parse_kml(input: &str) -> Result<KmlStatement, KipError> {
    validate_parser_budget(input)?;

    let (_, statement) = all_consuming(json::ws(kml::parse_kml_statement))
        .parse(input)
        .map_err(|err| format_nom_error(input, err))?;
    kml::validate_plan(&statement)?;
    Ok(statement)
}

/// Parses a META command.
///
/// # Examples
///
/// ```rust
/// use anda_kip::parse_meta;
///
/// let command = parse_meta("DESCRIBE PRIMER MODE \"compact\"").unwrap();
/// ```
pub fn parse_meta(input: &str) -> Result<MetaCommand, KipError> {
    validate_parser_budget(input)?;

    let (_, command) = all_consuming(json::ws(meta::parse_meta_command))
        .parse(input)
        .map_err(|err| format_nom_error(input, err))?;
    Ok(command)
}

/// Parses a standalone JSON value.
///
/// KIP's JSON dialect allows identifier keys, line comments and a trailing
/// comma, which is what makes model-written option blocks parse.
///
/// # Examples
///
/// ```rust
/// use anda_kip::parse_json;
///
/// let value = parse_json(r#"{ name: "Aspirin", dosage: 500 }"#).unwrap();
/// assert_eq!(value["dosage"], 500);
/// ```
pub fn parse_json(input: &str) -> Result<Json, KipError> {
    validate_parser_budget(input)?;

    let (_, value) = all_consuming(json::ws(json::json_value()))
        .parse(input)
        .map_err(|err| format_nom_error(input, err))?;
    Ok(value)
}

/// Rejects inputs that are too long or too deeply nested to parse safely.
fn validate_parser_budget(input: &str) -> Result<(), KipError> {
    if input.len() > MAX_KIP_INPUT_LEN {
        return Err(KipError::resource_exhausted(format!(
            "KIP input length {} exceeds maximum {MAX_KIP_INPUT_LEN}",
            input.len()
        )));
    }

    let mut stack = Vec::new();
    let mut in_string = false;
    let mut escaped = false;
    // Line comments must be skipped exactly as `skip_ws_and_comments` does, or
    // this scan desynchronizes from the real parser: a single `"` inside a
    // comment would latch `in_string` for the rest of the input and every
    // bracket after it would go uncounted, defeating the depth guard entirely.
    let mut in_line_comment = false;
    let mut prev_slash = false;

    for ch in input.chars() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_string {
            prev_slash = false;
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        if ch == '/' {
            if prev_slash {
                in_line_comment = true;
                prev_slash = false;
            } else {
                prev_slash = true;
            }
            continue;
        }
        prev_slash = false;

        match ch {
            '"' => in_string = true,
            '(' | '[' | '{' => {
                stack.push(ch);
                if stack.len() > MAX_KIP_NESTING_DEPTH {
                    return Err(KipError::resource_exhausted(format!(
                        "KIP input nesting exceeds maximum {MAX_KIP_NESTING_DEPTH}"
                    )));
                }
            }
            ')' => {
                if matches!(stack.last(), Some('(')) {
                    stack.pop();
                }
            }
            ']' => {
                if matches!(stack.last(), Some('[')) {
                    stack.pop();
                }
            }
            '}' => {
                if matches!(stack.last(), Some('{')) {
                    stack.pop();
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Converts a string to its JSON-quoted representation.
///
/// # Examples
///
/// ```rust
/// use anda_kip::quote_str;
///
/// assert_eq!(quote_str("hello"), "\"hello\"");
/// assert_eq!(quote_str("say \"hi\""), "\"say \\\"hi\\\"\"");
/// ```
pub fn quote_str(s: &str) -> String {
    Json::String(s.to_string()).to_string()
}

/// Unquotes a JSON string, returning the inner value.
///
/// # Examples
///
/// ```rust
/// use anda_kip::unquote_str;
///
/// assert_eq!(unquote_str("\"hello\""), Some("hello".to_string()));
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
    use super::*;
    use crate::ast::CommandType;

    #[test]
    fn classifies_each_surface_by_what_the_text_is() {
        let cases = [
            (r#"FIND(?x) WHERE { ?x {type: "T"} }"#, CommandType::Kql),
            (
                r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated" }"#,
                CommandType::Kml,
            ),
            (r#"MUTATE { ARCHIVE :old }"#, CommandType::Kml),
            (r#"DESCRIBE PRIMER"#, CommandType::Meta),
            (r#"SEARCH CONCEPT "aspirin""#, CommandType::Meta),
            (
                r#"EXPORT CAPSULE :out WHERE { ?c {type: "T"} }"#,
                CommandType::Meta,
            ),
        ];
        for (input, expected) in cases {
            let command = parse_kip(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(CommandType::from(&command), expected, "for {input}");
        }
    }

    #[test]
    fn a_write_is_a_write_whatever_it_is_labelled() {
        // Spec §73.1: the runtime classifies actual semantics.
        assert!(parse_kip(r#"TOMBSTONE :x"#).unwrap().is_mutation());
        assert!(!parse_kip(r#"SNAPSHOT"#).unwrap().is_mutation());
    }

    #[test]
    fn the_whole_input_must_be_one_command() {
        assert!(parse_kip(r#"DESCRIBE PRIMER DESCRIBE PROTOCOL"#).is_err());
        assert!(parse_kip(r#"FIND(?x) WHERE { ?x {a: 1} } trailing"#).is_err());
        assert!(parse_kip("").is_err());
    }

    #[test]
    fn plan_validation_runs_through_the_shared_entry_point() {
        let err = parse_kip(
            r#"MUTATE {
                CREATE CONCEPT ?c { TYPE "A" }
                CREATE CONCEPT ?c { TYPE "B" }
            }"#,
        )
        .expect_err("duplicate handle");
        assert_eq!(err.code, crate::error::KipErrorCode::DuplicateLocalHandle);
    }

    #[test]
    fn over_budget_input_is_rejected_before_parsing() {
        let deep = format!(
            "FIND(?x) WHERE {}{}",
            "[".repeat(MAX_KIP_NESTING_DEPTH + 1),
            "]".repeat(MAX_KIP_NESTING_DEPTH + 1)
        );
        let err = parse_kip(&deep).expect_err("too deep");
        assert_eq!(err.code, crate::error::KipErrorCode::ResourceExhausted);

        let long = "/".repeat(MAX_KIP_INPUT_LEN + 1);
        let err = parse_kip(&long).expect_err("too long");
        assert_eq!(err.code, crate::error::KipErrorCode::ResourceExhausted);
    }

    #[test]
    fn brackets_inside_strings_and_comments_do_not_count_against_the_budget() {
        let deep = "(".repeat(MAX_KIP_NESTING_DEPTH + 8);
        let inside_string = format!(r#"DESCRIBE TYPE "{deep}""#);
        assert!(validate_parser_budget(&inside_string).is_ok());

        let inside_comment = format!("// {deep}\nDESCRIBE PROTOCOL");
        assert!(validate_parser_budget(&inside_comment).is_ok());

        // A quote inside a comment must not latch string mode for the rest of
        // the input, which would let the real brackets go uncounted.
        let latched = format!("// \"\n{deep}");
        assert!(validate_parser_budget(&latched).is_err());
    }

    #[test]
    fn errors_point_at_a_line_and_column() {
        let err = parse_kip("FIND(?x)\nWHERE {\n  ?x CONCEPT\n}").expect_err("incomplete pattern");
        assert!(
            err.message.contains("line 3") || err.message.contains("line 4"),
            "unhelpful error: {}",
            err.message
        );
    }

    #[test]
    fn json_keeps_its_model_friendly_dialect() {
        let value = parse_json("{ a: 1, // why\n b: [2,], }").unwrap();
        assert_eq!(value["a"], 1);
        assert_eq!(value["b"][0], 2);
    }

    #[test]
    fn quoting_round_trips() {
        for raw in ["hello", "say \"hi\"", "line\nbreak", "unicode ✓"] {
            assert_eq!(unquote_str(&quote_str(raw)).as_deref(), Some(raw));
        }
        assert_eq!(unquote_str("invalid"), None);
    }
}
