//! WebAssembly bindings for the `anda_kip` parser.
//!
//! These exist to be the **oracle** in a differential test. The JavaScript KIP
//! engine (`@ldclabs/kip-do`) parses with `@ldclabs/kip-lang`, a native
//! TypeScript implementation; nothing structural forces the two grammars to
//! agree on what a command means, so `ts/kip-do/test/parser-oracle.test.ts`
//! compares them field for field over a corpus harvested from the conformance
//! fixtures and this crate's own tests. `anda_kip` is pure computation with no
//! I/O, so it compiles to `wasm32-unknown-unknown` unchanged and can serve as
//! the reference answer.
//!
//! Nothing ships this module: it is a test dependency of the JavaScript
//! package, not part of its published tarball.
//!
//! The boundary is deliberately narrow — one string in, one JSON string out.
//! Passing the AST as JSON rather than through `serde-wasm-bindgen` keeps the
//! ABI stable across wasm-bindgen versions and makes the payload trivially
//! inspectable when the two parsers disagree.

use anda_kip::{Command, KipError, KipErrorCode, parse_kip};
use wasm_bindgen::prelude::*;

/// Semantic version of this grammar, reported so a differential run can say
/// which revision it compared against.
const PARSER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Renders a `KipError` as the same JSON envelope the engine uses on the
/// wire, so a parse failure and an execution failure are indistinguishable to
/// the client. `hint` is the spec's agent-facing recovery instruction and is
/// what makes KIP errors self-correcting — dropping it here would silently
/// degrade every syntax error the agent sees.
fn error_json(err: &KipError) -> String {
    serde_json::json!({
        "error": {
            "code": err.code_str(),
            "name": err.name(),
            "message": err.message,
            "hint": err.hint(),
        }
    })
    .to_string()
}

/// Parses a KIP command (KQL, KML or META) into its AST.
///
/// Returns a JSON string of either `{"ok": <Command>}` or
/// `{"error": {code, name, message, hint}}`. A `Result`-shaped envelope is
/// used instead of a thrown exception because wasm-bindgen's error path
/// stringifies through JS, which would lose the structured code/name/hint
/// that the whole KIP error contract rests on.
#[wasm_bindgen]
pub fn parse(input: &str) -> String {
    match parse_kip(input) {
        Ok(command) => match serde_json::to_string(&serde_json::json!({ "ok": command })) {
            Ok(json) => json,
            // The AST is `Serialize` by construction, so this is unreachable
            // in practice; report it as a parse failure rather than panicking
            // inside a Worker, where a panic aborts the whole isolate.
            Err(err) => error_json(&KipError::invalid_syntax(format!(
                "failed to serialize the parsed AST: {err}"
            ))),
        },
        Err(err) => error_json(&err),
    }
}

/// Parses a batch of commands in one call.
///
/// A KIP request may carry a `commands` array (the multi-statement KML form),
/// and each crossing of the JS/WASM boundary costs a string copy in both
/// directions. Batching keeps that cost proportional to the payload rather
/// than to the number of statements.
///
/// Input is a JSON array of strings; output is a JSON array of the same
/// envelopes [`parse`] returns, positionally aligned with the input.
#[wasm_bindgen]
pub fn parse_batch(inputs_json: &str) -> String {
    let inputs: Vec<String> = match serde_json::from_str(inputs_json) {
        Ok(inputs) => inputs,
        Err(err) => {
            return error_json(&KipError::invalid_syntax(format!(
                "expected a JSON array of command strings: {err}"
            )));
        }
    };

    let results: Vec<serde_json::Value> = inputs
        .iter()
        .map(|input| match parse_kip(input) {
            Ok(command) => serde_json::json!({ "ok": command }),
            Err(err) => serde_json::json!({
                "error": {
                    "code": err.code_str(),
                    "name": err.name(),
                    "message": err.message,
                    "hint": err.hint(),
                }
            }),
        })
        .collect();

    serde_json::Value::Array(results).to_string()
}

/// Returns the grammar version this module was built from.
#[wasm_bindgen]
pub fn parser_version() -> String {
    PARSER_VERSION.to_string()
}

/// Every `KipErrorCode` variant. Listed explicitly because the enum has no
/// iterator; a missing or misspelled variant is a compile error, which is the
/// property that makes the generated TypeScript table trustworthy.
const ALL_ERROR_CODES: &[KipErrorCode] = &[
    KipErrorCode::InvalidSyntax,
    KipErrorCode::InvalidIdentifier,
    KipErrorCode::TypeMismatch,
    KipErrorCode::ConstraintViolation,
    KipErrorCode::InvalidValueType,
    KipErrorCode::ReferenceError,
    KipErrorCode::NotFound,
    KipErrorCode::DuplicateExists,
    KipErrorCode::ImmutableTarget,
    KipErrorCode::VersionConflict,
    KipErrorCode::ExecutionTimeout,
    KipErrorCode::ResourceExhausted,
    KipErrorCode::InternalError,
];

/// Dumps the complete KIP error taxonomy as JSON.
///
/// `scripts/codegen-errors.mjs` turns this into `src/errors.generated.ts` so
/// the TypeScript engine cannot drift from the Rust definitions. Transcribing
/// 13 codes, names and agent-facing hints by hand is exactly the kind of task
/// that looks done and is subtly wrong — and a wrong `hint` degrades the
/// agent's self-correction loop silently, with no test to catch it.
#[wasm_bindgen]
pub fn error_catalog() -> String {
    let entries: Vec<serde_json::Value> = ALL_ERROR_CODES
        .iter()
        .map(|code| {
            serde_json::json!({
                "code": code.code(),
                "name": code.name(),
                "hint": code.hint(),
            })
        })
        .collect();
    serde_json::Value::Array(entries).to_string()
}

/// Round-trips a command through parse and re-serialization.
///
/// Used by the conformance harness to assert that the TypeScript AST mirror
/// in `src/kip/ast.ts` stays structurally aligned with the Rust definitions:
/// the harness parses with WASM, reconstructs the value in TS, and compares.
#[wasm_bindgen]
pub fn parse_to_command_type(input: &str) -> String {
    match parse_kip(input) {
        Ok(Command::Kql(_)) => "KQL".to_string(),
        Ok(Command::Kml(_)) => "KML".to_string(),
        Ok(Command::Meta(_)) => "META".to_string(),
        Err(err) => error_json(&err),
    }
}
