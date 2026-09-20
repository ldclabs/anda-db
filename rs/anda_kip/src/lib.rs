//! # anda_kip — a Rust implementation of KIP 2.0
//!
//! **🧬 KIP (Knowledge Interaction Protocol)** is a cognitive state protocol
//! between an Agent and a persistent **Cognitive Nexus**. KIP 2.0 is not a
//! bigger KIP 1.x: it separates the things 1.x kept in one graph —
//!
//! ```text
//! meaning · belief · evidence · provenance · mnemonic state · retention · Governance · Schema
//! ```
//!
//! — because collapsing them is what lets a memory system confidently repeat
//! things nobody ever claimed. The single distinction the rest follows from:
//!
//! ```text
//! a Proposition existing  ≠  the Proposition being true
//! ```
//!
//! A Proposition is a truth-neutral tuple. An Assertion is one actor's
//! commitment about it, with a stance, a mode, a confidence and its Evidence.
//! What is *currently believed* is projected from those, never stored.
//!
//! ## What is public
//!
//! `pub` means API. Each module is re-exported wholesale at the crate root,
//! so an item is either part of the contract this crate keeps or it is
//! `pub(crate)`; the nom combinators, the raw single-surface sub-parsers, the
//! validation gates behind [`Operation::parse`] and the semantic checks the
//! parsers apply are the latter. `tests/surface.rs` lists every public item
//! from the sources and compares the list with
//! `tests/fixtures/public_surface.txt`, so widening or narrowing the surface
//! is a change to that file, made on purpose.
//!
//! ## What this crate is
//!
//! The protocol half: parse, classify, validate. Everything that needs state —
//! Schema resolution, Governance, transactions, projection — belongs to an
//! engine behind the [`Executor`] trait.
//!
//! - [`ast`] — the executable AST, field-for-field compatible with the
//!   reference toolkit `@ldclabs/kip-lang`, so a Rust engine and a TypeScript
//!   one can be differentially tested against each other;
//! - [`parser`] — nom parsers for the three surfaces, enforcing the
//!   schema-independent rules as they parse;
//! - [`semantics`] — the Core Package registries (§20.13) and the other rules
//!   decidable without a Schema Environment;
//! - [`error`] — the Core Error Registry (§87) with categories and retry
//!   classes;
//! - [`request`] — the runtime envelope (§71–§85);
//! - [`types`] — the Core data model (§6–§19);
//! - [`capsule`] — portable Cognitive Capsules (§37–§41);
//! - [`conformance`] — the profile names an implementation declares (§89);
//! - [`executor`] — the engine seam.
//!
//! ## Standards compliance
//!
//! Follows the official KIP 2.0 specification, a copy of which ships with this
//! crate as `SPECIFICATION.md`, alongside the LLM-facing `KIPSyntax.md`.
//!
//! **👉 [KIP Specification](https://github.com/ldclabs/KIP)**
//!
//! ## Quick start
//!
//! ```rust
//! use anda_kip::{Command, parse_kip};
//!
//! // Read raw claims — truth-neutral.
//! let read = parse_kip(
//!     r#"
//!     FIND(?a.asserted_by, ?a.confidence)
//!     WHERE {
//!         ?p (:alice, "timezone", ?tz)
//!         ?a ASSERTION {proposition: ?p}
//!     }
//!     ORDER BY ?a.confidence DESC
//!     LIMIT 10
//!     "#,
//! )
//! .unwrap();
//! assert!(matches!(read, Command::Kql(_)));
//!
//! // Read what is currently believed — a Projection, not stored state.
//! let belief = parse_kip(
//!     r#"FIND(?b) WHERE { ?b BELIEF (:alice, "timezone", ?tz) }"#,
//! )
//! .unwrap();
//! assert!(matches!(belief, Command::Kql(_)));
//!
//! // Record an attributed claim. `by` and `mode` have no safe default.
//! let write = parse_kip(
//!     r#"ASSERT (:alice, "prefers", :dark_mode) {
//!         by: :alice,
//!         mode: "stated",
//!         confidence: 0.9,
//!         evidence: :msg
//!     }"#,
//! )
//! .unwrap();
//! assert!(write.is_mutation());
//! ```
//!
//! Changing your mind never rewrites history:
//!
//! ```rust
//! use anda_kip::parse_kml;
//!
//! // Correcting a claim is a new Assertion plus supersession.
//! let revision = parse_kml(
//!     r#"ASSERT ?new (:alice, "timezone", "+09:00") { by: :alice, mode: "stated" }
//!        SUPERSEDING :old"#,
//! )
//! .unwrap();
//! assert_eq!(revision.clauses.len(), 3);
//!
//! // Rewriting the old one is rejected before it reaches an engine.
//! assert!(
//!     parse_kml(
//!         r#"UPDATE ?a SET FIELDS { confidence: 0.1 }
//!            WHERE { ?a ASSERTION {id: "A-1"} }"#,
//!     )
//!     .is_err()
//! );
//! ```

// Works around a trait-solver regression in Rust 1.98.0: this lint asks "would
// the bound still hold without the `&`?", which re-enters selection with the
// borrow stripped, and on this crate that recursion does not terminate —
// `evaluate_predicate_recursively` → `enter_forall::<HostEffectPredicate>` →
// itself, until the process is ~19 GB and the machine gives up. On a GitHub
// runner the OOM takes the agent with it, so the job reports `exit code 143`
// with no diagnostic at all, which is how this cost an afternoon to find.
//
// Scoped to the crate that reproduces it, and to the one lint: 1.97.1 checks
// this crate in seconds, and every other Clippy lint still runs here. Plain
// `rustc` is unaffected — only Clippy re-runs selection this way. Remove the
// attribute once the toolchain no longer hangs; the test for that is simply
// `cargo clippy -p anda_kip --lib` on a newer stable.
#![allow(clippy::needless_borrows_for_generic_args)]

use std::sync::LazyLock;

#[macro_use]
mod vocabulary;

pub mod ast;
pub mod capsule;
pub mod conformance;
pub mod error;
pub mod executor;
pub mod json;
pub mod memory;
pub mod parser;
pub mod request;
pub mod semantics;
pub mod timestamp;
pub mod types;

pub use ast::*;
pub use capsule::*;
pub use conformance::*;
pub use error::*;
pub use executor::*;
pub use json::{MAX_SAFE_INTEGER, parse_canonical_json, validate_json};
pub use memory::*;
pub use parser::*;
pub use request::*;
pub use semantics::*;
pub use types::*;

/// The KIP 2.0 syntax reference, condensed for a model to read in context.
pub static KIP_SYNTAX: &str = include_str!("../KIPSyntax.md");

/// The Cognitive Memory Profile — the standard portable memory structures the
/// bundled prompts are written against.
///
/// [`SELF_INSTRUCTIONS`] and [`SYSTEM_INSTRUCTIONS`] both name this document
/// among the ones they assume are loaded, so a host wiring those prompts needs
/// a way to supply it. It is the Profile's prose half; the executable half is
/// the Schema Package artifact an engine installs.
pub static COGNITIVE_MEMORY_PROFILE: &str =
    include_str!("../profiles/CognitiveMemoryProfile-2.0.md");

/// How an Agent should use KIP as its own memory protocol.
pub static SELF_INSTRUCTIONS: &str = include_str!("../SelfInstructions.md");

/// What a KIP runtime owes its callers, from the execution and governance side.
pub static SYSTEM_INSTRUCTIONS: &str = include_str!("../SystemInstructions.md");

/// The tool definition for the state-capable `execute_kip` entry point.
pub static KIP_FUNCTION_DEFINITION: LazyLock<Json> =
    LazyLock::new(|| serde_json::from_str(include_str!("../FunctionDefinition.json")).unwrap());

/// The tool definition for the read-only `execute_kip_readonly` entry point.
pub static KIP_READONLY_FUNCTION_DEFINITION: LazyLock<Json> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../FunctionDefinitionReadonly.json")).unwrap()
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_function_definitions_describe_the_2_0_envelope() {
        let json = KIP_FUNCTION_DEFINITION.clone();
        assert_eq!(json["name"], Json::String("execute_kip".to_string()));
        assert_eq!(
            json["parameters"]["properties"]["command"]["type"],
            "string"
        );
        assert_eq!(
            json["parameters"]["properties"]["operations"]["type"],
            "array"
        );
        // A batch must be able to say how its operations relate (§75.4).
        let modes = &json["parameters"]["properties"]["execution"]["properties"]["mode"]["enum"];
        assert_eq!(modes[0], "independent");
        assert_eq!(modes[1], "sequence");
        assert_eq!(modes[2], "atomic");
        // Recovery from a lost response must not become a second write (§80.4).
        assert!(
            json["parameters"]["properties"]["execution"]["properties"]["idempotency_key"]
                ["description"]
                .as_str()
                .unwrap()
                .contains("same key")
        );
    }

    #[test]
    fn the_readonly_definition_offers_no_write_vocabulary() {
        let json = KIP_READONLY_FUNCTION_DEFINITION.clone();
        assert_eq!(
            json["name"],
            Json::String("execute_kip_readonly".to_string())
        );
        let description = json["description"].as_str().unwrap();
        assert!(description.contains("EXPORT CAPSULE"));
        assert!(description.contains("rejected"));
        assert!(!description.contains("MUTATE"));
        // A read-only path has no execution modes to choose between.
        assert!(json["parameters"]["properties"].get("execution").is_none());
    }

    #[test]
    fn the_bundled_prompts_teach_the_v2_distinctions() {
        for (name, text) in [
            ("SelfInstructions.md", SELF_INSTRUCTIONS),
            ("SystemInstructions.md", SYSTEM_INSTRUCTIONS),
            ("KIPSyntax.md", KIP_SYNTAX),
            (
                "profiles/CognitiveMemoryProfile-2.0.md",
                COGNITIVE_MEMORY_PROFILE,
            ),
        ] {
            assert!(!text.is_empty(), "{name} is empty");
            // The 1.x vocabulary must not survive in agent-facing prompts.
            assert!(
                !text.contains("$ConceptType"),
                "{name} still teaches the KIP 1.x schema-graph model"
            );
        }
        assert!(SELF_INSTRUCTIONS.contains("SUPERSEDING"));
        // Both prompts are deltas over the brain/ policies (upstream
        // 793af73): they name the canonical documents they assume are loaded
        // rather than restating them.
        assert!(SELF_INSTRUCTIONS.contains("brain/BrainFormation.md"));
        assert!(SYSTEM_INSTRUCTIONS.contains("brain/BrainMaintenance.md"));
        // And the LLM-facing syntax card teaches the one lifecycle statement
        // and the recovery contract.
        assert!(KIP_SYNTAX.contains("TRANSITION"));
        assert!(!KIP_SYNTAX.contains("RETRACT ASSERTION"));
        assert!(KIP_SYNTAX.contains("outcome_unknown"));
        // Both prompts tell the Agent this document is part of its context, so
        // shipping the prompts without a way to reach it would leave a host
        // unable to honour what they promise.
        for prompt in [SELF_INSTRUCTIONS, SYSTEM_INSTRUCTIONS] {
            assert!(prompt.contains("brain/") && prompt.contains("KIP"));
        }
        assert!(COGNITIVE_MEMORY_PROFILE.contains("Cognitive Memory Profile"));
    }
}

/// Protected cognitive runtime host contracts.
pub mod cognitive;
