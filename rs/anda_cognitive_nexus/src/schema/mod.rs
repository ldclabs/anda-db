//! # Schema Packages and the Schema Environment
//!
//! In KIP 1.x, authoritative schema lived in the graph as `$ConceptType` and
//! `$PropositionType` nodes, which meant an ordinary write could change what a
//! type meant. KIP 2.0 moves it out: authoritative Schema is an **immutable
//! versioned Package Artifact**, and a graph node that mirrors one is a mirror,
//! never the authority (Spec §5, §240.1–§240.3).
//!
//! - [`symbol`] — canonical identity: `kip://<path>@<version>/<Symbol>`, and
//!   the rule that every persisted reference names an exact version;
//! - [`package`] — the artifact itself, modelled on the shipped format;
//! - [`env`] — the per-Space resolution set, which turns a model-facing local
//!   name into one exact symbol or fails saying why;
//! - [`validate`] — the package validation layer, which is deliberately
//!   narrower than Core validation and cannot weaken it;
//! - [`apply`] — the seam a mutation calls: resolve, validate, and hand back
//!   the exact symbol to persist.
//!
//! ## The line this module will not cross
//!
//! A Schema Package declares what things *are*. It never declares what is
//! *true*, who may read anything, or how much anything is trusted (§33, §96,
//! §240.23–§240.25). Concretely: a `functional` predicate does not reject a
//! second competing object, it creates a conflict for the Epistemic Projection
//! to report — because a memory system that cannot store disagreement cannot
//! report it either.

pub mod apply;
pub mod env;
pub mod package;
pub mod symbol;
pub mod validate;

pub use apply::EndpointFacts;
pub use env::{Intent, PackageState, SchemaEnvironment, SchemaLock};
pub use package::SchemaPackage;
pub use symbol::{PackageRef, SymbolKind, SymbolRef, Version};
pub use validate::{Severity, Validation, Violation};
