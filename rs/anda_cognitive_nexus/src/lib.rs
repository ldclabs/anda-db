//! # anda_cognitive_nexus — a KIP 2.0 Cognitive Nexus on Anda DB
//!
//! `anda_kip` is the protocol half: it parses, classifies and validates. This
//! crate is the engine behind [`anda_kip::Executor`] — everything that needs
//! state.
//!
//! ## The distinction the whole engine is built around
//!
//! ```text
//! a Proposition existing  ≠  the Proposition being true
//! ```
//!
//! A Proposition is a truth-neutral tuple. An Assertion is one actor's
//! commitment about it, carrying a stance, a mode, a confidence and its
//! Evidence. What is *currently believed* is projected from those Assertions
//! under a named policy and is never stored.
//!
//! That is why the storage layer has an [`AssertionRow`](store::rows::AssertionRow)
//! with a `confidence` column and a [`PropositionRow`](store::rows::PropositionRow)
//! without one, and why correcting a claim writes a new Assertion plus a
//! supersession link instead of updating the old row.
//!
//! Four more distinctions the code deliberately keeps apart, each of which a
//! well-meaning simplification would collapse:
//!
//! ```text
//! missing            ≠ false
//! confidence         ≠ trust ≠ memory strength
//! retention.expires_at ≠ valid_time.until
//! Space              ≠ Domain
//! ```
//!
//! ## Status
//!
//! The KIP 2.0 engine is being built in stages. What exists today is the
//! foundation — element identity, the reference and Literal model, time
//! normalization and the storage layer. KML, KQL, Epistemic Projection and META
//! are not implemented yet; the KIP 1.x engine that used to live here was
//! removed rather than ported, because 2.0 is a different data model and a
//! renamed 1.x engine would be a worse lie than an absent one.

#![doc(html_root_url = "https://docs.rs/anda_cognitive_nexus")]

pub mod error;
pub mod id;
pub mod store;
pub mod term;
pub mod time;

pub use error::*;
pub use id::*;
pub use store::{
    Element, Store, rows,
    space::{JournalEntry, SpaceDraft},
    write::{Row, WriteContext},
};
pub use term::*;
