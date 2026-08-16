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
//! A fifth distinction has its own module. In KIP 1.x, authoritative schema
//! was graph state, so an ordinary write could change what a type meant. Here
//! it is an immutable versioned artifact resolved through a per-Space
//! [`SchemaEnvironment`](schema::SchemaEnvironment), and every persisted
//! `schema_ref` names an exact version — which is why an element's meaning
//! cannot drift when somebody publishes something.
//!
//! ## Status
//!
//! The KIP 2.0 engine is being built in stages. [`CognitiveNexus`] implements
//! [`anda_kip::Executor`] and today runs:
//!
//! - **KML** — creation, `ENSURE`, `UPSERT`, the Assertion and Evidence
//!   lifecycles, retention and removal, inside real transactions with handles,
//!   preconditions, receipts and dry runs;
//! - **KQL** — element and tuple patterns, structural patterns, `FILTER`,
//!   `NOT` / `OPTIONAL` / `UNION`, projection by dot path, aggregates,
//!   `ORDER BY`, paging and `FOR TIME`;
//! - **BELIEF / BELIEF SLOT** — the [`Epistemic Projection`](projection),
//!   under a named, versioned policy, with an explanation ledger.
//!
//! The projection is partial and says so in its own output: there is no trust
//! model and no evidence-quality evaluation in this engine, so every eligible
//! corroboration group counts equally, and every answer carries that warning
//! rather than reading as a judgement it did not make.
//!
//! Not implemented yet, and reported as `UnsupportedCapability` rather than
//! answered wrongly:
//!
//! ```text
//! META introspection
//! AS OF                                   historical snapshots
//! hop quantifiers                         transitive traversal
//! UPDATE / PURGE / MERGE CONCEPT
//! clause forms with a WHERE block
//! ```
//!
//! An engine that returned empty results for a read it cannot perform would be
//! worse than one that says so: an Agent would read "no memories" as an answer
//! about the world.
//!
//! The KIP 1.x engine that used to live here was removed rather than ported,
//! because 2.0 is a different data model and a renamed 1.x engine would be a
//! worse lie than an absent one.

#![doc(html_root_url = "https://docs.rs/anda_cognitive_nexus")]

pub mod error;
pub mod id;
pub mod kml;
pub mod kql;
pub mod nexus;
pub mod projection;
pub mod schema;
pub mod store;
pub mod term;
pub mod time;
pub mod tx;
pub mod view;

pub use error::*;
pub use id::*;
pub use nexus::CognitiveNexus;
pub use store::{
    Element, Store, rows,
    space::{JournalEntry, SpaceDraft},
    write::{Row, WriteContext},
};
pub use term::*;
