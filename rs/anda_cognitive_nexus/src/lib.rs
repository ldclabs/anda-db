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
//! - **KML** — creation, `ENSURE`, `UPSERT`, `UPDATE`, `MERGE CONCEPT`, the
//!   Assertion and Evidence lifecycles, retention and removal, each with an
//!   optional `WHERE` selection block and `LIMIT`, inside real transactions
//!   with handles, preconditions, receipts and dry runs;
//! - **KQL** — element and tuple patterns, structural patterns, hop-quantified
//!   path traversal, `FILTER`, `NOT` / `OPTIONAL` / `UNION`, projection by dot
//!   path, aggregates, `ORDER BY`, paging, and both time axes: `FOR TIME` (what
//!   was true then) and `AS OF` (what this Brain held then);
//! - **BELIEF / BELIEF SLOT** — the [`Epistemic Projection`](projection),
//!   under a named, versioned policy, with an explanation ledger;
//! - **META** — `DESCRIBE`, `LIST`, `SEARCH`, `VALIDATE`, `PREVIEW`,
//!   `HISTORY`, `CHANGES`, `SNAPSHOT`, plus `EXPORT CAPSULE` and
//!   `VERIFY CAPSULE`; Capsule import is a host API
//!   ([`CognitiveNexus::import_capsule`]);
//! - **[Governance](governance)** — Principals, groups, Grants, Delegations,
//!   ActorBindings, versioned Policies, approvals and an append-preserving
//!   audit. Every command is authorized before it runs, under default deny,
//!   and so is every element a read or a write touches. Classification joins
//!   upward along derivation links, influence authority never amplifies along
//!   them, quarantine holds cognition out of use without claiming its author
//!   took it back, and `PURGE` erases content while leaving an identity stub.
//!
//! A caller reaches the engine through [`CognitiveNexus::session`], which binds
//! an [`AuthContext`](governance::AuthContext) the *host* built from
//! authenticated transport state — never from the request body, whose own
//! `context` block is documented as non-authoritative because an Agent under
//! prompt injection can write anything into it.
//!
//! An embedded host that simply executes against the [`CognitiveNexus`] runs as
//! the system Principal, which owns the default Space. That is a real
//! authorization through the same path, not a bypass; a host serving more than
//! one caller must authenticate them and open a session each.
//!
//! `DESCRIBE CAPABILITIES` reports the gaps below as structured data, so an
//! Agent can read what is missing instead of discovering it by triggering an
//! error — or, worse, reading an absent feature as an absent fact.
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
//! semantic / hybrid SEARCH       no embedding model
//! SEARCH ... AS OF SEQ           the index reflects the present only
//! Capsule signatures               nothing is signed
//! the "restore" import mode        identity continuity is not modelled
//! DESCRIBE TRUST                   no trust evaluation to report
//! Space-level retention defaults   retention is set per element
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

pub mod capsule;
pub mod error;
pub mod governance;
pub mod id;
pub mod kml;
pub mod kql;
pub mod meta;
pub mod nexus;
pub mod profiles;
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
