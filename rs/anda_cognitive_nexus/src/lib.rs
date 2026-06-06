//! # `anda_cognitive_nexus`
//!
//! A reference implementation of the **Knowledge Interaction Protocol (KIP)**
//! [`Executor`](anda_kip::Executor) backed by [Anda DB][anda_db].
//!
//! [anda_db]: https://crates.io/crates/anda_db
//!
//! The crate exposes a single high-level type, [`CognitiveNexus`], which
//! manages a persistent knowledge graph of:
//!
//! - **Concept nodes** ([`Concept`]) — identified by `{type, name}`, carrying
//!   `attributes` and `metadata`.
//! - **Proposition links** ([`Proposition`]) — directed triples
//!   `(subject, predicate, object)` where subject/object may themselves be
//!   propositions (higher-order facts). Each row stores all of its
//!   predicates in a single record to keep the link space compact.
//!
//! `CognitiveNexus` accepts the full KIP v1.0-RC7 grammar — KQL queries
//! (`FIND … WHERE …`), KML mutations (`UPSERT`, `DELETE …`) and META
//! introspection (`DESCRIBE …`, `SEARCH …`) — and translates them into
//! Anda DB collection operations using a small, well-defined index plan
//! (BTree + BM25 over the `concepts` and `propositions` collections).
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use anda_db::database::{AndaDB, DBConfig};
//! use anda_cognitive_nexus::CognitiveNexus;
//! use object_store::memory::InMemory;
//!
//! # async fn run() -> Result<(), anda_kip::KipError> {
//! let db = AndaDB::connect(Arc::new(InMemory::new()), DBConfig::default())
//!     .await
//!     .map_err(anda_cognitive_nexus::db_to_kip_error)?;
//! let nexus = CognitiveNexus::connect(Arc::new(db), async |_| Ok(())).await?;
//!
//! // Run any KIP command via the [`anda_kip::Executor`] trait.
//! use anda_kip::{parse_kml, parse_kql};
//! nexus.execute_kml(parse_kml(r#"
//!     UPSERT {
//!         CONCEPT ?d { {type: "$ConceptType", name: "Drug"} }
//!     }
//! "#)?, false).await?;
//! # Ok(()) }
//! ```
//!
//! ## Module layout
//!
//! - [`db`] — the [`CognitiveNexus`] type and KIP executor implementation.
//! - [`entity`] — persisted graph data model
//!   ([`Concept`], [`Proposition`], [`EntityID`], [`Properties`]).
//! - `helper` — small extraction / sorting / error-mapping utilities
//!   ([`extract_concept_field_value`], [`apply_order_by`],
//!   [`db_to_kip_error`], …).
//! - `types` — query-execution scaffolding ([`ConceptPK`],
//!   [`PropositionPK`], [`EntityPK`], [`QueryContext`], [`TargetEntities`]).
//!
//! See the technical reference at `docs/anda_cognitive_nexus.md` for the
//! full storage layout, indexing strategy, and KIP semantics.

pub mod db;
pub mod entity;

mod helper;
mod types;

pub use db::*;
pub use entity::*;
pub use helper::*;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db::database::{AndaDB, DBConfig};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn build_future() {
        let db = AndaDB::connect(Arc::new(InMemory::new()), DBConfig::default())
            .await
            .unwrap();

        let _nexus = CognitiveNexus::connect(Arc::new(db), async |_nexus| Ok(()))
            .await
            .unwrap();
    }
    fn assert_send<T: Send>(_: &T) {}

    #[tokio::test]
    async fn test_async_send_lifetime() {
        let fut = build_future();
        assert_send(&fut); // 编译报错信息会更聚焦
        fut.await;
    }
}
