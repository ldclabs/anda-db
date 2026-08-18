//! # Migrating a KIP 1.x Nexus into KIP 2.0
//!
//! A 1.x database and a 2.0 database are not two versions of one layout. They
//! disagree about what a row *means*, and they collide on the two collection
//! names that matter: `concepts` and `propositions`. So this is not a schema
//! upgrade — `anda_db` would not perform one anyway, because
//! `Schema::needs_upgrade` compares version numbers and both derive to 0, which
//! means the 2.0 schema is silently ignored and the collection keeps the 1.x
//! one. The engine then fails a few lines later building an index on a field
//! the old schema never had. Safe, but only by accident, and unreadable as a
//! diagnosis.
//!
//! What runs instead is a real migration, in three phases with a durable
//! staging area between them, so a crash at any point resumes rather than
//! loses:
//!
//! ```text
//! 1. extract   1.x rows           → kip_legacy_v1   (verbatim, no interpretation)
//! 2. drop      concepts, propositions               (2.0 can now create its own)
//! 3. load      kip_legacy_v1      → 2.0 elements    (idempotent, by client_key)
//! ```
//!
//! Phase 1 and 2 run before `Store::open`; phase 3 needs a working engine, so
//! it runs after the rest of `connect` has bootstrapped Governance and Core.
//!
//! ## What the staging area buys
//!
//! The 1.x rows are copied out *before* anything is deleted, and they are kept
//! afterwards. A migration that read into memory, dropped the source and then
//! crashed would have destroyed the only copy. Keeping `kip_legacy_v1` also
//! means the original is still inspectable when someone asks in six months why
//! a Concept looks the way it does — the answer is a row away, in the shape it
//! was actually stored in.
//!
//! ## What it deliberately does not do
//!
//! The migration guide's §2 is a list of things migration must not fabricate:
//! verified identity, source trust, Evidence that never existed, actor
//! authentication, independent corroboration. 1.x recorded none of them, so
//! neither does this. Concretely:
//!
//! - every migrated Assertion carries `mode: "imported"`, which is the
//!   registered mode meaning *carried in from another system* (§13). Nothing
//!   invents `observed` or `stated` for a record whose origin was a database
//!   row;
//! - `asserted_by` is the system Principal's own Concept unless the 1.x
//!   metadata named an author that resolves to a real migrated Concept. A
//!   string that resolves to nothing stays an attribute rather than becoming a
//!   speaker (§12);
//! - legacy `confidence` is carried onto the Assertion *and* preserved
//!   verbatim under `attributes.legacy`, because 1.x deployments used that
//!   field for several different things and only the operator knows which
//!   (§13, §14);
//! - `access_level` is preserved as a legacy attribute and does **not** become
//!   a classification. 1.x's value annotated; 2.0's enforces, and promoting one
//!   to the other silently would either over- or under-protect every migrated
//!   element (§21).

mod convert;
mod package;
mod stage;

pub use package::{LEGACY_PACKAGE_ID, legacy_package_ref};
pub use stage::{LEGACY_STAGING, LegacyKind, LegacyRow};

use anda_db::database::AndaDB;
use anda_kip::KipError;
use std::sync::Arc;

/// The `client_key` prefix every migrated element carries.
///
/// It is what makes phase 3 resumable: a re-run resolves the key to the
/// element a previous attempt created instead of writing a second one. The
/// legacy id is part of the key, so the mapping survives a restart without a
/// side table that could outlive the elements it points at.
pub const MIGRATION_KEY_PREFIX: &str = "kip:migrate:v1:";

/// Extracts and clears a 1.x layout, if one is present.
///
/// Runs before `Store::open`, because after it the 2.0 collections exist under
/// the names the 1.x ones occupy.
pub(crate) async fn prepare(db: &Arc<AndaDB>) -> Result<(), KipError> {
    stage::prepare(db).await
}

/// Loads staged 1.x rows into the 2.0 graph, if any are outstanding.
///
/// Runs after `connect` has bootstrapped, because it writes through the engine
/// rather than around it: every migrated element goes through the same
/// validation, Governance and transaction path as an ordinary write. A
/// migration that bypassed them would be the one writer in the system allowed
/// to produce elements the engine would have refused.
pub(crate) async fn load(nexus: &crate::CognitiveNexus) -> Result<(), KipError> {
    convert::load(nexus).await
}

/// Keeps the migration's generated vocabulary active across a host's own
/// activation.
///
/// A host calls `install_and_activate` with its baseline packages on every
/// start — that is the documented pattern, and the lock it builds names only
/// what the host knows about. It does not know about a package this engine
/// generated during a migration, so the next ordinary start would deactivate
/// it and every migrated Concept's `schema_ref` would stop resolving. The
/// elements would still be there; nothing would be able to read them.
///
/// So the legacy package is retained unless a caller deactivates it *by name*,
/// which stays possible: a lock that mentions the package is taken at its
/// word. This is the one package that survives an activation that forgot it,
/// and it exists only because the alternative is a host orphaning its own
/// migrated history by doing exactly what the documentation tells it to.
pub(crate) fn retain_legacy_package(
    current: &crate::schema::SchemaLock,
    next: &mut crate::schema::SchemaLock,
) {
    if next.packages.contains_key(LEGACY_PACKAGE_ID) {
        return;
    }
    let Some(version) = current.packages.get(LEGACY_PACKAGE_ID) else {
        return;
    };
    if current.states.get(LEGACY_PACKAGE_ID) != Some(&crate::schema::PackageState::Active) {
        return;
    }
    next.packages
        .insert(LEGACY_PACKAGE_ID.to_string(), version.clone());
    next.states.insert(
        LEGACY_PACKAGE_ID.to_string(),
        crate::schema::PackageState::Active,
    );
}
