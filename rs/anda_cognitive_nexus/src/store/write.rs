//! # The write path
//!
//! Everything durable goes through here, so that three things can be true of
//! every element without each call site having to remember them.
//!
//! **`_system` is engine truth, not payload.** A caller supplies semantics;
//! the engine stamps `version`, `created_at`, `updated_at`, `created_tx`,
//! `updated_tx`, `state`, `space_seq` and `origin`. These are non-malleable by
//! construction (Spec §26) — not because a validator rejects them, but because
//! the only code that writes them is this module.
//!
//! **Every write advances the Space sequence.** `space_seq` is the coordinate
//! `CHANGES` pages through and `AS OF SEQ` reads at, so a mutation that skipped
//! it would be invisible to both.
//!
//! **A version bump is a fact, not a courtesy.** `EXPECT VERSION` is the only
//! optimistic-concurrency primitive KIP has (§81), and it compares against this
//! counter.

use anda_db_schema::Json;
use anda_kip::{ElementKind, KipError};

use super::{Store, rows::*};
use crate::error::db_error;
use crate::id::ElementId;
use crate::time::Timestamp;

/// Mutable access to the columns every row shares.
///
/// Written by hand once per row through [`impl_row`], so that the generic
/// write path below exists once instead of five times.
pub struct EnvelopeMut<'a> {
    /// The row id; zero until the row is inserted.
    pub id: &'a mut u64,
    /// The home MemorySpace.
    pub space: &'a mut String,
    /// The engine-level state.
    pub state: &'a mut String,
    /// The mutation counter.
    pub version: &'a mut u64,
    /// The Space sequence of this change.
    pub seq: &'a mut u64,
    /// When the engine first wrote the element.
    pub created_at: &'a mut String,
    /// When the engine last wrote it.
    pub updated_at: &'a mut String,
    /// The transaction that created it.
    pub created_tx: &'a mut String,
    /// The transaction that last updated it.
    pub updated_tx: &'a mut String,
    /// Engine origin.
    pub origin: &'a mut Json,
    /// The element's own Governance members.
    ///
    /// Here rather than only on each row because it is written by exactly one
    /// generic path — an authorized Governance operation — and never by the
    /// cognitive stamping below, which leaves it untouched.
    pub governance: &'a mut Json,
}

/// A persisted row of one Core element kind.
pub trait Row: serde::Serialize + Send + Sync {
    /// Which Core kind this row stores.
    const KIND: ElementKind;

    /// Mutable access to the shared envelope columns.
    fn envelope_mut(&mut self) -> EnvelopeMut<'_>;

    /// The row id, readable without a mutable borrow.
    fn id(&self) -> u64;
}

macro_rules! impl_row {
    ($($ty:ident => $kind:ident),* $(,)?) => {
        $(
            impl Row for $ty {
                const KIND: ElementKind = ElementKind::$kind;

                fn id(&self) -> u64 {
                    self._id
                }

                fn envelope_mut(&mut self) -> EnvelopeMut<'_> {
                    EnvelopeMut {
                        id: &mut self._id,
                        space: &mut self.space,
                        state: &mut self.state,
                        version: &mut self.version,
                        seq: &mut self.seq,
                        created_at: &mut self.created_at,
                        updated_at: &mut self.updated_at,
                        created_tx: &mut self.created_tx,
                        updated_tx: &mut self.updated_tx,
                        origin: &mut self.origin,
                        governance: &mut self.governance,
                    }
                }
            }
        )*
    };
}

impl_row! {
    ConceptRow => Concept,
    PropositionRow => Proposition,
    AssertionRow => Assertion,
    EvidenceRow => Evidence,
    ActivityRow => Activity,
}

/// The engine truth one transaction stamps on everything it writes.
///
/// One context per transaction, not per element: elements written by the same
/// commit share a `tx_id` and a `space_seq`, which is what makes "what changed
/// in transaction T" and "what changed at sequence N" the same question.
#[derive(Clone, Debug)]
pub struct WriteContext {
    /// The Space being written to.
    pub space: String,
    /// The transaction id.
    pub tx_id: String,
    /// The Space sequence this commit produces.
    pub seq: u64,
    /// The commit instant.
    pub at: Timestamp,
    /// Engine origin: the authenticated Principal and channel behind the
    /// write. Never taken from content (§26).
    pub origin: Json,
}

impl WriteContext {
    /// Stamps a freshly created element.
    fn stamp_new<R: Row>(&self, row: &mut R) {
        let envelope = row.envelope_mut();
        *envelope.id = 0;
        *envelope.space = self.space.clone();
        if envelope.state.is_empty() {
            *envelope.state = state::ACTIVE.to_string();
        }
        // A new element is at version 1, not 0: `EXPECT VERSION 1` on a
        // just-created element must succeed, and a zero would make "never
        // written" and "written once" the same value.
        *envelope.version = 1;
        *envelope.seq = self.seq;
        *envelope.created_at = self.at.clone();
        *envelope.updated_at = self.at.clone();
        *envelope.created_tx = self.tx_id.clone();
        *envelope.updated_tx = self.tx_id.clone();
        *envelope.origin = self.origin.clone();
    }

    /// Stamps an update, preserving the creation coordinates.
    ///
    /// `created_at` and `created_tx` are deliberately untouched: they record
    /// when this element entered the Nexus, and an update that refreshed them
    /// would erase the only engine-side record of that.
    fn stamp_update<R: Row>(&self, row: &mut R) {
        let envelope = row.envelope_mut();
        *envelope.version = envelope.version.saturating_add(1);
        *envelope.seq = self.seq;
        *envelope.updated_at = self.at.clone();
        *envelope.updated_tx = self.tx_id.clone();
        *envelope.origin = self.origin.clone();
    }
}

impl Store {
    /// Inserts a new element and returns its minted id.
    ///
    /// The caller fills in semantics; everything in [`EnvelopeMut`] is
    /// overwritten here, so a caller cannot smuggle a `version` or a
    /// `created_tx` in through the row it hands over.
    pub async fn insert<R: Row>(
        &self,
        cx: &WriteContext,
        row: &mut R,
    ) -> Result<ElementId, KipError> {
        cx.stamp_new(row);
        let collection = self.elements(R::KIND);
        let seq = collection.add_from(row).await.map_err(db_error)?;
        row.envelope_mut().id.clone_from(&seq);
        Ok(ElementId::new(R::KIND, seq))
    }

    /// Writes an updated element back, bumping its version.
    ///
    /// The whole row is rewritten rather than a computed field delta. A delta
    /// would be smaller, but it would also be a second place where the set of
    /// columns is enumerated, and a column missing from that list would
    /// silently stop being persisted.
    pub async fn update<R: Row>(&self, cx: &WriteContext, row: &mut R) -> Result<u64, KipError> {
        cx.stamp_update(row);
        let id = *row.envelope_mut().id;
        let collection = self.elements(R::KIND);
        let fields = super::full_row_fields(collection.schema(), row)?;
        collection.update(id, fields).await.map_err(db_error)?;
        Ok(*row.envelope_mut().version)
    }

    /// Writes a row back exactly as given, touching no envelope column.
    ///
    /// The version-bumping [`Store::update`] is the right primitive for a
    /// standalone edit; this one is for a transaction commit, where the
    /// version was already decided once for the whole transaction (§44).
    pub(crate) async fn put_row<R: Row>(&self, row: &R) -> Result<(), KipError> {
        let collection = self.elements(R::KIND);
        let id = row.id();
        let fields = super::full_row_fields(collection.schema(), row)?;
        collection.update(id, fields).await.map_err(db_error)?;
        Ok(())
    }

    /// Checks an `EXPECT VERSION` precondition (Spec §81).
    ///
    /// A mismatch is a [`KipErrorCode::VersionConflict`](anda_kip::KipErrorCode::VersionConflict),
    /// whose retry class tells the caller to re-read before trying again —
    /// which is the whole point of asking.
    pub fn expect_version(id: ElementId, actual: u64, expected: u64) -> Result<(), KipError> {
        if actual != expected {
            return Err(KipError::version_conflict(format!(
                "{id} is at version {actual}, not the expected {expected}"
            )));
        }
        Ok(())
    }

    /// Rejects a reference that leaves the element's own MemorySpace.
    ///
    /// Baseline Core is same-Space closed (§7): an ordinary persisted
    /// reference resolves inside the writing element's Space. Letting one
    /// through would make a later read depend on a Space the reader may have
    /// no authority over, which is exactly the leak the rule prevents.
    pub async fn check_same_space(
        &self,
        space: &str,
        referenced: ElementId,
        field: &str,
    ) -> Result<(), KipError> {
        let element = self.get_element(referenced).await?;
        if element.space() != space {
            return Err(KipError::structural_reference_invalid(format!(
                "`{field}` references {referenced}, which lives in Space {:?} rather than {space:?}; \
                 baseline KIP references resolve inside one Space",
                element.space()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn context() -> WriteContext {
        WriteContext {
            space: "space-1".into(),
            tx_id: "tx-1".into(),
            seq: 7,
            at: "2026-08-16T00:00:00.000Z".into(),
            origin: json!({"principal_id": "p-1", "channel": "test"}),
        }
    }

    #[test]
    fn a_caller_cannot_supply_its_own_engine_truth() {
        // Spec §26: engine origin is non-malleable, and neither is the rest of
        // `_system`. A row arriving with a forged version and origin must come
        // out stamped by the engine.
        let mut row = ConceptRow {
            version: 99,
            created_tx: "tx-forged".into(),
            origin: json!({"principal_id": "root"}),
            name: "Alice".into(),
            ..Default::default()
        };
        context().stamp_new(&mut row);
        assert_eq!(row.version, 1);
        assert_eq!(row.created_tx, "tx-1");
        assert_eq!(
            row.origin,
            json!({"principal_id": "p-1", "channel": "test"})
        );
        assert_eq!(row.name, "Alice", "semantics are the caller's");
        assert_eq!(row.space, "space-1");
        assert_eq!(row.seq, 7);
        assert_eq!(row.state, state::ACTIVE);
    }

    #[test]
    fn an_update_keeps_the_creation_coordinates() {
        let mut row = ConceptRow::default();
        context().stamp_new(&mut row);
        let created_at = row.created_at.clone();

        let later = WriteContext {
            tx_id: "tx-2".into(),
            seq: 9,
            at: "2026-08-17T00:00:00.000Z".into(),
            ..context()
        };
        later.stamp_update(&mut row);

        assert_eq!(row.version, 2);
        assert_eq!(row.seq, 9);
        assert_eq!(row.updated_tx, "tx-2");
        assert_eq!(row.updated_at, "2026-08-17T00:00:00.000Z");
        // The only engine-side record of when this element entered the Nexus.
        assert_eq!(row.created_at, created_at);
        assert_eq!(row.created_tx, "tx-1");
    }

    #[test]
    fn a_new_element_starts_at_version_one() {
        // `EXPECT VERSION 1` against a just-created element must succeed.
        let mut row = AssertionRow::default();
        context().stamp_new(&mut row);
        let id = ElementId::new(ElementKind::Assertion, 1);
        assert!(Store::expect_version(id, row.version, 1).is_ok());
        let err = Store::expect_version(id, row.version, 2).unwrap_err();
        assert_eq!(err.name(), "VersionConflict");
        assert_eq!(err.retry_class().as_str(), "requires_refresh");
    }

    #[test]
    fn an_explicit_state_survives_creation() {
        // An imported element may arrive already archived; creation must not
        // quietly promote it back into ordinary recall.
        let mut row = EvidenceRow {
            state: state::ARCHIVED.into(),
            ..Default::default()
        };
        context().stamp_new(&mut row);
        assert_eq!(row.state, state::ARCHIVED);
    }
}
