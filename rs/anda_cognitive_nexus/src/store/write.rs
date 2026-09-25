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
//! **Every commit advances the Space sequence.** `space_seq` is the coordinate
//! `CHANGES` pages through and `AS OF SEQ` reads at, so a mutation that skipped
//! it would be invisible to both. A transaction reserves nothing while it is
//! planned; it takes its sequence in the redo plan that writes its rows, so a
//! dry run, a refusal or a no-op leaves the coordinate where it was.
//!
//! **A version bump is a fact, not a courtesy.** `EXPECT VERSION` is the only
//! optimistic-concurrency primitive KIP has (§81), and it compares against this
//! counter.
//!
//! ## The shared envelope
//!
//! All three hold for every element kind, so the write path is generic over
//! the columns the five rows share rather than written five times.
//! `envelope_columns!` below is the single list of those columns, and
//! [`Envelope`] / [`EnvelopeMut`] are the borrowed views of them; `Element`
//! dispatches to these once, so every consumer of a shared column — the wire
//! renderer, the plane counters, the retention hook — reads it through one
//! seam instead of matching on the kind again.

use anda_db_schema::{Json, Map};
use anda_kip::{ElementKind, KipError};

use super::{Store, rows::*};
use crate::error::db_error;
use crate::id::ElementId;
use crate::time::Timestamp;

/// The columns every element row shares, in one list.
///
/// The five row structs repeat these fifteen columns rather than nesting them,
/// because a B-Tree index is built over a named column (see [`rows`]). This
/// macro is the other half of that decision: the columns are repeated in
/// storage and enumerated *once* here, so the code that works on "whatever
/// element this is" — stamping a write, rendering the wire envelope, reading a
/// retention hook, bumping a plane counter — exists once instead of five times.
///
/// Each entry is `name: type`. The entries after `@readonly` are shared for
/// *reading* only: [`Envelope`] carries every column, [`EnvelopeMut`] carries
/// the ones before the marker. `structural` sits after it because Profile
/// structural fields are writable on a Concept and refused on the other four
/// kinds ([`kml::update`](crate::kml::update)); a generic `&mut` to it here
/// would put that rule behind a comment instead of behind the type.
macro_rules! envelope_columns {
    ($mac:ident $(, $($prefix:tt)*)?) => {
        $mac! {
            $($($prefix)*)?
            /// The home MemorySpace.
            space: String,
            /// The engine-level state.
            state: String,
            /// The mutation counter.
            version: u64,
            /// The per-plane counters, in the stored wire shape.
            plane_versions: Json,
            /// The Space sequence of this change.
            seq: u64,
            /// When the engine first wrote the element.
            created_at: String,
            /// When the engine last wrote it.
            updated_at: String,
            /// The transaction that created it.
            created_tx: String,
            /// The transaction that last updated it.
            updated_tx: String,
            /// Engine origin.
            origin: Json,
            /// The element's own Governance members.
            ///
            /// Here rather than only on each row because it is written by
            /// exactly one generic path — an authorized Governance operation —
            /// and never by the cognitive stamping below, which leaves it
            /// untouched.
            governance: Json,
            /// The storage-lifecycle hook.
            retention: Json,
            /// `retention.expires_at`, lifted out for the retention sweep.
            expires_at: String,
            /// Schema-validated Facets, keyed by facet symbol.
            facets: Map<String, Json>,
            @readonly
            /// Profile structural fields: symbol → ordered array of references.
            structural: Map<String, Json>,
        }
    };
}

macro_rules! declare_envelope {
    (
        $($(#[$doc:meta])* $name:ident: $ty:ty,)*
        @readonly
        $($(#[$ro_doc:meta])* $ro:ident: $ro_ty:ty,)*
    ) => {
        /// Read access to the columns every row shares.
        ///
        /// Carries the kind and row id too, so a holder never has to match on
        /// the element again to learn what it is looking at.
        ///
        /// `#[non_exhaustive]`: only this crate builds one, and a column added
        /// to `envelope_columns!` must not break an external destructure.
        #[non_exhaustive]
        pub struct Envelope<'a> {
            /// Which Core kind this row stores.
            pub kind: ElementKind,
            /// The row id; zero until the row is inserted.
            pub id: u64,
            $($(#[$doc])* pub $name: &'a $ty,)*
            $($(#[$ro_doc])* pub $ro: &'a $ro_ty,)*
        }

        /// Mutable access to the shared columns that are generically writable.
        ///
        /// The `@readonly` columns are absent by construction — see
        /// `envelope_columns!`.
        #[non_exhaustive]
        pub struct EnvelopeMut<'a> {
            /// Which Core kind this row stores.
            pub kind: ElementKind,
            /// The row id; zero until the row is inserted.
            pub id: &'a mut u64,
            $($(#[$doc])* pub $name: &'a mut $ty,)*
        }
    };
}

envelope_columns!(declare_envelope);

mod sealed {
    /// Closes [`Row`](super::Row) to this crate's five element rows.
    ///
    /// `Row` describes the storage shapes the engine itself defines, not an
    /// extension point: `Store::elements` maps a kind to a collection, so a
    /// sixth implementor would have no collection to be written to. Sealing
    /// says so, and keeps a new required method from breaking a downstream
    /// crate that could never have implemented it usefully.
    pub trait Sealed {}
}

/// A persisted row of one Core element kind.
pub trait Row: sealed::Sealed + serde::Serialize + Send + Sync {
    /// Which Core kind this row stores.
    const KIND: ElementKind;

    /// Read access to the shared envelope columns.
    fn envelope(&self) -> Envelope<'_>;

    /// Mutable access to the shared envelope columns.
    fn envelope_mut(&mut self) -> EnvelopeMut<'_>;

    /// The row id, readable without a mutable borrow.
    fn id(&self) -> u64;

    /// Recompute denormalized reference columns before storage/index updates.
    fn refresh_index_keys(&mut self);
}

macro_rules! refresh_keys {
    ($row:ident, Activity) => {
        $row.input_keys = $row
            .inputs
            .iter()
            .map(crate::kml::clauses::endpoint_key)
            .collect();
        $row.output_keys = $row
            .outputs
            .iter()
            .map(crate::kml::clauses::endpoint_key)
            .collect();
        $row.record_keys = crate::tx::learning_record_keys(&$row.facets);
    };
    ($row:ident, Evidence) => {
        $row.record_keys = crate::tx::learning_record_keys(&$row.facets);
    };
    ($row:ident, $kind:ident) => {};
}

macro_rules! impl_row {
    (
        $ty:ident => $kind:ident,
        $($(#[$doc:meta])* $name:ident: $column:ty,)*
        @readonly
        $($(#[$ro_doc:meta])* $ro:ident: $ro_column:ty,)*
    ) => {
        impl sealed::Sealed for $ty {}

        impl Row for $ty {
            const KIND: ElementKind = ElementKind::$kind;

            fn refresh_index_keys(&mut self) { refresh_keys!(self, $kind); }

            #[inline]
            fn id(&self) -> u64 {
                self._id
            }

            #[inline]
            fn envelope(&self) -> Envelope<'_> {
                Envelope {
                    kind: Self::KIND,
                    id: self._id,
                    $($name: &self.$name,)*
                    $($ro: &self.$ro,)*
                }
            }

            #[inline]
            fn envelope_mut(&mut self) -> EnvelopeMut<'_> {
                EnvelopeMut {
                    kind: Self::KIND,
                    id: &mut self._id,
                    $($name: &mut self.$name,)*
                }
            }
        }
    };
}

envelope_columns!(impl_row, ConceptRow => Concept,);
envelope_columns!(impl_row, PropositionRow => Proposition,);
envelope_columns!(impl_row, AssertionRow => Assertion,);
envelope_columns!(impl_row, EvidenceRow => Evidence,);
envelope_columns!(impl_row, ActivityRow => Activity,);

/// The engine truth one transaction stamps on everything it writes.
///
/// One context per transaction, not per element: elements written by the same
/// commit share a `tx_id` and a `space_seq`, which is what makes "what changed
/// in transaction T" and "what changed at sequence N" the same question.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
    /// The context of a transaction opening on a Space's current snapshot.
    ///
    /// Its sequence is the one the transaction will produce if it commits, and
    /// `{space}#{seq}` its id: a sequence is taken once and never reused, so
    /// the pair is a unique transaction identity that reads as the history
    /// coordinate it is. Nothing is reserved here: the commit's redo plan
    /// takes the sequence with the rows it writes.
    pub fn tentative(space: &SpaceRow, origin: Json) -> Result<Self, KipError> {
        let seq = space
            .seq
            .checked_add(1)
            .filter(|n| *n <= anda_kip::MAX_SAFE_INTEGER)
            .ok_or_else(|| {
                KipError::resource_exhausted("Space sequence exceeds the portable numeric range")
            })?;
        Ok(Self {
            tx_id: format!("{}#{seq}", space.space_id),
            space: space.space_id.clone(),
            seq,
            at: crate::time::now(),
            origin,
        })
    }

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
    pub(crate) fn stamp_update<R: Row>(&self, row: &mut R) {
        let envelope = row.envelope_mut();
        *envelope.version = envelope.version.saturating_add(1);
        *envelope.seq = self.seq;
        *envelope.updated_at = self.at.clone();
        *envelope.updated_tx = self.tx_id.clone();
        let runtime = envelope.origin.get("_kip_runtime").cloned();
        *envelope.origin = self.origin.clone();
        if let Some(runtime) = runtime {
            envelope.origin["_kip_runtime"] = runtime;
        }
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
        row.refresh_index_keys();
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
        row.refresh_index_keys();
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
    pub async fn put<R: Row>(&self, row: &R) -> Result<(), KipError> {
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

    /// Rejects a reference that leaves the referring element's MemorySpace.
    ///
    /// Baseline Core is same-Space closed (§7): an ordinary persisted
    /// reference resolves inside the writing element's Space. Letting one
    /// through would make a later read depend on a Space the reader may have
    /// no authority over, which is exactly the leak the rule prevents.
    ///
    /// A reference to an element that does not exist is not this rule's
    /// business and passes: nothing left the Space, and refusing here would
    /// turn a stale id into a commit failure under an error that names the
    /// wrong problem.
    pub async fn check_same_space(
        &self,
        space: &str,
        from: ElementId,
        referenced: ElementId,
    ) -> Result<(), KipError> {
        let Ok(element) = self.get_element(referenced).await else {
            return Ok(());
        };
        if element.space() != space {
            return Err(KipError::structural_reference_invalid(format!(
                "{from} references {referenced}, which lives in Space {:?} rather than \
                 {space:?}; baseline KIP references resolve inside one Space",
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
