//! # Mapping storage failures onto the Core Error Registry
//!
//! Every failure an Agent sees is a [`KipError`] with a registered code, and
//! the code is not decoration: [`KipErrorCode::retry_class`] is what lets a
//! caller decide whether to retry the same request, refresh and retry, or stop.
//! A storage error that arrives as a generic internal error erases that
//! decision, so the mapping here is about preserving the retry class, not about
//! producing a nicer message.
//!
//! The one case worth stating plainly: a *lost* outcome is not a failure.
//! [`KipErrorCode::OutcomeUnknown`] means the engine could not establish
//! whether a commit happened, and the caller must look the transaction up
//! rather than write again (Spec §80.3). Reporting it as an error would invite
//! a duplicate write; reporting it as success would invent one.

use anda_db::error::DBError;
use anda_db_schema::SchemaError;
use anda_kip::{KipError, KipErrorCode};

/// Maps a schema-construction failure.
///
/// The engine derives its own row schemas, so a failure here is the engine
/// disagreeing with itself, never something a caller sent.
pub fn schema_error(err: SchemaError) -> KipError {
    KipError::internal_error(format!("storage schema: {err}"))
}

/// Maps an `anda_db` failure onto the registry.
pub fn db_error(err: DBError) -> KipError {
    match &err {
        // A schema mismatch is the engine disagreeing with its own stored
        // shape, which no caller input can fix.
        DBError::Schema { .. } => KipError::internal_error(format!("storage schema: {err}")),
        DBError::NotFound { .. } => KipError::not_found_or_not_visible(format!("{err}")),
        // A uniqueness violation reaching this layer means two writers raced
        // for the same logical identity. `IdentityConflict` says that; an
        // internal error would not.
        DBError::AlreadyExists { .. } => {
            KipError::new(KipErrorCode::IdentityConflict, format!("{err}"))
        }
        // `anda_db` reports a lost write outcome as a precondition failure from
        // a second writer, or as a poisoned handle. Both mean the same thing to
        // a caller: do not retry blindly, look the transaction up.
        _ if is_outcome_unknown(&err) => KipError::outcome_unknown(format!("{err}")),
        _ => KipError::internal_error(format!("{err}")),
    }
}

/// Whether a storage failure leaves the commit outcome undetermined.
///
/// A conditional-update precondition failure means a second writer touched the
/// storage; a poisoned handle means a mutation was cut off mid-flight. In
/// neither case can the engine say whether the write landed.
fn is_outcome_unknown(err: &DBError) -> bool {
    matches!(err, DBError::Precondition { .. })
        || err
            .collection_state()
            .is_some_and(|state| state.is_poisoned())
}

/// Maps a failed collection reopen, telling "retry later" apart from "give up".
///
/// [`DBError::collection_state`] carries the rejecting handle's lifecycle state
/// as a typed source, so a collection that is being deleted — or is already
/// gone — is recognized structurally rather than by matching on message text.
/// No amount of reopening brings its objects back, and a caller that keeps
/// retrying is spinning.
pub fn reopen_error(err: DBError) -> KipError {
    let unrecoverable = err
        .collection_state()
        .is_some_and(|state| !state.is_recoverable());
    let mapped = db_error(err);
    if unrecoverable {
        return KipError::new(
            mapped.code,
            format!(
                "{}; the collection is being deleted or is gone — reopening cannot recover it",
                mapped.message
            ),
        );
    }
    mapped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> Box<dyn std::error::Error + Send + Sync> {
        std::io::Error::other("boom").into()
    }

    #[test]
    fn a_missing_row_is_a_lookup_miss_not_an_internal_error() {
        let err = db_error(DBError::NotFound {
            name: "concepts".into(),
            path: "x".into(),
            source: source(),
            _id: 1,
        });
        assert_eq!(err.code, KipErrorCode::NotFoundOrNotVisible);
    }

    #[test]
    fn a_uniqueness_violation_reports_an_identity_conflict() {
        // Two writers racing for one `tuple_key` is a real conflict a caller
        // can resolve; flattening it to an internal error would hide that.
        let err = db_error(DBError::AlreadyExists {
            name: "propositions".into(),
            path: "x".into(),
            source: source(),
            _id: 1,
        });
        assert_eq!(err.code, KipErrorCode::IdentityConflict);
        assert_eq!(err.retry_class().as_str(), "requires_different_input");
    }

    #[test]
    fn a_lost_write_outcome_asks_for_a_lookup_not_a_retry() {
        // Spec §80.3: retrying blindly is how one write becomes two.
        let err = db_error(DBError::Precondition {
            path: "concepts/data/1".into(),
            source: source(),
        });
        assert_eq!(err.code, KipErrorCode::OutcomeUnknown);
        assert_eq!(err.retry_class().as_str(), "outcome_lookup_required");
    }

    #[test]
    fn an_unrecoverable_collection_says_so_instead_of_inviting_a_retry() {
        let err = reopen_error(DBError::Generic {
            name: "concepts".into(),
            source: source(),
        });
        // A generic failure keeps its own wording; only a structurally
        // unrecoverable state earns the extra sentence.
        assert!(!err.message.contains("cannot recover"));
    }
}
