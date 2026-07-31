//! Error types for schema module
use anda_db_btree::BTreeError;
use anda_db_hnsw::HnswError;
use anda_db_tfs::BM25Error;
use std::fmt;
use thiserror::Error;

use crate::schema::{BoxError, SchemaError};

/// Lifecycle state of a [`Collection`](crate::collection::Collection) handle.
///
/// A handle in any state other than [`Active`](Self::Active) rejects every
/// operation. The rejection is a [`DBError::Generic`] whose `source` carries
/// the state as a [`CollectionStateError`]; recover it with
/// [`DBError::collection_state`] instead of matching on the message text,
/// which is not part of the API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CollectionState {
    /// The handle accepts operations.
    Active,
    /// [`Collection::close`](crate::collection::Collection::close) is running.
    Closing,
    /// The handle was closed; its storage is intact.
    Closed,
    /// A collection deletion is running; storage is being removed.
    Deleting,
    /// The collection was deleted; its storage is gone.
    Deleted,
    /// A mutating call was cancelled mid-operation, so the in-memory state may
    /// have diverged from storage. Reopening the collection loads a fresh,
    /// consistent generation.
    Poisoned,
}

impl CollectionState {
    /// Whether reopening the collection through the database yields a usable
    /// handle again.
    ///
    /// `true` for [`Poisoned`](Self::Poisoned) — the whole point of poisoning
    /// is that reopen recovery can reconcile the handle — and for
    /// [`Closing`](Self::Closing)/[`Closed`](Self::Closed), whose storage is
    /// untouched. `false` from [`Deleting`](Self::Deleting) onwards: the
    /// objects are (partially) gone and no reopen brings them back, so a
    /// caller should give up rather than retry.
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::Active | Self::Closing | Self::Closed | Self::Poisoned
        )
    }

    /// Whether the handle was poisoned by a cancelled mutation.
    pub fn is_poisoned(&self) -> bool {
        matches!(self, Self::Poisoned)
    }

    /// The human-readable phrase used in the rejection message.
    fn phrase(&self) -> &'static str {
        match self {
            Self::Active => "not writable",
            Self::Closing => "closing",
            Self::Closed => "closed",
            Self::Deleting => "being deleted",
            Self::Deleted => "deleted",
            Self::Poisoned => {
                "poisoned (a mutating call was cancelled mid-operation); reopen the collection to recover"
            }
        }
    }
}

/// The structured source of the [`DBError::Generic`] a collection handle
/// returns when its lifecycle state rejects an operation.
///
/// Its only purpose is to make the state recoverable by type instead of by
/// message; use [`DBError::collection_state`] or
/// [`DBError::is_poisoned`] rather than constructing or matching this
/// directly.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CollectionStateError(pub CollectionState);

impl fmt::Display for CollectionStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Collection handle is {}", self.0.phrase())
    }
}

/// Formats as the *quoted* display string — byte-for-byte what
/// `Box<dyn Error>::from(String)` produced before this type existed.
///
/// [`DBError`]'s own `Display` renders its `source` with `{:?}`, so a derived
/// `Debug` here would silently reword every collection lifecycle error. The
/// wording is no longer load-bearing inside this repo, but downstream crates
/// still read it, so it is kept stable deliberately.
impl fmt::Debug for CollectionStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.to_string(), f)
    }
}

impl std::error::Error for CollectionStateError {}

/// Anda DB related errors
#[derive(Error, Debug)]
pub enum DBError {
    /// General database-level failure.
    #[error("Anda DB {name:?} error: {source:?}")]
    Generic {
        /// Database or subsystem name associated with the failure.
        name: String,
        /// Original error returned by the lower-level operation.
        source: BoxError,
    },

    /// Collection-level failure.
    #[error("Collection {name:?} error: {source:?}")]
    Collection {
        /// Collection name associated with the failure.
        name: String,
        /// Original collection operation error.
        source: BoxError,
    },

    /// Schema validation or conversion failure.
    #[error("Schema error: {source:?}")]
    Schema {
        /// Schema, collection, or field name associated with the failure.
        name: String,
        /// Original schema error.
        source: BoxError,
    },

    /// Object-store or storage-wrapper failure.
    #[error("Storage error: {source:?}")]
    Storage {
        /// Storage namespace or object name associated with the failure.
        name: String,
        /// Original storage error.
        source: BoxError,
    },

    /// Index creation, update, lookup, or persistence failure.
    #[error("Index error: {source:?}")]
    Index {
        /// Index name associated with the failure.
        name: String,
        /// Original index error.
        source: BoxError,
    },

    /// Object or document was expected but not found.
    #[error("Object {name} at location {path} not found: {source:?}")]
    NotFound {
        /// Logical object name or index name.
        name: String,
        /// Object-store path or logical location.
        path: String,
        /// Original not-found error.
        source: BoxError,
        /// Document id when the error refers to a document; `0` otherwise.
        _id: u64,
    },

    /// Object, document, collection, or index already exists.
    #[error("Object {name} at location {path} already exists: {source:?}")]
    AlreadyExists {
        /// Logical object name or index name.
        name: String,
        /// Object-store path or logical location.
        path: String,
        /// Original duplicate-object error.
        source: BoxError,
        /// Document id when the error refers to a document; `0` otherwise.
        _id: u64,
    },

    /// Conditional storage update failed because the object version changed.
    #[error("Precondition failed at location {path}: {source:?}")]
    Precondition {
        /// Object-store path whose conditional update failed.
        path: String,
        /// Original precondition error.
        source: BoxError,
    },

    /// Serialization or deserialization failure.
    #[error("Serialization error: {source:?}")]
    Serialization {
        /// Logical object, schema, or index name being encoded or decoded.
        name: String,
        /// Original serialization error.
        source: BoxError,
    },

    /// Encoded payload exceeded the configured storage limit.
    #[error("Payload too large at location {path}: size {size} exceeds limit {limit}")]
    PayloadTooLarge {
        /// Object-store path that would receive the payload.
        path: String,
        /// Payload size in bytes.
        size: usize,
        /// Configured maximum payload size in bytes.
        limit: usize,
    },
}

impl DBError {
    /// Returns the collection lifecycle state that rejected the operation, if
    /// this error came from a handle that was closing, closed, being deleted,
    /// deleted, or poisoned.
    ///
    /// This is the supported way to classify a lifecycle rejection: the
    /// state travels as a typed [`CollectionStateError`] in the error's source
    /// chain, so callers never need to match on the message. Combine it with
    /// [`CollectionState::is_recoverable`] to decide between "reopen the
    /// collection and retry" and "give up".
    ///
    /// ```
    /// # use anda_db::error::{CollectionState, DBError};
    /// fn should_reopen(err: &DBError) -> bool {
    ///     err.collection_state()
    ///         .is_some_and(|state| state.is_poisoned())
    /// }
    /// ```
    pub fn collection_state(&self) -> Option<CollectionState> {
        let mut err: &(dyn std::error::Error + 'static) = self;
        loop {
            if let Some(state) = err.downcast_ref::<CollectionStateError>() {
                return Some(state.0);
            }
            err = err.source()?;
        }
    }

    /// Whether this error was produced by a handle poisoned by a cancelled
    /// mutation, i.e. one that a reopen can recover.
    ///
    /// Shorthand for `self.collection_state().is_some_and(|s| s.is_poisoned())`.
    pub fn is_poisoned(&self) -> bool {
        self.collection_state()
            .is_some_and(|state| state.is_poisoned())
    }
}

impl From<object_store::Error> for DBError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { path, source } => DBError::NotFound {
                name: "unknown".to_string(),
                path,
                source,
                _id: 0,
            },
            object_store::Error::AlreadyExists { path, source } => DBError::AlreadyExists {
                name: "unknown".to_string(),
                path,
                source,
                _id: 0,
            },
            object_store::Error::Precondition { path, source } => {
                DBError::Precondition { path, source }
            }
            err => DBError::Storage {
                name: "unknown".to_string(),
                source: err.into(),
            },
        }
    }
}

impl From<SchemaError> for DBError {
    fn from(err: SchemaError) -> Self {
        DBError::Schema {
            name: "unknown".to_string(),
            source: err.into(),
        }
    }
}

impl From<BTreeError> for DBError {
    fn from(err: BTreeError) -> Self {
        match &err {
            BTreeError::Generic { name, .. } => DBError::Index {
                name: name.clone(),
                source: err.into(),
            },
            BTreeError::Serialization { name, .. } => DBError::Index {
                name: name.clone(),
                source: err.into(),
            },
            BTreeError::NotFound { name, id, .. } => DBError::NotFound {
                name: name.clone(),
                path: "unknown".to_string(),
                _id: id.as_u64().unwrap_or(0),
                source: err.into(),
            },
            BTreeError::AlreadyExists { name, id, .. } => DBError::AlreadyExists {
                name: name.clone(),
                path: "unknown".to_string(),
                _id: id.as_u64().unwrap_or(0),
                source: err.into(),
            },
        }
    }
}

impl From<HnswError> for DBError {
    fn from(err: HnswError) -> Self {
        match &err {
            HnswError::Generic { name, .. } => DBError::Index {
                name: name.clone(),
                source: err.into(),
            },
            HnswError::Serialization { name, .. } => DBError::Index {
                name: name.clone(),
                source: err.into(),
            },
            HnswError::DimensionMismatch { name, .. } => DBError::Index {
                name: name.clone(),
                source: err.into(),
            },
            HnswError::NotFound { name, id, .. } => DBError::NotFound {
                name: name.clone(),
                path: "unknown".to_string(),
                _id: *id,
                source: err.into(),
            },
            HnswError::AlreadyExists { name, id, .. } => DBError::AlreadyExists {
                name: name.clone(),
                path: "unknown".to_string(),
                _id: *id,
                source: err.into(),
            },
        }
    }
}

impl From<BM25Error> for DBError {
    fn from(err: BM25Error) -> Self {
        match &err {
            BM25Error::Generic { name, .. } => DBError::Index {
                name: name.clone(),
                source: err.into(),
            },
            BM25Error::Serialization { name, .. } => DBError::Index {
                name: name.clone(),
                source: err.into(),
            },
            BM25Error::TokenizeFailed { name, .. } => DBError::Index {
                name: name.clone(),
                source: err.into(),
            },
            BM25Error::NotFound { name, id, .. } => DBError::NotFound {
                name: name.clone(),
                path: "unknown".to_string(),
                _id: *id,
                source: err.into(),
            },
            BM25Error::AlreadyExists { name, id, .. } => DBError::AlreadyExists {
                name: name.clone(),
                path: "unknown".to_string(),
                _id: *id,
                source: err.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db_btree::BTreeError;
    use anda_db_hnsw::HnswError;
    use anda_db_schema::SchemaError;
    use anda_db_tfs::BM25Error;
    use object_store::path::Path;
    use serde_json::json;

    fn assert_index(err: DBError, expected_name: &str) {
        match err {
            DBError::Index { name, source } => {
                assert_eq!(name, expected_name);
                assert!(!source.to_string().is_empty());
            }
            other => panic!("expected index error, got {other:?}"),
        }
    }

    fn assert_not_found(err: DBError, expected_name: &str, expected_id: u64) {
        match err {
            DBError::NotFound {
                name, path, _id, ..
            } => {
                assert_eq!(name, expected_name);
                assert_eq!(path, "unknown");
                assert_eq!(_id, expected_id);
            }
            other => panic!("expected not found error, got {other:?}"),
        }
    }

    fn assert_already_exists(err: DBError, expected_name: &str, expected_id: u64) {
        match err {
            DBError::AlreadyExists {
                name, path, _id, ..
            } => {
                assert_eq!(name, expected_name);
                assert_eq!(path, "unknown");
                assert_eq!(_id, expected_id);
            }
            other => panic!("expected already exists error, got {other:?}"),
        }
    }

    /// The lifecycle state must be recoverable from the error by *type*.
    /// Downstream crates used to probe the message for `"handle is poisoned"`;
    /// this is the replacement they migrate to.
    #[test]
    fn collection_state_is_recoverable_from_the_error_without_string_matching() {
        for state in [
            CollectionState::Closing,
            CollectionState::Closed,
            CollectionState::Deleting,
            CollectionState::Deleted,
            CollectionState::Poisoned,
        ] {
            let err = DBError::Generic {
                name: "coll".to_string(),
                source: CollectionStateError(state).into(),
            };
            assert_eq!(err.collection_state(), Some(state));
            assert_eq!(err.is_poisoned(), state == CollectionState::Poisoned);
        }

        // Only a poisoned handle is worth reopening for; a deletion is final.
        assert!(CollectionState::Poisoned.is_recoverable());
        assert!(CollectionState::Closed.is_recoverable());
        assert!(!CollectionState::Deleting.is_recoverable());
        assert!(!CollectionState::Deleted.is_recoverable());

        // Errors from other causes must not be misclassified.
        let other = DBError::Generic {
            name: "coll".to_string(),
            source: "Collection is read-only".into(),
        };
        assert_eq!(other.collection_state(), None);
        assert!(!other.is_poisoned());
        assert!(
            !DBError::Precondition {
                path: "p".to_string(),
                source: "stale".into(),
            }
            .is_poisoned()
        );
    }

    /// The rendered message is frozen: `anda_db_server` and
    /// `anda_cognitive_nexus` still read it while they migrate to
    /// [`DBError::collection_state`]. `DBError`'s `Display` renders its source
    /// with `{:?}`, so this pins [`CollectionStateError`]'s `Debug` too.
    #[test]
    fn lifecycle_error_message_is_unchanged() {
        for (state, phrase) in [
            (CollectionState::Closing, "closing"),
            (CollectionState::Closed, "closed"),
            (CollectionState::Deleting, "being deleted"),
            (CollectionState::Deleted, "deleted"),
            (
                CollectionState::Poisoned,
                "poisoned (a mutating call was cancelled mid-operation); \
                 reopen the collection to recover",
            ),
        ] {
            let typed = DBError::Generic {
                name: "coll".to_string(),
                source: CollectionStateError(state).into(),
            };
            // Byte-for-byte what the pre-typed `format!(...).into()` produced.
            let legacy = DBError::Generic {
                name: "coll".to_string(),
                source: format!("Collection handle is {phrase}").into(),
            };
            assert_eq!(typed.to_string(), legacy.to_string());
        }
        assert!(
            DBError::Generic {
                name: "coll".to_string(),
                source: CollectionStateError(CollectionState::Poisoned).into(),
            }
            .to_string()
            .contains("handle is poisoned")
        );
    }

    #[test]
    fn object_store_error_conversions_preserve_specific_variants() {
        let not_found = object_store::Error::NotFound {
            path: Path::from("missing").to_string(),
            source: "missing source".into(),
        };
        match DBError::from(not_found) {
            DBError::NotFound {
                name, path, _id, ..
            } => {
                assert_eq!(name, "unknown");
                assert_eq!(path, "missing");
                assert_eq!(_id, 0);
            }
            other => panic!("expected not found, got {other:?}"),
        }

        let already_exists = object_store::Error::AlreadyExists {
            path: Path::from("exists").to_string(),
            source: "exists source".into(),
        };
        match DBError::from(already_exists) {
            DBError::AlreadyExists {
                name, path, _id, ..
            } => {
                assert_eq!(name, "unknown");
                assert_eq!(path, "exists");
                assert_eq!(_id, 0);
            }
            other => panic!("expected already exists, got {other:?}"),
        }

        let precondition = object_store::Error::Precondition {
            path: Path::from("stale").to_string(),
            source: "stale source".into(),
        };
        match DBError::from(precondition) {
            DBError::Precondition { path, .. } => assert_eq!(path, "stale"),
            other => panic!("expected precondition, got {other:?}"),
        }

        let generic = object_store::Error::Generic {
            store: "memory",
            source: "generic source".into(),
        };
        match DBError::from(generic) {
            DBError::Storage { name, source } => {
                assert_eq!(name, "unknown");
                assert!(!source.to_string().is_empty());
            }
            other => panic!("expected storage error, got {other:?}"),
        }
    }

    #[test]
    fn schema_error_conversion_wraps_schema_variant() {
        let err = SchemaError::FieldName("bad".into());
        match DBError::from(err) {
            DBError::Schema { name, source } => {
                assert_eq!(name, "unknown");
                assert!(!source.to_string().is_empty());
            }
            other => panic!("expected schema error, got {other:?}"),
        }
    }

    #[test]
    fn btree_error_conversions_preserve_names_and_ids() {
        assert_index(
            DBError::from(BTreeError::Generic {
                name: "idx".into(),
                source: "generic".into(),
            }),
            "idx",
        );
        assert_index(
            DBError::from(BTreeError::Serialization {
                name: "idx".into(),
                source: "ser".into(),
            }),
            "idx",
        );
        assert_not_found(
            DBError::from(BTreeError::NotFound {
                name: "idx".into(),
                id: json!(42),
                value: json!("a"),
            }),
            "idx",
            42,
        );
        assert_already_exists(
            DBError::from(BTreeError::AlreadyExists {
                name: "idx".into(),
                id: json!(43),
                value: json!("b"),
            }),
            "idx",
            43,
        );
        assert_not_found(
            DBError::from(BTreeError::NotFound {
                name: "idx".into(),
                id: json!("not-u64"),
                value: json!("a"),
            }),
            "idx",
            0,
        );
    }

    #[test]
    fn hnsw_error_conversions_preserve_names_and_ids() {
        assert_index(
            DBError::from(HnswError::Generic {
                name: "vec".into(),
                source: "generic".into(),
            }),
            "vec",
        );
        assert_index(
            DBError::from(HnswError::Serialization {
                name: "vec".into(),
                source: "ser".into(),
            }),
            "vec",
        );
        assert_index(
            DBError::from(HnswError::DimensionMismatch {
                name: "vec".into(),
                expected: 3,
                got: 2,
            }),
            "vec",
        );
        assert_not_found(
            DBError::from(HnswError::NotFound {
                name: "vec".into(),
                id: 7,
            }),
            "vec",
            7,
        );
        assert_already_exists(
            DBError::from(HnswError::AlreadyExists {
                name: "vec".into(),
                id: 8,
            }),
            "vec",
            8,
        );
    }

    #[test]
    fn bm25_error_conversions_preserve_names_and_ids() {
        assert_index(
            DBError::from(BM25Error::Generic {
                name: "text".into(),
                source: "generic".into(),
            }),
            "text",
        );
        assert_index(
            DBError::from(BM25Error::Serialization {
                name: "text".into(),
                source: "ser".into(),
            }),
            "text",
        );
        assert_index(
            DBError::from(BM25Error::TokenizeFailed {
                name: "text".into(),
                id: 1,
                text: "".into(),
            }),
            "text",
        );
        assert_not_found(
            DBError::from(BM25Error::NotFound {
                name: "text".into(),
                id: 9,
            }),
            "text",
            9,
        );
        assert_already_exists(
            DBError::from(BM25Error::AlreadyExists {
                name: "text".into(),
                id: 10,
            }),
            "text",
            10,
        );
    }
}
