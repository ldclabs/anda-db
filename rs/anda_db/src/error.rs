//! Error types for schema module
use anda_db_btree::BTreeError;
use anda_db_hnsw::HnswError;
use anda_db_tfs::BM25Error;
use thiserror::Error;

use crate::schema::{BoxError, SchemaError};

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
