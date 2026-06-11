use serde_json::Value;
use thiserror::Error;

/// Boxed error type used to preserve lower-level storage and serialization
/// failures behind a sendable, thread-safe boundary.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors that can occur when working with B-tree index.
#[derive(Error, Debug)]
pub enum BTreeError {
    /// Index-related errors.
    #[error("BTree index {name:?}, error: {source:?}")]
    Generic {
        /// Name of the B-tree index that raised the error.
        name: String,
        /// Original error returned by the underlying operation.
        source: BoxError,
    },

    /// CBOR serialization/deserialization errors
    #[error("BTree index {name:?}, CBOR serialization error: {source:?}")]
    Serialization {
        /// Name of the B-tree index whose serialized state failed to encode or decode.
        name: String,
        /// Original serialization or deserialization error.
        source: BoxError,
    },

    /// Error when a token is not found.
    #[error("BTree index {name:?}, value {value:?} not found in document {id}")]
    NotFound {
        /// Name of the B-tree index that was searched.
        name: String,
        /// Document identifier involved in the lookup.
        id: Value,
        /// Indexed value that could not be found for the document.
        value: Value,
    },

    /// Error when trying to add a document with an ID that already exists
    #[error("BTree index {name:?}, value {value} already exists in document {id}")]
    AlreadyExists {
        /// Name of the B-tree index receiving the duplicate value.
        name: String,
        /// Document identifier associated with the duplicate entry.
        id: Value,
        /// Indexed value that already exists.
        value: Value,
    },
}
