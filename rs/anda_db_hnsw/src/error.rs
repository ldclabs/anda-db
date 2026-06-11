use thiserror::Error;

/// Boxed error type used to preserve lower-level storage and serialization
/// failures behind a sendable, thread-safe boundary.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors that can occur when working with HNSW index.
#[derive(Error, Debug)]
pub enum HnswError {
    /// Index-related errors.
    #[error("HNSW index {name:?}, error: {source:?}")]
    Generic {
        /// Name of the HNSW index that raised the error.
        name: String,
        /// Original error returned by the underlying operation.
        source: BoxError,
    },

    /// CBOR serialization/deserialization errors.
    #[error("HNSW index {name:?}, CBOR serialization error: {source:?}")]
    Serialization {
        /// Name of the HNSW index whose serialized state failed to encode or decode.
        name: String,
        /// Original serialization or deserialization error.
        source: BoxError,
    },

    /// Error when vector dimensions don't match the index dimension.
    #[error("HNSW index {name:?}, vector dimension mismatch, expected {expected}, got {got}")]
    DimensionMismatch {
        /// Name of the HNSW index that rejected the vector.
        name: String,
        /// Vector dimension configured for the index.
        expected: usize,
        /// Vector dimension supplied by the caller.
        got: usize,
    },

    /// Error when a token is not found.
    #[error("HNSW index {name:?}, node not found: {id:?}")]
    NotFound {
        /// Name of the HNSW index that was searched.
        name: String,
        /// Node/document id that was not present in the index.
        id: u64,
    },

    /// Error when trying to add a document with an ID that already exists
    #[error("HNSW index {name:?}, node {id} already exists")]
    AlreadyExists {
        /// Name of the HNSW index receiving the duplicate node.
        name: String,
        /// Node/document id that already exists in the index.
        id: u64,
    },
}
