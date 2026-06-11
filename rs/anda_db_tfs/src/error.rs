use thiserror::Error;

/// Boxed error type used to preserve lower-level storage, tokenization, and
/// serialization failures behind a sendable, thread-safe boundary.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors that can occur when working with BM25 index.
#[derive(Error, Debug)]
pub enum BM25Error {
    /// Index-related errors.
    #[error("BM25 index {name:?}, error: {source:?}")]
    Generic {
        /// Name of the BM25 index that raised the error.
        name: String,
        /// Original error returned by the underlying operation.
        source: BoxError,
    },

    /// CBOR serialization/deserialization errors
    #[error("BM25 index {name:?}, CBOR serialization error: {source:?}")]
    Serialization {
        /// Name of the BM25 index whose serialized state failed to encode or decode.
        name: String,
        /// Original serialization or deserialization error.
        source: BoxError,
    },

    /// Error when a token is not found.
    #[error("BM25 index {name:?}, document {id} not found")]
    NotFound {
        /// Name of the BM25 index that was searched.
        name: String,
        /// Document id that was not present in the index.
        id: u64,
    },

    /// Error when trying to add a document with an ID that already exists
    #[error("BM25 index {name:?}, document {id} already exists")]
    AlreadyExists {
        /// Name of the BM25 index receiving the duplicate document.
        name: String,
        /// Document id that already exists in the index.
        id: u64,
    },

    /// Error when tokenization produces no tokens for a document
    #[error("BM25 index {name:?}, document {id} tokenization failed: {text:?}")]
    TokenizeFailed {
        /// Name of the BM25 index processing the document.
        name: String,
        /// Document id whose text could not be tokenized into searchable terms.
        id: u64,
        /// Source text that produced no tokens.
        text: String,
    },
}
