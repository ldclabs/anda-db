//! API error type with HTTP status codes and machine-readable error codes.
//!
//! All handler failures funnel into [`ApiError`] and serialize as an RPC
//! error envelope: `{"error": {"code": "...", "message": "..."}}`.
//! Client-safe failures are constructed explicitly at the HTTP boundary;
//! raw engine [`DBError`] values are logged and sanitized by default.

use anda_db::{
    error::{CollectionState, DBError},
    schema::SchemaError,
};
use axum::http::StatusCode;
use serde::Serialize;

/// Errors that the HTTP boundary has positively identified as safe client
/// failures. Engine errors must not be converted into this enum solely from
/// their outer [`DBError`] variant: the same variant can wrap object-store,
/// serialization, or index-internal details.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// A syntactically valid request contains invalid parameters or values.
    InvalidInput(String),
    /// A query is incompatible with the selected collection or its indexes.
    InvalidQuery(String),
    /// The requested change conflicts with current logical state.
    Conflict(String),
}

/// A structured API error carrying an HTTP status and a stable error code.
#[derive(Debug)]
pub struct ApiError {
    /// HTTP status code for the response.
    pub status: StatusCode,
    /// Stable machine-readable error code.
    pub code: &'static str,
    /// Human-readable error message.
    pub message: String,
}

/// Wire format of an error: `{"error": {...}}`.
#[derive(Serialize)]
pub(crate) struct ErrorEnvelope<'a> {
    pub error: ErrorBody<'a>,
}

/// The `error` object inside [`ErrorEnvelope`].
#[derive(Serialize)]
pub(crate) struct ErrorBody<'a> {
    pub code: &'a str,
    pub message: &'a str,
}

impl ApiError {
    /// Creates an error with an explicit status and code.
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    /// `400 Bad Request` — malformed request body or invalid parameters.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    /// `400 Bad Request` — request parameters were explicitly validated at
    /// the HTTP boundary and found invalid.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        ClientError::InvalidInput(message.into()).into()
    }

    /// `400 Bad Request` — a query was explicitly validated against the
    /// selected collection and found invalid.
    pub fn invalid_query(message: impl Into<String>) -> Self {
        ClientError::InvalidQuery(message.into()).into()
    }

    /// `400 Bad Request` — the RPC method does not exist in this scope.
    pub fn method_not_found(method: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "method_not_found",
            format!("method not found: {method}"),
        )
    }

    /// `401 Unauthorized` — missing or invalid API key.
    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid or missing API key",
        )
    }

    /// `404 Not Found` — database, collection, or document does not exist.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    /// `409 Conflict` — the resource already exists.
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "already_exists", message)
    }

    /// `409 Conflict` — the HTTP boundary proved that the requested state
    /// transition conflicts with current logical state.
    pub fn conflict(message: impl Into<String>) -> Self {
        ClientError::Conflict(message.into()).into()
    }

    /// `408 Request Timeout` — the request exceeded the configured
    /// processing deadline.
    pub fn timeout() -> Self {
        Self::new(
            StatusCode::REQUEST_TIMEOUT,
            "timeout",
            "request processing exceeded the configured timeout",
        )
    }

    /// `503 Service Unavailable` — the server is shutting down and no longer
    /// accepts new requests.
    pub fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "server is shutting down",
        )
    }

    /// `410 Gone` — the collection was (or is being) deleted; its objects are
    /// removed, so no retry brings it back.
    pub fn gone(message: impl Into<String>) -> Self {
        Self::new(StatusCode::GONE, "gone", message)
    }

    /// `500 Internal Server Error` — storage or index failure.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    /// Classifies a collection handle's lifecycle state, or `None` when it is
    /// [`CollectionState::Active`].
    ///
    /// Every non-active state rejects operations with the same engine error,
    /// which used to be flattened into an opaque 500 — the client could not
    /// tell "retry, this recovers itself" from "this is gone". The split is
    /// taken from [`CollectionState::is_recoverable`] rather than enumerated
    /// here, so a state added later lands on the engine's own answer.
    pub fn from_collection_state(state: CollectionState) -> Option<Self> {
        match state {
            CollectionState::Active => None,
            // Poisoned / Closing / Closed: the storage is intact and
            // reopening the collection yields a usable handle, which the next
            // request does on its own.
            state if state.is_recoverable() => Some(Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "collection_unavailable",
                if state.is_poisoned() {
                    "collection was invalidated by a cancelled operation and is \
                     being recovered; retry the request"
                } else {
                    "collection is closing and will be reopened on demand; \
                     retry the request"
                },
            )),
            _ => Some(Self::gone(
                "collection has been deleted; the request cannot be retried",
            )),
        }
    }

    pub(crate) fn envelope(&self) -> ErrorEnvelope<'_> {
        ErrorEnvelope {
            error: ErrorBody {
                code: self.code,
                message: &self.message,
            },
        }
    }
}

impl From<ClientError> for ApiError {
    fn from(err: ClientError) -> Self {
        match err {
            ClientError::InvalidInput(message) => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_input", message)
            }
            ClientError::InvalidQuery(message) => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_query", message)
            }
            ClientError::Conflict(message) => Self::new(StatusCode::CONFLICT, "conflict", message),
        }
    }
}

impl From<DBError> for ApiError {
    fn from(err: DBError) -> Self {
        // `DBError` is an engine boundary, not a client-error taxonomy. Even
        // apparently semantic variants can originate in object_store and
        // contain physical paths or nested source errors. Always retain the
        // full value in server logs and expose only a generic response unless
        // the caller established a client-safe context before entering the
        // engine.
        log::error!(
            action = "ApiError::from";
            "database engine error: {err:?}",
        );
        // A collection handle rejected the operation because of its lifecycle
        // state. The engine reports that state as a typed source (never a
        // message the server has to parse), so it can be answered honestly
        // instead of being folded into the opaque internal fallback.
        if let Some(state) = err.collection_state()
            && let Some(api) = Self::from_collection_state(state)
        {
            return api;
        }
        match err {
            DBError::Generic { .. }
            | DBError::Collection { .. }
            | DBError::Schema { .. }
            | DBError::Storage { .. }
            | DBError::Index { .. }
            | DBError::NotFound { .. }
            | DBError::AlreadyExists { .. }
            | DBError::Precondition { .. }
            | DBError::Serialization { .. }
            | DBError::PayloadTooLarge { .. } => Self::internal("internal server error"),
        }
    }
}

impl From<SchemaError> for ApiError {
    fn from(err: SchemaError) -> Self {
        // A SchemaError can be raised while decoding persisted documents as
        // well as while handling input. Request-validation sites must map
        // their own errors through `invalid_input`; this fallback is internal.
        log::error!(
            action = "ApiError::from";
            "schema engine error: {err:?}",
        );
        Self::internal("internal server error")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db::error::CollectionStateError;
    use std::io;

    const SECRET: &str = "/srv/private/tenant-a/db_meta.cbor";

    fn source() -> anda_db::schema::BoxError {
        io::Error::other(SECRET).into()
    }

    fn assert_sanitized(err: DBError, status: StatusCode, code: &str, message: &str) {
        let api = ApiError::from(err);
        assert_eq!(api.status, status);
        assert_eq!(api.code, code);
        assert_eq!(api.message, message);
        assert!(!api.message.contains(SECRET));
    }

    #[test]
    fn untrusted_engine_variants_are_internal_and_sanitized() {
        let errors = [
            DBError::Generic {
                name: "db".to_string(),
                source: source(),
            },
            DBError::Collection {
                name: "collection".to_string(),
                source: source(),
            },
            DBError::Schema {
                name: "schema".to_string(),
                source: source(),
            },
            DBError::Storage {
                name: "storage".to_string(),
                source: source(),
            },
            DBError::Index {
                name: "index".to_string(),
                source: source(),
            },
            DBError::NotFound {
                name: "object".to_string(),
                path: SECRET.to_string(),
                source: source(),
                _id: 0,
            },
            DBError::Serialization {
                name: "document".to_string(),
                source: source(),
            },
        ];

        for err in errors {
            assert_sanitized(
                err,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "internal server error",
            );
        }
    }

    #[test]
    fn unproven_engine_conflicts_and_size_errors_are_internal() {
        for err in [
            DBError::AlreadyExists {
                name: "object".to_string(),
                path: SECRET.to_string(),
                source: source(),
                _id: 0,
            },
            DBError::Precondition {
                path: SECRET.to_string(),
                source: source(),
            },
        ] {
            assert_sanitized(
                err,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "internal server error",
            );
        }

        assert_sanitized(
            DBError::PayloadTooLarge {
                path: SECRET.to_string(),
                size: 2,
                limit: 1,
            },
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal server error",
        );
    }

    #[test]
    fn explicit_client_boundaries_preserve_only_proven_safe_messages() {
        let input = ApiError::invalid_input("field score must be an integer");
        assert_eq!(input.status, StatusCode::BAD_REQUEST);
        assert_eq!(input.code, "invalid_input");

        let query = ApiError::invalid_query("missing score index");
        assert_eq!(query.status, StatusCode::BAD_REQUEST);
        assert_eq!(query.code, "invalid_query");

        let conflict = ApiError::conflict("document changed concurrently");
        assert_eq!(conflict.status, StatusCode::CONFLICT);
        assert_eq!(conflict.code, "conflict");
    }

    /// Every non-active handle state used to collapse into one opaque 500, so
    /// a client could not tell "retry, this recovers itself" from "this is
    /// gone". Both must be distinguishable, and neither may leak a path.
    #[test]
    fn collection_lifecycle_states_are_classified_not_flattened_to_500() {
        for state in [
            CollectionState::Poisoned,
            CollectionState::Closing,
            CollectionState::Closed,
        ] {
            let api = ApiError::from(DBError::Generic {
                name: "articles".to_string(),
                source: CollectionStateError(state).into(),
            });
            assert_eq!(
                api.status,
                StatusCode::SERVICE_UNAVAILABLE,
                "state: {state:?}"
            );
            assert_eq!(api.code, "collection_unavailable", "state: {state:?}");
            assert!(api.message.contains("retry"), "state: {state:?}");
            assert!(!api.message.contains(SECRET));
        }
        // The poisoned message names its cause so an operator reading a log
        // or a client reading the body can tell the two apart.
        let poisoned = ApiError::from(DBError::Generic {
            name: "articles".to_string(),
            source: CollectionStateError(CollectionState::Poisoned).into(),
        });
        assert!(poisoned.message.contains("cancelled"));

        // A deleted collection is not coming back: retrying is pointless.
        for state in [CollectionState::Deleting, CollectionState::Deleted] {
            let api = ApiError::from(DBError::Generic {
                name: "articles".to_string(),
                source: CollectionStateError(state).into(),
            });
            assert_eq!(api.status, StatusCode::GONE, "state: {state:?}");
            assert_eq!(api.code, "gone", "state: {state:?}");
            assert!(!api.message.contains(SECRET));
        }

        // Other generic engine failures keep the conservative mapping.
        assert_sanitized(
            DBError::Generic {
                name: "db".to_string(),
                source: source(),
            },
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal server error",
        );
    }

    #[test]
    fn implicit_schema_conversion_is_internal() {
        let api = ApiError::from(SchemaError::Serialization(SECRET.to_string()));
        assert_eq!(api.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(api.code, "internal");
        assert_eq!(api.message, "internal server error");
    }
}
