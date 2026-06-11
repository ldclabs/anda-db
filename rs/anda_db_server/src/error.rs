//! API error type with HTTP status codes and machine-readable error codes.
//!
//! All handler failures funnel into [`ApiError`], which maps the engine's
//! [`DBError`] variants to meaningful HTTP statuses and serializes as an
//! RPC error envelope: `{"error": {"code": "...", "message": "..."}}`.

use anda_db::{error::DBError, schema::SchemaError};
use axum::http::StatusCode;
use serde::Serialize;

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

    /// `500 Internal Server Error` — storage or index failure.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
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

impl From<DBError> for ApiError {
    fn from(err: DBError) -> Self {
        let message = err.to_string();
        match err {
            DBError::NotFound { .. } => Self::new(StatusCode::NOT_FOUND, "not_found", message),
            DBError::AlreadyExists { .. } => {
                Self::new(StatusCode::CONFLICT, "already_exists", message)
            }
            DBError::Precondition { .. } => Self::new(
                StatusCode::PRECONDITION_FAILED,
                "precondition_failed",
                message,
            ),
            DBError::PayloadTooLarge { .. } => {
                Self::new(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large", message)
            }
            // Schema validation and serialization failures are caused by the
            // request payload; `Generic` is used by the engine for usage errors
            // such as writing to a read-only database or empty updates.
            DBError::Schema { .. } | DBError::Serialization { .. } | DBError::Generic { .. } => {
                Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
            }
            DBError::Collection { .. } | DBError::Index { .. } | DBError::Storage { .. } => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
            }
        }
    }
}

impl From<SchemaError> for ApiError {
    fn from(err: SchemaError) -> Self {
        Self::bad_request(err.to_string())
    }
}
