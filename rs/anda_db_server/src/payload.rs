//! RPC payload types with JSON/CBOR dual format support.
//!
//! This module provides lightweight RPC request/response types and
//! format negotiation based on HTTP headers:
//! - `Content-Type: application/cbor` for CBOR request bodies
//! - `Content-Type: application/json` (default) for JSON request bodies
//! - `Accept: application/cbor` for CBOR responses
//! - `Accept: application/json` (default) for JSON responses

use axum::{
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use ciborium::Value as CborValue;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Content format for request/response payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Json,
    Cbor,
}

impl ContentType {
    /// Detect content type from Content-Type header, falling back to Accept header.
    pub fn from_header(headers: &HeaderMap) -> Self {
        headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|ct| {
                if ct.contains("application/cbor") {
                    ContentType::Cbor
                } else {
                    ContentType::Json
                }
            })
            .unwrap_or_else(|| Self::from_accept(headers))
    }

    /// Detect preferred response format from Accept header.
    pub fn from_accept(headers: &HeaderMap) -> Self {
        headers
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|accept| {
                if accept.contains("application/cbor") {
                    ContentType::Cbor
                } else {
                    ContentType::Json
                }
            })
            .unwrap_or(ContentType::Json)
    }

    /// Get the corresponding HTTP Content-Type header value.
    pub fn header_value(&self) -> HeaderValue {
        match self {
            ContentType::Json => HeaderValue::from_static("application/json"),
            ContentType::Cbor => HeaderValue::from_static("application/cbor"),
        }
    }
}

/// Extracts the preferred response format from the `Accept` header.
///
/// Defaults to JSON if no Accept header is present or if the
/// Accept header does not contain `application/cbor`.
pub struct Accept(pub ContentType);

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for Accept {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(Accept(ContentType::from_header(&parts.headers)))
    }
}

// ─── RPC Types ────────────────────────────────────────────────────────────────

/// Common RPC error codes (JSON-RPC code space).
pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;

/// Application-specific RPC error codes (-32000 to -32099).
pub const NOT_FOUND: i32 = -32001;
pub const ALREADY_EXISTS: i32 = -32002;
pub const PAYLOAD_TOO_LARGE: i32 = -32003;

/// RPC request object.
#[derive(Debug, Deserialize)]
pub struct RpcRequest<T> {
    pub method: String,
    pub params: Option<T>,
}

/// RPC response object.
#[derive(Debug, Serialize)]
pub struct RpcResponse<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

pub type JsonRpcRequest = RpcRequest<JsonValue>;
pub type CborRpcRequest = RpcRequest<CborValue>;
pub type JsonRpcResponse = RpcResponse<JsonValue>;
pub type CborRpcResponse = RpcResponse<CborValue>;

impl<T> RpcResponse<T> {
    /// Create a successful RPC response.
    pub fn success(result: T) -> Self {
        Self {
            result: Some(result),
            error: None,
        }
    }

    /// Create an error RPC response.
    pub fn error(error: RpcError) -> Self {
        Self {
            result: None,
            error: Some(error),
        }
    }
}

/// RPC error object.
#[derive(Debug, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
}

impl RpcError {
    /// Create a new RPC error with the given code and message.
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

impl From<anda_db::error::DBError> for RpcError {
    fn from(e: anda_db::error::DBError) -> Self {
        let code = match &e {
            anda_db::error::DBError::NotFound { .. } => NOT_FOUND,
            anda_db::error::DBError::AlreadyExists { .. } => ALREADY_EXISTS,
            anda_db::error::DBError::PayloadTooLarge { .. } => PAYLOAD_TOO_LARGE,
            anda_db::error::DBError::Serialization { .. }
            | anda_db::error::DBError::Schema { .. } => INVALID_PARAMS,
            _ => INTERNAL_ERROR,
        };
        RpcError::new(code, e.to_string())
    }
}

impl From<anda_db_schema::SchemaError> for RpcError {
    fn from(e: anda_db_schema::SchemaError) -> Self {
        RpcError::new(INVALID_PARAMS, e.to_string())
    }
}

// ─── Response Encoding ────────────────────────────────────────────────────────

/// A response type that supports both JSON and CBOR serialization.
///
/// The format is determined by the `content_type` field, which should
/// be set from the `Accept` header via the [`Accept`] extractor.
pub struct AppResponse<T: Serialize> {
    pub data: T,
    pub content_type: ContentType,
}

impl<T: Serialize> AppResponse<T> {
    pub fn new(data: T, ct: ContentType) -> Self {
        Self {
            data,
            content_type: ct,
        }
    }
}

impl<T: Serialize> IntoResponse for AppResponse<T> {
    fn into_response(self) -> Response {
        match self.content_type {
            ContentType::Json => match serde_json::to_vec(&self.data) {
                Ok(bytes) => (
                    [(header::CONTENT_TYPE, self.content_type.header_value())],
                    bytes,
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("JSON serialization error: {e}"),
                )
                    .into_response(),
            },
            ContentType::Cbor => {
                let mut buf = Vec::new();
                match ciborium::ser::into_writer(&self.data, &mut buf) {
                    Ok(()) => (
                        [(header::CONTENT_TYPE, self.content_type.header_value())],
                        buf,
                    )
                        .into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("CBOR serialization error: {e}"),
                    )
                        .into_response(),
                }
            }
        }
    }
}
