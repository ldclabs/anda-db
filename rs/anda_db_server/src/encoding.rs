//! CBOR-first payload encoding with JSON fallback.
//!
//! The wire protocol is a lightweight RPC over HTTP POST:
//!
//! - Request: `{"method": "...", "params": {...}}`
//! - Success response: `{"result": ...}`
//! - Error response: `{"error": {"code": "...", "message": "..."}}`
//!
//! Format negotiation:
//!
//! - The request body format follows `Content-Type` (`application/cbor` is
//!   the default when the header is absent).
//! - The response format follows `Accept` when present, otherwise mirrors
//!   the request `Content-Type`, otherwise defaults to CBOR.

use anda_db::schema::{Cbor, Json};
use axum::{
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::error::ApiError;

/// MIME type for CBOR payloads.
pub const APPLICATION_CBOR: &str = "application/cbor";
/// MIME type for JSON payloads.
pub const APPLICATION_JSON: &str = "application/json";

/// Wire format of a request or response payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// CBOR payload (`application/cbor`), the primary format.
    Cbor,
    /// JSON payload (`application/json`), supported for debugging and
    /// non-CBOR clients.
    Json,
}

impl Encoding {
    /// Detects the encoding from the `Accept` header, if present.
    pub fn from_accept(headers: &HeaderMap) -> Option<Self> {
        Self::detect(headers.get(header::ACCEPT)?)
    }

    /// Detects the encoding from the `Content-Type` header, if present.
    pub fn from_content_type(headers: &HeaderMap) -> Option<Self> {
        Self::detect(headers.get(header::CONTENT_TYPE)?)
    }

    /// Negotiates the response encoding: `Accept` wins, then `Content-Type`,
    /// then the given default.
    pub fn negotiate_or(headers: &HeaderMap, default: Self) -> Self {
        Self::from_accept(headers)
            .or_else(|| Self::from_content_type(headers))
            .unwrap_or(default)
    }

    /// Negotiates the response encoding with CBOR as the default.
    pub fn negotiate(headers: &HeaderMap) -> Self {
        Self::negotiate_or(headers, Encoding::Cbor)
    }

    fn detect(value: &HeaderValue) -> Option<Self> {
        let value = value.to_str().ok()?;
        if value.contains(APPLICATION_CBOR) {
            Some(Encoding::Cbor)
        } else if value.contains(APPLICATION_JSON) {
            Some(Encoding::Json)
        } else {
            None
        }
    }

    /// Returns the `Content-Type` header value for this encoding.
    pub fn content_type(&self) -> HeaderValue {
        match self {
            Encoding::Cbor => HeaderValue::from_static(APPLICATION_CBOR),
            Encoding::Json => HeaderValue::from_static(APPLICATION_JSON),
        }
    }

    /// Serializes `value` into bytes in this encoding.
    pub fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, ApiError> {
        match self {
            Encoding::Cbor => {
                let mut buf = Vec::new();
                cbor2::ser::to_writer(value, &mut buf)
                    .map_err(|e| ApiError::internal(format!("failed to encode CBOR: {e}")))?;
                Ok(buf)
            }
            Encoding::Json => serde_json::to_vec(value)
                .map_err(|e| ApiError::internal(format!("failed to encode JSON: {e}"))),
        }
    }

    /// Builds a success response `{"result": ...}` in this encoding.
    pub fn reply<T: Serialize>(&self, result: &T) -> Response {
        #[derive(Serialize)]
        struct Envelope<'a, T> {
            result: &'a T,
        }

        match self.encode(&Envelope { result }) {
            Ok(buf) => ([(header::CONTENT_TYPE, self.content_type())], buf).into_response(),
            Err(err) => err.respond(*self),
        }
    }
}

impl ApiError {
    /// Converts this error into an HTTP response in the given encoding.
    pub fn respond(&self, enc: Encoding) -> Response {
        match enc.encode(&self.envelope()) {
            Ok(buf) => (
                self.status,
                [(header::CONTENT_TYPE, enc.content_type())],
                buf,
            )
                .into_response(),
            // Encoding an error envelope only fails if the serializer itself
            // is broken; fall back to plain text rather than recursing.
            Err(_) => (self.status, self.message.clone()).into_response(),
        }
    }
}

/// A parsed RPC request with format-preserving, lazily-decoded params.
#[derive(Debug)]
pub struct RpcRequest {
    /// Method name, e.g. `"collection.create"`.
    pub method: String,
    /// Raw params in the request's wire format.
    pub params: RpcParams,
}

/// RPC params kept in their wire format until a handler decodes them
/// into a typed struct. This avoids lossy intermediate conversions
/// (CBOR byte strings, for example, have no JSON equivalent).
#[derive(Debug)]
pub enum RpcParams {
    /// Params from a CBOR request body.
    Cbor(Cbor),
    /// Params from a JSON request body.
    Json(Json),
}

impl RpcRequest {
    /// Parses the request body according to its `Content-Type`
    /// (CBOR when the header is absent).
    pub fn parse(headers: &HeaderMap, body: &[u8]) -> Result<Self, ApiError> {
        match Encoding::from_content_type(headers).unwrap_or(Encoding::Cbor) {
            Encoding::Cbor => {
                #[derive(Deserialize)]
                struct Req {
                    method: String,
                    #[serde(default)]
                    params: Option<Cbor>,
                }

                let req: Req = cbor2::de::from_reader(body).map_err(|e| {
                    ApiError::bad_request(format!("failed to parse CBOR request: {e}"))
                })?;
                Ok(Self {
                    method: req.method,
                    params: RpcParams::Cbor(req.params.unwrap_or(Cbor::Null)),
                })
            }
            Encoding::Json => {
                #[derive(Deserialize)]
                struct Req {
                    method: String,
                    #[serde(default)]
                    params: Option<Json>,
                }

                let req: Req = serde_json::from_slice(body).map_err(|e| {
                    ApiError::bad_request(format!("failed to parse JSON request: {e}"))
                })?;
                Ok(Self {
                    method: req.method,
                    params: RpcParams::Json(req.params.unwrap_or(Json::Null)),
                })
            }
        }
    }
}

impl RpcParams {
    /// Decodes the params into a typed value without cross-format conversion.
    pub fn decode<T: DeserializeOwned>(self) -> Result<T, ApiError> {
        match self {
            RpcParams::Cbor(value) => value
                .deserialized()
                .map_err(|e| ApiError::bad_request(format!("invalid params: {e}"))),
            RpcParams::Json(value) => serde_json::from_value(value)
                .map_err(|e| ApiError::bad_request(format!("invalid params: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(ct: Option<&str>, accept: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(ct) = ct {
            h.insert(header::CONTENT_TYPE, ct.parse().unwrap());
        }
        if let Some(accept) = accept {
            h.insert(header::ACCEPT, accept.parse().unwrap());
        }
        h
    }

    #[test]
    fn negotiate_prefers_accept_then_content_type_then_cbor() {
        let h = headers(Some(APPLICATION_CBOR), Some(APPLICATION_JSON));
        assert_eq!(Encoding::negotiate(&h), Encoding::Json);

        let h = headers(Some(APPLICATION_JSON), None);
        assert_eq!(Encoding::negotiate(&h), Encoding::Json);

        let h = headers(None, Some("*/*"));
        assert_eq!(Encoding::negotiate(&h), Encoding::Cbor);

        let h = headers(None, None);
        assert_eq!(Encoding::negotiate(&h), Encoding::Cbor);
    }

    #[test]
    fn parse_request_defaults_to_cbor() {
        let mut body = Vec::new();
        cbor2::ser::to_writer(
            &serde_json::json!({"method": "info", "params": {"a": 1}}),
            &mut body,
        )
        .unwrap();

        let req = RpcRequest::parse(&HeaderMap::new(), &body).unwrap();
        assert_eq!(req.method, "info");
        #[derive(Deserialize)]
        struct P {
            a: u8,
        }
        let p: P = req.params.decode().unwrap();
        assert_eq!(p.a, 1);
    }

    #[test]
    fn parse_request_json_with_missing_params() {
        let h = headers(Some(APPLICATION_JSON), None);
        let req = RpcRequest::parse(&h, br#"{"method": "info"}"#).unwrap();
        assert_eq!(req.method, "info");
        let p: Option<u8> = req.params.decode().unwrap();
        assert!(p.is_none());
    }
}
