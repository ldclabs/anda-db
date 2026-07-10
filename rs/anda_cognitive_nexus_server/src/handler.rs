use anda_kip::{Request, Response};
use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    middleware::Next,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

use crate::nexus::{ListLogParams, ListLogsError, Nexus};

#[derive(Clone)]
pub struct AppState {
    pub name: String,
    pub version: String,
    pub nexus: Nexus,
    pub api_key: Option<String>,
    /// Per-request processing deadline for `/kip`.
    pub request_timeout: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JsonRpcRequest {
    /// The method name to call.
    pub method: String,

    pub params: Value,
}

pub async fn get_information(State(app): State<AppState>) -> impl IntoResponse {
    let info = json!({
        "name": app.name,
        "version": app.version,
    });

    Json(info)
}

/// POST /kip
pub async fn post_kip(
    State(app): State<AppState>,
    header: header::HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> Result<Json<Response>, (StatusCode, Json<Response>)> {
    if !authorize_api_key(app.api_key.as_deref(), &header) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(Response::err("invalid API key".to_string())),
        ));
    }

    match req.method.as_str() {
        "execute_kip" => {
            let params: Request = serde_json::from_value(req.params).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(Response::err(format!("invalid parameters: {}", e))),
                )
            })?;

            let response = tokio::time::timeout(app.request_timeout, app.nexus.execute_kip(params))
                .await
                .map_err(|_| {
                    (
                        StatusCode::REQUEST_TIMEOUT,
                        Json(Response::err(
                            "request processing exceeded the configured timeout".to_string(),
                        )),
                    )
                })?;
            Ok(Json(response))
        }
        "list_logs" => {
            let params: ListLogParams = serde_json::from_value(req.params).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(Response::err(format!("invalid parameters: {}", e))),
                )
            })?;

            let (logs, next_cursor) =
                tokio::time::timeout(app.request_timeout, app.nexus.list_logs(params))
                    .await
                    .map_err(|_| {
                        (
                            StatusCode::REQUEST_TIMEOUT,
                            Json(Response::err(
                                "request processing exceeded the configured timeout".to_string(),
                            )),
                        )
                    })?
                    .map_err(|err| match err {
                        // Client input error: an undecodable cursor.
                        ListLogsError::InvalidCursor(e) => (
                            StatusCode::BAD_REQUEST,
                            Json(Response::err(format!("invalid cursor: {}", e))),
                        ),
                        // Internal failure: log the details, return a generic
                        // message to the client.
                        ListLogsError::Internal(e) => {
                            log::error!(
                                action = "post_kip";
                                "failed to list logs: {e:?}",
                            );
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(Response::err("failed to list logs".to_string())),
                            )
                        }
                    })?;

            Ok(Json(anda_kip::Response::Ok {
                result: json!(logs),
                next_cursor,
            }))
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(Response::err(format!("unknown method: {}", req.method))),
        )),
    }
}

/// Rewrites extractor-level rejections (e.g. the body-limit 413, which axum
/// emits as plain text) into the JSON-RPC error format.
pub async fn normalize_rejections(
    req: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    let resp = next.run(req).await;
    if resp.status() == StatusCode::PAYLOAD_TOO_LARGE
        && resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_none_or(|ct| !ct.starts_with("application/json"))
    {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(Response::err(
                "request body exceeds the configured size limit".to_string(),
            )),
        )
            .into_response();
    }
    resp
}

fn authorize_api_key(expected: Option<&str>, header: &header::HeaderMap) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    if expected.trim().is_empty() {
        return false;
    }

    header
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|provided| constant_time_eq(provided.as_bytes(), expected.as_bytes()))
}

/// Constant-time byte comparison to avoid a timing side channel on the API
/// key. Only the length may leak, which is not considered secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn api_key_auth_rejects_empty_expected_key_and_missing_header() {
        let headers = header::HeaderMap::new();
        assert!(!authorize_api_key(Some(""), &headers));
        assert!(!authorize_api_key(Some("secret"), &headers));
        assert!(authorize_api_key(None, &headers));
    }

    #[test]
    fn api_key_auth_requires_bearer_token() {
        let mut headers = header::HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("secret"));
        assert!(!authorize_api_key(Some("secret"), &headers));

        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        assert!(authorize_api_key(Some("secret"), &headers));
    }
}
