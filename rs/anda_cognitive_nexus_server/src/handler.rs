use anda_kip::{Request, Response};
use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::nexus::{ListLogParams, Nexus};

#[derive(Clone)]
pub struct AppState {
    pub name: String,
    pub version: String,
    pub nexus: Nexus,
    pub api_key: Option<String>,
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

            let response = app.nexus.execute_kip(params).await;
            Ok(Json(response))
        }
        "list_logs" => {
            let params: ListLogParams = serde_json::from_value(req.params).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(Response::err(format!("invalid parameters: {}", e))),
                )
            })?;

            let (logs, next_cursor) = app.nexus.list_logs(params).await.map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(Response::err(format!("failed to list logs: {}", e))),
                )
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
        == Some(expected)
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
