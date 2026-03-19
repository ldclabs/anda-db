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
    if let Some(expected_api_key) = &app.api_key {
        let provided_api_key = header
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let provided_api_key = provided_api_key.trim_start_matches("Bearer ");
        if provided_api_key != expected_api_key {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(Response::err("invalid API key".to_string())),
            ));
        }
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
