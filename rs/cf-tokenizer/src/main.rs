//! cf-tokenizer — stateless HTTP adapter over the [`tokenizer`] pipeline.
//! Carries no user identity by contract; nothing is persisted or request-logged.
//!
//! Contract:
//!   POST /tokenize  { "texts": ["..."], "mode": "search" }
//!                   → { "tokens": [["tok", ...], ...] }
//!   GET  /healthz   → "ok"
//! Successful tokenization responses carry `X-Tokenizer-Version`; consumers
//! stamp it into index rows so a version bump triggers a rebuild-from-source
//! path instead of silently mixing token vocabularies.

mod tokenizer;

use std::net::SocketAddr;

use axum::http::{header::HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::tokenizer::{tokenize_for_search, TOKENIZER_VERSION};

/// Per-request cap, defensive only: the worker-side client (tokenize.ts)
/// chunks its batches to fit, so a legitimate oversized snapshot never 400s.
const MAX_TEXTS_PER_BATCH: usize = 256;

#[derive(Deserialize)]
struct TokenizeRequest {
    texts: Vec<String>,
    mode: String,
}

#[derive(Serialize)]
struct TokenizeResponse {
    tokens: Vec<Vec<String>>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn version_header() -> (HeaderName, HeaderValue) {
    (
        HeaderName::from_static("x-tokenizer-version"),
        HeaderValue::from_static(TOKENIZER_VERSION),
    )
}

fn bad_request(error: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        [version_header()],
        Json(ErrorResponse { error }),
    )
        .into_response()
}

fn internal_error(error: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [version_header()],
        Json(ErrorResponse { error }),
    )
        .into_response()
}

async fn healthz() -> Response {
    ([version_header()], "ok").into_response()
}

async fn tokenize(Json(request): Json<TokenizeRequest>) -> Response {
    if request.mode != "search" {
        return bad_request(format!(
            "unsupported mode '{}' (expected 'search')",
            request.mode
        ));
    }
    if request.texts.len() > MAX_TEXTS_PER_BATCH {
        return bad_request(format!("batch too large (max {MAX_TEXTS_PER_BATCH} texts)"));
    }
    // Segmentation is pure CPU — hundreds of ms for a full CJK batch — so it
    // runs on the blocking pool. Inline on the async workers it would stall
    // every concurrent request including `/healthz`, and a failed health
    // check restarts the container mid-batch.
    let result = tokio::task::spawn_blocking(move || {
        request
            .texts
            .iter()
            .map(|text| tokenize_for_search(text))
            .collect::<Vec<_>>()
    })
    .await;
    match result {
        Ok(tokens) => ([version_header()], Json(TokenizeResponse { tokens })).into_response(),
        Err(err) => internal_error(format!("tokenization task failed: {err}")),
    }
}

#[tokio::main]
async fn main() {
    tokenizer::warm_up();
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/tokenize", post(tokenize));
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind 0.0.0.0:8080");
    println!("cf-tokenizer v{TOKENIZER_VERSION} listening on {addr}");
    axum::serve(listener, app).await.expect("serve");
}
