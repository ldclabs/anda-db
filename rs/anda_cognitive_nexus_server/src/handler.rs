use anda_kip::{
    ErrorObject, KipError, KipErrorCode, PreparedRequest, Response, ResponseExecution, RetryClass,
    RetryInfo, TopLevelStatus,
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequest, Request as HttpRequest, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::IntoResponse,
    routing,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use crate::runtime::{DetachedError, EXECUTION_TIMEOUT_FACTOR, ExecutionManager};

use crate::nexus::{ListLogParams, ListLogsError, Nexus, RequestLanguages};

#[derive(Clone)]
pub struct AppState {
    pub name: String,
    pub version: String,
    pub nexus: Nexus,
    /// Per-request **response** deadline for `/kip`. A KIP execution that
    /// exceeds it gets a 408 response, but the already-started execution
    /// finishes in the background (see [`ExecutionManager::run`]).
    pub request_timeout: Duration,
    /// All admitted requests share one bounded execution registry.
    pub executions: ExecutionManager,
}

#[derive(Debug, PartialEq, Eq)]
enum CancelSafeError {
    Timeout,
    ShuttingDown,
}

/// Runs a cancel-safe read until it completes, reaches its response deadline,
/// or server shutdown closes admission. Unlike a mutation, `fut` is dropped
/// immediately on either cancellation path.
async fn run_cancel_safe_with_timeout<T, F>(
    admission: &CancellationToken,
    deadline: Duration,
    fut: F,
) -> Result<T, CancelSafeError>
where
    F: std::future::Future<Output = T>,
{
    tokio::select! {
        biased;
        _ = admission.cancelled() => Err(CancelSafeError::ShuttingDown),
        result = tokio::time::timeout(deadline, fut) => {
            result.map_err(|_| CancelSafeError::Timeout)
        }
    }
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

/// Maps a KIP error onto an HTTP status.
///
/// A failed KIP execution used to be returned as HTTP 200, so a broken KML
/// mutation or an internal graph error was indistinguishable from success to
/// load balancers, retry policies, uptime probes, and 5xx alerting — while a
/// malformed `params` on the same endpoint already produced 400 and a bad key
/// 401. The JSON body is unchanged, so existing clients keep parsing it.
///
/// KIP 2.0 replaced the numeric ranges with the named Core Error Registry
/// (§87), where every code carries a category and a retry class. Codes whose
/// HTTP meaning is more specific than their category are listed first; the
/// rest fall back to the category. An unrecognized code is treated as
/// internal: a code the server does not know cannot be proven client-caused.
fn kip_error_status(error: &ErrorObject) -> StatusCode {
    use KipErrorCode::*;
    let Some(code) = error.parsed_code() else {
        return StatusCode::INTERNAL_SERVER_ERROR;
    };
    match code {
        // Existence-neutral by design (§86.4): "absent" and "forbidden" must
        // not be distinguishable, so both answer 404.
        NotFoundOrNotVisible | TransactionUnknown => StatusCode::NOT_FOUND,
        Unauthenticated => StatusCode::UNAUTHORIZED,
        // The request conflicts with state the client must re-read first. The
        // epistemic rules land here too: an Assertion's payload is immutable,
        // so the fix is to assert anew, not to acquire authority.
        VersionConflict
        | PreconditionFailed
        | SerializationConflict
        | IdempotencyConflict
        | SchemaEnvironmentChanged
        | IdentityConflict
        | ClientKeyConflict
        | IdentityMergeConflict
        | ImportPreviewConflict
        | ImmutableField
        | EpistemicRevisionRequired
        | EvidenceCorrectionRequired
        | EvidenceCorrectionConflict
        | SupersessionMismatch
        | InvalidLifecycleTransition
        | ActivityTerminal
        | LegalHoldConflict => StatusCode::CONFLICT,
        // The coordinate the client is holding is gone for good; retrying the
        // same bytes cannot work.
        HistoricalSnapshotUnavailable
        | HistoricalSchemaUnavailable
        | CursorExpired
        | CursorInvalid => StatusCode::GONE,
        TransactionTooLarge | ArtifactTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        RateLimited => StatusCode::TOO_MANY_REQUESTS,
        ExecutionTimeout => StatusCode::REQUEST_TIMEOUT,
        // This runtime does not implement it — a deliberate gap, not a bad
        // request (see `DESCRIBE CAPABILITIES`).
        UnsupportedCapability
        | UnsupportedIsolation
        | SearchModeUnsupported
        | HistoricalSearchUnavailable => StatusCode::NOT_IMPLEMENTED,
        SearchIndexUnavailable | ArtifactUnavailable | BlobUnavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        // §80.3: the write may have committed. The client must look the
        // transaction up rather than re-issue it, so this must not read as a
        // clean client-side failure.
        OutcomeUnknown | InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        other => match other.category() {
            anda_kip::ErrorCategory::Governance => StatusCode::FORBIDDEN,
            anda_kip::ErrorCategory::Transport | anda_kip::ErrorCategory::System => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            // Syntax, protocol, schema, data, epistemic, history, search,
            // artifact and resource: the request itself must change.
            _ => StatusCode::BAD_REQUEST,
        },
    }
}

/// The HTTP status for a KIP response, logging the ones that count as server
/// failures so a 5xx in the access log is also visible in the service log.
///
/// A `partial` batch answers 207: under `sequence`, the operations that
/// committed before the failure stay durable (§75.2), and reporting the whole
/// request as an error invites a client to re-issue writes that already
/// landed.
fn kip_status(response: &Response) -> StatusCode {
    match response.status {
        TopLevelStatus::Succeeded => StatusCode::OK,
        TopLevelStatus::Partial => StatusCode::MULTI_STATUS,
        _ => {
            // An envelope failure reports at the request level; an ordinary
            // operation failure reports on its own result and leaves the
            // request-level slot empty.
            let error = response.error.as_ref().or_else(|| {
                response
                    .results
                    .iter()
                    .find_map(|result| result.error.as_ref())
            });
            let Some(error) = error else {
                return StatusCode::INTERNAL_SERVER_ERROR;
            };
            let status = kip_error_status(error);
            if status.is_server_error() {
                log::error!(
                    action = "post_kip",
                    code = error.code;
                    "KIP execution failed: {}", error.message,
                );
            }
            status
        }
    }
}

// Keep the handler's error variant small without changing the JSON envelope.
type KipHttpError = (StatusCode, Json<Box<Response>>);

/// A single-error response body, for the failures the HTTP layer itself
/// produces before or around execution.
fn error_response(code: KipErrorCode, message: impl Into<String>) -> Json<Box<Response>> {
    Json(Box::new(Response::failed(KipError::new(code, message))))
}

/// POST /kip
///
/// Authentication runs in the router layer ([`build_router`]), before this
/// handler and before the body is read or parsed.
pub async fn post_kip(
    State(app): State<AppState>,
    request: HttpRequest,
) -> Result<(StatusCode, Json<Response>), KipHttpError> {
    if app.executions.admission.is_cancelled() {
        return Err((StatusCode::SERVICE_UNAVAILABLE, shutting_down()));
    }

    // Capacity bounds body buffering and parsing as well as execution.
    let permit = app
        .executions
        .reserve()
        .map_err(|err| detached_error(err, false))?;
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if content_type != "application/json" && !content_type.ends_with("+json") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_response(
                KipErrorCode::InvalidRequestEnvelope,
                "expected application/json",
            ),
        ));
    }
    let decode_error = |message: String| {
        (
            StatusCode::BAD_REQUEST,
            error_response(KipErrorCode::InvalidRequestEnvelope, message),
        )
    };
    let body = run_cancel_safe_with_timeout(
        &app.executions.admission,
        app.request_timeout.saturating_mul(2),
        axum::body::Bytes::from_request(request, &app),
    )
    .await
    .map_err(|err| match err {
        CancelSafeError::Timeout => {
            timeout_error("request body reception exceeded the configured timeout")
        }
        CancelSafeError::ShuttingDown => (StatusCode::SERVICE_UNAVAILABLE, shutting_down()),
    })?
    .map_err(|err| {
        (
            err.status(),
            error_response(
                if err.status() == StatusCode::PAYLOAD_TOO_LARGE {
                    KipErrorCode::ResourceExhausted
                } else {
                    KipErrorCode::InvalidRequestEnvelope
                },
                err.body_text(),
            ),
        )
    })?;
    let source = std::str::from_utf8(&body).map_err(|e| decode_error(e.to_string()))?;
    let value = anda_kip::parse_canonical_json(source).map_err(|e| decode_error(e.message))?;
    let req: JsonRpcRequest =
        serde_json::from_value(value).map_err(|e| decode_error(e.to_string()))?;

    match req.method.as_str() {
        "execute_kip" => {
            let params = PreparedRequest::from_value(req.params).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    error_response(e.code, format!("invalid parameters: {e}")),
                )
            })?;

            let nexus = app.nexus.clone();
            let languages = RequestLanguages::of(&params);
            let has_mutation = languages.has_mutation() && !params.request().is_dry_run();
            let request_id = params.request().request_id.clone();
            let execution = ResponseExecution {
                mode: params.request().execution_mode(),
                on_error: Some(
                    params
                        .request()
                        .execution
                        .as_ref()
                        .and_then(|e| e.on_error)
                        .unwrap_or(anda_kip::OnError::Stop),
                ),
                isolation: params
                    .request()
                    .execution
                    .as_ref()
                    .and_then(|e| e.isolation.clone()),
                idempotency_key: params
                    .request()
                    .execution
                    .as_ref()
                    .and_then(|e| e.idempotency_key.clone()),
                extensions: None,
            };
            let response = app
                .executions
                .run(
                    permit,
                    app.request_timeout,
                    app.request_timeout.saturating_mul(EXECUTION_TIMEOUT_FACTOR),
                    async move { nexus.execute_kip(params, &languages).await },
                )
                .await
                .map_err(|err| {
                    let mut error = detached_error(err, has_mutation);
                    error.1.0.request_id = request_id;
                    error.1.0.execution = Some(execution);
                    error
                })?;
            match kip_status(&response) {
                status if status.is_success() => Ok((status, Json(response))),
                status => Err((status, Json(Box::new(response)))),
            }
        }
        "list_logs" => {
            let params: ListLogParams = serde_json::from_value(req.params).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    error_response(
                        KipErrorCode::InvalidRequestEnvelope,
                        format!("invalid parameters: {e}"),
                    ),
                )
            })?;

            // Listing is read-only and cancel-safe. Listen to admission even
            // after the request passed the initial check: aborting axum's
            // outer Serve future at the hard deadline does not necessarily
            // abort connection tasks that it already spawned. Dropping this
            // read future on shutdown prevents it from crossing DB close.
            let (logs, next_cursor) = run_cancel_safe_with_timeout(
                &app.executions.admission,
                app.request_timeout,
                app.nexus.list_logs(params),
            )
            .await
            .map_err(|err| match err {
                CancelSafeError::Timeout => {
                    timeout_error("request processing exceeded the configured timeout")
                }
                CancelSafeError::ShuttingDown => (StatusCode::SERVICE_UNAVAILABLE, shutting_down()),
            })?
            .map_err(|err| match err {
                // Client input error: an undecodable cursor.
                ListLogsError::InvalidCursor(e) => (
                    StatusCode::BAD_REQUEST,
                    error_response(KipErrorCode::CursorInvalid, format!("invalid cursor: {e}")),
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
                        error_response(KipErrorCode::InternalError, "failed to list logs"),
                    )
                }
            })?;

            Ok((
                StatusCode::OK,
                Json(Response {
                    next_cursor,
                    ..Response::ok(json!(logs))
                }),
            ))
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            error_response(
                KipErrorCode::InvalidRequestEnvelope,
                format!("unknown method: {}", req.method),
            ),
        )),
    }
}

/// Failures before execution are known refusals; a started write can have an
/// unknown outcome. Keep HTTP status, protocol code and retry advice aligned.
fn detached_error(err: DetachedError, has_mutation: bool) -> KipHttpError {
    match err {
        DetachedError::Timeout => abandoned_response(has_mutation),
        DetachedError::Join(err) => {
            log::error!("KIP execution task failed: {err:?}");
            if has_mutation {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(Box::new(Response::outcome_unknown(
                        KipError::outcome_unknown(
                            "the KIP execution task failed before reporting its outcome",
                        ),
                    ))),
                )
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    safe_error(KipErrorCode::InternalError, "KIP read task failed"),
                )
            }
        }
        DetachedError::ShuttingDown => (StatusCode::SERVICE_UNAVAILABLE, shutting_down()),
        DetachedError::Busy => (
            StatusCode::TOO_MANY_REQUESTS,
            error_response(
                KipErrorCode::RateLimited,
                "server execution capacity is exhausted; back off and retry",
            ),
        ),
    }
}

/// Override conservative registry defaults only where no cognitive write can
/// have occurred (body reception, a refused request, or a read-only request).
fn safe_error(code: KipErrorCode, message: impl Into<String>) -> Json<Box<Response>> {
    let mut error = ErrorObject::new(code, message);
    error.retry = Some(RetryInfo::new(RetryClass::SafeSameRequest));
    error.hint = Some(
        "Back off and retry the identical request; no cognitive mutation was executed.".into(),
    );
    Json(Box::new(Response::failed(error)))
}

fn timeout_error(message: impl Into<String>) -> KipHttpError {
    (
        StatusCode::REQUEST_TIMEOUT,
        safe_error(KipErrorCode::ExecutionTimeout, message),
    )
}

/// The response for an execution whose *response* deadline elapsed while the
/// execution itself kept running.
///
/// A read has no cognitive write to reconcile, so this transport can explicitly
/// report safe retry instead of the registry's conservative timeout default. For a mutation it is §80.3's unknown outcome — the write
/// may still commit — and answering with a `safe_same_request` class would be
/// an invitation to commit the same cognition twice.
fn abandoned_response(has_mutation: bool) -> KipHttpError {
    if has_mutation {
        (
            StatusCode::REQUEST_TIMEOUT,
            Json(Box::new(Response::outcome_unknown(
                KipError::outcome_unknown(
                    "the response deadline elapsed while the mutation was still running; it may \
                 still commit. Look the transaction up instead of re-issuing it",
                ),
            ))),
        )
    } else {
        timeout_error(
            "request processing exceeded the configured timeout; \
             the started KIP execution continues on the server",
        )
    }
}

/// A shutdown refusal is known not to have executed.
fn shutting_down() -> Json<Box<Response>> {
    safe_error(KipErrorCode::InternalError, "server is shutting down")
}

/// Rejects an unauthenticated `/kip` request before any extractor runs.
///
/// Checking the key inside the handler let the `Json` extractor parse the
/// body (and the body-limit layer reject it) first, so an anonymous caller
/// could tell 400/413 apart from 401 and make the server spend parsing work
/// on unauthenticated input.
async fn require_api_key(
    State(api_key): State<Arc<Option<String>>>,
    request: HttpRequest,
    next: Next,
) -> Result<axum::response::Response, KipHttpError> {
    if !authorize_api_key(api_key.as_deref(), request.headers()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            error_response(KipErrorCode::Unauthenticated, "invalid API key"),
        ));
    }
    Ok(next.run(request).await)
}

/// Builds the HTTP router.
///
/// `GET /` stays unauthenticated so load balancers can probe the instance;
/// `/kip` runs [`require_api_key`] as a route layer, i.e. before the body is
/// read or parsed.
pub fn build_router(state: AppState, api_key: Option<String>, max_body_size: usize) -> Router {
    Router::new()
        .route("/", routing::get(get_information))
        .route(
            "/kip",
            routing::post(post_kip).layer(middleware::from_fn_with_state(
                Arc::new(api_key),
                require_api_key,
            )),
        )
        .layer(DefaultBodyLimit::max(max_body_size.max(1024)))
        .with_state(state)
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
    use anda_db::{
        database::{AndaDB, DBConfig},
        storage::StorageConfig,
    };
    use anda_kip::KipErrorCode;
    use axum::{
        body::{Body, Bytes},
        http::{HeaderValue, Request as AxumRequest},
    };
    use http_body_util::BodyExt;
    use object_store::memory::InMemory;
    use tower::ServiceExt;

    async fn test_state() -> AppState {
        test_state_with_store(Arc::new(InMemory::new())).await
    }

    async fn test_state_with_store(store: Arc<dyn object_store::ObjectStore>) -> AppState {
        let db = AndaDB::connect(
            store,
            DBConfig {
                name: "kip_handler_test".to_string(),
                description: String::new(),
                storage: StorageConfig::default(),
                lock: None,
            },
        )
        .await
        .unwrap();
        let nexus = Nexus::connect(Arc::new(db), &[], 8 * 1024).await.unwrap();
        AppState {
            name: "test".to_string(),
            version: "0.0.0".to_string(),
            nexus,
            request_timeout: Duration::from_secs(30),
            executions: ExecutionManager::new(4),
        }
    }

    async fn test_app(api_key: Option<String>) -> Router {
        build_router(test_state().await, api_key, 2 * 1024 * 1024)
    }

    async fn post_json(app: &Router, body: &str, api_key: Option<&str>) -> (StatusCode, Value) {
        let mut builder =
            AxumRequest::post("/kip").header(header::CONTENT_TYPE, "application/json");
        if let Some(key) = api_key {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
        }
        let resp = app
            .clone()
            .oneshot(builder.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

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

    #[tokio::test]
    async fn route_timeout_covers_a_stalled_json_body() {
        let mut state = test_state().await;
        state.request_timeout = Duration::from_millis(20);
        let app = build_router(state, None, 2 * 1024 * 1024);

        let (mut tx, channel_body) = http_body_util::channel::Channel::<Bytes>::new(4);
        tx.send_data(Bytes::from_static(b"{\"method\":"))
            .await
            .unwrap();

        let resp = app
            .oneshot(
                AxumRequest::post("/kip")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::new(channel_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "ExecutionTimeout");
        assert_eq!(
            body["error"]["message"],
            "request body reception exceeded the configured timeout"
        );

        // Keep the producer alive until after the timeout response so the
        // route was ended by the deadline rather than end-of-stream.
        drop(tx);
    }

    #[tokio::test]
    async fn shutdown_cancels_an_in_flight_read() {
        let admission = CancellationToken::new();
        let read = tokio::spawn({
            let admission = admission.clone();
            async move {
                run_cancel_safe_with_timeout(
                    &admission,
                    Duration::from_secs(60),
                    std::future::pending::<()>(),
                )
                .await
            }
        });
        tokio::task::yield_now().await;

        admission.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), read)
            .await
            .expect("cancel-safe read must observe shutdown promptly")
            .expect("cancel-safe read task panicked");
        assert_eq!(result, Err(CancelSafeError::ShuttingDown));
    }

    /// Every KIP error class maps to a status a load balancer, retry policy,
    /// or 5xx alert can act on — client-caused to 4xx, internal to 5xx.
    #[test]
    fn kip_error_classes_map_to_meaningful_statuses() {
        let status = |code: KipErrorCode| kip_error_status(&ErrorObject::new(code, "boom"));

        for code in [
            KipErrorCode::InvalidSyntax,
            KipErrorCode::InvalidIdentifier,
            KipErrorCode::InvalidRequestEnvelope,
            KipErrorCode::LanguageMismatch,
            KipErrorCode::SchemaSymbolNotFound,
            KipErrorCode::TypeMismatch,
            KipErrorCode::ConstraintViolation,
            KipErrorCode::ReferenceError,
            KipErrorCode::ResourceExhausted,
            KipErrorCode::ResultLimitExceeded,
            KipErrorCode::CapsuleValidationFailed,
        ] {
            assert_eq!(status(code), StatusCode::BAD_REQUEST, "code: {code}");
        }
        assert_eq!(
            status(KipErrorCode::NotFoundOrNotVisible),
            StatusCode::NOT_FOUND
        );
        assert_eq!(status(KipErrorCode::VersionConflict), StatusCode::CONFLICT);
        // The epistemic rules are conflicts with recorded state, not authority
        // failures: the fix is a new Assertion, not a bigger permission.
        assert_eq!(status(KipErrorCode::ImmutableField), StatusCode::CONFLICT);
        assert_eq!(
            status(KipErrorCode::EpistemicRevisionRequired),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status(KipErrorCode::Unauthenticated),
            StatusCode::UNAUTHORIZED
        );
        for code in [
            KipErrorCode::NotAuthorized,
            KipErrorCode::ProtectedSystemField,
            KipErrorCode::PurgeDenied,
        ] {
            assert_eq!(status(code), StatusCode::FORBIDDEN, "code: {code}");
        }
        assert_eq!(status(KipErrorCode::CursorExpired), StatusCode::GONE);
        // A gap this engine declares (`DESCRIBE CAPABILITIES`) is not a
        // malformed request.
        assert_eq!(
            status(KipErrorCode::UnsupportedCapability),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            status(KipErrorCode::RateLimited),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            status(KipErrorCode::ExecutionTimeout),
            StatusCode::REQUEST_TIMEOUT
        );
        assert_eq!(
            status(KipErrorCode::InternalError),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        // §80.3: a write whose fate is unknown must not read as a clean
        // client-side failure.
        assert_eq!(
            status(KipErrorCode::OutcomeUnknown),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        // An unknown code cannot be proven client-caused.
        assert_eq!(
            kip_error_status(&ErrorObject {
                code: "SomeFutureCode".to_string(),
                message: "boom".to_string(),
                ..Default::default()
            }),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /// Abandoning the response is not abandoning the execution. A read may be
    /// re-sent as-is; a mutation that is still running must be looked up, not
    /// re-issued (§80.3).
    #[test]
    fn an_abandoned_mutation_is_reported_as_an_unknown_outcome() {
        let (status, Json(read)) = abandoned_response(false);
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(read.status, TopLevelStatus::Failed);
        assert_eq!(
            read.error.as_ref().and_then(|error| error.parsed_code()),
            Some(KipErrorCode::ExecutionTimeout)
        );

        let (status, Json(write)) = abandoned_response(true);
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(write.status, TopLevelStatus::OutcomeUnknown);
        assert_eq!(
            write.error.as_ref().and_then(|error| error.retry),
            Some(anda_kip::RetryInfo::new(
                anda_kip::RetryClass::OutcomeLookupRequired
            ))
        );
    }

    /// A failed KIP execution must not answer HTTP 200; the JSON body is the
    /// standard response envelope in every case.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_kip_execution_is_not_http_200() {
        let app = test_app(None).await;

        let (status, body) = post_json(
            &app,
            r#"{"method":"execute_kip","params":{"kip":"2.0","operations":[{"command":"THIS IS NOT KIP"}]}}"#,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
        assert_eq!(body["status"], "failed", "body: {body}");
        // A single-operation failure reports on its own result; the
        // request-level slot is for envelope failures.
        assert_eq!(
            body["results"][0]["error"]["code"], "InvalidSyntax",
            "body: {body}"
        );
        assert!(
            body["results"][0]["error"]["hint"].is_string(),
            "body: {body}"
        );

        // A successful execution still answers 200 with the same shape.
        let (status, body) = post_json(
            &app,
            r#"{"method":"execute_kip","params":{"kip":"2.0","operations":[{"command":"DESCRIBE PRIMER"}]}}"#,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body["status"], "succeeded", "body: {body}");
        assert!(body.get("error").is_none(), "body: {body}");

        // An envelope the protocol rejects never reaches the engine.
        let (status, body) = post_json(
            &app,
            r#"{"method":"execute_kip","params":{"kip":"1.0","operations":[{"command":"DESCRIBE PRIMER"}]}}"#,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
        assert_eq!(
            body["error"]["code"], "UnsupportedProtocolVersion",
            "body: {body}"
        );
    }

    /// A batch is not a transaction (§75.4): when one operation of a
    /// `sequence` fails after another committed, the request is `partial`, and
    /// answering 4xx would invite the client to re-issue a durable write.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_partial_batch_answers_207_rather_than_an_error() {
        let app = test_app(None).await;
        let (status, body) = post_json(
            &app,
            r#"{"method":"execute_kip","params":{"kip":"2.0",
                "execution":{"mode":"sequence","on_error":"continue"},
                "operations":[
                    {"command":"DESCRIBE PRIMER"},
                    {"command":"THIS IS NOT KIP"}
                ]}}"#,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::MULTI_STATUS, "body: {body}");
        assert_eq!(body["status"], "partial", "body: {body}");
        assert_eq!(body["results"][0]["status"], "succeeded", "body: {body}");
        assert_eq!(body["results"][1]["status"], "failed", "body: {body}");
    }

    /// `atomic` needs one transaction and one snapshot across the batch. This
    /// server runs operations one at a time, so it refuses rather than
    /// silently downgrading to `sequence`.
    #[tokio::test(flavor = "multi_thread")]
    async fn atomic_execution_is_refused_rather_than_downgraded() {
        let app = test_app(None).await;
        let (status, body) = post_json(
            &app,
            r#"{"method":"execute_kip","params":{"kip":"2.0",
                "execution":{"mode":"atomic"},
                "operations":[
                    {"command":"ARCHIVE :a"},
                    {"command":"ARCHIVE :b"}
                ]}}"#,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "body: {body}");
        assert_eq!(
            body["error"]["code"], "UnsupportedCapability",
            "body: {body}"
        );
    }

    /// Authentication must run before the body is parsed: an anonymous caller
    /// may not distinguish a malformed body (400) or an oversized one (413)
    /// from a rejected key, nor make the server parse its input.
    #[tokio::test(flavor = "multi_thread")]
    async fn unauthenticated_requests_are_rejected_before_the_body_is_parsed() {
        let app = test_app(Some("secret".to_string())).await;

        for (body, key) in [
            (r#"{"method": "#.to_string(), None),
            ("not json at all".to_string(), None),
            (r#"{"method":"execute_kip"}"#.to_string(), None),
            (
                format!(r#"{{"junk":"{}"}}"#, "x".repeat(4 * 1024 * 1024)),
                None,
            ),
            (r#"{"method": "#.to_string(), Some("wrong")),
        ] {
            let (status, _) = post_json(&app, &body, key).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "body prefix: {:.20}",
                body
            );
        }

        // With the right key the same malformed body is a normal 400.
        let (status, body) = post_json(&app, r#"{"method": "#, Some("secret")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");

        // The health endpoint stays unauthenticated.
        let resp = app
            .oneshot(AxumRequest::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admission_rejections_are_retryable_before_body_parsing() {
        let state = test_state().await;
        let manager = state.executions.clone();
        let permits: Vec<_> = (0..4).map(|_| manager.reserve().unwrap()).collect();
        let app = build_router(state, None, 1024);
        let (status, body) = post_json(&app, "not JSON", None).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body["error"]["code"], "RateLimited");
        assert_eq!(body["error"]["retry"]["class"], "safe_same_request");
        drop(permits);
        manager.admission.cancel();
        let (status, body) = post_json(&app, "not JSON", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["retry"]["class"], "safe_same_request");
    }

    #[tokio::test]
    async fn body_limit_and_strict_json_survive_manual_body_reception() {
        let app = build_router(test_state().await, None, 1024);
        let (status, body) = post_json(&app, &"x".repeat(2048), None).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["error"]["code"], "ResourceExhausted");
        let (status, _) = post_json(
            &app,
            r#"{"method":"list_logs","method":"execute_kip","params":{}}"#,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn delayed_body_does_not_override_a_started_writes_outcome() {
        use anda_object_store::{FaultGate, FaultKind, FaultOp, FaultRule, FaultStore};
        let (store, fault) = FaultStore::wrap(InMemory::new());
        let mut state = test_state_with_store(Arc::new(store)).await;
        state.request_timeout = Duration::from_millis(500);
        let manager = state.executions.clone();
        let nexus = state.nexus.clone();
        let app = build_router(state, None, 2 * 1024 * 1024);
        let gate = FaultGate::new();
        // Pause the audit append after the graph transaction committed. A
        // timeout still cannot claim a failed write or lose its recovery key.
        fault.push_rule(FaultRule {
            op: FaultOp::Put,
            path_contains: Some("kip_logs".into()),
            skip: 0,
            times: 1,
            kind: FaultKind::PauseBefore(gate.clone()),
        });
        let (mut tx, channel) = http_body_util::channel::Channel::<Bytes>::new(1);
        let request = tokio::spawn(
            app.oneshot(
                AxumRequest::post("/kip")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::new(channel))
                    .unwrap(),
            ),
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
        tx.send_data(Bytes::from_static(br#"{"method":"execute_kip","params":{"kip":"2.0","request_id":"slow-write","execution":{"mode":"independent","idempotency_key":"write-key"},"operations":[{"command":"CREATE CONCEPT ?p { TYPE \"Person\" NAME \"Alice\" }"}]}}"#)).await.unwrap();
        drop(tx);
        tokio::time::timeout(Duration::from_secs(5), gate.wait_entered())
            .await
            .unwrap();
        let response = request.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "outcome_unknown", "{body}");
        assert_eq!(body["error"]["retry"]["class"], "outcome_lookup_required");
        assert_eq!(body["request_id"], "slow-write");
        assert_eq!(body["execution"]["idempotency_key"], "write-key");
        assert_eq!(body["execution"]["mode"], "independent");
        gate.release();
        manager
            .drain(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let (logs, _) = nexus.list_logs(ListLogParams::default()).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].response["status"], "succeeded");
        assert!(logs[0].response["operations"][0]["tx_id"].is_string());
    }
}
