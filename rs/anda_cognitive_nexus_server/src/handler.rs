use anda_kip::{ErrorObject, KipError, KipErrorCode, Request, Response, TopLevelStatus};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request as HttpRequest, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::IntoResponse,
    routing,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::nexus::{ListLogParams, ListLogsError, Nexus, RequestLanguages};

#[derive(Clone)]
pub struct AppState {
    pub name: String,
    pub version: String,
    pub nexus: Nexus,
    /// Per-request **response** deadline for `/kip`. A KIP execution that
    /// exceeds it gets a 408 response, but the already-started execution
    /// finishes in the background (see [`run_detached_with_timeout`]).
    pub request_timeout: Duration,
    /// Hard upper bound on a detached KIP execution. It exists only so a
    /// stuck or pathologically slow execution eventually initiates process
    /// shutdown; see [`run_detached_with_timeout`].
    pub execution_timeout: Duration,
    /// Stops new KIP work as soon as graceful shutdown begins.
    pub admission: CancellationToken,
    /// Tracks non-cancel-safe KIP mutations that may outlive their HTTP
    /// response deadline.
    pub mutation_tasks: TaskTracker,
    /// Bounds detached mutation concurrency.
    pub mutation_permits: Arc<Semaphore>,
    /// Serializes the final admission check and task registration with
    /// shutdown's tracker close. The handles are retained only for the hard
    /// shutdown deadline.
    pub mutation_aborts: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
}

/// How a detached execution failed to produce a value before the deadline.
#[derive(Debug)]
pub enum DetachedError {
    /// The response deadline elapsed; the detached task keeps running. Its
    /// hard deadline initiates process shutdown without cancelling it.
    Timeout,
    /// The detached task itself failed (panicked or was aborted).
    Join(tokio::task::JoinError),
    /// Shutdown has closed request admission.
    ShuttingDown,
    /// The bounded mutation executor has no free capacity.
    Busy,
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

/// Runs `fut` on a detached task with a response deadline and a hard
/// execution deadline.
///
/// KML execution mutates the graph in multiple steps and has no rollback
/// log: cancelling it mid-flight — which is exactly what dropping the
/// future inside `tokio::time::timeout` does — can leave half-written
/// graph state (e.g. an `UPSERT` with only a prefix of its blocks
/// applied). Spawning first means `deadline` only abandons the *response*;
/// the execution itself keeps running in the background.
///
/// `hard_deadline` bounds how long the process may keep serving after a
/// runaway execution. The execution itself is never cancelled here: doing so
/// would poison the affected AndaDB collection while the process continued to
/// serve stale state. Instead the deadline closes admission through
/// `admission`; `main` observes that token, drains the server, and lets the
/// existing shutdown path either finish the mutation or terminate at a
/// crash-recoverable point. It must be set well above the response deadline
/// so a normal slow request never initiates shutdown.
pub(crate) async fn run_detached_with_timeout<T, F>(
    admission: &CancellationToken,
    tracker: &TaskTracker,
    permits: Arc<Semaphore>,
    aborts: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    deadline: Duration,
    hard_deadline: Duration,
    fut: F,
) -> Result<T, DetachedError>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    if admission.is_cancelled() {
        return Err(DetachedError::ShuttingDown);
    }
    let hard_shutdown = admission.clone();
    let permit = permits
        .try_acquire_owned()
        .map_err(|_| DetachedError::Busy)?;
    // Shutdown takes this same lock before closing the TaskTracker. This
    // closes the otherwise-real race where a handler passes its final token
    // check, is preempted, and registers a mutation after `wait()` returned.
    let mut handles = aborts.lock().await;
    if admission.is_cancelled() {
        return Err(DetachedError::ShuttingDown);
    }
    let task = tracker.spawn(async move {
        // The permit is released when this task ends, never when the response
        // waiter gives up.
        let _permit = permit;
        tokio::pin!(fut);
        tokio::select! {
            value = &mut fut => value,
            _ = tokio::time::sleep(hard_deadline) => {
                log::error!(
                    action = "run_detached_with_timeout",
                    hard_deadline_secs = hard_deadline.as_secs();
                    "detached execution exceeded its hard deadline; closing admission \
                     and initiating process shutdown without cancelling the mutation",
                );
                hard_shutdown.cancel();
                fut.await
            }
        }
    });
    handles.retain(|handle| !handle.is_finished());
    handles.push(task.abort_handle());
    drop(handles);
    match tokio::time::timeout(deadline, task).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(join_error)) => Err(DetachedError::Join(join_error)),
        // Dropping the JoinHandle detaches the task: it keeps running. The
        // hard deadline closes admission but does not drop the future.
        Err(_) => Err(DetachedError::Timeout),
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
        | CursorInvalidated
        | ChangeCursorExpired => StatusCode::GONE,
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

/// A single-error response body, for the failures the HTTP layer itself
/// produces before or around execution.
fn error_response(code: KipErrorCode, message: impl Into<String>) -> Json<Response> {
    Json(Response::failed(KipError::new(code, message)))
}

/// POST /kip
///
/// Authentication runs in the router layer ([`build_router`]), before this
/// handler and before the `Json` extractor parses the body.
pub async fn post_kip(
    State(app): State<AppState>,
    Json(req): Json<JsonRpcRequest>,
) -> Result<(StatusCode, Json<Response>), (StatusCode, Json<Response>)> {
    if app.admission.is_cancelled() {
        return Err((StatusCode::SERVICE_UNAVAILABLE, shutting_down()));
    }

    match req.method.as_str() {
        "execute_kip" => {
            let params: Request = serde_json::from_value(req.params).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    error_response(
                        KipErrorCode::InvalidRequestEnvelope,
                        format!("invalid parameters: {e}"),
                    ),
                )
            })?;

            // Detached execution: on timeout the client gets a 408, but the
            // started KIP execution (possibly a mid-write KML mutation)
            // continues to completion instead of being cancelled halfway.
            let nexus = app.nexus.clone();
            let languages = RequestLanguages::of(&params);
            let has_mutation = languages.has_mutation();
            let response = run_detached_with_timeout(
                &app.admission,
                &app.mutation_tasks,
                app.mutation_permits.clone(),
                app.mutation_aborts.clone(),
                app.request_timeout,
                app.execution_timeout,
                async move { nexus.execute_kip(params, &languages).await },
            )
            .await
            .map_err(|err| match err {
                DetachedError::Timeout => {
                    log::warn!(
                        action = "post_kip",
                        method = "execute_kip",
                        timeout_secs = app.request_timeout.as_secs();
                        "response deadline exceeded; KIP execution continues in the background",
                    );
                    abandoned_response(has_mutation)
                }
                DetachedError::Join(join_error) => {
                    log::error!(
                        action = "post_kip",
                        method = "execute_kip";
                        "KIP execution task failed: {join_error:?}",
                    );
                    // The task was spawned, so a KML mutation may have run to
                    // an arbitrary point before it died: §80.3 says the client
                    // must look the outcome up, not re-issue the write.
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(Response::outcome_unknown(KipError::outcome_unknown(
                            "the KIP execution task failed before reporting its outcome",
                        ))),
                    )
                }
                DetachedError::ShuttingDown => (StatusCode::SERVICE_UNAVAILABLE, shutting_down()),
                DetachedError::Busy => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    error_response(
                        KipErrorCode::ResourceExhausted,
                        "server mutation capacity is exhausted",
                    ),
                ),
            })?;
            match kip_status(&response) {
                status if status.is_success() => Ok((status, Json(response))),
                status => Err((status, Json(response))),
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
                &app.admission,
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
                    error_response(
                        KipErrorCode::CursorInvalidated,
                        format!("invalid cursor: {e}"),
                    ),
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

/// Bounds the complete HTTP route, including buffering the request body and
/// running extractors. Handler-local deadlines only start after `Json` has
/// consumed the body, so without this outer layer a slow client can occupy a
/// connection indefinitely.
///
/// As in `anda_db_server`, the transport deadline is twice the processing
/// timeout. This lets a request whose body arrived normally receive the more
/// precise handler-local timeout first, while still placing a finite bound on
/// a stalled body. A mutation already admitted by [`post_kip`] remains owned
/// by the mutation tracker when the response future is dropped.
pub async fn total_timeout(
    State(request_timeout): State<Duration>,
    req: HttpRequest,
    next: Next,
) -> axum::response::Response {
    let deadline = request_timeout.saturating_mul(2);
    match tokio::time::timeout(deadline, next.run(req)).await {
        Ok(resp) => resp,
        Err(_) => {
            timeout_error("request processing exceeded the configured timeout").into_response()
        }
    }
}

fn timeout_error(message: impl Into<String>) -> (StatusCode, Json<Response>) {
    (
        StatusCode::REQUEST_TIMEOUT,
        error_response(KipErrorCode::ExecutionTimeout, message),
    )
}

/// The response for an execution whose *response* deadline elapsed while the
/// execution itself kept running.
///
/// For a read that is a plain timeout: nothing was written, and re-sending the
/// identical request is safe, which is what `ExecutionTimeout`'s registered
/// retry class says. For a mutation it is §80.3's unknown outcome — the write
/// may still commit — and answering with a `safe_same_request` class would be
/// an invitation to commit the same cognition twice.
fn abandoned_response(has_mutation: bool) -> (StatusCode, Json<Response>) {
    if has_mutation {
        (
            StatusCode::REQUEST_TIMEOUT,
            Json(Response::outcome_unknown(KipError::outcome_unknown(
                "the response deadline elapsed while the mutation was still running; it may \
                 still commit. Look the transaction up instead of re-issuing it",
            ))),
        )
    } else {
        timeout_error(
            "request processing exceeded the configured timeout; \
             the started KIP execution continues on the server",
        )
    }
}

/// The body for a request refused because the process is draining.
///
/// `InternalError` rather than a bespoke code: its registered retry class is
/// `safe_same_request`, which is exactly right here — nothing was executed, so
/// re-sending the identical envelope to another instance is safe.
fn shutting_down() -> Json<Response> {
    error_response(KipErrorCode::InternalError, "server is shutting down")
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
            error_response(
                KipErrorCode::ResourceExhausted,
                "request body exceeds the configured size limit",
            ),
        )
            .into_response();
    }
    resp
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
) -> Result<axum::response::Response, (StatusCode, Json<Response>)> {
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
    let request_timeout = state.request_timeout;
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
        .layer(middleware::from_fn(normalize_rejections))
        .layer(middleware::from_fn_with_state(
            request_timeout,
            total_timeout,
        ))
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

    async fn test_app(api_key: Option<String>) -> Router {
        let db = AndaDB::connect(
            Arc::new(InMemory::new()),
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
        let state = AppState {
            name: "test".to_string(),
            version: "0.0.0".to_string(),
            nexus,
            request_timeout: Duration::from_secs(30),
            execution_timeout: Duration::from_secs(120),
            admission: CancellationToken::new(),
            mutation_tasks: TaskTracker::new(),
            mutation_permits: Arc::new(Semaphore::new(4)),
            mutation_aborts: Arc::new(Mutex::new(Vec::new())),
        };
        build_router(state, api_key, 2 * 1024 * 1024)
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
        async fn extract(Json(_): Json<JsonRpcRequest>) -> StatusCode {
            StatusCode::OK
        }

        let app = Router::new().route("/kip", routing::post(extract)).layer(
            middleware::from_fn_with_state(Duration::from_millis(20), total_timeout),
        );

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
            "request processing exceeded the configured timeout"
        );

        // Keep the producer alive until after the timeout response so the
        // route was ended by the deadline rather than end-of-stream.
        drop(tx);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn detached_timeout_returns_early_but_lets_execution_finish() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let completed = Arc::new(AtomicBool::new(false));
        let flag = completed.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let admission = CancellationToken::new();
        let tracker = TaskTracker::new();

        let result = run_detached_with_timeout(
            &admission,
            &tracker,
            Arc::new(Semaphore::new(1)),
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_millis(20),
            Duration::from_secs(30),
            async move {
                started_tx.send(()).unwrap();
                // Held open well past the deadline until the test releases it.
                let _ = release_rx.await;
                flag.store(true, Ordering::SeqCst);
                42u32
            },
        )
        .await;

        // The caller observes the timeout, not the value.
        assert!(matches!(result, Err(DetachedError::Timeout)));
        // The detached execution was started and, once unblocked, still
        // runs to completion instead of being cancelled by the timeout.
        started_rx.await.unwrap();
        assert!(!completed.load(Ordering::SeqCst));
        release_tx.send(()).unwrap();
        tracker.close();
        tokio::time::timeout(Duration::from_secs(5), tracker.wait())
            .await
            .expect("tracked execution must drain");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !completed.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("detached execution must finish after the timeout response");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn detached_timeout_returns_value_and_maps_panics() {
        let admission = CancellationToken::new();
        let tracker = TaskTracker::new();
        let permits = Arc::new(Semaphore::new(1));
        let ok = run_detached_with_timeout(
            &admission,
            &tracker,
            permits.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_secs(5),
            Duration::from_secs(30),
            async { 7u32 },
        )
        .await;
        assert!(matches!(ok, Ok(7)));

        let panicked = run_detached_with_timeout(
            &admission,
            &tracker,
            permits,
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_secs(5),
            Duration::from_secs(30),
            async {
                panic!("boom");
                #[allow(unreachable_code)]
                0u32
            },
        )
        .await;
        match panicked {
            Err(DetachedError::Join(err)) => assert!(err.is_panic()),
            other => panic!("expected a join error, got {other:?}"),
        }
        tracker.close();
        tracker.wait().await;
    }

    #[tokio::test]
    async fn shutdown_closes_admission_and_capacity_is_bounded() {
        let admission = CancellationToken::new();
        let tracker = TaskTracker::new();
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let busy = run_detached_with_timeout(
            &admission,
            &tracker,
            permits.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_secs(1),
            Duration::from_secs(30),
            async { 1u8 },
        )
        .await;
        assert!(matches!(busy, Err(DetachedError::Busy)));
        drop(permit);

        admission.cancel();
        let shutting_down = run_detached_with_timeout(
            &admission,
            &tracker,
            permits,
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_secs(1),
            Duration::from_secs(30),
            async { 2u8 },
        )
        .await;
        assert!(matches!(shutting_down, Err(DetachedError::ShuttingDown)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tracker_close_cannot_race_a_late_mutation_registration() {
        let admission = CancellationToken::new();
        let tracker = TaskTracker::new();
        let permits = Arc::new(Semaphore::new(1));
        let registry = Arc::new(Mutex::new(Vec::new()));
        let registration = registry.lock().await;

        let attempt = tokio::spawn({
            let admission = admission.clone();
            let tracker = tracker.clone();
            let permits = permits.clone();
            let registry = registry.clone();
            async move {
                run_detached_with_timeout(
                    &admission,
                    &tracker,
                    permits,
                    registry,
                    Duration::from_secs(1),
                    Duration::from_secs(30),
                    async { 1u8 },
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while permits.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("mutation did not reach the final registration gate");

        // This is the shutdown side of the shared critical section. The
        // waiting handler passed its first check, but cannot register after
        // cancellation and tracker close once this guard is released.
        admission.cancel();
        tracker.close();
        drop(registration);

        let result = attempt.await.expect("registration task panicked");
        assert!(matches!(result, Err(DetachedError::ShuttingDown)));
        tracker.wait().await;
        assert_eq!(tracker.len(), 0);
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

    /// A hard deadline must stop the process from admitting more mutations,
    /// but it must not cancel the in-flight database write and poison a live
    /// collection. The normal shutdown drain owns the eventual completion or
    /// crash-style abort.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_hard_deadline_closes_admission_without_cancelling_the_execution() {
        let admission = CancellationToken::new();
        let tracker = TaskTracker::new();
        let permits = Arc::new(Semaphore::new(1));
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed_in_task = completed.clone();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let result = run_detached_with_timeout(
            &admission,
            &tracker,
            permits.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_millis(20),
            Duration::from_millis(100),
            async move {
                let _ = release_rx.await;
                completed_in_task.store(true, std::sync::atomic::Ordering::SeqCst);
                1u8
            },
        )
        .await;

        // The client sees the response deadline, and the execution is still
        // running (and still holding the only permit).
        assert!(matches!(result, Err(DetachedError::Timeout)));
        assert_eq!(permits.available_permits(), 0);

        tokio::time::timeout(Duration::from_secs(5), admission.cancelled())
            .await
            .expect("the hard deadline must close admission");
        assert_eq!(
            permits.available_permits(),
            0,
            "the hard deadline must not cancel the execution or release its permit"
        );
        assert!(!completed.load(std::sync::atomic::Ordering::SeqCst));

        let rejected = run_detached_with_timeout(
            &admission,
            &tracker,
            permits.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_secs(5),
            Duration::from_secs(30),
            async { 2u8 },
        )
        .await;
        assert!(matches!(rejected, Err(DetachedError::ShuttingDown)));

        release_tx.send(()).unwrap();
        tracker.close();
        tokio::time::timeout(Duration::from_secs(5), tracker.wait())
            .await
            .expect("shutdown drain must observe the execution finish");
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(permits.available_permits(), 1);
    }

    /// Even when the hard deadline precedes the response deadline, the helper
    /// keeps polling the mutation after closing admission.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_hard_deadline_waits_for_the_mutation_to_finish() {
        let admission = CancellationToken::new();
        let tracker = TaskTracker::new();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn({
            let admission = admission.clone();
            let tracker = tracker.clone();
            async move {
                run_detached_with_timeout(
                    &admission,
                    &tracker,
                    Arc::new(Semaphore::new(1)),
                    Arc::new(Mutex::new(Vec::new())),
                    Duration::from_secs(5),
                    Duration::from_millis(20),
                    async move {
                        let _ = release_rx.await;
                        7u8
                    },
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), admission.cancelled())
            .await
            .expect("the hard deadline must close admission");
        assert!(
            !task.is_finished(),
            "closing admission must not cancel the mutation"
        );
        release_tx.send(()).unwrap();
        assert!(matches!(task.await.unwrap(), Ok(7)));
        tracker.close();
        tracker.wait().await;
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
}
