use anda_kip::{ErrorObject, KipError, Request, Response};
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

use crate::nexus::{ListLogParams, ListLogsError, Nexus};

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
    /// stuck or pathologically slow execution eventually returns its bounded
    /// mutation permit; see [`run_detached_with_timeout`].
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
    /// The response deadline elapsed; the detached task keeps running until
    /// it finishes or reaches its own hard deadline.
    Timeout,
    /// The detached task exceeded its hard deadline and was abandoned.
    Abandoned,
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
/// `hard_deadline` bounds that background execution. Without it the bounded
/// permit lives as long as the execution does, so a batch of expensive
/// requests that all time out keeps its permits forever and every later
/// request is refused with "server mutation capacity is exhausted" — until
/// the process exits, since shutdown then also has to escalate to an abort.
/// Reaching the hard deadline is deliberately crash-equivalent (exactly what
/// the shutdown abort path already does): the graph may be left partially
/// written and recovers on reopen. It must therefore be set well above the
/// response deadline so a normal slow request is never abandoned.
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
        // waiter gives up, so the hard deadline is what actually reclaims
        // capacity from a runaway execution.
        let _permit = permit;
        match tokio::time::timeout(hard_deadline, fut).await {
            Ok(value) => Some(value),
            Err(_) => {
                log::error!(
                    action = "run_detached_with_timeout",
                    hard_deadline_secs = hard_deadline.as_secs();
                    "detached execution exceeded its hard deadline and was abandoned; \
                     the graph may be left partially written and recovers on reopen",
                );
                None
            }
        }
    });
    handles.retain(|handle| !handle.is_finished());
    handles.push(task.abort_handle());
    drop(handles);
    match tokio::time::timeout(deadline, task).await {
        Ok(Ok(Some(value))) => Ok(value),
        Ok(Ok(None)) => Err(DetachedError::Abandoned),
        Ok(Err(join_error)) => Err(DetachedError::Join(join_error)),
        // Dropping the JoinHandle detaches the task: it keeps running until
        // it finishes or hits `hard_deadline`.
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

/// Maps a KIP error response onto an HTTP status.
///
/// A failed KIP execution used to be returned as HTTP 200, so a broken KML
/// mutation or an internal graph error was indistinguishable from success to
/// load balancers, retry policies, uptime probes, and 5xx alerting — while a
/// malformed `params` on the same endpoint already produced 400 and a bad key
/// 401. The JSON body is unchanged, so existing clients keep parsing it.
///
/// The mapping follows the KIP standard code ranges (1xxx syntax, 2xxx
/// schema, 3xxx logic/data, 4xxx system). An unrecognized code is treated as
/// internal: a code the server does not know cannot be proven client-caused.
fn kip_error_status(error: &ErrorObject) -> StatusCode {
    match error.code.as_str() {
        // 1xxx / 2xxx: the submitted KQL/KML is malformed or violates the
        // schema. 3001 references an undefined variable or handle.
        "KIP_1001" | "KIP_1002" | "KIP_2001" | "KIP_2002" | "KIP_2003" | "KIP_3001" => {
            StatusCode::BAD_REQUEST
        }
        "KIP_3002" => StatusCode::NOT_FOUND,
        // DuplicateExists / VersionConflict: retryable after re-reading.
        "KIP_3003" | "KIP_3005" => StatusCode::CONFLICT,
        // ImmutableTarget: modifying protected system nodes is prohibited.
        "KIP_3004" => StatusCode::FORBIDDEN,
        "KIP_4001" => StatusCode::REQUEST_TIMEOUT,
        // ResourceExhausted: the recovery hint is client-side pagination.
        "KIP_4002" => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// The HTTP status for a KIP response, logging the ones that count as server
/// failures so a 5xx in the access log is also visible in the service log.
fn kip_status(response: &Response) -> StatusCode {
    match response {
        Response::Ok { .. } => StatusCode::OK,
        Response::Err { error, .. } => {
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

/// POST /kip
///
/// Authentication runs in the router layer ([`build_router`]), before this
/// handler and before the `Json` extractor parses the body.
pub async fn post_kip(
    State(app): State<AppState>,
    Json(req): Json<JsonRpcRequest>,
) -> Result<Json<Response>, (StatusCode, Json<Response>)> {
    if app.admission.is_cancelled() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Response::err("server is shutting down".to_string())),
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

            // Detached execution: on timeout the client gets a 408, but the
            // started KIP execution (possibly a mid-write KML mutation)
            // continues to completion instead of being cancelled halfway.
            let nexus = app.nexus.clone();
            let response = run_detached_with_timeout(
                &app.admission,
                &app.mutation_tasks,
                app.mutation_permits.clone(),
                app.mutation_aborts.clone(),
                app.request_timeout,
                app.execution_timeout,
                async move { nexus.execute_kip(params).await },
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
                    timeout_error(
                        "request processing exceeded the configured timeout; \
                         the started KIP execution continues on the server",
                    )
                }
                DetachedError::Abandoned => {
                    log::error!(
                        action = "post_kip",
                        method = "execute_kip",
                        execution_timeout_secs = app.execution_timeout.as_secs();
                        "KIP execution exceeded the hard execution deadline and was abandoned",
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(Response::err(KipError::execution_timeout(
                            "KIP execution exceeded the maximum execution time and was abandoned"
                                .to_string(),
                        ))),
                    )
                }
                DetachedError::Join(join_error) => {
                    log::error!(
                        action = "post_kip",
                        method = "execute_kip";
                        "KIP execution task failed: {join_error:?}",
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(Response::err(
                            "KIP execution failed unexpectedly".to_string(),
                        )),
                    )
                }
                DetachedError::ShuttingDown => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(Response::err("server is shutting down".to_string())),
                ),
                DetachedError::Busy => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(Response::err(
                        "server mutation capacity is exhausted".to_string(),
                    )),
                ),
            })?;
            match kip_status(&response) {
                StatusCode::OK => Ok(Json(response)),
                status => Err((status, Json(response))),
            }
        }
        "list_logs" => {
            let params: ListLogParams = serde_json::from_value(req.params).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(Response::err(format!("invalid parameters: {}", e))),
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
                CancelSafeError::ShuttingDown => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(Response::err("server is shutting down".to_string())),
                ),
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
        Json(Response::err(KipError::execution_timeout(message.into()))),
    )
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
            Json(Response::err("invalid API key".to_string())),
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
        let nexus = Nexus::connect(Arc::new(db), "uuc56-gyb".to_string(), 8 * 1024)
            .await
            .unwrap();
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
        assert_eq!(body["error"]["code"], "KIP_4001");
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

    /// A timed-out execution used to keep its bounded permit until it
    /// finished — with nothing to stop it, a batch of expensive requests
    /// exhausted mutation capacity for the rest of the process's life. The
    /// hard deadline must actually return the permit.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_hard_deadline_reclaims_a_runaway_executions_permit() {
        let admission = CancellationToken::new();
        let tracker = TaskTracker::new();
        let permits = Arc::new(Semaphore::new(1));

        let result = run_detached_with_timeout(
            &admission,
            &tracker,
            permits.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_millis(20),
            Duration::from_millis(100),
            std::future::pending::<u8>(),
        )
        .await;

        // The client sees the response deadline, and the execution is still
        // running (and still holding the only permit).
        assert!(matches!(result, Err(DetachedError::Timeout)));
        assert_eq!(permits.available_permits(), 0);

        tokio::time::timeout(Duration::from_secs(5), async {
            while permits.available_permits() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the hard deadline must return the mutation permit");

        // Capacity is genuinely available again instead of answering 503.
        let admitted = run_detached_with_timeout(
            &admission,
            &tracker,
            permits,
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_secs(5),
            Duration::from_secs(30),
            async { 1u8 },
        )
        .await;
        assert!(matches!(admitted, Ok(1)));

        tracker.close();
        tokio::time::timeout(Duration::from_secs(5), tracker.wait())
            .await
            .expect("abandoned execution must have ended");
    }

    /// A response whose deadline is shorter than the hard deadline can still
    /// observe an abandoned execution; it must not be reported as success.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_abandoned_execution_is_not_reported_as_success() {
        let admission = CancellationToken::new();
        let tracker = TaskTracker::new();
        let result = run_detached_with_timeout(
            &admission,
            &tracker,
            Arc::new(Semaphore::new(1)),
            Arc::new(Mutex::new(Vec::new())),
            Duration::from_secs(5),
            Duration::from_millis(20),
            std::future::pending::<u8>(),
        )
        .await;
        assert!(matches!(result, Err(DetachedError::Abandoned)));
        tracker.close();
        tracker.wait().await;
    }

    /// Every KIP error class maps to a status a load balancer, retry policy,
    /// or 5xx alert can act on — client-caused to 4xx, internal to 5xx.
    #[test]
    fn kip_error_classes_map_to_meaningful_statuses() {
        let status = |code: KipErrorCode| {
            kip_error_status(&ErrorObject::new(code.code(), "boom".to_string()))
        };

        for code in [
            KipErrorCode::InvalidSyntax,
            KipErrorCode::InvalidIdentifier,
            KipErrorCode::TypeMismatch,
            KipErrorCode::ConstraintViolation,
            KipErrorCode::InvalidValueType,
            KipErrorCode::ReferenceError,
            KipErrorCode::ResourceExhausted,
        ] {
            assert_eq!(
                status(code),
                StatusCode::BAD_REQUEST,
                "code: {}",
                code.code()
            );
        }
        assert_eq!(status(KipErrorCode::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(status(KipErrorCode::DuplicateExists), StatusCode::CONFLICT);
        assert_eq!(status(KipErrorCode::VersionConflict), StatusCode::CONFLICT);
        assert_eq!(status(KipErrorCode::ImmutableTarget), StatusCode::FORBIDDEN);
        assert_eq!(
            status(KipErrorCode::ExecutionTimeout),
            StatusCode::REQUEST_TIMEOUT
        );
        assert_eq!(
            status(KipErrorCode::InternalError),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        // An unknown code cannot be proven client-caused.
        assert_eq!(
            kip_error_status(&ErrorObject::new("KIP_9999", "boom".to_string())),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /// A failed KIP execution must not answer HTTP 200; the JSON body shape
    /// stays exactly the same so existing clients keep parsing it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_kip_execution_is_not_http_200() {
        let app = test_app(None).await;

        let (status, body) = post_json(
            &app,
            r#"{"method":"execute_kip","params":{"command":"THIS IS NOT KIP"}}"#,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
        assert!(
            body["error"]["code"].as_str().unwrap().starts_with("KIP_"),
            "body: {body}"
        );
        assert!(body["error"]["message"].is_string(), "body: {body}");

        // A successful execution still answers 200 with the same shape.
        let (status, body) = post_json(
            &app,
            r#"{"method":"execute_kip","params":{"command":"DESCRIBE PRIMER"}}"#,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert!(body.get("error").is_none(), "body: {body}");
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
