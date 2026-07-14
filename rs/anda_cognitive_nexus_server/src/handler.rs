use anda_kip::{KipError, Request, Response};
use axum::{
    Json,
    extract::{Request as HttpRequest, State},
    http::{StatusCode, header},
    middleware::Next,
    response::IntoResponse,
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
    pub api_key: Option<String>,
    /// Per-request **response** deadline for `/kip`. A KIP execution that
    /// exceeds it gets a 408 response, but the already-started execution
    /// finishes in the background (see [`run_detached_with_timeout`]).
    pub request_timeout: Duration,
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
    /// The deadline elapsed; the detached task keeps running to completion.
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

/// Runs `fut` on a detached task with a response deadline.
///
/// KML execution mutates the graph in multiple steps and has no rollback
/// log: cancelling it mid-flight — which is exactly what dropping the
/// future inside `tokio::time::timeout` does — can leave half-written
/// graph state (e.g. an `UPSERT` with only a prefix of its blocks
/// applied). Spawning first means a timeout only abandons the *response*;
/// the execution itself runs to completion in the background.
pub(crate) async fn run_detached_with_timeout<T, F>(
    admission: &CancellationToken,
    tracker: &TaskTracker,
    permits: Arc<Semaphore>,
    aborts: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    deadline: Duration,
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
        let _permit = permit;
        fut.await
    });
    handles.retain(|handle| !handle.is_finished());
    handles.push(task.abort_handle());
    drop(handles);
    match tokio::time::timeout(deadline, task).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(join_error)) => Err(DetachedError::Join(join_error)),
        // Dropping the JoinHandle detaches the task: it keeps running.
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
            Ok(Json(response))
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
    use axum::{
        Router,
        body::{Body, Bytes},
        http::{HeaderValue, Request as AxumRequest},
        middleware, routing,
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

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
}
