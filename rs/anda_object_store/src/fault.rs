//! Deterministic failure injection for object-store tests.
//!
//! Mutation budgets count attempted puts, deletes, copies, renames and each
//! multipart stage. Already admitted requests may finish after another request
//! triggers a simulated crash; this models operation boundaries, not disk fsync.
//! The legacy mutation log records admitted requests, including backend failures.
//! The event log distinguishes backend success, response failure and cancellation.

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream, task::AtomicWaker};
use object_store::{path::Path, *};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

/// Operation matched by a fault rule. Put also matches MultipartStart for
/// compatibility with existing rules; the other multipart stages are explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    Put,
    Get,
    Delete,
    List,
    Copy,
    Rename,
    MultipartStart,
    MultipartPart,
    MultipartComplete,
    MultipartAbort,
}
impl FaultOp {
    fn is_mutation(self) -> bool {
        !matches!(self, Self::Get | Self::List)
    }
}

#[derive(Debug, Default)]
struct Signal {
    set: AtomicBool,
    waker: AtomicWaker,
}
impl Signal {
    fn fire(&self) {
        self.set.store(true, Ordering::Release);
        self.waker.wake();
    }
    async fn wait(&self) {
        futures::future::poll_fn(|cx| {
            self.waker.register(cx.waker());
            if self.set.load(Ordering::Acquire) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await
    }
}

/// One-shot gate for one intercepted request and one waiting test/controller.
/// Drop the suspended request to test cancellation, or call release to resume it.
#[derive(Debug, Clone, Default)]
pub struct FaultGate {
    entered: Arc<Signal>,
    released: Arc<Signal>,
}
impl FaultGate {
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn wait_entered(&self) {
        self.entered.wait().await;
    }
    pub fn release(&self) {
        self.released.fire();
    }
    async fn pause(&self) {
        self.entered.fire();
        self.released.wait().await;
    }
}

/// Effect of a matching rule.
#[derive(Debug, Clone)]
pub enum FaultKind {
    /// Reject before calling the backend.
    Error,
    /// Reject this and future requests until reset.
    Crash,
    /// Persist a prefix, then report an error. Only supported for ordinary Put;
    /// on every other operation (including MultipartStart) this is Error.
    TornWrite { keep_bytes: usize },
    /// Let the backend succeed, then lose the acknowledgement.
    ErrorAfter,
    /// Suspend before invoking the backend.
    PauseBefore(FaultGate),
    /// Suspend after the backend succeeds and before returning its result.
    PauseAfter(FaultGate),
}

/// Fire on matching operations after skip matches, at most times times.
#[derive(Debug, Clone)]
pub struct FaultRule {
    pub op: FaultOp,
    /// Matches either source or destination for copy/rename.
    pub path_contains: Option<String>,
    pub skip: u64,
    pub times: u64,
    pub kind: FaultKind,
}
impl FaultRule {
    pub fn fail_once(op: FaultOp, path: impl Into<String>) -> Self {
        Self {
            op,
            path_contains: Some(path.into()),
            skip: 0,
            times: 1,
            kind: FaultKind::Error,
        }
    }
}

/// BackendSucceeded means the backend returned Ok, not that disk fsync occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOutcome {
    Attempted,
    BackendSucceeded,
    BackendFailed,
    ResponseFailed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultEvent {
    pub op: FaultOp,
    pub path: String,
    pub target: Option<String>,
    pub outcome: FaultOutcome,
}

#[derive(Debug)]
struct RuleState {
    rule: FaultRule,
    matched: u64,
    fired: u64,
}
#[derive(Debug, Default)]
struct FaultState {
    powered_off: AtomicBool,
    mutations: AtomicU64,
    crash_at: AtomicU64,
    rules: Mutex<Vec<RuleState>>,
    log: Mutex<Vec<(FaultOp, String)>>,
    events: Mutex<Vec<FaultEvent>>,
}
impl FaultState {
    fn error(&self, op: FaultOp, path: &Path, why: &str) -> Error {
        Error::Generic {
            store: "FaultStore",
            source: format!("injected fault: {why} ({op:?} {path})").into(),
        }
    }
    fn record(&self, op: FaultOp, path: &Path, target: Option<&Path>, outcome: FaultOutcome) {
        if op.is_mutation() {
            self.events
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(FaultEvent {
                    op,
                    path: path.to_string(),
                    target: target.map(ToString::to_string),
                    outcome,
                });
        }
    }
    fn intercept(
        &self,
        op: FaultOp,
        path: &Path,
        target: Option<&Path>,
    ) -> Result<Option<FaultKind>> {
        if self.powered_off.load(Ordering::Acquire) {
            return Err(self.error(op, path, "power failure"));
        }
        if op.is_mutation()
            && self.mutations.fetch_add(1, Ordering::AcqRel)
                >= self.crash_at.load(Ordering::Acquire)
        {
            self.powered_off.store(true, Ordering::Release);
            return Err(self.error(op, path, "power failure"));
        }
        let mut effect = None;
        for state in self
            .rules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter_mut()
        {
            if state.rule.op != op
                && !(state.rule.op == FaultOp::Put && op == FaultOp::MultipartStart)
            {
                continue;
            }
            if let Some(part) = &state.rule.path_contains
                && !path.as_ref().contains(part.as_str())
                && !target.is_some_and(|p| p.as_ref().contains(part.as_str()))
            {
                continue;
            }
            state.matched = state.matched.saturating_add(1);
            if state.matched <= state.rule.skip || state.fired >= state.rule.times {
                continue;
            }
            state.fired += 1;
            match &state.rule.kind {
                FaultKind::Error => return Err(self.error(op, path, "error")),
                FaultKind::Crash => {
                    self.powered_off.store(true, Ordering::Release);
                    return Err(self.error(op, path, "power failure"));
                }
                FaultKind::TornWrite { .. } if op != FaultOp::Put => {
                    return Err(self.error(op, path, "error"));
                }
                kind => {
                    effect = Some(kind.clone());
                    break;
                }
            }
        }
        if op.is_mutation() {
            self.log
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((op, path.to_string()));
        }
        Ok(effect)
    }
    fn start(
        self: &Arc<Self>,
        op: FaultOp,
        path: &Path,
        target: Option<&Path>,
    ) -> Result<Operation> {
        self.record(op, path, target, FaultOutcome::Attempted);
        match self.intercept(op, path, target) {
            Ok(kind) => Ok(Operation {
                state: self.clone(),
                op,
                path: path.clone(),
                target: target.cloned(),
                kind,
                finished: false,
            }),
            Err(err) => {
                self.record(op, path, target, FaultOutcome::ResponseFailed);
                Err(err)
            }
        }
    }
}

struct Operation {
    state: Arc<FaultState>,
    op: FaultOp,
    path: Path,
    target: Option<Path>,
    kind: Option<FaultKind>,
    finished: bool,
}
impl Operation {
    async fn run<R>(mut self, future: impl Future<Output = Result<R>>) -> Result<R> {
        if let Some(FaultKind::PauseBefore(gate)) = &self.kind {
            gate.pause().await;
        }
        let result = future.await;
        self.state.record(
            self.op,
            &self.path,
            self.target.as_ref(),
            if result.is_ok() {
                FaultOutcome::BackendSucceeded
            } else {
                FaultOutcome::BackendFailed
            },
        );
        if result.is_ok() {
            if let Some(FaultKind::PauseAfter(gate)) = &self.kind {
                gate.pause().await;
            }
            if matches!(
                self.kind,
                Some(FaultKind::ErrorAfter | FaultKind::TornWrite { .. })
            ) {
                self.finished = true;
                self.state.record(
                    self.op,
                    &self.path,
                    self.target.as_ref(),
                    FaultOutcome::ResponseFailed,
                );
                return Err(self.state.error(
                    self.op,
                    &self.path,
                    if matches!(self.kind, Some(FaultKind::TornWrite { .. })) {
                        "torn write"
                    } else {
                        "response lost after backend success"
                    },
                ));
            }
        }
        self.finished = true;
        result
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        if !self.finished {
            self.state.record(
                self.op,
                &self.path,
                self.target.as_ref(),
                FaultOutcome::Cancelled,
            );
        }
    }
}

/// Shared controller. Reset when no requests are running.
#[derive(Clone, Debug)]
pub struct FaultHandle {
    state: Arc<FaultState>,
}
impl FaultHandle {
    pub fn push_rule(&self, rule: FaultRule) {
        self.state
            .rules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(RuleState {
                rule,
                matched: 0,
                fired: 0,
            });
    }
    /// Fail after n additional attempted mutation stages, including multipart.
    pub fn crash_after_mutations(&self, n: u64) {
        self.state
            .crash_at
            .store(self.mutation_count().saturating_add(n), Ordering::Release);
    }
    pub fn mutation_count(&self) -> u64 {
        self.state.mutations.load(Ordering::Acquire)
    }
    /// Admitted requests, including those for which the backend later failed.
    pub fn mutation_log(&self) -> Vec<(FaultOp, String)> {
        self.state
            .log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn event_log(&self) -> Vec<FaultEvent> {
        self.state
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn reset(&self) {
        self.state.crash_at.store(u64::MAX, Ordering::Release);
        self.state.mutations.store(0, Ordering::Release);
        self.state
            .rules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.state
            .log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.state
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.state.powered_off.store(false, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct FaultStore<T: ObjectStore> {
    inner: Arc<T>,
    state: Arc<FaultState>,
}
impl<T: ObjectStore> FaultStore<T> {
    pub fn wrap(inner: T) -> (Self, FaultHandle) {
        let state = Arc::new(FaultState {
            crash_at: AtomicU64::new(u64::MAX),
            ..Default::default()
        });
        (
            Self {
                inner: Arc::new(inner),
                state: state.clone(),
            },
            FaultHandle { state },
        )
    }
    pub fn inner(&self) -> &T {
        &self.inner
    }
    fn listing(
        &self,
        prefix: Option<&Path>,
        stream: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let operation = self
            .state
            .start(FaultOp::List, &prefix.cloned().unwrap_or_default(), None);
        futures::stream::once(async move { operation?.run(async { Ok(stream) }).await })
            .try_flatten()
            .boxed()
    }
}
impl<T: ObjectStore> std::fmt::Display for FaultStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}
#[async_trait]
impl<T: ObjectStore> ObjectStore for FaultStore<T> {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let op = self.state.start(FaultOp::Put, path, None)?;
        let payload = if let Some(FaultKind::TornWrite { keep_bytes }) = op.kind {
            let mut bytes = Vec::with_capacity(keep_bytes.min(payload.content_length()));
            for segment in &payload {
                let remaining = keep_bytes.saturating_sub(bytes.len());
                if remaining == 0 {
                    break;
                }
                bytes.extend_from_slice(&segment[..remaining.min(segment.len())]);
            }
            Bytes::from(bytes).into()
        } else {
            payload
        };
        op.run(self.inner.put_opts(path, payload, opts)).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let op = self.state.start(FaultOp::MultipartStart, path, None)?;
        let inner = op.run(self.inner.put_multipart_opts(path, opts)).await?;
        Ok(Box::new(FaultUploader {
            location: path.clone(),
            state: self.state.clone(),
            inner,
        }))
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        self.state
            .start(FaultOp::Get, path, None)?
            .run(self.inner.get_opts(path, opts))
            .await
    }
    async fn get_ranges(&self, path: &Path, ranges: &[std::ops::Range<u64>]) -> Result<Vec<Bytes>> {
        self.state
            .start(FaultOp::Get, path, None)?
            .run(self.inner.get_ranges(path, ranges))
            .await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let state = self.state.clone();
        locations
            .map(move |path| {
                let inner = inner.clone();
                let state = state.clone();
                async move {
                    let path = path?;
                    state
                        .start(FaultOp::Delete, &path, None)?
                        .run(inner.delete(&path))
                        .await?;
                    Ok(path)
                }
            })
            .buffered(10)
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.listing(prefix, self.inner.list(prefix))
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.listing(prefix, self.inner.list_with_offset(prefix, offset))
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.state
            .start(FaultOp::List, &prefix.cloned().unwrap_or_default(), None)?
            .run(self.inner.list_with_delimiter(prefix))
            .await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.state
            .start(FaultOp::Copy, from, Some(to))?
            .run(self.inner.copy_opts(from, to, opts))
            .await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> Result<()> {
        self.state
            .start(FaultOp::Rename, from, Some(to))?
            .run(self.inner.rename_opts(from, to, opts))
            .await
    }
}

#[derive(Debug)]
struct FaultUploader {
    location: Path,
    state: Arc<FaultState>,
    inner: Box<dyn MultipartUpload>,
}
#[async_trait]
impl MultipartUpload for FaultUploader {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let op = match self
            .state
            .start(FaultOp::MultipartPart, &self.location, None)
        {
            Ok(op) => op,
            Err(err) => return Box::pin(async { Err(err) }),
        };
        // Reserve the backend part number in invocation order. PauseBefore gates
        // future polling; an eager backend may already have buffered the part,
        // but publishing the object still goes through MultipartComplete.
        let future = self.inner.put_part(payload);
        Box::pin(op.run(future))
    }
    async fn complete(&mut self) -> Result<PutResult> {
        self.state
            .start(FaultOp::MultipartComplete, &self.location, None)?
            .run(self.inner.complete())
            .await
    }
    async fn abort(&mut self) -> Result<()> {
        self.state
            .start(FaultOp::MultipartAbort, &self.location, None)?
            .run(self.inner.abort())
            .await
    }
}

#[cfg(test)]
mod tests;
