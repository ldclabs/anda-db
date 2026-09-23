//! Admission, detached execution and bounded shutdown of non-cancel-safe work.
use std::{io, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    task::{AbortHandle, JoinHandle},
    time::Instant,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const FORCED_ABORT_JOIN_TIMEOUT: Duration = Duration::from_secs(1);
pub const EXECUTION_TIMEOUT_FACTOR: u32 = 4;

#[derive(Clone)]
pub struct ExecutionManager {
    pub admission: CancellationToken,
    tracker: TaskTracker,
    permits: Arc<Semaphore>,
    // Registration and tracker close share this gate: a tracker alone permits
    // late spawns even after close/wait completes.
    aborts: Arc<Mutex<Vec<AbortHandle>>>,
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
    /// The bounded request executor has no free capacity.
    Busy,
}

impl ExecutionManager {
    pub fn new(capacity: usize) -> Self {
        Self {
            admission: CancellationToken::new(),
            tracker: TaskTracker::new(),
            permits: Arc::new(Semaphore::new(capacity.max(1))),
            aborts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Reserve before buffering and parsing; keep this permit until the
    /// admitted request, including any detached execution, has finished.
    pub fn reserve(&self) -> Result<OwnedSemaphorePermit, DetachedError> {
        if self.admission.is_cancelled() {
            return Err(DetachedError::ShuttingDown);
        }
        self.permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| DetachedError::Busy)
    }

    /// A response timeout detaches the waiter, never the database write. The
    /// hard deadline requests process shutdown and keeps polling the write.
    pub async fn run<T, F>(
        &self,
        permit: OwnedSemaphorePermit,
        deadline: Duration,
        hard_deadline: Duration,
        fut: F,
    ) -> Result<T, DetachedError>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let mut handles = self.aborts.lock().await;
        if self.admission.is_cancelled() {
            return Err(DetachedError::ShuttingDown);
        }
        let hard_shutdown = self.admission.clone();
        let task = self.tracker.spawn(async move {
            let _permit = permit;
            tokio::pin!(fut);
            tokio::select! {
                value = &mut fut => value,
                _ = tokio::time::sleep(hard_deadline) => {
                    log::error!("KIP execution exceeded its hard deadline; initiating shutdown");
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
            Ok(Err(err)) => Err(DetachedError::Join(err)),
            Err(_) => Err(DetachedError::Timeout),
        }
    }

    pub async fn drain(&self, deadline: Instant) -> io::Result<()> {
        self.admission.cancel();
        {
            let _registration = self.aborts.lock().await;
            self.tracker.close();
        }
        if tokio::time::timeout_at(deadline, self.tracker.wait())
            .await
            .is_ok()
        {
            return Ok(());
        }
        for handle in self.aborts.lock().await.iter() {
            handle.abort();
        }
        let _ = tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, self.tracker.wait()).await;
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "KIP execution drain deadline exceeded",
        ))
    }
}

/// Await a task within the shared shutdown budget. A forced abort is always
/// an error, so callers cannot report a crash-style shutdown as success.
pub async fn finish_task<T>(
    task: &mut JoinHandle<T>,
    deadline: Instant,
    name: &str,
) -> io::Result<T> {
    match tokio::time::timeout_at(deadline, &mut *task).await {
        Ok(joined) => joined.map_err(|err| io::Error::other(format!("{name} task failed: {err}"))),
        Err(_) => {
            abort_task(task).await;
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{name} drain deadline exceeded"),
            ))
        }
    }
}

pub async fn abort_task<T>(task: &mut JoinHandle<T>) {
    task.abort();
    if tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, task)
        .await
        .is_err()
    {
        log::error!("aborted task did not terminate before cleanup deadline");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn run_detached_with_timeout<T, F>(
        admission: &CancellationToken,
        tracker: &TaskTracker,
        permits: Arc<Semaphore>,
        aborts: Arc<Mutex<Vec<AbortHandle>>>,
        deadline: Duration,
        hard_deadline: Duration,
        fut: F,
    ) -> Result<T, DetachedError>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let manager = ExecutionManager {
            admission: admission.clone(),
            tracker: tracker.clone(),
            permits,
            aborts,
        };
        let permit = manager.reserve()?;
        manager.run(permit, deadline, hard_deadline, fut).await
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
}
