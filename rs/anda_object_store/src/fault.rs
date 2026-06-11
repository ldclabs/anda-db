//! Fault-injection wrapper for any [`ObjectStore`], used for crash-consistency
//! and chaos testing.
//!
//! [`FaultStore`] forwards every operation to the wrapped store while a shared
//! [`FaultHandle`] lets tests inject failures at precise points:
//!
//! - **Power failure** ([`FaultHandle::crash_after_mutations`]): the first `n`
//!   mutating operations (put / delete / copy / rename / multipart) succeed,
//!   then the store "loses power" — every subsequent operation fails until
//!   [`FaultHandle::reset`] is called. Iterating `n` over the mutation count of
//!   a clean run simulates a crash at every possible point of a workload,
//!   which is the standard crash-consistency model for object storage: each
//!   individual put is atomic, but a sequence of puts can be interrupted
//!   anywhere.
//! - **Targeted faults** ([`FaultRule`]): fail the Nth operation whose path
//!   contains a given substring, or tear a write so that only a prefix of the
//!   payload reaches the backend (simulating non-atomic backends).
//!
//! The handle also records a log of all mutations that reached the wrapped
//! store, so tests can assert on write ordering (e.g. "the ids bitmap is
//! persisted after the metadata object").
//!
//! This module is intended for tests and chaos engineering. It has no effect
//! on the data path unless faults are injected.

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use object_store::{path::Path, *};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

/// The operation categories a [`FaultRule`] can match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    /// `put_opts` and `put_multipart_opts`.
    Put,
    /// `get_opts` and `get_ranges` (also covers `head`, which `object_store`
    /// routes through `get_opts`).
    Get,
    /// Each path processed by `delete_stream` (also covers `delete`).
    Delete,
    /// `list`, `list_with_offset` and `list_with_delimiter`.
    List,
    /// `copy_opts`.
    Copy,
    /// `rename_opts`.
    Rename,
}

impl FaultOp {
    fn is_mutation(self) -> bool {
        matches!(
            self,
            FaultOp::Put | FaultOp::Delete | FaultOp::Copy | FaultOp::Rename
        )
    }
}

/// What happens when a [`FaultRule`] fires.
#[derive(Debug, Clone)]
pub enum FaultKind {
    /// The operation fails and nothing reaches the wrapped store.
    Error,
    /// The store loses power: this operation and every following one fail
    /// until [`FaultHandle::reset`].
    Crash,
    /// Only the first `keep_bytes` bytes of the payload are written, then the
    /// operation reports failure. Applies to `put_opts` only; for other
    /// operations it behaves like [`FaultKind::Error`].
    TornWrite {
        /// Number of payload bytes that reach the wrapped store.
        keep_bytes: usize,
    },
}

/// A targeted fault: fires on operations matching `op` and `path_contains`,
/// after skipping the first `skip` matches, for at most `times` occurrences.
#[derive(Debug, Clone)]
pub struct FaultRule {
    /// Operation category to match.
    pub op: FaultOp,
    /// Substring the object path must contain; `None` matches every path.
    pub path_contains: Option<String>,
    /// Number of matching operations to let through before firing.
    pub skip: u64,
    /// Number of matches to fire on once active.
    pub times: u64,
    /// The fault to inject.
    pub kind: FaultKind,
}

impl FaultRule {
    /// A rule that fails the first operation of `op` whose path contains `path`.
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

#[derive(Debug)]
struct RuleState {
    rule: FaultRule,
    matched: u64,
    fired: u64,
}

#[derive(Debug, Default)]
struct FaultState {
    /// `true` after a simulated power failure: every operation fails.
    powered_off: AtomicBool,
    /// Count of mutating operations attempted so far.
    mutations: AtomicU64,
    /// Mutation index at which to simulate a power failure (`u64::MAX` = never).
    crash_at: AtomicU64,
    rules: Mutex<Vec<RuleState>>,
    /// Mutations that fully reached the wrapped store, in order.
    log: Mutex<Vec<(FaultOp, String)>>,
}

impl FaultState {
    fn injected(&self, op: FaultOp, path: &Path, reason: &str) -> Error {
        Error::Generic {
            store: "FaultStore",
            source: format!("injected fault: {reason} ({op:?} {path})").into(),
        }
    }

    /// Checks faults for one operation. Returns `Ok(None)` to proceed,
    /// `Ok(Some(kind))` for faults the caller must apply (torn writes), or an
    /// error for injected failures.
    fn intercept(&self, op: FaultOp, path: &Path) -> Result<Option<FaultKind>> {
        if self.powered_off.load(Ordering::Acquire) {
            return Err(self.injected(op, path, "power failure"));
        }

        if op.is_mutation() {
            let n = self.mutations.fetch_add(1, Ordering::AcqRel);
            if n >= self.crash_at.load(Ordering::Acquire) {
                self.powered_off.store(true, Ordering::Release);
                return Err(self.injected(op, path, "power failure"));
            }
        }

        let mut rules = self.rules.lock().expect("FaultStore rules lock poisoned");
        for rs in rules.iter_mut() {
            if rs.rule.op != op {
                continue;
            }
            if let Some(substr) = &rs.rule.path_contains
                && !path.as_ref().contains(substr.as_str())
            {
                continue;
            }
            rs.matched += 1;
            if rs.matched > rs.rule.skip && rs.fired < rs.rule.times {
                rs.fired += 1;
                match rs.rule.kind.clone() {
                    FaultKind::Error => return Err(self.injected(op, path, "error")),
                    FaultKind::Crash => {
                        self.powered_off.store(true, Ordering::Release);
                        return Err(self.injected(op, path, "power failure"));
                    }
                    kind @ FaultKind::TornWrite { .. } => {
                        if op == FaultOp::Put {
                            return Ok(Some(kind));
                        }
                        return Err(self.injected(op, path, "error"));
                    }
                }
            }
        }
        drop(rules);

        if op.is_mutation() {
            self.log
                .lock()
                .expect("FaultStore log lock poisoned")
                .push((op, path.to_string()));
        }
        Ok(None)
    }
}

/// Control handle for a [`FaultStore`]; clonable and shareable across tasks.
#[derive(Clone, Debug)]
pub struct FaultHandle {
    state: Arc<FaultState>,
}

impl FaultHandle {
    /// Injects a targeted fault rule.
    pub fn push_rule(&self, rule: FaultRule) {
        self.state
            .rules
            .lock()
            .expect("FaultStore rules lock poisoned")
            .push(RuleState {
                rule,
                matched: 0,
                fired: 0,
            });
    }

    /// Simulates a power failure after `n` more successful mutations,
    /// counted from the current mutation count.
    pub fn crash_after_mutations(&self, n: u64) {
        let base = self.state.mutations.load(Ordering::Acquire);
        self.state
            .crash_at
            .store(base.saturating_add(n), Ordering::Release);
    }

    /// Number of mutating operations attempted so far.
    pub fn mutation_count(&self) -> u64 {
        self.state.mutations.load(Ordering::Acquire)
    }

    /// Mutations that fully reached the wrapped store, in order.
    pub fn mutation_log(&self) -> Vec<(FaultOp, String)> {
        self.state
            .log
            .lock()
            .expect("FaultStore log lock poisoned")
            .clone()
    }

    /// Clears all faults and revives the store ("reboot"), keeping the
    /// wrapped store's data intact. Also clears the mutation log and counter.
    pub fn reset(&self) {
        self.state.powered_off.store(false, Ordering::Release);
        self.state.crash_at.store(u64::MAX, Ordering::Release);
        self.state.mutations.store(0, Ordering::Release);
        self.state
            .rules
            .lock()
            .expect("FaultStore rules lock poisoned")
            .clear();
        self.state
            .log
            .lock()
            .expect("FaultStore log lock poisoned")
            .clear();
    }
}

/// An [`ObjectStore`] wrapper that injects faults controlled by a [`FaultHandle`].
#[derive(Debug)]
pub struct FaultStore<T: ObjectStore> {
    inner: Arc<T>,
    state: Arc<FaultState>,
}

impl<T: ObjectStore> FaultStore<T> {
    /// Wraps `inner`, returning the store and its control handle.
    pub fn wrap(inner: T) -> (Self, FaultHandle) {
        let state = Arc::new(FaultState {
            crash_at: AtomicU64::new(u64::MAX),
            ..Default::default()
        });
        let handle = FaultHandle {
            state: state.clone(),
        };
        (
            Self {
                inner: Arc::new(inner),
                state,
            },
            handle,
        )
    }

    /// Returns the wrapped store, e.g. to corrupt objects directly in tests.
    pub fn inner(&self) -> &T {
        &self.inner
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
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        match self.state.intercept(FaultOp::Put, location)? {
            None => self.inner.put_opts(location, payload, opts).await,
            Some(FaultKind::TornWrite { keep_bytes }) => {
                let mut buf = Vec::with_capacity(keep_bytes.min(payload.content_length()));
                'fill: for segment in payload.iter() {
                    for byte in segment {
                        if buf.len() >= keep_bytes {
                            break 'fill;
                        }
                        buf.push(*byte);
                    }
                }
                let _ = self
                    .inner
                    .put_opts(location, Bytes::from(buf).into(), opts)
                    .await;
                Err(self.state.injected(FaultOp::Put, location, "torn write"))
            }
            Some(_) => unreachable!("intercept only returns TornWrite"),
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.state.intercept(FaultOp::Put, location)?;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(FaultUploader {
            location: location.clone(),
            state: self.state.clone(),
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.state.intercept(FaultOp::Get, location)?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<Bytes>> {
        self.state.intercept(FaultOp::Get, location)?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let state = self.state.clone();
        let checked = locations
            .map(move |location| {
                let location = location?;
                state.intercept(FaultOp::Delete, &location)?;
                Ok(location)
            })
            .boxed();
        self.inner.delete_stream(checked)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        if let Err(err) = self
            .state
            .intercept(FaultOp::List, &prefix.cloned().unwrap_or_default())
        {
            return futures::stream::once(async move { Err(err) }).boxed();
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        if let Err(err) = self
            .state
            .intercept(FaultOp::List, &prefix.cloned().unwrap_or_default())
        {
            return futures::stream::once(async move { Err(err) }).boxed();
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.state
            .intercept(FaultOp::List, &prefix.cloned().unwrap_or_default())?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.state.intercept(FaultOp::Copy, from)?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.state.intercept(FaultOp::Rename, from)?;
        self.inner.rename_opts(from, to, options).await
    }
}

/// Multipart upload wrapper that respects a simulated power failure.
#[derive(Debug)]
struct FaultUploader {
    location: Path,
    state: Arc<FaultState>,
    inner: Box<dyn MultipartUpload>,
}

#[async_trait]
impl MultipartUpload for FaultUploader {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        if self.state.powered_off.load(Ordering::Acquire) {
            let err = self
                .state
                .injected(FaultOp::Put, &self.location, "power failure");
            return Box::pin(async move { Err(err) });
        }
        self.inner.put_part(payload)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.state.powered_off.load(Ordering::Acquire) {
            return Err(self
                .state
                .injected(FaultOp::Put, &self.location, "power failure"));
        }
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn payload(data: &'static [u8]) -> PutPayload {
        Bytes::from_static(data).into()
    }

    #[tokio::test]
    async fn forwards_when_no_faults() {
        let (store, handle) = FaultStore::wrap(InMemory::new());
        let path = Path::from("a/b");
        store.put(&path, payload(b"hello")).await.unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, Bytes::from_static(b"hello"));
        assert_eq!(handle.mutation_count(), 1);
        assert_eq!(
            handle.mutation_log(),
            vec![(FaultOp::Put, "a/b".to_string())]
        );
    }

    #[tokio::test]
    async fn crash_after_mutations_powers_off_everything() {
        let (store, handle) = FaultStore::wrap(InMemory::new());
        handle.crash_after_mutations(2);

        store.put(&Path::from("1"), payload(b"x")).await.unwrap();
        store.put(&Path::from("2"), payload(b"x")).await.unwrap();
        // Third mutation hits the power failure.
        assert!(store.put(&Path::from("3"), payload(b"x")).await.is_err());
        // Reads are dead too until reset.
        assert!(store.get(&Path::from("1")).await.is_err());
        assert!(store.delete(&Path::from("1")).await.is_err());

        handle.reset();
        let got = store
            .get(&Path::from("1"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(got, Bytes::from_static(b"x"));
        // Object "3" never made it.
        assert!(matches!(
            store.get(&Path::from("3")).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn crash_after_is_relative_to_current_count() {
        let (store, handle) = FaultStore::wrap(InMemory::new());
        store.put(&Path::from("1"), payload(b"x")).await.unwrap();
        handle.crash_after_mutations(1);
        store.put(&Path::from("2"), payload(b"x")).await.unwrap();
        assert!(store.put(&Path::from("3"), payload(b"x")).await.is_err());
    }

    #[tokio::test]
    async fn targeted_rule_fails_nth_matching_put() {
        let (store, handle) = FaultStore::wrap(InMemory::new());
        handle.push_rule(FaultRule {
            op: FaultOp::Put,
            path_contains: Some("meta".to_string()),
            skip: 1,
            times: 1,
            kind: FaultKind::Error,
        });

        // First matching put passes (skip = 1).
        store
            .put(&Path::from("x/meta"), payload(b"a"))
            .await
            .unwrap();
        // Non-matching paths are unaffected.
        store
            .put(&Path::from("x/data"), payload(b"b"))
            .await
            .unwrap();
        // Second matching put fails once.
        assert!(
            store
                .put(&Path::from("y/meta"), payload(b"c"))
                .await
                .is_err()
        );
        // Rule exhausted: passes again.
        store
            .put(&Path::from("y/meta"), payload(b"d"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn torn_write_persists_prefix_and_reports_failure() {
        let (store, handle) = FaultStore::wrap(InMemory::new());
        handle.push_rule(FaultRule {
            op: FaultOp::Put,
            path_contains: Some("torn".to_string()),
            skip: 0,
            times: 1,
            kind: FaultKind::TornWrite { keep_bytes: 3 },
        });

        let path = Path::from("torn");
        assert!(store.put(&path, payload(b"hello world")).await.is_err());
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, Bytes::from_static(b"hel"));
    }

    #[tokio::test]
    async fn delete_stream_and_list_respect_power_failure() {
        let (store, handle) = FaultStore::wrap(InMemory::new());
        store.put(&Path::from("a"), payload(b"1")).await.unwrap();
        handle.crash_after_mutations(0);

        assert!(store.delete(&Path::from("a")).await.is_err());
        let listed: Vec<_> = store.list(None).collect().await;
        assert!(listed.iter().any(|r| r.is_err()));
        assert!(store.list_with_delimiter(None).await.is_err());

        handle.reset();
        // Data survived the failed delete.
        assert!(store.get(&Path::from("a")).await.is_ok());
    }

    #[tokio::test]
    async fn crash_rule_kind_powers_off() {
        let (store, handle) = FaultStore::wrap(InMemory::new());
        handle.push_rule(FaultRule {
            op: FaultOp::Put,
            path_contains: Some("ids".to_string()),
            skip: 0,
            times: 1,
            kind: FaultKind::Crash,
        });

        store.put(&Path::from("meta"), payload(b"m")).await.unwrap();
        assert!(
            store
                .put(&Path::from("col/ids"), payload(b"i"))
                .await
                .is_err()
        );
        // Everything is dead now.
        assert!(
            store
                .put(&Path::from("other"), payload(b"o"))
                .await
                .is_err()
        );
        assert!(store.get(&Path::from("meta")).await.is_err());
    }
}
