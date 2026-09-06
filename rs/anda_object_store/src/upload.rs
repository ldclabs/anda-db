//! Shared multipart lifecycle and bounded ciphertext-part assembly.

use crate::{
    limits::limit_error,
    sidecar::{InFlightGuard, SidecarMeta, SidecarStore},
};
use bytes::Bytes;
use object_store::{
    Extensions, MultipartUpload, ObjectStore, PutPayload, PutResult, Result, UploadPart, path::Path,
};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

pub(crate) const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Receiving,
    Finalizing,
    Ready,
    Complete,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbortAction {
    AbortInner,
    DeleteMaterialized,
    ResolveFailed,
    Done,
}

#[derive(Default)]
struct Parts {
    pending: AtomicUsize,
    failed: AtomicBool,
}

struct PartGuard {
    parts: Arc<Parts>,
    succeeded: bool,
}
impl Drop for PartGuard {
    fn drop(&mut self) {
        if !self.succeeded {
            self.parts.failed.store(true, Ordering::Release);
        }
        self.parts.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct Lifecycle {
    pub(crate) phase: Phase,
    pub(crate) result: Option<PutResult>,
    flight: Option<InFlightGuard>,
    parts: Arc<Parts>,
    name: &'static str,
}

impl Lifecycle {
    pub(crate) fn new(flight: InFlightGuard, name: &'static str) -> Self {
        Self {
            phase: Phase::Receiving,
            result: None,
            flight: Some(flight),
            parts: Arc::default(),
            name,
        }
    }
    pub(crate) fn receiving(&self) -> Result<()> {
        if self.phase != Phase::Receiving || self.parts.failed.load(Ordering::Acquire) {
            return Err(limit_error(
                self.name,
                "multipart upload is no longer accepting parts",
            ));
        }
        Ok(())
    }
    pub(crate) fn fail(&mut self) {
        self.phase = Phase::Failed;
        self.parts.failed.store(true, Ordering::Release);
        self.flight.take();
    }
    pub(crate) fn track(
        &self,
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) -> UploadPart {
        self.parts.pending.fetch_add(1, Ordering::AcqRel);
        let mut guard = PartGuard {
            parts: self.parts.clone(),
            succeeded: false,
        };
        Box::pin(async move {
            let result = future.await;
            guard.succeeded = result.is_ok();
            drop(guard);
            result
        })
    }
    pub(crate) fn finalizing(&mut self) -> Result<Finalizing<'_>> {
        if self.parts.failed.load(Ordering::Acquire) {
            self.fail();
        }
        self.receiving()?;
        if self.parts.pending.load(Ordering::Acquire) != 0 {
            return Err(limit_error(
                self.name,
                "multipart parts must finish before complete",
            ));
        }
        self.phase = Phase::Finalizing;
        Ok(Finalizing(self))
    }
    pub(crate) fn ready(&self) -> Result<()> {
        if self.phase != Phase::Ready {
            return Err(limit_error(
                self.name,
                "multipart upload failed or was aborted",
            ));
        }
        Ok(())
    }
    pub(crate) fn committed(&mut self, result: PutResult) -> PutResult {
        self.phase = Phase::Complete;
        self.flight.take();
        self.result = Some(result.clone());
        result
    }
    fn abort_action(&self) -> Result<AbortAction> {
        match self.phase {
            Phase::Receiving => Ok(AbortAction::AbortInner),
            Phase::Ready => Ok(AbortAction::DeleteMaterialized),
            Phase::Failed => Ok(AbortAction::ResolveFailed),
            Phase::Aborted => Ok(AbortAction::Done),
            Phase::Complete => Err(limit_error(self.name, "cannot abort a committed upload")),
            Phase::Finalizing => Err(limit_error(self.name, "multipart upload is finalizing")),
        }
    }

    fn finish_abort(&mut self) {
        self.phase = Phase::Aborted;
        self.flight.take();
    }
}

pub(crate) struct Finalizing<'a>(&'a mut Lifecycle);
impl Finalizing<'_> {
    pub(crate) fn materialized(self) {
        self.0.phase = Phase::Ready;
    }
}
impl Drop for Finalizing<'_> {
    fn drop(&mut self) {
        if self.0.phase == Phase::Finalizing {
            self.0.fail();
        }
    }
}

pub(crate) async fn abort_upload<T: ObjectStore, M: SidecarMeta>(
    lifecycle: &mut Lifecycle,
    store: &SidecarStore<T, M>,
    inner: &mut dyn MultipartUpload,
    location: &Path,
    generation: &str,
    extensions: Extensions,
) -> Result<()> {
    match lifecycle.abort_action()? {
        AbortAction::AbortInner => inner.abort().await?,
        AbortAction::DeleteMaterialized => {
            store
                .delete_uncommitted_generation(location, generation, extensions)
                .await?;
        }
        AbortAction::ResolveFailed => {
            if store
                .generation_exists(location, generation, extensions.clone())
                .await?
            {
                store
                    .delete_uncommitted_generation(location, generation, extensions)
                    .await?;
            } else {
                inner.abort().await?;
            }
        }
        AbortAction::Done => return Ok(()),
    }
    lifecycle.finish_abort();
    Ok(())
}

/// Ciphertext chunks need not align with physical multipart boundaries.
/// Slicing Bytes assembles transport parts without another ciphertext copy.
#[derive(Default)]
pub(crate) struct CiphertextParts {
    chunks: VecDeque<Bytes>,
    len: usize,
}
impl CiphertextParts {
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn push(&mut self, chunk: Bytes) {
        if !chunk.is_empty() {
            self.len += chunk.len();
            self.chunks.push_back(chunk);
        }
    }
    pub(crate) fn take(&mut self, size: usize) -> PutPayload {
        assert!(size <= self.len);
        self.len -= size;
        let mut remaining = size;
        let mut parts = Vec::new();
        while remaining > 0 {
            let chunk = self
                .chunks
                .pop_front()
                .expect("ciphertext length invariant");
            let take = chunk.len().min(remaining);
            parts.push(chunk.slice(..take));
            if take < chunk.len() {
                self.chunks.push_front(chunk.slice(take..));
            }
            remaining -= take;
        }
        parts.into_iter().collect()
    }

    /// Detaches a small residual tail from a much larger backing allocation.
    /// Call this only after all complete physical parts have been removed, so
    /// the copied amount is strictly less than the configured part size.
    pub(crate) fn compact_tail(&mut self) {
        if self.len == 0 {
            return;
        }
        let mut tail = Vec::with_capacity(self.len);
        while let Some(chunk) = self.chunks.pop_front() {
            tail.extend_from_slice(&chunk);
        }
        self.chunks.push_back(Bytes::from(tail));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_tail_preserves_only_the_residual_bytes() {
        let mut buffered = CiphertextParts::default();
        buffered.push(Bytes::from(vec![7; DEFAULT_PART_SIZE * 2 + 3]));
        let first = buffered.take(DEFAULT_PART_SIZE);
        let second = buffered.take(DEFAULT_PART_SIZE);
        assert_eq!(buffered.len(), 3);

        buffered.compact_tail();
        drop((first, second));
        let tail: Bytes = buffered.take(3).into();
        assert_eq!(tail, Bytes::from_static(&[7, 7, 7]));
        assert_eq!(tail.try_into_mut().unwrap().capacity(), 3);
    }
}
