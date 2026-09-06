//! Resource limits for metadata, caching and maintenance.

use moka::future::Cache;
use object_store::{Error, Result, path::Path};
use std::{sync::Arc, time::Duration};

/// Limits applied before publishing or accepting a metadata document.
/// Increase these explicitly for very large objects or unusually small chunks.
#[derive(Debug, Clone, Copy)]
pub struct MetadataLimits {
    /// Maximum encoded sidecar size, checked while streaming its body.
    pub max_metadata_bytes: usize,
    /// Maximum logical object size.
    pub max_object_size: u64,
    /// Maximum number of encryption chunks in one object.
    pub max_chunks: usize,
}

impl Default for MetadataLimits {
    fn default() -> Self {
        Self {
            max_metadata_bytes: 64 * 1024 * 1024,
            max_object_size: 1024 * 1024 * 1024 * 1024,
            max_chunks: 4 * 1024 * 1024,
        }
    }
}

impl MetadataLimits {
    pub(crate) fn check_size(&self, size: u64, store: &'static str) -> Result<()> {
        if size > self.max_object_size {
            return Err(limit_error(store, "object size limit exceeded"));
        }
        Ok(())
    }
}

/// Bounds one garbage-collection pass. A budget error occurs before any
/// deletion. Use a logical prefix to collect a large store in smaller scopes.
#[derive(Debug, Clone)]
pub struct GarbageCollectionOptions {
    /// Logical namespace to collect, or the whole store.
    pub prefix: Option<Path>,
    /// Maximum concurrent backend requests (at least one).
    pub concurrency: usize,
    /// Maximum metadata documents in the mark set.
    pub max_metadata_entries: usize,
    /// Maximum unreferenced payloads retained for the sweep.
    pub max_candidates: usize,
}

impl Default for GarbageCollectionOptions {
    fn default() -> Self {
        Self {
            prefix: None,
            concurrency: 8,
            max_metadata_entries: 1_000_000,
            max_candidates: 100_000,
        }
    }
}

pub(crate) const DEFAULT_CACHE_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) fn limit_error(store: &'static str, message: &str) -> Error {
    Error::Generic {
        store,
        source: message.to_string().into(),
    }
}

/// Quantized byte weights retain the historical entry-count upper bound.
/// Moka's admission/eviction is asynchronous; this is an estimated value/key
/// budget, not an exact limit on the allocator or cache bookkeeping.
pub(crate) fn metadata_cache<M: Send + Sync + 'static>(
    capacity: u64,
    byte_budget: u64,
    ttl: Duration,
    tti: Option<Duration>,
    weight: impl Fn(&M) -> usize + Send + Sync + 'static,
) -> Cache<Path, Arc<M>> {
    let capacity = capacity.min(byte_budget).min(u32::MAX as u64);
    let unit = byte_budget.checked_div(capacity).unwrap_or(1).max(1);
    let builder = Cache::builder()
        .max_capacity(capacity)
        .weigher(move |key: &Path, value: &Arc<M>| {
            let bytes = weight(value)
                .saturating_add(key.as_ref().len())
                .saturating_add(64);
            (bytes as u64).div_ceil(unit).clamp(1, u32::MAX as u64) as u32
        })
        .time_to_live(ttl);
    match tti {
        Some(tti) => builder.time_to_idle(tti).build(),
        None => builder.build(),
    }
}
