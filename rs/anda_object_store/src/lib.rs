//! # anda_object_store
//!
//! `anda_object_store` extends the [`object_store`] crate with two composable
//! wrappers that are used as the storage substrate for AndaDB and the AI memory
//! brain:
//!
//! - [`MetaStore`] provides sidecar metadata and per-commit logical ETags.
//! - [`EncryptedStore`] adds chunked AES-256-GCM encryption and authenticated
//!   metadata with seekable range access.
//!
//! ## Logical ETag
//!
//! ETags are opaque commit identities derived from fresh generation IDs, not
//! content fingerprints. Identical payloads in successive commits get distinct
//! tokens, closing ABA lost updates without hashing the full payload. Older
//! payload-derived ETags remain readable and comparable as opaque strings.
//!
//! ## Immutable-generation write protocol
//!
//! Both wrappers store a logical object as two backend objects:
//!
//! - `meta/<location>` — a small metadata document, the **only commit
//!   point**. It carries a pointer (the *generation*) to the payload.
//! - `gen/<location>/<generation>` — the immutable payload. Every put writes
//!   a fresh generation and then commits
//!   by atomically switching the metadata pointer with a single backend put.
//!
//! ## Crash semantics
//!
//! - A crash **before** the pointer switch leaves the previous version fully
//!   intact and readable; the new generation is unreferenced garbage.
//! - A crash **after** the pointer switch means the put took effect; the
//!   replaced generation is garbage.
//! - Torn reads ("old metadata + new payload") are impossible by
//!   construction: readers resolve the pointer and then read an immutable
//!   object.
//!
//! Garbage is deleted best-effort right after each successful pointer switch
//! and otherwise reclaimed by the explicit mark-sweep collector
//! ([`MetaStore::collect_garbage`] / [`EncryptedStore::collect_garbage`]),
//! which is designed to run when the store is otherwise quiescent (e.g. at
//! open) and never deletes a payload that a commit point references.
//!
//! Errors or cancellation after a metadata mutation has started invalidate the
//! shared cache synchronously: the backend may have committed already. Local
//! durability still depends on the backend and its fsync configuration.
//!
//! ## Backward compatibility
//!
//! Deployments written by anda_object_store < 0.10 store payloads directly at
//! `data/<location>` ("legacy layout"); their metadata carries no generation
//! pointer. Such objects stay fully readable, and the first overwrite
//! migrates them to the generation layout (the old `data/` object is deleted
//! after the pointer switch). The format only rolls forward: data written by
//! this version cannot be read by < 0.10.
//!
//! ## Single-writer contract
//!
//! Concurrent mutations of the **same key** must be coordinated by the
//! caller (AndaDB deploys one writer per store). Clones share the
//! per-key metadata critical section; separately built instances do not. A
//! second `PutMode::Create` writer is rejected by the backend's conditional
//! write of the commit point, but `Overwrite`/`Update` writers and the
//! garbage collector are only safe under the single-writer assumption.
//!
//! See `docs/anda_object_store.md` in the repository for the full design
//! document.

use async_trait::async_trait;
#[cfg(test)]
use base64::{Engine, prelude::BASE64_URL_SAFE};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use moka::future::Cache;
use object_store::{path::Path, *};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use sha3::Digest;
use std::{ops::Range, sync::Arc, time::Duration};

/// Transparent AES-256-GCM encryption-at-rest layer for any [`ObjectStore`].
pub mod encryption;
/// Fault-injection wrapper for crash-consistency and chaos testing.
pub mod fault;
mod generation;
mod limits;
mod sidecar;
mod upload;

use generation::commit_e_tag;
use limits::{DEFAULT_CACHE_BYTES, limit_error, metadata_cache};
pub use limits::{GarbageCollectionOptions, MetadataLimits};
use upload::{Lifecycle, Phase};

pub use encryption::{EncryptedStore, EncryptedStoreBuilder, EncryptedStoreUploader};
pub use fault::{
    FaultEvent, FaultGate, FaultHandle, FaultKind, FaultOp, FaultOutcome, FaultRule, FaultStore,
};

use sidecar::{
    ListingMetaPolicy, PublicationBaseline, SidecarMeta, SidecarStore, logical_last_modified,
    new_commit_timestamp_ms,
};

/// `MetaStore` is a wrapper around an `ObjectStore` implementation that adds metadata capabilities.
///
/// It stores metadata for each object in a separate location, which enables conditional updates
/// for storage backends that don't natively support them (like `LocalFileSystem`).
///
/// The metadata includes:
/// - Size of the object
/// - Opaque ETag (derived from the unique generation for each commit)
/// - The generation pointer to the immutable payload object
/// - The logical commit timestamp reported as `last_modified`
///
/// # Example
/// ```rust,no_run
/// use anda_object_store::MetaStoreBuilder;
/// use object_store::local::LocalFileSystem;
///
/// let storage = MetaStoreBuilder::new(
///    LocalFileSystem::new_with_prefix("my_store").unwrap(),
///    10000,
/// )
/// .build();
/// ```
pub struct MetaStore<T: ObjectStore> {
    inner: Arc<SidecarStore<T, Metadata>>,
}

impl<T: ObjectStore> Clone for MetaStore<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Builder for creating a `MetaStore` instance.
///
/// This builder configures:
/// - The underlying storage implementation
/// - Metadata cache settings
pub struct MetaStoreBuilder<T: ObjectStore> {
    /// The underlying storage implementation
    store: T,
    /// Cache for metadata to reduce storage operations
    meta_cache: Cache<Path, Arc<Metadata>>,
    /// Maximum number of metadata entries to cache
    meta_cache_capacity: u64,
    meta_cache_ttl: Duration,
    meta_cache_bytes: u64,
    limits: MetadataLimits,
}

/// Metadata structure for objects stored in `MetaStore`.
///
/// Serialized as compact CBOR (single-letter field names) and stored at
/// `meta/<location>`; it points at the immutable payload object at
/// `gen/<location>/<generation>` (or, for pre-0.10 documents without a
/// generation, at the legacy `data/<location>` object).
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Metadata {
    /// Size of the (logical) object in bytes.
    #[serde(rename = "s")]
    size: u64,

    /// Opaque logical ETag, derived from the generation for new writes and
    /// encoded with padded URL-safe Base64. Existing older tokens are kept as
    /// opaque strings. Used by all logical precondition checks.
    #[serde(rename = "e")]
    e_tag: Option<String>,

    /// Legacy field of the pre-0.10 mutable dual-object layout (the inner
    /// backend's ETag). Retained so old documents decode; never written.
    #[serde(rename = "o", default, skip_serializing_if = "Option::is_none")]
    original_tag: Option<String>,

    /// Legacy field of the pre-0.10 mutable dual-object layout (the inner
    /// backend's version). Retained so old documents decode; never written.
    #[serde(rename = "v", default, skip_serializing_if = "Option::is_none")]
    original_version: Option<String>,

    /// Generation pointer: the payload lives at
    /// `gen/<location>/<generation>`. `None` means the legacy layout
    /// (`data/<location>`). Internal to the protocol; never exposed as a
    /// caller-visible version.
    #[serde(rename = "g", default, skip_serializing_if = "Option::is_none")]
    generation: Option<String>,

    /// Logical commit timestamp in milliseconds since the Unix epoch. It is
    /// captured after the payload is complete and immediately before the
    /// metadata pointer is published.
    #[serde(rename = "m", default, skip_serializing_if = "Option::is_none")]
    committed_at_ms: Option<u64>,
}

impl SidecarMeta for Metadata {
    const STORE_NAME: &'static str = "MetaStore";

    fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn generation(&self) -> Option<&str> {
        self.generation.as_deref()
    }

    fn committed_at_ms(&self) -> Option<u64> {
        self.committed_at_ms
    }
}

impl Metadata {
    fn cache_weight(&self) -> usize {
        std::mem::size_of::<Self>()
            + [
                &self.e_tag,
                &self.original_tag,
                &self.original_version,
                &self.generation,
            ]
            .iter()
            .map(|v| v.as_ref().map_or(0, String::capacity))
            .sum::<usize>()
    }
}

impl<T: ObjectStore> std::fmt::Display for MetaStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetaStore({:?})", self.inner.store)
    }
}

impl<T: ObjectStore> std::fmt::Debug for MetaStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetaStore({:?})", self.inner.store)
    }
}

impl<T: ObjectStore> MetaStoreBuilder<T> {
    /// Creates a new `MetaStoreBuilder` with the specified underlying store and cache capacity.
    ///
    /// # Parameters
    /// - `store`: The underlying storage implementation
    /// - `meta_cache_capacity`: Maximum number of metadata entries to cache
    ///
    /// # Returns
    /// A new `MetaStoreBuilder` instance
    pub fn new(store: T, meta_cache_capacity: u64) -> Self {
        MetaStoreBuilder {
            store,
            meta_cache: metadata_cache(
                meta_cache_capacity,
                DEFAULT_CACHE_BYTES,
                Duration::from_secs(3600),
                None,
                Metadata::cache_weight,
            ),
            meta_cache_capacity,
            meta_cache_ttl: Duration::from_secs(3600),
            meta_cache_bytes: DEFAULT_CACHE_BYTES,
            limits: MetadataLimits::default(),
        }
    }

    /// Sets the time-to-live (TTL) for the metadata cache.
    pub fn with_meta_cache_ttl(mut self, ttl: Duration) -> Self {
        self.meta_cache_ttl = ttl;
        self.meta_cache = metadata_cache(
            self.meta_cache_capacity,
            self.meta_cache_bytes,
            ttl,
            None,
            Metadata::cache_weight,
        );
        self
    }

    /// Sets the estimated key/value byte budget while retaining the entry limit.
    pub fn with_meta_cache_bytes(mut self, bytes: u64) -> Self {
        self.meta_cache_bytes = bytes;
        self.meta_cache = metadata_cache(
            self.meta_cache_capacity,
            bytes,
            self.meta_cache_ttl,
            None,
            Metadata::cache_weight,
        );
        self
    }

    /// Configures bounds for metadata decoding and logical object sizes.
    pub fn with_metadata_limits(mut self, limits: MetadataLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Builds a `MetaStore` from this builder.
    ///
    /// # Returns
    /// A new `MetaStore` instance
    pub fn build(self) -> MetaStore<T> {
        MetaStore {
            inner: Arc::new(
                SidecarStore::new(self.store, self.meta_cache).with_limits(self.limits),
            ),
        }
    }
}

impl<T: ObjectStore> MetaStore<T> {
    /// Runs mark-sweep garbage collection over the payload objects.
    ///
    /// All commit points (`meta/` documents) are read first; a payload is
    /// only deleted when no commit point references it, with a fresh re-read
    /// of the key's metadata right before each deletion. Generations minted
    /// after the collection started are skipped. Run this when the store is
    /// otherwise quiescent (e.g. at open), in line with the single-writer
    /// contract.
    ///
    /// Returns the number of payload objects deleted.
    pub async fn collect_garbage(&self) -> Result<usize> {
        self.inner.collect_garbage().await
    }

    /// Collects a bounded logical namespace with configurable I/O concurrency.
    pub async fn collect_garbage_with_options(
        &self,
        options: GarbageCollectionOptions,
    ) -> Result<usize> {
        self.inner.collect_garbage_with_options(options).await
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for MetaStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let size = payload.content_length() as u64;
        self.inner.limits.check_size(size, "MetaStore")?;
        let extensions = opts.extensions.clone();
        let mut _in_flight = None;
        let in_flight_out = &mut _in_flight;
        let result = self
            .inner
            .update_meta_with(
                location,
                matches!(opts.mode, PutMode::Create),
                extensions,
                async |current| {
                    if let PutMode::Update(version) = &opts.mode {
                        let current = current.ok_or_else(|| Error::Precondition {
                            path: location.to_string(),
                            source: "metadata not found".into(),
                        })?;
                        check_update_version(
                            location,
                            &current.e_tag,
                            &current.generation,
                            version,
                        )?;
                    }
                    let (generation, in_flight) = self
                        .inner
                        .put_new_generation(location, payload, opts)
                        .await?;
                    *in_flight_out = Some(in_flight);
                    Ok(Metadata {
                        size,
                        e_tag: Some(commit_e_tag(&generation)),
                        original_tag: None,
                        original_version: None,
                        generation: Some(generation.clone()),
                        committed_at_ms: Some(new_commit_timestamp_ms()),
                    })
                },
            )
            .await?;
        Ok(PutResult {
            e_tag: result.e_tag.clone(),
            version: None,
            extensions: result.extensions,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let extensions = opts.extensions.clone();
        let (generation, flight) = self
            .inner
            .allocate_generation(location, extensions.clone())
            .await?;
        let inner = self
            .inner
            .store
            .put_multipart_opts(&self.inner.generation_path(location, &generation), opts)
            .await?;
        Ok(Box::new(MetaStoreUploader {
            size: 0,
            location: location.clone(),
            generation,
            lifecycle: Lifecycle::new(flight, "MetaStore"),
            prepared: None,
            publication_baseline: None,
            extensions,
            store: self.inner.clone(),
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut retried = false;
        loop {
            let meta = self
                .inner
                .get_meta_with_extensions(location, options.extensions.clone())
                .await?;
            let mut options = options.clone();
            let last_modified =
                logical_last_modified(meta.committed_at_ms, meta.generation.as_deref());
            check_get_preconditions(location, &mut options, meta.e_tag.as_deref(), last_modified)?;

            let payload_path = self
                .inner
                .payload_path(location, meta.generation.as_deref());
            match self
                .inner
                .store
                .get_opts(&payload_path, options.clone())
                .await
            {
                Ok(mut res) => {
                    res.meta.location = location.clone();
                    res.meta.e_tag = meta.e_tag.clone();
                    // Report the logical object, not the payload object it
                    // resolves to: the size comes from the commit point (the
                    // payload's own length is whatever the backend holds),
                    // and the timestamp from the generation pointer so
                    // listings and reads agree.
                    res.meta.size = meta.size;
                    res.meta.last_modified = last_modified.unwrap_or(res.meta.last_modified);
                    // Versions are not reported: replaced generations are
                    // reclaimed eagerly, so version-addressed reads cannot be
                    // honoured. Conditional updates use the logical e_tag,
                    // which is unique per commit.
                    res.meta.version = None;
                    return Ok(res);
                }
                Err(Error::NotFound { source, .. }) => {
                    // The cached pointer — generational or legacy — may be
                    // stale after a concurrent overwrite: the generation was
                    // replaced and reclaimed, or the legacy payload was
                    // migrated away. Re-resolve once.
                    if !retried {
                        retried = true;
                        self.inner
                            .refresh_meta_with_extensions(location, options.extensions.clone())
                            .await?;
                        continue;
                    }
                    return Err(Error::NotFound {
                        path: location.to_string(),
                        source,
                    });
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let mut retried = false;
        loop {
            let meta = self.inner.get_meta(location).await?;
            validate_ranges("MetaStore", ranges, meta.size)?;

            let payload_path = self
                .inner
                .payload_path(location, meta.generation.as_deref());
            match self.inner.store.get_ranges(&payload_path, ranges).await {
                Ok(rt) => return Ok(rt),
                Err(Error::NotFound { source, .. }) => {
                    if !retried {
                        retried = true;
                        self.inner.refresh_meta(location).await?;
                        continue;
                    }
                    return Err(Error::NotFound {
                        path: location.to_string(),
                        source,
                    });
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.clone().delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner
            .clone()
            .list(prefix, ListingMetaPolicy::unchecked())
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner
            .clone()
            .list_with_offset(prefix, offset, ListingMetaPolicy::unchecked())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner
            .list_with_delimiter(prefix, ListingMetaPolicy::unchecked())
            .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let CopyOptions { mode, extensions } = options;
        let create = matches!(mode, CopyMode::Create);
        // Copy the payload into a fresh generation of the target; the
        // pointer switch below is the commit point. `_in_flight` shields the
        // copied generation from garbage collection until then.
        let (src, generation, _in_flight) = self
            .inner
            .copy_payload(from, to, extensions.clone())
            .await?;
        self.inner
            .update_meta_with(to, create, extensions, async |_| {
                Ok(Metadata {
                    size: src.size,
                    e_tag: Some(commit_e_tag(&generation)),
                    original_tag: None,
                    original_version: None,
                    generation: Some(generation.clone()),
                    committed_at_ms: Some(new_commit_timestamp_ms()),
                })
            })
            .await?;
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        if from == to {
            // A self-rename must not be forwarded (copy + delete would
            // destroy the object). Validate existence and target mode, then
            // leave the object untouched.
            return self.inner.check_self_rename(from, &options).await;
        }

        let mode = match options.target_mode {
            RenameTargetMode::Overwrite => CopyMode::Overwrite,
            RenameTargetMode::Create => CopyMode::Create,
        };
        let extensions = options.extensions.clone();
        self.copy_opts(
            from,
            to,
            CopyOptions {
                mode,
                extensions: options.extensions,
            },
        )
        .await?;
        match self
            .inner
            .delete_object_with_extensions(from, extensions)
            .await
        {
            Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

/// Multipart upload with explicit failure and publication states.
/// Failed/cancelled parts require a new upload. A metadata-only failure after
/// payload completion can be retried without completing the backend twice.
pub struct MetaStoreUploader<T: ObjectStore> {
    size: u64,
    location: Path,
    generation: String,
    lifecycle: Lifecycle,
    prepared: Option<Metadata>,
    publication_baseline: PublicationBaseline,
    extensions: Extensions,
    store: Arc<SidecarStore<T, Metadata>>,
    inner: Box<dyn MultipartUpload>,
}
impl<T: ObjectStore> std::fmt::Debug for MetaStoreUploader<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetaStoreUploader({})", self.location)
    }
}
#[async_trait]
impl<T: ObjectStore> MultipartUpload for MetaStoreUploader<T> {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        if let Err(err) = self.lifecycle.receiving() {
            return Box::pin(async { Err(err) });
        }
        let checked = {
            self.size
                .checked_add(payload.content_length() as u64)
                .ok_or_else(|| limit_error("MetaStore", "object size overflow"))
        }
        .and_then(|size| {
            self.store.limits.check_size(size, "MetaStore")?;
            Ok(size)
        });
        let size = match checked {
            Ok(size) => size,
            Err(err) => {
                self.lifecycle.fail();
                return Box::pin(async { Err(err) });
            }
        };
        self.size = size;
        self.lifecycle.track(self.inner.put_part(payload))
    }
    async fn complete(&mut self) -> Result<PutResult> {
        if let Some(result) = &self.lifecycle.result {
            return Ok(result.clone());
        }
        if self.lifecycle.phase == Phase::Receiving {
            let attempt = self.lifecycle.finalizing()?;
            self.inner.complete().await?;
            self.prepared = Some(Metadata {
                size: self.size,
                e_tag: Some(commit_e_tag(&self.generation)),
                original_tag: None,
                original_version: None,
                generation: Some(self.generation.clone()),
                committed_at_ms: Some(new_commit_timestamp_ms()),
            });
            attempt.materialized();
        }
        self.lifecycle.ready()?;
        let metadata = self
            .prepared
            .as_ref()
            .expect("materialized upload has metadata")
            .clone();
        let result = self
            .store
            .publish_upload(
                &self.location,
                metadata,
                &mut self.publication_baseline,
                self.extensions.clone(),
            )
            .await?;
        self.prepared = None;
        Ok(self.lifecycle.committed(PutResult {
            e_tag: result.e_tag.clone(),
            version: None,
            extensions: result.extensions,
        }))
    }
    async fn abort(&mut self) -> Result<()> {
        upload::abort_upload(
            &mut self.lifecycle,
            self.store.as_ref(),
            self.inner.as_mut(),
            &self.location,
            &self.generation,
            self.extensions.clone(),
        )
        .await?;
        self.prepared = None;
        Ok(())
    }
}

/// Computes the SHA3-256 hash of `data` and returns it as a 32-byte array.
///
/// Test-only helper for building the fixtures of pre-0.10 layouts, whose
/// ETag was the bare hash of the payload.
#[cfg(test)]
pub(crate) fn sha3_256(data: &[u8]) -> [u8; 32] {
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Evaluates a `PutMode::Update` precondition against the committed metadata.
///
/// The logical ETag is the compare-and-swap token: it is minted fresh for
/// every commit (see the crate documentation), so comparing it answers "is
/// this still the version I read?" rather than "does it still hold the bytes
/// I read?".
fn check_update_version(
    location: &Path,
    current_e_tag: &Option<String>,
    _current_generation: &Option<String>,
    update: &UpdateVersion,
) -> Result<()> {
    // Mirror `object_store`'s in-memory reference behavior: an e_tag is
    // required for conditional updates.
    let Some(expected) = &update.e_tag else {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: "missing e_tag for conditional update".into(),
        });
    };

    if current_e_tag.as_ref() != Some(expected) {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: format!("{:?} does not match {:?}", current_e_tag, update.e_tag).into(),
        });
    }

    if update.version.is_some() {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: "version-addressed updates are not supported; use e_tag".into(),
        });
    }

    Ok(())
}

/// Evaluates the read preconditions against the logical object described by
/// the metadata commit point and strips what it answered from the request.
///
/// The ETag conditions are always answered here: the payload object is
/// immutable and carries the backend's own ETag, which is not the logical
/// one. The date conditions are answered here whenever the logical
/// `last_modified` is known (`Some`, the regular generation layout), so the
/// answer is consistent with the timestamp the same call reports; for
/// pre-0.10 documents (`None`) they are left to the backend, which evaluates
/// them against the legacy payload object — the very timestamp such a read
/// reports.
///
/// The evaluation mirrors [`GetOptions::check_preconditions`], including RFC
/// 9110 §13.2.2 precedence: when an ETag condition is present the
/// corresponding date condition is ignored, so it must not reach the backend
/// either.
fn check_get_preconditions(
    location: &Path,
    options: &mut GetOptions,
    logical_e_tag: Option<&str>,
    last_modified: Option<DateTime<Utc>>,
) -> Result<()> {
    if options.version.is_some() {
        return Err(Error::NotSupported {
            source: "version-addressed reads are not supported; use logical e_tag conditions"
                .into(),
        });
    }
    // The use of the invalid etag "*" means no ETag is equivalent to never matching.
    let e_tag = logical_e_tag.unwrap_or("*");
    let if_match = options.if_match.take();
    let if_none_match = options.if_none_match.take();

    if let Some(if_match) = if_match {
        options.if_unmodified_since = None;
        if if_match != "*" && if_match.split(',').map(str::trim).all(|tag| tag != e_tag) {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: format!("{e_tag} does not match {if_match}").into(),
            });
        }
    } else if let Some(last_modified) = last_modified
        && let Some(date) = options.if_unmodified_since.take()
        && last_modified > date
    {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: format!("{date} < {last_modified}").into(),
        });
    }

    if let Some(if_none_match) = if_none_match {
        options.if_modified_since = None;
        if if_none_match == "*"
            || if_none_match
                .split(',')
                .map(str::trim)
                .any(|tag| tag == e_tag)
        {
            return Err(Error::NotModified {
                path: location.to_string(),
                source: format!("{e_tag} matches {if_none_match}").into(),
            });
        }
    } else if let Some(last_modified) = last_modified
        && let Some(date) = options.if_modified_since.take()
        && last_modified <= date
    {
        return Err(Error::NotModified {
            path: location.to_string(),
            source: format!("{date} >= {last_modified}").into(),
        });
    }

    Ok(())
}

pub(crate) fn validate_ranges(store: &'static str, ranges: &[Range<u64>], len: u64) -> Result<()> {
    for range in ranges {
        if range.start >= len {
            return Err(Error::Generic {
                store,
                source: format!("start {} is larger than length {}", range.start, len).into(),
            });
        }
        if range.end <= range.start {
            return Err(Error::Generic {
                store,
                source: format!("end {} is less than start {}", range.end, range.start).into(),
            });
        }
        if range.end > len {
            return Err(Error::Generic {
                store,
                source: format!("end {} is larger than length {}", range.end, len).into(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
