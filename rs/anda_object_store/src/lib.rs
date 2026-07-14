//! # anda_object_store
//!
//! `anda_object_store` extends the [`object_store`] crate with two composable
//! wrappers that are used as the storage substrate for AndaDB and the AI memory
//! brain:
//!
//! - [`MetaStore`] — augments any [`ObjectStore`] backend with side-car
//!   metadata (object size, content hash, original backend ETag/version).
//!   This enables a uniform, content-addressable ETag and conditional
//!   `PutMode::Update` semantics on top of backends that lack them natively
//!   (notably `object_store::local::LocalFileSystem`).
//! - [`EncryptedStore`] — provides transparent, chunked AES-256-GCM
//!   encryption-at-rest. Objects are split into fixed-size chunks, each
//!   encrypted with a per-chunk nonce derived from a random per-object base
//!   nonce. Encryption metadata (base nonce, per-chunk authentication tags)
//!   is stored alongside content metadata.
//!
//! Both wrappers implement [`ObjectStore`] and place data and metadata under
//! two distinct path prefixes (`data/` and `meta/` by default) on the
//! underlying backend, so they can be layered on top of any compliant store
//! (in-memory, local filesystem, S3, GCS, Azure Blob, …).
//!
//! ## Crash semantics
//!
//! A logical put writes two backend objects in sequence: the data object at
//! `data/<location>` first, then the sidecar metadata at `meta/<location>`.
//! The pair is **not atomic**. A crash (or a failed metadata write) between
//! the two leaves the new data object with either no metadata — an "orphan"
//! on the first write of a key — or the previous metadata on an overwrite.
//! Until the key is written again, such an object is unreadable: `MetaStore`
//! reports a stale size/ETag and `EncryptedStore` fails decryption or
//! metadata authentication (indistinguishable from tampering). The previous
//! version of the payload is already overwritten and cannot be recovered.
//!
//! Both wrappers self-heal on the next write of the same key:
//! `PutMode::Overwrite` always rebuilds the pair (including when the sidecar
//! metadata is corrupted), and `PutMode::Create` succeeds over an orphaned
//! data object without metadata. Listings tolerate orphans — whether the
//! sidecar metadata is missing or fails to decode, the entry's `e_tag` is
//! `None` — and `delete` cleans up either half. Callers must therefore be
//! prepared to re-write objects that fail to read back after a crash, in
//! line with AndaDB's overwrite-self-heal convention.
//!
//! ## Single-writer assumption
//!
//! The `PutMode::Create` self-heal path assumes AndaDB's single-writer
//! deployment model: a data object without readable sidecar metadata is
//! treated as logically absent, so `Create` overwrites it. Within one
//! process the per-key metadata critical section serializes writers, but
//! **across processes this weakens `PutMode::Create`'s "exactly one winner"
//! guarantee**: two processes racing `Create` on the same key while a
//! sidecar metadata write has not yet become visible can each classify the
//! other's data object as an orphan and overwrite it. Do not rely on
//! `PutMode::Create` through these wrappers for cross-process mutual
//! exclusion; multi-writer deployments are not protected.
//!
//! See `docs/anda_object_store.md` in the repository for the full design
//! document.

use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_URL_SAFE};
use bytes::Bytes;
use futures::stream::BoxStream;
use moka::future::Cache;
use object_store::{path::Path, *};
use serde::{Deserialize, Serialize};
use sha3::Digest;
use std::{ops::Range, sync::Arc, time::Duration};

/// Transparent AES-256-GCM encryption-at-rest layer for any [`ObjectStore`].
pub mod encryption;
/// Fault-injection wrapper for crash-consistency and chaos testing.
pub mod fault;
mod sidecar;

pub use encryption::{EncryptedStore, EncryptedStoreBuilder, EncryptedStoreUploader};
pub use fault::{FaultHandle, FaultKind, FaultOp, FaultRule, FaultStore};

use sidecar::{ListingMetaPolicy, SidecarMeta, SidecarStore};

/// `MetaStore` is a wrapper around an `ObjectStore` implementation that adds metadata capabilities.
///
/// It stores metadata for each object in a separate location, which enables conditional updates
/// for storage backends that don't natively support them (like `LocalFileSystem`).
///
/// The metadata includes:
/// - Size of the object
/// - E-Tag (SHA3-256 hash of the content)
/// - Original tag from the underlying storage
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
#[derive(Clone)]
pub struct MetaStore<T: ObjectStore> {
    inner: Arc<SidecarStore<T, Metadata>>,
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
}

/// Metadata structure for objects stored in `MetaStore`.
///
/// Serialized as compact CBOR (single-letter field names) and stored at
/// `meta/<location>` alongside the data object at `data/<location>`.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Metadata {
    /// Size of the (logical) object in bytes.
    #[serde(rename = "s")]
    size: u64,

    /// Content-addressable ETag produced by [`sha3_256`] over the payload,
    /// encoded as URL-safe Base64 (without padding). This ETag is what
    /// [`MetaStore`] exposes to callers via [`ObjectStore`] APIs and uses
    /// for `PutMode::Update` precondition checks.
    #[serde(rename = "e")]
    e_tag: Option<String>,

    /// ETag returned by the underlying storage when the data object was
    /// written. Used to translate caller-provided `if_match`/`if_none_match`
    /// preconditions on [`MetaStore::get_opts`] into a request the inner
    /// store understands.
    #[serde(rename = "o")]
    original_tag: Option<String>,

    /// Version returned by the underlying storage on the most recent put,
    /// when the backend supports object versioning. Forwarded back to the
    /// caller via [`PutResult::version`].
    #[serde(rename = "v")]
    original_version: Option<String>,
}

impl SidecarMeta for Metadata {
    const STORE_NAME: &'static str = "MetaStore";

    fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    fn set_original(&mut self, e_tag: Option<String>, version: Option<String>) {
        self.original_tag = e_tag;
        self.original_version = version;
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
            meta_cache: Cache::builder()
                .max_capacity(meta_cache_capacity)
                .time_to_live(Duration::from_secs(60 * 60))
                .build(),
            meta_cache_capacity,
        }
    }

    /// Sets the time-to-live (TTL) for the metadata cache.
    pub fn with_meta_cache_ttl(mut self, ttl: Duration) -> Self {
        self.meta_cache = Cache::builder()
            .max_capacity(self.meta_cache_capacity)
            .time_to_live(ttl)
            .build();
        self
    }

    /// Builds a `MetaStore` from this builder.
    ///
    /// # Returns
    /// A new `MetaStore` instance
    pub fn build(self) -> MetaStore<T> {
        MetaStore {
            inner: Arc::new(SidecarStore::new(self.store, self.meta_cache)),
        }
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for MetaStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let rt = self
            .inner
            .update_meta_with(location, async |meta| {
                // Without sidecar metadata the object does not logically
                // exist; remember this so a conflicting orphaned data object
                // (crash between the data and metadata writes) can be healed
                // below instead of failing a `Create` forever.
                let heal_create = meta.is_none() && matches!(opts.mode, PutMode::Create);

                if let PutMode::Update(v) = &opts.mode {
                    match meta {
                        Some(m) => {
                            check_update_version(location, &m.e_tag, &m.original_version, v)?;
                        }
                        None => {
                            return Err(Error::Precondition {
                                path: location.to_string(),
                                source: "metadata not found".into(),
                            });
                        }
                    }

                    opts.mode = PutMode::Overwrite;
                }

                let full_path = self.inner.full_path(location);
                // Hash segment-by-segment so multi-segment payloads are not
                // concatenated into a temporary contiguous buffer.
                let mut hasher = sha3::Sha3_256::new();
                for segment in payload.iter() {
                    hasher.update(segment);
                }
                let hash: [u8; 32] = hasher.finalize().into();

                let mut meta = Metadata {
                    size: payload.content_length() as u64,
                    e_tag: Some(BASE64_URL_SAFE.encode(hash)),
                    original_tag: None,
                    original_version: None,
                };

                let rt = if heal_create {
                    match self
                        .inner
                        .store
                        .put_opts(&full_path, payload.clone(), opts.clone())
                        .await
                    {
                        Err(Error::AlreadyExists { .. }) => {
                            // The conflicting data object is an orphan left
                            // by a crash; overwrite it to self-heal.
                            //
                            // NOTE: this weakens `PutMode::Create` across
                            // processes. Another process racing `Create` on
                            // the same key before our sidecar metadata is
                            // visible can also classify our data object as
                            // an orphan and overwrite it. Acceptable under
                            // AndaDB's single-writer-per-store deployment
                            // assumption; see the crate-level docs
                            // ("Single-writer assumption").
                            log::warn!(
                                "MetaStore: healing orphaned data object at {location} on create"
                            );
                            opts.mode = PutMode::Overwrite;
                            self.inner.store.put_opts(&full_path, payload, opts).await?
                        }
                        rt => rt?,
                    }
                } else {
                    self.inner.store.put_opts(&full_path, payload, opts).await?
                };
                meta.original_tag = rt.e_tag;
                meta.original_version = rt.version;
                Ok(meta)
            })
            .await?;

        Ok(PutResult {
            e_tag: rt.e_tag.clone(),
            version: rt.original_version.clone(),
            extensions: Extensions::default(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let full_path = self.inner.full_path(location);
        let inner = self
            .inner
            .store
            .put_multipart_opts(&full_path, opts)
            .await?;

        Ok(Box::new(MetaStoreUploader {
            hasher: sha3::Sha3_256::new(),
            size: 0,
            location: location.clone(),
            store: self.inner.clone(),
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, mut options: GetOptions) -> Result<GetResult> {
        let full_path = self.inner.full_path(location);
        let meta = self.inner.get_meta(location).await?;
        apply_logical_etag_preconditions(
            location,
            &mut options,
            meta.e_tag.as_deref(),
            meta.original_tag.clone(),
        )?;

        let mut res = self.inner.store.get_opts(&full_path, options).await?;
        res.meta.location = self.inner.strip_prefix(res.meta.location);
        res.meta.e_tag = meta.e_tag.clone();

        Ok(res)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let meta = self.inner.get_meta(location).await?;
        validate_ranges("MetaStore", ranges, meta.size)?;

        let full_path = self.inner.full_path(location);
        self.inner.store.get_ranges(&full_path, ranges).await
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
            .list(prefix, ListingMetaPolicy::unchecked(true))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner
            .clone()
            .list_with_offset(prefix, offset, ListingMetaPolicy::unchecked(true))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner
            .list_with_delimiter(prefix, ListingMetaPolicy::unchecked(true))
            .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Handler for multipart uploads to a `MetaStore`.
///
/// This struct:
/// 1. Tracks the size of the uploaded content
/// 2. Calculates a hash of the content
/// 3. Creates metadata when the upload completes
pub struct MetaStoreUploader<T: ObjectStore> {
    /// Hasher for calculating the content hash
    hasher: sha3::Sha3_256,
    /// Total size of the uploaded content
    size: usize,
    /// Logical path of the object
    location: Path,
    /// Shared sidecar core of the originating `MetaStore`
    store: Arc<SidecarStore<T, Metadata>>,
    /// Underlying multipart upload handler
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
        self.size += payload.content_length();
        for segment in payload.iter() {
            self.hasher.update(segment);
        }
        self.inner.put_part(payload)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let hash: [u8; 32] = self.hasher.clone().finalize().into();
        let e_tag = Some(BASE64_URL_SAFE.encode(hash));

        // Commit the data object and persist the metadata inside the per-key
        // critical section of `update_meta_with`, so a concurrent multipart
        // complete on the same location cannot interleave its data commit
        // between our commit and our metadata write (which would leave
        // mismatched data and metadata).
        let store = self.store.clone();
        let location = self.location.clone();
        let size = self.size as u64;
        let inner = &mut self.inner;
        let mut result: Option<PutResult> = None;
        let out = &mut result;
        store
            .update_meta_with(&location, async |_| {
                let rt = inner.complete().await?;
                let obj = store.store.head(&store.full_path(&location)).await?;
                *out = Some(rt);
                Ok(Metadata {
                    size,
                    e_tag: e_tag.clone(),
                    original_tag: obj.e_tag,
                    original_version: obj.version,
                })
            })
            .await?;

        let mut rt = result.expect("multipart complete did not run");
        rt.e_tag = e_tag;
        Ok(rt)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

/// Computes the SHA3-256 hash of `data` and returns it as a 32-byte array.
///
/// Used by [`MetaStore`] to derive a content-addressable ETag, and by
/// [`crate::encryption::EncryptedStore`] to hash the produced ciphertext.
fn sha3_256(data: &[u8]) -> [u8; 32] {
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(data);
    hasher.finalize().into()
}

fn check_update_version(
    location: &Path,
    current_e_tag: &Option<String>,
    current_version: &Option<String>,
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

    if let Some(version) = &update.version
        && current_version.as_ref() != Some(version)
    {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: format!("{:?} does not match {:?}", current_version, update.version).into(),
        });
    }

    Ok(())
}

fn apply_logical_etag_preconditions(
    location: &Path,
    options: &mut GetOptions,
    logical_e_tag: Option<&str>,
    original_tag: Option<String>,
) -> Result<()> {
    let e_tag = logical_e_tag.unwrap_or("*");

    if let Some(if_match) = options.if_match.take() {
        if if_match != "*" && if_match.split(',').map(str::trim).all(|tag| tag != e_tag) {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: format!("{e_tag} does not match {if_match}").into(),
            });
        }

        options.if_match = if if_match == "*" {
            Some(if_match)
        } else {
            original_tag
        };
    }

    if let Some(if_none_match) = options.if_none_match.take()
        && (if_none_match == "*"
            || if_none_match
                .split(',')
                .map(str::trim)
                .any(|tag| tag == e_tag))
    {
        return Err(Error::NotModified {
            path: location.to_string(),
            source: format!("{e_tag} matches {if_none_match}").into(),
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

/// Re-clones an [`Arc<Error>`] returned from a `moka` shared computation
/// (e.g. [`Cache::try_get_with`]) into an owned [`Error`].
///
/// `moka` deduplicates concurrent loaders by returning the same `Arc<Error>`
/// to every waiter. Because [`object_store::Error`] is not [`Clone`], we must
/// reconstruct an equivalent variant by hand. Variants that carry a `path`
/// are reconstructed with their `path` and a stringified `source`; everything
/// else collapses into [`Error::Generic`].
fn map_arc_error(store: &'static str, err: Arc<Error>) -> Error {
    match err.as_ref() {
        Error::NotFound { path, source } => Error::NotFound {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::AlreadyExists { path, source } => Error::AlreadyExists {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::Precondition { path, source } => Error::Precondition {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::NotModified { path, source } => Error::NotModified {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::PermissionDenied { path, source } => Error::PermissionDenied {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::Unauthenticated { path, source } => Error::Unauthenticated {
            path: path.clone(),
            source: source.to_string().into(),
        },
        err => Error::Generic {
            store,
            source: err.to_string().into(),
        },
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use std::{
        fmt,
        sync::atomic::{AtomicBool, Ordering},
    };
    use tokio::sync::Notify;

    #[derive(Debug)]
    pub(crate) enum GateOperation {
        Put(Path),
        Get(Path),
        Copy { from: Path, to: Path },
        Rename { from: Path, to: Path },
        MultipartComplete(Path),
    }

    #[derive(Debug)]
    pub(crate) struct OperationGate {
        operation: GateOperation,
        triggered: AtomicBool,
        entered: Notify,
        release: Notify,
    }

    impl OperationGate {
        fn new(operation: GateOperation) -> Self {
            Self {
                operation,
                triggered: AtomicBool::new(false),
                entered: Notify::new(),
                release: Notify::new(),
            }
        }

        fn matches(&self, operation: &GateOperation) -> bool {
            match (&self.operation, operation) {
                (GateOperation::Put(expected), GateOperation::Put(actual))
                | (GateOperation::Get(expected), GateOperation::Get(actual))
                | (
                    GateOperation::MultipartComplete(expected),
                    GateOperation::MultipartComplete(actual),
                ) => expected == actual,
                (
                    GateOperation::Copy {
                        from: expected_from,
                        to: expected_to,
                    },
                    GateOperation::Copy {
                        from: actual_from,
                        to: actual_to,
                    },
                )
                | (
                    GateOperation::Rename {
                        from: expected_from,
                        to: expected_to,
                    },
                    GateOperation::Rename {
                        from: actual_from,
                        to: actual_to,
                    },
                ) => expected_from == actual_from && expected_to == actual_to,
                _ => false,
            }
        }

        async fn pause(&self, operation: GateOperation) {
            if self.matches(&operation) && !self.triggered.swap(true, Ordering::AcqRel) {
                self.entered.notify_one();
                self.release.notified().await;
            }
        }

        pub(crate) async fn wait_until_entered(&self) {
            self.entered.notified().await;
        }

        pub(crate) fn release(&self) {
            self.release.notify_one();
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct GatedStore {
        inner: InMemory,
        gate: Arc<OperationGate>,
    }

    impl GatedStore {
        pub(crate) fn new(operation: GateOperation) -> (Self, Arc<OperationGate>) {
            let gate = Arc::new(OperationGate::new(operation));
            (
                Self {
                    inner: InMemory::new(),
                    gate: gate.clone(),
                },
                gate,
            )
        }
    }

    impl fmt::Display for GatedStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "GatedStore")
        }
    }

    #[async_trait]
    impl ObjectStore for GatedStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            let result = self.inner.put_opts(location, payload, opts).await;
            if result.is_ok() {
                self.gate.pause(GateOperation::Put(location.clone())).await;
            }
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            let inner = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(GatedUpload {
                inner,
                location: location.clone(),
                gate: self.gate.clone(),
            }))
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            let result = self.inner.get_opts(location, options).await;
            if result.is_ok() {
                self.gate.pause(GateOperation::Get(location.clone())).await;
            }
            result
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            let result = self.inner.copy_opts(from, to, options).await;
            if result.is_ok() {
                self.gate
                    .pause(GateOperation::Copy {
                        from: from.clone(),
                        to: to.clone(),
                    })
                    .await;
            }
            result
        }

        async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
            let result = self.inner.rename_opts(from, to, options).await;
            if result.is_ok() {
                self.gate
                    .pause(GateOperation::Rename {
                        from: from.clone(),
                        to: to.clone(),
                    })
                    .await;
            }
            result
        }
    }

    #[derive(Debug)]
    struct GatedUpload {
        inner: Box<dyn MultipartUpload>,
        location: Path,
        gate: Arc<OperationGate>,
    }

    #[async_trait]
    impl MultipartUpload for GatedUpload {
        fn put_part(&mut self, payload: PutPayload) -> UploadPart {
            self.inner.put_part(payload)
        }

        async fn complete(&mut self) -> Result<PutResult> {
            let result = self.inner.complete().await;
            if result.is_ok() {
                self.gate
                    .pause(GateOperation::MultipartComplete(self.location.clone()))
                    .await;
            }
            result
        }

        async fn abort(&mut self) -> Result<()> {
            self.inner.abort().await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{integration::*, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[test]
    fn builder_display_debug_and_prefix_helpers_are_exercised() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100)
            .with_meta_cache_ttl(Duration::from_secs(1))
            .build();

        assert!(format!("{storage}").contains("MetaStore"));
        assert!(format!("{storage:?}").contains("MetaStore"));

        let location = Path::from("nested/object");
        assert_eq!(
            storage.inner.full_path(&location).to_string(),
            "data/nested/object"
        );
        assert_eq!(
            storage.inner.meta_path(&location).to_string(),
            "meta/nested/object"
        );
        assert_eq!(
            storage
                .inner
                .strip_prefix(Path::from("data/nested/object"))
                .to_string(),
            "nested/object"
        );
        assert_eq!(
            storage
                .inner
                .strip_prefix(Path::from("other/nested/object"))
                .to_string(),
            "other/nested/object"
        );
    }

    #[test]
    fn validate_ranges_rejects_invalid_boundaries() {
        fn check(range: Range<u64>, len: u64) -> Result<()> {
            validate_ranges("MetaStore", std::slice::from_ref(&range), len)
        }

        assert!(check(0..1, 1).is_ok());

        let err = check(1..2, 1).unwrap_err();
        assert!(err.to_string().contains("start 1 is larger than length 1"));

        let err = check(1..1, 3).unwrap_err();
        assert!(err.to_string().contains("end 1 is less than start 1"));

        let err = check(1..4, 3).unwrap_err();
        assert!(err.to_string().contains("end 4 is larger than length 3"));
    }

    #[test]
    fn map_arc_error_reconstructs_path_variants_and_generic_fallback() {
        let cases = [
            Error::NotFound {
                path: "not-found".to_string(),
                source: "missing".into(),
            },
            Error::AlreadyExists {
                path: "exists".to_string(),
                source: "exists".into(),
            },
            Error::Precondition {
                path: "precondition".to_string(),
                source: "stale".into(),
            },
            Error::NotModified {
                path: "not-modified".to_string(),
                source: "fresh".into(),
            },
            Error::PermissionDenied {
                path: "denied".to_string(),
                source: "denied".into(),
            },
            Error::Unauthenticated {
                path: "unauthenticated".to_string(),
                source: "auth".into(),
            },
        ];

        for err in cases {
            let mapped = map_arc_error("MetaStore", Arc::new(err));
            match mapped {
                Error::NotFound { path, source }
                | Error::AlreadyExists { path, source }
                | Error::Precondition { path, source }
                | Error::NotModified { path, source }
                | Error::PermissionDenied { path, source }
                | Error::Unauthenticated { path, source } => {
                    assert!(!path.is_empty());
                    assert!(!source.to_string().is_empty());
                }
                other => panic!("unexpected mapped error: {other:?}"),
            }
        }

        let mapped = map_arc_error(
            "MetaStore",
            Arc::new(Error::Generic {
                store: "Inner",
                source: "fallback".into(),
            }),
        );
        assert!(matches!(
            mapped,
            Error::Generic {
                store: "MetaStore",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_with_memory() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 10000).build();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&storage, Some(location))
            .await
            .unwrap_err();
        if let crate::Error::NotFound { path, .. } = err {
            assert!(path.ends_with(NON_EXISTENT_NAME));
        } else {
            panic!("unexpected error type: {err:?}");
        }

        put_get_delete_list(&storage).await;
        put_get_attributes(&storage).await;
        get_opts(&storage).await;
        put_opts(&storage, true).await;

        list_uses_directories_correctly(&storage).await;
        list_with_delimiter(&storage).await;
        rename_and_copy(&storage).await;
        copy_if_not_exists(&storage).await;
        copy_rename_nonexistent_object(&storage).await;
        multipart_race_condition(&storage, true).await;
        multipart_out_of_order(&storage).await;

        let storage = MetaStoreBuilder::new(InMemory::new(), 10000).build();
        stream_get(&storage).await;
    }

    #[tokio::test]
    async fn get_ranges_requires_metadata() {
        let inner = InMemory::new();
        inner
            .put(
                &Path::from("data/missing-meta"),
                Bytes::from_static(b"abc").into(),
            )
            .await
            .unwrap();
        let storage = MetaStoreBuilder::new(inner, 100).build();

        let requested = 0..1;
        let err = storage
            .get_ranges(
                &Path::from("missing-meta"),
                std::slice::from_ref(&requested),
            )
            .await
            .unwrap_err();

        // The internal `meta/` prefix must not leak into caller-visible errors.
        assert!(matches!(err, Error::NotFound { path, .. } if path == "missing-meta"));
    }

    #[tokio::test]
    async fn get_opts_accepts_comma_separated_logical_etags() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("etag-list");
        let put = storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        let e_tag = put.e_tag.unwrap();

        let bytes = storage
            .get_opts(
                &location,
                GetOptions {
                    if_match: Some(format!("other, {e_tag}")),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));

        let err = storage
            .get_opts(
                &location,
                GetOptions {
                    if_none_match: Some(format!("other, {e_tag}")),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotModified { .. }));
    }

    #[tokio::test]
    async fn copy_and_rename_refresh_original_tag_for_logical_etag_preconditions() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let source = Path::from("copy-source");
        let copied = Path::from("copy-target");
        let renamed = Path::from("rename-target");
        let put = storage
            .put(&source, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        let e_tag = put.e_tag.unwrap();

        storage.copy(&source, &copied).await.unwrap();
        let bytes = storage
            .get_opts(
                &copied,
                GetOptions {
                    if_match: Some(e_tag.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));

        storage.rename(&copied, &renamed).await.unwrap();
        let bytes = storage
            .get_opts(
                &renamed,
                GetOptions {
                    if_match: Some(e_tag),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));
    }

    #[tokio::test]
    async fn put_update_rejects_stale_version() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("stale-version");
        let put = storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        let err = storage
            .put_opts(
                &location,
                Bytes::from_static(b"def").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: put.e_tag,
                        version: Some("stale".to_string()),
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Precondition { .. }));
    }

    #[tokio::test]
    async fn put_update_requires_e_tag() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("missing-etag");
        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        let err = storage
            .put_opts(
                &location,
                Bytes::from_static(b"def").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: None,
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }));
    }

    #[tokio::test]
    async fn delete_nonexistent_reports_logical_path() {
        let root = TempDir::new().unwrap();
        let storage =
            MetaStoreBuilder::new(LocalFileSystem::new_with_prefix(root.path()).unwrap(), 100)
                .build();

        let err = storage
            .delete(&Path::from("missing/object"))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::NotFound { path, .. } if path == "missing/object"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn delete_tolerates_missing_metadata_and_heals_orphans() {
        let root = TempDir::new().unwrap();
        let storage =
            MetaStoreBuilder::new(LocalFileSystem::new_with_prefix(root.path()).unwrap(), 100)
                .build();
        let location = Path::from("orphan");

        // Orphaned data (metadata lost): delete succeeds and removes the data.
        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        storage
            .inner
            .store
            .delete(&Path::from("meta/orphan"))
            .await
            .unwrap();
        storage.delete(&location).await.unwrap();
        let err = storage
            .inner
            .store
            .get(&Path::from("data/orphan"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));

        // Orphaned metadata (data lost): delete reports NotFound for the data
        // object but still cleans up the metadata.
        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        storage
            .inner
            .store
            .delete(&Path::from("data/orphan"))
            .await
            .unwrap();
        let err = storage.delete(&location).await.unwrap_err();
        assert!(
            matches!(&err, Error::NotFound { path, .. } if path == "orphan"),
            "unexpected error: {err:?}"
        );
        let err = storage
            .inner
            .store
            .get(&Path::from("meta/orphan"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[tokio::test]
    async fn corrupted_metadata_heals_on_overwrite() {
        let inner = InMemory::new();
        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();
        let location = Path::from("self-heal");

        storage
            .put(&location, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        // Corrupt the sidecar metadata (e.g. a torn write before a crash).
        inner
            .put(
                &Path::from("meta/self-heal"),
                Bytes::from_static(b"\xffgarbage").into(),
            )
            .await
            .unwrap();

        // A fresh instance (bypassing the cache) cannot read the object...
        let reopened = MetaStoreBuilder::new(inner.clone(), 100).build();
        assert!(reopened.get(&location).await.is_err());

        // ...but a plain overwrite put must rebuild it.
        reopened
            .put(&location, Bytes::from_static(b"new").into())
            .await
            .unwrap();
        let bytes = reopened
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"new"));
    }

    #[tokio::test]
    async fn orphaned_data_does_not_fail_listings() {
        use futures::TryStreamExt;

        let inner = InMemory::new();
        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();
        let healthy = Path::from("list/healthy");
        let orphan = Path::from("list/orphan");

        storage
            .put(&healthy, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        storage
            .put(&orphan, Bytes::from_static(b"def").into())
            .await
            .unwrap();
        // Simulate a crash between the data and metadata writes.
        inner.delete(&Path::from("meta/list/orphan")).await.unwrap();

        let reopened = MetaStoreBuilder::new(inner.clone(), 100).build();
        let listed: Vec<_> = reopened
            .list(Some(&Path::from("list")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 2);
        for obj in &listed {
            if obj.location == orphan {
                assert_eq!(obj.e_tag, None);
            } else {
                assert!(obj.e_tag.is_some());
            }
        }

        let listed: Vec<_> = reopened
            .list_with_offset(Some(&Path::from("list")), &Path::from("list/a"))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 2);

        let rt = reopened
            .list_with_delimiter(Some(&Path::from("list")))
            .await
            .unwrap();
        assert_eq!(rt.objects.len(), 2);
        let orphaned = rt.objects.iter().find(|o| o.location == orphan).unwrap();
        assert_eq!(orphaned.e_tag, None);
    }

    #[tokio::test]
    async fn corrupted_metadata_does_not_fail_listings() {
        use futures::TryStreamExt;

        let inner = InMemory::new();
        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();
        let healthy = Path::from("clist/healthy");
        let corrupt = Path::from("clist/corrupt");

        storage
            .put(&healthy, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        storage
            .put(&corrupt, Bytes::from_static(b"def").into())
            .await
            .unwrap();
        // Simulate a torn sidecar write before a crash: the document exists
        // but no longer decodes.
        inner
            .put(
                &Path::from("meta/clist/corrupt"),
                Bytes::from_static(b"\xffgarbage").into(),
            )
            .await
            .unwrap();

        // A fresh instance (bypassing the cache) must list both objects; the
        // corrupted one is reported as an orphan (no logical e_tag), matching
        // the write path that treats it as absent and self-heals it.
        let reopened = MetaStoreBuilder::new(inner.clone(), 100).build();
        let listed: Vec<_> = reopened
            .list(Some(&Path::from("clist")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 2);
        for obj in &listed {
            if obj.location == corrupt {
                assert_eq!(obj.e_tag, None);
            } else {
                assert!(obj.e_tag.is_some());
            }
        }

        let listed: Vec<_> = reopened
            .list_with_offset(Some(&Path::from("clist")), &Path::from("clist/a"))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 2);

        let rt = reopened
            .list_with_delimiter(Some(&Path::from("clist")))
            .await
            .unwrap();
        assert_eq!(rt.objects.len(), 2);
        let orphaned = rt.objects.iter().find(|o| o.location == corrupt).unwrap();
        assert_eq!(orphaned.e_tag, None);

        // Reads still fail loudly (the listing tolerance must not mask the
        // corruption from readers), and an overwrite heals the object.
        assert!(reopened.get(&corrupt).await.is_err());
        reopened
            .put(&corrupt, Bytes::from_static(b"new").into())
            .await
            .unwrap();
        let listed: Vec<_> = reopened
            .list(Some(&Path::from("clist")))
            .try_collect()
            .await
            .unwrap();
        assert!(listed.iter().all(|o| o.e_tag.is_some()));
    }

    #[tokio::test]
    async fn create_heals_orphaned_data() {
        let inner = InMemory::new();
        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();
        let location = Path::from("create-heal");

        storage
            .put(&location, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        inner.delete(&Path::from("meta/create-heal")).await.unwrap();

        // The object no longer logically exists, so `Create` must succeed
        // over the orphaned data object.
        let reopened = MetaStoreBuilder::new(inner.clone(), 100).build();
        reopened
            .put_opts(
                &location,
                Bytes::from_static(b"new").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let bytes = reopened
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"new"));

        // A `Create` over a live object still fails.
        let err = reopened
            .put_opts(
                &location,
                Bytes::from_static(b"again").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn rename_and_copy_to_self_preserve_object() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("self-target");

        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        storage.rename(&location, &location).await.unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));

        let err = storage
            .rename_if_not_exists(&location, &location)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }));

        storage.copy(&location, &location).await.unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));

        // Self-rename of a missing object reports NotFound.
        let missing = Path::from("self-missing");
        let err = storage.rename(&missing, &missing).await.unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[tokio::test]
    async fn crash_between_data_and_meta_write_is_recoverable() {
        use futures::TryStreamExt;

        let (fault, handle) = crate::FaultStore::wrap(InMemory::new());
        let storage = MetaStoreBuilder::new(fault, 100).build();
        let location = Path::from("crash/object");

        // Fail the metadata write: the data object lands, the sidecar
        // metadata does not (crash window of the two-object put).
        handle.push_rule(crate::FaultRule::fail_once(crate::FaultOp::Put, "meta/"));
        let err = storage
            .put(&location, Bytes::from_static(b"v1").into())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("injected fault"));

        // The orphan reads as NotFound under the logical path...
        let err = storage.get(&location).await.unwrap_err();
        assert!(
            matches!(&err, Error::NotFound { path, .. } if path == "crash/object"),
            "unexpected error: {err:?}"
        );

        // ...does not fail listings (the recovery scan)...
        let listed: Vec<_> = storage
            .list(Some(&Path::from("crash")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].e_tag, None);
        let rt = storage
            .list_with_delimiter(Some(&Path::from("crash")))
            .await
            .unwrap();
        assert_eq!(rt.objects.len(), 1);

        // ...and both Overwrite and Create self-heal it.
        storage
            .put(&location, Bytes::from_static(b"v2").into())
            .await
            .unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v2"));
    }

    #[tokio::test]
    async fn concurrent_puts_to_same_key_stay_consistent() {
        let storage = Arc::new(MetaStoreBuilder::new(InMemory::new(), 100).build());
        let location = Path::from("put-race");

        let contents: Vec<Bytes> = (0..8u8)
            .map(|i| Bytes::from(vec![i; (i as usize + 1) * 3]))
            .collect();
        let mut tasks = Vec::new();
        for content in &contents {
            let storage = storage.clone();
            let location = location.clone();
            let content = content.clone();
            tasks.push(tokio::spawn(async move {
                storage.put(&location, content.into()).await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }

        // The winning put's data and metadata must agree.
        let res = storage.get(&location).await.unwrap();
        let e_tag = res.meta.e_tag.clone();
        let bytes = res.bytes().await.unwrap();
        assert!(contents.contains(&bytes));
        let expected = BASE64_URL_SAFE.encode(sha3_256(&bytes));
        assert_eq!(e_tag.as_deref(), Some(expected.as_str()));
    }

    #[tokio::test]
    async fn concurrent_multipart_completes_stay_consistent() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("multipart-race");
        let content_a = Bytes::from_static(b"aaaaaaaaaaaaaaaa");
        let content_b = Bytes::from_static(b"bbbbbbbb");

        let mut up_a = storage.put_multipart(&location).await.unwrap();
        let mut up_b = storage.put_multipart(&location).await.unwrap();
        up_a.put_part(content_a.clone().into()).await.unwrap();
        up_b.put_part(content_b.clone().into()).await.unwrap();

        let (ra, rb) = futures::join!(up_a.complete(), up_b.complete());
        ra.unwrap();
        rb.unwrap();

        // Whichever complete ran last, data and metadata must agree.
        let res = storage.get(&location).await.unwrap();
        let e_tag = res.meta.e_tag.clone();
        let bytes = res.bytes().await.unwrap();
        assert!(bytes == content_a || bytes == content_b);
        let expected = BASE64_URL_SAFE.encode(sha3_256(&bytes));
        assert_eq!(e_tag.as_deref(), Some(expected.as_str()));
    }

    #[tokio::test]
    async fn delete_waits_for_in_flight_put_on_the_same_key() {
        use crate::test_support::{GateOperation, GatedStore};

        let location = Path::from("delete-put-race");
        let (inner, gate) = GatedStore::new(GateOperation::Put(Path::from("data/delete-put-race")));
        let storage = Arc::new(MetaStoreBuilder::new(inner, 100).build());

        let put_storage = storage.clone();
        let put_location = location.clone();
        let put = tokio::spawn(async move {
            put_storage
                .put(&put_location, Bytes::from_static(b"new-value").into())
                .await
        });
        gate.wait_until_entered().await;

        let delete_storage = storage.clone();
        let delete_location = location.clone();
        let mut delete = tokio::spawn(async move { delete_storage.delete(&delete_location).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut delete)
                .await
                .is_err(),
            "delete bypassed the put's per-key mutation lease"
        );

        gate.release();
        put.await.unwrap().unwrap();
        delete.await.unwrap().unwrap();
        assert!(matches!(
            storage.get(&location).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn target_put_waits_for_copy_and_preserves_data_metadata_pair() {
        use crate::test_support::{GateOperation, GatedStore};

        let source = Path::from("copy-put-source");
        let target = Path::from("copy-put-target");
        let (inner, gate) = GatedStore::new(GateOperation::Copy {
            from: Path::from("data/copy-put-source"),
            to: Path::from("data/copy-put-target"),
        });
        let storage = Arc::new(MetaStoreBuilder::new(inner, 100).build());
        storage
            .put(&source, Bytes::from_static(b"source-value").into())
            .await
            .unwrap();

        let copy_storage = storage.clone();
        let copy_source = source.clone();
        let copy_target = target.clone();
        let copy = tokio::spawn(async move { copy_storage.copy(&copy_source, &copy_target).await });
        gate.wait_until_entered().await;

        let put_storage = storage.clone();
        let put_target = target.clone();
        let mut put = tokio::spawn(async move {
            put_storage
                .put(&put_target, Bytes::from_static(b"put-value").into())
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut put)
                .await
                .is_err(),
            "target put bypassed the copy's two-key mutation lease"
        );

        gate.release();
        copy.await.unwrap().unwrap();
        put.await.unwrap().unwrap();
        let result = storage.get(&target).await.unwrap();
        let e_tag = result.meta.e_tag.clone();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"put-value"));
        assert_eq!(e_tag, Some(BASE64_URL_SAFE.encode(sha3_256(&bytes))));
    }

    #[tokio::test]
    async fn rename_waits_for_target_multipart_complete() {
        use crate::test_support::{GateOperation, GatedStore};

        let source = Path::from("rename-multipart-source");
        let target = Path::from("rename-multipart-target");
        let (inner, gate) = GatedStore::new(GateOperation::MultipartComplete(Path::from(
            "data/rename-multipart-target",
        )));
        let storage = Arc::new(MetaStoreBuilder::new(inner, 100).build());
        storage
            .put(&source, Bytes::from_static(b"source-value").into())
            .await
            .unwrap();
        let mut upload = storage.put_multipart(&target).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"multipart-value").into())
            .await
            .unwrap();
        let complete = tokio::spawn(async move { upload.complete().await });
        gate.wait_until_entered().await;

        let rename_storage = storage.clone();
        let rename_source = source.clone();
        let rename_target = target.clone();
        let mut rename =
            tokio::spawn(
                async move { rename_storage.rename(&rename_source, &rename_target).await },
            );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut rename)
                .await
                .is_err(),
            "rename bypassed the multipart completion's target lease"
        );

        gate.release();
        complete.await.unwrap().unwrap();
        rename.await.unwrap().unwrap();
        let bytes = storage.get(&target).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"source-value"));
        assert!(matches!(
            storage.get(&source).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_with_local_file() {
        let root = TempDir::new().unwrap();
        let storage = MetaStoreBuilder::new(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            10000,
        )
        .build();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&storage, Some(location))
            .await
            .unwrap_err();
        if let crate::Error::NotFound { path, .. } = err {
            assert!(path.ends_with(NON_EXISTENT_NAME));
        } else {
            panic!("unexpected error type: {err:?}");
        }

        // put_get_delete_list(&storage).await;
        put_get_attributes(&storage).await;
        get_opts(&storage).await;
        put_opts(&storage, true).await;

        list_uses_directories_correctly(&storage).await;
        list_with_delimiter(&storage).await;
        rename_and_copy(&storage).await;
        copy_if_not_exists(&storage).await;
        copy_rename_nonexistent_object(&storage).await;
        multipart_race_condition(&storage, true).await;
        multipart_out_of_order(&storage).await;

        let root = TempDir::new().unwrap();
        let storage = MetaStoreBuilder::new(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            10000,
        )
        .build();
        stream_get(&storage).await;
    }
}
