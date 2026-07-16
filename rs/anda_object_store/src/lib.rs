//! # anda_object_store
//!
//! `anda_object_store` extends the [`object_store`] crate with two composable
//! wrappers that are used as the storage substrate for AndaDB and the AI memory
//! brain:
//!
//! - [`MetaStore`] — augments any [`ObjectStore`] backend with side-car
//!   metadata (object size, content hash). This enables a uniform,
//!   content-addressable ETag and conditional `PutMode::Update` semantics on
//!   top of backends that lack them natively (notably
//!   `object_store::local::LocalFileSystem`).
//! - [`EncryptedStore`] — provides transparent, chunked AES-256-GCM
//!   encryption-at-rest. Objects are split into fixed-size chunks, each
//!   encrypted with a per-chunk nonce derived from a random per-object base
//!   nonce. Encryption metadata (base nonce, per-chunk authentication tags)
//!   is stored alongside content metadata.
//!
//! ## Immutable-generation write protocol
//!
//! Both wrappers store a logical object as two backend objects:
//!
//! - `meta/<location>` — a small metadata document, the **only commit
//!   point**. It carries a pointer (the *generation*) to the payload.
//! - `gen/<location>/<generation>` — the immutable payload. Every put writes
//!   a fresh generation (a unique, never-overwritten path) and then commits
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
//! caller (AndaDB deploys one writer per store). Within one process the
//! per-key metadata critical section serializes writers; across processes a
//! second `PutMode::Create` writer is rejected by the backend's conditional
//! write of the commit point, but `Overwrite`/`Update` writers and the
//! garbage collector are only safe under the single-writer assumption.
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

use sidecar::{ListingMetaPolicy, SidecarMeta, SidecarStore, new_generation};

/// `MetaStore` is a wrapper around an `ObjectStore` implementation that adds metadata capabilities.
///
/// It stores metadata for each object in a separate location, which enables conditional updates
/// for storage backends that don't natively support them (like `LocalFileSystem`).
///
/// The metadata includes:
/// - Size of the object
/// - E-Tag (SHA3-256 hash of the content)
/// - The generation pointer to the immutable payload object
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
/// `meta/<location>`; it points at the immutable payload object at
/// `gen/<location>/<generation>` (or, for pre-0.10 documents without a
/// generation, at the legacy `data/<location>` object).
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
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for MetaStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let create = matches!(opts.mode, PutMode::Create);
        let rt = self
            .inner
            .update_meta_with(location, create, async |meta| {
                if let PutMode::Update(v) = &opts.mode {
                    match meta {
                        Some(m) => {
                            check_update_version(location, &m.e_tag, &m.generation, v)?;
                        }
                        None => {
                            return Err(Error::Precondition {
                                path: location.to_string(),
                                source: "metadata not found".into(),
                            });
                        }
                    }
                }

                // Hash segment-by-segment so multi-segment payloads are not
                // concatenated into a temporary contiguous buffer.
                let mut hasher = sha3::Sha3_256::new();
                for segment in payload.iter() {
                    hasher.update(segment);
                }
                let hash: [u8; 32] = hasher.finalize().into();

                // Write the payload to a fresh immutable generation; the
                // metadata put below is the commit point.
                let generation = new_generation();
                let gen_path = self.inner.generation_path(location, &generation);
                let mut data_opts = opts.clone();
                data_opts.mode = PutMode::Overwrite;
                self.inner
                    .store
                    .put_opts(&gen_path, payload.clone(), data_opts)
                    .await?;

                Ok(Metadata {
                    size: payload.content_length() as u64,
                    e_tag: Some(BASE64_URL_SAFE.encode(hash)),
                    original_tag: None,
                    original_version: None,
                    generation: Some(generation),
                })
            })
            .await?;

        Ok(PutResult {
            e_tag: rt.e_tag.clone(),
            version: None,
            extensions: Extensions::default(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // Upload into a fresh immutable generation; `complete` switches the
        // metadata pointer, so an unfinished upload never affects readers.
        let generation = new_generation();
        let gen_path = self.inner.generation_path(location, &generation);
        let inner = self.inner.store.put_multipart_opts(&gen_path, opts).await?;

        Ok(Box::new(MetaStoreUploader {
            hasher: sha3::Sha3_256::new(),
            size: 0,
            location: location.clone(),
            generation,
            store: self.inner.clone(),
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut retried = false;
        loop {
            let meta = self.inner.get_meta(location).await?;
            let mut options = options.clone();
            check_get_preconditions(location, &mut options, meta.e_tag.as_deref())?;

            let payload_path = self
                .inner
                .payload_path(location, meta.generation.as_deref());
            match self.inner.store.get_opts(&payload_path, options).await {
                Ok(mut res) => {
                    res.meta.location = location.clone();
                    res.meta.e_tag = meta.e_tag.clone();
                    // Versions are not reported: replaced generations are
                    // reclaimed eagerly, so version-addressed reads cannot be
                    // honoured. Conditional updates use the logical e_tag.
                    res.meta.version = None;
                    return Ok(res);
                }
                Err(Error::NotFound { source, .. }) => {
                    // The cached pointer may be stale and its generation
                    // already replaced and reclaimed; re-resolve once.
                    if !retried && meta.generation.is_some() {
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
                    if !retried && meta.generation.is_some() {
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
        let create = matches!(options.mode, CopyMode::Create);
        // Copy the payload into a fresh generation of the target; the
        // pointer switch below is the commit point.
        let (src, generation) = self.inner.copy_payload(from, to, |_, _| Ok(())).await?;
        self.inner
            .update_meta_with(to, create, async |_| {
                Ok(Metadata {
                    size: src.size,
                    e_tag: src.e_tag.clone(),
                    original_tag: None,
                    original_version: None,
                    generation: Some(generation.clone()),
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
        self.copy_opts(
            from,
            to,
            CopyOptions {
                mode,
                extensions: options.extensions,
            },
        )
        .await?;
        match self.inner.delete_object(from).await {
            Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

/// Handler for multipart uploads to a `MetaStore`.
///
/// This struct:
/// 1. Streams parts into a fresh immutable generation object
/// 2. Calculates a hash of the content
/// 3. Commits the metadata pointer when the upload completes
pub struct MetaStoreUploader<T: ObjectStore> {
    /// Hasher for calculating the content hash
    hasher: sha3::Sha3_256,
    /// Total size of the uploaded content
    size: usize,
    /// Logical path of the object
    location: Path,
    /// Generation the parts are uploaded into
    generation: String,
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

        // Materialize the generation object, then switch the metadata
        // pointer inside the per-key critical section. A failure (or crash)
        // before the switch leaves the previous version fully readable.
        let store = self.store.clone();
        let location = self.location.clone();
        let generation = self.generation.clone();
        let size = self.size as u64;
        let inner = &mut self.inner;
        store
            .update_meta_with(&location, false, async |_| {
                inner.complete().await?;
                Ok(Metadata {
                    size,
                    e_tag: e_tag.clone(),
                    original_tag: None,
                    original_version: None,
                    generation: Some(generation.clone()),
                })
            })
            .await?;

        Ok(PutResult {
            e_tag,
            version: None,
            extensions: Extensions::default(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

/// Computes the SHA3-256 hash of `data` and returns it as a 32-byte array.
///
/// Used by [`MetaStore`] to derive a content-addressable ETag, and by
/// [`crate::encryption::EncryptedStore`] to hash the produced ciphertext.
#[cfg(test)]
pub(crate) fn sha3_256(data: &[u8]) -> [u8; 32] {
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(data);
    hasher.finalize().into()
}

fn check_update_version(
    location: &Path,
    current_e_tag: &Option<String>,
    current_generation: &Option<String>,
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

    // Versions are not reported by this store (replaced generations are
    // reclaimed eagerly), so a caller-provided version precondition can only
    // come from a stale or foreign source; it is checked against the
    // internal generation and therefore never matches.
    if let Some(version) = &update.version
        && current_generation.as_ref() != Some(version)
    {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: format!(
                "{:?} does not match {:?}",
                current_generation, update.version
            )
            .into(),
        });
    }

    Ok(())
}

/// Evaluates `if_match` / `if_none_match` against the logical
/// (content-addressable) ETag and strips them from the request: the payload
/// object is immutable, so once the precondition holds against the current
/// metadata there is nothing further for the backend to check.
fn check_get_preconditions(
    location: &Path,
    options: &mut GetOptions,
    logical_e_tag: Option<&str>,
) -> Result<()> {
    let e_tag = logical_e_tag.unwrap_or("*");

    if let Some(if_match) = options.if_match.take()
        && if_match != "*"
        && if_match.split(',').map(str::trim).all(|tag| tag != e_tag)
    {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: format!("{e_tag} does not match {if_match}").into(),
        });
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
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::{integration::*, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    /// Serialization shape of the pre-0.10 mutable dual-object layout: the
    /// `o`/`v` fields were always present and there was no generation.
    #[derive(Serialize)]
    struct LegacyMetadata {
        #[serde(rename = "s")]
        size: u64,
        #[serde(rename = "e")]
        e_tag: Option<String>,
        #[serde(rename = "o")]
        original_tag: Option<String>,
        #[serde(rename = "v")]
        original_version: Option<String>,
    }

    /// Writes an object in the legacy (pre-0.10) layout directly into the
    /// backend: payload at `data/<location>`, metadata without a generation.
    async fn put_legacy_object<T: ObjectStore>(inner: &T, location: &Path, payload: &'static [u8]) {
        let put = inner
            .put(
                &Path::from(format!("data/{location}")),
                Bytes::from_static(payload).into(),
            )
            .await
            .unwrap();
        let meta = LegacyMetadata {
            size: payload.len() as u64,
            e_tag: Some(BASE64_URL_SAFE.encode(sha3_256(payload))),
            original_tag: put.e_tag,
            original_version: put.version,
        };
        let mut buf = Vec::new();
        cbor2::to_writer(&meta, &mut buf).unwrap();
        inner
            .put(&Path::from(format!("meta/{location}")), buf.into())
            .await
            .unwrap();
    }

    /// Resolves the full backend path of `location`'s current payload.
    async fn payload_backend_path<T: ObjectStore>(storage: &MetaStore<T>, location: &Path) -> Path {
        let meta = storage.inner.get_meta(location).await.unwrap();
        storage
            .inner
            .payload_path(location, meta.generation.as_deref())
    }

    #[test]
    fn builder_display_debug_and_path_helpers_are_exercised() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100)
            .with_meta_cache_ttl(Duration::from_secs(1))
            .build();

        assert!(format!("{storage}").contains("MetaStore"));
        assert!(format!("{storage:?}").contains("MetaStore"));

        let location = Path::from("nested/object");
        assert_eq!(
            storage.inner.meta_path(&location).to_string(),
            "meta/nested/object"
        );
        assert_eq!(
            storage.inner.legacy_path(&location).to_string(),
            "data/nested/object"
        );
        assert_eq!(
            storage
                .inner
                .generation_path(&location, "0123-abcd")
                .to_string(),
            "gen/nested/object/0123-abcd"
        );
        assert_eq!(
            storage.inner.payload_path(&location, None),
            storage.inner.legacy_path(&location)
        );
        assert_eq!(
            storage.inner.payload_path(&location, Some("0123-abcd")),
            storage.inner.generation_path(&location, "0123-abcd")
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
        // A legacy payload without a commit point does not logically exist.
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
    async fn copy_and_rename_preserve_logical_etag_preconditions() {
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
    async fn versions_are_not_reported() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("versioned");

        // Replaced generations are reclaimed eagerly, so version-addressed
        // reads cannot be honoured; no operation reports a version and
        // conditional updates rely on the content-addressable e_tag.
        let put = storage
            .put(&location, Bytes::from_static(b"v1").into())
            .await
            .unwrap();
        assert_eq!(put.version, None);

        let res = storage.get(&location).await.unwrap();
        assert_eq!(res.meta.version, None);
        let listed: Vec<_> = storage.list(None).try_collect().await.unwrap();
        assert_eq!(listed[0].version, None);

        // An e_tag-only Update succeeds; any version precondition fails.
        storage
            .put_opts(
                &location,
                Bytes::from_static(b"v2").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: put.e_tag,
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
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
    async fn delete_removes_commit_point_and_payload() {
        let inner = InMemory::new();
        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();
        let location = Path::from("delete-me");

        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        let payload = payload_backend_path(&storage, &location).await;

        storage.delete(&location).await.unwrap();
        assert!(matches!(
            inner.get(&Path::from("meta/delete-me")).await,
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(
            inner.get(&payload).await,
            Err(Error::NotFound { .. })
        ));

        // A payload without a commit point does not logically exist, so
        // deleting it reports NotFound; garbage collection reclaims it.
        inner
            .put(
                &Path::from("data/orphan-legacy"),
                Bytes::from_static(b"zzz").into(),
            )
            .await
            .unwrap();
        let err = storage
            .delete(&Path::from("orphan-legacy"))
            .await
            .unwrap_err();
        assert!(matches!(&err, Error::NotFound { path, .. } if path == "orphan-legacy"));
        assert_eq!(storage.collect_garbage().await.unwrap(), 1);
        assert!(matches!(
            inner.get(&Path::from("data/orphan-legacy")).await,
            Err(Error::NotFound { .. })
        ));

        // Deleting a key whose payload is already gone still succeeds: the
        // commit point is the source of truth.
        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        let payload = payload_backend_path(&storage, &location).await;
        inner.delete(&payload).await.unwrap();
        storage.delete(&location).await.unwrap();
        assert!(matches!(
            storage.get(&location).await,
            Err(Error::NotFound { .. })
        ));
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
        // Corrupt the commit point (external corruption; backend puts are
        // atomic in the crash model).
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
    async fn create_over_corrupted_metadata_heals() {
        let inner = InMemory::new();
        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();
        let location = Path::from("create-heal");

        storage
            .put(&location, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        inner
            .put(
                &Path::from("meta/create-heal"),
                Bytes::from_static(b"\xffgarbage").into(),
            )
            .await
            .unwrap();

        // The object is unreadable, so `Create` treats it as absent and
        // rebuilds it.
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
    async fn listing_skips_corrupt_commit_points_and_orphans() {
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
        inner
            .put(
                &Path::from("meta/clist/corrupt"),
                Bytes::from_static(b"\xffgarbage").into(),
            )
            .await
            .unwrap();
        // An uncommitted generation (e.g. from a crash before the pointer
        // switch) is invisible to listings by construction.
        inner
            .put(
                &Path::from("gen/clist/orphan/0000000000000001-00000000"),
                Bytes::from_static(b"ghost").into(),
            )
            .await
            .unwrap();

        let reopened = MetaStoreBuilder::new(inner.clone(), 100).build();
        let listed: Vec<_> = reopened
            .list(Some(&Path::from("clist")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, healthy);
        assert!(listed[0].e_tag.is_some());
        assert_eq!(listed[0].size, 3);

        let listed: Vec<_> = reopened
            .list_with_offset(Some(&Path::from("clist")), &Path::from("clist/a"))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);

        let rt = reopened
            .list_with_delimiter(Some(&Path::from("clist")))
            .await
            .unwrap();
        assert_eq!(rt.objects.len(), 1);

        // Reads of the corrupted key still fail loudly (the listing
        // tolerance must not mask the corruption), and an overwrite heals it.
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
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().all(|o| o.e_tag.is_some()));
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
    async fn crash_before_pointer_switch_preserves_old_version() {
        let inner = InMemory::new();
        let (fault, handle) = crate::FaultStore::wrap(inner.clone());
        let storage = MetaStoreBuilder::new(fault, 100).build();
        let location = Path::from("crash/object");

        storage
            .put(&location, Bytes::from_static(b"v1").into())
            .await
            .unwrap();

        // Fail the commit point write of the overwrite: the new generation
        // lands, the pointer switch does not (crash window of the put).
        handle.push_rule(crate::FaultRule::fail_once(crate::FaultOp::Put, "meta/"));
        let err = storage
            .put(&location, Bytes::from_static(b"v2").into())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("injected fault"));

        // The old version stays fully readable — through the warm cache...
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v1"));

        // ...and after a "reboot" (fresh instance, cold cache).
        let reopened = MetaStoreBuilder::new(inner.clone(), 100).build();
        let bytes = reopened
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v1"));

        // Listings show exactly the committed object.
        let listed: Vec<_> = reopened
            .list(Some(&Path::from("crash")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].e_tag.as_deref(),
            Some(BASE64_URL_SAFE.encode(sha3_256(b"v1")).as_str())
        );

        // The abandoned generation is garbage; collection reclaims it and
        // the object remains intact. (Generations minted in the same
        // millisecond as the collection start are conservatively skipped,
        // hence the sleep.)
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(reopened.collect_garbage().await.unwrap(), 1);
        let bytes = reopened
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v1"));

        // The next write succeeds normally.
        storage
            .put(&location, Bytes::from_static(b"v3").into())
            .await
            .unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v3"));
    }

    #[tokio::test]
    async fn crash_after_pointer_switch_serves_new_version() {
        let (fault, handle) = crate::FaultStore::wrap(InMemory::new());
        let storage = MetaStoreBuilder::new(fault, 100).build();
        let location = Path::from("crash/object");

        storage
            .put(&location, Bytes::from_static(b"v1").into())
            .await
            .unwrap();

        // Fail the best-effort cleanup of the replaced generation: the
        // pointer switch has already committed, so the put succeeds.
        handle.push_rule(crate::FaultRule::fail_once(crate::FaultOp::Delete, "gen/"));
        storage
            .put(&location, Bytes::from_static(b"v2").into())
            .await
            .unwrap();

        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v2"));

        // The replaced generation survived the failed cleanup; garbage
        // collection reclaims exactly it (after the same-millisecond
        // in-flight guard has lapsed).
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(storage.collect_garbage().await.unwrap(), 1);
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v2"));
        assert_eq!(storage.collect_garbage().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn collect_garbage_preserves_referenced_payloads() {
        let inner = InMemory::new();
        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();

        // A generation-layout object and a legacy-layout object.
        let modern = Path::from("gc/modern");
        let legacy = Path::from("gc/legacy");
        storage
            .put(&modern, Bytes::from_static(b"modern").into())
            .await
            .unwrap();
        put_legacy_object(&inner, &legacy, b"legacy").await;

        // Plant garbage: an unreferenced old generation and an orphaned
        // legacy payload without a commit point.
        inner
            .put(
                // Outside the managed prefixes: must never be touched.
                &Path::from("gc-noise/modern"),
                Bytes::from_static(b"noise").into(),
            )
            .await
            .unwrap();
        inner
            .put(
                &Path::from("gen/gc/modern/0000000000000001-deadbeef"),
                Bytes::from_static(b"stale-gen").into(),
            )
            .await
            .unwrap();
        inner
            .put(
                &Path::from("data/gc/orphan"),
                Bytes::from_static(b"orphan").into(),
            )
            .await
            .unwrap();
        // An in-flight generation (timestamp in the future) must survive.
        inner
            .put(
                &Path::from("gen/gc/inflight/ffffffffffffffff-00000000"),
                Bytes::from_static(b"inflight").into(),
            )
            .await
            .unwrap();

        let deleted = storage.collect_garbage().await.unwrap();
        assert_eq!(deleted, 2, "stale generation + orphaned legacy payload");

        // Referenced payloads and unknown/in-flight objects are untouched.
        let bytes = storage.get(&modern).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"modern"));
        let bytes = storage.get(&legacy).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"legacy"));
        assert!(inner.get(&Path::from("gc-noise/modern")).await.is_ok());
        assert!(
            inner
                .get(&Path::from("gen/gc/inflight/ffffffffffffffff-00000000"))
                .await
                .is_ok()
        );
        assert!(matches!(
            inner
                .get(&Path::from("gen/gc/modern/0000000000000001-deadbeef"))
                .await,
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(
            inner.get(&Path::from("data/gc/orphan")).await,
            Err(Error::NotFound { .. })
        ));

        // A key whose commit point is corrupted keeps all its payloads.
        inner
            .put(
                &Path::from("meta/gc/modern"),
                Bytes::from_static(b"\xffgarbage").into(),
            )
            .await
            .unwrap();
        let reopened = MetaStoreBuilder::new(inner.clone(), 100).build();
        assert_eq!(reopened.collect_garbage().await.unwrap(), 0);

        // Idempotent once everything is clean.
        let clean = MetaStoreBuilder::new(InMemory::new(), 100).build();
        clean
            .put(&modern, Bytes::from_static(b"x").into())
            .await
            .unwrap();
        assert_eq!(clean.collect_garbage().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn legacy_layout_readable_and_upgraded_on_overwrite() {
        let inner = InMemory::new();
        let location = Path::from("compat/legacy");
        put_legacy_object(&inner, &location, b"legacy payload").await;

        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();

        // Reads, range reads and listings all work on the legacy layout.
        let res = storage.get(&location).await.unwrap();
        assert_eq!(
            res.meta.e_tag.as_deref(),
            Some(BASE64_URL_SAFE.encode(sha3_256(b"legacy payload")).as_str())
        );
        assert_eq!(res.meta.version, None); // legacy: no generation
        let bytes = res.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"legacy payload"));

        let requested = 0..6;
        let ranges = storage
            .get_ranges(&location, std::slice::from_ref(&requested))
            .await
            .unwrap();
        assert_eq!(ranges[0], Bytes::from_static(b"legacy"));

        let listed: Vec<_> = storage
            .list(Some(&Path::from("compat")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].size, 14);

        // The first overwrite migrates the key to the generation layout and
        // removes the legacy payload.
        storage
            .put(&location, Bytes::from_static(b"upgraded").into())
            .await
            .unwrap();
        let meta = storage.inner.get_meta(&location).await.unwrap();
        assert!(meta.generation.is_some());
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"upgraded"));
        assert!(matches!(
            inner.get(&Path::from("data/compat/legacy")).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn create_is_arbitrated_across_instances() {
        let inner = InMemory::new();
        let a = Arc::new(MetaStoreBuilder::new(inner.clone(), 100).build());
        let b = Arc::new(MetaStoreBuilder::new(inner.clone(), 100).build());
        let location = Path::from("create-race");

        // Two independent instances (separate caches, shared backend) race
        // `Create` on the same key: the backend's conditional write of the
        // commit point admits exactly one winner.
        let mut tasks = Vec::new();
        for (i, storage) in [a.clone(), b.clone(), a.clone(), b.clone()]
            .into_iter()
            .enumerate()
        {
            let location = location.clone();
            tasks.push(tokio::spawn(async move {
                storage
                    .put_opts(
                        &location,
                        Bytes::from(vec![i as u8; 4]).into(),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
            }));
        }
        let mut winners = 0;
        for task in tasks {
            match task.await.unwrap() {
                Ok(_) => winners += 1,
                Err(Error::AlreadyExists { .. }) => {}
                Err(err) => panic!("unexpected error: {err:?}"),
            }
        }
        assert_eq!(winners, 1);

        // The winner's committed payload and metadata agree.
        let fresh = MetaStoreBuilder::new(inner, 100).build();
        let res = fresh.get(&location).await.unwrap();
        let e_tag = res.meta.e_tag.clone();
        let bytes = res.bytes().await.unwrap();
        assert_eq!(
            e_tag.as_deref(),
            Some(BASE64_URL_SAFE.encode(sha3_256(&bytes)).as_str())
        );
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

        // The winning put's payload and metadata must agree.
        let res = storage.get(&location).await.unwrap();
        let e_tag = res.meta.e_tag.clone();
        let bytes = res.bytes().await.unwrap();
        assert!(contents.contains(&bytes));
        let expected = BASE64_URL_SAFE.encode(sha3_256(&bytes));
        assert_eq!(e_tag.as_deref(), Some(expected.as_str()));

        // The object stays intact across garbage collection.
        storage.collect_garbage().await.unwrap();
        let res = storage.get(&location).await.unwrap();
        let bytes = res.bytes().await.unwrap();
        assert!(contents.contains(&bytes));
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

        // Whichever complete committed last, payload and metadata agree.
        let res = storage.get(&location).await.unwrap();
        let e_tag = res.meta.e_tag.clone();
        let bytes = res.bytes().await.unwrap();
        assert!(bytes == content_a || bytes == content_b);
        let expected = BASE64_URL_SAFE.encode(sha3_256(&bytes));
        assert_eq!(e_tag.as_deref(), Some(expected.as_str()));
    }

    #[tokio::test]
    async fn multipart_crash_before_complete_preserves_old_version() {
        let (fault, handle) = crate::FaultStore::wrap(InMemory::new());
        let storage = MetaStoreBuilder::new(fault, 100).build();
        let location = Path::from("multipart-crash");

        storage
            .put(&location, Bytes::from_static(b"v1").into())
            .await
            .unwrap();

        let mut upload = storage.put_multipart(&location).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"v2-multipart").into())
            .await
            .unwrap();
        // Fail the pointer switch; the upload reports failure and the old
        // version remains committed.
        handle.push_rule(crate::FaultRule::fail_once(crate::FaultOp::Put, "meta/"));
        assert!(upload.complete().await.is_err());

        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v1"));
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

    #[tokio::test]
    async fn local_file_legacy_layout_upgrade() {
        // On a real filesystem the legacy payload (`data/<key>`, a file) and
        // the generation tree (`gen/<key>/…`, a directory) must coexist
        // during migration — this is why generations live under their own
        // prefix.
        let root = TempDir::new().unwrap();
        let inner = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let location = Path::from("compat/legacy");
        put_legacy_object(&inner, &location, b"legacy payload").await;

        let storage = MetaStoreBuilder::new(inner, 100).build();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"legacy payload"));

        storage
            .put(&location, Bytes::from_static(b"upgraded").into())
            .await
            .unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"upgraded"));
        assert_eq!(storage.collect_garbage().await.unwrap(), 0);
    }
}
