//! # anda_object_store
//!
//! `anda_object_store` extends the [`object_store`] crate with two composable
//! wrappers that are used as the storage substrate for AndaDB and the AI memory
//! brain:
//!
//! - [`MetaStore`] — augments any [`ObjectStore`] backend with side-car
//!   metadata (object size, payload hash). This enables a uniform logical
//!   ETag and conditional `PutMode::Update` semantics on top of backends
//!   that lack them natively (notably
//!   `object_store::local::LocalFileSystem`).
//!
//! ## Logical ETag
//!
//! Both wrappers expose an ETag that identifies the **commit**, not the
//! bytes: it hashes the payload together with the generation minted for that
//! commit. Two commits of identical content therefore carry different ETags,
//! which is what makes `PutMode::Update` a compare-and-swap on the version
//! rather than on the content — an ETag that repeated for repeating content
//! would let a token captured before an A → B → A sequence still pass the
//! precondition (a lost update).
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
use chrono::{DateTime, Utc};
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

use sidecar::{
    InFlightGuard, ListingMetaPolicy, SidecarMeta, SidecarStore, logical_last_modified,
    new_commit_timestamp_ms, new_generation,
};

/// `MetaStore` is a wrapper around an `ObjectStore` implementation that adds metadata capabilities.
///
/// It stores metadata for each object in a separate location, which enables conditional updates
/// for storage backends that don't natively support them (like `LocalFileSystem`).
///
/// The metadata includes:
/// - Size of the object
/// - E-Tag (SHA3-256 over the generation and the content, unique per commit)
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

    /// Logical ETag: SHA3-256 over the generation followed by the payload,
    /// encoded as URL-safe Base64 (without padding). This ETag is what
    /// [`MetaStore`] exposes to callers via [`ObjectStore`] APIs and uses
    /// for `PutMode::Update` precondition checks; the generation makes it
    /// unique per commit (see the crate documentation).
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
        // Mint the generation and register it as in-flight before anything
        // reaches the backend: between the payload write and the pointer
        // switch nothing references it, so garbage collection must be told
        // to leave it alone. The guard is released when this call returns.
        let generation = new_generation();
        let _in_flight = self.inner.track_in_flight(location, &generation);
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

                // The logical ETag must be unique per commit: conditional
                // updates compare it as the CAS token, and hashing the bare
                // payload makes it repeat whenever the content does — a
                // reader holding the token for content A can then still pass
                // the precondition after A → B → A committed twice, which is
                // a lost update. Seeding the hash with the generation, minted
                // fresh for every commit, turns it into a commit identity.
                //
                // Hash segment-by-segment so multi-segment payloads are not
                // concatenated into a temporary contiguous buffer.
                let mut hasher = sha3::Sha3_256::new();
                hasher.update(generation.as_bytes());
                for segment in payload.iter() {
                    hasher.update(segment);
                }
                let hash: [u8; 32] = hasher.finalize().into();

                // Write the payload to the fresh immutable generation; the
                // metadata put below is the commit point.
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
                    generation: Some(generation.clone()),
                    committed_at_ms: Some(new_commit_timestamp_ms()),
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
        let in_flight = self.inner.track_in_flight(location, &generation);
        let gen_path = self.inner.generation_path(location, &generation);
        let inner = self.inner.store.put_multipart_opts(&gen_path, opts).await?;

        // Seed the running payload hasher with the generation so the logical
        // ETag is unique per commit; see `put_opts`.
        let mut hasher = sha3::Sha3_256::new();
        hasher.update(generation.as_bytes());
        Ok(Box::new(MetaStoreUploader {
            hasher,
            size: 0,
            location: location.clone(),
            generation,
            _in_flight: in_flight,
            store: self.inner.clone(),
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut retried = false;
        loop {
            let meta = self.inner.get_meta(location).await?;
            let mut options = options.clone();
            let last_modified =
                logical_last_modified(meta.committed_at_ms, meta.generation.as_deref());
            check_get_preconditions(location, &mut options, meta.e_tag.as_deref(), last_modified)?;

            let payload_path = self
                .inner
                .payload_path(location, meta.generation.as_deref());
            match self.inner.store.get_opts(&payload_path, options).await {
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
            .copy_payload(from, to, extensions, |_, _| Ok(()))
            .await?;
        self.inner
            .update_meta_with(to, create, async |_| {
                Ok(Metadata {
                    size: src.size,
                    e_tag: Some(derive_copy_e_tag(&generation, src.e_tag.as_deref())),
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
    /// Keeps that generation registered as in-flight for the whole upload,
    /// so garbage collection cannot reclaim it before `complete` commits the
    /// pointer
    _in_flight: InFlightGuard,
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
                    committed_at_ms: Some(new_commit_timestamp_ms()),
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
/// Test-only helper for building the fixtures of pre-0.10 layouts, whose
/// ETag was the bare hash of the payload.
#[cfg(test)]
pub(crate) fn sha3_256(data: &[u8]) -> [u8; 32] {
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Derives the target's logical ETag for a copy.
///
/// A copy publishes the source bytes under a *new* commit of a *different*
/// key, and the logical ETag identifies the commit rather than the bytes.
/// Propagating the source token would hand two keys the same CAS token, and
/// would hand the target a token it may already have retired — both let a
/// stale `PutMode::Update` precondition pass. Mixing in the freshly minted
/// generation makes the token unique per commit, exactly as a put does.
pub(crate) fn derive_copy_e_tag(generation: &str, source_e_tag: Option<&str>) -> String {
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(generation.as_bytes());
    hasher.update(source_e_tag.unwrap_or_default().as_bytes());
    let hash: [u8; 32] = hasher.finalize().into();
    BASE64_URL_SAFE.encode(hash)
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
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::{integration::*, local::LocalFileSystem, memory::InMemory};
    use std::sync::atomic::{AtomicBool, Ordering};
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

    /// The logical ETag the currently committed generation of `location` must
    /// carry for `payload`.
    ///
    /// The ETag is unique per commit — it hashes the generation together with
    /// the payload — so a test cannot derive it from the content alone;
    /// asserting against this value still proves that the committed metadata
    /// and the committed payload describe the same write.
    async fn committed_e_tag<T: ObjectStore>(
        storage: &MetaStore<T>,
        location: &Path,
        payload: &[u8],
    ) -> String {
        let meta = storage.inner.get_meta(location).await.unwrap();
        let mut hasher = sha3::Sha3_256::new();
        hasher.update(meta.generation.as_deref().unwrap_or_default().as_bytes());
        hasher.update(payload);
        let hash: [u8; 32] = hasher.finalize().into();
        BASE64_URL_SAFE.encode(hash)
    }

    /// Resolves the full backend path of `location`'s current payload.
    async fn payload_backend_path<T: ObjectStore>(storage: &MetaStore<T>, location: &Path) -> Path {
        let meta = storage.inner.get_meta(location).await.unwrap();
        storage
            .inner
            .payload_path(location, meta.generation.as_deref())
    }

    /// Where a [`Gate`] suspends the operation it is armed for.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum GateOp {
        /// Before the put reaches the backend, i.e. with the payload written
        /// and the pointer switch still pending.
        BeforePut,
        /// After the backend answered the get, i.e. with the caller holding a
        /// document a concurrent commit may replace at any moment.
        AfterGet,
    }

    /// Suspends exactly one matching backend operation until the test
    /// releases it, so writer/reader/collector interleavings can be driven
    /// deterministically instead of hoped for.
    struct Gate {
        op: GateOp,
        path: String,
        armed: AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl Gate {
        fn new(op: GateOp, path: &str) -> Arc<Self> {
            Arc::new(Self {
                op,
                path: path.to_string(),
                armed: AtomicBool::new(true),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            })
        }

        /// Suspends the caller if it is the operation this gate waits for.
        async fn check(&self, op: GateOp, location: &Path) {
            if op != self.op
                || !location.as_ref().contains(self.path.as_str())
                || !self.armed.swap(false, Ordering::SeqCst)
            {
                return;
            }
            self.entered.notify_one();
            self.release.notified().await;
        }

        /// Waits until the gated operation has reached the gate.
        async fn wait_entered(&self) {
            self.entered.notified().await;
        }

        /// Lets the suspended operation continue.
        fn release(&self) {
            self.release.notify_one();
        }
    }

    /// [`ObjectStore`] wrapper that applies a [`Gate`] to the backend.
    struct GateStore<T: ObjectStore> {
        inner: T,
        gate: Arc<Gate>,
    }

    impl<T: ObjectStore> std::fmt::Debug for GateStore<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GateStore({:?})", self.inner)
        }
    }

    impl<T: ObjectStore> std::fmt::Display for GateStore<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GateStore({})", self.inner)
        }
    }

    #[async_trait]
    impl<T: ObjectStore> ObjectStore for GateStore<T> {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.gate.check(GateOp::BeforePut, location).await;
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            let rt = self.inner.get_opts(location, options).await;
            self.gate.check(GateOp::AfterGet, location).await;
            rt
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

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Marker inserted into a request's [`Extensions`] to observe that the
    /// caller's context reaches the backend.
    #[derive(Clone, Debug, PartialEq)]
    struct Marker(&'static str);

    /// [`ObjectStore`] wrapper recording the [`Marker`] each backend copy
    /// carried.
    struct RecordingStore<T: ObjectStore> {
        inner: T,
        copies: Arc<std::sync::Mutex<Vec<Option<Marker>>>>,
    }

    impl<T: ObjectStore> std::fmt::Debug for RecordingStore<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecordingStore({:?})", self.inner)
        }
    }

    impl<T: ObjectStore> std::fmt::Display for RecordingStore<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecordingStore({})", self.inner)
        }
    }

    #[async_trait]
    impl<T: ObjectStore> ObjectStore for RecordingStore<T> {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(location, options).await
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

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.copies
                .lock()
                .unwrap()
                .push(options.extensions.get::<Marker>().cloned());
            self.inner.copy_opts(from, to, options).await
        }
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
    async fn copy_and_rename_mint_their_own_logical_etag() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let source = Path::from("copy-source");
        let copied = Path::from("copy-target");
        let renamed = Path::from("rename-target");
        let put = storage
            .put(&source, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        let e_tag = put.e_tag.unwrap();

        // A copy is a commit of its own: it must not hand the target the
        // source's CAS token, or the two keys would share one.
        storage.copy(&source, &copied).await.unwrap();
        let copied_meta = storage.head(&copied).await.unwrap();
        let copied_e_tag = copied_meta.e_tag.clone().unwrap();
        assert_ne!(copied_e_tag, e_tag);
        let err = storage
            .get_opts(
                &copied,
                GetOptions {
                    if_match: Some(e_tag.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }));

        // The target's own token addresses it, on both the read and the
        // write path.
        let bytes = storage
            .get_opts(
                &copied,
                GetOptions {
                    if_match: Some(copied_e_tag.clone()),
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
            .put_opts(
                &copied,
                Bytes::from_static(b"def").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: Some(e_tag.clone()),
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }));

        // A rename is a copy plus a delete, so the target is a new commit too.
        storage.rename(&copied, &renamed).await.unwrap();
        let renamed_e_tag = storage.head(&renamed).await.unwrap().e_tag.unwrap();
        assert_ne!(renamed_e_tag, e_tag);
        assert_ne!(renamed_e_tag, copied_e_tag);
        let bytes = storage
            .get_opts(
                &renamed,
                GetOptions {
                    if_match: Some(renamed_e_tag),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));

        // The source is untouched by either operation.
        assert_eq!(storage.head(&source).await.unwrap().e_tag, Some(e_tag));
    }

    #[tokio::test]
    async fn stale_cas_token_cannot_survive_an_aba_rewrite() {
        // The logical ETag identifies the commit, not the content: a token
        // captured before an A -> B -> A sequence must not pass the
        // `PutMode::Update` precondition afterwards, or the two intervening
        // writes are silently lost.
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("aba");

        let first = storage
            .put(&location, Bytes::from_static(b"A").into())
            .await
            .unwrap();
        let stale = first.e_tag.unwrap();
        let second = storage
            .put(&location, Bytes::from_static(b"B").into())
            .await
            .unwrap();
        let third = storage
            .put_opts(
                &location,
                Bytes::from_static(b"A").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: second.e_tag,
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        // Back to the original content, but a different commit.
        assert_ne!(third.e_tag.as_deref(), Some(stale.as_str()));

        let err = storage
            .put_opts(
                &location,
                Bytes::from_static(b"C").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: Some(stale.clone()),
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }));

        // The read path rejects it for the same reason.
        let err = storage
            .get_opts(
                &location,
                GetOptions {
                    if_match: Some(stale),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }));

        // The current token still commits.
        storage
            .put_opts(
                &location,
                Bytes::from_static(b"C").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: third.e_tag,
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"C"));

        // Multipart commits mint their own token as well.
        let mut upload = storage.put_multipart(&location).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"D").into())
            .await
            .unwrap();
        let one = upload.complete().await.unwrap();
        let mut upload = storage.put_multipart(&location).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"D").into())
            .await
            .unwrap();
        let two = upload.complete().await.unwrap();
        assert_ne!(one.e_tag, two.e_tag);
    }

    #[tokio::test]
    async fn get_opts_ignores_date_conditions_paired_with_etag_conditions() {
        // RFC 9110 §13.2.2: a present ETag condition makes the corresponding
        // date condition irrelevant, so it must not be forwarded to the
        // backend either — there it would be answered against the payload
        // object and fail a request the specification says must succeed.
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("etag-outranks-date");
        let put = storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        let e_tag = put.e_tag.unwrap();
        let head = storage.head(&location).await.unwrap();
        let before = head.last_modified - chrono::TimeDelta::hours(10);
        let after = head.last_modified + chrono::TimeDelta::hours(10);

        // `if_match` holds, so the failing `if_unmodified_since` is ignored.
        let bytes = storage
            .get_opts(
                &location,
                GetOptions {
                    if_match: Some(e_tag.clone()),
                    if_unmodified_since: Some(before),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));

        // `if_none_match` does not match, so the matching `if_modified_since`
        // is ignored instead of reporting NotModified.
        let bytes = storage
            .get_opts(
                &location,
                GetOptions {
                    if_none_match: Some("other".to_string()),
                    if_modified_since: Some(after),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));

        // On their own the date conditions still apply.
        let err = storage
            .get_opts(
                &location,
                GetOptions {
                    if_unmodified_since: Some(before),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }));
        let err = storage
            .get_opts(
                &location,
                GetOptions {
                    if_modified_since: Some(after),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotModified { .. }));
    }

    #[tokio::test]
    async fn every_api_reports_the_same_size_and_timestamp() {
        let inner = InMemory::new();
        let storage = MetaStoreBuilder::new(inner.clone(), 100).build();
        let location = Path::from("one-clock");
        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        let listed: Vec<_> = storage.list(None).try_collect().await.unwrap();
        assert_eq!(listed.len(), 1);
        let head = storage.head(&location).await.unwrap();
        assert_eq!(head.last_modified, listed[0].last_modified);
        assert_eq!(head.size, listed[0].size);
        assert_eq!(head.e_tag, listed[0].e_tag);
        let res = storage.get(&location).await.unwrap();
        assert_eq!(res.meta.last_modified, listed[0].last_modified);
        assert_eq!(res.meta.size, listed[0].size);

        // A timestamp taken from a listing therefore answers a conditional
        // read about the same commit.
        let err = storage
            .get_opts(
                &location,
                GetOptions {
                    if_modified_since: Some(listed[0].last_modified),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotModified { .. }));
        storage
            .get_opts(
                &location,
                GetOptions {
                    if_unmodified_since: Some(listed[0].last_modified),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // The reported size is the committed one: replacing the payload
        // behind the wrapper's back cannot change what `head` reports.
        let payload = payload_backend_path(&storage, &location).await;
        inner
            .put(&payload, Bytes::from_static(b"abcdefghij").into())
            .await
            .unwrap();
        assert_eq!(storage.head(&location).await.unwrap().size, 3);
    }

    #[tokio::test]
    async fn multipart_last_modified_is_the_commit_time() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("multipart-commit-time");
        let mut upload = storage.put_multipart(&location).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        let after_upload_started = chrono::Utc::now();
        tokio::time::sleep(Duration::from_millis(20)).await;
        upload.complete().await.unwrap();

        let head = storage.head(&location).await.unwrap();
        assert!(
            head.last_modified > after_upload_started,
            "last_modified must describe the metadata commit, not multipart creation"
        );
        storage
            .get_opts(
                &location,
                GetOptions {
                    if_modified_since: Some(after_upload_started),
                    ..Default::default()
                },
            )
            .await
            .expect("an object committed after the condition date is modified");
    }

    #[tokio::test]
    async fn copy_forwards_extensions_and_preserves_attributes() {
        let inner = InMemory::new();
        let recorder = Arc::new(std::sync::Mutex::new(Vec::<Option<Marker>>::new()));
        let storage = MetaStoreBuilder::new(
            RecordingStore {
                inner: inner.clone(),
                copies: recorder.clone(),
            },
            100,
        )
        .build();

        let attributes = Attributes::from_iter([(Attribute::ContentType, "text/plain")]);
        let source = Path::from("ext-source");
        let target = Path::from("ext-target");
        storage
            .put_opts(
                &source,
                Bytes::from_static(b"abc").into(),
                PutOptions {
                    attributes: attributes.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut extensions = Extensions::new();
        extensions.insert(Marker("copy"));
        storage
            .copy_opts(
                &source,
                &target,
                CopyOptions {
                    mode: CopyMode::Overwrite,
                    extensions,
                },
            )
            .await
            .unwrap();

        // The caller's request context reached the backend copy...
        assert_eq!(recorder.lock().unwrap().as_slice(), &[Some(Marker("copy"))]);
        // ...and `copy` + `get` answers with the same attributes as
        // `put` + `get`.
        let res = storage.get(&target).await.unwrap();
        assert_eq!(res.attributes, attributes);
        assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"abc"));

        // A rename forwards its own extensions the same way.
        let mut extensions = Extensions::new();
        extensions.insert(Marker("rename"));
        storage
            .rename_opts(
                &target,
                &Path::from("ext-renamed"),
                RenameOptions {
                    target_mode: RenameTargetMode::Overwrite,
                    extensions,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            recorder.lock().unwrap().as_slice(),
            &[Some(Marker("copy")), Some(Marker("rename"))]
        );
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
        // conditional updates rely on the per-commit logical e_tag.
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
            listed[0].e_tag,
            Some(committed_e_tag(&reopened, &location, b"v1").await)
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
    async fn collect_garbage_spares_uncommitted_generations() {
        // A put publishes its payload before its pointer, so a collection
        // that runs in between finds no commit point for the key. Reclaiming
        // the payload there would make the put return `Ok` for an object
        // whose payload is already gone — a silent data loss the commit-point
        // re-check cannot catch, because there is nothing to re-check yet.
        let inner = InMemory::new();
        let gate = Gate::new(GateOp::BeforePut, "meta/gc/object");
        let storage = Arc::new(
            MetaStoreBuilder::new(
                GateStore {
                    inner: inner.clone(),
                    gate: gate.clone(),
                },
                100,
            )
            .build(),
        );
        let location = Path::from("gc/object");

        // Real garbage, so the run below still has something to reclaim.
        inner
            .put(
                &Path::from("gen/gc/stale/0000000000000001-deadbeef"),
                Bytes::from_static(b"stale").into(),
            )
            .await
            .unwrap();

        let writer = {
            let storage = storage.clone();
            let location = location.clone();
            tokio::spawn(async move {
                storage
                    .put(&location, Bytes::from_static(b"v1").into())
                    .await
            })
        };
        // The payload is on the backend; the pointer switch is suspended.
        gate.wait_entered().await;

        // Age the generation past the collection's floor, so only the
        // in-flight registration can save it.
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(
            storage.collect_garbage().await.unwrap(),
            1,
            "only the planted stale generation is garbage"
        );

        gate.release();
        writer.await.unwrap().unwrap();

        // The committed pointer still resolves to a payload that exists.
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v1"));

        // The registration ends with the put: drop the commit point and the
        // payload is ordinary garbage again (a leaked entry would keep it
        // alive forever).
        let gen_path = payload_backend_path(&storage, &location).await;
        inner.delete(&Path::from("meta/gc/object")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(storage.collect_garbage().await.unwrap(), 1);
        assert!(matches!(
            inner.get(&gen_path).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn cold_read_cannot_resurrect_a_replaced_pointer() {
        // A reader resolves the commit point before it caches it. If that
        // insert is not serialized with the commits, a reader holding a
        // pre-commit document can land it *after* a newer one was committed,
        // pinning the previous version (its payload, size and ETag) for the
        // rest of the cache TTL.
        let inner = InMemory::new();
        let location = Path::from("race/object");

        // Seed v1 through a separate instance, so the store under test starts
        // with a cold cache for the key.
        let seed = MetaStoreBuilder::new(inner.clone(), 100).build();
        seed.put(&location, Bytes::from_static(b"v1").into())
            .await
            .unwrap();

        let (fault, handle) = crate::FaultStore::wrap(inner.clone());
        let gate = Gate::new(GateOp::AfterGet, "meta/race/object");
        let storage = Arc::new(
            MetaStoreBuilder::new(
                GateStore {
                    inner: fault,
                    gate: gate.clone(),
                },
                100,
            )
            .build(),
        );
        // Fail the (best-effort) cleanup of v1's generation, so a resurrected
        // pointer keeps resolving instead of being healed by the read path's
        // `NotFound` retry.
        handle.push_rule(crate::FaultRule::fail_once(crate::FaultOp::Delete, "gen/"));

        let reader = {
            let storage = storage.clone();
            let location = location.clone();
            tokio::spawn(async move { storage.get(&location).await?.bytes().await })
        };
        // The reader holds the v1 document and has not cached it yet.
        gate.wait_entered().await;

        let writer = {
            let storage = storage.clone();
            let location = location.clone();
            tokio::spawn(async move {
                storage
                    .put(&location, Bytes::from_static(b"v2-longer").into())
                    .await
            })
        };
        // Give the writer every chance to commit while the reader is parked.
        tokio::time::sleep(Duration::from_millis(50)).await;
        gate.release();

        // Either version is a legitimate answer for the reader itself; what
        // matters is that its document does not outlive the commit.
        let read = reader.await.unwrap().unwrap();
        assert!(read == Bytes::from_static(b"v1") || read == Bytes::from_static(b"v2-longer"));
        writer.await.unwrap().unwrap();

        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"v2-longer"));

        // Listings answer from the same documents, so a resurrected one also
        // reports the previous version's size and ETag.
        let listed: Vec<_> = storage
            .list(Some(&Path::from("race")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].size, 9);
        assert_eq!(
            listed[0].e_tag,
            Some(committed_e_tag(&storage, &location, b"v2-longer").await)
        );
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
    async fn cached_legacy_pointer_heals_after_migration() {
        // A cached legacy (pre-0.10, generation-less) document points at
        // `data/<location>`. The first overwrite — here through a second
        // instance, so the first one's cache does not see it — migrates the
        // key to the generation layout and deletes that payload. The stale
        // pointer must heal through the `NotFound` re-resolve like a stale
        // generational pointer does, not report the object missing until
        // the cache entry expires.
        let inner = InMemory::new();
        let location = Path::from("compat/legacy-stale");
        put_legacy_object(&inner, &location, b"legacy payload").await;

        // Warm one instance per resolving path, so each exercises its own
        // retry against a stale cache.
        let getter = MetaStoreBuilder::new(inner.clone(), 100).build();
        let ranger = MetaStoreBuilder::new(inner.clone(), 100).build();
        let copier = MetaStoreBuilder::new(inner.clone(), 100).build();
        for storage in [&getter, &ranger, &copier] {
            let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
            assert_eq!(bytes, Bytes::from_static(b"legacy payload"));
        }

        // The overwrite migrates the key and removes the legacy payload.
        let writer = MetaStoreBuilder::new(inner.clone(), 100).build();
        writer
            .put(&location, Bytes::from_static(b"upgraded").into())
            .await
            .unwrap();
        assert!(matches!(
            inner.get(&Path::from("data/compat/legacy-stale")).await,
            Err(Error::NotFound { .. })
        ));

        let res = getter.get(&location).await.unwrap();
        assert_eq!(
            res.meta.e_tag,
            Some(committed_e_tag(&getter, &location, b"upgraded").await)
        );
        let bytes = res.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"upgraded"));

        // One range covering the whole object, not a range-valued collection.
        #[allow(clippy::single_range_in_vec_init)]
        let ranges = ranger.get_ranges(&location, &[0..8]).await.unwrap();
        assert_eq!(ranges[0], Bytes::from_static(b"upgraded"));

        let target = Path::from("compat/legacy-copy");
        copier.copy(&location, &target).await.unwrap();
        let bytes = copier.get(&target).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"upgraded"));
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
            e_tag,
            Some(committed_e_tag(&fresh, &location, &bytes).await)
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
        assert_eq!(
            e_tag,
            Some(committed_e_tag(&storage, &location, &bytes).await)
        );

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
        assert_eq!(
            e_tag,
            Some(committed_e_tag(&storage, &location, &bytes).await)
        );
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
