//! Crate-internal generic core shared by [`MetaStore`](crate::MetaStore) and
//! [`EncryptedStore`](crate::EncryptedStore).
//!
//! Both wrappers follow the same *immutable-generation* layout on the
//! underlying backend:
//!
//! - `meta/<location>` — a small CBOR-encoded metadata document. It is the
//!   **only commit point**: a logical object exists iff its metadata document
//!   exists, and the document carries a pointer (the *generation*) to the
//!   payload object.
//! - `gen/<location>/<generation>` — the payload. Generation objects are
//!   immutable after publication. Every write targets a fresh path named by a
//!   timestamped 128-bit random ID, so payload writes need no conditional
//!   create. Replaced generations are deleted best-effort after the pointer
//!   switch and otherwise reclaimed by [`SidecarStore::collect_garbage`].
//! - `data/<location>` — the *legacy* payload location used by the mutable
//!   dual-object layout of anda_object_store < 0.10. Metadata without a
//!   generation pointer refers to this path; the first overwrite of such a
//!   key migrates it to the generation layout.
//!
//! A logical put therefore is: write the payload to a fresh immutable
//! generation, then atomically switch the metadata pointer (one backend put).
//! Readers resolve the pointer and read an immutable object, so torn
//! "old metadata + new payload" reads are impossible by construction, and a
//! crash before the pointer switch leaves the previous version fully intact
//! and readable.
//!
//! [`SidecarStore`] implements everything that depends only on this layout —
//! path mapping, the cached metadata pipeline, the commit protocol
//! ([`SidecarStore::update_meta_with`]), delete, listing, and garbage
//! collection — generically over the concrete metadata type
//! ([`SidecarMeta`]). Hashing, encryption/decryption and metadata
//! authentication stay in the wrappers.

pub(crate) use crate::generation::new_generation;
use crate::{
    generation::{generation_timestamp_ms, unix_ms},
    limits::{GarbageCollectionOptions, MetadataLimits, limit_error},
};
use cbor2::{from_reader, to_writer};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use moka::{future::Cache, ops::compute::Op};
use object_store::{path::Path, *};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};

type MetadataValidator<M> = dyn Fn(&Path, &M) -> Result<()> + Send + Sync;

/// Sidecar metadata document maintained by [`SidecarStore`] for every object.
///
/// Implemented by the `Metadata` types of `MetaStore` and `EncryptedStore`.
/// The serialized representation is owned entirely by the implementor, so
/// each wrapper keeps its existing (and distinct) compact CBOR format.
pub(crate) trait SidecarMeta: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Store name used in error messages (e.g. `"MetaStore"`).
    const STORE_NAME: &'static str;

    /// The logical ETag exposed to callers; unique per commit.
    fn e_tag(&self) -> Option<&str>;

    /// Size of the logical object in bytes, reported in listings.
    fn size(&self) -> u64;

    /// The generation this document points to. `None` means the legacy
    /// (pre-0.10) layout: the payload lives directly at `data/<location>`.
    fn generation(&self) -> Option<&str>;

    /// Millisecond timestamp captured immediately before publishing this
    /// metadata commit point. Absent on older metadata.
    fn committed_at_ms(&self) -> Option<u64>;
}

/// A cancelled commit can already have reached the backend, and `Drop`
/// cannot run the async per-key eviction, so it evicts the whole shared cache
/// synchronously. Explicit errors evict only their key; successful writes
/// keep the cache hot.
struct CommitGuard<'a, M: Send + Sync + 'static> {
    cache: &'a Cache<Path, Arc<M>>,
    armed: bool,
}

impl<M: Send + Sync + 'static> Drop for CommitGuard<'_, M> {
    fn drop(&mut self) {
        if self.armed {
            self.cache.invalidate_all();
        }
    }
}

pub(crate) struct CommitResult<M> {
    pub(crate) meta: Arc<M>,
    pub(crate) extensions: Extensions,
}

impl<M> std::ops::Deref for CommitResult<M> {
    type Target = M;
    fn deref(&self) -> &M {
        &self.meta
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommitIdentity {
    e_tag: Option<String>,
    generation: Option<String>,
}

impl CommitIdentity {
    fn from_meta<M: SidecarMeta>(meta: &M) -> Self {
        Self {
            e_tag: meta.e_tag().map(String::from),
            generation: meta.generation().map(String::from),
        }
    }
}

/// `None` means publication has not reached the point where it observed the
/// current commit. `Some(None)` records that the logical key was absent.
pub(crate) type PublicationBaseline = Option<Option<CommitIdentity>>;

pub(crate) fn new_commit_timestamp_ms() -> u64 {
    unix_ms()
}

/// The caller-visible `last_modified` of a logical object: the instant its
/// metadata commit point was prepared for publication.
///
/// Every API must report the same timestamp for the same logical object —
/// listings and reads resolve *different* backend objects (`meta/<loc>` and
/// the payload), whose own timestamps differ by however long the write took,
/// so neither can serve as the shared basis. The explicit timestamp lives in
/// the commit point and, for `EncryptedStore`, is covered by the metadata
/// authentication tag.
///
/// Returns `None` for pre-0.10 documents, which carry no generation (and for
/// foreign generation identifiers). Callers then fall back to the backend
/// timestamp of the object they resolved, and leave date preconditions to
/// the backend so both stay on the same clock. Metadata written by early
/// 0.11 builds has a generation but no explicit commit timestamp; it falls
/// back to the generation timestamp for compatibility.
pub(crate) fn logical_last_modified(
    committed_at_ms: Option<u64>,
    generation: Option<&str>,
) -> Option<DateTime<Utc>> {
    committed_at_ms
        .or_else(|| generation.and_then(generation_timestamp_ms))
        .and_then(|ms| DateTime::from_timestamp_millis(ms as i64))
}

/// The caller-visible description of a logical object: the size, ETag and
/// timestamp come from its commit point, never from the payload object it
/// resolves to. `backend_modified` is the timestamp of the backend object
/// that was resolved, used only for pre-0.10 documents (see
/// [`logical_last_modified`]). Versions are not reported: replaced
/// generations are reclaimed eagerly, so version-addressed reads cannot be
/// honoured, and conditional updates use the per-commit logical ETag.
pub(crate) fn logical_object_meta<M: SidecarMeta>(
    location: &Path,
    meta: &M,
    backend_modified: DateTime<Utc>,
) -> ObjectMeta {
    ObjectMeta {
        location: location.clone(),
        last_modified: logical_last_modified(meta.committed_at_ms(), meta.generation())
            .unwrap_or(backend_modified),
        size: meta.size(),
        e_tag: meta.e_tag().map(String::from),
        version: None,
    }
}

/// Answers a HEAD request from the commit point alone. A logical object
/// exists iff its commit point does, which is also what listings report.
pub(crate) fn head_result(meta: ObjectMeta) -> GetResult {
    GetResult {
        payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
        range: 0..meta.size,
        meta,
        attributes: Attributes::default(),
        extensions: Extensions::default(),
    }
}

/// The set of payloads that have been (or are being) written but whose
/// pointer is not committed yet, keyed by `(location, generation)`.
type InFlightSet = Arc<Mutex<HashMap<Path, HashSet<String>>>>;

/// Locks the in-flight registry, ignoring poisoning: the guarded set is a
/// plain [`HashSet`] whose critical sections cannot panic, and [`Drop`] of an
/// [`InFlightGuard`] must not panic while unwinding.
fn lock_in_flight(set: &InFlightSet) -> MutexGuard<'_, HashMap<Path, HashSet<String>>> {
    set.lock().unwrap_or_else(|err| err.into_inner())
}

/// RAII registration of an in-flight generation; see
/// [`SidecarStore::track_in_flight`]. Dropping it unregisters the generation,
/// so an early `?` or a panic on the commit path releases the registration
/// instead of leaking it.
pub(crate) struct InFlightGuard {
    in_flight: InFlightSet,
    key: (Path, String),
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut active = lock_in_flight(&self.in_flight);
        if let Some(generations) = active.get_mut(&self.key.0) {
            generations.remove(&self.key.1);
            if generations.is_empty() {
                active.remove(&self.key.0);
            }
        }
    }
}

/// A key's committed document as seen inside its per-key critical section.
enum Committed<M> {
    Missing,
    /// The document exists but does not decode or validate.
    Invalid(Error),
    Valid(Arc<M>),
}

/// What a committed metadata document says about its key's payload, as
/// gathered by the garbage collector's mark phase.
enum PayloadRef {
    Missing,
    /// Points at `gen/<location>/<generation>`.
    Generation(String),
    /// Legacy layout: points at `data/<location>`.
    Legacy,
    /// The document exists but cannot be decoded; keep every payload of the
    /// key (conservative).
    Unknown,
}

impl PayloadRef {
    fn references(&self, generation: Option<&str>) -> bool {
        match self {
            Self::Unknown => true,
            Self::Generation(current) => generation == Some(current.as_str()),
            Self::Legacy => generation.is_none(),
            Self::Missing => false,
        }
    }
}

/// Generic immutable-generation store core.
///
/// Owns the underlying [`ObjectStore`], the path prefixes and the metadata
/// cache, and provides the commit protocol plus the structurally identical
/// [`ObjectStore`] operations on top of them. The wrappers hold it behind an
/// [`Arc`] so the `'static` streams returned by
/// [`SidecarStore::delete_stream`] and the listing helpers can share it.
pub(crate) struct SidecarStore<T: ObjectStore, M: SidecarMeta> {
    /// The underlying storage implementation.
    pub(crate) store: T,
    /// Prefix for legacy (pre-0.10) payload objects.
    data_prefix: Path,
    /// Prefix for immutable generation payload objects.
    gen_prefix: Path,
    /// Prefix for metadata objects (the commit points).
    meta_prefix: Path,
    /// Cache for metadata to reduce storage operations.
    pub(crate) meta_cache: Cache<Path, Arc<M>>,
    /// Generations written by this process whose pointer is not committed
    /// yet; see [`SidecarStore::track_in_flight`].
    in_flight: InFlightSet,
    validator: Option<Arc<MetadataValidator<M>>>,
    pub(crate) limits: MetadataLimits,
    /// When false, listings skip documents that no longer decode (with a
    /// warning); validation failures always propagate.
    strict_listing: bool,
}

impl<T: ObjectStore, M: SidecarMeta> SidecarStore<T, M> {
    /// Creates a core with the default `data/`, `gen/` and `meta/` prefixes.
    pub(crate) fn new(store: T, meta_cache: Cache<Path, Arc<M>>) -> Self {
        SidecarStore {
            store,
            data_prefix: Path::from("data"),
            gen_prefix: Path::from("gen"),
            meta_prefix: Path::from("meta"),
            meta_cache,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            validator: None,
            limits: MetadataLimits::default(),
            strict_listing: false,
        }
    }

    pub(crate) fn with_limits(mut self, limits: MetadataLimits) -> Self {
        self.limits = limits;
        self
    }

    pub(crate) fn with_validator(
        mut self,
        validator: impl Fn(&Path, &M) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.validator = Some(Arc::new(validator));
        self
    }

    pub(crate) fn with_strict_listing(mut self, strict: bool) -> Self {
        self.strict_listing = strict;
        self
    }

    fn validate_meta(&self, location: &Path, meta: &M) -> Result<()> {
        if meta
            .generation()
            .is_some_and(|g| generation_timestamp_ms(g).is_none())
        {
            return Err(limit_error(M::STORE_NAME, "invalid generation identifier"));
        }
        self.limits.check_size(meta.size(), M::STORE_NAME)?;
        if let Some(validate) = &self.validator {
            validate(location, meta)?;
        }
        Ok(())
    }

    fn decode_valid_meta(&self, location: &Path, data: &[u8]) -> Result<M> {
        let meta = self.decode_meta(location, data)?;
        self.validate_meta(location, &meta)?;
        Ok(meta)
    }

    /// Mints an uncommitted generation of `location` and registers it as
    /// in-flight (see [`SidecarStore::track_in_flight`]).
    pub(crate) fn allocate_generation(&self, location: &Path) -> (String, InFlightGuard) {
        let generation = new_generation();
        let guard = self.track_in_flight(location, &generation);
        (generation, guard)
    }

    /// Writes `payload` to a fresh generation of `location`.
    pub(crate) async fn put_new_generation(
        &self,
        location: &Path,
        payload: PutPayload,
        mut options: PutOptions,
    ) -> Result<(String, InFlightGuard)> {
        let (generation, guard) = self.allocate_generation(location);
        options.mode = PutMode::Overwrite;
        self.store
            .put_opts(
                &self.generation_path(location, &generation),
                payload,
                options,
            )
            .await?;
        Ok((generation, guard))
    }

    /// Registers `(location, generation)` as **in-flight**: its payload is
    /// about to be written, but no commit point references it yet, so
    /// [`SidecarStore::collect_garbage`] would otherwise be free to reclaim
    /// it right out from under the writer.
    ///
    /// The registration lasts exactly as long as the returned guard, which
    /// the caller must hold across the payload write *and* the pointer
    /// switch that commits it.
    pub(crate) fn track_in_flight(&self, location: &Path, generation: &str) -> InFlightGuard {
        let key = (location.clone(), generation.to_string());
        lock_in_flight(&self.in_flight)
            .entry(location.clone())
            .or_default()
            .insert(generation.to_string());
        InFlightGuard {
            in_flight: self.in_flight.clone(),
            key,
        }
    }

    /// Whether this process is currently writing the given generation of
    /// `location` (see [`SidecarStore::track_in_flight`]).
    fn is_in_flight(&self, location: &Path, generation: &str) -> bool {
        lock_in_flight(&self.in_flight)
            .get(location)
            .is_some_and(|generations| generations.contains(generation))
    }

    /// Maps a logical location to its metadata path: `loc` → `meta/<loc>`.
    pub(crate) fn meta_path(&self, location: &Path) -> Path {
        self.meta_prefix.parts().chain(location.parts()).collect()
    }

    /// Maps a logical location and generation to the immutable payload path:
    /// `loc` → `gen/<loc>/<generation>`.
    pub(crate) fn generation_path(&self, location: &Path, generation: &str) -> Path {
        self.gen_prefix
            .parts()
            .chain(location.parts())
            .chain(Path::from(generation).parts())
            .collect()
    }

    /// Maps a logical location to its legacy payload path: `loc` → `data/<loc>`.
    pub(crate) fn legacy_path(&self, location: &Path) -> Path {
        self.data_prefix.parts().chain(location.parts()).collect()
    }

    /// Resolves the payload path a metadata document points at.
    pub(crate) fn payload_path(&self, location: &Path, generation: Option<&str>) -> Path {
        match generation {
            Some(generation) => self.generation_path(location, generation),
            None => self.legacy_path(location),
        }
    }

    /// Maps a metadata path back to the logical location:
    /// `meta/<loc>` → `<loc>` (paths outside the prefix pass through).
    fn strip_meta_prefix(&self, path: Path) -> Path {
        if let Some(suffix) = path.prefix_match(&self.meta_prefix) {
            return suffix.collect();
        }
        path
    }

    /// Splits a full generation payload path into `(location, generation)`.
    fn split_generation(&self, path: &Path) -> Option<(Path, String)> {
        let mut parts: Vec<_> = path.prefix_match(&self.gen_prefix)?.collect();
        if parts.len() < 2 {
            return None;
        }
        let generation = parts.pop()?.as_ref().to_string();
        Some((parts.into_iter().collect(), generation))
    }

    /// Fetches the raw metadata document from the underlying store,
    /// bypassing the cache. A missing document is reported as
    /// [`Error::NotFound`] under the caller's logical `location`, not the
    /// internal `meta/` path.
    async fn fetch_meta_bytes(
        &self,
        location: &Path,
        extensions: Extensions,
    ) -> Result<bytes::Bytes> {
        let meta_path = self.meta_path(location);
        let data = self
            .store
            .get_opts(
                &meta_path,
                GetOptions {
                    extensions,
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| match err {
                Error::NotFound { source, .. } => Error::NotFound {
                    path: location.to_string(),
                    source,
                },
                err => err,
            })?;
        if data.meta.size > self.limits.max_metadata_bytes as u64 {
            return Err(limit_error(M::STORE_NAME, "metadata byte limit exceeded"));
        }
        let mut stream = data.into_stream();
        let mut bytes = bytes::BytesMut::new();
        while let Some(chunk) = stream.try_next().await? {
            if chunk.len() > self.limits.max_metadata_bytes.saturating_sub(bytes.len()) {
                return Err(limit_error(M::STORE_NAME, "metadata byte limit exceeded"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes.freeze())
    }

    /// Deserializes a metadata document fetched by
    /// [`SidecarStore::fetch_meta_bytes`].
    fn decode_meta(&self, location: &Path, data: &[u8]) -> Result<M> {
        from_reader(data).map_err(|err| Error::Generic {
            store: M::STORE_NAME,
            source: format!("Failed to deserialize Metadata for path {location}: {err:?}").into(),
        })
    }

    /// Resolves the committed document of `location` inside its per-key
    /// critical section. Every in-process commit updates the cache inside
    /// the same section, so a valid `cached` entry is the committed truth
    /// under the single-writer contract; otherwise the backend is read.
    async fn committed(
        &self,
        location: &Path,
        cached: Option<Arc<M>>,
        extensions: Extensions,
    ) -> Result<Committed<M>> {
        if let Some(meta) = cached
            && self.validate_meta(location, &meta).is_ok()
        {
            return Ok(Committed::Valid(meta));
        }
        match self.fetch_meta_bytes(location, extensions).await {
            Ok(data) => Ok(match self.decode_valid_meta(location, &data) {
                Ok(meta) => Committed::Valid(Arc::new(meta)),
                Err(err) => Committed::Invalid(err),
            }),
            Err(Error::NotFound { .. }) => Ok(Committed::Missing),
            Err(err) => Err(err),
        }
    }

    /// Returns the metadata for `location`, loading and caching it on miss.
    ///
    /// A miss loads **inside the per-key critical section** the commit paths
    /// use, and never overwrites a document that appeared while it waited.
    /// Loading outside that section would let a reader that resolved an old
    /// document cache it *after* a concurrent commit replaced it, resurrecting
    /// the previous version for the rest of the cache TTL — the same hazard
    /// [`SidecarStore::refresh_meta`] and [`SidecarStore::listing_entry`]
    /// avoid. Concurrent loads of the same key are deduplicated by the
    /// section.
    pub(crate) async fn get_meta(&self, location: &Path, extensions: Extensions) -> Result<Arc<M>> {
        Ok(self
            .cached_meta(location, extensions, false)
            .await?
            .expect("ordinary lookups do not skip metadata"))
    }

    /// Listings may skip missing/undecodable documents, but use the same
    /// per-key load and validation path as reads before filling the cache.
    async fn cached_meta(
        &self,
        location: &Path,
        extensions: Extensions,
        listing: bool,
    ) -> Result<Option<Arc<M>>> {
        if let Some(meta) = self.meta_cache.get(location).await {
            self.validate_meta(location, &meta)?;
            return Ok(Some(meta));
        }

        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|entry| async move {
                if let Some(entry) = entry {
                    // Loaded or committed while we waited for the section:
                    // that document is at least as fresh as ours would be.
                    self.validate_meta(location, entry.value())?;
                    return Ok(Op::Nop);
                }
                let bytes = match self.fetch_meta_bytes(location, extensions).await {
                    Ok(bytes) => bytes,
                    Err(Error::NotFound { .. }) if listing => return Ok(Op::Nop),
                    Err(err) => return Err(err),
                };
                let meta = match self.decode_meta(location, &bytes) {
                    Ok(meta) => meta,
                    Err(err) if listing && !self.strict_listing => {
                        log::warn!(
                            "{}: skipping object with corrupted metadata in listing: {location}: {err}",
                            M::STORE_NAME
                        );
                        return Ok(Op::Nop);
                    }
                    Err(err) => return Err(err),
                };
                self.validate_meta(location, &meta)?;
                Ok::<_, Error>(Op::Put(Arc::new(meta)))
            })
            .await?;
        Ok(rt.into_entry().map(|entry| entry.value().clone()))
    }

    /// Re-resolves the metadata from the backend **inside the per-key
    /// critical section** and replaces the cached document with the result.
    ///
    /// Read paths call this when a resolved payload turns out to be gone
    /// (their cached pointer was stale). A plain remove-and-reload could
    /// race a concurrent commit and clobber the newer cached document with
    /// the older one it just read — serializing the reload with the commits
    /// makes that impossible. A key deleted meanwhile is evicted; any other
    /// failure leaves the entry for the next read to re-resolve.
    pub(crate) async fn refresh_meta(
        &self,
        location: &Path,
        extensions: Extensions,
    ) -> Result<Arc<M>> {
        let mut missing = None;
        let missing_out = &mut missing;
        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|_| async move {
                match self.fetch_meta_bytes(location, extensions).await {
                    Ok(bytes) => Ok(Op::Put(Arc::new(self.decode_valid_meta(location, &bytes)?))),
                    Err(err @ Error::NotFound { .. }) => {
                        *missing_out = Some(err);
                        Ok(Op::Remove)
                    }
                    Err(err) => Err(err),
                }
            })
            .await?;
        match missing {
            Some(err) => Err(err),
            None => Ok(rt.unwrap().value().clone()),
        }
    }

    /// Resolves `location` and runs `read` against the payload its pointer
    /// names. A cached pointer — generational or legacy — may be stale after
    /// a concurrent overwrite: the generation was replaced and reclaimed, or
    /// the legacy payload was migrated away. A payload `NotFound` therefore
    /// re-resolves the pointer once before it is reported.
    pub(crate) async fn with_payload<R, Fut>(
        &self,
        location: &Path,
        extensions: Extensions,
        mut read: impl FnMut(Arc<M>) -> Fut,
    ) -> Result<R>
    where
        Fut: Future<Output = Result<R>>,
    {
        let meta = self.get_meta(location, extensions.clone()).await?;
        match read(meta).await {
            Err(Error::NotFound { .. }) => {}
            result => return result,
        }
        let meta = self.refresh_meta(location, extensions).await?;
        read(meta).await.map_err(|err| match err {
            Error::NotFound { source, .. } => Error::NotFound {
                path: location.to_string(),
                source,
            },
            err => err,
        })
    }

    /// Atomically (per key) computes and commits a new metadata document —
    /// the pointer switch of the immutable-generation protocol.
    ///
    /// `f` receives the current committed metadata (`None` when no valid
    /// document exists yet), resolved by [`SidecarStore::committed`], so
    /// caller preconditions are checked against the committed truth. It
    /// typically validates those preconditions, writes the new immutable
    /// payload generation, and returns the new metadata. Once the metadata
    /// put starts, its outcome is unknown on failure: an error evicts the key
    /// and a cancellation the whole shared cache (see [`CommitGuard`]).
    /// Errors before publication leave the previous commit intact; abandoned
    /// unreferenced generations are reclaimed by garbage collection.
    ///
    /// With `create`, the commit fails with [`Error::AlreadyExists`] when a
    /// document already exists, even if validation would fail. When none exists,
    /// the metadata put is forwarded with [`PutMode::Create`], so a second writer
    /// racing the same key **across processes** is rejected by the backend's
    /// conditional write. Only an explicit overwrite may rebuild an invalid
    /// document; validation failure can also mean a wrong key or changed limits,
    /// and must not grant a creating put permission to replace existing data.
    /// An invalid document's untrusted payload pointer is never used for cleanup.
    ///
    /// After a successful commit the replaced payload (previous generation,
    /// or the legacy `data/` object) is deleted best-effort; failures are
    /// logged and left to [`SidecarStore::collect_garbage`].
    pub(crate) async fn update_meta_with<F>(
        &self,
        location: &Path,
        create: bool,
        extensions: Extensions,
        f: F,
    ) -> Result<CommitResult<M>>
    where
        F: AsyncFnOnce(Option<&M>) -> Result<M>,
    {
        let mut replaced: Option<Path> = None;
        let replaced_out = &mut replaced;
        let mut failed = None;
        let failed_out = &mut failed;
        let mut guard = CommitGuard {
            cache: &self.meta_cache,
            armed: false,
        };
        let guard_ref = &mut guard;
        let mut reply = Extensions::default();
        let reply_ref = &mut reply;
        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|entry| async move {
                let current = self
                    .committed(location, entry.map(|e| e.into_value()), extensions.clone())
                    .await?;
                if create && !matches!(current, Committed::Missing) {
                    return Err(Error::AlreadyExists {
                        path: location.to_string(),
                        source: "object already exists".into(),
                    });
                }
                let val = match current {
                    Committed::Valid(cur) => {
                        *replaced_out = Some(self.payload_path(location, cur.generation()));
                        f(Some(&cur)).await?
                    }
                    Committed::Invalid(err) => {
                        // A corrupted commit point (external corruption;
                        // backend puts are atomic in the crash model)
                        // must not make the key permanently unwritable:
                        // treat it as absent so the put can rebuild the
                        // object. Its payload is unreachable either way
                        // and is left to garbage collection.
                        log::warn!(
                            "{}: replacing corrupted metadata for {location}: {err}",
                            M::STORE_NAME
                        );
                        f(None).await?
                    }
                    Committed::Missing => f(None).await?,
                };

                self.validate_meta(location, &val)?;
                let size = cbor2::serialized_size(&val).map_err(|err| Error::Generic {
                    store: M::STORE_NAME,
                    source: err.into(),
                })?;
                if size > self.limits.max_metadata_bytes as u64 {
                    return Err(limit_error(M::STORE_NAME, "metadata byte limit exceeded"));
                }
                let mut data = Vec::with_capacity(size as usize);
                to_writer(&val, &mut data).map_err(|err| Error::Generic {
                    store: M::STORE_NAME,
                    source: format!("Failed to serialize Metadata for path {location}: {err:?}")
                        .into(),
                })?;
                guard_ref.armed = true;
                let mode = if create {
                    PutMode::Create
                } else {
                    PutMode::Overwrite
                };
                let options = PutOptions {
                    mode,
                    extensions,
                    ..Default::default()
                };
                match self
                    .store
                    .put_opts(&self.meta_path(location), data.into(), options)
                    .await
                {
                    Ok(result) => {
                        *reply_ref = result.extensions;
                        Ok::<_, Error>(Op::Put(Arc::new(val)))
                    }
                    Err(err) => {
                        *failed_out = Some(match err {
                            Error::AlreadyExists { source, .. } => Error::AlreadyExists {
                                path: location.to_string(),
                                source,
                            },
                            err => err,
                        });
                        Ok(Op::Remove)
                    }
                }
            })
            .await?;
        guard.armed = false;
        if let Some(err) = failed {
            return Err(err);
        }
        let rt = rt.unwrap().value().clone();

        // The pointer switch committed; the replaced payload is garbage now.
        // Deleting it here is best-effort — an interruption leaves it to
        // `collect_garbage`, never in the read path.
        if let Some(old) = replaced
            && old != self.payload_path(location, rt.generation())
        {
            self.best_effort_delete(&old).await;
        }
        Ok(CommitResult {
            meta: rt,
            extensions: reply,
        })
    }

    async fn best_effort_delete(&self, path: &Path) {
        match self.store.delete(path).await {
            Ok(()) | Err(Error::NotFound { .. }) => {}
            Err(err) => log::warn!(
                "{}: failed to delete replaced payload {path}: {err}",
                M::STORE_NAME
            ),
        }
    }

    pub(crate) async fn publish_upload<F>(
        &self,
        location: &Path,
        mut meta: M,
        baseline: &mut PublicationBaseline,
        extensions: Extensions,
        prepare: F,
    ) -> Result<CommitResult<M>>
    where
        M: Clone,
        F: FnOnce(&mut M) -> Result<()>,
    {
        let retry = baseline.is_some();
        let baseline_out = baseline;
        self.update_meta_with(location, false, extensions.clone(), async |current| {
            if let Some(current) = current
                && current.generation() == meta.generation()
            {
                return Ok(current.clone());
            }

            let current_identity = current.map(CommitIdentity::from_meta);
            match baseline_out {
                None => *baseline_out = Some(current_identity),
                Some(expected) if *expected != current_identity => {
                    return Err(Error::Precondition {
                        path: location.to_string(),
                        source: "multipart publication was superseded by another commit".into(),
                    });
                }
                Some(_) => {}
            }

            // On a retry, make sure the completed but uncommitted payload was
            // not reclaimed. Matching the publication baseline above prevents
            // an old retry from resurrecting a generation after a newer commit.
            if retry {
                let path = self.payload_path(location, meta.generation());
                let result = self
                    .store
                    .get_opts(
                        &path,
                        GetOptions {
                            head: true,
                            extensions,
                            ..Default::default()
                        },
                    )
                    .await?;
                if result.meta.size != meta.size() {
                    return Err(limit_error(
                        M::STORE_NAME,
                        "multipart payload is missing or incomplete",
                    ));
                }
            }
            // Timestamp/seal only an actual publication, after acquiring the
            // key lock. A retry of an already committed generation keeps the
            // original metadata above, including its authenticated timestamp.
            prepare(&mut meta)?;
            Ok(meta)
        })
        .await
    }

    pub(crate) async fn delete_uncommitted_generation(
        &self,
        location: &Path,
        generation: &str,
        extensions: Extensions,
    ) -> Result<()> {
        let generation_path = self.generation_path(location, generation);
        self.meta_cache
            .entry(location.clone())
            .and_try_compute_with(|_| async move {
                // Publication may have reached the backend even when complete
                // returned an error or was cancelled. Re-read the commit point
                // while holding the same per-key lock as writers before
                // deleting a materialized generation.
                let cache_op = match self.fetch_meta_bytes(location, extensions).await {
                    Ok(data) => {
                        let current = self.decode_valid_meta(location, &data)?;
                        if current.generation() == Some(generation) {
                            return Err(Error::Precondition {
                                path: location.to_string(),
                                source: "cannot abort a committed multipart upload".into(),
                            });
                        }
                        Op::Put(Arc::new(current))
                    }
                    Err(Error::NotFound { .. }) => Op::Remove,
                    Err(err) => return Err(err),
                };

                match self.store.delete(&generation_path).await {
                    Ok(()) | Err(Error::NotFound { .. }) => {}
                    Err(err) => return Err(err),
                }
                Ok::<_, Error>(cache_op)
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn generation_exists(
        &self,
        location: &Path,
        generation: &str,
        extensions: Extensions,
    ) -> Result<bool> {
        match self
            .store
            .get_opts(
                &self.generation_path(location, generation),
                GetOptions {
                    head: true,
                    extensions,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Logically deletes `location`: removes the metadata document (the
    /// commit point) and then deletes the payload best-effort. Reports
    /// [`Error::NotFound`] when no metadata document exists.
    pub(crate) async fn delete_object(
        &self,
        location: &Path,
        extensions: Extensions,
    ) -> Result<()> {
        let mut payload: Option<Path> = None;
        let payload_out = &mut payload;
        let mut failed = None;
        let failed_out = &mut failed;
        let mut guard = CommitGuard {
            cache: &self.meta_cache,
            armed: false,
        };
        let guard_ref = &mut guard;
        self.meta_cache
            .entry(location.clone())
            .and_try_compute_with(|entry| async move {
                match self
                    .committed(location, entry.map(|e| e.into_value()), extensions)
                    .await?
                {
                    Committed::Valid(cur) => {
                        *payload_out = Some(self.payload_path(location, cur.generation()));
                    }
                    Committed::Invalid(err) => {
                        // The payload cannot be resolved; delete the commit
                        // point anyway and leave the payload to garbage
                        // collection.
                        log::warn!(
                            "{}: deleting object with corrupted metadata at {location}: {err}",
                            M::STORE_NAME
                        );
                    }
                    Committed::Missing => {
                        return Err(Error::NotFound {
                            path: location.to_string(),
                            source: "object not found".into(),
                        });
                    }
                }

                guard_ref.armed = true;
                match self.store.delete(&self.meta_path(location)).await {
                    Ok(()) | Err(Error::NotFound { .. }) => {}
                    Err(err) => *failed_out = Some(err),
                }
                Ok::<_, Error>(Op::Remove)
            })
            .await?;
        guard.armed = false;
        if let Some(err) = failed {
            return Err(err);
        }

        if let Some(path) = payload {
            self.best_effort_delete(&path).await;
        }
        Ok(())
    }

    /// Shared implementation of [`ObjectStore::delete_stream`].
    pub(crate) fn delete_stream(
        self: Arc<Self>,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self;
        locations
            .map(move |location| {
                let inner = inner.clone();
                async move {
                    let location = location?;
                    inner
                        .delete_object(&location, Extensions::default())
                        .await?;
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    /// Shared implementation of [`ObjectStore::list`]: enumerates the
    /// metadata documents (the commit points), so uncommitted generations and
    /// crash leftovers are invisible by construction. Each entry reports the
    /// logical size and the logical ETag from the decoded document.
    pub(crate) fn list(
        self: Arc<Self>,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = self.meta_path(prefix.unwrap_or(&Path::default()));
        let stream = self.store.list(Some(&prefix));
        self.decorate_listing(stream)
    }

    /// Shared implementation of [`ObjectStore::list_with_offset`]; see
    /// [`SidecarStore::list`].
    pub(crate) fn list_with_offset(
        self: Arc<Self>,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = self.meta_path(offset);
        let prefix = self.meta_path(prefix.unwrap_or(&Path::default()));
        let stream = self.store.list_with_offset(Some(&prefix), &offset);
        self.decorate_listing(stream)
    }

    fn decorate_listing(
        self: Arc<Self>,
        stream: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self;
        stream
            .map_ok(move |obj| {
                let store = inner.clone();
                async move { store.listing_entry(obj).await }
            })
            .try_buffered(8) // fetch metadata concurrently
            .try_filter_map(|entry| async move { Ok(entry) })
            .boxed()
    }

    /// Builds the caller-visible [`ObjectMeta`] for one listed metadata
    /// document. Returns `Ok(None)` for entries that must be skipped: keys
    /// deleted while the listing was running, and (unless listings are
    /// strict) documents that no longer decode.
    ///
    /// Cold entries are loaded and cached under the same per-key lock as writes,
    /// so a listing cannot cache an old document after a newer commit.
    async fn listing_entry(&self, obj: ObjectMeta) -> Result<Option<ObjectMeta>> {
        let location = self.strip_meta_prefix(obj.location);
        Ok(self
            .cached_meta(&location, Extensions::default(), true)
            .await?
            .map(|meta| logical_object_meta(&location, &*meta, obj.last_modified)))
    }

    /// Shared implementation of [`ObjectStore::list_with_delimiter`]; see
    /// [`SidecarStore::list`] for listing semantics.
    pub(crate) async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = self.meta_path(prefix.unwrap_or(&Path::default()));
        let rt = self.store.list_with_delimiter(Some(&prefix)).await?;
        let common_prefixes = rt
            .common_prefixes
            .into_iter()
            .map(|p| self.strip_meta_prefix(p))
            .collect::<Vec<_>>();

        // Fetch the metadata for each object concurrently while preserving
        // the original listing order.
        let objects =
            futures::stream::iter(rt.objects.into_iter().map(|obj| self.listing_entry(obj)))
                .buffered(8)
                .try_filter_map(|entry| async move { Ok(entry) })
                .try_collect()
                .await?;

        Ok(ListResult {
            common_prefixes,
            objects,
            extensions: rt.extensions,
        })
    }

    /// Reject an existing copy target before paying for a payload copy. The
    /// final conditional metadata put still arbitrates a concurrent create.
    async fn check_copy_target_absent(
        &self,
        location: &Path,
        extensions: Extensions,
    ) -> Result<()> {
        match self
            .store
            .get_opts(
                &self.meta_path(location),
                GetOptions {
                    head: true,
                    extensions,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Err(Error::AlreadyExists {
                path: location.to_string(),
                source: "object already exists".into(),
            }),
            Err(Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Copies the current payload of `from` into a fresh generation of `to`,
    /// re-resolving a stale cached pointer once (see
    /// [`SidecarStore::with_payload`]). The target's commit point is **not**
    /// touched: the caller builds the new metadata around the returned
    /// generation and commits it via [`SidecarStore::update_meta_with`], so a
    /// failure in between leaves the target unchanged and the copied
    /// generation as collectable garbage.
    ///
    /// Source metadata is validated by the shared policy before copying.
    /// `extensions` are the caller's, forwarded to the
    /// backend so implementation-specific request context (tracing spans,
    /// credentials) reaches it. The returned [`InFlightGuard`] keeps the
    /// copied generation off the garbage collector's reach and must be held
    /// until the caller has committed the pointer.
    pub(crate) async fn copy_payload(
        &self,
        from: &Path,
        to: &Path,
        create: bool,
        extensions: Extensions,
    ) -> Result<(Arc<M>, String, InFlightGuard)> {
        self.with_payload(from, extensions.clone(), |src| {
            let extensions = extensions.clone();
            async move {
                // Preserve source-NotFound precedence while rejecting an
                // occupied target before allocating/copying a generation.
                if create {
                    self.check_copy_target_absent(to, extensions.clone())
                        .await?;
                }
                let (generation, in_flight) = self.allocate_generation(to);
                self.store
                    .copy_opts(
                        &self.payload_path(from, src.generation()),
                        &self.generation_path(to, &generation),
                        CopyOptions {
                            mode: CopyMode::Overwrite,
                            extensions,
                        },
                    )
                    .await?;
                Ok((src, generation, in_flight))
            }
        })
        .await
    }

    /// Shared implementation of [`ObjectStore::rename_opts`]: `store.copy_opts`
    /// (the wrapper's own copy, which re-commits the payload under `to`) and
    /// then a logical delete of `from`.
    ///
    /// A self-rename must not be forwarded (copy + delete would destroy the
    /// object): the object must exist, and a [`RenameTargetMode::Create`]
    /// rename fails because the target (the object itself) already exists.
    pub(crate) async fn rename(
        &self,
        store: &impl ObjectStore,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<()> {
        let create = matches!(options.target_mode, RenameTargetMode::Create);
        if from == to {
            self.get_meta(from, options.extensions).await?;
            if create {
                return Err(Error::AlreadyExists {
                    path: from.to_string(),
                    source: "rename target already exists".into(),
                });
            }
            return Ok(());
        }

        let mode = if create {
            CopyMode::Create
        } else {
            CopyMode::Overwrite
        };
        store
            .copy_opts(
                from,
                to,
                CopyOptions {
                    mode,
                    extensions: options.extensions.clone(),
                },
            )
            .await?;
        match self.delete_object(from, options.extensions).await {
            Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Mark-sweep garbage collection over the payload prefixes.
    ///
    /// **Mark**: every metadata document (commit point) is read first, before
    /// anything is deleted. **Sweep**: a payload object is a candidate only
    /// when the marked state does not reference it, and immediately before
    /// deletion the key's metadata is re-read from the backend — a payload
    /// that is (or has become) referenced is never deleted. Generations
    /// minted at or after the collection started are skipped, as are
    /// generations this process registered as in-flight (see
    /// [`SidecarStore::track_in_flight`]), foreign objects under `gen/` and
    /// every payload of a key whose metadata exists but does not decode
    /// (conservative).
    ///
    /// The collector is designed to run when no other **process** writes the
    /// store (e.g. at open), in line with the crate's single-writer contract.
    /// Concurrent **in-process** writers are kept safe by the in-flight
    /// registry, not by the re-check: the re-check only consults the commit
    /// point, which a put publishes *after* its payload, so on its own it
    /// would happily reclaim the payload of a put whose pointer switch is
    /// still pending. A foreign process could still commit a swept generation
    /// between the re-check and the delete.
    ///
    /// Returns the number of payload objects deleted.
    pub(crate) async fn collect_garbage(&self) -> Result<usize> {
        self.collect_garbage_with_options(GarbageCollectionOptions::default())
            .await
    }

    pub(crate) async fn collect_garbage_with_options(
        &self,
        options: GarbageCollectionOptions,
    ) -> Result<usize> {
        let floor_ms = unix_ms();
        let concurrency = options.concurrency.clamp(1, 64);
        let prefix = options.prefix.as_ref().cloned().unwrap_or_default();
        let meta_prefix = self.meta_path(&prefix);
        let gen_prefix: Path = self.gen_prefix.parts().chain(prefix.parts()).collect();
        let data_prefix = self.legacy_path(&prefix);
        let mut referenced = HashMap::new();
        let mut metas = self
            .store
            .list(Some(&meta_prefix))
            .map_ok(|obj| async move {
                let location = self.strip_meta_prefix(obj.location);
                let state = self.reference_state(&location).await?;
                Ok::<_, Error>((location, state))
            })
            .try_buffer_unordered(concurrency);
        while let Some((location, state)) = metas.try_next().await? {
            if referenced.len() >= options.max_metadata_entries {
                return Err(limit_error(
                    M::STORE_NAME,
                    "GC metadata entry budget exceeded; use a narrower prefix or raise the budget",
                ));
            }
            referenced.insert(location, state);
        }
        drop(metas);

        // All mark and candidate I/O finishes before deleting anything. Group
        // generations by logical key to avoid repeated paths and metadata gets.
        let mut candidates: HashMap<Path, Vec<Option<String>>> = HashMap::new();
        let mut count = 0usize;
        let mut add = |location: Path, generation: Option<String>| -> Result<()> {
            if count >= options.max_candidates {
                return Err(limit_error(
                    M::STORE_NAME,
                    "GC candidate budget exceeded; use a narrower prefix or raise the budget",
                ));
            }
            candidates.entry(location).or_default().push(generation);
            count += 1;
            Ok(())
        };
        let mut gens = self.store.list(Some(&gen_prefix));
        while let Some(obj) = gens.try_next().await? {
            let Some((location, generation)) = self.split_generation(&obj.location) else {
                continue;
            };
            let Some(ts) = generation_timestamp_ms(&generation) else {
                continue;
            };
            if ts >= floor_ms || self.is_in_flight(&location, &generation) {
                continue;
            }
            if referenced
                .get(&location)
                .is_some_and(|state| state.references(Some(&generation)))
            {
                continue;
            }
            add(location, Some(generation))?;
        }
        drop(gens);
        let mut legacy = self.store.list(Some(&data_prefix));
        while let Some(obj) = legacy.try_next().await? {
            let Some(parts) = obj.location.prefix_match(&self.data_prefix) else {
                continue;
            };
            let location: Path = parts.collect();
            if referenced
                .get(&location)
                .is_some_and(|state| state.references(None))
            {
                continue;
            }
            add(location, None)?;
        }
        drop(legacy);

        futures::stream::iter(
            candidates
                .into_iter()
                .map(|(location, generations)| async move {
                    let mut deleted = 0;
                    // Serialize the fresh per-key recheck and deletions with commits.
                    // No new pointer to a candidate can be published in this section.
                    self.meta_cache
                        .entry(location.clone())
                        .and_try_compute_with(|_| async {
                            let current = self.reference_state(&location).await?;
                            for generation in generations {
                                if current.references(generation.as_deref())
                                    || generation
                                        .as_ref()
                                        .is_some_and(|g| self.is_in_flight(&location, g))
                                {
                                    continue;
                                }
                                let path = self.payload_path(&location, generation.as_deref());
                                match self.store.delete(&path).await {
                                    Ok(()) => deleted += 1,
                                    Err(Error::NotFound { .. }) => {}
                                    Err(err) => return Err(err),
                                }
                            }
                            Ok::<_, Error>(Op::Nop)
                        })
                        .await?;
                    Ok::<_, Error>(deleted)
                }),
        )
        .buffer_unordered(concurrency)
        .try_fold(0, |sum, n| async move { Ok(sum + n) })
        .await
    }

    /// The collector always reads the backend: it is the safety net for
    /// state this process's cache cannot know about.
    async fn reference_state(&self, location: &Path) -> Result<PayloadRef> {
        Ok(
            match self
                .committed(location, None, Extensions::default())
                .await?
            {
                Committed::Missing => PayloadRef::Missing,
                Committed::Invalid(_) => PayloadRef::Unknown,
                Committed::Valid(meta) => meta
                    .generation()
                    .map_or(PayloadRef::Legacy, |g| PayloadRef::Generation(g.into())),
            },
        )
    }
}

#[cfg(test)]
mod tests;
