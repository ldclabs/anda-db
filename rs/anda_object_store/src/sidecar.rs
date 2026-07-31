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
//!   **immutable**: they are written exactly once (at a fresh, unique path)
//!   and never overwritten. Replaced generations are deleted best-effort
//!   after the pointer switch and otherwise reclaimed by
//!   [`SidecarStore::collect_garbage`].
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

use cbor2::{from_reader, to_writer};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use moka::{future::Cache, ops::compute::Op};
use object_store::{path::Path, *};
use rand::RngExt;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};

type MetadataValidator<M> = dyn Fn(&Path, &M) -> Result<()> + Send + Sync;

/// Wrapper-supplied policy for interpreting sidecar metadata in listings.
///
/// `validator` lets wrappers authenticate a decoded metadata document before
/// it is surfaced. `reject_corrupt` distinguishes strict mode (a present but
/// undecodable document fails the listing) from compatibility mode (the entry
/// is skipped with a warning; reads of the key keep failing loudly and an
/// overwrite or garbage collection resolves it).
pub(crate) struct ListingMetaPolicy<M> {
    reject_corrupt: bool,
    validator: Option<Arc<MetadataValidator<M>>>,
}

impl<M> Clone for ListingMetaPolicy<M> {
    fn clone(&self) -> Self {
        Self {
            reject_corrupt: self.reject_corrupt,
            validator: self.validator.clone(),
        }
    }
}

impl<M> ListingMetaPolicy<M> {
    pub(crate) fn unchecked() -> Self {
        Self {
            reject_corrupt: false,
            validator: None,
        }
    }

    pub(crate) fn verified(
        reject_corrupt: bool,
        validator: impl Fn(&Path, &M) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            reject_corrupt,
            validator: Some(Arc::new(validator)),
        }
    }
}

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
}

/// Mints a fresh generation identifier: a 16-hex-digit millisecond timestamp
/// followed by a random 8-hex-digit salt. Timestamps make identifiers roughly
/// monotonic (useful for the garbage collector's in-flight guard); the salt
/// makes collisions between concurrent writers of the same key negligible.
pub(crate) fn new_generation() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let salt: u32 = rand::rng().random();
    format!("{ms:016x}-{salt:08x}")
}

/// Extracts the millisecond timestamp from a generation identifier minted by
/// [`new_generation`]. Returns `None` for foreign objects.
fn generation_timestamp_ms(generation: &str) -> Option<u64> {
    let (ts, salt) = generation.split_once('-')?;
    if ts.len() != 16 || salt.len() != 8 {
        return None;
    }
    u64::from_str_radix(ts, 16).ok()
}

/// The caller-visible `last_modified` of a logical object: the instant its
/// current generation was minted, decoded from the generation pointer.
///
/// Every API must report the same timestamp for the same logical object —
/// listings and reads resolve *different* backend objects (`meta/<loc>` and
/// the payload), whose own timestamps differ by however long the write took,
/// so neither can serve as the shared basis. The generation pointer can: it
/// is part of the commit point (and, for `EncryptedStore`, covered by the
/// metadata authentication tag), which also keeps the timestamp out of reach
/// of anyone who can only write to the backend.
///
/// Returns `None` for pre-0.10 documents, which carry no generation (and for
/// foreign generation identifiers). Callers then fall back to the backend
/// timestamp of the object they resolved, and leave date preconditions to
/// the backend so both stay on the same clock.
pub(crate) fn logical_last_modified(generation: Option<&str>) -> Option<DateTime<Utc>> {
    generation
        .and_then(generation_timestamp_ms)
        .and_then(|ms| DateTime::from_timestamp_millis(ms as i64))
}

/// The set of payloads that have been (or are being) written but whose
/// pointer is not committed yet, keyed by `(location, generation)`.
type InFlightSet = Arc<Mutex<HashSet<(Path, String)>>>;

/// Locks the in-flight registry, ignoring poisoning: the guarded set is a
/// plain [`HashSet`] whose critical sections cannot panic, and [`Drop`] of an
/// [`InFlightGuard`] must not panic while unwinding.
fn lock_in_flight(set: &InFlightSet) -> MutexGuard<'_, HashSet<(Path, String)>> {
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
        lock_in_flight(&self.in_flight).remove(&self.key);
    }
}

/// What a committed metadata document says about its key's payload, as
/// gathered by the garbage collector's mark phase.
enum PayloadRef {
    /// Points at `gen/<location>/<generation>`.
    Generation(String),
    /// Legacy layout: points at `data/<location>`.
    Legacy,
    /// The document exists but cannot be decoded; keep every payload of the
    /// key (conservative).
    Unknown,
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
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
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
        lock_in_flight(&self.in_flight).insert(key.clone());
        InFlightGuard {
            in_flight: self.in_flight.clone(),
            key,
        }
    }

    /// Whether this process is currently writing the given generation of
    /// `location` (see [`SidecarStore::track_in_flight`]).
    fn is_in_flight(&self, location: &Path, generation: &str) -> bool {
        lock_in_flight(&self.in_flight).contains(&(location.clone(), generation.to_string()))
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
    async fn fetch_meta_bytes(&self, location: &Path) -> Result<bytes::Bytes> {
        let meta_path = self.meta_path(location);
        let data = self.store.get(&meta_path).await.map_err(|err| match err {
            Error::NotFound { source, .. } => Error::NotFound {
                path: location.to_string(),
                source,
            },
            err => err,
        })?;
        data.bytes().await
    }

    /// Deserializes a metadata document fetched by
    /// [`SidecarStore::fetch_meta_bytes`].
    fn decode_meta(&self, location: &Path, data: &[u8]) -> Result<M> {
        from_reader(data).map_err(|err| Error::Generic {
            store: M::STORE_NAME,
            source: format!("Failed to deserialize Metadata for path {location}: {err:?}").into(),
        })
    }

    /// Loads and deserializes the metadata document from the underlying
    /// store, bypassing the cache.
    async fn load_meta(&self, location: &Path) -> Result<M> {
        let data = self.fetch_meta_bytes(location).await?;
        self.decode_meta(location, &data)
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
    pub(crate) async fn get_meta(&self, location: &Path) -> Result<Arc<M>> {
        if let Some(meta) = self.meta_cache.get(location).await {
            return Ok(meta);
        }

        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|entry| async move {
                if entry.is_some() {
                    // Loaded or committed while we waited for the section:
                    // that document is at least as fresh as ours would be.
                    return Ok(Op::Nop);
                }
                let meta = self.load_meta(location).await?;
                Ok::<_, Error>(Op::Put(Arc::new(meta)))
            })
            .await?;
        Ok(rt.unwrap().value().clone())
    }

    /// Re-resolves the metadata from the backend **inside the per-key
    /// critical section** and replaces the cached document with the result.
    ///
    /// Read paths call this when a resolved payload turns out to be gone
    /// (their cached pointer was stale). A plain remove-and-reload could
    /// race a concurrent commit and clobber the newer cached document with
    /// the older one it just read — serializing the reload with the commits
    /// makes that impossible.
    pub(crate) async fn refresh_meta(&self, location: &Path) -> Result<Arc<M>> {
        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|_| async {
                let meta = self.load_meta(location).await?;
                Ok::<_, Error>(Op::Put(Arc::new(meta)))
            })
            .await?;
        Ok(rt.unwrap().value().clone())
    }

    /// Atomically (per key) computes and commits a new metadata document —
    /// the pointer switch of the immutable-generation protocol.
    ///
    /// `f` receives the current committed metadata, always freshly loaded
    /// from the backend (`None` when no document exists yet) so caller
    /// preconditions are checked against the committed truth rather than a
    /// possibly lagging cache entry. It typically validates those
    /// preconditions, writes the new immutable payload generation, and
    /// returns the new metadata. The document put is the commit point: on
    /// any error the cache is left untouched and the current version stays
    /// fully readable (a freshly written generation is unreferenced garbage
    /// for [`SidecarStore::collect_garbage`]).
    ///
    /// With `create`, the commit fails with [`Error::AlreadyExists`] when a
    /// decodable document already exists; when no document exists at all the
    /// metadata put is forwarded with [`PutMode::Create`], so a second writer
    /// racing the same key **across processes** is rejected by the backend's
    /// conditional write. A document that exists but does not decode (torn by
    /// external corruption) is treated as absent so an overwriting or
    /// creating put can rebuild the key; its unreachable payload is left to
    /// garbage collection.
    ///
    /// After a successful commit the replaced payload (previous generation,
    /// or the legacy `data/` object) is deleted best-effort; failures are
    /// logged and left to [`SidecarStore::collect_garbage`].
    pub(crate) async fn update_meta_with<F>(
        &self,
        location: &Path,
        create: bool,
        f: F,
    ) -> Result<Arc<M>>
    where
        F: AsyncFnOnce(Option<&M>) -> Result<M>,
    {
        let already_exists = || Error::AlreadyExists {
            path: location.to_string(),
            source: "object already exists".into(),
        };
        let mut replaced: Option<Path> = None;
        let replaced_out = &mut replaced;
        let mut f = Some(f);
        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|_entry| async move {
                let f = f.take().expect("update_meta_with closure invoked twice");
                let mut meta_mode = PutMode::Overwrite;
                // Resolve the current document from the backend, not from
                // the (possibly lagging) cache entry: conditional writes
                // must be checked against the committed truth.
                let val = match self.fetch_meta_bytes(location).await {
                    Ok(data) => match self.decode_meta(location, &data) {
                        Ok(cur) => {
                            if create {
                                return Err(already_exists());
                            }
                            *replaced_out = Some(self.payload_path(location, cur.generation()));
                            f(Some(&cur)).await?
                        }
                        Err(err) => {
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
                    },
                    Err(Error::NotFound { .. }) => {
                        if create {
                            meta_mode = PutMode::Create;
                        }
                        f(None).await?
                    }
                    Err(err) => return Err(err),
                };

                let meta_path = self.meta_path(location);
                let mut data = Vec::new();
                to_writer(&val, &mut data).map_err(|err| Error::Generic {
                    store: M::STORE_NAME,
                    source: format!("Failed to serialize Metadata for path {location}: {err:?}")
                        .into(),
                })?;
                self.store
                    .put_opts(
                        &meta_path,
                        data.into(),
                        PutOptions {
                            mode: meta_mode,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|err| match err {
                        Error::AlreadyExists { source, .. } => Error::AlreadyExists {
                            path: location.to_string(),
                            source,
                        },
                        err => err,
                    })?;
                Ok::<_, Error>(Op::Put(Arc::new(val)))
            })
            .await?;
        let rt = rt.unwrap().value().clone();

        // The pointer switch committed; the replaced payload is garbage now.
        // Deleting it here is best-effort — an interruption leaves it to
        // `collect_garbage`, never in the read path.
        if let Some(old) = replaced
            && old != self.payload_path(location, rt.generation())
        {
            self.best_effort_delete(&old).await;
        }
        Ok(rt)
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

    /// Logically deletes `location`: removes the metadata document (the
    /// commit point) and then deletes the payload best-effort. Reports
    /// [`Error::NotFound`] when no metadata document exists.
    pub(crate) async fn delete_object(&self, location: &Path) -> Result<()> {
        let mut payload: Option<Path> = None;
        let payload_out = &mut payload;
        self.meta_cache
            .entry(location.clone())
            .and_try_compute_with(|_entry| async move {
                // Resolve the payload from the backend, not from the
                // (possibly lagging) cache entry.
                match self.fetch_meta_bytes(location).await {
                    Ok(data) => match self.decode_meta(location, &data) {
                        Ok(cur) => {
                            *payload_out = Some(self.payload_path(location, cur.generation()));
                        }
                        Err(err) => {
                            // The payload cannot be resolved; delete the
                            // commit point anyway and leave the payload
                            // to garbage collection.
                            log::warn!(
                                "{}: deleting object with corrupted metadata at {location}: {err}",
                                M::STORE_NAME
                            );
                        }
                    },
                    Err(Error::NotFound { source, .. }) => {
                        return Err(Error::NotFound {
                            path: location.to_string(),
                            source,
                        });
                    }
                    Err(err) => return Err(err),
                }

                match self.store.delete(&self.meta_path(location)).await {
                    Ok(()) | Err(Error::NotFound { .. }) => {}
                    Err(err) => return Err(err),
                }
                Ok::<_, Error>(Op::Remove)
            })
            .await?;

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
                    inner.delete_object(&location).await?;
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
        policy: ListingMetaPolicy<M>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = self.meta_path(prefix.unwrap_or(&Path::default()));
        let stream = self.store.list(Some(&prefix));
        self.decorate_listing(stream, policy)
    }

    /// Shared implementation of [`ObjectStore::list_with_offset`]; see
    /// [`SidecarStore::list`].
    pub(crate) fn list_with_offset(
        self: Arc<Self>,
        prefix: Option<&Path>,
        offset: &Path,
        policy: ListingMetaPolicy<M>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = self.meta_path(offset);
        let prefix = self.meta_path(prefix.unwrap_or(&Path::default()));
        let stream = self.store.list_with_offset(Some(&prefix), &offset);
        self.decorate_listing(stream, policy)
    }

    fn decorate_listing(
        self: Arc<Self>,
        stream: BoxStream<'static, Result<ObjectMeta>>,
        policy: ListingMetaPolicy<M>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self;
        stream
            .map_ok(move |obj| {
                let store = inner.clone();
                let policy = policy.clone();
                async move { store.listing_entry(obj, &policy).await }
            })
            .try_buffered(8) // fetch metadata concurrently
            .try_filter_map(|entry| async move { Ok(entry) })
            .boxed()
    }

    /// Builds the caller-visible [`ObjectMeta`] for one listed metadata
    /// document. Returns `Ok(None)` for entries that must be skipped: keys
    /// deleted while the listing was running, and (in compatibility mode)
    /// documents that no longer decode.
    ///
    /// Decoded documents seen here are deliberately **not** inserted into the
    /// metadata cache: an insert could clobber a newer document committed by
    /// a concurrent writer between our fetch and the insert.
    async fn listing_entry(
        &self,
        obj: ObjectMeta,
        policy: &ListingMetaPolicy<M>,
    ) -> Result<Option<ObjectMeta>> {
        let location = self.strip_meta_prefix(obj.location);
        let meta: Arc<M> = if let Some(meta) = self.meta_cache.get(&location).await {
            meta
        } else {
            match self.fetch_meta_bytes(&location).await {
                Ok(data) => match self.decode_meta(&location, &data) {
                    Ok(meta) => Arc::new(meta),
                    Err(err) => {
                        if policy.reject_corrupt {
                            return Err(err);
                        }
                        log::warn!(
                            "{}: skipping object with corrupted metadata in listing: {location}: {err}",
                            M::STORE_NAME
                        );
                        return Ok(None);
                    }
                },
                Err(Error::NotFound { .. }) => return Ok(None),
                Err(err) => return Err(err),
            }
        };

        if let Some(validator) = &policy.validator {
            validator(&location, &meta)?;
        }
        Ok(Some(ObjectMeta {
            location,
            last_modified: logical_last_modified(meta.generation()).unwrap_or(obj.last_modified),
            size: meta.size(),
            e_tag: meta.e_tag().map(String::from),
            // Versions are not reported; see the crate documentation.
            version: None,
        }))
    }

    /// Shared implementation of [`ObjectStore::list_with_delimiter`]; see
    /// [`SidecarStore::list`] for listing semantics.
    pub(crate) async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
        policy: ListingMetaPolicy<M>,
    ) -> Result<ListResult> {
        let prefix = self.meta_path(prefix.unwrap_or(&Path::default()));
        let rt = self.store.list_with_delimiter(Some(&prefix)).await?;
        let common_prefixes = rt
            .common_prefixes
            .into_iter()
            .map(|p| self.strip_meta_prefix(p))
            .collect::<Vec<_>>();

        // Fetch the metadata for each object concurrently while preserving
        // the original listing order.
        let mut indexed =
            futures::stream::iter(rt.objects.into_iter().enumerate().map(move |(idx, obj)| {
                let policy = policy.clone();
                async move {
                    let entry = self.listing_entry(obj, &policy).await?;
                    Ok::<_, Error>((idx, entry))
                }
            }))
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;

        // Restore the original order based on the captured index.
        indexed.sort_by_key(|(idx, _)| *idx);
        let objects = indexed.into_iter().filter_map(|(_, entry)| entry).collect();

        Ok(ListResult {
            common_prefixes,
            objects,
            extensions: Extensions::default(),
        })
    }

    /// Copies the current payload of `from` into a fresh generation of `to`,
    /// re-resolving a stale cached pointer once (see the read paths). The
    /// target's commit point is **not** touched: the caller builds the new
    /// metadata around the returned generation and commits it via
    /// [`SidecarStore::update_meta_with`], so a failure in between leaves the
    /// target unchanged and the copied generation as collectable garbage.
    ///
    /// `verify` lets wrappers authenticate the source document before its
    /// payload is copied. `extensions` are the caller's, forwarded to the
    /// backend so implementation-specific request context (tracing spans,
    /// credentials) reaches it. The returned [`InFlightGuard`] keeps the
    /// copied generation off the garbage collector's reach and must be held
    /// until the caller has committed the pointer.
    pub(crate) async fn copy_payload<F>(
        &self,
        from: &Path,
        to: &Path,
        extensions: Extensions,
        verify: F,
    ) -> Result<(Arc<M>, String, InFlightGuard)>
    where
        F: Fn(&Path, &M) -> Result<()>,
    {
        let mut retried = false;
        loop {
            let src = self.get_meta(from).await?;
            verify(from, &src)?;
            let src_path = self.payload_path(from, src.generation());
            let generation = new_generation();
            let dst_path = self.generation_path(to, &generation);
            let in_flight = self.track_in_flight(to, &generation);
            // The target generation path is fresh and unique, so the payload
            // copy itself is unconditional; the caller's `CopyMode` is
            // enforced by the pointer switch that commits it.
            let options = CopyOptions {
                mode: CopyMode::Overwrite,
                extensions: extensions.clone(),
            };
            match self.store.copy_opts(&src_path, &dst_path, options).await {
                Ok(()) => return Ok((src, generation, in_flight)),
                Err(Error::NotFound { source, .. }) => {
                    if !retried && src.generation().is_some() {
                        retried = true;
                        self.refresh_meta(from).await?;
                        continue;
                    }
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source,
                    });
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Validates a rename where `from == to`: the object must exist, and a
    /// [`RenameTargetMode::Create`] rename fails because the target (the
    /// object itself) already exists. The object is left untouched.
    pub(crate) async fn check_self_rename(
        &self,
        location: &Path,
        options: &RenameOptions,
    ) -> Result<()> {
        self.get_meta(location).await?;
        match options.target_mode {
            RenameTargetMode::Overwrite => Ok(()),
            RenameTargetMode::Create => Err(Error::AlreadyExists {
                path: location.to_string(),
                source: "rename target already exists".into(),
            }),
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
        let floor_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Mark: snapshot every commit point.
        let mut referenced: HashMap<Path, PayloadRef> = HashMap::new();
        let mut metas = self.store.list(Some(&self.meta_prefix));
        while let Some(obj) = metas.try_next().await? {
            let location = self.strip_meta_prefix(obj.location);
            let state = match self.fetch_meta_bytes(&location).await {
                Ok(data) => match self.decode_meta(&location, &data) {
                    Ok(meta) => meta
                        .generation()
                        .map(|g| PayloadRef::Generation(g.to_string()))
                        .unwrap_or(PayloadRef::Legacy),
                    Err(_) => PayloadRef::Unknown,
                },
                Err(Error::NotFound { .. }) => continue, // deleted mid-scan
                Err(err) => return Err(err),
            };
            referenced.insert(location, state);
        }

        // Sweep: collect candidates first (never mutate under a listing).
        let mut candidates: Vec<(Path, Path, Option<String>)> = Vec::new();
        let mut gens = self.store.list(Some(&self.gen_prefix));
        while let Some(obj) = gens.try_next().await? {
            let parsed = self
                .split_generation(&obj.location)
                .and_then(|(loc, g)| generation_timestamp_ms(&g).map(|ts| (loc, g, ts)));
            let Some((location, generation, ts)) = parsed else {
                log::warn!(
                    "{}: skipping unrecognized object under generation prefix: {}",
                    M::STORE_NAME,
                    obj.location
                );
                continue;
            };
            if ts >= floor_ms {
                // In-flight write (or clock skew): try again next run.
                continue;
            }
            match referenced.get(&location) {
                Some(PayloadRef::Generation(g)) if *g == generation => continue,
                Some(PayloadRef::Unknown) => continue,
                _ => candidates.push((obj.location, location, Some(generation))),
            }
        }
        let mut legacy = self.store.list(Some(&self.data_prefix));
        while let Some(obj) = legacy.try_next().await? {
            let location = match obj.location.prefix_match(&self.data_prefix) {
                Some(suffix) => suffix.collect::<Path>(),
                None => continue,
            };
            match referenced.get(&location) {
                Some(PayloadRef::Legacy) | Some(PayloadRef::Unknown) => continue,
                _ => candidates.push((obj.location, location, None)),
            }
        }

        let mut deleted = 0usize;
        for (full_path, location, generation) in candidates {
            // Never reclaim a generation this process is still writing: its
            // payload is already on the backend, but the pointer that will
            // reference it has not been committed yet, so no amount of
            // re-reading commit points can see it.
            if let Some(generation) = &generation
                && self.is_in_flight(&location, generation)
            {
                continue;
            }
            // Re-read the commit point right before deleting: never delete a
            // payload that is referenced now, even if the mark snapshot is
            // stale.
            if self.is_referenced(&location, generation.as_deref()).await? {
                continue;
            }
            match self.store.delete(&full_path).await {
                Ok(()) => deleted += 1,
                Err(Error::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(deleted)
    }

    /// Whether the key's *current* on-disk metadata references the given
    /// payload (`Some(generation)` for a generation object, `None` for the
    /// legacy `data/` object). A document that fails to decode counts as
    /// referencing everything (conservative).
    async fn is_referenced(&self, location: &Path, generation: Option<&str>) -> Result<bool> {
        match self.fetch_meta_bytes(location).await {
            Ok(data) => match self.decode_meta(location, &data) {
                Ok(meta) => Ok(meta.generation() == generation),
                Err(_) => Ok(true),
            },
            Err(Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_ids_are_unique_and_carry_timestamps() {
        let a = new_generation();
        let b = new_generation();
        assert_ne!(a, b);

        let ts = generation_timestamp_ms(&a).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(
            ts <= now && ts + 60_000 > now,
            "timestamp {ts} vs now {now}"
        );

        assert_eq!(generation_timestamp_ms("not-a-generation"), None);
        assert_eq!(generation_timestamp_ms("0123"), None);
        assert_eq!(generation_timestamp_ms("zzzzzzzzzzzzzzzz-00000000"), None);
    }
}
