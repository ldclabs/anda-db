//! Crate-internal generic core shared by [`MetaStore`](crate::MetaStore) and
//! [`EncryptedStore`](crate::EncryptedStore).
//!
//! Both wrappers follow the same "sidecar metadata" layout on the underlying
//! backend: the payload of a logical object `<location>` lives at
//! `data/<location>`, while a small CBOR-encoded metadata document describing
//! it lives at `meta/<location>`. [`SidecarStore`] implements everything that
//! depends only on this layout — path rewriting, the cached metadata
//! pipeline, and the [`ObjectStore`] operations whose logic is identical for
//! both wrappers (delete, list, copy, rename) — generically over the concrete
//! metadata type ([`SidecarMeta`]).
//!
//! Wrapper-specific behavior stays in the wrappers: `MetaStore` hashes
//! plaintext payloads, while `EncryptedStore` encrypts/decrypts chunks and
//! only exposes the logical (content-addressable) ETag when its
//! `conditional_put` switch is enabled. Listing metadata is interpreted
//! through a wrapper-supplied policy, so encrypted metadata is authenticated
//! before it is cached or surfaced.

use cbor2::{from_reader, to_writer};
use futures::{
    StreamExt, TryStreamExt,
    lock::{Mutex, MutexGuard},
    stream::BoxStream,
};
use moka::{future::Cache, ops::compute::Op};
use object_store::{path::Path, *};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::map_arc_error;

const MUTATION_LEASE_SHARDS: usize = 256;
type MetadataValidator<M> = dyn Fn(&Path, &M) -> Result<()> + Send + Sync;

/// Wrapper-supplied policy for interpreting sidecar metadata in listings.
///
/// `validator` lets wrappers authenticate a decoded metadata document before
/// it is cached or used. `reject_corrupt` distinguishes strict mode (a
/// present but undecodable document is an error) from compatibility mode
/// (the object is listed as an orphan so a recovery scan can heal it).
pub(crate) struct ListingMetaPolicy<M> {
    logical_e_tag: bool,
    reject_corrupt: bool,
    validator: Option<Arc<MetadataValidator<M>>>,
}

impl<M> Clone for ListingMetaPolicy<M> {
    fn clone(&self) -> Self {
        Self {
            logical_e_tag: self.logical_e_tag,
            reject_corrupt: self.reject_corrupt,
            validator: self.validator.clone(),
        }
    }
}

impl<M> ListingMetaPolicy<M> {
    pub(crate) fn unchecked(logical_e_tag: bool) -> Self {
        Self {
            logical_e_tag,
            reject_corrupt: false,
            validator: None,
        }
    }

    pub(crate) fn verified(
        logical_e_tag: bool,
        reject_corrupt: bool,
        validator: impl Fn(&Path, &M) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            logical_e_tag,
            reject_corrupt,
            validator: Some(Arc::new(validator)),
        }
    }
}

/// Guards one or two logical paths for a sidecar mutation.
///
/// Two-path operations acquire path-derived shards in stable numeric order.
/// The bounded lock table deliberately allows unrelated paths to collide:
/// this may add contention, but never weakens the same-path exclusion
/// guarantee and avoids an unbounded per-path lock registry.
pub(crate) struct MutationLease<'a> {
    _first: MutexGuard<'a, ()>,
    _second: Option<MutexGuard<'a, ()>>,
}

/// Sidecar metadata document maintained by [`SidecarStore`] for every object.
///
/// Implemented by the `Metadata` types of `MetaStore` and `EncryptedStore`.
/// The serialized representation is owned entirely by the implementor, so
/// each wrapper keeps its existing (and distinct) compact CBOR format.
pub(crate) trait SidecarMeta: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Store name used in error messages (e.g. `"MetaStore"`).
    const STORE_NAME: &'static str;

    /// The logical, content-addressable ETag exposed to callers.
    fn e_tag(&self) -> Option<&str>;

    /// Records the ETag/version reported by the underlying backend for the
    /// most recent write of the data object, so caller-provided preconditions
    /// can later be translated into requests the backend understands.
    fn set_original(&mut self, e_tag: Option<String>, version: Option<String>);
}

/// Generic "data + sidecar metadata" store core.
///
/// Owns the underlying [`ObjectStore`], the `data/`/`meta/` path prefixes and
/// the metadata cache, and provides the metadata pipeline plus the
/// structurally identical [`ObjectStore`] operations on top of them. The
/// wrappers hold it behind an [`Arc`] so the `'static` streams returned by
/// [`SidecarStore::delete_stream`] and the listing helpers can share it.
pub(crate) struct SidecarStore<T: ObjectStore, M: SidecarMeta> {
    /// The underlying storage implementation.
    pub(crate) store: T,
    /// Prefix for actual data objects.
    data_prefix: Path,
    /// Prefix for metadata objects.
    meta_prefix: Path,
    /// Cache for metadata to reduce storage operations.
    meta_cache: Cache<Path, Arc<M>>,
    /// Bounded per-path mutation lease table. Every mutation of a logical
    /// path (put, multipart completion, delete, copy or rename) goes through
    /// the same shard.
    mutation_leases: Box<[Mutex<()>]>,
}

impl<T: ObjectStore, M: SidecarMeta> SidecarStore<T, M> {
    /// Creates a core with the default `data/` and `meta/` prefixes.
    pub(crate) fn new(store: T, meta_cache: Cache<Path, Arc<M>>) -> Self {
        SidecarStore {
            store,
            data_prefix: Path::from("data"),
            meta_prefix: Path::from("meta"),
            meta_cache,
            mutation_leases: (0..MUTATION_LEASE_SHARDS).map(|_| Mutex::new(())).collect(),
        }
    }

    fn mutation_lease_index(location: &Path) -> usize {
        let mut hasher = DefaultHasher::new();
        location.hash(&mut hasher);
        hasher.finish() as usize % MUTATION_LEASE_SHARDS
    }

    async fn mutation_lease(&self, location: &Path) -> MutexGuard<'_, ()> {
        self.mutation_leases[Self::mutation_lease_index(location)]
            .lock()
            .await
    }

    /// Acquires the path-derived mutation leases for `first` and `second` in
    /// stable shard order. If both paths map to the same shard, it is locked
    /// only once.
    pub(crate) async fn mutation_leases(&self, first: &Path, second: &Path) -> MutationLease<'_> {
        let first_idx = Self::mutation_lease_index(first);
        let second_idx = Self::mutation_lease_index(second);
        let (first_idx, second_idx) = if first_idx <= second_idx {
            (first_idx, second_idx)
        } else {
            (second_idx, first_idx)
        };
        let first_guard = self.mutation_leases[first_idx].lock().await;
        let second_guard = if first_idx == second_idx {
            None
        } else {
            Some(self.mutation_leases[second_idx].lock().await)
        };
        MutationLease {
            _first: first_guard,
            _second: second_guard,
        }
    }

    /// Maps a logical location to its metadata path: `loc` → `meta/<loc>`.
    pub(crate) fn meta_path(&self, location: &Path) -> Path {
        self.meta_prefix.parts().chain(location.parts()).collect()
    }

    /// Maps a logical location to its data path: `loc` → `data/<loc>`.
    pub(crate) fn full_path(&self, location: &Path) -> Path {
        self.data_prefix.parts().chain(location.parts()).collect()
    }

    /// Maps a data path back to the logical location: `data/<loc>` → `<loc>`
    /// (paths outside the data prefix pass through unchanged).
    pub(crate) fn strip_prefix(&self, path: Path) -> Path {
        if let Some(suffix) = path.prefix_match(&self.data_prefix) {
            return suffix.collect();
        }
        path
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
    /// Concurrent loads of the same key are deduplicated by the cache.
    pub(crate) async fn get_meta(&self, location: &Path) -> Result<Arc<M>> {
        let meta = self
            .meta_cache
            .try_get_with(location.clone(), async {
                let meta = self.load_meta(location).await?;
                Ok(Arc::new(meta))
            })
            .await
            .map_err(|err| map_arc_error(M::STORE_NAME, err))?;

        Ok(meta)
    }

    /// Serializes and persists the metadata document, then updates the cache.
    pub(crate) async fn put_meta(&self, location: &Path, meta: M) -> Result<PutResult> {
        let meta_path = self.meta_path(location);
        let mut data = Vec::new();
        to_writer(&meta, &mut data).map_err(|err| Error::Generic {
            store: M::STORE_NAME,
            source: format!("Failed to serialize Metadata for path {location}: {err:?}").into(),
        })?;
        // Persist to the underlying store first, then update cache.
        // If we cached before the put and the put failed, readers would
        // observe a non-persisted metadata until the cache entry expired.
        let rt = self
            .store
            .put_opts(&meta_path, data.into(), PutOptions::default())
            .await?;
        self.meta_cache
            .insert(location.clone(), Arc::new(meta))
            .await;
        Ok(rt)
    }

    /// Atomically (per key) computes and persists a new metadata document.
    ///
    /// `f` receives the current metadata (cached, or freshly loaded; `None`
    /// when no document exists yet), typically validates preconditions and
    /// writes the data object, and returns the new metadata. The new document
    /// is persisted before the cache entry is replaced; on any error the
    /// cache is left untouched.
    pub(crate) async fn update_meta_with<F>(&self, location: &Path, f: F) -> Result<Arc<M>>
    where
        F: AsyncFnOnce(Option<&M>) -> Result<M>,
    {
        let _lease = self.mutation_lease(location).await;
        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|entry| async {
                let val = match entry {
                    Some(meta) => f(Some(meta.value())).await?,
                    None => match self.fetch_meta_bytes(location).await {
                        Ok(data) => match self.decode_meta(location, &data) {
                            Ok(meta) => f(Some(&meta)).await?,
                            Err(err) => {
                                // A corrupted sidecar (e.g. a torn write
                                // followed by a crash) must not make the key
                                // permanently unwritable: treat it as absent
                                // so an overwriting put can rebuild both the
                                // data object and its metadata.
                                log::warn!(
                                    "{}: replacing corrupted metadata for {location}: {err}",
                                    M::STORE_NAME
                                );
                                f(None).await?
                            }
                        },
                        Err(Error::NotFound { .. }) => f(None).await?,
                        Err(err) => return Err(err),
                    },
                };

                let meta_path = self.meta_path(location);
                let mut data = Vec::new();
                to_writer(&val, &mut data).map_err(|err| Error::Generic {
                    store: M::STORE_NAME,
                    source: format!("Failed to serialize Metadata for path {location}: {err:?}")
                        .into(),
                })?;
                self.store
                    .put_opts(&meta_path, data.into(), PutOptions::default())
                    .await?;
                Ok::<_, Error>(Op::Put(Arc::new(val)))
            })
            .await?;
        Ok(rt.unwrap().value().clone())
    }

    pub(crate) async fn remove_meta_cache(&self, location: &Path) {
        self.meta_cache.remove(location).await;
    }

    /// Re-reads the data object's backend ETag/version (after a copy or
    /// rename produced a new one) and persists it into the metadata.
    async fn refresh_meta_original_tag(&self, location: &Path) -> Result<()> {
        let mut meta = self.load_meta(location).await?;
        let obj = self.store.head(&self.full_path(location)).await?;
        meta.set_original(obj.e_tag, obj.version);
        self.put_meta(location, meta).await?;
        Ok(())
    }

    /// Shared implementation of [`ObjectStore::delete_stream`].
    pub(crate) fn delete_stream(
        self: Arc<Self>,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Each location is handled end-to-end (data object, then metadata
        // object) so failures always carry the caller's logical path. Error
        // paths reported by the inner store cannot be mapped back reliably
        // (e.g. `LocalFileSystem` reports filesystem paths).
        let inner = self;
        locations
            .map(move |location| {
                let inner = inner.clone();
                async move {
                    let location = location?;
                    let _lease = inner.mutation_lease(&location).await;
                    let data_res = inner.store.delete(&inner.full_path(&location)).await;
                    // Attempt metadata deletion even when the data object was
                    // missing, so orphaned metadata heals itself.
                    let meta_res = inner.store.delete(&inner.meta_path(&location)).await;
                    inner.remove_meta_cache(&location).await;

                    match (data_res, meta_res) {
                        // Missing metadata is tolerated: the data object is
                        // the source of truth.
                        (Ok(()), Ok(()) | Err(Error::NotFound { .. })) => Ok(location),
                        (Ok(()), Err(err)) => Err(err),
                        // Surface a missing data object under the logical
                        // path, matching the inner store's NotFound behavior.
                        (Err(Error::NotFound { source, .. }), _) => Err(Error::NotFound {
                            path: location.to_string(),
                            source,
                        }),
                        (Err(err), _) => Err(err),
                    }
                }
            })
            .buffered(10)
            .boxed()
    }

    /// Shared implementation of [`ObjectStore::list`]. When requested by the
    /// policy, each entry's ETag is replaced by the logical
    /// (content-addressable) one from the sidecar metadata.
    pub(crate) fn list(
        self: Arc<Self>,
        prefix: Option<&Path>,
        policy: ListingMetaPolicy<M>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = self.full_path(prefix.unwrap_or(&Path::default()));
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
        let offset = self.full_path(offset);
        let prefix = self.full_path(prefix.unwrap_or(&Path::default()));
        let stream = self.store.list_with_offset(Some(&prefix), &offset);
        self.decorate_listing(stream, policy)
    }

    fn decorate_listing(
        self: Arc<Self>,
        stream: BoxStream<'static, Result<ObjectMeta>>,
        policy: ListingMetaPolicy<M>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self;
        if !policy.logical_e_tag && policy.validator.is_none() {
            return stream
                .map_ok(move |mut obj| {
                    obj.location = inner.strip_prefix(obj.location);
                    obj
                })
                .boxed();
        }

        stream
            .map_ok(move |mut obj| {
                let store = inner.clone();
                let policy = policy.clone();
                async move {
                    let location = store.strip_prefix(obj.location);
                    let logical_e_tag = store.listing_e_tag(&location, &policy).await?;
                    if policy.logical_e_tag {
                        obj.e_tag = logical_e_tag;
                    }
                    obj.location = location;
                    Ok::<ObjectMeta, Error>(obj)
                }
            })
            .try_buffered(8) // fetch metadata concurrently
            .boxed()
    }

    /// Loads and validates metadata for `location` during a listing, and
    /// resolves its logical ETag. An orphaned
    /// data object (metadata lost, e.g. after a crash between the data and
    /// metadata writes) does not fail a listing because recovery scans need
    /// to observe and heal it. Compatibility mode also treats a torn CBOR
    /// document as an orphan; strict mode rejects every present document that
    /// cannot be decoded and verified.
    async fn listing_e_tag(
        &self,
        location: &Path,
        policy: &ListingMetaPolicy<M>,
    ) -> Result<Option<String>> {
        if let Some(meta) = self.meta_cache.get(location).await {
            if let Some(validator) = &policy.validator {
                validator(location, &meta)?;
            }
            return Ok(meta.e_tag().map(String::from));
        }

        // A listing cache miss must participate in the same per-key critical
        // section as every mutation. Without this lease, a listing can fetch
        // old metadata, a put can then commit new data and metadata, and the
        // listing can finally overwrite the cache with its authenticated but
        // stale document. Re-check after acquiring the lease because a
        // mutation (or another listing) may have filled the cache while this
        // task was waiting. Cache hits above remain lock-free.
        let _lease = self.mutation_lease(location).await;
        if let Some(meta) = self.meta_cache.get(location).await {
            if let Some(validator) = &policy.validator {
                validator(location, &meta)?;
            }
            return Ok(meta.e_tag().map(String::from));
        }

        let data = match self.fetch_meta_bytes(location).await {
            Ok(data) => data,
            Err(Error::NotFound { .. }) => {
                log::warn!(
                    "{}: listing orphaned data object without metadata: {location}",
                    M::STORE_NAME
                );
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        match self.decode_meta(location, &data) {
            Ok(meta) => {
                if let Some(validator) = &policy.validator {
                    // Authenticate before caching: a failed listing must not
                    // seed the shared cache with attacker-controlled data.
                    validator(location, &meta)?;
                }
                let meta = Arc::new(meta);
                self.meta_cache.insert(location.clone(), meta.clone()).await;
                Ok(meta.e_tag().map(String::from))
            }
            Err(err) => {
                if policy.reject_corrupt {
                    return Err(err);
                }
                // Do not cache anything for the corrupted document: reads
                // must keep failing loudly and the next overwrite heals it.
                log::warn!(
                    "{}: listing data object with corrupted metadata as orphan: {location}: {err}",
                    M::STORE_NAME
                );
                Ok(None)
            }
        }
    }

    /// Shared implementation of [`ObjectStore::list_with_delimiter`]; see
    /// [`SidecarStore::list`] for listing-policy semantics.
    pub(crate) async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
        policy: ListingMetaPolicy<M>,
    ) -> Result<ListResult> {
        let prefix = self.full_path(prefix.unwrap_or(&Path::default()));
        let rt = self.store.list_with_delimiter(Some(&prefix)).await?;
        let common_prefixes = rt
            .common_prefixes
            .into_iter()
            .map(|p| self.strip_prefix(p))
            .collect::<Vec<_>>();

        let objects = rt
            .objects
            .into_iter()
            .map(|mut meta| {
                meta.location = self.strip_prefix(meta.location);
                meta
            })
            .collect::<Vec<_>>();

        if !policy.logical_e_tag && policy.validator.is_none() {
            return Ok(ListResult {
                common_prefixes,
                objects,
                extensions: Extensions::default(),
            });
        }

        // Fetch the metadata for each object concurrently while preserving
        // the original listing order.
        let mut indexed =
            futures::stream::iter(objects.into_iter().enumerate().map(move |(idx, mut obj)| {
                let policy = policy.clone();
                async move {
                    let logical_e_tag = self.listing_e_tag(&obj.location, &policy).await?;
                    if policy.logical_e_tag {
                        obj.e_tag = logical_e_tag;
                    }
                    Ok::<(usize, ObjectMeta), Error>((idx, obj))
                }
            }))
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;

        // Restore the original order based on the captured index.
        indexed.sort_by_key(|(idx, _)| *idx);
        let objects = indexed.into_iter().map(|(_, obj)| obj).collect();

        Ok(ListResult {
            common_prefixes,
            objects,
            extensions: Extensions::default(),
        })
    }

    /// Shared implementation of [`ObjectStore::copy_opts`]: copies the data
    /// object honouring the requested mode, then mirrors the metadata.
    pub(crate) async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        let _leases = self.mutation_leases(from, to).await;
        let full_from = self.full_path(from);
        let full_to = self.full_path(to);
        self.store
            .copy_opts(&full_from, &full_to, options.clone())
            .await?;

        // The data copy above already enforced the requested CopyMode; copy
        // the sidecar metadata with Overwrite so stale/orphaned metadata at
        // the target cannot fail the operation halfway.
        let meta_from = self.meta_path(from);
        let meta_to = self.meta_path(to);
        let meta_options = CopyOptions {
            mode: CopyMode::Overwrite,
            extensions: options.extensions,
        };
        self.store
            .copy_opts(&meta_from, &meta_to, meta_options)
            .await?;
        self.remove_meta_cache(to).await;
        self.refresh_meta_original_tag(to).await?;
        Ok(())
    }

    /// Shared implementation of [`ObjectStore::rename_opts`]; see
    /// [`SidecarStore::copy_opts`].
    pub(crate) async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<()> {
        let _leases = self.mutation_leases(from, to).await;
        if from == to {
            // A self-rename must not be forwarded: the default rename
            // implementation is copy + delete, which would destroy the
            // object on some backends. Validate existence and target mode,
            // then leave the object untouched.
            return self.check_self_rename(from, &options).await;
        }

        let full_from = self.full_path(from);
        let full_to = self.full_path(to);
        self.store
            .rename_opts(&full_from, &full_to, options.clone())
            .await?;
        self.remove_meta_cache(from).await;

        // See copy_opts: the data rename already enforced the requested
        // target mode, so always overwrite the target metadata.
        let meta_from = self.meta_path(from);
        let meta_to = self.meta_path(to);
        let meta_options = RenameOptions {
            target_mode: RenameTargetMode::Overwrite,
            extensions: options.extensions,
        };
        self.store
            .rename_opts(&meta_from, &meta_to, meta_options)
            .await?;
        self.remove_meta_cache(to).await;
        self.refresh_meta_original_tag(to).await?;
        Ok(())
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
}
