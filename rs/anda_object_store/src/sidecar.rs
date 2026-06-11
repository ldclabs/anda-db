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
//! `conditional_put` switch is enabled — the listing helpers surface that
//! switch as their `with_logical_e_tag` parameter.

use ciborium::{from_reader, into_writer};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use moka::{future::Cache, ops::compute::Op};
use object_store::{path::Path, *};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

use crate::map_arc_error;

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
}

impl<T: ObjectStore, M: SidecarMeta> SidecarStore<T, M> {
    /// Creates a core with the default `data/` and `meta/` prefixes.
    pub(crate) fn new(store: T, meta_cache: Cache<Path, Arc<M>>) -> Self {
        SidecarStore {
            store,
            data_prefix: Path::from("data"),
            meta_prefix: Path::from("meta"),
            meta_cache,
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

    /// Loads and deserializes the metadata document from the underlying
    /// store, bypassing the cache.
    async fn load_meta(&self, location: &Path) -> Result<M> {
        let meta_path = self.meta_path(location);
        let data = self.store.get(&meta_path).await?;
        let data = data.bytes().await?;
        let meta: M = from_reader(&data[..]).map_err(|err| Error::Generic {
            store: M::STORE_NAME,
            source: format!("Failed to deserialize Metadata for path {location}: {err:?}").into(),
        })?;
        Ok(meta)
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
        into_writer(&meta, &mut data).map_err(|err| Error::Generic {
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
        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|entry| async {
                let val = match entry {
                    Some(meta) => f(Some(meta.value())).await?,
                    None => match self.load_meta(location).await {
                        Ok(meta) => f(Some(&meta)).await?,
                        Err(Error::NotFound { .. }) => f(None).await?,
                        Err(err) => return Err(err),
                    },
                };

                let meta_path = self.meta_path(location);
                let mut data = Vec::new();
                into_writer(&val, &mut data).map_err(|err| Error::Generic {
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

    async fn remove_meta_cache(&self, location: &Path) {
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

    /// Shared implementation of [`ObjectStore::list`]. With
    /// `with_logical_e_tag` each entry's ETag is replaced by the logical
    /// (content-addressable) one from the sidecar metadata; otherwise only
    /// the locations are rewritten.
    pub(crate) fn list(
        self: Arc<Self>,
        prefix: Option<&Path>,
        with_logical_e_tag: bool,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = self.full_path(prefix.unwrap_or(&Path::default()));
        let stream = self.store.list(Some(&prefix));
        self.decorate_listing(stream, with_logical_e_tag)
    }

    /// Shared implementation of [`ObjectStore::list_with_offset`]; see
    /// [`SidecarStore::list`].
    pub(crate) fn list_with_offset(
        self: Arc<Self>,
        prefix: Option<&Path>,
        offset: &Path,
        with_logical_e_tag: bool,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = self.full_path(offset);
        let prefix = self.full_path(prefix.unwrap_or(&Path::default()));
        let stream = self.store.list_with_offset(Some(&prefix), &offset);
        self.decorate_listing(stream, with_logical_e_tag)
    }

    fn decorate_listing(
        self: Arc<Self>,
        stream: BoxStream<'static, Result<ObjectMeta>>,
        with_logical_e_tag: bool,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self;
        if !with_logical_e_tag {
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
                async move {
                    let location = store.strip_prefix(obj.location);
                    let meta = store.get_meta(&location).await?;
                    obj.location = location;
                    obj.e_tag = meta.e_tag().map(String::from);
                    Ok::<ObjectMeta, Error>(obj)
                }
            })
            .try_buffered(8) // fetch metadata concurrently
            .boxed()
    }

    /// Shared implementation of [`ObjectStore::list_with_delimiter`]; see
    /// [`SidecarStore::list`] for the `with_logical_e_tag` semantics.
    pub(crate) async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
        with_logical_e_tag: bool,
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

        if !with_logical_e_tag {
            return Ok(ListResult {
                common_prefixes,
                objects,
            });
        }

        // Fetch the metadata for each object concurrently while preserving
        // the original listing order.
        let mut indexed = futures::stream::iter(objects.into_iter().enumerate().map(
            move |(idx, mut obj)| async move {
                let meta = self.get_meta(&obj.location).await?;
                obj.e_tag = meta.e_tag().map(String::from);
                Ok::<(usize, ObjectMeta), Error>((idx, obj))
            },
        ))
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;

        // Restore the original order based on the captured index.
        indexed.sort_by_key(|(idx, _)| *idx);
        let objects = indexed.into_iter().map(|(_, obj)| obj).collect();

        Ok(ListResult {
            common_prefixes,
            objects,
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
}
