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
//!   (notably [`object_store::local::LocalFileSystem`]).
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
//! See [`docs/anda_object_store.md`] for the full design document.

use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_URL_SAFE};
use bytes::Bytes;
use ciborium::{from_reader, into_writer};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use moka::{future::Cache, ops::compute::Op};
use object_store::{path::Path, *};
use serde::{Deserialize, Serialize};
use sha3::Digest;
use std::{fmt::Debug, ops::Range, sync::Arc, time::Duration};

/// Transparent AES-256-GCM encryption-at-rest layer for any [`ObjectStore`].
pub mod encryption;

pub use encryption::{EncryptedStore, EncryptedStoreBuilder, EncryptedStoreUploader};

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
    inner: Arc<MetaStoreBuilder<T>>,
}

/// Builder for creating a `MetaStore` instance.
///
/// This builder configures:
/// - The underlying storage implementation
/// - Data and metadata path prefixes
/// - Metadata cache settings
pub struct MetaStoreBuilder<T: ObjectStore> {
    /// The underlying storage implementation
    store: T,
    /// Prefix for actual data objects
    data_prefix: Path,
    /// Prefix for metadata objects
    meta_prefix: Path,
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
            data_prefix: Path::from("data"),
            meta_prefix: Path::from("meta"),
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
            inner: Arc::new(self),
        }
    }

    fn meta_path(&self, location: &Path) -> Path {
        self.meta_prefix.parts().chain(location.parts()).collect()
    }

    fn full_path(&self, location: &Path) -> Path {
        self.data_prefix.parts().chain(location.parts()).collect()
    }

    fn strip_prefix(&self, path: Path) -> Path {
        if let Some(suffix) = path.prefix_match(&self.data_prefix) {
            return suffix.collect();
        }
        path
    }

    fn strip_meta_prefix(&self, path: Path) -> Path {
        if let Some(suffix) = path.prefix_match(&self.meta_prefix) {
            return suffix.collect();
        }
        path
    }

    async fn load_meta(&self, location: &Path) -> Result<Metadata> {
        let meta_path = self.meta_path(location);
        let data = self.store.get(&meta_path).await?;
        let data = data.bytes().await?;
        let meta: Metadata = from_reader(&data[..]).map_err(|err| Error::Generic {
            store: "MetaStore",
            source: format!("Failed to deserialize Metadata for path {location}: {err:?}").into(),
        })?;
        Ok(meta)
    }

    async fn get_meta(&self, location: &Path) -> Result<Metadata> {
        let meta = self
            .meta_cache
            .try_get_with(location.clone(), async {
                let meta = self.load_meta(location).await?;
                Ok(Arc::new(meta))
            })
            .await
            .map_err(map_arc_error)?;

        Ok(meta.as_ref().clone())
    }

    async fn put_meta(&self, location: &Path, meta: Metadata) -> Result<PutResult> {
        let meta_path = self.meta_path(location);
        let mut data = Vec::new();
        into_writer(&meta, &mut data).map_err(|err| Error::Generic {
            store: "MetaStore",
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

    async fn update_meta_with<F>(&self, location: &Path, f: F) -> Result<Arc<Metadata>>
    where
        F: AsyncFnOnce(Option<&Metadata>) -> Result<Metadata>,
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
                    store: "MetaStore",
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
                if let PutMode::Update(v) = &opts.mode {
                    match meta {
                        Some(m) => {
                            if m.e_tag != v.e_tag {
                                return Err(Error::Precondition {
                                    path: location.to_string(),
                                    source: format!("{:?} does not match {:?}", m.e_tag, v.e_tag)
                                        .into(),
                                });
                            }
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
                let payload = Bytes::from(payload);
                let hash = sha3_256(&payload);

                let mut meta = Metadata {
                    size: payload.len() as u64,
                    e_tag: Some(BASE64_URL_SAFE.encode(hash)),
                    original_tag: None,
                    original_version: None,
                };

                let rt = self
                    .inner
                    .store
                    .put_opts(&full_path, payload.into(), opts)
                    .await?;
                meta.original_tag = rt.e_tag;
                meta.original_version = rt.version;
                Ok(meta)
            })
            .await?;

        Ok(PutResult {
            e_tag: rt.e_tag.clone(),
            version: rt.original_version.clone(),
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
        if meta.e_tag == options.if_match {
            options.if_match = meta.original_tag.clone();
        }
        if meta.e_tag == options.if_none_match {
            options.if_none_match = meta.original_tag.clone();
        }

        let mut res = self.inner.store.get_opts(&full_path, options).await?;
        res.meta.location = self.inner.strip_prefix(res.meta.location);
        res.meta.e_tag = meta.e_tag;

        Ok(res)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let full_path = self.inner.full_path(location);
        self.inner.store.get_ranges(&full_path, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();

        // 1) Delete the data objects via the underlying store's delete_stream,
        //    rewriting each logical path into its `data/` full path.
        let data_locations = locations
            .map_ok({
                let inner = inner.clone();
                move |location| inner.full_path(&location)
            })
            .boxed();

        let data_deleted = inner.store.delete_stream(data_locations);

        // 2) Map each successfully deleted data path back to its logical path,
        //    then delete the corresponding metadata object.
        let meta_locations = data_deleted
            .map_ok({
                let inner = inner.clone();
                move |full_path| {
                    let location = inner.strip_prefix(full_path);
                    inner.meta_path(&location)
                }
            })
            .boxed();

        let meta_deleted = inner.store.delete_stream(meta_locations);

        // 3) Suppress NotFound on metadata (data deletion is the source of
        //    truth), invalidate the cache, and surface the logical path.
        meta_deleted
            .map({
                let inner = inner.clone();
                move |res| {
                    let inner = inner.clone();
                    async move {
                        match res {
                            Ok(meta_full_path) => {
                                let location = inner.strip_meta_prefix(meta_full_path);
                                inner.remove_meta_cache(&location).await;
                                Ok(location)
                            }
                            Err(Error::NotFound { path, .. }) => {
                                // Tolerate missing metadata; still return the
                                // corresponding logical path for the caller.
                                let location = inner.strip_meta_prefix(Path::from(path.as_str()));
                                inner.remove_meta_cache(&location).await;
                                Ok(location)
                            }
                            Err(err) => Err(err),
                        }
                    }
                }
            })
            .buffered(8)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = self.inner.full_path(prefix.unwrap_or(&Path::default()));
        let stream = self.inner.store.list(Some(&prefix));

        let inner = self.inner.clone();
        stream
            .map_ok(move |mut obj| {
                let store = inner.clone();
                async move {
                    let location = store.strip_prefix(obj.location);
                    let meta = store.get_meta(&location).await?;
                    obj.location = location;
                    obj.e_tag = meta.e_tag;
                    Ok::<ObjectMeta, Error>(obj)
                }
            })
            .try_buffered(8) // fetch metadata concurrently
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = self.inner.full_path(offset);
        let prefix = self.inner.full_path(prefix.unwrap_or(&Path::default()));
        let stream = self.inner.store.list_with_offset(Some(&prefix), &offset);

        let inner = self.inner.clone();
        stream
            .map_ok(move |mut obj| {
                let store = inner.clone();
                async move {
                    let location = store.strip_prefix(obj.location);
                    let meta = store.get_meta(&location).await?;
                    obj.location = location;
                    obj.e_tag = meta.e_tag;
                    Ok::<ObjectMeta, Error>(obj)
                }
            })
            .try_buffered(8) // fetch metadata concurrently
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = self.inner.full_path(prefix.unwrap_or(&Path::default()));
        let rt = self.inner.store.list_with_delimiter(Some(&prefix)).await?;
        let common_prefixes = rt
            .common_prefixes
            .into_iter()
            .map(|p| self.inner.strip_prefix(p))
            .collect::<Vec<_>>();

        let objects = rt
            .objects
            .into_iter()
            .map(|mut meta| {
                meta.location = self.inner.strip_prefix(meta.location);
                meta
            })
            .collect::<Vec<_>>();

        // Fetch the metadata for each object concurrently while preserving
        // the original listing order.
        let inner = self.inner.clone();
        let mut indexed =
            futures::stream::iter(objects.into_iter().enumerate().map(move |(idx, mut obj)| {
                let store = inner.clone();
                async move {
                    let meta = store.get_meta(&obj.location).await?;
                    obj.e_tag = meta.e_tag;
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
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let full_from = self.inner.full_path(from);
        let full_to = self.inner.full_path(to);
        self.inner
            .store
            .copy_opts(&full_from, &full_to, options.clone())
            .await?;

        let meta_from = self.inner.meta_path(from);
        let meta_to = self.inner.meta_path(to);
        self.inner
            .store
            .copy_opts(&meta_from, &meta_to, options)
            .await?;
        self.inner.remove_meta_cache(to).await;
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let full_from = self.inner.full_path(from);
        let full_to = self.inner.full_path(to);
        self.inner
            .store
            .rename_opts(&full_from, &full_to, options.clone())
            .await?;
        self.inner.remove_meta_cache(from).await;

        let meta_from = self.inner.meta_path(from);
        let meta_to = self.inner.meta_path(to);
        self.inner
            .store
            .rename_opts(&meta_from, &meta_to, options)
            .await?;
        self.inner.remove_meta_cache(to).await;
        Ok(())
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
    /// Reference to the MetaStoreBuilder
    store: Arc<MetaStoreBuilder<T>>,
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
        let payload = Bytes::from(payload);
        self.size += payload.len();
        self.hasher.update(&payload);
        self.inner.put_part(payload.into())
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let hash: [u8; 32] = self.hasher.clone().finalize().into();
        let mut rt = self.inner.complete().await?;
        let obj = self
            .store
            .store
            .head(&self.store.full_path(&self.location))
            .await?;

        let meta = Metadata {
            size: self.size as u64,
            e_tag: Some(BASE64_URL_SAFE.encode(hash)),
            original_tag: obj.e_tag,
            original_version: obj.version,
        };
        rt.e_tag = meta.e_tag.clone();
        self.store.put_meta(&self.location, meta).await?;
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

/// Re-clones an [`Arc<Error>`] returned from a `moka` shared computation
/// (e.g. [`Cache::try_get_with`]) into an owned [`Error`].
///
/// `moka` deduplicates concurrent loaders by returning the same `Arc<Error>`
/// to every waiter. Because [`object_store::Error`] is not [`Clone`], we must
/// reconstruct an equivalent variant by hand. Variants that carry a `path`
/// are reconstructed with their `path` and a stringified `source`; everything
/// else collapses into [`Error::Generic`].
fn map_arc_error(err: Arc<Error>) -> Error {
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
            store: "MetaStore",
            source: err.to_string().into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{integration::*, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

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
    #[ignore]
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
