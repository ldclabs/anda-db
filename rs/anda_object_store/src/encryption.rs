use aes_gcm::{AeadInPlace, Aes256Gcm, Key, Nonce, Tag};
use async_stream::try_stream;
use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_URL_SAFE};
use bytes::Bytes;
use ciborium::{from_reader, into_writer};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use moka::{future::Cache, ops::compute::Op};
use object_store::{path::Path, *};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteArray;
use sha3::Digest;
use std::{fmt::Debug, ops::Range, sync::Arc, time::Duration};

use crate::{
    apply_logical_etag_preconditions, check_update_version, map_arc_error, sha3_256,
    validate_ranges,
};

const DEFAULT_CHUNK_SIZE: u64 = 256 * 1024;

/// An object store implementation that provides transparent AES-256-GCM encryption and decryption
/// for stored objects.
///
/// `EncryptedStore` wraps another object store implementation and handles encryption/decryption
/// of data before it is passed to the underlying store. It also manages metadata for each object
/// to store encryption details.
///
/// # Features
/// - Transparent encryption/decryption using AES-256-GCM
/// - Chunked encryption for large objects
/// - Metadata caching for improved performance
/// - Optional conditional put operations
///
/// # Security considerations
///
/// This implementation uses AES-256-GCM for encryption which provides:
/// - Confidentiality: Data is encrypted and cannot be read without the key
/// - Integrity: Tampering with encrypted data will be detected
/// - Authentication: Only possessors of the key can modify data
///
/// # Performance considerations
///
/// - Chunk size affects both storage efficiency and random access performance
/// - Increasing chunk size improves throughput but reduces random access efficiency
/// - For large objects with frequent random access, consider using smaller chunks
///
/// # Example
/// ```rust,no_run
/// use anda_object_store::EncryptedStoreBuilder;
/// use object_store::memory::InMemory;
///
/// // Create a secret key
/// let secret = [0u8; 32]; // In production, use a secure random key
///
/// // Create an encrypted store with an in-memory backend
/// let store = InMemory::new();
/// let encrypted_store = EncryptedStoreBuilder::with_secret(store, 1000, secret)
///     .build();
/// ```
///
/// # Example 2
/// ```rust,no_run
/// use anda_object_store::EncryptedStoreBuilder;
/// use object_store::local::LocalFileSystem;
///
/// // Create a secret key
/// let secret = [0u8; 32]; // In production, use a secure random key
///
/// // Create an encrypted store with an local file system backend
/// let store = LocalFileSystem::new_with_prefix("my_store").unwrap();
/// let encrypted_store = EncryptedStoreBuilder::with_secret(store, 1000, secret)
///     .with_chunk_size(1024 * 1024) // Set chunk size to 1 MB
///     .with_conditional_put() // Should be enabled for LocalFileSystem
///     .build();
/// ```
#[derive(Clone)]
pub struct EncryptedStore<T: ObjectStore> {
    inner: Arc<EncryptedStoreBuilder<T>>,
}

/// Builder for configuring and creating an [`EncryptedStore`] instance.
///
/// All optional knobs (chunk size, conditional put, custom metadata cache)
/// have sensible defaults; only the underlying store, metadata cache
/// capacity and AES-256-GCM key need to be supplied.
pub struct EncryptedStoreBuilder<T: ObjectStore> {
    /// The underlying object store that holds ciphertext and metadata.
    store: T,
    /// Shared AES-256-GCM cipher used for both encryption and decryption.
    cipher: Arc<Aes256Gcm>,
    /// Plaintext chunk size in bytes. Each chunk is encrypted independently
    /// with its own derived nonce and authentication tag.
    chunk_size: u64,
    /// When true, expose the content-addressable e_tag and honour
    /// `PutMode::Update`/`if_match`/`if_none_match` preconditions even on
    /// backends (such as the local filesystem) that don't support them
    /// natively.
    conditional_put: bool,
    /// Path prefix for the encrypted data objects (default: `"data"`).
    data_prefix: Path,
    /// Path prefix for the per-object metadata objects (default: `"meta"`).
    meta_prefix: Path,
    /// In-memory metadata cache to avoid round-trips on hot paths.
    meta_cache: Cache<Path, Arc<Metadata>>,
}

/// Per-object encryption metadata stored alongside the ciphertext.
///
/// Serialized as compact CBOR (single-letter field names) and persisted at
/// `meta/<location>`. The corresponding ciphertext lives at `data/<location>`
/// and is laid out as `ceil(size / chunk_size)` fixed-size encrypted chunks.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Metadata {
    /// Size of the ciphertext in bytes (also the plaintext size, since
    /// AES-256-GCM in this implementation is length-preserving and the
    /// authentication tags are stored out-of-band in [`Metadata::aes_tags`]).
    #[serde(rename = "s")]
    size: u64,

    /// Content-addressable ETag computed as the URL-safe Base64 encoding of
    /// SHA3-256 over the *ciphertext*. Exposed to callers as the object's
    /// ETag whenever conditional-put mode is enabled.
    #[serde(rename = "e")]
    e_tag: Option<String>,

    /// ETag returned by the underlying storage when the ciphertext was
    /// written. Used to translate `if_match`/`if_none_match` preconditions.
    #[serde(rename = "o")]
    original_tag: Option<String>,

    /// Version returned by the underlying storage on the most recent put,
    /// when the backend supports object versioning.
    #[serde(rename = "v")]
    original_version: Option<String>,

    /// 12-byte base nonce, randomly generated per object. The per-chunk GCM
    /// nonce is derived as `derive_gcm_nonce(base_nonce, chunk_index)` so
    /// that every chunk uses a unique nonce under the shared key.
    #[serde(rename = "n")]
    aes_nonce: ByteArray<12>,

    /// 16-byte AES-GCM authentication tag for each ciphertext chunk, in
    /// chunk-index order. The number of entries equals
    /// `ceil(size / chunk_size)`.
    #[serde(rename = "t")]
    aes_tags: Vec<ByteArray<16>>,
}

impl<T: ObjectStore> std::fmt::Display for EncryptedStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptedStore({:?})", self.inner.store)
    }
}

impl<T: ObjectStore> std::fmt::Debug for EncryptedStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptedStore({:?})", self.inner.store)
    }
}

impl<T: ObjectStore> EncryptedStoreBuilder<T> {
    /// Creates a new `EncryptedStoreBuilder` with the provided secret key.
    ///
    /// This method initializes an AES-256-GCM cipher using the provided secret key.
    ///
    /// # Parameters
    /// - `store`: The underlying object store implementation
    /// - `meta_cache_capacity`: Maximum number of metadata entries to cache
    /// - `secret`: A 32-byte secret key for AES-256-GCM encryption
    ///
    /// # Returns
    /// A new `EncryptedStoreBuilder` instance
    pub fn with_secret(store: T, meta_cache_capacity: u64, secret: [u8; 32]) -> Self {
        use aes_gcm::aead::KeyInit;

        let key = Key::<Aes256Gcm>::from(secret);
        EncryptedStoreBuilder::new(store, meta_cache_capacity, Arc::new(Aes256Gcm::new(&key)))
    }

    /// Creates a new `EncryptedStoreBuilder` with the provided AES-256-GCM cipher.
    ///
    /// This method allows for more flexibility in how the cipher is created.
    ///
    /// # Parameters
    /// - `store`: The underlying object store implementation
    /// - `meta_cache_capacity`: Maximum number of metadata entries to cache
    /// - `cipher`: An AES-256-GCM cipher instance
    ///
    /// # Returns
    /// A new `EncryptedStoreBuilder` instance with default settings
    pub fn new(store: T, meta_cache_capacity: u64, cipher: Arc<Aes256Gcm>) -> Self {
        EncryptedStoreBuilder {
            store,
            cipher,
            chunk_size: DEFAULT_CHUNK_SIZE,
            conditional_put: false,
            data_prefix: Path::from("data"),
            meta_prefix: Path::from("meta"),
            meta_cache: Cache::builder()
                .max_capacity(meta_cache_capacity)
                .time_to_live(Duration::from_secs(60 * 60))
                .time_to_idle(Duration::from_secs(20 * 60))
                .build(),
        }
    }

    /// Sets the cache for metadata.
    ///
    /// This cache is used to store metadata for objects, improving performance.
    ///
    /// # Parameters
    /// - `cache`: The cache to use for metadata
    ///
    /// # Returns
    /// The builder with the updated metadata cache
    pub fn with_meta_cache(self, cache: Cache<Path, Arc<Metadata>>) -> Self {
        Self {
            meta_cache: cache,
            ..self
        }
    }

    /// Sets the chunk size for encryption operations.
    ///
    /// Large objects are split into chunks of this size before encryption.
    /// Each chunk is encrypted separately. Values smaller than 1 byte are
    /// normalized to 1 byte.
    ///
    /// # Parameters
    /// - `chunk_size`: The size of each chunk in bytes, default is 256 KB
    ///
    /// # Returns
    /// The builder with the updated chunk size
    pub fn with_chunk_size(self, chunk_size: u64) -> Self {
        Self {
            chunk_size: normalize_chunk_size(chunk_size),
            ..self
        }
    }

    /// Enables conditional put operations (Should enable with LocalFileSystem store).
    ///
    /// When enabled, put operations will check the extend e-tag of the existing object
    /// before overwriting it, providing optimistic concurrency control.
    ///
    /// # Returns
    /// The builder with conditional put enabled
    pub fn with_conditional_put(self) -> Self {
        Self {
            conditional_put: true,
            ..self
        }
    }

    /// Builds and returns an `EncryptedStore` with the configured settings.
    ///
    /// # Returns
    /// A new `EncryptedStore` instance
    pub fn build(self) -> EncryptedStore<T> {
        EncryptedStore {
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
            store: "EncryptedStore",
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
            .map_err(|err| map_arc_error("EncryptedStore", err))?;

        Ok(meta.as_ref().clone())
    }

    async fn put_meta(&self, location: &Path, meta: Metadata) -> Result<PutResult> {
        let meta_path = self.meta_path(location);
        let mut data = Vec::new();
        into_writer(&meta, &mut data).map_err(|err| Error::Generic {
            store: "EncryptedStore",
            source: format!("Failed to serialize Metadata: {err:?}").into(),
        })?;
        // Persist first, then cache. Avoids leaving a non-persisted entry
        // visible to readers if the underlying put fails.
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
                    store: "EncryptedStore",
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

    async fn get_chunk(&self, location: &Path, idx: u64, total_size: u64) -> Result<Vec<u8>> {
        let full_path = self.full_path(location);
        let start = idx * self.chunk_size;
        let end = ((idx + 1) * self.chunk_size).min(total_size);
        if start >= end {
            return Ok(Vec::new());
        }
        let data = self.store.get_range(&full_path, start..end).await?;
        Ok(data.into())
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for EncryptedStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let rt = self
            .inner
            .update_meta_with(location, async |meta| {
                if self.inner.conditional_put
                    && let PutMode::Update(v) = &opts.mode
                {
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
                let payload = Bytes::from(payload);

                let base_nonce: [u8; 12] = rand_bytes();
                let mut data: Vec<u8> = payload.into();
                let mut aes_tags: Vec<ByteArray<16>> = Vec::new();
                for (i, chunk) in data.chunks_mut(self.inner.chunk_size as usize).enumerate() {
                    let nonce = derive_gcm_nonce(&base_nonce, i as u64);
                    let tag = self
                        .inner
                        .cipher
                        .encrypt_in_place_detached(Nonce::from_slice(&nonce), &[], chunk)
                        .map_err(|err| Error::Generic {
                            store: "EncryptedStore",
                            source: format!("AES256 encrypt failed for path {location}: {err:?}")
                                .into(),
                        })?;
                    let tag: [u8; 16] = tag.into();
                    aes_tags.push(tag.into());
                }

                let hash = sha3_256(&data);
                let mut meta = Metadata {
                    size: data.len() as u64,
                    e_tag: Some(BASE64_URL_SAFE.encode(hash)),
                    original_tag: None,
                    original_version: None,
                    aes_nonce: base_nonce.into(),
                    aes_tags,
                };

                let rt = self
                    .inner
                    .store
                    .put_opts(&full_path, data.into(), opts)
                    .await?;

                meta.original_tag = rt.e_tag;
                meta.original_version = rt.version;
                Ok(meta)
            })
            .await?;

        if self.inner.conditional_put {
            Ok(PutResult {
                e_tag: rt.e_tag.clone(),
                version: rt.original_version.clone(),
            })
        } else {
            Ok(PutResult {
                e_tag: rt.original_tag.clone(),
                version: rt.original_version.clone(),
            })
        }
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

        Ok(Box::new(EncryptedStoreUploader {
            buf: Vec::new(),
            hasher: sha3::Sha3_256::new(),
            size: 0,
            aes_nonce: rand_bytes(),
            aes_tags: Vec::new(),
            chunk_index: 0,
            location: location.clone(),
            store: self.inner.clone(),
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, mut options: GetOptions) -> Result<GetResult> {
        let full_path = self.inner.full_path(location);
        let meta = self.inner.get_meta(location).await?;

        if self.inner.conditional_put {
            apply_logical_etag_preconditions(
                location,
                &mut options,
                meta.e_tag.as_deref(),
                meta.original_tag.clone(),
            )?;
        }

        // Resolve the caller-supplied (plaintext) range, defaulting to the
        // full object when no range is specified.
        let range = if let Some(r) = &options.range {
            r.as_range(meta.size)
                .map_err(|source| object_store::Error::Generic {
                    store: "EncryptedStore",
                    source: source.into(),
                })?
        } else {
            0..meta.size
        };

        // Expand the request to whole-chunk boundaries: AES-GCM is not a
        // streaming cipher, so we must read each chunk in full to verify its
        // authentication tag before yielding the (possibly trimmed) plaintext.
        let rr = if range.start == range.end {
            options.range = None;
            options.head = true;
            range.start..range.start
        } else {
            let rr_start = (range.start / self.inner.chunk_size) * self.inner.chunk_size;
            let rr_end = range
                .end
                .saturating_sub(1)
                .checked_div(self.inner.chunk_size)
                .and_then(|idx| idx.checked_add(1))
                .and_then(|idx| idx.checked_mul(self.inner.chunk_size))
                .unwrap_or(u64::MAX)
                .min(meta.size);

            rr_start..rr_end
        };

        if rr.end > rr.start {
            options.range = Some(GetRange::Bounded(rr.clone()));
        }

        let res = self.inner.store.get_opts(&full_path, options).await?;
        let mut obj = res.meta.clone();
        obj.location = self.inner.strip_prefix(obj.location);
        if self.inner.conditional_put {
            obj.e_tag = meta.e_tag;
        }

        let attributes = res.attributes.clone();

        let chunk_size = self.inner.chunk_size as usize;
        let start_idx = rr.start as usize / chunk_size;
        let start_offset = (range.start - rr.start) as usize;
        let size = (range.end - range.start) as usize;

        let stream = create_decryption_stream(
            res,
            self.inner.cipher.clone(),
            meta.aes_tags.clone(),
            *meta.aes_nonce,
            location.clone(),
            chunk_size,
            start_idx,
            start_offset,
            size,
        );

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta: obj,
            range,
            attributes,
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let meta = self.inner.get_meta(location).await?;
        validate_ranges("EncryptedStore", ranges, meta.size)?;

        let mut result: Vec<Bytes> = Vec::with_capacity(ranges.len());
        let mut chunk_cache: Option<(usize, Vec<u8>)> = None; // cache the last chunk read
        for &Range { start, end } in ranges {
            let mut buf = Vec::with_capacity((end - start) as usize);
            // Calculate the chunk indices we need to read
            let start_chunk = (start / self.inner.chunk_size) as usize;
            let end_chunk = ((end - 1) / self.inner.chunk_size) as usize;

            for idx in start_chunk..=end_chunk {
                // Calculate the byte range within this chunk
                let chunk_start = if idx == start_chunk {
                    start % self.inner.chunk_size
                } else {
                    0
                };

                let chunk_end = if idx == end_chunk {
                    (end - 1) % self.inner.chunk_size + 1
                } else {
                    self.inner.chunk_size
                };

                match &chunk_cache {
                    Some((cached_idx, cached_chunk)) if *cached_idx == idx => {
                        buf.extend_from_slice(
                            &cached_chunk[chunk_start as usize..chunk_end as usize],
                        );
                    }
                    _ => {
                        let tag = meta.aes_tags.get(idx).ok_or_else(|| Error::Generic {
                            store: "EncryptedStore",
                            source: format!(
                                "missing AES256 tag for chunk {idx} for path {location}"
                            )
                            .into(),
                        })?;

                        let nonce = derive_gcm_nonce(&meta.aes_nonce, idx as u64);
                        let mut chunk = self
                            .inner
                            .get_chunk(location, idx as u64, meta.size)
                            .await?;
                        self.inner
                            .cipher
                            .decrypt_in_place_detached(
                                Nonce::from_slice(&nonce),
                                &[],
                                &mut chunk,
                                Tag::from_slice(tag.as_slice()),
                            )
                            .map_err(|err| Error::Generic {
                                store: "EncryptedStore",
                                source: format!(
                                    "AES256 decrypt failed for path {location}: {err:?}"
                                )
                                .into(),
                            })?;
                        buf.extend_from_slice(&chunk[chunk_start as usize..chunk_end as usize]);
                        chunk_cache = Some((idx, chunk));
                    }
                }
            }
            result.push(buf.into());
        }

        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();

        // 1) Delete the encrypted data via the inner store's delete_stream,
        //    rewriting each logical path to its `data/` full path.
        let data_locations = locations
            .map_ok({
                let inner = inner.clone();
                move |location| inner.full_path(&location)
            })
            .boxed();

        let data_deleted = inner.store.delete_stream(data_locations);

        // 2) Map each successfully deleted data path back to its logical path,
        //    then delete the corresponding metadata object via delete_stream.
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

        // 3) Suppress NotFound on metadata, invalidate the cache, and return
        //    the logical path.
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
                                // corresponding logical path.
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
        if !self.inner.conditional_put {
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
        if !self.inner.conditional_put {
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

        if !self.inner.conditional_put {
            return Ok(ListResult {
                common_prefixes,
                objects,
            });
        }

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

        // Restore the original listing order based on the captured index.
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

/// Streaming multipart-upload handler for [`EncryptedStore`].
///
/// Buffers caller-supplied parts until at least one full plaintext chunk is
/// available, then encrypts the chunk in place, records its authentication
/// tag, and forwards the ciphertext to the underlying multipart upload. The
/// final, possibly short, tail chunk is flushed by [`MultipartUpload::complete`].
pub struct EncryptedStoreUploader<T: ObjectStore> {
    /// Plaintext bytes that have not yet been packed into a full chunk.
    buf: Vec<u8>,
    /// Running SHA3-256 hasher over the *ciphertext*. Provides the
    /// content-addressable e_tag for the finished object.
    hasher: sha3::Sha3_256,
    /// Total number of plaintext bytes accepted so far.
    size: usize,
    /// Per-chunk AES-GCM authentication tags, in chunk-index order.
    aes_tags: Vec<ByteArray<16>>,
    /// 12-byte base nonce, randomly generated when the upload starts. Each
    /// chunk uses `derive_gcm_nonce(aes_nonce, chunk_index)`.
    aes_nonce: [u8; 12],
    /// Index of the next chunk to encrypt, used as the GCM nonce counter.
    chunk_index: u64,
    /// Logical (caller-visible) path of the object being uploaded.
    location: Path,
    /// Shared reference back to the configured store builder.
    store: Arc<EncryptedStoreBuilder<T>>,
    /// Underlying multipart upload handler against the inner store.
    inner: Box<dyn MultipartUpload>,
}

impl<T: ObjectStore> std::fmt::Debug for EncryptedStoreUploader<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptedStoreUploader({})", self.location)
    }
}

#[async_trait]
impl<T: ObjectStore> MultipartUpload for EncryptedStoreUploader<T> {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let payload = Bytes::from(payload);
        self.size += payload.len();
        self.buf.extend_from_slice(&payload);
        if self.buf.len() < self.store.chunk_size as usize {
            return Box::pin(futures::future::ready(Ok(())));
        }

        let mut parts: Vec<UploadPart> = Vec::new();

        while self.buf.len() >= self.store.chunk_size as usize {
            let mut chunk = self
                .buf
                .drain(..self.store.chunk_size as usize)
                .collect::<Vec<u8>>();

            let nonce = derive_gcm_nonce(&self.aes_nonce, self.chunk_index);
            self.chunk_index = self.chunk_index.wrapping_add(1);
            match self.store.cipher.encrypt_in_place_detached(
                Nonce::from_slice(&nonce),
                &[],
                &mut chunk,
            ) {
                Ok(tag) => {
                    let tag: [u8; 16] = tag.into();
                    self.aes_tags.push(tag.into());
                }
                Err(err) => {
                    return Box::pin(futures::future::ready(Err(Error::Generic {
                        store: "EncryptedStore",
                        source: format!(
                            "AES256 encrypt failed for path {}: {err:?}",
                            self.location
                        )
                        .into(),
                    })));
                }
            }
            self.hasher.update(&chunk);
            parts.push(self.inner.put_part(chunk.into()))
        }

        Box::pin(async move {
            for part in parts {
                part.await?;
            }

            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let mut processed = 0u64;
        for chunk in self.buf.chunks_mut(self.store.chunk_size as usize) {
            let nonce = derive_gcm_nonce(&self.aes_nonce, self.chunk_index + processed);
            let tag = self
                .store
                .cipher
                .encrypt_in_place_detached(Nonce::from_slice(&nonce), &[], chunk)
                .map_err(|err| Error::Generic {
                    store: "EncryptedStore",
                    source: format!("AES256 encrypt failed for path {}: {err:?}", self.location)
                        .into(),
                })?;
            let tag: [u8; 16] = tag.into();
            self.aes_tags.push(tag.into());
            self.hasher.update(&chunk);
            self.inner.put_part(chunk.to_vec().into()).await?;
            processed += 1;
        }
        self.chunk_index = self.chunk_index.wrapping_add(processed);

        self.buf.clear();
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
            aes_nonce: self.aes_nonce.into(),
            aes_tags: self.aes_tags.clone(),
        };

        if self.store.conditional_put {
            rt.e_tag = meta.e_tag.clone();
        }
        self.store.put_meta(&self.location, meta).await?;
        Ok(rt)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

/// Builds a [`BoxStream`] of plaintext bytes from the underlying ciphertext
/// stream returned by `inner.store.get_opts(...)`.
///
/// The stream re-buffers incoming bytes into chunk-sized blocks, decrypts
/// each block in place using the supplied per-chunk authentication tag, and
/// trims the leading and trailing bytes so the consumer only sees the
/// caller's requested plaintext range:
///
/// - `start_idx` — index of the first chunk that intersects the request.
/// - `start_offset` — byte offset within the first chunk to begin yielding.
/// - `size` — total number of plaintext bytes to yield before completing.
///
/// The function expects the upstream stream to deliver every requested
/// ciphertext chunk in full; partial trailing data is decrypted in the
/// post-loop fallback so short last chunks (length < `chunk_size`) are
/// handled correctly.
#[allow(clippy::too_many_arguments)]
fn create_decryption_stream(
    res: GetResult,
    cipher: Arc<Aes256Gcm>,
    aes_tags: Vec<ByteArray<16>>,
    base_nonce: [u8; 12],
    location: Path,
    chunk_size: usize,
    start_idx: usize,
    start_offset: usize,
    size: usize,
) -> BoxStream<'static, Result<Bytes>> {
    try_stream! {
        let mut stream = res.into_stream();
        // Pre-allocate enough capacity to absorb at least two chunks before
        // hitting a reallocation in the steady state.
        let mut buf = Vec::with_capacity(chunk_size * 2);
        let mut idx = start_idx;
        let mut remaining = size;

        if remaining == 0 {
            return;
        }

        while let Some(data) = stream.next().await {
            let data = data?;
            buf.extend_from_slice(&data);

            while remaining > 0 && buf.len() >= chunk_size {
                let mut chunk = buf.drain(..chunk_size).collect::<Vec<u8>>();

                let tag = aes_tags.get(idx).ok_or_else(|| Error::Generic {
                    store: "EncryptedStore",
                    source: format!("missing AES256 tag for chunk {idx} for path {location}").into(),
                })?;

                let nonce = derive_gcm_nonce(&base_nonce, idx as u64);
                cipher.decrypt_in_place_detached(
                    Nonce::from_slice(&nonce),
                    &[],
                    &mut chunk,
                    Tag::from_slice(tag.as_slice())
                )
                .map_err(|err| Error::Generic {
                    store: "EncryptedStore",
                    source: format!("AES256 decrypt failed for path {location}: {err:?}").into(),
                })?;
                // Trim the leading offset on the first chunk.
                if idx == start_idx && start_offset > 0 {
                    chunk.drain(..start_offset);
                }

                if chunk.len() > remaining {
                    chunk.truncate(remaining);
                }

                remaining = remaining.saturating_sub(chunk.len());
                yield Bytes::from(chunk);

                idx += 1;
                if remaining == 0 {
                    // Requested size satisfied; stop early.
                    return;
                }
            }
        }

        if remaining > 0 && !buf.is_empty() {
            let tag = aes_tags.get(idx).ok_or_else(|| Error::Generic {
                store: "EncryptedStore",
                source: format!("missing AES256 tag for chunk {idx} for path {location}").into(),
            })?;
            let nonce = derive_gcm_nonce(&base_nonce, idx as u64);
            cipher.decrypt_in_place_detached(
                Nonce::from_slice(&nonce),
                &[],
                &mut buf,
                Tag::from_slice(tag.as_slice())
            )
            .map_err(|err| Error::Generic {
                store: "EncryptedStore",
                source: format!("AES256 decrypt failed for path {location}: {err:?}").into(),
            })?;

            if idx == start_idx && start_offset > 0 {
                if start_offset > buf.len() {
                    Err(Error::Generic {
                        store: "EncryptedStore",
                        source: format!(
                            "truncated encrypted data for path {location}: expected at least {start_offset} bytes in chunk {idx}, got {}",
                            buf.len()
                        )
                        .into(),
                    })?;
                }
                buf.drain(..start_offset);
            }

            if buf.len() < remaining {
                Err(Error::Generic {
                    store: "EncryptedStore",
                    source: format!(
                        "truncated encrypted data for path {location}: expected {remaining} more bytes, got {}",
                        buf.len()
                    )
                    .into(),
                })?;
            }

            let final_len = remaining;
            buf.truncate(final_len);
            remaining = 0;
            yield Bytes::from(buf);
        }

        if remaining > 0 {
            Err(Error::Generic {
                store: "EncryptedStore",
                source: format!(
                    "truncated encrypted data for path {location}: expected {remaining} more bytes"
                )
                .into(),
            })?;
        }
    }.boxed()
}

fn normalize_chunk_size(chunk_size: u64) -> u64 {
    chunk_size.clamp(1, usize::MAX as u64)
}

/// Generates `N` cryptographically-strong random bytes using the OS RNG.
fn rand_bytes<const N: usize>() -> [u8; N] {
    let mut rng = rand::rng();
    let mut bytes = [0u8; N];
    rng.fill_bytes(&mut bytes);
    bytes
}

/// Derives a unique 96-bit AES-GCM nonce for chunk `idx` from a per-object
/// `base` nonce.
///
/// The first 4 bytes of `base` are kept as a random salt; the trailing 8
/// bytes are interpreted as a little-endian counter and incremented by `idx`.
/// Because each object has its own random `base`, distinct chunks of distinct
/// objects always produce distinct nonces under the shared key, satisfying
/// AES-GCM's nonce-uniqueness requirement.
fn derive_gcm_nonce(base: &[u8; 12], idx: u64) -> [u8; 12] {
    let mut nonce = *base;
    let mut ctr = [0u8; 8];
    ctr.copy_from_slice(&nonce[4..12]);
    let c = u64::from_le_bytes(ctr).wrapping_add(idx);
    nonce[4..12].copy_from_slice(&c.to_le_bytes());
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{integration::*, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn test_with_memory() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 10000, [0u8; 32]).build();

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

        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 10000, [0u8; 32]).build();
        stream_get(&storage).await;
    }

    #[tokio::test]
    async fn test_with_memory_conditional_put() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 10000, [0u8; 32])
            .with_conditional_put()
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

        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 10000, [0u8; 32])
            .with_conditional_put()
            .build();
        stream_get(&storage).await;
    }

    #[tokio::test]
    async fn zero_chunk_size_is_normalized() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_chunk_size(0)
            .build();
        let location = Path::from("zero-chunk-size");

        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        let requested = 0..3;
        let ranges = storage
            .get_ranges(&location, std::slice::from_ref(&requested))
            .await
            .unwrap();
        assert_eq!(ranges, vec![Bytes::from_static(b"abc")]);
    }

    #[tokio::test]
    async fn conditional_get_opts_accepts_comma_separated_logical_etags() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_conditional_put()
            .build();
        let location = Path::from("encrypted-etag-list");
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
    async fn conditional_put_update_rejects_stale_version() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_conditional_put()
            .build();
        let location = Path::from("encrypted-stale-version");
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
    async fn truncated_ciphertext_errors_on_stream_read() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("truncated");

        storage
            .put(&location, Bytes::from_static(b"abcdefgh").into())
            .await
            .unwrap();

        let data_path = Path::from("data/truncated");
        let ciphertext = inner.get(&data_path).await.unwrap().bytes().await.unwrap();
        inner
            .put(&data_path, ciphertext.slice(..4).into())
            .await
            .unwrap();

        let err = storage
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap_err();

        assert!(err.to_string().contains("truncated encrypted data"));
    }

    #[tokio::test]
    #[ignore]
    async fn test_with_local_file() {
        let root = TempDir::new().unwrap();
        let storage = EncryptedStoreBuilder::with_secret(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            10000,
            [0u8; 32],
        )
        .with_conditional_put()
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
        let storage = EncryptedStoreBuilder::with_secret(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            10000,
            [0u8; 32],
        )
        .with_conditional_put()
        .build();
        stream_get(&storage).await;
    }
}
