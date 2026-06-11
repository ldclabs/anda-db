use aes_gcm::{AeadInPlace, Aes256Gcm, Key, Nonce, Tag};
use async_stream::try_stream;
use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_URL_SAFE};
use bytes::{Buf, Bytes, BytesMut};
use futures::{StreamExt, stream::BoxStream};
use moka::future::Cache;
use object_store::{path::Path, *};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteArray;
use sha3::Digest;
use std::{ops::Range, sync::Arc, time::Duration};

use crate::{
    apply_logical_etag_preconditions, check_update_version, sha3_256,
    sidecar::{SidecarMeta, SidecarStore},
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
    /// Shared sidecar core: underlying store, path prefixes, metadata cache.
    inner: Arc<SidecarStore<T, Metadata>>,
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

    /// Plaintext chunk size (in bytes) the object was encrypted with.
    /// Recorded at write time so reads keep working even when the store is
    /// later reconfigured with a different chunk size. Metadata written by
    /// older versions lacks this field; readers then fall back to the
    /// store's configured chunk size.
    #[serde(rename = "c", default, skip_serializing_if = "Option::is_none")]
    chunk_size: Option<u64>,
}

impl SidecarMeta for Metadata {
    const STORE_NAME: &'static str = "EncryptedStore";

    fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    fn set_original(&mut self, e_tag: Option<String>, version: Option<String>) {
        self.original_tag = e_tag;
        self.original_version = version;
    }
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
    /// The chunk size is recorded in each object's metadata at write time,
    /// so existing objects remain readable after the store is reconfigured
    /// with a different chunk size.
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
            inner: Arc::new(SidecarStore::new(self.store, self.meta_cache)),
            cipher: self.cipher,
            chunk_size: self.chunk_size,
            conditional_put: self.conditional_put,
        }
    }
}

impl<T: ObjectStore> EncryptedStore<T> {
    /// Chunk size to use when reading an object, preferring the size
    /// recorded in its metadata over the store's current configuration.
    fn read_chunk_size(&self, meta: &Metadata) -> u64 {
        meta.chunk_size
            .filter(|&c| c > 0)
            .map(normalize_chunk_size)
            .unwrap_or(self.chunk_size)
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
                if self.conditional_put
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

                // Gather the payload into a single mutable buffer for
                // in-place chunked encryption (exactly one copy, even for
                // multi-segment payloads).
                let mut data = Vec::with_capacity(payload.content_length());
                for segment in payload.iter() {
                    data.extend_from_slice(segment);
                }

                let base_nonce: [u8; 12] = rand_bytes();
                let chunk_size = self.chunk_size as usize;
                let mut aes_tags: Vec<ByteArray<16>> =
                    Vec::with_capacity(data.len().div_ceil(chunk_size));
                for (i, chunk) in data.chunks_mut(chunk_size).enumerate() {
                    let nonce = derive_gcm_nonce(&base_nonce, i as u64);
                    let tag = self
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
                    chunk_size: Some(self.chunk_size),
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

        if self.conditional_put {
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
            cipher: self.cipher.clone(),
            chunk_size: self.chunk_size,
            conditional_put: self.conditional_put,
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, mut options: GetOptions) -> Result<GetResult> {
        let full_path = self.inner.full_path(location);
        let meta = self.inner.get_meta(location).await?;

        if self.conditional_put {
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
        let chunk_size = self.read_chunk_size(&meta);
        let rr = if range.start == range.end {
            options.range = None;
            options.head = true;
            range.start..range.start
        } else {
            let rr_start = (range.start / chunk_size) * chunk_size;
            let rr_end = range
                .end
                .saturating_sub(1)
                .checked_div(chunk_size)
                .and_then(|idx| idx.checked_add(1))
                .and_then(|idx| idx.checked_mul(chunk_size))
                .unwrap_or(u64::MAX)
                .min(meta.size);

            rr_start..rr_end
        };

        if rr.end > rr.start {
            options.range = Some(GetRange::Bounded(rr.clone()));
        }

        let mut res = self.inner.store.get_opts(&full_path, options).await?;
        let attributes = std::mem::take(&mut res.attributes);
        let mut obj = res.meta.clone();
        obj.location = self.inner.strip_prefix(obj.location);
        if self.conditional_put {
            obj.e_tag = meta.e_tag.clone();
        }

        let start_idx = (rr.start / chunk_size) as usize;
        let start_offset = (range.start - rr.start) as usize;
        let size = range.end - range.start;

        let stream = create_decryption_stream(
            res,
            self.cipher.clone(),
            meta,
            location.clone(),
            chunk_size as usize,
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

        let chunk_size = self.read_chunk_size(&meta);
        let full_path = self.inner.full_path(location);

        let mut result: Vec<Bytes> = Vec::with_capacity(ranges.len());
        // The most recently decrypted, chunk-aligned plaintext span. It
        // serves subsequent ranges that fall entirely within it, which is
        // common for clustered reads.
        let mut cached_span = 0u64..0u64;
        let mut cached = Bytes::new();

        for &Range { start, end } in ranges {
            if start < cached_span.start || end > cached_span.end {
                // Fetch all chunks intersecting the range with a single
                // request and decrypt them in place.
                let span_start = (start / chunk_size) * chunk_size;
                let span_end = ((end - 1) / chunk_size)
                    .saturating_add(1)
                    .saturating_mul(chunk_size)
                    .min(meta.size);
                let first_idx = start / chunk_size;

                let data = self
                    .inner
                    .store
                    .get_range(&full_path, span_start..span_end)
                    .await?;
                if data.len() as u64 != span_end - span_start {
                    return Err(Error::Generic {
                        store: "EncryptedStore",
                        source: format!(
                            "truncated encrypted data for path {location}: expected {} bytes, got {}",
                            span_end - span_start,
                            data.len()
                        )
                        .into(),
                    });
                }

                let mut data: Vec<u8> = data.into();
                for (i, chunk) in data.chunks_mut(chunk_size as usize).enumerate() {
                    let idx = first_idx + i as u64;
                    let tag = meta
                        .aes_tags
                        .get(idx as usize)
                        .ok_or_else(|| Error::Generic {
                            store: "EncryptedStore",
                            source: format!(
                                "missing AES256 tag for chunk {idx} for path {location}"
                            )
                            .into(),
                        })?;
                    let nonce = derive_gcm_nonce(&meta.aes_nonce, idx);
                    self.cipher
                        .decrypt_in_place_detached(
                            Nonce::from_slice(&nonce),
                            &[],
                            chunk,
                            Tag::from_slice(tag.as_slice()),
                        )
                        .map_err(|err| Error::Generic {
                            store: "EncryptedStore",
                            source: format!("AES256 decrypt failed for path {location}: {err:?}")
                                .into(),
                        })?;
                }

                cached = Bytes::from(data);
                cached_span = span_start..span_end;
            }

            let s = (start - cached_span.start) as usize;
            let e = (end - cached_span.start) as usize;
            // Share the decrypted buffer when the caller asked for most of
            // it; copy small slices so they don't pin a whole span in memory.
            if (e - s) * 2 >= cached.len() {
                result.push(cached.slice(s..e));
            } else {
                result.push(Bytes::copy_from_slice(&cached[s..e]));
            }
        }

        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.clone().delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.clone().list(prefix, self.conditional_put)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner
            .clone()
            .list_with_offset(prefix, offset, self.conditional_put)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner
            .list_with_delimiter(prefix, self.conditional_put)
            .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Streaming multipart-upload handler for [`EncryptedStore`].
///
/// Buffers caller-supplied parts until at least one full plaintext chunk is
/// available, then encrypts all complete chunks in place, records their
/// authentication tags, and forwards them to the underlying multipart upload
/// as a single part (preserving the caller's part granularity, which matters
/// for backends with minimum part sizes). The final, possibly short, tail
/// chunk is flushed by [`MultipartUpload::complete`].
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
    /// Shared sidecar core of the originating [`EncryptedStore`].
    store: Arc<SidecarStore<T, Metadata>>,
    /// Shared AES-256-GCM cipher.
    cipher: Arc<Aes256Gcm>,
    /// Plaintext chunk size (in bytes) the upload encrypts with.
    chunk_size: u64,
    /// Whether the originating store exposes the content-addressable e_tag.
    conditional_put: bool,
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
        let chunk_size = self.chunk_size as usize;
        self.size += payload.content_length();
        for segment in payload.iter() {
            self.buf.extend_from_slice(segment);
        }
        if self.buf.len() < chunk_size {
            return Box::pin(futures::future::ready(Ok(())));
        }

        // Split off the bytes beyond the last complete chunk boundary,
        // encrypt the complete chunks in place, and forward them as a single
        // part. This preserves the caller's part granularity, which matters
        // for backends with minimum part sizes (e.g. S3).
        let split = self.buf.len() / chunk_size * chunk_size;
        let mut data = std::mem::take(&mut self.buf);
        self.buf = data.split_off(split);

        for chunk in data.chunks_mut(chunk_size) {
            let nonce = derive_gcm_nonce(&self.aes_nonce, self.chunk_index);
            self.chunk_index = self.chunk_index.wrapping_add(1);
            match self
                .cipher
                .encrypt_in_place_detached(Nonce::from_slice(&nonce), &[], chunk)
            {
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
        }

        self.hasher.update(&data);
        self.inner.put_part(data.into())
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // Flush the tail. After put_part the buffer holds less than one
        // chunk, but stay defensive and handle any leftover amount.
        if !self.buf.is_empty() {
            let mut data = std::mem::take(&mut self.buf);
            for chunk in data.chunks_mut(self.chunk_size as usize) {
                let nonce = derive_gcm_nonce(&self.aes_nonce, self.chunk_index);
                self.chunk_index = self.chunk_index.wrapping_add(1);
                let tag = self
                    .cipher
                    .encrypt_in_place_detached(Nonce::from_slice(&nonce), &[], chunk)
                    .map_err(|err| Error::Generic {
                        store: "EncryptedStore",
                        source: format!(
                            "AES256 encrypt failed for path {}: {err:?}",
                            self.location
                        )
                        .into(),
                    })?;
                let tag: [u8; 16] = tag.into();
                self.aes_tags.push(tag.into());
            }
            self.hasher.update(&data);
            self.inner.put_part(data.into()).await?;
        }

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
            chunk_size: Some(self.chunk_size),
        };

        if self.conditional_put {
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
    meta: Arc<Metadata>,
    location: Path,
    chunk_size: usize,
    start_idx: usize,
    start_offset: usize,
    size: u64,
) -> BoxStream<'static, Result<Bytes>> {
    try_stream! {
        let mut stream = res.into_stream();
        let mut buf = BytesMut::new();
        let mut idx = start_idx;
        let mut remaining = size;

        if remaining == 0 {
            return;
        }

        while let Some(data) = stream.next().await {
            let data = data?;
            buf.extend_from_slice(&data);

            while remaining > 0 && buf.len() >= chunk_size {
                // O(1) split; the chunk is a unique view into the buffer.
                let mut chunk = buf.split_to(chunk_size);

                let tag = meta.aes_tags.get(idx).ok_or_else(|| Error::Generic {
                    store: "EncryptedStore",
                    source: format!("missing AES256 tag for chunk {idx} for path {location}").into(),
                })?;

                let nonce = derive_gcm_nonce(&meta.aes_nonce, idx as u64);
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
                    chunk.advance(start_offset);
                }

                if chunk.len() as u64 > remaining {
                    chunk.truncate(remaining as usize);
                }

                remaining -= chunk.len() as u64;
                idx += 1;
                yield chunk.freeze();

                if remaining == 0 {
                    // Requested size satisfied; stop early.
                    return;
                }
            }
        }

        if remaining > 0 && !buf.is_empty() {
            let tag = meta.aes_tags.get(idx).ok_or_else(|| Error::Generic {
                store: "EncryptedStore",
                source: format!("missing AES256 tag for chunk {idx} for path {location}").into(),
            })?;
            let nonce = derive_gcm_nonce(&meta.aes_nonce, idx as u64);
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
                buf.advance(start_offset);
            }

            if (buf.len() as u64) < remaining {
                Err(Error::Generic {
                    store: "EncryptedStore",
                    source: format!(
                        "truncated encrypted data for path {location}: expected {remaining} more bytes, got {}",
                        buf.len()
                    )
                    .into(),
                })?;
            }

            buf.truncate(remaining as usize);
            remaining = 0;
            yield buf.freeze();
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

    #[test]
    fn builder_custom_cache_and_display_debug_are_exercised() {
        let cache = Cache::builder().max_capacity(1).build();
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_meta_cache(cache)
            .build();

        assert!(format!("{storage}").contains("EncryptedStore"));
        assert!(format!("{storage:?}").contains("EncryptedStore"));

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
    async fn recorded_chunk_size_survives_reconfiguration() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("chunked");
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz");

        storage
            .put(&location, payload.clone().into())
            .await
            .unwrap();

        // Reopen the store with a different configured chunk size; reads
        // must honour the chunk size recorded in the object's metadata.
        let storage = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_chunk_size(16)
            .build();

        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, payload);

        let ranges = storage
            .get_ranges(&location, &[3..11, 0..26, 7..8])
            .await
            .unwrap();
        assert_eq!(ranges[0], payload.slice(3..11));
        assert_eq!(ranges[1], payload);
        assert_eq!(ranges[2], payload.slice(7..8));

        let bytes = storage.get_range(&location, 5..23).await.unwrap();
        assert_eq!(bytes, payload.slice(5..23));
    }

    #[tokio::test]
    async fn get_ranges_covers_multi_chunk_and_repeated_ranges() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("multi-chunk");
        let payload: Vec<u8> = (0u8..=255).collect();

        storage
            .put(&location, Bytes::from(payload.clone()).into())
            .await
            .unwrap();

        let ranges = vec![
            0..256,
            5..6,
            4..8,
            1..2,
            250..256,
            0..1,
            255..256,
            8..200,
            7..9,
        ];
        let got = storage.get_ranges(&location, &ranges).await.unwrap();
        for (range, bytes) in ranges.iter().zip(&got) {
            assert_eq!(
                bytes.as_ref(),
                &payload[range.start as usize..range.end as usize],
                "range {range:?}"
            );
        }
    }

    #[tokio::test]
    async fn delete_nonexistent_reports_logical_path() {
        let root = TempDir::new().unwrap();
        let storage = EncryptedStoreBuilder::with_secret(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            100,
            [0u8; 32],
        )
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
    async fn conditional_copy_and_rename_refresh_original_tag_for_logical_etag_preconditions() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_conditional_put()
            .build();
        let source = Path::from("encrypted-copy-source");
        let copied = Path::from("encrypted-copy-target");
        let renamed = Path::from("encrypted-rename-target");
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
