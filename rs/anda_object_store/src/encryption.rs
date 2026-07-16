use aes_gcm::{AeadInOut, Aes256Gcm, Key, Nonce, Tag};
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
    check_get_preconditions, check_update_version,
    sidecar::{ListingMetaPolicy, SidecarMeta, SidecarStore, new_generation},
    validate_ranges,
};

const DEFAULT_CHUNK_SIZE: u64 = 256 * 1024;
const CHUNK_AAD_LEGACY: u8 = 0;
const CHUNK_AAD_BOUND: u8 = 1;

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
/// - Conditional put operations on every backend
///
/// # Security considerations
///
/// This implementation uses AES-256-GCM for encryption which provides:
/// - Confidentiality: Data is encrypted and cannot be read without the key
/// - Integrity: Tampering with encrypted data will be detected
/// - Authentication: Only possessors of the key can modify data
///
/// Each object is encrypted with a random 96-bit base nonce whose trailing
/// 64 bits act as a per-chunk counter, so nonce uniqueness within an object
/// is guaranteed and cross-object collisions require both a 32-bit salt
/// match and overlapping counter ranges. Following NIST SP 800-38D guidance
/// for random IVs, keep the total number of objects encrypted under a single
/// key well below 2^32; rotate the key (or derive per-tenant subkeys) for
/// larger deployments.
///
/// # Crash semantics
///
/// A put writes the ciphertext to a fresh immutable generation object and
/// then commits by switching the metadata pointer with a single backend put.
/// A crash before the pointer switch leaves the previous version fully
/// intact and decryptable; a crash after it means the put took effect. Torn
/// "old metadata + new ciphertext" states — which would surface as AES-GCM
/// authentication failures indistinguishable from tampering — are impossible
/// by construction. See the crate-level documentation for the full contract.
///
/// # Performance considerations
///
/// - Chunk size affects both storage efficiency and random access performance
/// - Increasing chunk size improves throughput but reduces random access efficiency
/// - For large objects with frequent random access, consider using smaller chunks
/// - `put`/`put_opts` buffers the whole payload once for in-place encryption
///   (peak memory ≈ 2× object size including the caller's copy); prefer
///   `put_multipart`, which encrypts streaming chunk by chunk, for large
///   objects
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
    /// When true, reject legacy sidecar metadata that carries no
    /// authentication fields instead of accepting it with a warning.
    strict_metadata_auth: bool,
}

/// Builder for configuring and creating an [`EncryptedStore`] instance.
///
/// All optional knobs (chunk size, custom metadata cache) have sensible
/// defaults; only the underlying store, metadata cache capacity and
/// AES-256-GCM key need to be supplied.
pub struct EncryptedStoreBuilder<T: ObjectStore> {
    /// The underlying object store that holds ciphertext and metadata.
    store: T,
    /// Shared AES-256-GCM cipher used for both encryption and decryption.
    cipher: Arc<Aes256Gcm>,
    /// Plaintext chunk size in bytes. Each chunk is encrypted independently
    /// with its own derived nonce and authentication tag.
    chunk_size: u64,
    /// When true, reject legacy sidecar metadata without authentication.
    strict_metadata_auth: bool,
    /// In-memory metadata cache to avoid round-trips on hot paths.
    meta_cache: Cache<Path, Arc<Metadata>>,
}

/// Per-object encryption metadata stored alongside the ciphertext.
///
/// Serialized as compact CBOR (single-letter field names) and persisted at
/// `meta/<location>` — the object's commit point. The ciphertext lives at the
/// immutable generation object `gen/<location>/<generation>` (or, for
/// pre-0.10 documents without a generation, at the legacy `data/<location>`
/// object) and is laid out as `ceil(size / chunk_size)` fixed-size encrypted
/// chunks.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Metadata {
    /// Size of the ciphertext in bytes (also the plaintext size, since
    /// AES-256-GCM in this implementation is length-preserving and the
    /// authentication tags are stored out-of-band in [`Metadata::aes_tags`]).
    #[serde(rename = "s")]
    size: u64,

    /// Content-addressable ETag computed as the URL-safe Base64 encoding of
    /// SHA3-256 over the *ciphertext*. Exposed to callers as the object's
    /// ETag.
    #[serde(rename = "e")]
    e_tag: Option<String>,

    /// Legacy field of the pre-0.10 mutable dual-object layout (the inner
    /// backend's ETag). Retained because it participates in the sealed AAD
    /// of existing documents; never populated by new writes.
    #[serde(rename = "o")]
    original_tag: Option<String>,

    /// Legacy field of the pre-0.10 mutable dual-object layout (the inner
    /// backend's version). Retained because it participates in the sealed
    /// AAD of existing documents; never populated by new writes.
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

    /// Chunk authentication-data version.
    ///
    /// Older objects used an empty AAD for each AES-GCM chunk. New objects
    /// bind the chunk size and index into the chunk tag. This field lets
    /// path-authenticated metadata keep legacy objects readable after a
    /// copy/rename migration.
    #[serde(rename = "av", default, skip_serializing_if = "Option::is_none")]
    chunk_aad_version: Option<u8>,

    /// Nonce used to authenticate the sidecar metadata with AES-GCM GMAC.
    #[serde(rename = "an", default, skip_serializing_if = "Option::is_none")]
    auth_nonce: Option<ByteArray<12>>,

    /// Authentication tag over the logical path and metadata fields.
    #[serde(rename = "at", default, skip_serializing_if = "Option::is_none")]
    auth_tag: Option<ByteArray<16>>,

    /// Generation pointer: the ciphertext lives at
    /// `gen/<location>/<generation>`. `None` means the legacy layout
    /// (`data/<location>`). Bound into the metadata authentication AAD when
    /// present (absent for pre-0.10 documents, whose AAD layout is
    /// preserved byte-for-byte).
    #[serde(rename = "g", default, skip_serializing_if = "Option::is_none")]
    generation: Option<String>,
}

impl SidecarMeta for Metadata {
    const STORE_NAME: &'static str = "EncryptedStore";

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
            strict_metadata_auth: false,
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

    /// Retained for API compatibility: conditional-put semantics (the
    /// content-addressable ETag, `PutMode::Update` and
    /// `if_match`/`if_none_match` preconditions) are now always enabled on
    /// every backend, because the immutable-generation protocol evaluates
    /// them against the metadata commit point instead of forwarding them.
    ///
    /// # Returns
    /// The builder, unchanged
    pub fn with_conditional_put(self) -> Self {
        self
    }

    /// Requires every sidecar metadata document to be authenticated.
    ///
    /// Metadata written since the introduction of metadata authentication is
    /// always sealed with an AES-GCM tag binding it to its logical path.
    /// Metadata written by older versions carries no such tag ("legacy") and
    /// is accepted by default — with a warning log — so existing data stays
    /// readable. An attacker with write access to the underlying store could
    /// exploit that fallback by stripping the authentication fields from a
    /// sealed document (a downgrade attack); stripped documents that still
    /// carry other v1 fields (or a generation pointer) are always rejected,
    /// but fully stripped ones are indistinguishable from genuine legacy
    /// metadata.
    ///
    /// Enable strict mode once all legacy objects have been rewritten (e.g.
    /// via copy/rename, which reseals metadata): legacy metadata is then
    /// rejected outright, closing the downgrade window.
    ///
    /// The policy also applies to `list`, `list_with_offset`, and
    /// `list_with_delimiter`: authenticated-but-tampered metadata is rejected
    /// in both modes; compatibility mode accepts genuine legacy documents and
    /// skips documents that no longer decode, while strict mode rejects
    /// legacy and undecodable documents.
    ///
    /// # Returns
    /// The builder with strict metadata authentication enabled
    pub fn with_strict_metadata_auth(self) -> Self {
        Self {
            strict_metadata_auth: true,
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
            strict_metadata_auth: self.strict_metadata_auth,
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

    fn seal_metadata(&self, location: &Path, meta: &mut Metadata) -> Result<()> {
        seal_metadata(&self.cipher, location, meta)
    }

    fn verify_metadata(&self, location: &Path, meta: &Metadata) -> Result<MetadataAuth> {
        verify_metadata(&self.cipher, location, meta, self.strict_metadata_auth)
    }

    /// Listing policy shared by all three `list*` entry points.
    ///
    /// Every decoded document is authenticated before it is surfaced.
    /// Compatibility mode accepts genuine legacy metadata and skips torn
    /// CBOR; strict mode rejects both.
    fn listing_meta_policy(&self) -> ListingMetaPolicy<Metadata> {
        let cipher = self.cipher.clone();
        let strict = self.strict_metadata_auth;
        ListingMetaPolicy::verified(strict, move |location, meta| {
            verify_metadata(&cipher, location, meta, strict)?;
            Ok(())
        })
    }

    async fn verified_metadata(&self, location: &Path) -> Result<Metadata> {
        let meta = self.inner.get_meta(location).await?;
        self.verify_metadata(location, &meta)?;
        Ok((*meta).clone())
    }

    /// Runs mark-sweep garbage collection over the ciphertext objects.
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
impl<T: ObjectStore> ObjectStore for EncryptedStore<T> {
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
                            self.verify_metadata(location, m)?;
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
                    let aad = chunk_aad(self.chunk_size, i as u64);
                    let tag = self
                        .cipher
                        .encrypt_inout_detached(&Nonce::from(nonce), &aad, chunk.into())
                        .map_err(|err| Error::Generic {
                            store: "EncryptedStore",
                            source: format!("AES256 encrypt failed for path {location}: {err:?}")
                                .into(),
                        })?;
                    let tag: [u8; 16] = tag.into();
                    aes_tags.push(tag.into());
                }

                // The logical ETag must be unique per commit: conditional
                // updates compare it as the CAS token, and hashing the bare
                // ciphertext collides for short payloads (a one-byte
                // ciphertext has only 256 possible values, so distinct
                // counter values can produce the same tag and a stale token
                // can pass the precondition — a lost update). Seeding the
                // hash with the per-commit random nonce makes collisions
                // negligible while still revealing nothing about the
                // plaintext.
                let mut hasher = sha3::Sha3_256::new();
                hasher.update(base_nonce);
                hasher.update(&data);
                let hash: [u8; 32] = hasher.finalize().into();
                let generation = new_generation();
                let mut meta = Metadata {
                    size: data.len() as u64,
                    e_tag: Some(BASE64_URL_SAFE.encode(hash)),
                    original_tag: None,
                    original_version: None,
                    aes_nonce: base_nonce.into(),
                    aes_tags,
                    chunk_size: Some(self.chunk_size),
                    chunk_aad_version: Some(CHUNK_AAD_BOUND),
                    auth_nonce: None,
                    auth_tag: None,
                    generation: Some(generation.clone()),
                };

                // Write the ciphertext to a fresh immutable generation; the
                // metadata put below is the commit point.
                let gen_path = self.inner.generation_path(location, &generation);
                let ciphertext: PutPayload = data.into();
                let mut data_opts = opts.clone();
                data_opts.mode = PutMode::Overwrite;
                self.inner
                    .store
                    .put_opts(&gen_path, ciphertext, data_opts)
                    .await?;

                self.seal_metadata(location, &mut meta)?;
                Ok(meta)
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

        // Seed the running ciphertext hasher with the per-upload nonce so
        // the logical ETag is unique per commit; see `put_opts`.
        let aes_nonce: [u8; 12] = rand_bytes();
        let mut hasher = sha3::Sha3_256::new();
        hasher.update(aes_nonce);
        Ok(Box::new(EncryptedStoreUploader {
            buf: Vec::new(),
            hasher,
            size: 0,
            aes_nonce,
            aes_tags: Vec::new(),
            chunk_index: 0,
            location: location.clone(),
            generation,
            store: self.inner.clone(),
            cipher: self.cipher.clone(),
            chunk_size: self.chunk_size,
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut retried = false;
        loop {
            let meta = self.inner.get_meta(location).await?;
            self.verify_metadata(location, &meta)?;

            let mut options = options.clone();
            check_get_preconditions(location, &mut options, meta.e_tag.as_deref())?;

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

            // A HEAD request must not fetch or decrypt any payload: backends
            // that honour `head` return an empty body, which the decryption
            // stream would otherwise report as truncated ciphertext.
            let range = if options.head {
                range.start..range.start
            } else {
                range
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

            let payload_path = self
                .inner
                .payload_path(location, meta.generation.as_deref());
            let mut res = match self.inner.store.get_opts(&payload_path, options).await {
                Ok(res) => res,
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
            };
            let attributes = std::mem::take(&mut res.attributes);
            let mut obj = res.meta.clone();
            obj.location = location.clone();
            obj.e_tag = meta.e_tag.clone();
            // Versions are not reported; see the crate documentation.
            obj.version = None;

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

            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta: obj,
                range,
                attributes,
                extensions: Extensions::default(),
            });
        }
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let mut retried = false;
        'retry: loop {
            let meta = self.inner.get_meta(location).await?;
            self.verify_metadata(location, &meta)?;
            validate_ranges("EncryptedStore", ranges, meta.size)?;

            let chunk_size = self.read_chunk_size(&meta);
            let payload_path = self
                .inner
                .payload_path(location, meta.generation.as_deref());

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

                    let data = match self
                        .inner
                        .store
                        .get_range(&payload_path, span_start..span_end)
                        .await
                    {
                        Ok(data) => data,
                        Err(Error::NotFound { source, .. }) => {
                            if !retried && meta.generation.is_some() {
                                retried = true;
                                self.inner.refresh_meta(location).await?;
                                continue 'retry;
                            }
                            return Err(Error::NotFound {
                                path: location.to_string(),
                                source,
                            });
                        }
                        Err(err) => return Err(err),
                    };
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
                        let tag =
                            meta.aes_tags
                                .get(idx as usize)
                                .ok_or_else(|| Error::Generic {
                                    store: "EncryptedStore",
                                    source: format!(
                                        "missing AES256 tag for chunk {idx} for path {location}"
                                    )
                                    .into(),
                                })?;
                        let nonce = derive_gcm_nonce(&meta.aes_nonce, idx);
                        let aad = chunk_aad_for_meta(&meta, chunk_size, idx)?;
                        self.cipher
                            .decrypt_inout_detached(
                                &Nonce::from(nonce),
                                &aad,
                                chunk.into(),
                                &Tag::from(**tag),
                            )
                            .map_err(|err| Error::Generic {
                                store: "EncryptedStore",
                                source: format!(
                                    "AES256 decrypt failed for path {location}: {err:?}"
                                )
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

            return Ok(result);
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.clone().delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.clone().list(prefix, self.listing_meta_policy())
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner
            .clone()
            .list_with_offset(prefix, offset, self.listing_meta_policy())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner
            .list_with_delimiter(prefix, self.listing_meta_policy())
            .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let create = matches!(options.mode, CopyMode::Create);
        // The ciphertext chunks are not bound to the path (their AAD carries
        // chunk size and index only), so the payload is copied verbatim;
        // only the metadata document is resealed below for the target path.
        // The pointer switch is the commit point.
        let cipher = self.cipher.clone();
        let strict = self.strict_metadata_auth;
        let (src, generation) = self
            .inner
            .copy_payload(from, to, |location, meta| {
                verify_metadata(&cipher, location, meta, strict)?;
                Ok(())
            })
            .await?;

        let mut meta = (*src).clone();
        meta.generation = Some(generation);
        meta.original_tag = None;
        meta.original_version = None;
        // Pin the chunk-AAD version explicitly so legacy ciphertext stays
        // readable under the resealed (authenticated) target document.
        ensure_chunk_aad_version(&mut meta)?;
        self.seal_metadata(to, &mut meta)?;
        self.inner
            .update_meta_with(to, create, async |_| Ok(meta))
            .await?;
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        if from == to {
            // A self-rename must not delete the object's commit point
            // (`from` and `to` share the same document), nor be forwarded to
            // the backend (whose rename may be implemented as copy+delete).
            self.verified_metadata(from).await?;
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

/// Streaming multipart-upload handler for [`EncryptedStore`].
///
/// Buffers caller-supplied parts until at least one full plaintext chunk is
/// available, then encrypts all complete chunks in place, records their
/// authentication tags, and forwards them to the underlying multipart upload
/// as a single part (preserving the caller's part granularity, which matters
/// for backends with minimum part sizes). The final, possibly short, tail
/// chunk is flushed by [`MultipartUpload::complete`].
///
/// Because forwarded parts are trimmed down to a whole multiple of the
/// chunk size (the remainder stays buffered), a caller part can shrink by up
/// to `chunk_size - 1` bytes before reaching the backend. On backends with a
/// minimum part size (e.g. S3's 5 MiB), supply parts of at least the
/// backend minimum plus one chunk size.
///
/// The parts are uploaded into a fresh immutable generation object;
/// `complete` materializes it and then switches the metadata pointer under
/// the store's per-location critical section, so a crash (or failure) before
/// the switch leaves the previous version fully readable.
pub struct EncryptedStoreUploader<T: ObjectStore> {
    /// Plaintext bytes that have not yet been packed into a full chunk.
    buf: Vec<u8>,
    /// Running SHA3-256 hasher over the per-upload nonce followed by the
    /// *ciphertext* (unique per commit; see `put_opts`). Provides the
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
    /// Generation the ciphertext is uploaded into.
    generation: String,
    /// Shared sidecar core of the originating [`EncryptedStore`].
    store: Arc<SidecarStore<T, Metadata>>,
    /// Shared AES-256-GCM cipher.
    cipher: Arc<Aes256Gcm>,
    /// Plaintext chunk size (in bytes) the upload encrypts with.
    chunk_size: u64,
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
            let aad = chunk_aad(self.chunk_size, self.chunk_index);
            self.chunk_index = self.chunk_index.wrapping_add(1);
            match self
                .cipher
                .encrypt_inout_detached(&Nonce::from(nonce), &aad, chunk.into())
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
                let aad = chunk_aad(self.chunk_size, self.chunk_index);
                self.chunk_index = self.chunk_index.wrapping_add(1);
                let tag = self
                    .cipher
                    .encrypt_inout_detached(&Nonce::from(nonce), &aad, chunk.into())
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
        let e_tag = Some(BASE64_URL_SAFE.encode(hash));

        // Materialize the generation object, then switch the metadata
        // pointer inside the per-key critical section. A failure (or crash)
        // before the switch leaves the previous version fully readable.
        let store = self.store.clone();
        let location = self.location.clone();
        let cipher = self.cipher.clone();
        let generation = self.generation.clone();
        let size = self.size as u64;
        let aes_nonce = self.aes_nonce;
        let aes_tags = self.aes_tags.clone();
        let chunk_size = self.chunk_size;
        let inner = &mut self.inner;
        store
            .update_meta_with(&location, false, async |_| {
                inner.complete().await?;
                let mut meta = Metadata {
                    size,
                    e_tag: e_tag.clone(),
                    original_tag: None,
                    original_version: None,
                    aes_nonce: aes_nonce.into(),
                    aes_tags,
                    chunk_size: Some(chunk_size),
                    chunk_aad_version: Some(CHUNK_AAD_BOUND),
                    auth_nonce: None,
                    auth_tag: None,
                    generation: Some(generation.clone()),
                };
                seal_metadata(&cipher, &location, &mut meta)?;
                Ok(meta)
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
                let aad = chunk_aad_for_meta(&meta, chunk_size as u64, idx as u64)?;
                cipher.decrypt_inout_detached(
                    &Nonce::from(nonce),
                    &aad,
                    (&mut chunk[..]).into(),
                    &Tag::from(**tag)
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
            let aad = chunk_aad_for_meta(&meta, chunk_size as u64, idx as u64)?;
            cipher.decrypt_inout_detached(
                &Nonce::from(nonce),
                &aad,
                (&mut buf[..]).into(),
                &Tag::from(**tag)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataAuth {
    Authenticated,
    Legacy,
}

fn seal_metadata(cipher: &Aes256Gcm, location: &Path, meta: &mut Metadata) -> Result<()> {
    let nonce: [u8; 12] = rand_bytes();
    let aad = metadata_auth_aad(location, meta);
    let mut empty = [];
    let tag = cipher
        .encrypt_inout_detached(&Nonce::from(nonce), &aad, (&mut empty[..]).into())
        .map_err(|err| Error::Generic {
            store: "EncryptedStore",
            source: format!("metadata authentication failed for path {location}: {err:?}").into(),
        })?;
    let tag: [u8; 16] = tag.into();
    meta.auth_nonce = Some(nonce.into());
    meta.auth_tag = Some(tag.into());
    Ok(())
}

fn verify_metadata(
    cipher: &Aes256Gcm,
    location: &Path,
    meta: &Metadata,
    strict: bool,
) -> Result<MetadataAuth> {
    let (nonce, tag) = match (meta.auth_nonce.as_ref(), meta.auth_tag.as_ref()) {
        (Some(nonce), Some(tag)) => (nonce, tag),
        (None, None) => {
            // `chunk_aad_version` and the generation pointer were introduced
            // together with (or after) metadata authentication: every writer
            // that records them also seals the document. Their presence
            // without authentication fields therefore means the fields were
            // stripped (a downgrade attack) or the document is corrupted —
            // never genuine legacy metadata.
            if meta.chunk_aad_version.is_some() || meta.generation.is_some() {
                return Err(Error::Generic {
                    store: "EncryptedStore",
                    source: format!("stripped metadata authentication fields for path {location}")
                        .into(),
                });
            }
            if strict {
                return Err(Error::Generic {
                    store: "EncryptedStore",
                    source: format!(
                        "unauthenticated legacy metadata rejected (strict mode) for path {location}"
                    )
                    .into(),
                });
            }
            chunk_aad_version(meta)?;
            log::warn!(
                "EncryptedStore: accepting unauthenticated legacy metadata for {location}; \
                 rewrite the object (or copy/rename it) to seal it, then enable \
                 strict metadata authentication"
            );
            return Ok(MetadataAuth::Legacy);
        }
        (None, Some(_)) => {
            return Err(Error::Generic {
                store: "EncryptedStore",
                source: format!("missing metadata authentication nonce for path {location}").into(),
            });
        }
        (Some(_), None) => {
            return Err(Error::Generic {
                store: "EncryptedStore",
                source: format!("missing metadata authentication tag for path {location}").into(),
            });
        }
    };

    let aad = metadata_auth_aad(location, meta);
    let mut empty = [];
    cipher
        .decrypt_inout_detached(
            &Nonce::from(**nonce),
            &aad,
            (&mut empty[..]).into(),
            &Tag::from(**tag),
        )
        .map_err(|err| Error::Generic {
            store: "EncryptedStore",
            source: format!("metadata authentication failed for path {location}: {err:?}").into(),
        })?;
    chunk_aad_version(meta)?;
    Ok(MetadataAuth::Authenticated)
}

fn metadata_auth_aad(location: &Path, meta: &Metadata) -> Vec<u8> {
    let mut aad = Vec::new();
    aad.extend_from_slice(b"anda_object_store.encrypted.metadata.v1");
    push_bytes(&mut aad, location.to_string().as_bytes());
    aad.extend_from_slice(&meta.size.to_le_bytes());
    push_opt_str(&mut aad, meta.e_tag.as_deref());
    push_opt_str(&mut aad, meta.original_tag.as_deref());
    push_opt_str(&mut aad, meta.original_version.as_deref());
    push_bytes(&mut aad, meta.aes_nonce.as_slice());
    push_opt_u64(&mut aad, meta.chunk_size);
    push_opt_u8(&mut aad, meta.chunk_aad_version);
    aad.extend_from_slice(&(meta.aes_tags.len() as u64).to_le_bytes());
    for tag in &meta.aes_tags {
        push_bytes(&mut aad, tag.as_slice());
    }
    // Documents sealed before the immutable-generation protocol carry no
    // generation; append the field only when present so their AAD stays
    // byte-identical and they keep verifying.
    if let Some(generation) = &meta.generation {
        aad.extend_from_slice(b".g");
        push_bytes(&mut aad, generation.as_bytes());
    }
    aad
}

fn ensure_chunk_aad_version(meta: &mut Metadata) -> Result<()> {
    let version = chunk_aad_version(meta)?;
    meta.chunk_aad_version = Some(version);
    Ok(())
}

fn chunk_aad_version(meta: &Metadata) -> Result<u8> {
    let version = meta.chunk_aad_version.unwrap_or_else(|| {
        if meta.auth_nonce.is_some() && meta.auth_tag.is_some() {
            CHUNK_AAD_BOUND
        } else {
            CHUNK_AAD_LEGACY
        }
    });
    match version {
        CHUNK_AAD_LEGACY | CHUNK_AAD_BOUND => Ok(version),
        _ => Err(Error::Generic {
            store: "EncryptedStore",
            source: format!("unsupported encrypted chunk AAD version {version}").into(),
        }),
    }
}

fn chunk_aad_for_meta(meta: &Metadata, chunk_size: u64, chunk_index: u64) -> Result<Vec<u8>> {
    match chunk_aad_version(meta)? {
        CHUNK_AAD_LEGACY => Ok(Vec::new()),
        CHUNK_AAD_BOUND => Ok(chunk_aad(chunk_size, chunk_index)),
        _ => unreachable!("chunk_aad_version validates known versions"),
    }
}

fn chunk_aad(chunk_size: u64, chunk_index: u64) -> Vec<u8> {
    // 36 bytes of domain separation + 8 + 8 bytes of chunk binding.
    let mut aad = Vec::with_capacity(52);
    aad.extend_from_slice(b"anda_object_store.encrypted.chunk.v1");
    aad.extend_from_slice(&chunk_size.to_le_bytes());
    aad.extend_from_slice(&chunk_index.to_le_bytes());
    aad
}

fn push_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u64).to_le_bytes());
    out.extend_from_slice(value);
}

fn push_opt_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            push_bytes(out, value.as_bytes());
        }
        None => out.push(0),
    }
}

fn push_opt_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn push_opt_u8(out: &mut Vec<u8>, value: Option<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            out.push(value);
        }
        None => out.push(0),
    }
}

/// Generates `N` cryptographically-strong random bytes using [`rand::rng`],
/// a user-space CSPRNG (ChaCha) that is periodically reseeded from OS
/// entropy.
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
    use crate::sha3_256;
    use aes_gcm::KeyInit;
    use futures::TryStreamExt;
    use object_store::{integration::*, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    fn test_cipher() -> Aes256Gcm {
        Aes256Gcm::new(&Key::<Aes256Gcm>::from([0u8; 32]))
    }

    fn encrypt_chunks(
        cipher: &Aes256Gcm,
        base_nonce: &[u8; 12],
        plaintext: &[u8],
        chunk_size: u64,
        bound_aad: bool,
    ) -> (Vec<u8>, Vec<ByteArray<16>>) {
        let mut ciphertext = plaintext.to_vec();
        let mut aes_tags = Vec::with_capacity(ciphertext.len().div_ceil(chunk_size as usize));
        for (idx, chunk) in ciphertext.chunks_mut(chunk_size as usize).enumerate() {
            let nonce = derive_gcm_nonce(base_nonce, idx as u64);
            let aad = if bound_aad {
                chunk_aad(chunk_size, idx as u64)
            } else {
                Vec::new()
            };
            let tag = cipher
                .encrypt_inout_detached(&Nonce::from(nonce), &aad, chunk.into())
                .unwrap();
            let tag: [u8; 16] = tag.into();
            aes_tags.push(tag.into());
        }
        (ciphertext, aes_tags)
    }

    /// Writes an object in the pre-auth legacy layout directly into the
    /// backend: ciphertext at `data/<location>`, metadata without any
    /// authentication fields, chunk-AAD version or generation.
    async fn put_legacy_encrypted_object(
        inner: &InMemory,
        location: &Path,
        plaintext: &'static [u8],
        chunk_size: u64,
    ) {
        let cipher = test_cipher();
        let base_nonce = [7u8; 12];
        let chunk_size = normalize_chunk_size(chunk_size);
        let (ciphertext, aes_tags) =
            encrypt_chunks(&cipher, &base_nonce, plaintext, chunk_size, false);

        let hash = sha3_256(&ciphertext);
        let put = inner
            .put(
                &Path::from(format!("data/{location}")),
                Bytes::from(ciphertext).into(),
            )
            .await
            .unwrap();
        let meta = Metadata {
            size: plaintext.len() as u64,
            e_tag: Some(BASE64_URL_SAFE.encode(hash)),
            original_tag: put.e_tag,
            original_version: put.version,
            aes_nonce: base_nonce.into(),
            aes_tags,
            chunk_size: Some(chunk_size),
            chunk_aad_version: None,
            auth_nonce: None,
            auth_tag: None,
            generation: None,
        };
        let mut buf = Vec::new();
        cbor2::to_writer(&meta, &mut buf).unwrap();
        inner
            .put(&Path::from(format!("meta/{location}")), buf.into())
            .await
            .unwrap();
    }

    /// Writes an object exactly as anda_object_store 0.9.x did: ciphertext
    /// at `data/<location>`, sealed (authenticated) metadata with a bound
    /// chunk AAD but **no generation pointer**. Verifying it exercises the
    /// AAD compatibility of the generation field.
    async fn put_sealed_v1_object(
        inner: &InMemory,
        location: &Path,
        plaintext: &'static [u8],
        chunk_size: u64,
    ) {
        let cipher = test_cipher();
        let base_nonce = [9u8; 12];
        let chunk_size = normalize_chunk_size(chunk_size);
        let (ciphertext, aes_tags) =
            encrypt_chunks(&cipher, &base_nonce, plaintext, chunk_size, true);

        let hash = sha3_256(&ciphertext);
        let put = inner
            .put(
                &Path::from(format!("data/{location}")),
                Bytes::from(ciphertext).into(),
            )
            .await
            .unwrap();
        let mut meta = Metadata {
            size: plaintext.len() as u64,
            e_tag: Some(BASE64_URL_SAFE.encode(hash)),
            original_tag: put.e_tag,
            original_version: put.version,
            aes_nonce: base_nonce.into(),
            aes_tags,
            chunk_size: Some(chunk_size),
            chunk_aad_version: Some(CHUNK_AAD_BOUND),
            auth_nonce: None,
            auth_tag: None,
            generation: None,
        };
        seal_metadata(&cipher, location, &mut meta).unwrap();
        let mut buf = Vec::new();
        cbor2::to_writer(&meta, &mut buf).unwrap();
        inner
            .put(&Path::from(format!("meta/{location}")), buf.into())
            .await
            .unwrap();
    }

    /// Decodes the metadata document of `location` directly from the backend.
    async fn read_meta(inner: &InMemory, location: &Path) -> Metadata {
        let bytes = inner
            .get(&Path::from(format!("meta/{location}")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        cbor2::from_reader(&bytes[..]).unwrap()
    }

    /// Resolves the full backend path of `location`'s current ciphertext.
    async fn ciphertext_path(inner: &InMemory, location: &Path) -> Path {
        let meta = read_meta(inner, location).await;
        match meta.generation {
            Some(g) => Path::from(format!("gen/{location}/{g}")),
            None => Path::from(format!("data/{location}")),
        }
    }

    #[test]
    fn builder_custom_cache_and_display_debug_are_exercised() {
        let cache = Cache::builder().max_capacity(1).build();
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_meta_cache(cache)
            .with_conditional_put() // retained no-op, part of the public API
            .build();

        assert!(format!("{storage}").contains("EncryptedStore"));
        assert!(format!("{storage:?}").contains("EncryptedStore"));

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
    async fn legacy_metadata_without_auth_remains_readable() {
        let inner = InMemory::new();
        let location = Path::from("legacy-object");
        let payload = b"legacy encrypted payload";
        put_legacy_encrypted_object(&inner, &location, payload, 4).await;

        let storage = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_chunk_size(16)
            .build();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), payload);

        let ranges = storage.get_ranges(&location, &[0..6, 7..16]).await.unwrap();
        assert_eq!(ranges[0].as_ref(), &payload[0..6]);
        assert_eq!(ranges[1].as_ref(), &payload[7..16]);

        let range = storage.get_range(&location, 3..19).await.unwrap();
        assert_eq!(range.as_ref(), &payload[3..19]);
    }

    #[tokio::test]
    async fn sealed_v1_layout_still_verifies_and_upgrades() {
        let inner = InMemory::new();
        let location = Path::from("sealed-v1");
        let payload = b"sealed v1 payload";
        put_sealed_v1_object(&inner, &location, payload, 4).await;

        // A document sealed before the generation field existed must keep
        // verifying: its AAD layout is preserved byte-for-byte, even under
        // strict metadata authentication.
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .with_strict_metadata_auth()
            .build();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), payload);

        // The first overwrite migrates to the generation layout and removes
        // the legacy ciphertext.
        storage
            .put(&location, Bytes::from_static(b"upgraded").into())
            .await
            .unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"upgraded"));
        let meta = read_meta(&inner, &location).await;
        assert!(meta.generation.is_some());
        assert!(matches!(
            inner.get(&Path::from("data/sealed-v1")).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn legacy_metadata_copy_and_rename_reseal_legacy_chunk_aad() {
        let inner = InMemory::new();
        let source = Path::from("legacy-copy-source");
        let copied = Path::from("legacy-copy-target");
        let renamed = Path::from("legacy-rename-target");
        let payload = b"legacy copy rename payload";
        put_legacy_encrypted_object(&inner, &source, payload, 4).await;

        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(16)
            .build();
        storage.copy(&source, &copied).await.unwrap();

        let copied_meta = read_meta(&inner, &copied).await;
        assert_eq!(copied_meta.chunk_aad_version, Some(CHUNK_AAD_LEGACY));
        assert!(copied_meta.auth_nonce.is_some());
        assert!(copied_meta.auth_tag.is_some());
        assert!(copied_meta.generation.is_some());

        let bytes = storage.get(&copied).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), payload);

        storage.rename(&copied, &renamed).await.unwrap();
        let renamed_meta = read_meta(&inner, &renamed).await;
        assert_eq!(renamed_meta.chunk_aad_version, Some(CHUNK_AAD_LEGACY));
        assert!(renamed_meta.auth_nonce.is_some());
        assert!(renamed_meta.auth_tag.is_some());

        let bytes = storage.get(&renamed).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), payload);
        assert!(matches!(
            storage.get(&copied).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn metadata_path_binding_rejects_swapped_data_and_sidecar() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let a = Path::from("object-a");
        let b = Path::from("object-b");

        storage
            .put(&a, Bytes::from_static(b"aaaaaaaa").into())
            .await
            .unwrap();
        storage
            .put(&b, Bytes::from_static(b"bbbbbbbb").into())
            .await
            .unwrap();

        // Transplant object-a's ciphertext and sidecar wholesale onto
        // object-b's paths.
        let a_meta = read_meta(&inner, &a).await;
        let a_gen = a_meta.generation.clone().unwrap();
        let a_data = inner
            .get(&Path::from(format!("gen/object-a/{a_gen}")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        inner
            .put(&Path::from(format!("gen/object-b/{a_gen}")), a_data.into())
            .await
            .unwrap();
        let a_meta_bytes = inner
            .get(&Path::from("meta/object-a"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        inner
            .put(&Path::from("meta/object-b"), a_meta_bytes.into())
            .await
            .unwrap();

        let reopened = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let err = match reopened.get(&b).await {
            Ok(_) => panic!("swapped sidecar should fail metadata authentication"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("metadata authentication failed"));
    }

    #[tokio::test]
    async fn metadata_authentication_rejects_sidecar_mutation() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("tamper-meta");

        storage
            .put(&location, Bytes::from_static(b"abcdefgh").into())
            .await
            .unwrap();

        // Mutating the size fails authentication.
        let meta_path = Path::from("meta/tamper-meta");
        let mut meta = read_meta(&inner, &location).await;
        meta.size += 1;
        let mut tampered = Vec::new();
        cbor2::to_writer(&meta, &mut tampered).unwrap();
        inner.put(&meta_path, tampered.into()).await.unwrap();

        let reopened = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let err = match reopened.get(&location).await {
            Ok(_) => panic!("tampered sidecar should fail metadata authentication"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("metadata authentication failed"));

        // Repointing the generation is equally rejected: the pointer is
        // bound into the sealed AAD.
        let mut meta = read_meta(&inner, &location).await;
        meta.size -= 1; // restore
        meta.generation = Some("0000000000000009-11111111".to_string());
        let mut tampered = Vec::new();
        cbor2::to_writer(&meta, &mut tampered).unwrap();
        inner.put(&meta_path, tampered.into()).await.unwrap();

        let reopened = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let err = reopened.get(&location).await.unwrap_err();
        assert!(err.to_string().contains("metadata authentication failed"));
    }

    #[tokio::test]
    async fn copy_and_rename_reject_tampered_source_metadata_without_resealing() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32]).build();
        let copy_source = Path::from("tamper-copy-source");
        let copy_target = Path::from("tamper-copy-target");
        let rename_source = Path::from("tamper-rename-source");
        let rename_target = Path::from("tamper-rename-target");

        storage
            .put(&copy_source, Bytes::from_static(b"copy").into())
            .await
            .unwrap();
        storage
            .put(&rename_source, Bytes::from_static(b"rename").into())
            .await
            .unwrap();

        for meta_path in [
            Path::from("meta/tamper-copy-source"),
            Path::from("meta/tamper-rename-source"),
        ] {
            let meta_bytes = inner.get(&meta_path).await.unwrap().bytes().await.unwrap();
            let mut meta: Metadata = cbor2::from_reader(&meta_bytes[..]).unwrap();
            meta.e_tag = Some("forged".to_string());
            let mut tampered = Vec::new();
            cbor2::to_writer(&meta, &mut tampered).unwrap();
            inner.put(&meta_path, tampered.into()).await.unwrap();
        }

        let reopened = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32]).build();
        let err = reopened.copy(&copy_source, &copy_target).await.unwrap_err();
        assert!(err.to_string().contains("metadata authentication failed"));
        assert!(matches!(
            reopened.get(&copy_target).await,
            Err(Error::NotFound { .. })
        ));

        let err = reopened
            .rename(&rename_source, &rename_target)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("metadata authentication failed"));
        assert!(matches!(
            reopened.get(&rename_target).await,
            Err(Error::NotFound { .. })
        ));
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
    async fn get_opts_accepts_comma_separated_logical_etags() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32]).build();
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
    async fn copy_and_rename_preserve_logical_etag_preconditions() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32]).build();
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
    async fn put_update_rejects_stale_version() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32]).build();
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
                        e_tag: put.e_tag.clone(),
                        version: Some("stale".to_string()),
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }));

        // An e_tag-only Update succeeds (versions are not reported).
        storage
            .put_opts(
                &location,
                Bytes::from_static(b"def").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: put.e_tag,
                        version: put.version,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
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

        let data_path = ciphertext_path(&inner, &location).await;
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
    async fn stripped_metadata_auth_is_rejected() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("stripped");

        storage
            .put(&location, Bytes::from_static(b"abcdefgh").into())
            .await
            .unwrap();

        // Strip the authentication fields (downgrade attack) while keeping
        // the other v1 fields intact.
        let meta_path = Path::from("meta/stripped");
        let mut meta = read_meta(&inner, &location).await;
        assert!(meta.auth_nonce.is_some() && meta.auth_tag.is_some());
        meta.auth_nonce = None;
        meta.auth_tag = None;
        let mut stripped = Vec::new();
        cbor2::to_writer(&meta, &mut stripped).unwrap();
        inner.put(&meta_path, stripped.into()).await.unwrap();

        // Even the default (non-strict) store must reject it: v1 fields (or
        // a generation pointer) without authentication can only mean
        // stripping or corruption.
        let reopened = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let err = reopened.get(&location).await.unwrap_err();
        assert!(
            err.to_string().contains("stripped metadata authentication"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn strict_mode_rejects_legacy_metadata() {
        let inner = InMemory::new();
        let legacy = Path::from("strict-legacy");
        let sealed = Path::from("strict-sealed");
        let payload = b"legacy encrypted payload";
        put_legacy_encrypted_object(&inner, &legacy, payload, 4).await;

        let strict = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .with_strict_metadata_auth()
            .build();

        // Sealed objects keep working under strict mode.
        strict
            .put(&sealed, Bytes::from_static(b"sealed").into())
            .await
            .unwrap();
        let bytes = strict.get(&sealed).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"sealed"));

        // Legacy metadata is rejected under strict mode...
        let err = strict.get(&legacy).await.unwrap_err();
        assert!(
            err.to_string().contains("strict mode"),
            "unexpected error: {err:?}"
        );

        // ...but remains readable with the default (compatible) settings.
        let lenient = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let bytes = lenient.get(&legacy).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), payload);
    }

    #[tokio::test]
    async fn tampered_metadata_is_rejected_in_all_listing_variants() {
        let inner = InMemory::new();
        let location = Path::from("strict-list/tampered");
        let writer = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32]).build();
        writer
            .put(&location, Bytes::from_static(b"authenticated").into())
            .await
            .unwrap();

        // Keep the CBOR well-formed but alter an authenticated field.
        let meta_path = Path::from("meta/strict-list/tampered");
        let bytes = inner.get(&meta_path).await.unwrap().bytes().await.unwrap();
        let mut meta: Metadata = cbor2::from_reader(&bytes[..]).unwrap();
        meta.size += 1;
        let mut tampered = Vec::new();
        cbor2::to_writer(&meta, &mut tampered).unwrap();
        inner.put(&meta_path, tampered.into()).await.unwrap();

        // Reopen to bypass the writer's valid cached metadata. Compatibility
        // mode accepts genuine legacy documents, not failed authentication.
        let compatible = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32]).build();
        let err = compatible
            .list(Some(&Path::from("strict-list")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("metadata authentication failed"));

        // A failed listing must not cache the attacker-controlled
        // replacement, so all three strict variants independently reach the
        // verifier and reject it.
        let strict = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_strict_metadata_auth()
            .build();
        let err = strict
            .list(Some(&Path::from("strict-list")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("metadata authentication failed"));

        let err = strict
            .list_with_offset(
                Some(&Path::from("strict-list")),
                &Path::from("strict-list/a"),
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("metadata authentication failed"));

        let err = strict
            .list_with_delimiter(Some(&Path::from("strict-list")))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("metadata authentication failed"));
    }

    #[tokio::test]
    async fn corrupted_metadata_heals_on_overwrite() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("self-heal");

        storage
            .put(&location, Bytes::from_static(b"old-data").into())
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

        let reopened = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        assert!(reopened.get(&location).await.is_err());

        reopened
            .put(&location, Bytes::from_static(b"new-data").into())
            .await
            .unwrap();
        let bytes = reopened
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"new-data"));
    }

    #[tokio::test]
    async fn uncommitted_payloads_are_invisible_and_collected() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32]).build();
        let healthy = Path::from("list/healthy");

        storage
            .put(&healthy, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        // A ciphertext generation whose pointer switch never happened (crash
        // window) is invisible to listings and reclaimed by the collector.
        inner
            .put(
                &Path::from("gen/list/orphan/0000000000000001-00000000"),
                Bytes::from_static(b"ghost").into(),
            )
            .await
            .unwrap();

        let reopened = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32]).build();
        let listed: Vec<_> = reopened
            .list(Some(&Path::from("list")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, healthy);
        assert!(listed[0].e_tag.is_some());

        assert_eq!(reopened.collect_garbage().await.unwrap(), 1);
        assert!(matches!(
            inner
                .get(&Path::from("gen/list/orphan/0000000000000001-00000000"))
                .await,
            Err(Error::NotFound { .. })
        ));
        let bytes = reopened.get(&healthy).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abc"));
    }

    #[tokio::test]
    async fn create_succeeds_after_commit_point_loss() {
        let inner = InMemory::new();
        let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("create-heal");

        storage
            .put(&location, Bytes::from_static(b"old-data").into())
            .await
            .unwrap();
        inner.delete(&Path::from("meta/create-heal")).await.unwrap();

        // Without its commit point the object does not logically exist, so
        // `Create` succeeds; the abandoned ciphertext is left to the
        // collector.
        let reopened = EncryptedStoreBuilder::with_secret(inner, 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        reopened
            .put_opts(
                &location,
                Bytes::from_static(b"new-data").into(),
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
        assert_eq!(bytes, Bytes::from_static(b"new-data"));

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
    async fn crash_before_pointer_switch_keeps_old_ciphertext_decryptable() {
        let inner = InMemory::new();
        let (fault, handle) = crate::FaultStore::wrap(inner.clone());
        let storage = EncryptedStoreBuilder::with_secret(fault, 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("crash/encrypted");

        storage
            .put(&location, Bytes::from_static(b"version-1").into())
            .await
            .unwrap();

        // Fail the pointer switch of the overwrite. Under the old mutable
        // layout this crash window produced an AES-GCM authentication
        // failure (old metadata + new ciphertext); now the old version stays
        // fully decryptable.
        handle.push_rule(crate::FaultRule::fail_once(crate::FaultOp::Put, "meta/"));
        assert!(
            storage
                .put(&location, Bytes::from_static(b"version-2").into())
                .await
                .is_err()
        );

        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"version-1"));

        // Same through a fresh instance (cold cache, "after reboot").
        let reopened = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let bytes = reopened
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"version-1"));

        // The collector reclaims the abandoned ciphertext generation (after
        // the same-millisecond in-flight guard has lapsed).
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(reopened.collect_garbage().await.unwrap(), 1);
        let bytes = reopened
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"version-1"));
    }

    #[tokio::test]
    async fn multipart_crash_before_complete_preserves_old_version() {
        let (fault, handle) = crate::FaultStore::wrap(InMemory::new());
        let storage = EncryptedStoreBuilder::with_secret(fault, 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("multipart-crash");

        storage
            .put(&location, Bytes::from_static(b"version-1").into())
            .await
            .unwrap();

        let mut upload = storage.put_multipart(&location).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"multipart-version-2").into())
            .await
            .unwrap();
        handle.push_rule(crate::FaultRule::fail_once(crate::FaultOp::Put, "meta/"));
        assert!(upload.complete().await.is_err());

        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"version-1"));
    }

    #[tokio::test]
    async fn rename_and_copy_to_self_preserve_object() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("self-target");

        storage
            .put(&location, Bytes::from_static(b"abcdefgh").into())
            .await
            .unwrap();

        storage.rename(&location, &location).await.unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abcdefgh"));

        let err = storage
            .rename_if_not_exists(&location, &location)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }));

        storage.copy(&location, &location).await.unwrap();
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"abcdefgh"));

        let missing = Path::from("self-missing");
        let err = storage.rename(&missing, &missing).await.unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[tokio::test]
    async fn head_request_returns_empty_stream() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("head-object");
        let payload = Bytes::from_static(b"abcdefghij");

        storage
            .put(&location, payload.clone().into())
            .await
            .unwrap();

        let res = storage
            .get_opts(
                &location,
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(res.meta.size, payload.len() as u64);
        let bytes = res.bytes().await.unwrap();
        assert!(bytes.is_empty());

        let obj = storage.head(&location).await.unwrap();
        assert_eq!(obj.size, payload.len() as u64);

        // Empty objects behave the same.
        let empty = Path::from("head-empty");
        storage.put(&empty, Bytes::new().into()).await.unwrap();
        let res = storage
            .get_opts(
                &empty,
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(res.meta.size, 0);
        assert!(res.bytes().await.unwrap().is_empty());
        let bytes = storage.get(&empty).await.unwrap().bytes().await.unwrap();
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn concurrent_multipart_completes_leave_readable_object() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
            .with_chunk_size(4)
            .build();
        let location = Path::from("multipart-race");
        let content_a = Bytes::from_static(b"aaaaaaaaaaaaaa");
        let content_b = Bytes::from_static(b"bbbbbbbbbb");

        let mut up_a = storage.put_multipart(&location).await.unwrap();
        let mut up_b = storage.put_multipart(&location).await.unwrap();
        up_a.put_part(content_a.clone().into()).await.unwrap();
        up_b.put_part(content_b.clone().into()).await.unwrap();

        let (ra, rb) = futures::join!(up_a.complete(), up_b.complete());
        ra.unwrap();
        rb.unwrap();

        // Whichever complete committed last, the object must decrypt:
        // ciphertext and metadata are switched atomically at the pointer.
        let bytes = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert!(bytes == content_a || bytes == content_b);
    }

    #[test]
    fn derive_gcm_nonce_counter_wraparound() {
        // Base counter at u64::MAX: idx 1 wraps to 0.
        let base = [0xffu8; 12];
        let n0 = derive_gcm_nonce(&base, 0);
        let n1 = derive_gcm_nonce(&base, 1);
        assert_ne!(n0, n1);
        // The 4-byte salt is preserved.
        assert_eq!(n0[..4], base[..4]);
        assert_eq!(n1[..4], base[..4]);
        assert_eq!(u64::from_le_bytes(n0[4..].try_into().unwrap()), u64::MAX);
        assert_eq!(u64::from_le_bytes(n1[4..].try_into().unwrap()), 0);

        // Nonces stay unique within an object across the wrap boundary.
        let mut seen = std::collections::HashSet::new();
        for idx in 0..1000u64 {
            assert!(seen.insert(derive_gcm_nonce(&base, idx)));
        }
    }

    #[tokio::test]
    async fn test_with_local_file() {
        let root = TempDir::new().unwrap();
        let storage = EncryptedStoreBuilder::with_secret(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            10000,
            [0u8; 32],
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
        let storage = EncryptedStoreBuilder::with_secret(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            10000,
            [0u8; 32],
        )
        .build();
        stream_get(&storage).await;
    }

    /// Regression stress test for OCC lost updates over short payloads.
    ///
    /// One-byte counter values make bare-ciphertext ETags collide with
    /// probability 1/256 per pair; a collision lets a stale CAS token pass
    /// the precondition and silently rewind the counter. The logical ETag
    /// is therefore seeded with the per-commit nonce (see `put_opts`).
    #[tokio::test(flavor = "multi_thread")]
    async fn stress_occ_counter_local_file() {
        const NUM_WORKERS: usize = 16;
        const NUM_INCREMENTS: usize = 25;

        let root = TempDir::new().unwrap();
        let storage = std::sync::Arc::new(
            EncryptedStoreBuilder::with_secret(
                LocalFileSystem::new_with_prefix(root.path()).unwrap(),
                10000,
                [7u8; 32],
            )
            .build(),
        );
        let path = Path::from("RACE");
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..NUM_WORKERS {
            let storage = storage.clone();
            let path = path.clone();
            tasks.spawn(async move {
                for _ in 0..NUM_INCREMENTS {
                    loop {
                        match storage.get(&path).await {
                            Ok(r) => {
                                let mode = PutMode::Update(UpdateVersion {
                                    e_tag: r.meta.e_tag.clone(),
                                    version: r.meta.version.clone(),
                                });
                                let b = r.bytes().await.unwrap();
                                let v: usize = std::str::from_utf8(&b).unwrap().parse().unwrap();
                                let new = (v + 1).to_string();
                                match storage.put_opts(&path, new.into(), mode.into()).await {
                                    Ok(_) => break,
                                    Err(object_store::Error::Precondition { .. }) => continue,
                                    Err(e) => panic!("unexpected error: {e:?}"),
                                }
                            }
                            Err(object_store::Error::NotFound { .. }) => {
                                match storage
                                    .put_opts(&path, "1".into(), PutMode::Create.into())
                                    .await
                                {
                                    Ok(_) => break,
                                    Err(object_store::Error::AlreadyExists { .. }) => continue,
                                    Err(e) => panic!("unexpected error: {e:?}"),
                                }
                            }
                            Err(e) => panic!("unexpected error: {e:?}"),
                        }
                    }
                }
            });
        }
        while let Some(rt) = tasks.join_next().await {
            rt.unwrap();
        }

        let b = storage.get(&path).await.unwrap().bytes().await.unwrap();
        let v = std::str::from_utf8(&b).unwrap().parse::<usize>().unwrap();
        assert_eq!(v, NUM_WORKERS * NUM_INCREMENTS, "lost updates");
    }
}
