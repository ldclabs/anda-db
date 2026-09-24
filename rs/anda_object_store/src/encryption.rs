use aes_gcm::{AeadInOut, Aes256Gcm, Key, Nonce, Tag};
use async_stream::try_stream;
use async_trait::async_trait;
#[cfg(test)]
use base64::{Engine, prelude::BASE64_URL_SAFE};
use bytes::{Buf, Bytes, BytesMut};
use futures::{StreamExt, stream::BoxStream};
use moka::future::Cache;
use object_store::{path::Path, *};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteArray;
use std::{
    ops::Range,
    sync::{Arc, OnceLock, Weak},
    time::Duration,
};

use crate::{
    check_get_preconditions, check_update_version,
    generation::commit_e_tag,
    limits::{
        DEFAULT_CACHE_BYTES, GarbageCollectionOptions, MetadataLimits, limit_error, metadata_cache,
    },
    sidecar::{
        PublicationBaseline, SidecarMeta, SidecarStore, head_result, logical_last_modified,
        logical_object_meta, new_commit_timestamp_ms,
    },
    upload::{CiphertextParts, DEFAULT_PART_SIZE, Lifecycle, Phase},
    validate_ranges,
};

mod ranges;
use ranges::{aligned_range, decrypt_chunk, decrypt_span, retain};

const DEFAULT_CHUNK_SIZE: u64 = 256 * 1024;
const CHUNK_AAD_LEGACY: u8 = 0;
const CHUNK_AAD_BOUND: u8 = 1;
/// Default time-to-live of the metadata cache.
const DEFAULT_META_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
/// Default time-to-idle of the metadata cache.
const DEFAULT_META_CACHE_TTI: Duration = Duration::from_secs(20 * 60);

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
pub struct EncryptedStore<T: ObjectStore> {
    /// Shared sidecar core: underlying store, path prefixes, metadata cache.
    inner: Arc<SidecarStore<T, Metadata>>,
    /// Key, chunking and metadata-authentication policy.
    crypto: Arc<Crypto>,
    /// Physical multipart part size.
    part_size: usize,
}

impl<T: ObjectStore> Clone for EncryptedStore<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            crypto: self.crypto.clone(),
            part_size: self.part_size,
        }
    }
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
    /// Optional custom cache. The built-in cache is constructed at build time.
    meta_cache: Option<Cache<Path, Arc<Metadata>>>,
    /// Maximum number of metadata entries the built-in cache holds. Retained
    /// so selecting the built-in cache after a custom one retains capacity.
    meta_cache_capacity: u64,
    meta_cache_ttl: Duration,
    meta_cache_bytes: u64,
    limits: MetadataLimits,
    part_size: usize,
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
    /// In-memory certificate; never deserialized or copied to a changed value.
    #[serde(skip)]
    validation: ValidationCertificate,
    /// Size of the ciphertext in bytes (also the plaintext size, since
    /// AES-256-GCM in this implementation is length-preserving and the
    /// authentication tags are stored out-of-band in [`Metadata::aes_tags`]).
    #[serde(rename = "s")]
    size: u64,

    /// Opaque commit token. New writes derive it from the generation; old
    /// nonce/ciphertext-derived tokens remain valid opaque strings. The token
    /// participates in metadata authentication, not payload checksum validation.
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

    /// Logical commit timestamp in milliseconds since the Unix epoch. Bound
    /// into the authenticated metadata for new writes.
    #[serde(rename = "m", default, skip_serializing_if = "Option::is_none")]
    committed_at_ms: Option<u64>,
}

#[derive(Default)]
struct ValidationCertificate(OnceLock<ValidatedFor>);
struct ValidatedFor {
    context: Weak<Crypto>,
    path: Path,
}
impl Clone for ValidationCertificate {
    fn clone(&self) -> Self {
        Self::default()
    }
}
impl std::fmt::Debug for ValidationCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ValidationCertificate")
            .field(&self.0.get().is_some())
            .finish()
    }
}

/// Key, chunking and metadata-authentication policy of one store.
///
/// Documents it validated or sealed carry a certificate bound to this
/// context and their path, so cache hits skip re-authentication while a
/// custom cache shared with another key or path is still re-verified.
struct Crypto {
    cipher: Arc<Aes256Gcm>,
    /// Reject legacy sidecar metadata that carries no authentication fields
    /// instead of accepting it with a warning.
    strict: bool,
    /// Plaintext chunk size for new writes. Each chunk is encrypted
    /// independently with its own derived nonce and authentication tag.
    chunk_size: u64,
    limits: MetadataLimits,
    #[cfg(test)]
    authentications: std::sync::atomic::AtomicUsize,
}
impl Crypto {
    fn chunks(&self, size: u64, chunk_size: u64) -> Result<usize> {
        self.limits.check_size(size, "EncryptedStore")?;
        if chunk_size == 0 || chunk_size > usize::MAX as u64 {
            return Err(limit_error(
                "EncryptedStore",
                "invalid encryption chunk size",
            ));
        }
        let count = usize::try_from(size.div_ceil(chunk_size))
            .map_err(|_| limit_error("EncryptedStore", "encryption chunk count overflow"))?;
        if count > self.limits.max_chunks || count > self.limits.max_metadata_bytes / 17 {
            return Err(limit_error(
                "EncryptedStore",
                "encryption metadata chunk limit exceeded",
            ));
        }
        Ok(count)
    }

    /// Chunk size an object was written with, preferring the size recorded
    /// in its metadata over the store's current configuration. Validation
    /// rejects zero and oversized values.
    fn read_chunk_size(&self, meta: &Metadata) -> u64 {
        meta.chunk_size.unwrap_or(self.chunk_size)
    }

    fn check_tags(&self, meta: &Metadata) -> Result<()> {
        if meta.aes_tags.len() != self.chunks(meta.size, self.read_chunk_size(meta))? {
            return Err(limit_error(
                "EncryptedStore",
                "encryption tag count does not match object size",
            ));
        }
        Ok(())
    }

    fn certify(self: &Arc<Self>, path: &Path, meta: &Metadata) {
        let _ = meta.validation.0.set(ValidatedFor {
            context: Arc::downgrade(self),
            path: path.clone(),
        });
    }

    fn validate(self: &Arc<Self>, path: &Path, meta: &Metadata) -> Result<()> {
        if let Some(cert) = meta.validation.0.get()
            && cert.path == *path
            && cert
                .context
                .upgrade()
                .is_some_and(|context| Arc::ptr_eq(&context, self))
        {
            return Ok(());
        }
        // Encoded size is bounded when a document is fetched or written.
        if meta.aes_tags.len() > self.limits.max_chunks {
            return Err(limit_error(
                "EncryptedStore",
                "encryption metadata limit exceeded",
            ));
        }
        // Authenticate before interpreting fields or caching a certificate.
        #[cfg(test)]
        self.authentications
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        verify_metadata(&self.cipher, path, meta, self.strict)?;
        self.check_tags(meta)?;
        self.certify(path, meta);
        Ok(())
    }

    /// Seals `meta` for `path`. The document is certified directly, so its
    /// commit does not authenticate it a second time.
    fn seal(self: &Arc<Self>, path: &Path, meta: &mut Metadata) -> Result<()> {
        self.check_tags(meta)?;
        seal_metadata(&self.cipher, path, meta)?;
        self.certify(path, meta);
        Ok(())
    }
}

impl Metadata {
    fn cache_weight(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.aes_tags.capacity().saturating_mul(16)
            + self
                .validation
                .0
                .get()
                .map_or(0, |cert| cert.path.as_ref().len())
            + [
                &self.e_tag,
                &self.original_tag,
                &self.original_version,
                &self.generation,
            ]
            .iter()
            .map(|s| s.as_ref().map_or(0, String::capacity))
            .sum::<usize>()
    }
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

    fn committed_at_ms(&self) -> Option<u64> {
        self.committed_at_ms
    }
}

impl Metadata {
    /// An unsealed commit of `generation` for freshly encrypted chunks.
    fn commit(
        size: u64,
        aes_nonce: [u8; 12],
        aes_tags: Vec<ByteArray<16>>,
        chunk_size: u64,
        generation: String,
    ) -> Self {
        Self {
            validation: ValidationCertificate::default(),
            size,
            e_tag: Some(commit_e_tag(&generation)),
            original_tag: None,
            original_version: None,
            aes_nonce: aes_nonce.into(),
            aes_tags,
            chunk_size: Some(chunk_size),
            chunk_aad_version: Some(CHUNK_AAD_BOUND),
            auth_nonce: None,
            auth_tag: None,
            generation: Some(generation),
            committed_at_ms: None,
        }
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
            meta_cache: None,
            meta_cache_capacity,
            meta_cache_ttl: DEFAULT_META_CACHE_TTL,
            meta_cache_bytes: DEFAULT_CACHE_BYTES,
            limits: MetadataLimits::default(),
            part_size: DEFAULT_PART_SIZE,
        }
    }

    /// Sets the cache for metadata.
    ///
    /// This cache is used to store metadata for objects, improving performance.
    ///
    /// The supplied cache replaces the built-in one wholesale, so its own
    /// capacity and eviction policy apply instead of the capacity passed to
    /// [`EncryptedStoreBuilder::new`]. A later
    /// [`EncryptedStoreBuilder::with_meta_cache_ttl`] selects the built-in
    /// cache and discards the supplied one.
    ///
    /// # Parameters
    /// - `cache`: The cache to use for metadata
    ///
    /// # Returns
    /// The builder with the updated metadata cache
    pub fn with_meta_cache(self, cache: Cache<Path, Arc<Metadata>>) -> Self {
        Self {
            meta_cache: Some(cache),
            ..self
        }
    }

    /// Sets the time-to-live (TTL) for the metadata cache.
    ///
    /// The built-in cache will use the capacity passed to
    /// [`EncryptedStoreBuilder::new`] and the default time-to-idle.
    pub fn with_meta_cache_ttl(mut self, ttl: Duration) -> Self {
        self.meta_cache_ttl = ttl;
        self.meta_cache = None;
        self
    }

    /// Selects the built-in cache with an estimated byte budget and the original
    /// entry limit. Like with_meta_cache_ttl, replaces a supplied custom cache.
    pub fn with_meta_cache_bytes(mut self, bytes: u64) -> Self {
        self.meta_cache_bytes = bytes;
        self.meta_cache = None;
        self
    }

    pub fn with_metadata_limits(mut self, limits: MetadataLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Fixed physical multipart size, independent of encryption chunk size.
    /// Values below 5 MiB are normalized to 5 MiB; the final part may be shorter.
    pub fn with_multipart_part_size(mut self, bytes: usize) -> Self {
        self.part_size = bytes.max(5 * 1024 * 1024);
        self
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

    /// Retained for API compatibility: conditional-put semantics (the logical
    /// ETag, `PutMode::Update` and `if_match`/`if_none_match` preconditions)
    /// are now always enabled on every backend, because the
    /// immutable-generation protocol evaluates them against the metadata
    /// commit point instead of forwarding them.
    ///
    /// # Returns
    /// The builder, unchanged
    #[deprecated(
        since = "0.10.0",
        note = "no-op: conditional-put semantics are unconditional since the immutable-generation refactor; remove the call"
    )]
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
        let cache = self.meta_cache.unwrap_or_else(|| {
            build_meta_cache(
                self.meta_cache_capacity,
                self.meta_cache_ttl,
                self.meta_cache_bytes,
            )
        });
        let crypto = Arc::new(Crypto {
            cipher: self.cipher,
            strict: self.strict_metadata_auth,
            chunk_size: self.chunk_size,
            limits: self.limits,
            #[cfg(test)]
            authentications: std::sync::atomic::AtomicUsize::new(0),
        });
        let validator = crypto.clone();
        EncryptedStore {
            inner: Arc::new(
                SidecarStore::new(self.store, cache)
                    .with_limits(self.limits)
                    .with_strict_listing(self.strict_metadata_auth)
                    .with_validator(move |path, meta| validator.validate(path, meta)),
            ),
            crypto,
            part_size: self.part_size,
        }
    }
}

impl<T: ObjectStore> EncryptedStore<T> {
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

    pub async fn collect_garbage_with_options(
        &self,
        options: GarbageCollectionOptions,
    ) -> Result<usize> {
        self.inner.collect_garbage_with_options(options).await
    }

    /// Serves one `get_opts` attempt against a resolved commit point.
    async fn read_object(
        &self,
        location: &Path,
        meta: Arc<Metadata>,
        mut options: GetOptions,
    ) -> Result<GetResult> {
        let last_modified = logical_last_modified(meta.committed_at_ms, meta.generation.as_deref());
        check_get_preconditions(location, &mut options, meta.e_tag.as_deref(), last_modified)?;
        if let Some(last_modified) = last_modified
            && options.head
            && options.range.is_none()
        {
            return Ok(head_result(logical_object_meta(
                location,
                &*meta,
                last_modified,
            )));
        }

        // Resolve the caller-supplied (plaintext) range, defaulting to the
        // full object when no range is specified.
        let range = match &options.range {
            Some(r) => r
                .as_range(meta.size)
                .map_err(|source| object_store::Error::Generic {
                    store: "EncryptedStore",
                    source: source.into(),
                })?,
            None => 0..meta.size,
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
        let chunk_size = self.crypto.read_chunk_size(&meta);
        let aligned = aligned_range(&range, chunk_size, meta.size);
        if aligned.is_empty() {
            options.range = None;
            options.head = true;
        } else {
            options.range = Some(GetRange::Bounded(aligned.clone()));
        }

        let payload_path = self
            .inner
            .payload_path(location, meta.generation.as_deref());
        let mut res = self.inner.store.get_opts(&payload_path, options).await?;
        let attributes = std::mem::take(&mut res.attributes);
        let extensions = std::mem::take(&mut res.extensions);
        let object = logical_object_meta(location, &*meta, res.meta.last_modified);
        let stream = create_decryption_stream(
            res,
            self.crypto.cipher.clone(),
            meta,
            location.clone(),
            chunk_size as usize,
            (aligned.start / chunk_size) as usize,
            (range.start - aligned.start) as usize,
            range.end - range.start,
        );
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta: object,
            range,
            attributes,
            extensions,
        })
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
        let chunk_size = self.crypto.chunk_size;
        self.crypto
            .chunks(payload.content_length() as u64, chunk_size)?;
        let create = matches!(opts.mode, PutMode::Create);
        let extensions = opts.extensions.clone();
        let mut _in_flight = None;
        let in_flight_out = &mut _in_flight;
        let rt = self
            .inner
            .update_meta_with(location, create, extensions, async |meta| {
                if let PutMode::Update(v) = &opts.mode {
                    let current = meta.ok_or_else(|| Error::Precondition {
                        path: location.to_string(),
                        source: "metadata not found".into(),
                    })?;
                    check_update_version(location, &current.e_tag, v)?;
                }

                // Gather the payload into a single mutable buffer for
                // in-place chunked encryption (exactly one copy, even for
                // multi-segment payloads).
                let mut data = Vec::with_capacity(payload.content_length());
                for segment in payload.iter() {
                    data.extend_from_slice(segment);
                }
                let size = data.len() as u64;
                let base_nonce: [u8; 12] = rand_bytes();
                let mut aes_tags = Vec::with_capacity(size.div_ceil(chunk_size) as usize);
                encrypt_chunks(
                    &self.crypto.cipher,
                    &base_nonce,
                    chunk_size,
                    &mut 0,
                    &mut aes_tags,
                    &mut data,
                    location,
                )?;

                let (generation, in_flight) = self
                    .inner
                    .put_new_generation(location, data.into(), opts)
                    .await?;
                *in_flight_out = Some(in_flight);

                let mut meta = Metadata::commit(size, base_nonce, aes_tags, chunk_size, generation);
                meta.committed_at_ms = Some(new_commit_timestamp_ms());
                self.crypto.seal(location, &mut meta)?;
                Ok(meta)
            })
            .await?;

        Ok(PutResult {
            e_tag: rt.e_tag.clone(),
            version: None,
            extensions: rt.extensions,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let extensions = opts.extensions.clone();
        let (generation, flight) = self.inner.allocate_generation(location);
        let inner = self
            .inner
            .store
            .put_multipart_opts(&self.inner.generation_path(location, &generation), opts)
            .await?;
        Ok(Box::new(EncryptedStoreUploader {
            buffer: EncryptionBuffer::new(),
            transport: CiphertextParts::default(),
            size: 0,
            location: location.clone(),
            generation,
            lifecycle: Lifecycle::new(flight, "EncryptedStore"),
            prepared: None,
            publication_baseline: None,
            store: self.inner.clone(),
            crypto: self.crypto.clone(),
            part_size: self.part_size,
            extensions,
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let extensions = options.extensions.clone();
        self.inner
            .with_payload(location, extensions, |meta| {
                self.read_object(location, meta, options.clone())
            })
            .await
    }

    async fn get_ranges(&self, location: &Path, requested: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        self.inner
            .with_payload(location, Extensions::default(), |meta| async move {
                validate_ranges("EncryptedStore", requested, meta.size)?;
                ranges::read_ranges(
                    &self.inner.store,
                    &self
                        .inner
                        .payload_path(location, meta.generation.as_deref()),
                    location,
                    &self.crypto.cipher,
                    &meta,
                    self.crypto.read_chunk_size(&meta),
                    requested,
                )
                .await
            })
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.clone().delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.clone().list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.clone().list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let CopyOptions { mode, extensions } = options;
        let create = matches!(mode, CopyMode::Create);
        // The ciphertext chunks are not bound to the path (their AAD carries
        // chunk size and index only), so the payload is copied verbatim;
        // only the metadata document is resealed below for the target path.
        // The pointer switch is the commit point; `_in_flight` shields the
        // copied generation from garbage collection until then.
        let (src, generation, _in_flight) = self
            .inner
            .copy_payload(from, to, create, extensions.clone())
            .await?;

        let mut meta = (*src).clone();
        // A copy is a commit of its own, so it gets its own CAS token
        // instead of the source's.
        meta.e_tag = Some(commit_e_tag(&generation));
        meta.chunk_size = Some(self.crypto.read_chunk_size(&src));
        meta.generation = Some(generation);
        meta.original_tag = None;
        meta.original_version = None;
        // Pin the chunk-AAD version explicitly so legacy ciphertext stays
        // readable under the resealed (authenticated) target document.
        ensure_chunk_aad_version(&mut meta)?;
        self.inner
            .update_meta_with(to, create, extensions, async |_| {
                meta.committed_at_ms = Some(new_commit_timestamp_ms());
                self.crypto.seal(to, &mut meta)?;
                Ok(meta)
            })
            .await?;
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename(self, from, to, options).await
    }
}

/// Streaming encryption with independent, fixed-size physical upload parts.
/// All non-final backend parts have part_size bytes, regardless of caller or
/// encryption chunk boundaries. Failed/cancelled parts require a new upload;
/// metadata publication may be retried after the payload has completed.
pub struct EncryptedStoreUploader<T: ObjectStore> {
    buffer: EncryptionBuffer,
    transport: CiphertextParts,
    size: u64,
    location: Path,
    generation: String,
    lifecycle: Lifecycle,
    prepared: Option<Metadata>,
    publication_baseline: PublicationBaseline,
    extensions: Extensions,
    store: Arc<SidecarStore<T, Metadata>>,
    crypto: Arc<Crypto>,
    part_size: usize,
    inner: Box<dyn MultipartUpload>,
}
impl<T: ObjectStore> std::fmt::Debug for EncryptedStoreUploader<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptedStoreUploader({})", self.location)
    }
}
/// Buffers one plaintext tail and records chunk tags. Keeping this separate
/// lets regular parts and finalization share encryption while the lifecycle
/// drop guard remains borrowed throughout finalization.
struct EncryptionBuffer {
    plaintext: Vec<u8>,
    tags: Vec<ByteArray<16>>,
    nonce: [u8; 12],
    index: u64,
}

impl EncryptionBuffer {
    fn new() -> Self {
        Self {
            plaintext: Vec::new(),
            tags: Vec::new(),
            nonce: rand_bytes(),
            index: 0,
        }
    }

    fn encrypt(&mut self, crypto: &Crypto, tail: bool, location: &Path) -> Result<Option<Bytes>> {
        let chunk_size = crypto.chunk_size;
        let split = if tail {
            self.plaintext.len()
        } else {
            self.plaintext.len() / chunk_size as usize * chunk_size as usize
        };
        if split == 0 {
            return Ok(None);
        }
        let mut data = std::mem::take(&mut self.plaintext);
        self.plaintext = data.split_off(split);
        encrypt_chunks(
            &crypto.cipher,
            &self.nonce,
            chunk_size,
            &mut self.index,
            &mut self.tags,
            &mut data,
            location,
        )?;
        Ok(Some(data.into()))
    }
}
#[async_trait]
impl<T: ObjectStore> MultipartUpload for EncryptedStoreUploader<T> {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        if let Err(err) = self.lifecycle.receiving() {
            return Box::pin(async { Err(err) });
        }
        let checked = self
            .size
            .checked_add(payload.content_length() as u64)
            .ok_or_else(|| limit_error("EncryptedStore", "object size overflow"))
            .and_then(|size| {
                self.crypto.chunks(size, self.crypto.chunk_size)?;
                Ok(size)
            });
        let size = match checked {
            Ok(size) => size,
            Err(err) => {
                self.lifecycle.fail();
                return Box::pin(async { Err(err) });
            }
        };
        self.size = size;
        for segment in payload.iter() {
            self.buffer.plaintext.extend_from_slice(segment);
        }
        match self.buffer.encrypt(&self.crypto, false, &self.location) {
            Ok(Some(data)) => self.transport.push(data),
            Ok(None) => {}
            Err(err) => {
                self.lifecycle.fail();
                return Box::pin(async { Err(err) });
            }
        }
        let mut parts = Vec::new();
        while self.transport.len() >= self.part_size {
            parts.push(self.inner.put_part(self.transport.take(self.part_size)));
        }
        if !parts.is_empty() {
            self.transport.compact_tail();
        }
        self.lifecycle.track(async move {
            use futures::TryStreamExt;
            futures::stream::iter(parts)
                .buffer_unordered(8)
                .try_collect::<Vec<_>>()
                .await?;
            Ok(())
        })
    }
    async fn complete(&mut self) -> Result<PutResult> {
        if let Some(result) = &self.lifecycle.result {
            return Ok(result.clone());
        }
        if self.lifecycle.phase == Phase::Receiving {
            // Finalizing's drop guard poisons the upload on error or cancellation.
            // Split field borrows so no mutation can escape that guard.
            let attempt = self.lifecycle.finalizing()?;
            if let Some(data) = self.buffer.encrypt(&self.crypto, true, &self.location)? {
                self.transport.push(data);
            }
            while self.transport.len() != 0 {
                let size = self.transport.len().min(self.part_size);
                self.inner.put_part(self.transport.take(size)).await?;
            }
            self.inner.complete().await?;
            self.prepared = Some(Metadata::commit(
                self.size,
                self.buffer.nonce,
                std::mem::take(&mut self.buffer.tags),
                self.crypto.chunk_size,
                self.generation.clone(),
            ));
            attempt.materialized();
        }
        self.lifecycle.ready()?;
        let meta = self
            .prepared
            .as_ref()
            .expect("materialized upload has metadata")
            .clone();
        let result = self
            .store
            .publish_upload(
                &self.location,
                meta,
                &mut self.publication_baseline,
                self.extensions.clone(),
                |meta| {
                    meta.committed_at_ms = Some(new_commit_timestamp_ms());
                    self.crypto.seal(&self.location, meta)
                },
            )
            .await?;
        self.prepared = None;
        Ok(self.lifecycle.committed(PutResult {
            e_tag: result.e_tag.clone(),
            version: None,
            extensions: result.extensions,
        }))
    }
    async fn abort(&mut self) -> Result<()> {
        crate::upload::abort_upload(
            &mut self.lifecycle,
            self.store.as_ref(),
            self.inner.as_mut(),
            &self.location,
            &self.generation,
            self.extensions.clone(),
        )
        .await?;
        self.buffer.plaintext = Vec::new();
        self.buffer.tags = Vec::new();
        self.transport = CiphertextParts::default();
        self.prepared = None;
        Ok(())
    }
}

fn encrypt_chunks(
    cipher: &Aes256Gcm,
    base: &[u8; 12],
    chunk_size: u64,
    index: &mut u64,
    tags: &mut Vec<ByteArray<16>>,
    data: &mut [u8],
    path: &Path,
) -> Result<()> {
    for chunk in data.chunks_mut(chunk_size as usize) {
        let nonce = derive_gcm_nonce(base, *index);
        let tag: [u8; 16] = cipher
            .encrypt_inout_detached(
                &Nonce::from(nonce),
                &chunk_aad(chunk_size, *index),
                chunk.into(),
            )
            .map_err(|err| Error::Generic {
                store: "EncryptedStore",
                source: format!("AES256 encrypt failed for path {path}: {err:?}").into(),
            })?
            .into();
        *index = index
            .checked_add(1)
            .ok_or_else(|| limit_error("EncryptedStore", "encryption chunk counter exhausted"))?;
        tags.push(tag.into());
    }
    Ok(())
}

/// Ciphertext spans up to this size are read with a single body read and
/// decrypted in place. File backends otherwise deliver small blocks, each
/// read on the blocking pool.
const WHOLE_READ_BYTES: u64 = 1024 * 1024;

/// Builds a [`BoxStream`] of plaintext bytes from the underlying ciphertext
/// stream returned by `inner.store.get_opts(...)`.
///
/// Small spans are read whole. Larger ones are re-buffered into chunk-sized
/// blocks, and each block is decrypted in place using the supplied per-chunk
/// authentication tag. The leading and trailing bytes are trimmed so the
/// consumer only sees the caller's requested plaintext range:
///
/// - `start_idx` — index of the first chunk that intersects the request.
/// - `start_offset` — byte offset within the first chunk to begin yielding.
/// - `size` — total number of plaintext bytes to yield before completing.
///
/// The function expects the upstream stream to deliver every requested
/// ciphertext chunk in full. The final batch is bounded by the authenticated
/// object size, including a short last chunk; truncated batches are rejected.
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
        if size == 0 { return; }
        let available = meta.size - start_idx as u64 * chunk_size as u64;
        let mut ciphertext_remaining = (size + start_offset as u64).div_ceil(chunk_size as u64)
            .saturating_mul(chunk_size as u64).min(available);
        if ciphertext_remaining <= WHOLE_READ_BYTES {
            let mut data = res.bytes().await?;
            if (data.len() as u64) < ciphertext_remaining {
                Err(limit_error("EncryptedStore", "truncated encrypted data"))?;
            }
            data.truncate(ciphertext_remaining as usize);
            let plaintext = decrypt_span(
                &cipher, &meta, chunk_size as u64, start_idx as u64, data, &location,
            )?;
            yield retain(&plaintext, start_offset..start_offset + size as usize);
            return;
        }

        let mut stream = res.into_stream();
        let mut buf = BytesMut::new();
        let mut index = start_idx as u64;
        let mut skip = start_offset;
        let mut remaining = size;
        // Batch small crypto chunks to avoid allocating once per tiny chunk,
        // but never copy an unbounded upstream buffer into plaintext storage.
        let batch = (64 * 1024 / chunk_size).max(1) * chunk_size;
        while let Some(data) = stream.next().await {
            let mut data = data?;
            while !data.is_empty() {
                let target = (ciphertext_remaining.min(batch as u64)) as usize;
                if buf.is_empty() { buf.reserve(target); }
                let take = (target - buf.len()).min(data.len());
                buf.extend_from_slice(&data[..take]);
                data.advance(take);
                if buf.len() != target { continue; }
                let mut chunk = std::mem::take(&mut buf);
                for bytes in chunk.chunks_mut(chunk_size) {
                    decrypt_chunk(&cipher, &meta, chunk_size as u64, index, bytes, &location)?;
                    index += 1;
                }
                ciphertext_remaining -= chunk.len() as u64;
                let end = (skip as u64 + remaining).min(chunk.len() as u64) as usize;
                remaining -= (end - skip) as u64;
                yield retain(&chunk.freeze(), skip..end);
                skip = 0;
                if remaining == 0 { return; }
            }
        }
        // No partial batch is exposed: its required ciphertext never arrived.
        if remaining != 0 { Err(limit_error("EncryptedStore", "truncated encrypted data"))?; }
    }
    .boxed()
}

fn normalize_chunk_size(chunk_size: u64) -> u64 {
    chunk_size.clamp(1, usize::MAX as u64)
}

/// Builds the built-in metadata cache with the configured capacity and TTL.
fn build_meta_cache(capacity: u64, ttl: Duration, bytes: u64) -> Cache<Path, Arc<Metadata>> {
    metadata_cache(
        capacity,
        bytes,
        ttl,
        Some(DEFAULT_META_CACHE_TTI),
        Metadata::cache_weight,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataAuth {
    Authenticated,
    Legacy,
}

fn seal_metadata(cipher: &Aes256Gcm, location: &Path, meta: &mut Metadata) -> Result<()> {
    meta.validation = ValidationCertificate::default();
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
    let mut aad = Vec::with_capacity(
        meta.aes_tags
            .len()
            .saturating_mul(24)
            .saturating_add(location.as_ref().len())
            .saturating_add(256),
    );
    aad.extend_from_slice(b"anda_object_store.encrypted.metadata.v1");
    push_bytes(&mut aad, location.as_ref().as_bytes());
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
    if let Some(committed_at_ms) = meta.committed_at_ms {
        aad.extend_from_slice(b".m");
        aad.extend_from_slice(&committed_at_ms.to_le_bytes());
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

fn chunk_aad_for_meta(
    meta: &Metadata,
    chunk_size: u64,
    chunk_index: u64,
) -> Result<Option<[u8; 52]>> {
    Ok(match chunk_aad_version(meta)? {
        CHUNK_AAD_LEGACY => None,
        _ => Some(chunk_aad(chunk_size, chunk_index)),
    })
}

fn chunk_aad(chunk_size: u64, chunk_index: u64) -> [u8; 52] {
    let mut aad = [0u8; 52];
    aad[..36].copy_from_slice(b"anda_object_store.encrypted.chunk.v1");
    aad[36..44].copy_from_slice(&chunk_size.to_le_bytes());
    aad[44..].copy_from_slice(&chunk_index.to_le_bytes());
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
/// objects have probabilistically separated counter ranges. Within one object,
/// distinct indices produce distinct nonces; random cross-object collisions are
/// not impossible and key-use limits must account for all AEAD invocations.
fn derive_gcm_nonce(base: &[u8; 12], idx: u64) -> [u8; 12] {
    let mut nonce = *base;
    let mut ctr = [0u8; 8];
    ctr.copy_from_slice(&nonce[4..12]);
    let c = u64::from_le_bytes(ctr).wrapping_add(idx);
    nonce[4..12].copy_from_slice(&c.to_le_bytes());
    nonce
}

#[cfg(test)]
mod tests;
