use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use bytes::Bytes;
use ciborium::{from_reader, into_writer};
use futures::{
    StreamExt, TryStreamExt,
    future::{BoxFuture, FutureExt},
    stream::BoxStream,
};
use moka::future::Cache;
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, PutOptions, PutResult, UpdateVersion,
    buffered::{BufReader, BufWriter},
    path::Path,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

pub use object_store::PutMode;

use crate::error::DBError;

/// Anda DB storage layer implementation based on `object_store`.
#[derive(Clone)]
pub struct Storage {
    inner: Arc<InnerStorage>,
}

/// Inner representation of the storage layer.
struct InnerStorage {
    /// Object store reference.
    object_store: Arc<dyn ObjectStore>,
    /// Base path for all documents within the object store.
    base_path: Path,
    // /// Zstd compression level (0 for no compression, 1-22).
    // compress_level: i32,
    // /// Chunk size for buffered reading/writing, streaming operations and encryption.
    // /// Cannot changed after initialization.
    // object_chunk_size: usize,
    // /// Maximum object size (in bytes) that can be stored using a single `put` operation.
    // /// Objects larger than this size must be written using `stream_writer`.
    // max_small_object_size: usize,
    /// Atomic storage statistics.
    stats: StorageStatsAtomic,
    /// Storage metadata (configuration and non-atomic stats).
    metadata: StorageMetadata,
    /// Optional cache for frequently accessed small objects.
    cache: Option<Cache<Path, Arc<(Bytes, ObjectVersion)>>>,
}

/// Configuration for the object store storage layer.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    /// Maximum number of items in the cache. 0 disables the cache.
    pub cache_max_capacity: u64,
    /// Zstd compression level. 0 disables compression, 1-22 represent compression levels.
    /// Default is 3. 22 indicates the highest compression ratio.
    pub compress_level: i32,
    /// Chunk size for buffered reading/writing, streaming operations and encryption.
    /// Cannot changed after initialization.
    pub object_chunk_size: usize,
    /// Maximum size (in bytes) for objects considered "small" enough for single `put` operations and caching.
    pub max_small_object_size: usize,
    /// Maximum size of a index bucket before creating a new one
    /// When a bucket's stored data exceeds this size,
    /// a new bucket should be created for new data
    pub bucket_overload_size: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            cache_max_capacity: 10000,          // Default cache capacity
            compress_level: 3,                  // Default compression level
            object_chunk_size: 256 * 1024,      // Default chunk size (256 KiB)
            max_small_object_size: 2000 * 1024, // Default max small object size (2 MiB)
            bucket_overload_size: 1024 * 1024,  // Default bucket overload size (1 MiB)
        }
    }
}

/// Metadata associated with the storage instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetadata {
    /// The base path used for this storage instance.
    pub path: String,
    /// Storage configuration.
    pub config: StorageConfig,
    /// Storage statistics.
    pub stats: StorageStats,
}

/// Atomic version of storage statistics for concurrent updates.
struct StorageStatsAtomic {
    check_point: AtomicU64,
    /// Internal version counter, incremented on metadata save.
    version: AtomicU64,
    /// Timestamp (ms) of the last metadata save.
    last_saved: AtomicU64,
    /// Total number of `get` operations (cache hits + fetches).
    total_cache_get_count: AtomicU64,
    /// Total number of `fetch` operations (actual object store reads).
    total_fetch_count: AtomicU64,
    /// Total bytes fetched from the object store.
    total_fetch_bytes: AtomicU64,
    /// Total number of `put` operations.
    total_put_count: AtomicU64,
    /// Total bytes submitted for writing (pre-compression size).
    total_put_bytes: AtomicU64,
    /// Total number of `delete` operations.
    total_delete_count: AtomicU64,
}

/// Storage statistics.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct StorageStats {
    pub check_point: u64,
    /// Internal version counter.
    pub version: u64,
    /// Timestamp (ms) of the last metadata save.
    pub last_saved: u64,
    /// Total number of `get` from cache operations.
    pub total_cache_get_count: u64,
    /// Total number of `fetch` operations (actual object store reads).
    pub total_fetch_count: u64,
    /// Total bytes fetched from the object store.
    pub total_fetch_bytes: u64,
    /// Total number of `put` operations.
    pub total_put_count: u64,
    /// Total bytes submitted for writing (pre-compression size).
    pub total_put_bytes: u64,
    /// Total number of `delete` operations.
    pub total_delete_count: u64,
}

impl StorageStats {
    /// Accumulates counters from `other` into `self`.
    ///
    /// Summable counters (counts, bytes) are added.
    pub fn merge(&mut self, other: &StorageStats) {
        self.total_cache_get_count = self
            .total_cache_get_count
            .saturating_add(other.total_cache_get_count);
        self.total_fetch_count = self
            .total_fetch_count
            .saturating_add(other.total_fetch_count);
        self.total_fetch_bytes = self
            .total_fetch_bytes
            .saturating_add(other.total_fetch_bytes);
        self.total_put_count = self.total_put_count.saturating_add(other.total_put_count);
        self.total_put_bytes = self.total_put_bytes.saturating_add(other.total_put_bytes);
        self.total_delete_count = self
            .total_delete_count
            .saturating_add(other.total_delete_count);
    }
}

impl From<&StorageStatsAtomic> for StorageStats {
    /// Creates `StorageStats` from `StorageStatsAtomic` by loading atomic values.
    fn from(stats: &StorageStatsAtomic) -> Self {
        StorageStats {
            check_point: stats.check_point.load(Ordering::Relaxed),
            version: stats.version.load(Ordering::Relaxed),
            last_saved: stats.last_saved.load(Ordering::Relaxed),
            total_cache_get_count: stats.total_cache_get_count.load(Ordering::Relaxed),
            total_fetch_count: stats.total_fetch_count.load(Ordering::Relaxed),
            total_fetch_bytes: stats.total_fetch_bytes.load(Ordering::Relaxed),
            total_put_count: stats.total_put_count.load(Ordering::Relaxed),
            total_put_bytes: stats.total_put_bytes.load(Ordering::Relaxed),
            total_delete_count: stats.total_delete_count.load(Ordering::Relaxed),
        }
    }
}

impl From<&StorageStats> for StorageStatsAtomic {
    /// Creates `StorageStatsAtomic` from `StorageStats` by initializing atomic values.
    fn from(stats: &StorageStats) -> Self {
        StorageStatsAtomic {
            check_point: AtomicU64::new(stats.check_point),
            version: AtomicU64::new(stats.version),
            last_saved: AtomicU64::new(stats.last_saved),
            total_cache_get_count: AtomicU64::new(stats.total_cache_get_count),
            total_fetch_count: AtomicU64::new(stats.total_fetch_count),
            total_fetch_bytes: AtomicU64::new(stats.total_fetch_bytes),
            total_put_count: AtomicU64::new(stats.total_put_count),
            total_put_bytes: AtomicU64::new(stats.total_put_bytes),
            total_delete_count: AtomicU64::new(stats.total_delete_count),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ObjectVersion {
    /// The ETag (entity tag) of the object, used for caching and conditional requests.
    /// See <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    pub e_tag: Option<String>,
    /// The version ID of the object, specific to the object store implementation (e.g., S3 versioning).
    /// Note: Many `ObjectStore` implementations might not populate this field.
    pub version: Option<String>,
}

impl From<ObjectVersion> for UpdateVersion {
    /// Converts `ObjectVersion` to `object_store::UpdateVersion`.
    fn from(version: ObjectVersion) -> Self {
        UpdateVersion {
            e_tag: version.e_tag,
            version: version.version,
        }
    }
}

impl From<PutResult> for ObjectVersion {
    /// Converts `object_store::PutResult` to `ObjectVersion`.
    fn from(version: PutResult) -> Self {
        ObjectVersion {
            e_tag: version.e_tag,
            version: version.version,
        }
    }
}

impl From<&ObjectMeta> for ObjectVersion {
    /// Converts `object_store::ObjectMeta` to `ObjectVersion`.
    fn from(meta: &ObjectMeta) -> Self {
        ObjectVersion {
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        }
    }
}

impl Storage {
    /// The fixed path for storing storage metadata within the base path.
    const METADATA_PATH: &'static str = "storage_meta.cbor";

    /// Constructs the full object store path for a given document path.
    fn full_path(&self, path: &str) -> Path {
        // don't use Path::join to avoid percent encoded
        Path::from(format!("{}/{}", self.inner.base_path, path))
    }

    /// Connects to or initializes the storage at the given path.
    ///
    /// Attempts to load existing metadata. If not found, initializes with the provided config.
    ///
    /// # Arguments
    ///
    /// * `path` - The base path for this storage instance within the object store.
    /// * `object_store` - The underlying object store implementation.
    /// * `config` - The configuration for the storage layer.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if metadata loading fails (other than NotFound) or initialization fails.
    pub async fn connect(
        path: String,
        object_store: Arc<dyn ObjectStore>,
        config: StorageConfig,
    ) -> Result<Storage, DBError> {
        let stats = StorageStats::default();
        let metadata = StorageMetadata {
            path,
            config,
            stats,
        };

        let storage = Storage::new(object_store.clone(), metadata)?;
        match storage.fetch(Storage::METADATA_PATH).await {
            Ok((metadata, _)) => Storage::new(object_store, metadata),
            Err(DBError::NotFound { .. }) => Ok(storage),
            Err(err) => Err(err),
        }
    }

    /// Stores the current storage metadata (config and stats) to the object store.
    ///
    /// This operation is rate-limited by `last_saved` timestamp to avoid frequent writes.
    /// It increments the internal version counter upon successful save.
    ///
    /// # Arguments
    ///
    /// * `check_point` - The current checkpoint value, used for `check_point`, usually the max document ID.
    /// * `now_ms` - The current timestamp in milliseconds, used for `last_saved`.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if writing the metadata fails.
    pub async fn store_metadata(&self, check_point: u64, now_ms: u64) -> Result<(), DBError> {
        let prev_last_saved = self
            .inner
            .stats
            .last_saved
            .fetch_max(now_ms, Ordering::Acquire);
        let check_point_advanced = if check_point > 0 {
            let prev_check_point = self
                .inner
                .stats
                .check_point
                .fetch_max(check_point, Ordering::AcqRel);
            check_point > prev_check_point
        } else {
            false
        };

        if prev_last_saved >= now_ms && !check_point_advanced {
            // Skip only when both timestamp and checkpoint are unchanged.
            return Ok(());
        }

        self.inner.stats.version.fetch_add(1, Ordering::Release);
        let metadata = self.metadata();
        self.put(Storage::METADATA_PATH, &metadata, None).await?;

        Ok(())
    }

    /// Creates a new `Storage` instance. Internal use only.
    /// Use `connect` for public instantiation.
    fn new(
        object_store: Arc<dyn ObjectStore>,
        metadata: StorageMetadata,
    ) -> Result<Storage, DBError> {
        // Create a cache with a size limit
        let cache = if metadata.config.cache_max_capacity > 0 {
            Some(
                Cache::builder()
                    .max_capacity(metadata.config.cache_max_capacity)
                    .build(),
            )
        } else {
            None
        };

        Ok(Storage {
            inner: Arc::new(InnerStorage {
                object_store,
                base_path: Path::from(metadata.path.as_str()),
                stats: (&metadata.stats).into(),
                metadata,
                cache,
            }),
        })
    }

    /// Returns a copy of the current storage metadata.
    pub fn metadata(&self) -> StorageMetadata {
        StorageMetadata {
            path: self.inner.metadata.path.clone(),
            config: self.inner.metadata.config.clone(),
            stats: self.stats(),
        }
    }

    /// Returns the base path of this storage instance.
    pub fn base_path(&self) -> &Path {
        &self.inner.base_path
    }

    /// Returns the configured bucket overload size.
    pub fn bucket_overload_size(&self) -> usize {
        self.inner.metadata.config.bucket_overload_size
    }

    /// Returns a copy of the current storage statistics.
    pub fn stats(&self) -> StorageStats {
        (&self.inner.stats).into()
    }

    /// Fetches and deserializes a document directly from the object store, bypassing the cache.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the document within the storage base path.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if the object is not found, fetching fails, or deserialization fails.
    pub async fn fetch<T>(&self, doc_path: &str) -> Result<(T, ObjectVersion), DBError>
    where
        T: DeserializeOwned,
    {
        let path = self.full_path(doc_path);
        // Try to get the document
        let (bytes, version) = self.inner_fetch(&path).await?;
        let doc: T = from_reader(&bytes[..]).map_err(|err| DBError::Serialization {
            name: self.inner.base_path.to_string(),
            source: err.into(),
        })?;

        Ok((doc, version))
    }

    /// Fetches the raw bytes of a document directly from the object store, bypassing the cache.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the document within the storage base path.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if the object is not found or fetching fails.
    pub async fn fetch_bytes(&self, doc_path: &str) -> Result<(Bytes, ObjectVersion), DBError> {
        let path = self.full_path(doc_path);
        // Try to get the document
        self.inner_fetch(&path).await
    }

    /// Internal helper to fetch raw bytes and handle decompression and stats updates.
    async fn inner_fetch(&self, path: &Path) -> Result<(Bytes, ObjectVersion), DBError> {
        // Try to get the document
        let result = self
            .inner
            .object_store
            .get_opts(path, GetOptions::default())
            .await
            .map_err(DBError::from)?;

        let size = result.meta.size;
        let version: ObjectVersion = (&result.meta).into();
        let bytes = result.bytes().await.map_err(DBError::from)?;

        // Use a generous multiple of max_small_object_size as the decompression
        // bomb guard. The put path checks pre-compression size against
        // max_small_object_size, so normal data always fits within this limit.
        let max_decompress_size =
            (self.inner.metadata.config.max_small_object_size as u64).saturating_mul(16);
        let bytes = try_decompress(bytes, max_decompress_size);
        self.inner
            .stats
            .total_fetch_count
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .stats
            .total_fetch_bytes
            .fetch_add(size, Ordering::Relaxed);

        Ok((bytes, version))
    }

    /// Gets and deserializes a document, potentially using the cache.
    ///
    /// If the cache is enabled and the object is found and considered "small",
    /// it will be served from the cache. Otherwise, it fetches from the object store
    /// and potentially caches it if small enough.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the document within the storage base path.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if the object is not found, fetching fails, or deserialization fails.
    pub async fn get<T>(&self, doc_path: &str) -> Result<(T, ObjectVersion), DBError>
    where
        T: DeserializeOwned,
    {
        let path = self.full_path(doc_path);

        self.inner_get(&path).await
    }

    /// Internal helper to get a document, handling cache logic.
    async fn inner_get<T>(&self, path: &Path) -> Result<(T, ObjectVersion), DBError>
    where
        T: DeserializeOwned,
    {
        if let Some(cache) = &self.inner.cache
            && let Some(arc) = cache.get(path).await
        {
            let doc: T = from_reader(&arc.0[..]).map_err(|err| DBError::Serialization {
                name: self.inner.base_path.to_string(),
                source: err.into(),
            })?;
            self.inner
                .stats
                .total_cache_get_count
                .fetch_add(1, Ordering::Relaxed);
            return Ok((doc, arc.1.clone()));
        }

        let (bytes, version) = self.inner_fetch(path).await?;
        let doc: T = from_reader(&bytes[..]).map_err(|err| DBError::Serialization {
            name: self.inner.base_path.to_string(),
            source: err.into(),
        })?;

        if let Some(cache) = &self.inner.cache
            && bytes.len() <= self.inner.metadata.config.max_small_object_size
        {
            // Cache the document if it is small enough
            cache
                .insert(path.clone(), Arc::new((bytes, version.clone())))
                .await;
        }

        Ok((doc, version))
    }

    /// Creates an asynchronous reader (`AsyncRead`) for streaming a document's content.
    /// Handles decompression automatically if enabled.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the document.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if fetching the object metadata fails.
    pub async fn stream_reader(
        &self,
        doc_path: &str,
    ) -> Result<Pin<Box<dyn tokio::io::AsyncRead + Send>>, DBError> {
        let path = self.full_path(doc_path);
        let meta = self
            .inner
            .object_store
            .head(&path)
            .await
            .map_err(DBError::from)?;
        let reader = BufReader::with_capacity(
            self.inner.object_store.clone(),
            &meta,
            self.inner.metadata.config.object_chunk_size,
        );

        // Coerce both branches to the same trait-object type to avoid
        // concrete-type mismatches between `ZstdDecoder` and `BufReader`.
        if self.inner.metadata.config.compress_level > 0 {
            let r: Pin<Box<dyn tokio::io::AsyncRead + Send>> = Box::pin(ZstdDecoder::new(reader));
            Ok(r)
        } else {
            let r: Pin<Box<dyn tokio::io::AsyncRead + Send>> = Box::pin(reader);
            Ok(r)
        }
    }

    /// Creates a new document in the object store. Fails if the document already exists.
    ///
    /// Serializes the document using `ciborium` and compresses it if enabled.
    /// This method is suitable only for objects smaller than `max_small_object_size`.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path for the new document.
    /// * `doc` - The document to serialize and store.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if serialization fails, the object is too large, or the put operation fails (e.g., object already exists).
    pub async fn create<T>(&self, doc_path: &str, doc: &T) -> Result<ObjectVersion, DBError>
    where
        T: Serialize,
    {
        let path = self.full_path(doc_path);
        let mut buf: Vec<u8> = Vec::new();

        into_writer(doc, &mut buf).map_err(|err| DBError::Serialization {
            name: self.inner.base_path.to_string(),
            source: err.into(),
        })?;

        self.inner.put(path, buf.into(), PutMode::Create).await
    }

    /// Puts (creates or overwrites/updates) a document in the object store.
    ///
    /// Serializes the document using `ciborium` and compresses it if enabled.
    /// This method is suitable only for objects smaller than `max_small_object_size`.
    /// Use `stream_writer` for larger objects.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the document.
    /// * `doc` - The document to serialize and store.
    /// * `version` - If `Some`, performs a conditional update based on the provided `ObjectVersion` (e.g., ETag match). If `None`, overwrites unconditionally.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if serialization fails, the object is too large, or the put operation fails (e.g., conditional update fails).
    pub async fn put<T>(
        &self,
        doc_path: &str,
        doc: &T,
        version: Option<ObjectVersion>,
    ) -> Result<ObjectVersion, DBError>
    where
        T: Serialize,
    {
        let path = self.full_path(doc_path);
        let mut buf: Vec<u8> = Vec::new();

        into_writer(doc, &mut buf).map_err(|err| DBError::Serialization {
            name: self.inner.base_path.to_string(),
            source: err.into(),
        })?;

        let mode = if let Some(version) = version {
            PutMode::Update(version.into())
        } else {
            PutMode::Overwrite
        };
        self.inner.put(path, buf.into(), mode).await
    }

    /// Puts raw bytes into the object store.
    ///
    /// Compresses the data if enabled.
    /// This method is suitable only for data smaller than `max_small_object_size`.
    /// Use `stream_writer` for larger data.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the object.
    /// * `data` - The raw bytes to store.
    /// * `mode` - The `PutMode` (Create, Overwrite, Update).
    ///
    /// # Errors
    ///
    /// Returns `DBError` if the data is too large or the put operation fails.
    pub async fn put_bytes(
        &self,
        doc_path: &str,
        data: Bytes,
        mode: PutMode,
    ) -> Result<ObjectVersion, DBError> {
        let path = self.full_path(doc_path);
        self.inner.put(path, data, mode).await
    }

    /// Creates an asynchronous writer (`AsyncWrite`) for writing small objects (< `max_small_object_size`).
    ///
    /// Data is buffered in memory and written in a single `put` operation on `poll_flush` or `poll_close`.
    /// Use `stream_writer` for large objects.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the object.
    /// * `mode` - The `PutMode` (Create, Overwrite, Update).
    pub fn to_writer(&self, doc_path: &str, mode: PutMode) -> SingleWriter {
        let path = self.full_path(doc_path);
        SingleWriter {
            inner: self.inner.clone(),
            path,
            buf: Vec::new(),
            mode,
            flushing: None,
            last_version: None,
        }
    }

    /// Creates an asynchronous writer (`AsyncWrite`) for streaming large objects.
    ///
    /// Data is written in chunks using the underlying object store's multipart upload or equivalent.
    /// Handles compression automatically if enabled.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the object.
    pub fn stream_writer(&self, doc_path: &str) -> Pin<Box<dyn tokio::io::AsyncWrite>> {
        let path = self.full_path(doc_path);
        let writer = BufWriter::with_capacity(
            self.inner.object_store.clone(),
            path.clone(),
            self.inner.metadata.config.object_chunk_size,
        );

        let level = async_compression::Level::Precise(self.inner.metadata.config.compress_level);
        if self.inner.metadata.config.compress_level > 0 {
            Box::pin(ZstdEncoder::with_quality(writer, level))
        } else {
            Box::pin(writer)
        }
    }

    /// Deletes a document from the object store and removes it from the cache if present.
    ///
    /// # Arguments
    ///
    /// * `doc_path` - The relative path of the document to delete.
    ///
    /// # Errors
    ///
    /// Returns `DBError` if the delete operation fails.
    pub async fn delete(&self, doc_path: &str) -> Result<(), DBError> {
        let path = self.full_path(doc_path);

        self.inner
            .object_store
            .delete(&path)
            .await
            .map_err(DBError::from)?;

        if let Some(cache) = &self.inner.cache {
            cache.remove(&path).await;
        }

        self.inner
            .stats
            .total_delete_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Lists documents in the storage, optionally filtering by prefix and starting from an offset.
    ///
    /// Fetches and deserializes each listed object. This can be inefficient for large listings.
    /// Consider using `object_store.list` directly if only metadata is needed.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Optional path prefix to filter the listing.
    /// * `offset` - Optional path to start the listing after.
    ///
    /// # Returns
    ///
    /// A stream of `Result<(T, ObjectVersion), DBError>`.
    pub fn list<T>(
        &self,
        prefix: Option<&str>,
        offset: Option<&str>,
    ) -> BoxStream<'_, Result<(T, ObjectVersion), DBError>>
    where
        T: DeserializeOwned + Send,
    {
        let path_prefix = if let Some(p) = prefix {
            self.full_path(p)
        } else {
            self.inner.base_path.clone()
        };

        let offset = offset.map(|o| self.full_path(o));

        let stream = if let Some(offset) = offset {
            self.inner
                .object_store
                .list_with_offset(Some(&path_prefix), &offset)
        } else {
            self.inner.object_store.list(Some(&path_prefix))
        };

        // Use inner_fetch (bypassing cache) to avoid polluting the cache
        // with every listed object during large scans.
        (stream
            .map_err(DBError::from)
            .try_filter_map(|meta| {
                let this = self.clone();
                async move {
                    let (bytes, version) = this.inner_fetch(&meta.location).await?;
                    let doc: T = from_reader(&bytes[..]).map_err(|err| DBError::Serialization {
                        name: this.inner.base_path.to_string(),
                        source: err.into(),
                    })?;
                    Ok(Some((doc, version)))
                }
            })
            .boxed()) as _
    }

    /// Lists object metadata in the storage, optionally filtering by prefix and starting from an offset.
    pub fn list_meta(
        &self,
        prefix: Option<&str>,
        offset: Option<&str>,
    ) -> BoxStream<'_, Result<ObjectMeta, DBError>> {
        let path_prefix = if let Some(p) = prefix {
            self.full_path(p)
        } else {
            self.inner.base_path.clone()
        };

        let offset = offset.map(|o| self.full_path(o));

        let stream = if let Some(offset) = offset {
            self.inner
                .object_store
                .list_with_offset(Some(&path_prefix), &offset)
        } else {
            self.inner.object_store.list(Some(&path_prefix))
        };

        (stream.map_err(DBError::from).boxed()) as _
    }

    /// Drops all objects under the storage's base path, effectively deleting the entire storage.
    pub async fn drop_data(&self) -> Result<(), DBError> {
        // 并发删除 base_path 下的所有对象，失败立即返回
        self.inner
            .object_store
            .list(Some(&self.inner.base_path))
            .map_err(DBError::from)
            .try_for_each_concurrent(16, move |meta| {
                let object_store = self.inner.object_store.clone();
                async move {
                    // 删除对象
                    object_store.delete(&meta.location).await?;
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }
}

impl InnerStorage {
    /// Internal helper to put bytes, handling compression, size checks, cache invalidation, and stats updates.
    async fn put(&self, path: Path, data: Bytes, mode: PutMode) -> Result<ObjectVersion, DBError> {
        // Check original (pre-compression) size to ensure the decompression path
        // can always recover the data within the same limit.
        let original_len = data.len();
        if data.len() > self.metadata.config.max_small_object_size {
            return Err(DBError::PayloadTooLarge {
                path: path.to_string(),
                size: original_len,
                limit: self.metadata.config.max_small_object_size,
            });
        }

        let data = if self.metadata.config.compress_level > 0 {
            try_compress(data, self.metadata.config.compress_level)
        } else {
            data
        };

        let result = self
            .object_store
            .put_opts(
                &path,
                data.into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
            .map_err(DBError::from)?;

        if let Some(cache) = &self.cache {
            cache.remove(&path).await;
        }

        self.stats.total_put_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_put_bytes
            .fetch_add(original_len as u64, Ordering::Relaxed);

        Ok(result.into())
    }
}

/// An `AsyncWrite` implementation for writing small objects (< `max_small_object_size`)
/// in a single `put` operation.
///
/// Buffers all written data in memory until `poll_flush` or `poll_close` is called.
/// If the total size exceeds `max_small_object_size`, the final `put` operation will fail.
/// Use `StreamWriter` for potentially large objects.
pub struct SingleWriter {
    inner: Arc<InnerStorage>,
    path: Path,
    buf: Vec<u8>,
    mode: PutMode,
    /// Holds the future for the ongoing flush operation.
    flushing: Option<BoxFuture<'static, Result<ObjectVersion, DBError>>>,
    last_version: Option<ObjectVersion>,
}

impl SingleWriter {
    /// Takes the last stored `ObjectVersion` after a successful flush.
    pub fn take_version(&mut self) -> Option<ObjectVersion> {
        self.last_version.take()
    }
}

impl futures::io::AsyncWrite for SingleWriter {
    /// Appends the buffer to the internal `Vec<u8>`.
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut written = 0;
        for s in bufs {
            self.buf.extend_from_slice(s);
            written += s.len();
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // 获取 self 的可变引用
        let this = self.as_mut().get_mut();

        // 如果没有正在进行的 flush 操作且 buffer 不为空，则创建一个
        if this.flushing.is_none() && !this.buf.is_empty() {
            let buf = std::mem::take(&mut this.buf);
            let inner = this.inner.clone();
            let path = this.path.clone();
            let mode = this.mode.clone();

            this.flushing = Some(Box::pin(
                async move { inner.put(path, buf.into(), mode).await },
            ));
        }

        // 如果有正在进行的 flush 操作，轮询它
        if let Some(fut) = &mut this.flushing {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(v)) => {
                    this.last_version = Some(v);
                    this.flushing = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    this.flushing = None;
                    Poll::Ready(Err(io::Error::other(e)))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            // 没有需要 flush 的数据
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl tokio::io::AsyncWrite for SingleWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        futures::io::AsyncWrite::poll_write(self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        futures::io::AsyncWrite::poll_flush(self, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        futures::io::AsyncWrite::poll_close(self, cx)
    }
}

/// Compresses bytes using zstd-safe.
/// Returns the original data unchanged if compression does not reduce size
/// (e.g. already-compressed or incompressible payloads).
#[inline]
fn try_compress(data: Bytes, compress_level: i32) -> Bytes {
    let size = zstd_safe::compress_bound(data.len());
    let mut buf = Vec::with_capacity(size);
    match zstd_safe::compress(&mut buf, &data[..], compress_level) {
        Ok(_) if buf.len() < data.len() => buf.into(),
        Ok(_) => data, // Compression did not reduce size, return original
        Err(err) => {
            log::error!("Failed to compress data: {err:?}");
            data
        }
    }
}

/// Decompresses bytes using zstd-safe.
#[inline]
fn try_decompress(data: Bytes, max_size: u64) -> Bytes {
    if !zstd_compressed(data.as_ref()) {
        return data;
    }

    match zstd_safe::find_decompressed_size(data.as_ref()) {
        Ok(Some(size)) => {
            if size > max_size {
                log::warn!(
                    "Decompressed size {} exceeds max_size {}, skip decompress",
                    size,
                    max_size
                );
                return data;
            }
            let mut buf = Vec::with_capacity(size as usize);
            match zstd_safe::decompress(&mut buf, &data[..]) {
                Ok(_) => buf.into(),
                Err(err) => {
                    log::error!("Failed to decompress (known size): {err:?}");
                    data
                }
            }
        }
        // Unknown size => 目前仍返回原数据（保持旧语义），但标注改进点
        Ok(None) => {
            log::debug!("Unknown decompressed size (zstd frame). Consider streaming fallback.");
            data
        }
        Err(err) => {
            log::error!("find_decompressed_size error: {err:?}");
            data
        }
    }
}

fn zstd_compressed(data: &[u8]) -> bool {
    // Check if the data is compressed using zstd
    if data.len() < 4 {
        return false;
    }
    let magic = &data[0..4];
    magic == b"\x28\xb5\x2f\xfd"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unix_ms;
    use object_store::memory::InMemory;
    use tokio::io::AsyncReadExt;

    // 创建一个测试用的存储实例
    async fn create_test_storage() -> Storage {
        let object_store = Arc::new(InMemory::new());
        let config = StorageConfig::default();
        Storage::connect("test".to_string(), object_store, config)
            .await
            .expect("Failed to create test storage")
    }

    #[tokio::test]
    async fn test_storage_connect() {
        let storage = create_test_storage().await;

        // 验证基本属性
        assert_eq!(storage.base_path().as_ref(), "test");
        assert_eq!(storage.bucket_overload_size(), 1024 * 1024);

        // 验证元数据
        let metadata = storage.metadata();
        assert_eq!(metadata.path, "test");
        assert_eq!(metadata.config.compress_level, 3);
        assert_eq!(metadata.config.cache_max_capacity, 10000);
    }

    #[tokio::test]
    async fn test_storage_metadata() {
        let storage = create_test_storage().await;
        let now_ms = unix_ms();

        // 存储元数据
        storage
            .store_metadata(0, now_ms)
            .await
            .expect("Failed to store metadata");

        // 验证元数据已更新
        let metadata = storage.metadata();
        assert!(metadata.stats.last_saved <= now_ms);
        assert_eq!(metadata.stats.version, 1);

        // 再次存储，但使用较小的时间戳（应该被忽略）
        storage
            .store_metadata(0, now_ms - 1000)
            .await
            .expect("Failed to store metadata");
        let metadata2 = storage.metadata();
        assert_eq!(metadata.stats.version, metadata2.stats.version);
    }

    #[tokio::test]
    async fn test_create_get_put() {
        let storage = create_test_storage().await;

        // 测试数据
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct TestDoc {
            id: String,
            value: i32,
        }

        let doc = TestDoc {
            id: "test1".to_string(),
            value: 42,
        };

        // 创建文档
        let version = storage
            .create("doc1", &doc)
            .await
            .expect("Failed to create document");

        // 获取文档
        let (fetched_doc, fetched_version) = storage
            .get::<TestDoc>("doc1")
            .await
            .expect("Failed to get document");

        // 验证文档内容和版本
        assert_eq!(doc, fetched_doc);
        assert_eq!(version, fetched_version);

        // 更新文档
        let updated_doc = TestDoc {
            id: "test1".to_string(),
            value: 100,
        };

        let new_version = storage
            .put("doc1", &updated_doc, Some(fetched_version.clone()))
            .await
            .expect("Failed to update document");
        println!("Updated version: {new_version:?}");

        // 获取更新后的文档
        let (fetched_updated_doc, _) = storage
            .get::<TestDoc>("doc1")
            .await
            .expect("Failed to get updated document");

        // 验证更新后的文档内容
        assert_eq!(updated_doc, fetched_updated_doc);

        // 验证统计信息
        let stats = storage.stats();
        println!("Stats: {stats:?}");
        assert_eq!(stats.total_fetch_count, 2);
        assert_eq!(stats.total_put_count, 2);
        assert_eq!(stats.total_cache_get_count, 0);
    }

    #[tokio::test]
    async fn test_fetch_and_cache() {
        let storage = create_test_storage().await;

        // 测试数据
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct TestDoc {
            id: String,
            data: Vec<u8>,
        }

        // 创建一个小文档（应该会被缓存）
        let small_doc = TestDoc {
            id: "small".to_string(),
            data: vec![1, 2, 3],
        };

        storage
            .create("small_doc", &small_doc)
            .await
            .expect("Failed to create small document");

        // 获取文档两次（第二次应该从缓存获取）
        let stats_before = storage.stats();
        let (_, _) = storage
            .get::<TestDoc>("small_doc")
            .await
            .expect("Failed to get small document");
        let (_, _) = storage
            .get::<TestDoc>("small_doc")
            .await
            .expect("Failed to get small document again");
        let stats_after = storage.stats();

        // 验证 fetch 计数只增加了一次（第二次从缓存获取）
        assert_eq!(
            stats_after.total_fetch_count,
            stats_before.total_fetch_count + 1
        );
        assert_eq!(
            stats_after.total_cache_get_count,
            stats_before.total_cache_get_count + 1
        );

        // 直接 fetch（绕过缓存）
        let stats_before = storage.stats();
        let (_, _) = storage
            .fetch::<TestDoc>("small_doc")
            .await
            .expect("Failed to fetch small document");
        let stats_after = storage.stats();

        // 验证 fetch 计数增加了一次
        assert_eq!(
            stats_after.total_fetch_count,
            stats_before.total_fetch_count + 1
        );
    }

    #[tokio::test]
    async fn test_fetch_bytes() {
        let storage = create_test_storage().await;

        // 创建原始数据
        let data = b"raw data for testing".to_vec();
        storage
            .inner
            .put(
                storage.full_path("raw_doc"),
                data.clone().into(),
                PutMode::Create,
            )
            .await
            .expect("Failed to create raw document");

        // 获取原始数据
        let (fetched_data, _) = storage
            .fetch_bytes("raw_doc")
            .await
            .expect("Failed to fetch raw document");

        // 验证数据内容
        assert_eq!(&data[..], &fetched_data[..]);
    }

    #[tokio::test]
    async fn test_stream_reader() {
        let storage = create_test_storage().await;

        // Write data using stream_writer (the paired write path for stream_reader)
        let data = b"test data for streaming".to_vec();
        let mut writer = storage.stream_writer("stream_doc");
        tokio::io::AsyncWriteExt::write_all(&mut writer, &data)
            .await
            .expect("Failed to write data");
        tokio::io::AsyncWriteExt::shutdown(&mut writer)
            .await
            .expect("Failed to shutdown writer");

        // 创建流式读取器
        let mut reader = storage
            .stream_reader("stream_doc")
            .await
            .expect("Failed to create stream reader");

        // 读取数据
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .await
            .expect("Failed to read from stream");

        // 验证数据内容
        assert_eq!(data, buffer);
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = create_test_storage().await;

        // 创建测试数据
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct TestDoc {
            id: String,
        }

        let doc = TestDoc {
            id: "to_delete".to_string(),
        };

        storage
            .create("delete_doc", &doc)
            .await
            .expect("Failed to create document");

        // 验证文档存在
        let result = storage.get::<TestDoc>("delete_doc").await;
        assert!(result.is_ok());

        // 删除文档
        storage
            .delete("delete_doc")
            .await
            .expect("Failed to delete document");

        // 验证文档已删除
        let result = storage.get::<TestDoc>("delete_doc").await;
        assert!(matches!(result, Err(DBError::NotFound { .. })));

        // 验证删除计数
        let stats = storage.stats();
        assert!(stats.total_delete_count > 0);
    }

    #[tokio::test]
    async fn test_compression() {
        // 创建一个启用压缩的存储
        let object_store = Arc::new(InMemory::new());
        let config = StorageConfig {
            compress_level: 3,
            ..Default::default()
        };
        let storage_with_compression =
            Storage::connect("compressed".to_string(), object_store.clone(), config)
                .await
                .expect("Failed to create storage with compression");

        // 创建一个禁用压缩的存储
        let config = StorageConfig {
            compress_level: 0,
            ..Default::default()
        }; // 禁用压缩
        let storage_without_compression =
            Storage::connect("uncompressed".to_string(), object_store, config)
                .await
                .expect("Failed to create storage without compression");

        // 创建一个可压缩的大文档
        let large_data = vec![b'a'; 10000];

        // 分别存储到两个存储中
        storage_with_compression
            .put_bytes("large_doc", large_data.clone().into(), PutMode::Create)
            .await
            .expect("Failed to store compressed document");

        storage_without_compression
            .put_bytes("large_doc", large_data.clone().into(), PutMode::Create)
            .await
            .expect("Failed to store uncompressed document");

        // 获取两个文档的元数据
        let compressed_meta = storage_with_compression
            .inner
            .object_store
            .head(&storage_with_compression.full_path("large_doc"))
            .await
            .expect("Failed to get compressed document metadata");

        let uncompressed_meta = storage_without_compression
            .inner
            .object_store
            .head(&storage_without_compression.full_path("large_doc"))
            .await
            .expect("Failed to get uncompressed document metadata");

        // 验证压缩版本的大小应该小于未压缩版本
        println!(
            "Compressed size: {}, Uncompressed size: {}",
            compressed_meta.size, uncompressed_meta.size
        );
        assert!(compressed_meta.size < uncompressed_meta.size);
        let (large_doc, _) = storage_with_compression
            .fetch_bytes("large_doc")
            .await
            .expect("Failed to get compressed document");
        assert_eq!(&large_doc, &large_data);

        let (large_doc, _) = storage_without_compression
            .fetch_bytes("large_doc")
            .await
            .expect("Failed to get uncompressed document");
        assert_eq!(&large_doc, &large_data);
    }

    #[tokio::test]
    async fn test_to_writer() {
        let storage = create_test_storage().await;

        // 使用 to_writer 写入数据
        let mut writer = storage.to_writer("writer_doc", PutMode::Create);

        let test_data = b"Hello, this is test data for to_writer!";
        tokio::io::AsyncWriteExt::write_all(&mut writer, test_data)
            .await
            .expect("Failed to write data");

        // 确保数据被刷新到存储中
        tokio::io::AsyncWriteExt::flush(&mut writer)
            .await
            .expect("Failed to flush data");

        // 读取并验证数据
        let (fetched_data, _) = storage
            .fetch_bytes("writer_doc")
            .await
            .expect("Failed to fetch written document");

        assert_eq!(&fetched_data[..], &test_data[..]);
    }

    #[tokio::test]
    async fn test_stream_writer() {
        let storage = create_test_storage().await;

        // 创建一个流式写入器
        let mut writer = storage.stream_writer("stream_write_doc");

        // 写入大量数据以测试分块和潜在的压缩
        let large_data = vec![b'x'; 1024 * 1024]; // 1MB 数据
        tokio::io::AsyncWriteExt::write_all(&mut writer, &large_data)
            .await
            .expect("Failed to write large data");

        // 关闭写入器以确保所有数据被写入
        tokio::io::AsyncWriteExt::shutdown(&mut writer)
            .await
            .expect("Failed to close writer");

        // 读取并验证数据
        let mut reader = storage
            .stream_reader("stream_write_doc")
            .await
            .expect("Failed to create stream reader");

        let mut read_data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut read_data)
            .await
            .expect("Failed to read from stream");

        assert_eq!(read_data.len(), large_data.len());
        assert_eq!(read_data, large_data);
    }

    #[tokio::test]
    async fn test_stream_reader_writer_with_compression() {
        let storage = create_test_storage().await;

        // 创建可压缩的重复数据
        let compressible_data = vec![b'y'; 500 * 1024]; // 500KB 重复数据

        // 使用流式写入器写入数据
        let mut writer = storage.stream_writer("compressed_doc");
        tokio::io::AsyncWriteExt::write_all(&mut writer, &compressible_data)
            .await
            .expect("Failed to write compressible data");
        tokio::io::AsyncWriteExt::shutdown(&mut writer)
            .await
            .expect("Failed to close writer");

        // 使用流式读取器读回数据
        let mut reader = storage
            .stream_reader("compressed_doc")
            .await
            .expect("Failed to create stream reader");

        let mut read_data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut read_data)
            .await
            .expect("Failed to read from stream");

        // 验证数据完整性
        assert_eq!(read_data.len(), compressible_data.len());
        assert_eq!(read_data, compressible_data);

        // 验证文件大小（如果启用了压缩，存储大小应小于原始大小）
        if storage.inner.metadata.config.compress_level > 0 {
            let meta = storage
                .inner
                .object_store
                .head(&storage.full_path("compressed_doc"))
                .await
                .expect("Failed to get metadata");

            println!(
                "Original size: {}, Stored size: {}",
                compressible_data.len(),
                meta.size
            );
            assert!(meta.size < compressible_data.len() as u64);
        }
    }

    #[tokio::test]
    async fn test_interleaved_read_write() {
        let storage = create_test_storage().await;

        // 写入部分数据
        let mut writer = storage.to_writer("interleaved_doc", PutMode::Create);
        tokio::io::AsyncWriteExt::write_all(&mut writer, b"First part of data.")
            .await
            .expect("Failed to write first part");
        tokio::io::AsyncWriteExt::flush(&mut writer)
            .await
            .expect("Failed to flush first part");

        // 读取已写入的数据
        let (fetched_data, _) = storage
            .fetch_bytes("interleaved_doc")
            .await
            .expect("Failed to fetch first part");
        assert_eq!(&fetched_data[..], b"First part of data.");

        // 覆盖写入新数据
        let mut writer = storage.to_writer("interleaved_doc", PutMode::Overwrite);
        tokio::io::AsyncWriteExt::write_all(&mut writer, b"Completely new data!")
            .await
            .expect("Failed to write new data");
        tokio::io::AsyncWriteExt::flush(&mut writer)
            .await
            .expect("Failed to flush new data");

        // 读取并验证新数据
        let (fetched_data, _) = storage
            .fetch_bytes("interleaved_doc")
            .await
            .expect("Failed to fetch new data");
        assert_eq!(&fetched_data[..], b"Completely new data!");
    }

    #[tokio::test]
    async fn test_storage_drop_all() {
        let storage = create_test_storage().await;

        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        struct T {
            k: &'static str,
        }

        storage.create("a", &T { k: "1" }).await.unwrap();
        storage.create("b", &T { k: "2" }).await.unwrap();
        storage.create("dir/c", &T { k: "3" }).await.unwrap();

        // 确认存在对象
        let before = storage
            .list_meta(None, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(before.len() >= 3);

        // 删除全部
        storage.drop_data().await.unwrap();

        // 应为空
        let after = storage
            .list_meta(None, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(after.is_empty());
    }

    #[tokio::test]
    async fn test_storage_list_and_meta() {
        let storage = create_test_storage().await;

        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        struct T {
            id: i32,
        }

        // 创建测试数据
        storage.create("users/1", &T { id: 1 }).await.unwrap();
        storage.create("users/2", &T { id: 2 }).await.unwrap();
        storage.create("users/3", &T { id: 3 }).await.unwrap();
        storage.create("apps/1", &T { id: 101 }).await.unwrap();

        // 1. 测试 list_meta (无 prefix)
        let all_meta = storage
            .list_meta(None, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(all_meta.len(), 4);

        // 2. 测试 list_meta (有 prefix)
        let users_meta = storage
            .list_meta(Some("users/"), None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(users_meta.len(), 3);

        // 3. 测试 list_meta (有 offset)
        // object_store 的 list 结果通常是按字典序排序的
        let offset = "users/1";
        let users_meta_offset = storage
            .list_meta(Some("users/"), Some(offset))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(users_meta_offset.len(), 2);
        // 应该剩下 "users/2" 和 "users/3"

        // 4. 测试 list (有 prefix)
        let users: Vec<(T, ObjectVersion)> = storage
            .list::<T>(Some("users/"), None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(users.len(), 3);

        // 5. 测试 list (有 offset)
        let users_offset: Vec<(T, ObjectVersion)> = storage
            .list::<T>(Some("users/"), Some("users/2"))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(users_offset.len(), 1);
        assert_eq!(users_offset[0].0.id, 3);
    }
}
