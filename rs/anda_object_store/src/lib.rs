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
//!   (notably `object_store::local::LocalFileSystem`).
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
//! See `docs/anda_object_store.md` in the repository for the full design
//! document.

use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_URL_SAFE};
use bytes::Bytes;
use futures::stream::BoxStream;
use moka::future::Cache;
use object_store::{path::Path, *};
use serde::{Deserialize, Serialize};
use sha3::Digest;
use std::{ops::Range, sync::Arc, time::Duration};

/// Transparent AES-256-GCM encryption-at-rest layer for any [`ObjectStore`].
pub mod encryption;
mod sidecar;

pub use encryption::{EncryptedStore, EncryptedStoreBuilder, EncryptedStoreUploader};

use sidecar::{SidecarMeta, SidecarStore};

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
    inner: Arc<SidecarStore<T, Metadata>>,
}

/// Builder for creating a `MetaStore` instance.
///
/// This builder configures:
/// - The underlying storage implementation
/// - Metadata cache settings
pub struct MetaStoreBuilder<T: ObjectStore> {
    /// The underlying storage implementation
    store: T,
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

impl SidecarMeta for Metadata {
    const STORE_NAME: &'static str = "MetaStore";

    fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    fn set_original(&mut self, e_tag: Option<String>, version: Option<String>) {
        self.original_tag = e_tag;
        self.original_version = version;
    }
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
            inner: Arc::new(SidecarStore::new(self.store, self.meta_cache)),
        }
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
                // Hash segment-by-segment so multi-segment payloads are not
                // concatenated into a temporary contiguous buffer.
                let mut hasher = sha3::Sha3_256::new();
                for segment in payload.iter() {
                    hasher.update(segment);
                }
                let hash: [u8; 32] = hasher.finalize().into();

                let mut meta = Metadata {
                    size: payload.content_length() as u64,
                    e_tag: Some(BASE64_URL_SAFE.encode(hash)),
                    original_tag: None,
                    original_version: None,
                };

                let rt = self.inner.store.put_opts(&full_path, payload, opts).await?;
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
        apply_logical_etag_preconditions(
            location,
            &mut options,
            meta.e_tag.as_deref(),
            meta.original_tag.clone(),
        )?;

        let mut res = self.inner.store.get_opts(&full_path, options).await?;
        res.meta.location = self.inner.strip_prefix(res.meta.location);
        res.meta.e_tag = meta.e_tag.clone();

        Ok(res)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let meta = self.inner.get_meta(location).await?;
        validate_ranges("MetaStore", ranges, meta.size)?;

        let full_path = self.inner.full_path(location);
        self.inner.store.get_ranges(&full_path, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.clone().delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.clone().list(prefix, true)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.clone().list_with_offset(prefix, offset, true)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix, true).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
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
    /// Shared sidecar core of the originating `MetaStore`
    store: Arc<SidecarStore<T, Metadata>>,
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
        self.size += payload.content_length();
        for segment in payload.iter() {
            self.hasher.update(segment);
        }
        self.inner.put_part(payload)
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

fn check_update_version(
    location: &Path,
    current_e_tag: &Option<String>,
    current_version: &Option<String>,
    update: &UpdateVersion,
) -> Result<()> {
    // Mirror `object_store`'s in-memory reference behavior: an e_tag is
    // required for conditional updates.
    let Some(expected) = &update.e_tag else {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: "missing e_tag for conditional update".into(),
        });
    };

    if current_e_tag.as_ref() != Some(expected) {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: format!("{:?} does not match {:?}", current_e_tag, update.e_tag).into(),
        });
    }

    if let Some(version) = &update.version
        && current_version.as_ref() != Some(version)
    {
        return Err(Error::Precondition {
            path: location.to_string(),
            source: format!("{:?} does not match {:?}", current_version, update.version).into(),
        });
    }

    Ok(())
}

fn apply_logical_etag_preconditions(
    location: &Path,
    options: &mut GetOptions,
    logical_e_tag: Option<&str>,
    original_tag: Option<String>,
) -> Result<()> {
    let e_tag = logical_e_tag.unwrap_or("*");

    if let Some(if_match) = options.if_match.take() {
        if if_match != "*" && if_match.split(',').map(str::trim).all(|tag| tag != e_tag) {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: format!("{e_tag} does not match {if_match}").into(),
            });
        }

        options.if_match = if if_match == "*" {
            Some(if_match)
        } else {
            original_tag
        };
    }

    if let Some(if_none_match) = options.if_none_match.take()
        && (if_none_match == "*"
            || if_none_match
                .split(',')
                .map(str::trim)
                .any(|tag| tag == e_tag))
    {
        return Err(Error::NotModified {
            path: location.to_string(),
            source: format!("{e_tag} matches {if_none_match}").into(),
        });
    }

    Ok(())
}

pub(crate) fn validate_ranges(store: &'static str, ranges: &[Range<u64>], len: u64) -> Result<()> {
    for range in ranges {
        if range.start >= len {
            return Err(Error::Generic {
                store,
                source: format!("start {} is larger than length {}", range.start, len).into(),
            });
        }
        if range.end <= range.start {
            return Err(Error::Generic {
                store,
                source: format!("end {} is less than start {}", range.end, range.start).into(),
            });
        }
        if range.end > len {
            return Err(Error::Generic {
                store,
                source: format!("end {} is larger than length {}", range.end, len).into(),
            });
        }
    }
    Ok(())
}

/// Re-clones an [`Arc<Error>`] returned from a `moka` shared computation
/// (e.g. [`Cache::try_get_with`]) into an owned [`Error`].
///
/// `moka` deduplicates concurrent loaders by returning the same `Arc<Error>`
/// to every waiter. Because [`object_store::Error`] is not [`Clone`], we must
/// reconstruct an equivalent variant by hand. Variants that carry a `path`
/// are reconstructed with their `path` and a stringified `source`; everything
/// else collapses into [`Error::Generic`].
fn map_arc_error(store: &'static str, err: Arc<Error>) -> Error {
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
            store,
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

    #[test]
    fn builder_display_debug_and_prefix_helpers_are_exercised() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100)
            .with_meta_cache_ttl(Duration::from_secs(1))
            .build();

        assert!(format!("{storage}").contains("MetaStore"));
        assert!(format!("{storage:?}").contains("MetaStore"));

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

    #[test]
    fn validate_ranges_rejects_invalid_boundaries() {
        fn check(range: Range<u64>, len: u64) -> Result<()> {
            validate_ranges("MetaStore", std::slice::from_ref(&range), len)
        }

        assert!(check(0..1, 1).is_ok());

        let err = check(1..2, 1).unwrap_err();
        assert!(err.to_string().contains("start 1 is larger than length 1"));

        let err = check(1..1, 3).unwrap_err();
        assert!(err.to_string().contains("end 1 is less than start 1"));

        let err = check(1..4, 3).unwrap_err();
        assert!(err.to_string().contains("end 4 is larger than length 3"));
    }

    #[test]
    fn map_arc_error_reconstructs_path_variants_and_generic_fallback() {
        let cases = [
            Error::NotFound {
                path: "not-found".to_string(),
                source: "missing".into(),
            },
            Error::AlreadyExists {
                path: "exists".to_string(),
                source: "exists".into(),
            },
            Error::Precondition {
                path: "precondition".to_string(),
                source: "stale".into(),
            },
            Error::NotModified {
                path: "not-modified".to_string(),
                source: "fresh".into(),
            },
            Error::PermissionDenied {
                path: "denied".to_string(),
                source: "denied".into(),
            },
            Error::Unauthenticated {
                path: "unauthenticated".to_string(),
                source: "auth".into(),
            },
        ];

        for err in cases {
            let mapped = map_arc_error("MetaStore", Arc::new(err));
            match mapped {
                Error::NotFound { path, source }
                | Error::AlreadyExists { path, source }
                | Error::Precondition { path, source }
                | Error::NotModified { path, source }
                | Error::PermissionDenied { path, source }
                | Error::Unauthenticated { path, source } => {
                    assert!(!path.is_empty());
                    assert!(!source.to_string().is_empty());
                }
                other => panic!("unexpected mapped error: {other:?}"),
            }
        }

        let mapped = map_arc_error(
            "MetaStore",
            Arc::new(Error::Generic {
                store: "Inner",
                source: "fallback".into(),
            }),
        );
        assert!(matches!(
            mapped,
            Error::Generic {
                store: "MetaStore",
                ..
            }
        ));
    }

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
    async fn get_ranges_requires_metadata() {
        let inner = InMemory::new();
        inner
            .put(
                &Path::from("data/missing-meta"),
                Bytes::from_static(b"abc").into(),
            )
            .await
            .unwrap();
        let storage = MetaStoreBuilder::new(inner, 100).build();

        let requested = 0..1;
        let err = storage
            .get_ranges(
                &Path::from("missing-meta"),
                std::slice::from_ref(&requested),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::NotFound { path, .. } if path.ends_with("meta/missing-meta")));
    }

    #[tokio::test]
    async fn get_opts_accepts_comma_separated_logical_etags() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("etag-list");
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
    async fn copy_and_rename_refresh_original_tag_for_logical_etag_preconditions() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let source = Path::from("copy-source");
        let copied = Path::from("copy-target");
        let renamed = Path::from("rename-target");
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
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("stale-version");
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
    async fn put_update_requires_e_tag() {
        let storage = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let location = Path::from("missing-etag");
        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        let err = storage
            .put_opts(
                &location,
                Bytes::from_static(b"def").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: None,
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }));
    }

    #[tokio::test]
    async fn delete_nonexistent_reports_logical_path() {
        let root = TempDir::new().unwrap();
        let storage =
            MetaStoreBuilder::new(LocalFileSystem::new_with_prefix(root.path()).unwrap(), 100)
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
    async fn delete_tolerates_missing_metadata_and_heals_orphans() {
        let root = TempDir::new().unwrap();
        let storage =
            MetaStoreBuilder::new(LocalFileSystem::new_with_prefix(root.path()).unwrap(), 100)
                .build();
        let location = Path::from("orphan");

        // Orphaned data (metadata lost): delete succeeds and removes the data.
        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        storage
            .inner
            .store
            .delete(&Path::from("meta/orphan"))
            .await
            .unwrap();
        storage.delete(&location).await.unwrap();
        let err = storage
            .inner
            .store
            .get(&Path::from("data/orphan"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));

        // Orphaned metadata (data lost): delete reports NotFound for the data
        // object but still cleans up the metadata.
        storage
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        storage
            .inner
            .store
            .delete(&Path::from("data/orphan"))
            .await
            .unwrap();
        let err = storage.delete(&location).await.unwrap_err();
        assert!(
            matches!(&err, Error::NotFound { path, .. } if path == "orphan"),
            "unexpected error: {err:?}"
        );
        let err = storage
            .inner
            .store
            .get(&Path::from("meta/orphan"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[tokio::test]
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
