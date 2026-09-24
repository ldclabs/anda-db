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
            chunk_aad(chunk_size, idx as u64).to_vec()
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
    let (ciphertext, aes_tags) = encrypt_chunks(&cipher, &base_nonce, plaintext, chunk_size, false);

    let hash = sha3_256(&ciphertext);
    let put = inner
        .put(
            &Path::from(format!("data/{location}")),
            Bytes::from(ciphertext).into(),
        )
        .await
        .unwrap();
    let meta = Metadata {
        validation: ValidationCertificate::default(),
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
        committed_at_ms: None,
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
    let (ciphertext, aes_tags) = encrypt_chunks(&cipher, &base_nonce, plaintext, chunk_size, true);

    let hash = sha3_256(&ciphertext);
    let put = inner
        .put(
            &Path::from(format!("data/{location}")),
            Bytes::from(ciphertext).into(),
        )
        .await
        .unwrap();
    let mut meta = Metadata {
        validation: ValidationCertificate::default(),
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
        committed_at_ms: None,
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
    // `with_conditional_put` is a deprecated no-op that is still part of
    // the public API, so it must keep compiling and building a store.
    #[allow(deprecated)]
    let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32])
        .with_meta_cache(cache)
        .with_conditional_put()
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

#[test]
fn meta_cache_ttl_preserves_the_configured_capacity() {
    // Regression: `with_meta_cache_ttl` must rebuild the cache from the
    // capacity passed to the constructor instead of silently dropping it.
    let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 1000, [0u8; 32])
        .with_meta_cache_ttl(Duration::from_secs(30))
        .build();
    let policy = storage.inner.meta_cache.policy();
    assert_eq!(policy.max_capacity(), Some(1000));
    assert_eq!(policy.time_to_live(), Some(Duration::from_secs(30)));
    assert_eq!(policy.time_to_idle(), Some(DEFAULT_META_CACHE_TTI));

    // Without the knob the defaults apply.
    let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 7, [0u8; 32]).build();
    let policy = storage.inner.meta_cache.policy();
    assert_eq!(policy.max_capacity(), Some(7));
    assert_eq!(policy.time_to_live(), Some(DEFAULT_META_CACHE_TTL));
    assert_eq!(policy.time_to_idle(), Some(DEFAULT_META_CACHE_TTI));
}

#[test]
fn builder_cache_setter_order_is_preserved() {
    let custom = Cache::builder().max_capacity(3).build();
    let store = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32])
        .with_meta_cache_ttl(Duration::from_secs(7))
        .with_meta_cache_bytes(1024)
        .with_meta_cache(custom.clone())
        .build();
    assert_eq!(store.inner.meta_cache.policy().max_capacity(), Some(3));
    assert_eq!(store.inner.meta_cache.policy().time_to_live(), None);

    let store = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32])
        .with_meta_cache(custom.clone())
        .with_meta_cache_ttl(Duration::from_secs(7))
        .build();
    assert_eq!(store.inner.meta_cache.policy().max_capacity(), Some(100));
    assert_eq!(
        store.inner.meta_cache.policy().time_to_live(),
        Some(Duration::from_secs(7))
    );

    let store = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32])
        .with_meta_cache(custom)
        .with_meta_cache_bytes(0)
        .build();
    assert_eq!(store.inner.meta_cache.policy().max_capacity(), Some(0));
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
async fn every_api_reports_the_same_authenticated_size_and_timestamp() {
    let inner = InMemory::new();
    let storage = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0u8; 32]).build();
    let location = Path::from("one-clock");
    storage
        .put(&location, Bytes::from_static(b"abc").into())
        .await
        .unwrap();

    let listed: Vec<_> = storage.list(None).try_collect().await.unwrap();
    assert_eq!(listed.len(), 1);
    let head = storage.head(&location).await.unwrap();
    assert_eq!(head.last_modified, listed[0].last_modified);
    assert_eq!(head.size, listed[0].size);
    assert_eq!(head.e_tag, listed[0].e_tag);
    let res = storage.get(&location).await.unwrap();
    assert_eq!(res.meta.last_modified, listed[0].last_modified);
    assert_eq!(res.meta.size, listed[0].size);

    // A timestamp taken from a listing therefore answers a conditional
    // read about the same commit.
    let err = storage
        .get_opts(
            &location,
            GetOptions {
                if_modified_since: Some(listed[0].last_modified),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotModified { .. }));
    storage
        .get_opts(
            &location,
            GetOptions {
                if_unmodified_since: Some(listed[0].last_modified),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // The size comes from the authenticated metadata: swapping the
    // ciphertext for a longer object — the one thing an attacker with
    // backend write access controls — cannot change what `head` reports
    // (and the content itself stays protected by the chunk tags).
    let ciphertext = ciphertext_path(&inner, &location).await;
    inner
        .put(&ciphertext, Bytes::from_static(b"0123456789").into())
        .await
        .unwrap();
    assert_eq!(storage.head(&location).await.unwrap().size, 3);
    assert!(storage.get(&location).await.unwrap().bytes().await.is_err());
}

#[tokio::test]
async fn multipart_last_modified_is_the_authenticated_commit_time() {
    let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32]).build();
    let location = Path::from("multipart-commit-time");
    let mut upload = storage.put_multipart(&location).await.unwrap();
    upload
        .put_part(Bytes::from_static(b"abc").into())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    let after_upload_started = chrono::Utc::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    upload.complete().await.unwrap();

    let head = storage.head(&location).await.unwrap();
    assert!(
        head.last_modified > after_upload_started,
        "last_modified must describe the authenticated metadata commit"
    );
    storage
        .get_opts(
            &location,
            GetOptions {
                if_modified_since: Some(after_upload_started),
                ..Default::default()
            },
        )
        .await
        .expect("an object committed after the condition date is modified");
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
async fn copy_and_rename_mint_their_own_logical_etag() {
    let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0u8; 32]).build();
    let source = Path::from("encrypted-copy-source");
    let copied = Path::from("encrypted-copy-target");
    let renamed = Path::from("encrypted-rename-target");
    let put = storage
        .put(&source, Bytes::from_static(b"abc").into())
        .await
        .unwrap();
    let e_tag = put.e_tag.unwrap();

    // A copy is a commit of its own: it must not hand the target the
    // source's CAS token, or the two keys would share one.
    storage.copy(&source, &copied).await.unwrap();
    let copied_e_tag = storage.head(&copied).await.unwrap().e_tag.unwrap();
    assert_ne!(copied_e_tag, e_tag);
    let err = storage
        .get_opts(
            &copied,
            GetOptions {
                if_match: Some(e_tag.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Precondition { .. }));

    // The target's own token addresses it, and the resealed document
    // still decrypts.
    let bytes = storage
        .get_opts(
            &copied,
            GetOptions {
                if_match: Some(copied_e_tag.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(bytes, Bytes::from_static(b"abc"));

    // A rename is a copy plus a delete, so the target is a new commit too.
    storage.rename(&copied, &renamed).await.unwrap();
    let renamed_e_tag = storage.head(&renamed).await.unwrap().e_tag.unwrap();
    assert_ne!(renamed_e_tag, e_tag);
    assert_ne!(renamed_e_tag, copied_e_tag);
    let bytes = storage
        .get_opts(
            &renamed,
            GetOptions {
                if_match: Some(renamed_e_tag),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(bytes, Bytes::from_static(b"abc"));

    // The source is untouched by either operation.
    assert_eq!(storage.head(&source).await.unwrap().e_tag, Some(e_tag));
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
/// therefore identifies the unique generation of each commit (see `put_opts`).
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

#[tokio::test]
async fn authenticated_metadata_is_verified_once_per_cached_context() {
    use std::sync::atomic::Ordering;
    let backend = InMemory::new();
    let cache = Cache::builder().max_capacity(100).build();
    let store = EncryptedStoreBuilder::with_secret(backend.clone(), 100, [0; 32])
        .with_chunk_size(8)
        .with_meta_cache(cache.clone())
        .build();
    let path = Path::from("verified");
    store
        .put(&path, Bytes::from(vec![1; 8192]).into())
        .await
        .unwrap();
    // Sealing certifies the document, so its own commit re-verifies nothing.
    let validations = store.crypto.authentications.load(Ordering::Relaxed);
    assert_eq!(validations, 0);
    for _ in 0..10 {
        store.head(&path).await.unwrap();
        store.get_range(&path, 3..4).await.unwrap();
        store.list(None).try_collect::<Vec<_>>().await.unwrap();
    }
    store.clone().head(&path).await.unwrap();
    assert_eq!(
        store.crypto.authentications.load(Ordering::Relaxed),
        validations
    );

    // A raw custom cache is not proof of authentication for another key/path.
    let foreign = EncryptedStoreBuilder::with_secret(backend.clone(), 100, [1; 32])
        .with_meta_cache(cache.clone())
        .build();
    assert!(foreign.head(&path).await.is_err());
    let meta = cache.get(&path).await.unwrap();
    cache.insert(Path::from("different"), meta).await;
    assert!(store.head(&Path::from("different")).await.is_err());

    // Nor may a lenient context certify legacy metadata for strict mode.
    let legacy = Path::from("legacy-certificate");
    put_legacy_encrypted_object(&backend, &legacy, b"abcdefgh", 4).await;
    let lenient = EncryptedStoreBuilder::with_secret(backend.clone(), 100, [0; 32])
        .with_meta_cache(cache.clone())
        .build();
    lenient.head(&legacy).await.unwrap();
    let strict = EncryptedStoreBuilder::with_secret(backend, 100, [0; 32])
        .with_meta_cache(cache)
        .with_strict_metadata_auth()
        .build();
    assert!(strict.head(&legacy).await.is_err());
}

#[tokio::test]
async fn weighted_cache_and_authenticated_layout_respect_limits() {
    let backend = InMemory::new();
    let store = EncryptedStoreBuilder::with_secret(backend.clone(), 100, [0; 32])
        .with_chunk_size(1)
        .with_meta_cache_bytes(512)
        .build();
    let path = Path::from("too-large-for-cache");
    store
        .put(&path, Bytes::from(vec![1; 1024]).into())
        .await
        .unwrap();
    store.inner.meta_cache.run_pending_tasks().await;
    assert!(store.inner.meta_cache.get(&path).await.is_none());
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap().len(),
        1024
    );

    let mut meta = read_meta(&backend, &path).await;
    meta.aes_tags.pop();
    seal_metadata(&test_cipher(), &path, &mut meta).unwrap();
    let mut data = Vec::new();
    cbor2::to_writer(&meta, &mut data).unwrap();
    backend
        .put(&Path::from("meta/too-large-for-cache"), data.into())
        .await
        .unwrap();
    let fresh = EncryptedStoreBuilder::with_secret(backend, 100, [0; 32]).build();
    let error = fresh.get(&path).await.unwrap_err();
    assert!(error.to_string().contains("tag count"));
}
