use anda_object_store::{
    EncryptedStoreBuilder, FaultKind, FaultOp, FaultRule, FaultStore, MetaStoreBuilder,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{memory::InMemory, path::Path, *};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Debug, Default)]
struct Controls {
    fail_after_meta_put: AtomicBool,
    pause_after_meta_put: AtomicBool,
    entered: tokio::sync::Notify,
    fail_part: AtomicBool,
    payload_gets: AtomicUsize,
    part_sizes: Mutex<Vec<usize>>,
    requests: Mutex<Vec<(String, Option<Marker>)>>,
    strict_parts: AtomicBool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Marker(u32);
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reply(&'static str);

impl Controls {
    fn request(&self, operation: &str, path: &Path, extensions: &Extensions) {
        self.requests.lock().unwrap().push((
            format!("{operation}:{path}"),
            extensions.get::<Marker>().cloned(),
        ));
    }
}

#[derive(Debug)]
struct ProbeStore {
    inner: InMemory,
    controls: Arc<Controls>,
}

#[derive(Debug)]
struct OverwriteOnlyStore<T>(T);

impl<T> std::fmt::Display for OverwriteOnlyStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OverwriteOnlyStore")
    }
}

fn conditional_not_implemented() -> Error {
    Error::NotImplemented {
        operation: "conditional create".into(),
        implementer: "OverwriteOnlyStore".into(),
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for OverwriteOnlyStore<T> {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if matches!(options.mode, PutMode::Create) {
            return Err(conditional_not_implemented());
        }
        self.0.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.0.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.0.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        if matches!(options.mode, CopyMode::Create) {
            return Err(conditional_not_implemented());
        }
        self.0.copy_opts(from, to, options).await
    }
}
impl std::fmt::Display for ProbeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProbeStore")
    }
}
fn injected() -> Error {
    Error::Generic {
        store: "review",
        source: "injected failure".into(),
    }
}
#[async_trait]
impl ObjectStore for ProbeStore {
    async fn put_opts(&self, p: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.controls.request("put", p, &opts.extensions);
        let mut result = self.inner.put_opts(p, payload, opts).await?;
        result.extensions.insert(Reply("put"));
        if p.as_ref().starts_with("meta/")
            && self
                .controls
                .pause_after_meta_put
                .swap(false, Ordering::SeqCst)
        {
            self.controls.entered.notify_one();
            futures::future::pending::<()>().await;
        }
        if p.as_ref().starts_with("meta/")
            && self
                .controls
                .fail_after_meta_put
                .swap(false, Ordering::SeqCst)
        {
            return Err(injected());
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.controls.request("multipart", p, &opts.extensions);
        Ok(Box::new(ProbeUpload {
            inner: self.inner.put_multipart_opts(p, opts).await?,
            controls: self.controls.clone(),
        }))
    }
    async fn get_opts(&self, p: &Path, opts: GetOptions) -> Result<GetResult> {
        self.controls.request("get", p, &opts.extensions);
        if p.as_ref().starts_with("gen/") {
            self.controls.payload_gets.fetch_add(1, Ordering::SeqCst);
        }
        let mut result = self.inner.get_opts(p, opts).await?;
        result.extensions.insert(Reply("get"));
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(p).await?;
        result.extensions.insert(Reply("list"));
        Ok(result)
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.controls.request("copy", to, &opts.extensions);
        self.inner.copy_opts(from, to, opts).await
    }
}
#[derive(Debug)]
struct ProbeUpload {
    inner: Box<dyn MultipartUpload>,
    controls: Arc<Controls>,
}
#[async_trait]
impl MultipartUpload for ProbeUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.controls.fail_part.swap(false, Ordering::SeqCst) {
            return Box::pin(async { Err(injected()) });
        }
        self.controls
            .part_sizes
            .lock()
            .unwrap()
            .push(data.content_length());
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> Result<PutResult> {
        if self.controls.strict_parts.load(Ordering::SeqCst) {
            let sizes = self.controls.part_sizes.lock().unwrap();
            if sizes.len() > 1
                && sizes[..sizes.len() - 1]
                    .iter()
                    .any(|size| *size < 5 * 1024 * 1024 || *size != sizes[0])
            {
                return Err(injected());
            }
        }
        self.inner.complete().await
    }
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

async fn meta_value(inner: &InMemory, p: &Path) -> cbor2::Value {
    let bytes = inner
        .get(&Path::from(format!("meta/{p}")))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    cbor2::from_reader(bytes.as_ref()).unwrap()
}
fn set_field(value: &mut cbor2::Value, name: &str, replacement: cbor2::Value) {
    let cbor2::Value::Map(fields) = value else {
        panic!()
    };
    fields
        .iter_mut()
        .find(|(k, _)| *k == cbor2::Value::Text(name.into()))
        .unwrap()
        .1 = replacement;
}
fn field_str(value: &cbor2::Value, name: &str) -> String {
    let cbor2::Value::Map(fields) = value else {
        panic!()
    };
    let v = &fields
        .iter()
        .find(|(k, _)| *k == cbor2::Value::Text(name.into()))
        .unwrap()
        .1;
    let cbor2::Value::Text(s) = v else { panic!() };
    s.clone()
}
async fn save_meta(inner: &InMemory, p: &Path, v: &cbor2::Value) {
    let mut bytes = vec![];
    cbor2::to_writer(v, &mut bytes).unwrap();
    inner
        .put(&Path::from(format!("meta/{p}")), bytes.into())
        .await
        .unwrap();
}

#[tokio::test]
async fn acknowledged_backend_commit_error_refreshes_cache() {
    for encrypted in [false, true] {
        let inner = InMemory::new();
        let controls = Arc::new(Controls::default());
        let backend = ProbeStore {
            inner: inner.clone(),
            controls: controls.clone(),
        };
        let store: Box<dyn ObjectStore> = if encrypted {
            Box::new(EncryptedStoreBuilder::with_secret(backend, 100, [0; 32]).build())
        } else {
            Box::new(MetaStoreBuilder::new(backend, 100).build())
        };
        let p = Path::from("commit");
        store
            .put(&p, Bytes::from_static(b"v1").into())
            .await
            .unwrap();
        controls.fail_after_meta_put.store(true, Ordering::SeqCst);
        assert!(
            store
                .put(&p, Bytes::from_static(b"v2").into())
                .await
                .is_err()
        );
        assert_eq!(
            store.get(&p).await.unwrap().bytes().await.unwrap().as_ref(),
            b"v2"
        );
        let fresh: Box<dyn ObjectStore> = if encrypted {
            Box::new(EncryptedStoreBuilder::with_secret(inner, 100, [0; 32]).build())
        } else {
            Box::new(MetaStoreBuilder::new(inner, 100).build())
        };
        assert_eq!(
            fresh.get(&p).await.unwrap().bytes().await.unwrap().as_ref(),
            b"v2"
        );
    }
}

#[tokio::test]
async fn cancelled_after_commit_refreshes_cache() {
    let inner = InMemory::new();
    let controls = Arc::new(Controls::default());
    let store = MetaStoreBuilder::new(
        ProbeStore {
            inner: inner.clone(),
            controls: controls.clone(),
        },
        100,
    )
    .build();
    let p = Path::from("cancel");
    store
        .put(&p, Bytes::from_static(b"v1").into())
        .await
        .unwrap();
    controls.pause_after_meta_put.store(true, Ordering::SeqCst);
    let mut future = Box::pin(store.put(&p, Bytes::from_static(b"v2").into()));
    tokio::select! { _ = &mut future => panic!("must park"), _ = controls.entered.notified() => () }
    drop(future);
    assert_eq!(
        store.get(&p).await.unwrap().bytes().await.unwrap().as_ref(),
        b"v2"
    );
    let fresh = MetaStoreBuilder::new(inner, 100).build();
    assert_eq!(
        fresh.get(&p).await.unwrap().bytes().await.unwrap().as_ref(),
        b"v2"
    );
}

#[tokio::test]
async fn strict_gc_preserves_payload_of_tampered_metadata() {
    let inner = InMemory::new();
    let store = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0; 32])
        .with_strict_metadata_auth()
        .build();
    let p = Path::from("gc");
    store
        .put(&p, Bytes::from_static(b"valid payload").into())
        .await
        .unwrap();
    let mut value = meta_value(&inner, &p).await;
    let old = Path::from(format!("gen/{p}/{}", field_str(&value, "g")));
    set_field(
        &mut value,
        "g",
        cbor2::Value::Text("0000000000000000-deadbeef".into()),
    );
    save_meta(&inner, &p, &value).await;
    let cold = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0; 32])
        .with_strict_metadata_auth()
        .build();
    assert!(cold.get(&p).await.is_err());
    tokio::time::sleep(Duration::from_millis(3)).await;
    assert_eq!(cold.collect_garbage().await.unwrap(), 0);
    assert!(inner.head(&old).await.is_ok());
}

#[tokio::test]
async fn forged_cleanup_pointer_cannot_delete_another_key() {
    for overwrite in [false, true] {
        let inner = InMemory::new();
        let store = EncryptedStoreBuilder::with_secret(inner.clone(), 100, [0; 32])
            .with_strict_metadata_auth()
            .build();
        let parent = Path::from("parent");
        let child = Path::from("parent/child");
        store
            .put(&parent, Bytes::from_static(b"parent bytes").into())
            .await
            .unwrap();
        store
            .put(&child, Bytes::from_static(b"child bytes").into())
            .await
            .unwrap();
        let child_meta = meta_value(&inner, &child).await;
        let mut parent_meta = meta_value(&inner, &parent).await;
        set_field(
            &mut parent_meta,
            "g",
            cbor2::Value::Text(format!("child/{}", field_str(&child_meta, "g"))),
        );
        save_meta(&inner, &parent, &parent_meta).await;
        if overwrite {
            store
                .put(&parent, Bytes::from_static(b"new parent").into())
                .await
                .unwrap();
        } else {
            store.delete(&parent).await.unwrap();
        }
        assert_eq!(
            store
                .get(&child)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"child bytes"
        );
    }
}

#[tokio::test]
async fn encrypted_complete_failure_cannot_publish_missing_tail() {
    let inner = InMemory::new();
    let controls = Arc::new(Controls::default());
    let store = EncryptedStoreBuilder::with_secret(
        ProbeStore {
            inner,
            controls: controls.clone(),
        },
        100,
        [0; 32],
    )
    .with_chunk_size(4)
    .build();
    let p = Path::from("retry");
    store
        .put(&p, Bytes::from_static(b"old").into())
        .await
        .unwrap();
    let mut upload = store.put_multipart(&p).await.unwrap();
    upload
        .put_part(Bytes::from_static(b"abc").into())
        .await
        .unwrap();
    controls.fail_part.store(true, Ordering::SeqCst);
    assert!(upload.complete().await.is_err());
    assert!(upload.complete().await.is_err());
    assert_eq!(
        store.get(&p).await.unwrap().bytes().await.unwrap().as_ref(),
        b"old"
    );
}

#[tokio::test]
async fn fault_torn_write_rejects_multipart_start() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    handle.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: None,
        skip: 0,
        times: 1,
        kind: FaultKind::TornWrite { keep_bytes: 1 },
    });
    assert!(store.put_multipart(&Path::from("torn")).await.is_err());
}

#[tokio::test]
async fn fault_crash_budget_intercepts_multipart_commit() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    let p = Path::from("power");
    let mut upload = store.put_multipart(&p).await.unwrap();
    upload
        .put_part(Bytes::from_static(b"abcd").into())
        .await
        .unwrap();
    let count = handle.mutation_count();
    handle.crash_after_mutations(0);
    assert!(upload.complete().await.is_err());
    assert_eq!(handle.mutation_count(), count + 1);
    assert!(store.inner().head(&p).await.is_err());
}

#[tokio::test]
async fn disabled_cache_still_supports_put_get() {
    let store = MetaStoreBuilder::new(InMemory::new(), 0).build();
    let p = Path::from("zero");
    store
        .put(&p, Bytes::from_static(b"x").into())
        .await
        .unwrap();
    assert_eq!(
        store.get(&p).await.unwrap().bytes().await.unwrap().as_ref(),
        b"x"
    );
}

#[tokio::test]
async fn fixed_sized_plaintext_parts_produce_equal_backend_parts() {
    let controls = Arc::new(Controls::default());
    controls.strict_parts.store(true, Ordering::SeqCst);
    let store = EncryptedStoreBuilder::with_secret(
        ProbeStore {
            inner: InMemory::new(),
            controls: controls.clone(),
        },
        10,
        [0; 32],
    )
    .build();
    let mut upload = store.put_multipart(&Path::from("r2")).await.unwrap();
    let part_size = 5 * 1024 * 1024 + 384 * 1024;
    for _ in 0..3 {
        upload.put_part(vec![0; part_size].into()).await.unwrap();
    }
    upload.complete().await.unwrap();
    let sizes = controls.part_sizes.lock().unwrap().clone();
    assert_eq!(sizes, vec![8 * 1024 * 1024, 8 * 1024 * 1024, 128 * 1024]);
}

#[tokio::test]
async fn scattered_ranges_fetch_each_chunk_once() {
    let controls = Arc::new(Controls::default());
    let store = EncryptedStoreBuilder::with_secret(
        ProbeStore {
            inner: InMemory::new(),
            controls: controls.clone(),
        },
        10,
        [0; 32],
    )
    .with_chunk_size(4)
    .build();
    let p = Path::from("ranges");
    store
        .put(&p, Bytes::from_static(b"abcdefgh").into())
        .await
        .unwrap();
    let ranges = [0..1, 4..5, 1..2, 5..6];
    let got = store.get_ranges(&p, &ranges).await.unwrap();
    assert_eq!(
        got,
        vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"e"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"f")
        ]
    );
    assert_eq!(controls.payload_gets.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_copy_pins_original_chunk_size() {
    use aes_gcm::{AeadInOut, Aes256Gcm, Key, KeyInit, Nonce};
    use serde::Serialize;
    use serde_bytes::ByteArray;
    #[derive(Serialize)]
    struct Legacy {
        s: u64,
        e: Option<String>,
        o: Option<String>,
        v: Option<String>,
        n: ByteArray<12>,
        t: Vec<ByteArray<16>>,
    }
    let inner = InMemory::new();
    let source = Path::from("legacy");
    let target = Path::from("migrated");
    let cipher = Aes256Gcm::new(&Key::<Aes256Gcm>::from([0; 32]));
    let mut ciphertext = b"abcdefgh".to_vec();
    let mut tags = vec![];
    for (idx, chunk) in ciphertext.chunks_mut(4).enumerate() {
        let mut nonce = [0u8; 12];
        nonce[4..12].copy_from_slice(&(idx as u64).to_le_bytes());
        let tag: [u8; 16] = cipher
            .encrypt_inout_detached(&Nonce::from(nonce), b"", chunk.into())
            .unwrap()
            .into();
        tags.push(tag.into());
    }
    let old = Legacy {
        s: 8,
        e: Some("old".into()),
        o: None,
        v: None,
        n: [0; 12].into(),
        t: tags,
    };
    let mut bytes = vec![];
    cbor2::to_writer(&old, &mut bytes).unwrap();
    inner
        .put(&Path::from("data/legacy"), ciphertext.into())
        .await
        .unwrap();
    inner
        .put(&Path::from("meta/legacy"), bytes.into())
        .await
        .unwrap();
    let migrator = EncryptedStoreBuilder::with_secret(inner.clone(), 10, [0; 32])
        .with_chunk_size(4)
        .build();
    migrator.copy(&source, &target).await.unwrap();
    assert_eq!(
        migrator
            .get(&target)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"abcdefgh"
    );
    let reopened = EncryptedStoreBuilder::with_secret(inner, 10, [0; 32])
        .with_chunk_size(8)
        .with_strict_metadata_auth()
        .build();
    assert_eq!(
        reopened
            .get(&target)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"abcdefgh"
    );
}

#[tokio::test]
async fn gc_ignores_non_hex_salt() {
    let inner = InMemory::new();
    let p = Path::from("gen/object/0000000000000001-not-hex!");
    inner
        .put(&p, Bytes::from_static(b"foreign").into())
        .await
        .unwrap();
    assert_eq!(
        MetaStoreBuilder::new(inner.clone(), 10)
            .build()
            .collect_garbage()
            .await
            .unwrap(),
        0
    );
    assert!(inner.head(&p).await.is_ok());
}

#[tokio::test]
async fn fault_log_records_failed_backend_create_as_reached_mutation() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    let p = Path::from("exists");
    store
        .put(&p, Bytes::from_static(b"x").into())
        .await
        .unwrap();
    assert!(
        store
            .put_opts(&p, Bytes::from_static(b"y").into(), PutMode::Create.into())
            .await
            .is_err()
    );
    assert_eq!(handle.mutation_log().len(), 2);
}

fn assert_clone<T: Clone>() {}
#[test]
fn wrappers_clone_non_clone_backends() {
    assert_clone::<anda_object_store::MetaStore<FaultStore<InMemory>>>();
    assert_clone::<anda_object_store::EncryptedStore<FaultStore<InMemory>>>();
}

#[tokio::test]
async fn fault_after_commit_and_cancellation_refresh_all_read_paths() {
    use anda_object_store::{FaultGate, FaultOutcome};
    for encrypted in [false, true] {
        for delete in [false, true] {
            for cancel in [false, true] {
                let backend = InMemory::new();
                let (fault, handle) = FaultStore::wrap(backend.clone());
                let store: Box<dyn ObjectStore> = if encrypted {
                    Box::new(EncryptedStoreBuilder::with_secret(fault, 100, [0; 32]).build())
                } else {
                    Box::new(MetaStoreBuilder::new(fault, 100).build())
                };
                let path = Path::from("outcome");
                store
                    .put(&path, Bytes::from_static(b"old").into())
                    .await
                    .unwrap();
                let gate = FaultGate::new();
                let operation = if delete {
                    FaultOp::Delete
                } else {
                    FaultOp::Put
                };
                handle.push_rule(FaultRule {
                    op: operation,
                    path_contains: Some("meta/".into()),
                    skip: 0,
                    times: 1,
                    kind: if cancel {
                        FaultKind::PauseAfter(gate.clone())
                    } else {
                        FaultKind::ErrorAfter
                    },
                });
                let mut future = Box::pin(async {
                    if delete {
                        store.delete(&path).await
                    } else {
                        store
                            .put(&path, Bytes::from_static(b"new").into())
                            .await
                            .map(|_| ())
                    }
                });
                if cancel {
                    tokio::select! { _ = &mut future => panic!("expected gate"), _ = gate.wait_entered() => () }
                    drop(future);
                } else {
                    assert!(future.await.is_err());
                }
                let events = handle.event_log();
                assert!(events.iter().any(|e| e.op == operation
                    && e.path == "meta/outcome"
                    && e.outcome == FaultOutcome::BackendSucceeded));
                if cancel {
                    assert!(events.iter().any(|e| e.outcome == FaultOutcome::Cancelled));
                }
                use futures::TryStreamExt;
                let listing: Vec<_> = store.list(None).try_collect().await.unwrap();
                if delete {
                    assert!(listing.is_empty());
                    assert!(matches!(
                        store.head(&path).await,
                        Err(Error::NotFound { .. })
                    ));
                    assert!(backend.head(&Path::from("meta/outcome")).await.is_err());
                } else {
                    let head = store.head(&path).await.unwrap();
                    assert_eq!(listing[0].e_tag, head.e_tag);
                    let result = store
                        .get_opts(
                            &path,
                            GetOptions {
                                if_match: head.e_tag,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    assert_eq!(result.bytes().await.unwrap().as_ref(), b"new");
                }
            }
        }
    }
}

async fn metadata_retry<T: ObjectStore>(backend: T, encrypted: bool) {
    let (fault, handle) = FaultStore::wrap(backend);
    let store: Box<dyn ObjectStore> = if encrypted {
        Box::new(
            EncryptedStoreBuilder::with_secret(fault, 100, [0; 32])
                .with_chunk_size(4)
                .build(),
        )
    } else {
        Box::new(MetaStoreBuilder::new(fault, 100).build())
    };
    let path = Path::from("multipart-retry");
    store
        .put(&path, Bytes::from_static(b"old").into())
        .await
        .unwrap();
    for after in [false, true] {
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"new value").into())
            .await
            .unwrap();
        handle.push_rule(FaultRule {
            op: FaultOp::Put,
            path_contains: Some("meta/".into()),
            skip: 0,
            times: 1,
            kind: if after {
                FaultKind::ErrorAfter
            } else {
                FaultKind::Error
            },
        });
        assert!(upload.complete().await.is_err());
        upload.complete().await.unwrap();
        let backend_completions = handle
            .event_log()
            .iter()
            .filter(|e| {
                e.op == FaultOp::MultipartComplete
                    && e.outcome == anda_object_store::FaultOutcome::BackendSucceeded
            })
            .count();
        assert_eq!(backend_completions, if after { 2 } else { 1 });
        assert_eq!(
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"new value"
        );
        let again = upload.complete().await.unwrap();
        assert_eq!(again.e_tag, store.head(&path).await.unwrap().e_tag);
        assert!(upload.abort().await.is_err());
    }
}

#[tokio::test]
async fn multipart_metadata_retry_never_recompletes_backend() {
    for encrypted in [false, true] {
        metadata_retry(InMemory::new(), encrypted).await;
        let dir = tempfile::tempdir().unwrap();
        metadata_retry(
            object_store::local::LocalFileSystem::new_with_prefix(dir.path())
                .unwrap()
                .with_fsync(true),
            encrypted,
        )
        .await;
    }
}

#[tokio::test]
async fn multipart_failed_or_cancelled_parts_cannot_be_committed() {
    for encrypted in [false, true] {
        for cancel in [false, true] {
            let (fault, handle) = FaultStore::wrap(InMemory::new());
            let store: Box<dyn ObjectStore> = if encrypted {
                Box::new(EncryptedStoreBuilder::with_secret(fault, 100, [0; 32]).build())
            } else {
                Box::new(MetaStoreBuilder::new(fault, 100).build())
            };
            let path = Path::from("parts");
            store
                .put(&path, Bytes::from_static(b"old").into())
                .await
                .unwrap();
            let mut upload = store.put_multipart(&path).await.unwrap();
            if !cancel {
                handle.push_rule(FaultRule::fail_once(FaultOp::MultipartPart, "gen/"));
            }
            let future = upload.put_part(vec![42; 8 * 1024 * 1024].into());
            if cancel {
                drop(future);
            } else {
                assert!(future.await.is_err());
            }
            assert!(upload.complete().await.is_err());
            assert!(upload.complete().await.is_err());
            assert_eq!(
                store
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .as_ref(),
                b"old"
            );
            upload.abort().await.unwrap();
            upload.abort().await.unwrap();
        }
    }
}

#[tokio::test]
async fn cancelled_multipart_materialization_is_terminal() {
    use anda_object_store::FaultGate;
    for encrypted in [false, true] {
        let backend = InMemory::new();
        let (fault, handle) = FaultStore::wrap(backend.clone());
        let store: Box<dyn ObjectStore> = if encrypted {
            Box::new(EncryptedStoreBuilder::with_secret(fault, 100, [0; 32]).build())
        } else {
            Box::new(MetaStoreBuilder::new(fault, 100).build())
        };
        let path = Path::from("cancel-complete");
        store
            .put(&path, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"new").into())
            .await
            .unwrap();
        let gate = FaultGate::new();
        handle.push_rule(FaultRule {
            op: FaultOp::MultipartComplete,
            path_contains: None,
            skip: 0,
            times: 1,
            kind: FaultKind::PauseAfter(gate.clone()),
        });
        let mut future = Box::pin(upload.complete());
        tokio::select! { _ = &mut future => panic!(), _ = gate.wait_entered() => () }
        drop(future);
        assert!(upload.complete().await.is_err());
        assert_eq!(
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"old"
        );
        upload.abort().await.unwrap();
        use futures::TryStreamExt;
        assert_eq!(
            backend
                .list(Some(&Path::from("gen")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1,
            "abort must remove only the uncommitted replacement generation"
        );
    }
}

#[tokio::test]
async fn decryption_first_chunk_does_not_retain_whole_upstream_allocation() {
    use futures::StreamExt;
    let store = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32])
        .with_chunk_size(4096)
        .build();
    let path = Path::from("stream");
    store
        .put(&path, vec![1; 2 * 1024 * 1024].into())
        .await
        .unwrap();
    let mut stream = store.get(&path).await.unwrap().into_stream();
    let first = stream.next().await.unwrap().unwrap();
    drop(stream);
    assert_eq!(first.len(), 64 * 1024);
    let buffer = first.try_into_mut().expect("chunk has its own allocation");
    assert!(
        buffer.capacity() <= 64 * 1024,
        "retained capacity {}",
        buffer.capacity()
    );
}

#[tokio::test]
async fn range_planning_matches_plaintext_oracle() {
    let payload: Vec<u8> = (0..10003).map(|i| (i % 251) as u8).collect();
    let store = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32])
        .with_chunk_size(127)
        .build();
    let path = Path::from("oracle");
    store.put(&path, payload.clone().into()).await.unwrap();
    let mut state = 17u64;
    let mut ranges = vec![0..10003, 10000..10003, 0..1];
    for _ in 0..300 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let start = state % 10003;
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let end = start + 1 + state % (10003 - start);
        ranges.push(start..end);
    }
    let values = store.get_ranges(&path, &ranges).await.unwrap();
    for (range, value) in ranges.iter().zip(values) {
        assert_eq!(
            value.as_ref(),
            &payload[range.start as usize..range.end as usize]
        );
    }
}

#[tokio::test]
async fn unsupported_versions_never_reach_payload_store() {
    for encrypted in [false, true] {
        let controls = Arc::new(Controls::default());
        let backend = ProbeStore {
            inner: InMemory::new(),
            controls: controls.clone(),
        };
        let store: Box<dyn ObjectStore> = if encrypted {
            Box::new(EncryptedStoreBuilder::with_secret(backend, 10, [0; 32]).build())
        } else {
            Box::new(MetaStoreBuilder::new(backend, 10).build())
        };
        let path = Path::from("version");
        store
            .put(&path, Bytes::from_static(b"data").into())
            .await
            .unwrap();
        assert!(matches!(
            store
                .get_opts(
                    &path,
                    GetOptions {
                        version: Some("foreign".into()),
                        ..Default::default()
                    }
                )
                .await,
            Err(Error::NotSupported { .. })
        ));
        assert_eq!(controls.payload_gets.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn gc_budget_failure_deletes_nothing_and_prefixes_allow_scoping() {
    use anda_object_store::GarbageCollectionOptions;
    let inner = InMemory::new();
    for key in ["a/1", "b/2"] {
        inner
            .put(
                &Path::from(format!("gen/{key}/0000000000000001-deadbeef")),
                Bytes::from_static(b"orphan").into(),
            )
            .await
            .unwrap();
    }
    let store = MetaStoreBuilder::new(inner.clone(), 10).build();
    assert!(
        store
            .collect_garbage_with_options(GarbageCollectionOptions {
                max_candidates: 1,
                ..Default::default()
            })
            .await
            .is_err()
    );
    use futures::TryStreamExt;
    assert_eq!(
        inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store
            .collect_garbage_with_options(GarbageCollectionOptions {
                prefix: Some(Path::from("a")),
                max_candidates: 1,
                ..Default::default()
            })
            .await
            .unwrap(),
        1
    );
    assert!(
        inner
            .head(&Path::from("gen/b/2/0000000000000001-deadbeef"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn metadata_resource_limits_apply_to_read_write_and_multipart() {
    use anda_object_store::MetadataLimits;
    let inner = InMemory::new();
    let writer = EncryptedStoreBuilder::with_secret(inner.clone(), 10, [0; 32]).build();
    let path = Path::from("limits");
    writer
        .put(&path, Bytes::from_static(b"abcdefgh").into())
        .await
        .unwrap();
    let reader = EncryptedStoreBuilder::with_secret(inner, 10, [0; 32])
        .with_metadata_limits(MetadataLimits {
            max_metadata_bytes: 16,
            ..Default::default()
        })
        .build();
    assert!(reader.get(&path).await.is_err());
    let small = EncryptedStoreBuilder::with_secret(InMemory::new(), 10, [0; 32])
        .with_chunk_size(1)
        .with_metadata_limits(MetadataLimits {
            max_object_size: 4,
            max_chunks: 3,
            ..Default::default()
        })
        .build();
    assert!(
        small
            .put(&path, Bytes::from_static(b"abcd").into())
            .await
            .is_err()
    );
    let mut upload = small.put_multipart(&path).await.unwrap();
    assert!(
        upload
            .put_part(Bytes::from_static(b"abcd").into())
            .await
            .is_err()
    );
    assert!(upload.complete().await.is_err());
}

#[tokio::test]
async fn options_and_response_extensions_follow_logical_operations() {
    for encrypted in [false, true] {
        let controls = Arc::new(Controls::default());
        let backend = ProbeStore {
            inner: InMemory::new(),
            controls: controls.clone(),
        };
        // Disable caching so every metadata subrequest is observed.
        let store: Box<dyn ObjectStore> = if encrypted {
            Box::new(EncryptedStoreBuilder::with_secret(backend, 0, [0; 32]).build())
        } else {
            Box::new(MetaStoreBuilder::new(backend, 0).build())
        };
        let mut extensions = Extensions::new();
        extensions.insert(Marker(7));
        let path = Path::from("extensions");
        let put = store
            .put_opts(
                &path,
                Bytes::from_static(b"data").into(),
                PutOptions {
                    extensions: extensions.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(put.extensions.get::<Reply>(), Some(&Reply("put")));
        let get = store
            .get_opts(
                &path,
                GetOptions {
                    extensions: extensions.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(get.extensions.get::<Reply>(), Some(&Reply("get")));
        assert_eq!(get.bytes().await.unwrap().as_ref(), b"data");
        store
            .rename_opts(
                &path,
                &path,
                RenameOptions {
                    extensions: extensions.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let target = Path::from("copy-extensions");
        store
            .copy_opts(
                &path,
                &target,
                CopyOptions {
                    extensions: extensions.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store
            .rename_opts(
                &target,
                &Path::from("renamed"),
                RenameOptions {
                    extensions: extensions.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut upload = store
            .put_multipart_opts(
                &path,
                PutMultipartOptions {
                    extensions,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        upload
            .put_part(Bytes::from_static(b"parts").into())
            .await
            .unwrap();
        let result = upload.complete().await.unwrap();
        assert_eq!(result.extensions.get::<Reply>(), Some(&Reply("put")));
        for (operation, marker) in controls.requests.lock().unwrap().iter() {
            assert_eq!(
                *marker,
                Some(Marker(7)),
                "missing request context at {operation}"
            );
        }
        let listing = store.list_with_delimiter(None).await.unwrap();
        assert_eq!(listing.extensions.get::<Reply>(), Some(&Reply("list")));
    }
}

#[tokio::test]
async fn pending_publication_cannot_resurrect_reclaimed_payload() {
    let (fault, handle) = FaultStore::wrap(InMemory::new());
    let store = MetaStoreBuilder::new(fault, 100).build();
    let path = Path::from("retired");
    let mut upload = store.put_multipart(&path).await.unwrap();
    upload
        .put_part(Bytes::from_static(b"first").into())
        .await
        .unwrap();
    handle.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: Some("meta/".into()),
        skip: 0,
        times: 1,
        kind: FaultKind::ErrorAfter,
    });
    assert!(upload.complete().await.is_err());
    // Keep the first generation physically present after the newer commit.
    // An idempotent retry must still reject the changed commit point.
    handle.push_rule(FaultRule::fail_once(FaultOp::Delete, "gen/"));
    store
        .put(&path, Bytes::from_static(b"replacement").into())
        .await
        .unwrap();
    assert!(upload.complete().await.is_err());
    assert_eq!(
        store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"replacement"
    );
}

#[tokio::test]
async fn multipart_terminal_states_release_gc_registration() {
    let backend = InMemory::new();
    let (fault, handle) = FaultStore::wrap(backend.clone());
    let store = MetaStoreBuilder::new(fault, 100).build();
    let path = Path::from("gc-upload");
    let mut upload = store.put_multipart(&path).await.unwrap();
    upload
        .put_part(Bytes::from_static(b"first").into())
        .await
        .unwrap();
    upload.complete().await.unwrap();
    handle.push_rule(FaultRule::fail_once(FaultOp::Delete, "gen/"));
    store
        .put(&path, Bytes::from_static(b"second").into())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert_eq!(store.collect_garbage().await.unwrap(), 1);
    // Keep the completed handle alive across GC to prove registration was freed.
    assert!(upload.complete().await.is_ok());
}

#[tokio::test]
async fn large_ranges_cross_transport_and_crypto_boundaries() {
    let chunk = 3 * 1024 * 1024;
    let payload: Vec<u8> = (0..4 * chunk + 17).map(|i| (i % 251) as u8).collect();
    let store = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32])
        .with_chunk_size(chunk as u64)
        .build();
    let path = Path::from("large-ranges");
    store.put(&path, payload.clone().into()).await.unwrap();
    let ranges = vec![
        5 * 1024 * 1024..8 * 1024 * 1024,
        11 * 1024 * 1024..payload.len() as u64,
        0..1,
        0..payload.len() as u64,
    ];
    for (range, result) in ranges
        .iter()
        .zip(store.get_ranges(&path, &ranges).await.unwrap())
    {
        assert_eq!(
            result.as_ref(),
            &payload[range.start as usize..range.end as usize]
        );
    }
}

#[tokio::test]
async fn overwrite_only_backends_support_regular_copy_and_multipart_writes() {
    for encrypted in [false, true] {
        let backend = OverwriteOnlyStore(InMemory::new());
        let store: Box<dyn ObjectStore> = if encrypted {
            Box::new(EncryptedStoreBuilder::with_secret(backend, 100, [0; 32]).build())
        } else {
            Box::new(MetaStoreBuilder::new(backend, 100).build())
        };
        let path = Path::from("overwrite-only");
        let first = store
            .put(&path, Bytes::from_static(b"first").into())
            .await
            .unwrap();
        store
            .put_opts(
                &path,
                Bytes::from_static(b"updated").into(),
                PutMode::Update(UpdateVersion {
                    e_tag: first.e_tag,
                    version: None,
                })
                .into(),
            )
            .await
            .unwrap();

        let copied = Path::from("overwrite-copy");
        store.copy(&path, &copied).await.unwrap();
        assert_eq!(
            store.get(&copied).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"updated")
        );

        let multipart = Path::from("overwrite-multipart");
        let mut upload = store.put_multipart(&multipart).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"multipart").into())
            .await
            .unwrap();
        upload.complete().await.unwrap();
        assert_eq!(
            store.get(&multipart).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"multipart")
        );
    }
}

#[tokio::test]
async fn abort_retries_backend_failures_and_cleans_materialized_generations() {
    for encrypted in [false, true] {
        // A definite backend abort failure must leave the operation retryable.
        let backend = InMemory::new();
        let (fault, handle) = FaultStore::wrap(backend.clone());
        let store: Box<dyn ObjectStore> = if encrypted {
            Box::new(EncryptedStoreBuilder::with_secret(fault, 100, [0; 32]).build())
        } else {
            Box::new(MetaStoreBuilder::new(fault, 100).build())
        };
        let path = Path::from("abort-retry");
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"part").into())
            .await
            .unwrap();
        handle.push_rule(FaultRule::fail_once(FaultOp::MultipartAbort, "gen/"));
        assert!(upload.abort().await.is_err());
        upload.abort().await.unwrap();
        use futures::TryStreamExt;
        assert!(
            backend
                .list(Some(&Path::from("gen")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );

        // Once the payload is materialized, abort must delete it rather than
        // invoking the already-completed backend uploader. A failed delete is
        // likewise retryable.
        let materialized = Path::from("abort-materialized");
        let mut upload = store.put_multipart(&materialized).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"complete payload").into())
            .await
            .unwrap();
        handle.push_rule(FaultRule::fail_once(FaultOp::Put, "meta/"));
        assert!(upload.complete().await.is_err());
        handle.push_rule(FaultRule::fail_once(FaultOp::Delete, "gen/"));
        assert!(upload.abort().await.is_err());
        upload.abort().await.unwrap();
        assert!(
            backend
                .list(Some(&Path::from("gen")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );

        // A lost response after metadata publication means the generation is
        // already committed. Abort must rediscover that fact and preserve it;
        // retrying complete resolves the uncertain result idempotently.
        let committed = Path::from("abort-after-commit");
        let mut upload = store.put_multipart(&committed).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"committed payload").into())
            .await
            .unwrap();
        handle.push_rule(FaultRule {
            op: FaultOp::Put,
            path_contains: Some("meta/abort-after-commit".into()),
            skip: 0,
            times: 1,
            kind: FaultKind::ErrorAfter,
        });
        assert!(upload.complete().await.is_err());
        assert!(upload.abort().await.is_err());
        assert_eq!(
            store.get(&committed).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"committed payload")
        );
        upload.complete().await.unwrap();
        assert!(upload.abort().await.is_err());
    }
}

#[tokio::test]
async fn failed_multipart_initialization_leaves_no_generation_object() {
    let backend = InMemory::new();
    let (fault, handle) = FaultStore::wrap(backend.clone());
    let store = MetaStoreBuilder::new(fault, 100).build();
    handle.push_rule(FaultRule::fail_once(FaultOp::MultipartStart, "gen/"));
    assert!(
        store
            .put_multipart(&Path::from("setup-failure"))
            .await
            .is_err()
    );
    use futures::TryStreamExt;
    assert!(
        backend
            .list(Some(&Path::from("gen")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn gc_rechecks_once_per_key_and_mark_budget_precedes_deletes() {
    use anda_object_store::GarbageCollectionOptions;
    let backend = InMemory::new();
    let controls = Arc::new(Controls::default());
    let store = MetaStoreBuilder::new(
        ProbeStore {
            inner: backend.clone(),
            controls: controls.clone(),
        },
        100,
    )
    .build();
    let path = Path::from("grouped");
    store
        .put(&path, Bytes::from_static(b"live").into())
        .await
        .unwrap();
    for n in 0..10 {
        backend
            .put(
                &Path::from(format!("gen/grouped/0000000000000001-{n:08x}")),
                Bytes::from_static(b"garbage").into(),
            )
            .await
            .unwrap();
    }
    assert!(
        store
            .collect_garbage_with_options(GarbageCollectionOptions {
                max_metadata_entries: 0,
                ..Default::default()
            })
            .await
            .is_err()
    );
    assert!(
        backend
            .head(&Path::from("gen/grouped/0000000000000001-00000000"))
            .await
            .is_ok()
    );
    controls.requests.lock().unwrap().clear();
    assert_eq!(store.collect_garbage().await.unwrap(), 10);
    assert_eq!(
        controls
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(op, _)| op == "get:meta/grouped")
            .count(),
        2
    );
}

fn wrapped_store<T: ObjectStore>(
    backend: T,
    encrypted: bool,
    capacity: u64,
) -> Box<dyn ObjectStore> {
    if encrypted {
        Box::new(EncryptedStoreBuilder::with_secret(backend, capacity, [0; 32]).build())
    } else {
        Box::new(MetaStoreBuilder::new(backend, capacity).build())
    }
}

#[tokio::test]
async fn create_with_wrong_key_preserves_existing_commit() {
    let backend = InMemory::new();
    let path = Path::from("wrong-key");
    let writer = wrapped_store(backend.clone(), true, 100);
    writer
        .put(&path, Bytes::from_static(b"original").into())
        .await
        .unwrap();
    drop(writer);
    let before = backend
        .get(&Path::from("meta/wrong-key"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let wrong = EncryptedStoreBuilder::with_secret(backend.clone(), 100, [1; 32])
        .with_strict_metadata_auth()
        .build();
    assert!(wrong.get(&path).await.is_err());
    let result = wrong
        .put_opts(
            &path,
            Bytes::from_static(b"replacement").into(),
            PutMode::Create.into(),
        )
        .await;
    assert!(matches!(result, Err(Error::AlreadyExists { .. })));
    assert_eq!(
        backend
            .get(&Path::from("meta/wrong-key"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        before
    );
    let reopened = wrapped_store(backend, true, 100);
    assert_eq!(
        reopened.get(&path).await.unwrap().bytes().await.unwrap(),
        "original"
    );
}

#[tokio::test]
async fn create_after_lowering_limits_preserves_existing_commit() {
    use anda_object_store::MetadataLimits;
    for encrypted in [false, true] {
        let backend = InMemory::new();
        let path = Path::from("lowered-limits");
        let writer = wrapped_store(backend.clone(), encrypted, 100);
        writer
            .put(&path, Bytes::from_static(b"original").into())
            .await
            .unwrap();
        drop(writer);
        let limits = MetadataLimits {
            max_object_size: 4,
            ..Default::default()
        };
        let smaller: Box<dyn ObjectStore> = if encrypted {
            Box::new(
                EncryptedStoreBuilder::with_secret(backend.clone(), 100, [0; 32])
                    .with_metadata_limits(limits)
                    .build(),
            )
        } else {
            Box::new(
                MetaStoreBuilder::new(backend.clone(), 100)
                    .with_metadata_limits(limits)
                    .build(),
            )
        };
        assert!(smaller.get(&path).await.is_err());
        assert!(matches!(
            smaller
                .put_opts(
                    &path,
                    Bytes::from_static(b"new").into(),
                    PutMode::Create.into()
                )
                .await,
            Err(Error::AlreadyExists { .. })
        ));
        let reopened = wrapped_store(backend, encrypted, 100);
        assert_eq!(
            reopened.get(&path).await.unwrap().bytes().await.unwrap(),
            "original"
        );
    }
}

#[tokio::test]
async fn multipart_retry_timestamp_tracks_first_successful_publication() {
    for encrypted in [false, true] {
        for after in [false, true] {
            let backend = InMemory::new();
            let (fault, handle) = FaultStore::wrap(backend.clone());
            let store = wrapped_store(fault, encrypted, 100);
            let path = Path::from("retry-timestamp");
            store
                .put(&path, Bytes::from_static(b"old").into())
                .await
                .unwrap();
            let mut upload = store.put_multipart(&path).await.unwrap();
            upload
                .put_part(Bytes::from_static(b"new").into())
                .await
                .unwrap();
            handle.push_rule(FaultRule {
                op: FaultOp::Put,
                path_contains: Some("meta/".into()),
                skip: 0,
                times: 1,
                kind: if after {
                    FaultKind::ErrorAfter
                } else {
                    FaultKind::Error
                },
            });
            assert!(upload.complete().await.is_err());
            let initial = store.head(&path).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            let observed = chrono::Utc::now();
            let content = store.get(&path).await.unwrap().bytes().await.unwrap();
            assert_eq!(content, if after { "new" } else { "old" });
            tokio::time::sleep(Duration::from_millis(10)).await;
            let committed = upload.complete().await.unwrap();
            // Read through a new context to also verify the timestamp's seal.
            let reopened = wrapped_store(backend, encrypted, 100);
            let head = reopened.head(&path).await.unwrap();
            assert_eq!(
                reopened.get(&path).await.unwrap().bytes().await.unwrap(),
                "new"
            );
            let conditional = reopened
                .get_opts(
                    &path,
                    GetOptions {
                        if_modified_since: Some(observed),
                        ..Default::default()
                    },
                )
                .await;
            if after {
                assert_eq!(head.last_modified, initial.last_modified);
                assert_eq!(head.e_tag, initial.e_tag);
                assert!(matches!(conditional, Err(Error::NotModified { .. })));
            } else {
                assert!(head.last_modified > observed);
                assert_eq!(conditional.unwrap().bytes().await.unwrap(), "new");
            }
            assert_eq!(upload.complete().await.unwrap().e_tag, committed.e_tag);
            assert_eq!(
                store.head(&path).await.unwrap().last_modified,
                head.last_modified
            );
        }
    }
}

#[tokio::test]
async fn tiny_single_ranges_release_crypto_batch_allocation() {
    for chunk_size in [1024, 256 * 1024] {
        let store = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32])
            .with_chunk_size(chunk_size)
            .build();
        let path = Path::from("tiny-range");
        let payload: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
        store.put(&path, payload.clone().into()).await.unwrap();
        for range in [
            100..101,
            chunk_size - 1..chunk_size + 1,
            chunk_size - 1..chunk_size,
        ] {
            let bytes = store.get_range(&path, range.clone()).await.unwrap();
            assert_eq!(
                bytes.as_ref(),
                &payload[range.start as usize..range.end as usize]
            );
            let capacity = bytes.try_into_mut().unwrap().capacity();
            assert!(
                capacity <= 2 * (range.end - range.start) as usize,
                "retained {capacity} bytes for {range:?}"
            );
        }
    }
}

#[tokio::test]
async fn existing_copy_and_rename_targets_do_not_allocate_generations() {
    use futures::TryStreamExt;
    for encrypted in [false, true] {
        let backend = InMemory::new();
        let (fault, handle) = FaultStore::wrap(backend.clone());
        let store = wrapped_store(fault, encrypted, 100);
        let from = Path::from("copy-source");
        let to = Path::from("copy-target");
        store.put(&from, vec![0; 1024 * 1024].into()).await.unwrap();
        store
            .put(&to, Bytes::from_static(b"exists").into())
            .await
            .unwrap();
        // Existence must also protect a corrupt target without attempting repair.
        for corrupt in [false, true] {
            if corrupt {
                backend
                    .put(
                        &Path::from("meta/copy-target"),
                        Bytes::from_static(b"\xffgarbage").into(),
                    )
                    .await
                    .unwrap();
            }
            handle.reset();
            for _ in 0..3 {
                assert!(matches!(
                    store.copy_if_not_exists(&from, &to).await,
                    Err(Error::AlreadyExists { .. })
                ));
                assert!(matches!(
                    store.rename_if_not_exists(&from, &to).await,
                    Err(Error::AlreadyExists { .. })
                ));
            }
            assert!(handle.mutation_log().is_empty());
            assert_eq!(
                backend
                    .list(Some(&Path::from("gen")))
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .len(),
                2
            );
            assert_eq!(
                store.get(&from).await.unwrap().bytes().await.unwrap().len(),
                1024 * 1024
            );
        }
    }
}

#[tokio::test]
async fn conditional_copy_still_arbitrates_after_target_preflight() {
    use anda_object_store::FaultGate;
    for encrypted in [false, true] {
        let (fault, handle) = FaultStore::wrap(InMemory::new());
        let store = wrapped_store(fault, encrypted, 100);
        let from = Path::from("race-source");
        let to = Path::from("race-target");
        store
            .put(&from, Bytes::from_static(b"source").into())
            .await
            .unwrap();
        let gate = FaultGate::new();
        handle.push_rule(FaultRule {
            op: FaultOp::Copy,
            path_contains: Some("gen/".into()),
            skip: 0,
            times: 1,
            kind: FaultKind::PauseBefore(gate.clone()),
        });
        let mut copy = Box::pin(store.copy_if_not_exists(&from, &to));
        tokio::select! { _ = &mut copy => panic!("copy should wait"), _ = gate.wait_entered() => () }
        store
            .put(&to, Bytes::from_static(b"winner").into())
            .await
            .unwrap();
        gate.release();
        assert!(matches!(copy.await, Err(Error::AlreadyExists { .. })));
        assert_eq!(
            store.get(&to).await.unwrap().bytes().await.unwrap(),
            "winner"
        );
    }
}

async fn listing_variant(store: &dyn ObjectStore, variant: u8) -> Vec<ObjectMeta> {
    use futures::TryStreamExt;
    let prefix = Path::from("listing");
    match variant {
        0 => store.list(Some(&prefix)).try_collect().await.unwrap(),
        1 => store
            .list_with_offset(Some(&prefix), &prefix)
            .try_collect()
            .await
            .unwrap(),
        _ => {
            store
                .list_with_delimiter(Some(&prefix))
                .await
                .unwrap()
                .objects
        }
    }
}

#[tokio::test]
async fn cold_listing_variants_fill_cache_and_respect_disabled_cache() {
    for encrypted in [false, true] {
        let backend = InMemory::new();
        let writer = wrapped_store(backend.clone(), encrypted, 100);
        for i in 0..3 {
            writer
                .put(
                    &Path::from(format!("listing/{i}")),
                    Bytes::from_static(b"data").into(),
                )
                .await
                .unwrap();
        }
        drop(writer);
        for capacity in [0, 100] {
            for variant in 0..3 {
                let controls = Arc::new(Controls::default());
                let store = wrapped_store(
                    ProbeStore {
                        inner: backend.clone(),
                        controls: controls.clone(),
                    },
                    encrypted,
                    capacity,
                );
                assert_eq!(listing_variant(store.as_ref(), variant).await.len(), 3);
                assert_eq!(listing_variant(store.as_ref(), variant).await.len(), 3);
                store.head(&Path::from("listing/0")).await.unwrap();
                let metadata_gets = controls
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(op, _)| op.starts_with("get:meta/"))
                    .count();
                assert_eq!(metadata_gets, if capacity == 0 { 7 } else { 3 });
            }
        }
    }
}

#[tokio::test]
async fn listing_cannot_cache_a_pointer_after_its_replacement() {
    use anda_object_store::{FaultGate, FaultOutcome};
    for encrypted in [false, true] {
        let backend = InMemory::new();
        let path = Path::from("listing/race");
        let seed = wrapped_store(backend.clone(), encrypted, 100);
        seed.put(&path, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        drop(seed);
        let (fault, handle) = FaultStore::wrap(backend);
        let store = wrapped_store(fault, encrypted, 100);
        let gate = FaultGate::new();
        handle.push_rule(FaultRule {
            op: FaultOp::Get,
            path_contains: Some("meta/listing/race".into()),
            skip: 0,
            times: 1,
            kind: FaultKind::PauseAfter(gate.clone()),
        });
        // Leave the old generation physically present so a stale cache cannot
        // be masked by the payload-NotFound refresh path.
        handle.push_rule(FaultRule::fail_once(FaultOp::Delete, "gen/"));
        let mut listing = Box::pin(listing_variant(store.as_ref(), 0));
        tokio::select! { _ = &mut listing => panic!("listing should wait"), _ = gate.wait_entered() => () }
        let mut write = Box::pin(store.put(&path, Bytes::from_static(b"new value").into()));
        assert!(futures::poll!(&mut write).is_pending());
        assert!(!handle.event_log().iter().any(
            |event| event.op == FaultOp::Put && event.outcome == FaultOutcome::BackendSucceeded
        ));
        gate.release();
        let (listed, result) = futures::join!(listing, write);
        assert_eq!(listed.len(), 1);
        let committed = result.unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "new value"
        );
        let latest = listing_variant(store.as_ref(), 0).await;
        assert_eq!(latest[0].e_tag, committed.e_tag);
        assert_eq!(latest[0].size, 9);
    }
}

#[tokio::test]
async fn cached_commits_deletes_and_heads_skip_metadata_reads() {
    for encrypted in [false, true] {
        let controls = Arc::new(Controls::default());
        let backend = ProbeStore {
            inner: InMemory::new(),
            controls: controls.clone(),
        };
        let store = wrapped_store(backend, encrypted, 100);
        let path = Path::from("hot");
        store
            .put(&path, Bytes::from_static(b"v1").into())
            .await
            .unwrap();
        controls.requests.lock().unwrap().clear();

        // The cached commit point is the committed truth for this writer.
        store
            .put(&path, Bytes::from_static(b"v2").into())
            .await
            .unwrap();
        assert_eq!(store.head(&path).await.unwrap().size, 2);
        store.delete(&path).await.unwrap();
        let requests: Vec<_> = controls
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|(op, _)| op.clone())
            .collect();
        assert!(
            requests.iter().all(|op| !op.starts_with("get:")),
            "{requests:?}"
        );
        assert!(matches!(
            store.head(&path).await,
            Err(Error::NotFound { .. })
        ));
    }
}
