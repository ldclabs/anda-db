use super::*;
use object_store::memory::InMemory;

fn payload(data: &'static [u8]) -> PutPayload {
    Bytes::from_static(data).into()
}

#[tokio::test]
async fn forwards_when_no_faults() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    let path = Path::from("a/b");
    store.put(&path, payload(b"hello")).await.unwrap();
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got, Bytes::from_static(b"hello"));
    assert_eq!(handle.mutation_count(), 1);
    assert_eq!(
        handle.mutation_log(),
        vec![(FaultOp::Put, "a/b".to_string())]
    );
}

#[tokio::test]
async fn crash_after_mutations_powers_off_everything() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    handle.crash_after_mutations(2);

    store.put(&Path::from("1"), payload(b"x")).await.unwrap();
    store.put(&Path::from("2"), payload(b"x")).await.unwrap();
    // Third mutation hits the power failure.
    assert!(store.put(&Path::from("3"), payload(b"x")).await.is_err());
    // Reads are dead too until reset.
    assert!(store.get(&Path::from("1")).await.is_err());
    assert!(store.delete(&Path::from("1")).await.is_err());

    handle.reset();
    let got = store
        .get(&Path::from("1"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(got, Bytes::from_static(b"x"));
    // Object "3" never made it.
    assert!(matches!(
        store.get(&Path::from("3")).await,
        Err(Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn crash_after_is_relative_to_current_count() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    store.put(&Path::from("1"), payload(b"x")).await.unwrap();
    handle.crash_after_mutations(1);
    store.put(&Path::from("2"), payload(b"x")).await.unwrap();
    assert!(store.put(&Path::from("3"), payload(b"x")).await.is_err());
}

#[tokio::test]
async fn targeted_rule_fails_nth_matching_put() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    handle.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: Some("meta".to_string()),
        skip: 1,
        times: 1,
        kind: FaultKind::Error,
    });

    // First matching put passes (skip = 1).
    store
        .put(&Path::from("x/meta"), payload(b"a"))
        .await
        .unwrap();
    // Non-matching paths are unaffected.
    store
        .put(&Path::from("x/data"), payload(b"b"))
        .await
        .unwrap();
    // Second matching put fails once.
    assert!(
        store
            .put(&Path::from("y/meta"), payload(b"c"))
            .await
            .is_err()
    );
    // Rule exhausted: passes again.
    store
        .put(&Path::from("y/meta"), payload(b"d"))
        .await
        .unwrap();
}

#[tokio::test]
async fn torn_write_persists_prefix_and_reports_failure() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    handle.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: Some("torn".to_string()),
        skip: 0,
        times: 1,
        kind: FaultKind::TornWrite { keep_bytes: 3 },
    });

    let path = Path::from("torn");
    assert!(store.put(&path, payload(b"hello world")).await.is_err());
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got, Bytes::from_static(b"hel"));
}

#[tokio::test]
async fn delete_stream_and_list_respect_power_failure() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    store.put(&Path::from("a"), payload(b"1")).await.unwrap();
    handle.crash_after_mutations(0);

    assert!(store.delete(&Path::from("a")).await.is_err());
    let listed: Vec<_> = store.list(None).collect().await;
    assert!(listed.iter().any(|r| r.is_err()));
    assert!(store.list_with_delimiter(None).await.is_err());

    handle.reset();
    // Data survived the failed delete.
    assert!(store.get(&Path::from("a")).await.is_ok());
}

#[tokio::test]
async fn crash_rule_kind_powers_off() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    handle.push_rule(FaultRule {
        op: FaultOp::Put,
        path_contains: Some("ids".to_string()),
        skip: 0,
        times: 1,
        kind: FaultKind::Crash,
    });

    store.put(&Path::from("meta"), payload(b"m")).await.unwrap();
    assert!(
        store
            .put(&Path::from("col/ids"), payload(b"i"))
            .await
            .is_err()
    );
    // Everything is dead now.
    assert!(
        store
            .put(&Path::from("other"), payload(b"o"))
            .await
            .is_err()
    );
    assert!(store.get(&Path::from("meta")).await.is_err());
}

#[tokio::test]
async fn each_multipart_stage_accepts_error_and_crash_rules() {
    for op in [
        FaultOp::MultipartStart,
        FaultOp::MultipartPart,
        FaultOp::MultipartComplete,
        FaultOp::MultipartAbort,
    ] {
        for crash in [false, true] {
            let (store, handle) = FaultStore::wrap(InMemory::new());
            let anchor = Path::from("anchor");
            store.put(&anchor, payload(b"persistent")).await.unwrap();
            let path = Path::from("multipart");
            let mut upload = if op == FaultOp::MultipartStart {
                None
            } else {
                Some(store.put_multipart(&path).await.unwrap())
            };
            handle.push_rule(FaultRule {
                op,
                path_contains: None,
                skip: 0,
                times: 1,
                kind: if crash {
                    FaultKind::Crash
                } else {
                    FaultKind::Error
                },
            });
            let result = match op {
                FaultOp::MultipartStart => store.put_multipart(&path).await.map(|_| ()),
                FaultOp::MultipartPart => upload.as_mut().unwrap().put_part(payload(b"part")).await,
                FaultOp::MultipartComplete => upload.as_mut().unwrap().complete().await.map(|_| ()),
                FaultOp::MultipartAbort => upload.as_mut().unwrap().abort().await,
                _ => unreachable!(),
            };
            assert!(result.is_err());
            assert_eq!(store.get(&anchor).await.is_err(), crash);
            handle.reset();
            assert_eq!(
                store.get(&anchor).await.unwrap().bytes().await.unwrap(),
                Bytes::from_static(b"persistent")
            );
        }
    }
}
