//! Crash-consistency and fault-injection tests for the whole database stack.
//!
//! The crash model is the standard one for object storage: every individual
//! put is atomic, but a sequence of puts/deletes can be interrupted anywhere
//! (process kill, power loss). `crash_at_every_mutation_point_is_recoverable`
//! replays a deterministic workload and simulates a power failure after every
//! possible mutation, then "reboots" and checks the durability contract:
//!
//! - the database always reopens (possibly via the documented
//!   `delete_collection` cleanup for a crash mid-creation) — never bricked;
//! - documents acknowledged by a successful `flush` and untouched afterwards
//!   are intact, byte for byte, and findable through their indexes;
//! - documents mutated after the last acknowledged flush are in one of the
//!   states the mutation history allows (old value, new value, or absent for
//!   unflushed inserts) — never a corrupted third state;
//! - the database accepts new writes after recovery.

use anda_db::{
    collection::{Collection, CollectionConfig},
    database::{AndaDB, DBConfig},
    error::DBError,
    index::HnswConfig,
    query::{Filter, Query, RangeQuery},
    schema::{AndaDBSchema, FieldEntry, FieldType, Fv, Schema, SchemaError, Vector, bf16},
    storage::StorageConfig,
    unix_ms,
};
use anda_object_store::{FaultOp, FaultRule, FaultStore};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, AndaDBSchema)]
struct CrashDoc {
    _id: u64,
    name: String,
    body: String,
    age: u64,
    embedding: Vector,
}

fn crash_doc(name: &str, age: u64) -> CrashDoc {
    CrashDoc {
        _id: 0,
        name: name.to_string(),
        body: format!("{name} body text"),
        age,
        embedding: [age as f32, 1.0, 2.0, 3.0]
            .into_iter()
            .map(bf16::from_f32)
            .collect(),
    }
}

fn db_config() -> DBConfig {
    DBConfig {
        name: "crashdb".to_string(),
        description: "crash recovery tests".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    }
}

async fn open_docs_collection(db: &AndaDB) -> Result<Arc<Collection>, DBError> {
    db.open_or_create_collection(
        CrashDoc::schema()?,
        CollectionConfig {
            name: "docs".to_string(),
            description: "crash recovery docs".to_string(),
        },
        async |collection| {
            collection.create_btree_index_nx(&["age"]).await?;
            collection.create_bm25_index_nx(&["body"]).await?;
            collection
                .create_hnsw_index_nx(
                    "embedding",
                    HnswConfig {
                        dimension: 4,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        },
    )
    .await
}

/// What the workload knows it has been promised at each point in time.
#[derive(Debug, Default, Clone)]
struct Progress {
    /// Full expected contents at the last `flush` that returned `Ok`.
    acked: BTreeMap<u64, CrashDoc>,
    /// Whether any workload-level flush was acknowledged.
    any_flush_acked: bool,
    /// Documents mutated since the last acknowledged flush, mapped to the set
    /// of states a crash may legally leave them in (`None` = absent).
    touched: BTreeMap<u64, Vec<Option<CrashDoc>>>,
}

impl Progress {
    fn touch(&mut self, current: &BTreeMap<u64, CrashDoc>, id: u64, new: Option<CrashDoc>) {
        self.touched
            .entry(id)
            .or_insert_with(|| vec![current.get(&id).cloned()])
            .push(new);
    }

    fn ack(&mut self, current: &BTreeMap<u64, CrashDoc>) {
        self.acked = current.clone();
        self.any_flush_acked = true;
        self.touched.clear();
    }
}

/// Deterministic workload. Aborts on the first error, like a crashed process.
async fn run_workload(store: Arc<dyn ObjectStore>, progress: &mut Progress) -> Result<(), DBError> {
    let db = AndaDB::connect(store, db_config()).await?;
    let collection = open_docs_collection(&db).await?;
    let mut current: BTreeMap<u64, CrashDoc> = BTreeMap::new();

    // Phase 1: initial inserts.
    for (name, age) in [("alpha", 10), ("beta", 20), ("gamma", 30)] {
        let mut doc = crash_doc(name, age);
        let id = collection.add_from(&doc).await?;
        doc._id = id;
        progress.touch(&current, id, Some(doc.clone()));
        current.insert(id, doc);
    }
    collection.flush(unix_ms()).await?;
    progress.ack(&current);

    // Phase 2: more inserts plus an update.
    for (name, age) in [("delta", 40), ("epsilon", 50)] {
        let mut doc = crash_doc(name, age);
        let id = collection.add_from(&doc).await?;
        doc._id = id;
        progress.touch(&current, id, Some(doc.clone()));
        current.insert(id, doc);
    }
    let beta_id = 2;
    collection
        .update(beta_id, BTreeMap::from([("age".to_string(), Fv::U64(21))]))
        .await?;
    let mut updated = current[&beta_id].clone();
    updated.age = 21;
    progress.touch(&current, beta_id, Some(updated.clone()));
    current.insert(beta_id, updated);
    collection.flush(unix_ms()).await?;
    progress.ack(&current);

    // Phase 3: a removal plus one more insert.
    let alpha_id = 1;
    collection.remove(alpha_id).await?;
    progress.touch(&current, alpha_id, None);
    current.remove(&alpha_id);
    let mut doc = crash_doc("zeta", 60);
    let id = collection.add_from(&doc).await?;
    doc._id = id;
    progress.touch(&current, id, Some(doc.clone()));
    current.insert(id, doc);
    collection.flush(unix_ms()).await?;
    progress.ack(&current);

    Ok(())
}

async fn ids_with_age(collection: &Collection, age: u64) -> Result<Vec<u64>, DBError> {
    collection
        .search_ids(Query {
            filter: Some(Filter::Field((
                "age".to_string(),
                RangeQuery::Eq(Fv::U64(age)),
            ))),
            ..Default::default()
        })
        .await
}

/// Reopens the database fault-free and checks the durability contract.
async fn verify_after_reboot(
    store: Arc<dyn ObjectStore>,
    progress: &Progress,
    crash_point: u64,
) -> Result<(), DBError> {
    let db = AndaDB::connect(store, db_config()).await?;
    let collection = match open_docs_collection(&db).await {
        Ok(collection) => collection,
        Err(DBError::AlreadyExists { .. }) if !progress.any_flush_acked => {
            // A crash inside collection creation can leave orphaned files that
            // are not registered in the database metadata. The documented
            // remedy is `delete_collection`, which cleans up unregistered
            // leftovers so the name can be created again.
            db.delete_collection("docs").await?;
            open_docs_collection(&db).await?
        }
        Err(err) => {
            panic!("crash point {crash_point}: database failed to reopen: {err:?}");
        }
    };

    // Acked documents untouched after the ack are fully durable and indexed.
    for (id, expected) in &progress.acked {
        if progress.touched.contains_key(id) {
            continue;
        }
        let got: CrashDoc = collection.get_as(*id).await.unwrap_or_else(|err| {
            panic!("crash point {crash_point}: acked doc {id} lost: {err:?}")
        });
        assert_eq!(
            &got, expected,
            "crash point {crash_point}: acked doc {id} corrupted"
        );
        let found = ids_with_age(&collection, expected.age).await?;
        assert!(
            found.contains(id),
            "crash point {crash_point}: acked doc {id} missing from btree index after repair"
        );
    }

    // Touched documents must be in one of the states the history allows.
    for (id, candidates) in &progress.touched {
        match collection.get_as::<CrashDoc>(*id).await {
            Ok(got) => assert!(
                candidates.iter().any(|c| c.as_ref() == Some(&got)),
                "crash point {crash_point}: doc {id} in impossible state {got:?}, allowed {candidates:?}"
            ),
            Err(DBError::NotFound { .. }) => assert!(
                candidates.iter().any(|c| c.is_none()),
                "crash point {crash_point}: doc {id} absent but absence not allowed"
            ),
            Err(err) => {
                panic!("crash point {crash_point}: doc {id} unreadable (corrupt?): {err:?}")
            }
        }
    }

    // The database must stay writable after recovery.
    let sentinel = crash_doc("sentinel", 999);
    let sid = collection.add_from(&sentinel).await?;
    collection.flush(unix_ms()).await?;
    let got: CrashDoc = collection.get_as(sid).await?;
    assert_eq!(got.name, "sentinel");
    let found = ids_with_age(&collection, 999).await?;
    assert!(
        found.contains(&sid),
        "crash point {crash_point}: post-recovery write not indexed"
    );

    db.close().await
}

#[tokio::test]
async fn crash_at_every_mutation_point_is_recoverable() {
    // Reference run: count the mutations of a clean workload.
    let (store, handle) = FaultStore::wrap(InMemory::new());
    let store = Arc::new(store);
    let mut progress = Progress::default();
    run_workload(store.clone(), &mut progress)
        .await
        .expect("clean workload run failed");
    let total = handle.mutation_count();
    assert!(total > 10, "workload too small to be meaningful: {total}");

    let mut crashed = 0u64;
    for crash_point in 0..total {
        let (store, handle) = FaultStore::wrap(InMemory::new());
        let store = Arc::new(store);
        handle.crash_after_mutations(crash_point);

        let mut progress = Progress::default();
        let result = run_workload(store.clone(), &mut progress).await;
        // The storage-metadata writer is rate-limited by wall-clock
        // milliseconds, so a rerun can issue slightly fewer mutations than the
        // reference run and a late crash point may never fire. That run then
        // completes cleanly and verification degenerates to a final-state
        // check, which is fine — but most crash points must actually crash.
        if result.is_err() {
            crashed += 1;
        }

        // "Reboot": clear faults, keep the data the crash left behind.
        handle.reset();
        verify_after_reboot(store, &progress, crash_point)
            .await
            .unwrap_or_else(|err| {
                panic!("crash point {crash_point}: verification failed: {err:?}")
            });
    }
    assert!(
        crashed >= total.saturating_sub(8),
        "only {crashed} of {total} crash points fired; the fault injection is not working"
    );
}

/// Guards the flush invariant: `save_extension` persists collection metadata
/// immediately (`flush_metadata`), but must NOT advance `last_saved_version`,
/// otherwise the next `flush` would skip persisting the document id bitmap.
#[tokio::test]
async fn flush_after_save_extension_still_persists_ids() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    let store = Arc::new(store);
    let db = AndaDB::connect(store, db_config()).await.unwrap();
    let collection = open_docs_collection(&db).await.unwrap();

    collection.add_from(&crash_doc("first", 1)).await.unwrap();
    collection.flush(unix_ms()).await.unwrap();

    // Immediate metadata persistence outside the flush path.
    collection
        .save_extension("checkpoint".to_string(), Fv::U64(42))
        .await
        .unwrap();
    collection.add_from(&crash_doc("second", 2)).await.unwrap();

    let ids_puts_before = handle
        .mutation_log()
        .iter()
        .filter(|(op, path)| *op == FaultOp::Put && path.ends_with("/ids.cbor"))
        .count();
    collection.flush(unix_ms()).await.unwrap();
    let ids_puts_after = handle
        .mutation_log()
        .iter()
        .filter(|(op, path)| *op == FaultOp::Put && path.ends_with("/ids.cbor"))
        .count();

    assert!(
        ids_puts_after > ids_puts_before,
        "flush after save_extension must persist ids.cbor; \
         flush_metadata advanced last_saved_version and broke the flush contract"
    );
}

/// Transient read failures while opening must surface as clean errors and a
/// plain retry must succeed with all data intact.
#[tokio::test]
async fn read_fault_during_open_fails_cleanly_then_recovers() {
    let (store, handle) = FaultStore::wrap(InMemory::new());
    let store = Arc::new(store);

    {
        let db = AndaDB::connect(store.clone(), db_config()).await.unwrap();
        let collection = open_docs_collection(&db).await.unwrap();
        collection.add_from(&crash_doc("alpha", 10)).await.unwrap();
        db.close().await.unwrap();
    }

    // Fail the next read of the database metadata object. The injected error
    // is not NotFound, so `connect` must propagate it instead of silently
    // creating a fresh database over the existing one.
    handle.reset();
    handle.push_rule(FaultRule::fail_once(FaultOp::Get, "db_meta.cbor"));
    assert!(AndaDB::connect(store.clone(), db_config()).await.is_err());

    // And fail the collection metadata read on the next attempt.
    handle.push_rule(FaultRule::fail_once(FaultOp::Get, "docs/meta.cbor"));
    let db = AndaDB::connect(store.clone(), db_config()).await.unwrap();
    assert!(open_docs_collection(&db).await.is_err());

    // Plain retry succeeds with the data intact.
    let collection = open_docs_collection(&db).await.unwrap();
    let got: CrashDoc = collection.get_as(1).await.unwrap();
    assert_eq!(got.name, "alpha");
}

/// Corrupted objects (bit rot, truncated files from non-atomic backends) must
/// produce errors, never panics or silently wrong data.
#[tokio::test]
async fn corrupted_objects_error_not_panic() {
    let (store, _handle) = FaultStore::wrap(InMemory::new());
    let store = Arc::new(store);

    {
        let db = AndaDB::connect(store.clone(), db_config()).await.unwrap();
        let collection = open_docs_collection(&db).await.unwrap();
        collection.add_from(&crash_doc("alpha", 10)).await.unwrap();
        collection.add_from(&crash_doc("beta", 20)).await.unwrap();
        db.close().await.unwrap();
    }

    // Corrupt one document object.
    store
        .inner()
        .put(
            &Path::from("crashdb/docs/data/1.cbor"),
            bytes::Bytes::from_static(b"this is not cbor").into(),
        )
        .await
        .unwrap();

    let db = AndaDB::connect(store.clone(), db_config()).await.unwrap();
    let collection = db
        .open_collection("docs".to_string(), async |_| Ok(()))
        .await
        .unwrap();
    assert!(matches!(
        collection.get(1).await,
        Err(DBError::Serialization { .. })
    ));
    // Healthy neighbours stay readable.
    let got: CrashDoc = collection.get_as(2).await.unwrap();
    assert_eq!(got.name, "beta");
    drop(collection);
    drop(db);

    // Corrupt the collection metadata object: opening the collection must be
    // a clean error, and the database itself must still open.
    store
        .inner()
        .put(
            &Path::from("crashdb/docs/meta.cbor"),
            bytes::Bytes::from_static(b"garbage metadata").into(),
        )
        .await
        .unwrap();
    let db = AndaDB::connect(store.clone(), db_config()).await.unwrap();
    assert!(matches!(
        db.open_collection("docs".to_string(), async |_| Ok(()))
            .await,
        Err(DBError::Serialization { .. })
    ));
}
