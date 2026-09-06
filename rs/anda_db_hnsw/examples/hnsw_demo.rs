//! Single-writer demo: cross-platform atomic object replacement and recovery
//! of partial progress. Local filesystem writes request fsync durability where
//! the platform supports it.
//! Optional first argument: a fresh directory. Default: a new temporary directory.
use anda_db_hnsw::{BoxError, FlushOptions, FlushOutcome, HnswConfig, HnswIndex};
use anda_object_store::MetaStoreBuilder;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, local::LocalFileSystem,
    path::Path as ObjectPath,
};
use rand::{RngExt, SeedableRng};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

type ObjectVersions = Arc<Mutex<BTreeMap<String, UpdateVersion>>>;
type Store = Arc<dyn ObjectStore>;

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as u64
}

async fn put_object(
    store: Store,
    versions: ObjectVersions,
    path: String,
    bytes: Vec<u8>,
) -> Result<(), BoxError> {
    // LocalFileSystem stages and atomically publishes conditional object
    // writes on every supported platform, including Windows.
    let mode = versions
        .lock()
        .expect("version map")
        .get(&path)
        .cloned()
        .map(PutMode::Update)
        .unwrap_or(PutMode::Create);
    let result = store
        .put_opts(
            &ObjectPath::from(path.clone()),
            bytes.into(),
            PutOptions {
                mode,
                ..Default::default()
            },
        )
        .await?;
    versions.lock().expect("version map").insert(
        path,
        UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        },
    );
    Ok(())
}

async fn read_object(
    store: &dyn ObjectStore,
    path: impl Into<ObjectPath>,
) -> Result<Vec<u8>, BoxError> {
    Ok(store.get(&path.into()).await?.bytes().await?.to_vec())
}

async fn save_with_versions(
    index: &HnswIndex,
    store: Store,
    versions: ObjectVersions,
) -> Result<FlushOutcome, BoxError> {
    let node_store = store.clone();
    let ids_store = store.clone();
    let metadata_store = store;
    let node_versions = versions.clone();
    let ids_versions = versions.clone();
    let metadata_versions = versions;
    Ok(index
        .flush_with_options(
            unix_ms(),
            FlushOptions {
                node_concurrency: 8,
                ..Default::default()
            },
            move |id, bytes| {
                let store = node_store.clone();
                let versions = node_versions.clone();
                async move {
                    put_object(store, versions, format!("node_{id}.cbor"), bytes).await?;
                    Ok(true)
                }
            },
            move |bytes| put_object(ids_store, ids_versions, "ids.cbor".into(), bytes),
            move |bytes| {
                put_object(
                    metadata_store,
                    metadata_versions,
                    "metadata.cbor".into(),
                    bytes,
                )
            },
        )
        .await?)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    structured_logger::Builder::new().init();
    let dir = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!(
                "anda_hnsw_demo_{}_{}",
                std::process::id(),
                unix_ms()
            ))
        });
    tokio::fs::create_dir_all(&dir).await?;
    if std::fs::read_dir(&dir)?.next().transpose()?.is_some() {
        return Err("The demo requires a fresh directory; existing data was left untouched".into());
    }
    let local = LocalFileSystem::new_with_prefix(&dir)?.with_fsync(true);
    let store: Store = Arc::new(MetaStoreBuilder::new(local, 10_000).build());
    let versions = Arc::new(Mutex::new(BTreeMap::new()));
    const DIM: usize = 384;
    const N: u64 = 1000;
    let index = HnswIndex::try_new_seeded(
        "demo".into(),
        Some(HnswConfig {
            dimension: DIM,
            ..Default::default()
        }),
        42,
    )?;
    let mut random = rand::rngs::StdRng::seed_from_u64(42);
    let start = Instant::now();
    for id in 0..N {
        index.insert_f32(
            id,
            (0..DIM).map(|_| random.random::<f32>()).collect(),
            unix_ms(),
        )?;
    }
    println!("Inserted {N} vectors in {:?}", start.elapsed());
    assert_eq!(
        save_with_versions(&index, store.clone(), versions.clone()).await?,
        FlushOutcome::Committed
    );

    // Commit deletions before purging blobs, then save the cleared tombstones.
    for id in (0..N).step_by(10) {
        assert!(index.remove(id, unix_ms()));
    }
    save_with_versions(&index, store.clone(), versions.clone()).await?;
    let purge_store = store.clone();
    let purge_versions = versions.clone();
    index
        .purge_removed_nodes(async move |id| {
            let path = ObjectPath::from(format!("node_{id}.cbor"));
            match purge_store.delete(&path).await {
                Ok(()) => {
                    purge_versions
                        .lock()
                        .expect("version map")
                        .remove(path.as_ref());
                    Ok(true)
                }
                Err(object_store::Error::NotFound { .. }) => {
                    purge_versions
                        .lock()
                        .expect("version map")
                        .remove(path.as_ref());
                    Ok(true)
                }
                Err(error) => Err(error.into()),
            }
        })
        .await?;
    save_with_versions(&index, store.clone(), versions.clone()).await?;
    assert_eq!(
        save_with_versions(&index, store.clone(), versions).await?,
        FlushOutcome::NoChanges
    );

    let metadata = read_object(&store, "metadata.cbor").await?;
    let ids = read_object(&store, "ids.cbor").await?;
    let load_store = store.clone();
    let loaded = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async move |id| {
        let path = ObjectPath::from(format!("node_{id}.cbor"));
        match load_store.get(&path).await {
            Ok(result) => Ok(Some(result.bytes().await?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    })
    .await?;
    assert_eq!(loaded.len(), 900);
    assert!(!loaded.has_removed_nodes());
    let query: Vec<f32> = (0..DIM).map(|_| random.random::<f32>()).collect();
    let results = loaded.search_f32(&query, 10)?;
    assert!(results.iter().all(|(id, _)| id % 10 != 0));
    println!(
        "Reloaded {} vectors; query returned {} hits. Files: {}",
        loaded.len(),
        results.len(),
        dir.display()
    );
    Ok(())
}
