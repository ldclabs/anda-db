//! Single-writer demo: atomic per-object writes and recovery of partial progress.
//! Optional first argument: a fresh directory. Default: a new temporary directory.
use anda_db_hnsw::{BoxError, FlushOptions, FlushOutcome, HnswConfig, HnswIndex};
use rand::{RngExt, SeedableRng};
use std::{
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncWriteExt;

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as u64
}

async fn atomic_write(path: PathBuf, bytes: Vec<u8>) -> Result<(), BoxError> {
    let temporary = path.with_extension("cbor.tmp");
    let mut file = tokio::fs::File::create(&temporary).await?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary, &path).await?;
    tokio::fs::File::open(path.parent().expect("object directory"))
        .await?
        .sync_all()
        .await?;
    Ok(())
}

async fn save(index: &HnswIndex, dir: &Path) -> Result<FlushOutcome, BoxError> {
    Ok(index
        .flush_with_options(
            unix_ms(),
            FlushOptions {
                node_concurrency: 8,
                ..Default::default()
            },
            |id, bytes| {
                let path = dir.join(format!("node_{id}.cbor"));
                async move {
                    atomic_write(path, bytes).await?;
                    Ok(true)
                }
            },
            |bytes| atomic_write(dir.join("ids.cbor"), bytes),
            |bytes| atomic_write(dir.join("metadata.cbor"), bytes),
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
    let metadata_path = dir.join("metadata.cbor");
    if metadata_path.try_exists()? {
        return Err("The demo requires a fresh directory; existing data was left untouched".into());
    }
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
    assert_eq!(save(&index, &dir).await?, FlushOutcome::Committed);

    // Commit deletions before purging blobs, then save the cleared tombstones.
    for id in (0..N).step_by(10) {
        assert!(index.remove(id, unix_ms()));
    }
    save(&index, &dir).await?;
    index
        .purge_removed_nodes(async |id| {
            match tokio::fs::remove_file(dir.join(format!("node_{id}.cbor"))).await {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
                Err(error) => Err(error.into()),
            }
        })
        .await?;
    save(&index, &dir).await?;
    assert_eq!(save(&index, &dir).await?, FlushOutcome::NoChanges);

    let metadata = tokio::fs::read(&metadata_path).await?;
    let ids = tokio::fs::read(dir.join("ids.cbor")).await?;
    let loaded =
        HnswIndex::load_all(
            metadata.as_slice(),
            ids.as_slice(),
            async |id| match tokio::fs::read(dir.join(format!("node_{id}.cbor"))).await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error.into()),
            },
        )
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
