//! Shared conditional metadata publication and best-effort object retirement.
//! HNSW keeps its distinct node/ids/tombstone protocol.
use futures::StreamExt;
use parking_lot::RwLock;

use crate::{
    error::DBError,
    schema::BoxError,
    storage::{ObjectVersion, PutMode, Storage},
};

pub(super) async fn commit_metadata(
    storage: &Storage,
    path: &str,
    version: &RwLock<ObjectVersion>,
    data: Vec<u8>,
) -> Result<(), BoxError> {
    let expected = version.read().clone();
    let published = storage
        .put_bytes(path, data.into(), PutMode::Update(expected.into()))
        .await?;
    *version.write() = published;
    Ok(())
}

pub(super) async fn retire_objects(
    storage: &Storage,
    index: &str,
    paths: impl IntoIterator<Item = String>,
) {
    let mut retiring = futures::stream::iter(paths)
        .map(|path| async move {
            match storage.delete(&path).await {
                Ok(()) | Err(DBError::NotFound { .. }) => {}
                Err(err) => {
                    log::warn!("Index {index:?}: failed to retire obsolete object {path}: {err}")
                }
            }
        })
        .buffer_unordered(8);
    while retiring.next().await.is_some() {}
}
