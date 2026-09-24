//! Shared conditional metadata publication, bucket loading and best-effort
//! object retirement. HNSW keeps its distinct node/ids/tombstone protocol.
use futures::StreamExt;
use parking_lot::RwLock;

use crate::{
    error::DBError,
    schema::BoxError,
    storage::{ObjectVersion, PutMode, Storage},
};

/// Object path of one manifest bucket generation under the index directory
/// `dir`. Generation `0` is the legacy (pre-manifest) un-suffixed object and
/// is only ever read; the manifest protocol writes suffixed generations.
pub(super) fn bucket_path(dir: &str, bucket_id: u32, generation: u64) -> String {
    if generation == 0 {
        format!("{dir}b_{bucket_id}.cbor")
    } else {
        format!("{dir}b_{bucket_id}_{generation}.cbor")
    }
}

/// Reads one internal index object for a loader; a missing object is `None`
/// so the index decides whether its manifest tolerates the gap.
pub(super) async fn load_object(
    storage: &Storage,
    path: &str,
) -> Result<Option<Vec<u8>>, BoxError> {
    match storage.fetch_internal_bytes(path).await {
        Ok((data, _)) => Ok(Some(data.into())),
        Err(DBError::NotFound { .. }) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub(super) async fn commit_metadata(
    storage: &Storage,
    path: &str,
    version: &RwLock<ObjectVersion>,
    data: Vec<u8>,
) -> Result<(), BoxError> {
    let expected = version.read().clone();
    let published = storage
        .put_internal_bytes(path, data.into(), PutMode::Update(expected.into()))
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
