//! Named runtime state. Persistence keeps the existing tuple/array wire format.
use super::*;

#[derive(Debug)]
pub(super) struct BucketState<FV> {
    pub(super) size: usize,
    pub(super) dirty: bool,
    // Bucket membership has no ordering contract; queries use the global
    // BTreeSet, and the bucket payload is serialized as a map.
    pub(super) fields: FxHashSet<FV>,
    pub(super) dirty_version: u64,
}
impl<FV> Default for BucketState<FV> {
    fn default() -> Self {
        Self::new(0, false, FxHashSet::default(), 0)
    }
}
impl<FV> BucketState<FV> {
    pub(super) fn new(size: usize, dirty: bool, fields: FxHashSet<FV>, dirty_version: u64) -> Self {
        Self {
            size,
            dirty,
            fields,
            dirty_version,
        }
    }
}

#[derive(Debug)]
pub(super) struct Posting<PK> {
    pub(super) bucket_id: u32,
    pub(super) version: u64,
    pub(super) docs: PostingList<PK>,
}
impl<PK: Eq + Hash + Clone> Posting<PK> {
    pub(super) fn new(bucket_id: u32, id: PK) -> Self {
        Self {
            bucket_id,
            version: 1,
            docs: vec![id].into(),
        }
    }
}
impl<PK> From<(u32, u64, PostingList<PK>)> for Posting<PK> {
    fn from((bucket_id, version, docs): (u32, u64, PostingList<PK>)) -> Self {
        Self {
            bucket_id,
            version,
            docs,
        }
    }
}
impl<PK: Serialize> Serialize for Posting<PK> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.bucket_id, self.version, &self.docs).serialize(serializer)
    }
}

pub(super) struct Removal<FV> {
    pub(super) field_value: FV,
    pub(super) bucket_id: u32,
    pub(super) doc_size: usize,
    pub(super) full_size: usize,
    pub(super) empty: bool,
}
impl<FV> Removal<FV> {
    pub(super) fn size_decrease(&self, entry_removed: bool) -> usize {
        if entry_removed {
            self.full_size
        } else {
            self.doc_size
        }
    }
}
