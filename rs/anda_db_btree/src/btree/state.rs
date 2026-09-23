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
    pub(super) docs: PostingList<PK>,
}
impl<PK: Eq + Hash + Clone> Posting<PK> {
    pub(super) fn new(bucket_id: u32, id: PK) -> Self {
        Self {
            bucket_id,
            docs: vec![id].into(),
        }
    }
}
// The persisted triple still carries the per-posting update counter that
// earlier releases kept but never read. It is ignored on load and written as
// a constant, so older releases can still decode new buckets.
impl<PK> From<StoredPosting<PK>> for Posting<PK> {
    fn from((bucket_id, _, docs): StoredPosting<PK>) -> Self {
        Self { bucket_id, docs }
    }
}
impl<PK: Serialize> Serialize for Posting<PK> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        stored_posting(self.bucket_id, &self.docs).serialize(serializer)
    }
}

/// The persisted `(bucket_id, legacy counter, doc_ids)` triple.
pub(super) fn stored_posting<D: ?Sized>(bucket_id: u32, docs: &D) -> (u32, u64, &D) {
    (bucket_id, 0, docs)
}

pub(super) struct Removal<FV> {
    pub(super) field_value: FV,
    pub(super) bucket_id: u32,
    pub(super) size_decrease: usize,
    pub(super) entry_removed: bool,
}
