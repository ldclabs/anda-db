//! One lock protects both query ordering and the persisted bitmap. Only the
//! mutation methods below can change membership, and only a successful bitmap
//! PUT clears `dirty` while the collection's exclusive operation gate is held.
use super::*;
use std::ops::RangeInclusive;

pub(super) struct DocumentIds {
    bitmap: Treemap,
    dirty: bool,
}

impl DocumentIds {
    pub(super) fn from_bitmap(bitmap: Treemap) -> Self {
        Self {
            bitmap,
            dirty: false,
        }
    }

    pub(super) fn insert(&mut self, id: DocumentId) -> bool {
        if self.bitmap.contains(id) {
            return false;
        }
        self.bitmap.add(id);
        self.dirty = true;
        true
    }

    pub(super) fn contains(&self, id: &DocumentId) -> bool {
        self.bitmap.contains(*id)
    }

    pub(super) fn remove(&mut self, id: &DocumentId) -> bool {
        if !self.bitmap.contains(*id) {
            return false;
        }
        self.bitmap.remove(*id);
        self.dirty = true;
        true
    }

    pub(super) fn snapshot_if_dirty(&self) -> Option<Vec<u8>> {
        if !self.dirty {
            return None;
        }
        // Keep the mutable bitmap in its append-friendly form.
        let mut bitmap = self.bitmap.clone();
        bitmap.run_optimize();
        Some(bitmap.serialize::<Portable>())
    }

    pub(super) fn mark_saved(&mut self) {
        self.dirty = false;
    }
}

impl DocumentIds {
    pub(super) fn len(&self) -> usize {
        self.bitmap.cardinality() as usize
    }
    pub(super) fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
    pub(super) fn last(&self) -> Option<u64> {
        self.bitmap.maximum()
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.bitmap.iter()
    }
    pub(super) fn range_len(&self, range: RangeInclusive<u64>) -> usize {
        let range = self.range(range);
        (range.end - range.start) as usize
    }
    pub(super) fn range(&self, range: RangeInclusive<u64>) -> IdRange<'_> {
        let lo = *range.start();
        let hi = *range.end();
        let start = if lo == 0 { 0 } else { self.bitmap.rank(lo - 1) };
        let end = if lo > hi { start } else { self.bitmap.rank(hi) };
        IdRange {
            bitmap: &self.bitmap,
            start,
            end,
        }
    }
}

/// Rank/select keeps reverse and bounded ID scans ordered without a second
/// tree containing every id. The persisted bitmap format remains unchanged.
pub(super) struct IdRange<'a> {
    bitmap: &'a Treemap,
    start: u64,
    end: u64,
}
impl Iterator for IdRange<'_> {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if self.start >= self.end {
            return None;
        }
        let id = self.bitmap.select(self.start);
        self.start += 1;
        id
    }
}
impl DoubleEndedIterator for IdRange<'_> {
    fn next_back(&mut self) -> Option<u64> {
        if self.start >= self.end {
            return None;
        }
        self.end -= 1;
        self.bitmap.select(self.end)
    }
}
