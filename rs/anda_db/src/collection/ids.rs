//! One lock protects both query ordering and the persisted bitmap. Only the
//! mutation methods below can change membership, and only a successful bitmap
//! PUT clears `dirty` while the collection's exclusive operation gate is held.
use super::*;
use croaring::{Bitmap, bitmap::BitmapCursor};
use std::{collections::btree_map, ops::RangeInclusive};

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
        self.range(range).remaining as usize
    }
    pub(super) fn range(&self, range: RangeInclusive<u64>) -> IdRange<'_> {
        let (lo, hi) = (*range.start(), *range.end());
        if lo > hi {
            return IdRange {
                buckets: self.bitmap.map.range(..0),
                front: None,
                back: None,
                lo,
                hi,
                remaining: 0,
            };
        }
        let before = if lo == 0 { 0 } else { self.bitmap.rank(lo - 1) };
        IdRange {
            buckets: self.bitmap.map.range(high(lo)..=high(hi)),
            front: None,
            back: None,
            lo,
            hi,
            remaining: self.bitmap.rank(hi) - before,
        }
    }
}

fn high(id: u64) -> u32 {
    (id >> 32) as u32
}

fn join(bucket: u32, low: u32) -> u64 {
    (u64::from(bucket) << 32) | u64::from(low)
}

type Bucket<'a> = (u32, &'a Bitmap, BitmapCursor<'a>);

/// Ordered bounded ID scans without a second tree containing every id. The
/// rank difference counts the range; per-bucket cursors walk it in O(1) per
/// step (a `select` per id re-scans every container). The persisted bitmap
/// format remains unchanged.
pub(super) struct IdRange<'a> {
    buckets: btree_map::Range<'a, u32, Bitmap>,
    front: Option<Bucket<'a>>,
    back: Option<Bucket<'a>>,
    lo: u64,
    hi: u64,
    remaining: u64,
}
impl Iterator for IdRange<'_> {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        while self.remaining > 0 {
            if let Some((bucket, _, cursor)) = &mut self.front
                && let Some(low) = cursor.current()
            {
                cursor.move_next();
                self.remaining -= 1;
                return Some(join(*bucket, low));
            }
            // Past the last unopened bucket, the rest sits in the back's.
            let (bucket, bitmap) = match self.buckets.next() {
                Some((bucket, bitmap)) => (*bucket, bitmap),
                None => match (&self.front, &self.back) {
                    (Some((front, ..)), Some((back, ..))) if front == back => return None,
                    (_, Some((back, bitmap, _))) => (*back, *bitmap),
                    (_, None) => return None,
                },
            };
            let mut cursor = bitmap.cursor();
            if bucket == high(self.lo) {
                cursor.reset_at_or_after(self.lo as u32);
            }
            self.front = Some((bucket, bitmap, cursor));
        }
        None
    }
}
impl DoubleEndedIterator for IdRange<'_> {
    fn next_back(&mut self) -> Option<u64> {
        while self.remaining > 0 {
            if let Some((bucket, _, cursor)) = &mut self.back
                && let Some(low) = cursor.current()
            {
                cursor.move_prev();
                self.remaining -= 1;
                return Some(join(*bucket, low));
            }
            let (bucket, bitmap) = match self.buckets.next_back() {
                Some((bucket, bitmap)) => (*bucket, bitmap),
                None => match (&self.back, &self.front) {
                    (Some((back, ..)), Some((front, ..))) if back == front => return None,
                    (_, Some((front, bitmap, _))) => (*front, *bitmap),
                    (_, None) => return None,
                },
            };
            let mut cursor = bitmap.cursor_to_last();
            if bucket == high(self.hi) {
                // Largest id at or below `hi`: seek past it, then step back.
                let last = self.hi as u32;
                cursor.reset_at_or_after(last);
                if cursor.current() != Some(last) {
                    cursor.move_prev();
                }
            }
            self.back = Some((bucket, bitmap, cursor));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[u64]) -> DocumentIds {
        let mut ids = DocumentIds::from_bitmap(Treemap::new());
        for &id in values {
            ids.insert(id);
        }
        ids
    }

    #[test]
    fn ranges_walk_like_an_ordered_set_across_buckets_and_ends() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..200 {
            // Dense, sparse and multi-bucket id sets, including the u64 edges.
            let mut values: Vec<u64> = (0..(next() % 300))
                .map(|_| match round % 3 {
                    0 => next() % 2_000,
                    1 => ((next() % 4) << 32) | (next() % 500),
                    _ => next(),
                })
                .collect();
            values.extend([0, u64::MAX].into_iter().filter(|_| round % 5 == 0));
            let set: BTreeSet<u64> = values.iter().copied().collect();
            let ids = ids(&values);
            for _ in 0..20 {
                let (a, b) = match round % 3 {
                    0 => (next() % 2_100, next() % 2_100),
                    1 => (
                        ((next() % 5) << 32) | (next() % 600),
                        ((next() % 5) << 32) | (next() % 600),
                    ),
                    _ => (next(), next()),
                };
                let (lo, hi) = if next() % 4 == 0 {
                    (b, a)
                } else {
                    (a.min(b), a.max(b))
                };
                let expected: Vec<u64> = if lo <= hi {
                    set.range(lo..=hi).copied().collect()
                } else {
                    Vec::new()
                };
                assert_eq!(ids.range_len(lo..=hi), expected.len());
                assert_eq!(ids.range(lo..=hi).collect::<Vec<_>>(), expected);
                assert_eq!(
                    ids.range(lo..=hi).rev().collect::<Vec<_>>(),
                    expected.iter().rev().copied().collect::<Vec<_>>()
                );
                // Alternate ends: each id comes out exactly once.
                let mut range = ids.range(lo..=hi);
                let (mut front, mut back) = (Vec::new(), Vec::new());
                loop {
                    let item = if next() % 2 == 0 {
                        range.next().map(|id| front.push(id))
                    } else {
                        range.next_back().map(|id| back.push(id))
                    };
                    if item.is_none() {
                        break;
                    }
                }
                assert!(range.next().is_none() && range.next_back().is_none());
                front.extend(back.into_iter().rev());
                assert_eq!(front, expected);
            }
        }
    }
}
