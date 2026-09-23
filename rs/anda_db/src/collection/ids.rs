//! One lock protects both query ordering and the persisted bitmap. Only the
//! mutation methods below can change membership, and only a successful bitmap
//! PUT clears `dirty` while the collection's exclusive operation gate is held.
use super::*;
use std::ops::Deref;

pub(super) struct DocumentIds {
    ordered: BTreeSet<DocumentId>,
    bitmap: Treemap,
    dirty: bool,
}

impl DocumentIds {
    pub(super) fn from_bitmap(bitmap: Treemap) -> Self {
        Self {
            ordered: bitmap.iter().collect(),
            bitmap,
            dirty: false,
        }
    }

    pub(super) fn insert(&mut self, id: DocumentId) -> bool {
        if !self.ordered.insert(id) {
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
        if !self.ordered.remove(id) {
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

// Read-only access retains range/iterator ergonomics without exposing a way to
// mutate the ordered representation separately from its bitmap or dirty flag.
impl Deref for DocumentIds {
    type Target = BTreeSet<DocumentId>;
    fn deref(&self) -> &Self::Target {
        &self.ordered
    }
}
