//! Posting storage: a compact small-list path, then a position map for
//! constant-time membership and swap-removal. Only the ordered ids are persisted.
use rustc_hash::FxHashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{hash::Hash, ops::Deref};

const SMALL_LIMIT: usize = 8;

#[derive(Clone, Debug)]
pub(super) struct PostingList<PK> {
    ids: Vec<PK>,
    positions: Option<FxHashMap<PK, usize>>,
}
impl<PK> Default for PostingList<PK> {
    fn default() -> Self {
        Self {
            ids: Vec::new(),
            positions: None,
        }
    }
}
impl<PK> Deref for PostingList<PK> {
    type Target = Vec<PK>;
    fn deref(&self) -> &Self::Target {
        &self.ids
    }
}
impl<PK: Eq + Hash + Clone> PostingList<PK> {
    pub(super) fn contains(&self, id: &PK) -> bool {
        match &self.positions {
            Some(positions) => positions.contains_key(id),
            None => self.ids.contains(id),
        }
    }
    pub(super) fn push(&mut self, id: PK) -> bool {
        if let Some(positions) = &mut self.positions {
            // One hash lookup on the large-posting append path.
            let std::collections::hash_map::Entry::Vacant(entry) = positions.entry(id.clone())
            else {
                return false;
            };
            let index = self.ids.len();
            self.ids.push(id);
            entry.insert(index);
            return true;
        }
        if self.ids.contains(&id) {
            return false;
        }
        if self.ids.len() == SMALL_LIMIT {
            let positions = self
                .ids
                .iter()
                .cloned()
                .chain(std::iter::once(id.clone()))
                .enumerate()
                .map(|(i, id)| (id, i))
                .collect();
            self.ids.reserve(1);
            self.positions = Some(positions);
        }
        self.ids.push(id);
        true
    }
    pub(super) fn remove(&mut self, id: &PK) -> Option<PK> {
        let index = match &self.positions {
            Some(positions) => *positions.get(id)?,
            None => self.ids.iter().position(|candidate| candidate == id)?,
        };
        if let Some(positions) = &mut self.positions {
            positions.remove(id);
            if index + 1 != self.ids.len() {
                *positions
                    .get_mut(self.ids.last().expect("nonempty posting"))
                    .expect("position exists") = index;
            }
        }
        let removed = self.ids.swap_remove(index);
        // Hysteresis avoids repeatedly building/dropping the map at the cutoff.
        if self.ids.len() <= SMALL_LIMIT / 2 {
            self.positions = None;
        }
        Some(removed)
    }
    #[cfg(test)]
    pub(super) fn swap_remove_if(&mut self, predicate: impl FnMut(&PK) -> bool) -> Option<PK> {
        let index = self.ids.iter().position(predicate)?;
        let id = self.ids[index].clone();
        self.remove(&id)
    }
}
impl<PK: Eq + Hash + Clone> From<Vec<PK>> for PostingList<PK> {
    fn from(mut ids: Vec<PK>) -> Self {
        // Retain the input allocation, especially for singleton postings.
        if ids.len() <= SMALL_LIMIT {
            let mut unique = 0;
            for i in 0..ids.len() {
                if !ids[..unique].contains(&ids[i]) {
                    ids.swap(unique, i);
                    unique += 1;
                }
            }
            ids.truncate(unique);
            Self {
                ids,
                positions: None,
            }
        } else {
            let mut positions = FxHashMap::default();
            let mut next = 0;
            ids.retain(|id| {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    positions.entry(id.clone())
                {
                    entry.insert(next);
                    next += 1;
                    true
                } else {
                    false
                }
            });
            let positions = (ids.len() > SMALL_LIMIT).then_some(positions);
            Self { ids, positions }
        }
    }
}

impl<PK: Serialize> Serialize for PostingList<PK> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.ids.serialize(s)
    }
}
impl<'de, PK: Eq + Hash + Clone + Deserialize<'de>> Deserialize<'de> for PostingList<PK> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Vec::<PK>::deserialize(d).map(Self::from)
    }
}
