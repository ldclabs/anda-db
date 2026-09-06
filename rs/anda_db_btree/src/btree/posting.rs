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
        // Work on the position map out of place. If a user-defined Hash/Eq
        // implementation panics below, unwinding drops the map and leaves the
        // still-unchanged id vector as the authoritative linear fallback.
        if let Some(mut positions) = self.positions.take() {
            positions.remove(id);
            if index + 1 != self.ids.len() {
                *positions
                    .get_mut(self.ids.last().expect("nonempty posting"))
                    .expect("position exists") = index;
            }
            self.positions = Some(positions);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::Cell,
        hash::{Hash, Hasher},
    };

    thread_local! {
        static HASH_PANIC_AFTER: Cell<Option<usize>> = const { Cell::new(None) };
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Key(u8);

    impl Hash for Key {
        fn hash<H: Hasher>(&self, state: &mut H) {
            HASH_PANIC_AFTER.with(|countdown| {
                if let Some(remaining) = countdown.get() {
                    if remaining == 0 {
                        countdown.set(None);
                        panic!("intentional hash panic");
                    }
                    countdown.set(Some(remaining - 1));
                }
            });
            self.0.hash(state);
        }
    }

    #[test]
    fn hash_panic_during_remove_falls_back_to_the_id_vector() {
        let mut posting = PostingList::from((0..10).map(Key).collect::<Vec<_>>());
        HASH_PANIC_AFTER.with(|countdown| countdown.set(Some(2)));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            posting.remove(&Key(3));
        }));
        HASH_PANIC_AFTER.with(|countdown| countdown.set(None));

        assert!(result.is_err());
        assert!(posting.positions.is_none());
        assert_eq!(posting.ids, (0..10).map(Key).collect::<Vec<_>>());
        assert!(
            !posting.push(Key(3)),
            "the existing id must not be duplicated"
        );
        assert_eq!(posting.remove(&Key(3)), Some(Key(3)));
    }
}
