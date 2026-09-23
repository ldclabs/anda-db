use super::*;
use std::borrow::Cow;

#[test]
fn test_unique_vec_new() {
    let uv: UniqueVec<i32> = UniqueVec::new();
    assert_eq!(uv.len(), 0);
    assert!(uv.is_empty());
}

#[test]
fn test_unique_vec_with_capacity() {
    let uv: UniqueVec<i32> = UniqueVec::with_capacity(10);
    assert_eq!(uv.len(), 0);
    assert_eq!(uv.capacity(), 10);
}

#[test]
fn test_unique_vec_from_vec() {
    let vec = vec![1, 2, 2, 3, 2, 1];
    let uv = UniqueVec::from(vec);
    assert_eq!(uv.len(), 3);
    assert!(uv.contains(&1));
    assert!(uv.contains(&2));
    assert!(uv.contains(&3));
}

#[test]
fn test_unique_vec_from_iterator() {
    let uv: UniqueVec<i32> = [2, 2, 1, 3, 2, 1].iter().cloned().collect();
    assert_eq!(uv.len(), 3);
    assert!(uv.contains(&2));
    assert!(uv.contains(&1));
    assert!(uv.contains(&3));
}

#[test]
fn test_unique_vec_push() {
    let mut uv = UniqueVec::new();

    // Push new items
    assert!(uv.push(1));
    assert!(uv.push(2));
    assert!(uv.push(3));
    assert_eq!(uv.len(), 3);

    // Push duplicate items
    assert!(!uv.push(1));
    assert!(!uv.push(2));
    assert_eq!(uv.len(), 3);

    // Verify order is maintained
    assert_eq!(uv.as_ref(), &[1, 2, 3]);
}

#[test]
fn test_unique_vec_extend() {
    let mut uv = UniqueVec::from(vec![1, 2, 3]);

    // Extend with mix of new and existing items
    uv.extend(vec![3, 4, 5, 2, 6]);

    assert_eq!(uv.len(), 6);
    assert_eq!(uv.as_ref(), &[1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_unique_vec_retain() {
    let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

    // Retain only even numbers
    uv.retain(|&x| x % 2 == 0);

    assert_eq!(uv.len(), 2);
    assert_eq!(uv.as_ref(), &[2, 4]);
    assert!(uv.contains(&2));
    assert!(uv.contains(&4));
    assert!(!uv.contains(&1));
    assert!(!uv.contains(&3));
    assert!(!uv.contains(&5));
}

#[test]
fn test_unique_vec_retain_keeps_set_consistent_after_panic() {
    let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        uv.retain(|&x| {
            if x == 3 {
                panic!("intentional retain panic");
            }

            x % 2 == 0
        });
    }));

    assert!(result.is_err());
    for value in 1..=5 {
        assert_eq!(uv.contains(&value), uv.as_ref().contains(&value));
    }
}

#[test]
fn test_unique_vec_retain_without_removal_keeps_set_consistent() {
    let mut uv = UniqueVec::from(vec![1, 2, 3]);

    uv.retain(|_| true);

    assert_eq!(uv.as_ref(), &[1, 2, 3]);
    // The set must still reject duplicates and accept new items.
    assert!(!uv.push(2));
    assert!(uv.push(4));
    assert_eq!(uv.as_ref(), &[1, 2, 3, 4]);
}

#[test]
fn test_unique_vec_intersect_with_superset_and_disjoint() {
    let mut uv = UniqueVec::from(vec![1, 2, 3]);
    let superset = UniqueVec::from(vec![1, 2, 3, 4, 5]);

    // Intersecting with a superset removes nothing.
    uv.intersect_with(&superset);
    assert_eq!(uv.as_ref(), &[1, 2, 3]);
    assert!(!uv.push(3));

    // Intersecting with a disjoint set removes everything; removed items
    // must be insertable again afterwards.
    let disjoint = UniqueVec::from(vec![7, 8]);
    uv.intersect_with(&disjoint);
    assert!(uv.is_empty());
    assert!(!uv.contains(&1));
    assert!(uv.push(1));
}

#[test]
fn test_unique_vec_remove() {
    let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

    let removed = uv.remove(2); // Remove element at index 2 (value 3)
    assert_eq!(removed, 3);
    assert_eq!(uv.len(), 4);
    assert_eq!(uv.as_ref(), &[1, 2, 4, 5]);
    assert!(!uv.contains(&3));
}

#[test]
#[should_panic]
fn test_unique_vec_remove_out_of_bounds() {
    let mut uv = UniqueVec::from(vec![1, 2, 3]);
    uv.remove(5); // Should panic
}

#[test]
fn test_unique_vec_remove_if() {
    let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

    // Remove first even number
    let removed = uv.remove_if(|&x| x % 2 == 0);
    assert_eq!(removed, Some(2));
    assert_eq!(uv.len(), 4);
    assert_eq!(uv.as_ref(), &[1, 3, 4, 5]);
    assert!(!uv.contains(&2));

    // Try to remove non-existent condition
    let removed = uv.remove_if(|&x| x > 10);
    assert_eq!(removed, None);
    assert_eq!(uv.len(), 4);
}

#[test]
fn test_unique_vec_swap_remove_if() {
    let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

    // Remove first even number (swap with last)
    let removed = uv.swap_remove_if(|&x| x % 2 == 0);
    assert_eq!(removed, Some(2));
    assert_eq!(uv.len(), 4);
    // After swap_remove, the last element (5) should be in position of removed element
    assert_eq!(uv.as_ref(), &[1, 5, 3, 4]);
    assert!(!uv.contains(&2));
}

#[test]
fn test_unique_vec_contains() {
    let uv = UniqueVec::from(vec![1, 2, 3]);

    assert!(uv.contains(&1));
    assert!(uv.contains(&2));
    assert!(uv.contains(&3));
    assert!(!uv.contains(&4));
}

#[test]
fn test_unique_vec_intersect_with() {
    let mut uv1 = UniqueVec::from(vec![1, 2, 3, 4, 5]);
    let uv2 = UniqueVec::from(vec![3, 4, 5, 6, 7]);

    uv1.intersect_with(&uv2);

    assert_eq!(uv1.len(), 3);
    assert!(uv1.contains(&3));
    assert!(uv1.contains(&4));
    assert!(uv1.contains(&5));
    assert!(!uv1.contains(&1));
    assert!(!uv1.contains(&2));
}

#[test]
fn test_unique_vec_to_vec() {
    let uv = UniqueVec::from(vec![1, 2, 2, 3]);
    let vec = uv.to_vec();
    assert_eq!(vec, vec![1, 2, 3]);
}

#[test]
fn test_unique_vec_to_set() {
    let uv = UniqueVec::from(vec![1, 2, 3]);
    let set = uv.to_set();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1));
    assert!(set.contains(&2));
    assert!(set.contains(&3));
}

#[test]
fn test_unique_vec_as_ref() {
    let uv = UniqueVec::from(vec![1, 2, 3]);
    let slice: &[i32] = uv.as_ref();
    assert_eq!(slice, &[1, 2, 3]);
}

#[test]
fn test_unique_vec_deref() {
    let uv = UniqueVec::from(vec![1, 2, 3]);
    // Test deref by calling Vec methods directly
    assert_eq!(uv.len(), 3);
    assert_eq!(uv[0], 1);
    assert_eq!(uv[1], 2);
    assert_eq!(uv[2], 3);
}

#[test]
fn test_unique_vec_into_vec() {
    let uv = UniqueVec::from(vec![1, 2, 3]);
    let vec: Vec<i32> = uv.into();
    assert_eq!(vec, vec![1, 2, 3]);
}

#[test]
fn test_unique_vec_serialize_deserialize() {
    let uv = UniqueVec::from(vec![1, 2, 2, 3, 2, 1]); // Duplicates should be removed

    // Serialize
    let json = serde_json::to_string(&uv).unwrap();
    assert_eq!(json, "[1,2,3]");

    // Deserialize
    let deserialized: UniqueVec<i32> = serde_json::from_str("[1,3,2,3,3,2,1]").unwrap();
    assert_eq!(deserialized.len(), 3);
    assert_eq!(deserialized.as_ref(), &[1, 3, 2]);
}

#[test]
fn test_unique_vec_deserialize_borrowed_values() {
    fn deserialize_unique_vec_cow<'a>(json: &'a str) -> UniqueVec<Cow<'a, str>> {
        serde_json::from_str(json).unwrap()
    }

    let json = String::from(r#"["alpha","beta","alpha"]"#);
    let deserialized = deserialize_unique_vec_cow(&json);

    assert_eq!(
        deserialized.as_ref(),
        &[Cow::Borrowed("alpha"), Cow::Borrowed("beta")]
    );
}

#[test]
fn test_unique_vec_clone() {
    let uv1 = UniqueVec::from(vec![1, 2, 3]);
    let uv2 = uv1.clone();

    assert_eq!(uv1.len(), uv2.len());
    assert_eq!(uv1.as_ref(), uv2.as_ref());
}

#[test]
fn test_unique_vec_edge_cases() {
    // Test with empty vector
    let uv = UniqueVec::from(vec![] as Vec<i32>);
    assert_eq!(uv.len(), 0);
    assert!(uv.is_empty());

    // Test with single element
    let mut uv = UniqueVec::from(vec![42]);
    assert_eq!(uv.len(), 1);
    assert!(uv.contains(&42));

    // Test removing the only element
    let removed = uv.remove(0);
    assert_eq!(removed, 42);
    assert_eq!(uv.len(), 0);
    assert!(!uv.contains(&42));
}

#[test]
fn test_unique_vec_string_type() {
    let mut uv = UniqueVec::new();

    uv.push("hello".to_string());
    uv.push("world".to_string());
    uv.push("hello".to_string()); // Duplicate

    assert_eq!(uv.len(), 2);
    assert!(uv.contains("hello"));
    assert!(uv.contains("world"));
}

#[test]
fn test_unique_vec_extend_panicking_iterator_keeps_set_consistent() {
    let mut uv = UniqueVec::from(vec![1, 2]);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        uv.extend((3..10).inspect(|&x| {
            if x == 5 {
                panic!("intentional extend panic");
            }
        }));
    }));
    assert!(result.is_err());

    // Elements yielded before the panic are applied; set and vec agree.
    assert_eq!(uv.as_ref(), &[1, 2, 3, 4]);
    for value in 1..10 {
        assert_eq!(uv.contains(&value), uv.as_ref().contains(&value));
    }
    // The set still rejects duplicates and accepts new items.
    assert!(!uv.push(4));
    assert!(uv.push(5));
    assert_eq!(uv.as_ref(), &[1, 2, 3, 4, 5]);
}

#[test]
fn test_unique_vec_cbor_round_trip_and_dedup() {
    // Round-trip through CBOR (the workspace's on-disk format).
    let uv = UniqueVec::from(vec![1u64, 2, 3]);
    let mut buf = Vec::new();
    cbor2::to_writer(&uv, &mut buf).unwrap();
    let decoded: UniqueVec<u64> = cbor2::from_reader(&buf[..]).unwrap();
    assert_eq!(decoded, uv);

    // CBOR input containing duplicates (e.g. a corrupted or crash-window
    // bucket file in anda_db_btree) is deduplicated on load, preserving
    // first-occurrence order.
    let mut buf = Vec::new();
    cbor2::to_writer(&vec![5u64, 1, 5, 2, 1], &mut buf).unwrap();
    let decoded: UniqueVec<u64> = cbor2::from_reader(&buf[..]).unwrap();
    assert_eq!(decoded.as_ref(), &[5, 1, 2]);
    assert!(decoded.contains(&2));
    assert!(!decoded.contains(&9));
}

#[test]
fn test_unique_vec_partial_eq() {
    let a = UniqueVec::from(vec![1, 2, 3]);
    let b = UniqueVec::from(vec![1, 2, 2, 3]);
    let c = UniqueVec::from(vec![3, 2, 1]);
    assert_eq!(a, b);
    assert_ne!(a, c);
}

use std::cell::Cell;
use std::hash::{Hash, Hasher};

thread_local! {
    /// Remaining `Hash::hash` calls before the next one panics.
    /// `None` disables panicking.
    static HASH_PANIC_COUNTDOWN: Cell<Option<usize>> = const { Cell::new(None) };
    /// When `true`, the next `Clone::clone` call panics.
    static CLONE_PANIC: Cell<bool> = const { Cell::new(false) };
}

/// Element type whose `Hash` / `Clone` impls can be armed to panic,
/// simulating adversarial or buggy user types.
#[derive(Debug, PartialEq, Eq)]
struct Evil(u32);

impl Hash for Evil {
    fn hash<H: Hasher>(&self, state: &mut H) {
        HASH_PANIC_COUNTDOWN.with(|c| {
            if let Some(n) = c.get() {
                if n == 0 {
                    c.set(None);
                    panic!("intentional Hash panic");
                }
                c.set(Some(n - 1));
            }
        });
        self.0.hash(state);
    }
}

impl Clone for Evil {
    fn clone(&self) -> Self {
        CLONE_PANIC.with(|c| {
            if c.get() {
                c.set(false);
                panic!("intentional Clone panic");
            }
        });
        Evil(self.0)
    }
}

fn assert_evil_invariant(uv: &UniqueVec<Evil>, universe: std::ops::RangeInclusive<u32>) {
    // set and vec must agree exactly, and vec must have no duplicates.
    for value in universe {
        let item = Evil(value);
        assert_eq!(
            uv.contains(&item),
            uv.as_ref().contains(&item),
            "set/vec diverged for {value}"
        );
        assert!(
            uv.as_ref().iter().filter(|x| **x == item).count() <= 1,
            "duplicate element {value} in vec"
        );
    }
}

#[test]
fn test_unique_vec_push_hash_panic_keeps_invariant() {
    let mut uv: UniqueVec<Evil> = UniqueVec::new();
    assert!(uv.push(Evil(1)));
    assert!(uv.push(Evil(2)));

    // `push` hashes twice: once in `contains`, once in `set.insert`.
    // Arm the panic for the second hash so `vec.push` has already
    // succeeded when `set.insert` unwinds — the historical window where
    // vec ⊋ set let a later push insert a duplicate.
    HASH_PANIC_COUNTDOWN.with(|c| c.set(Some(1)));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        uv.push(Evil(3));
    }));
    HASH_PANIC_COUNTDOWN.with(|c| c.set(None));
    assert!(result.is_err());

    // The drop guard must have rolled the vector back.
    assert_eq!(uv.as_ref(), &[Evil(1), Evil(2)]);
    assert_evil_invariant(&uv, 1..=3);

    // Re-pushing the same element must add it exactly once.
    assert!(uv.push(Evil(3)));
    assert!(!uv.push(Evil(3)));
    assert_eq!(uv.as_ref(), &[Evil(1), Evil(2), Evil(3)]);
    assert_evil_invariant(&uv, 1..=3);
}

#[test]
fn test_unique_vec_push_clone_panic_keeps_invariant() {
    let mut uv: UniqueVec<Evil> = UniqueVec::new();
    assert!(uv.push(Evil(1)));

    CLONE_PANIC.with(|c| c.set(true));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        uv.push(Evil(2));
    }));
    CLONE_PANIC.with(|c| c.set(false));
    assert!(result.is_err());

    assert_eq!(uv.as_ref(), &[Evil(1)]);
    assert_evil_invariant(&uv, 1..=2);
    assert!(uv.push(Evil(2)));
    assert_evil_invariant(&uv, 1..=2);
}

#[test]
fn test_unique_vec_remove_hash_panic_keeps_invariant() {
    let mut uv: UniqueVec<Evil> = UniqueVec::new();
    for v in 1..=3 {
        assert!(uv.push(Evil(v)));
    }

    // `remove` / `remove_if` / `swap_remove_if` hash once (`set.remove`).
    // A panic there must leave both containers untouched instead of the
    // historical set ⊋ vec state where the element could never be
    // re-inserted.
    HASH_PANIC_COUNTDOWN.with(|c| c.set(Some(0)));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        uv.remove(1);
    }));
    HASH_PANIC_COUNTDOWN.with(|c| c.set(None));
    assert!(result.is_err());
    assert_eq!(uv.as_ref(), &[Evil(1), Evil(2), Evil(3)]);
    assert_evil_invariant(&uv, 1..=3);

    HASH_PANIC_COUNTDOWN.with(|c| c.set(Some(0)));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        uv.remove_if(|x| x.0 == 2);
    }));
    HASH_PANIC_COUNTDOWN.with(|c| c.set(None));
    assert!(result.is_err());
    assert_eq!(uv.as_ref(), &[Evil(1), Evil(2), Evil(3)]);
    assert_evil_invariant(&uv, 1..=3);

    HASH_PANIC_COUNTDOWN.with(|c| c.set(Some(0)));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        uv.swap_remove_if(|x| x.0 == 2);
    }));
    HASH_PANIC_COUNTDOWN.with(|c| c.set(None));
    assert!(result.is_err());
    assert_eq!(uv.as_ref(), &[Evil(1), Evil(2), Evil(3)]);
    assert_evil_invariant(&uv, 1..=3);

    // With panics disarmed, removal still works normally.
    assert_eq!(uv.remove_if(|x| x.0 == 2), Some(Evil(2)));
    assert_eq!(uv.as_ref(), &[Evil(1), Evil(3)]);
    assert_evil_invariant(&uv, 1..=3);
    assert!(uv.push(Evil(2)));
    assert_evil_invariant(&uv, 1..=3);
}

#[test]
fn unique_vec_remove_panicking_destructor_preserves_membership() {
    use super::UniqueVec;
    use std::{
        cell::Cell,
        hash::{Hash, Hasher},
        panic::{AssertUnwindSafe, catch_unwind},
    };
    thread_local! { static PANIC: Cell<bool> = const { Cell::new(false) }; }
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Item(u32);
    impl Hash for Item {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.0.hash(state);
        }
    }
    impl Drop for Item {
        fn drop(&mut self) {
            PANIC.with(|flag| {
                if flag.replace(false) {
                    panic!("one-shot drop");
                }
            });
        }
    }
    let mut uv = UniqueVec::from(vec![Item(1), Item(2)]);
    PANIC.with(|flag| flag.set(true));
    assert!(catch_unwind(AssertUnwindSafe(|| uv.remove(0))).is_err());
    assert_eq!(uv.len(), 1);
    assert!(!uv.contains(&Item(1)));
    assert!(uv.push(Item(1)));
    assert_eq!(uv.iter().filter(|x| x.0 == 1).count(), 1);
}

#[test]
fn unique_vec_growth_hash_panic_probe() {
    use super::UniqueVec;
    use std::{
        cell::Cell,
        hash::{Hash, Hasher},
        panic::{AssertUnwindSafe, catch_unwind},
    };
    thread_local! { static PANIC: Cell<bool> = const { Cell::new(false) }; }
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Item(u32);
    impl Hash for Item {
        fn hash<H: Hasher>(&self, state: &mut H) {
            if self.0 != 4 {
                PANIC.with(|flag| {
                    if flag.replace(false) {
                        panic!("growth rehash");
                    }
                });
            }
            self.0.hash(state);
        }
    }
    let mut uv = UniqueVec::with_capacity(3);
    for i in 1..4 {
        uv.push(Item(i));
    }
    PANIC.with(|flag| flag.set(true));
    assert!(catch_unwind(AssertUnwindSafe(|| uv.push(Item(4)))).is_err());
    PANIC.with(|flag| flag.set(false));
    for i in 1..=4 {
        assert_eq!(uv.contains(&Item(i)), uv.as_ref().contains(&Item(i)));
    }
}

#[test]
fn duplicate_collection_uses_unique_sized_capacity() {
    let uv: super::UniqueVec<u64> = std::iter::repeat_n(7, 1_000_000).collect();
    assert_eq!(uv.len(), 1);
    let vec_capacity = uv.capacity();
    let set_capacity = uv.into_set().capacity();
    assert!(vec_capacity <= 4);
    assert!(set_capacity <= 4);
    println!("1 distinct u64: vec capacity={vec_capacity}, set capacity={set_capacity}");
}

#[test]
fn streaming_deserialization_preserves_borrowed_strings_and_compacts_duplicates() {
    let input = String::from(r#"["alpha","beta","alpha"]"#);
    let values: UniqueVec<&str> = serde_json::from_str(&input).unwrap();
    assert_eq!(values.as_ref(), &["alpha", "beta"]);
    assert_eq!(values[0].as_ptr(), input[2..7].as_ptr());
    let json = format!(
        "[{}]",
        std::iter::repeat_n("7", 10000)
            .collect::<Vec<_>>()
            .join(",")
    );
    let values: UniqueVec<u64> = serde_json::from_str(&json).unwrap();
    assert_eq!(values.as_ref(), &[7]);
    assert!(values.capacity() <= 4);
}

#[test]
fn all_removal_variants_keep_membership_after_drop_panics() {
    thread_local! { static DROP_PANIC: Cell<bool> = const { Cell::new(false) }; }
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct Item(u32);
    impl Drop for Item {
        fn drop(&mut self) {
            DROP_PANIC.with(|flag| {
                if flag.replace(false) {
                    panic!("one-shot destructor");
                }
            });
        }
    }
    for mode in 0..4 {
        let mut values = UniqueVec::from(vec![Item(1), Item(2), Item(3)]);
        DROP_PANIC.with(|flag| flag.set(true));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match mode {
            0 => {
                values.remove(0);
            }
            1 => {
                values.remove_if(|v| v.0 == 1);
            }
            2 => {
                values.swap_remove_if(|v| v.0 == 1);
            }
            _ => values.retain(|v| v.0 != 1),
        }));
        assert!(result.is_err());
        for id in 1..=3 {
            assert_eq!(
                values.contains(&Item(id)),
                values.as_ref().contains(&Item(id))
            );
        }
        let already = values.contains(&Item(1));
        assert_eq!(values.push(Item(1)), !already);
        assert_eq!(values.iter().filter(|v| v.0 == 1).count(), 1);
    }
}

#[test]
fn equality_panic_does_not_remove_membership() {
    thread_local! { static EQ_PANIC: Cell<bool> = const { Cell::new(false) }; }
    #[derive(Clone, Debug)]
    struct Item(u32);
    impl Hash for Item {
        fn hash<H: Hasher>(&self, state: &mut H) {
            0u8.hash(state);
        }
    }
    impl PartialEq for Item {
        fn eq(&self, other: &Self) -> bool {
            EQ_PANIC.with(|flag| {
                if flag.replace(false) {
                    panic!("one-shot equality");
                }
            });
            self.0 == other.0
        }
    }
    impl Eq for Item {}
    let mut values = UniqueVec::from(vec![Item(1), Item(2), Item(3)]);
    EQ_PANIC.with(|flag| flag.set(true));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| values.remove(1))).is_err());
    assert_eq!(values.as_ref(), &[Item(1), Item(2), Item(3)]);
    for id in 1..=3 {
        assert!(values.contains(&Item(id)));
    }
}
