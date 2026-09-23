//! Allocation budgets run in their own test binary so other index tests cannot
//! contaminate the allocator counters. These are broad budgets, not exact layouts.
use anda_db_tfs::BM25Index;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeSet,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
};
use tantivy::tokenizer::{RawTokenizer, SimpleTokenizer};

struct MeteredAllocator;
static METERING: AtomicBool = AtomicBool::new(false);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every operation forwards the original pointer and layout to System.
// Accounting uses only atomics and cannot recursively allocate.
unsafe impl GlobalAlloc for MeteredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && METERING.load(Relaxed) {
            ALLOCATED.fetch_add(layout.size(), Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if METERING.load(Relaxed) {
            FREED.fetch_add(layout.size(), Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, new_size) };
        if !next.is_null() && METERING.load(Relaxed) {
            ALLOCATED.fetch_add(new_size, Relaxed);
            FREED.fetch_add(layout.size(), Relaxed);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: MeteredAllocator = MeteredAllocator;

fn measure<R>(f: impl FnOnce() -> R) -> (R, usize, usize) {
    ALLOCATED.store(0, Relaxed);
    FREED.store(0, Relaxed);
    METERING.store(true, Relaxed);
    let result = f();
    METERING.store(false, Relaxed);
    (result, ALLOCATED.load(Relaxed), FREED.load(Relaxed))
}

#[test]
fn search_allocations_and_maintenance_reclamation() {
    for count in [1000, 20_000] {
        let index = BM25Index::new("allocations".into(), SimpleTokenizer::default(), None);
        for id in 0..count {
            index.insert(id, &format!("common term{id:06}"), 0).unwrap();
        }
        for query in ["missing", "term000099"] {
            let (hits, allocated, _) = measure(|| index.search(query, 10, None));
            assert_eq!(hits.len(), usize::from(query != "missing"));
            eprintln!("TFS_ALLOC docs={count} query={query} allocated={allocated}");
            assert!(allocated < 8192, "small query allocated {allocated} bytes");
        }
        for logical in [false, true] {
            let (hits, allocated, _) = measure(|| {
                index
                    .try_search_in_ids("common", 10, None, &[], logical)
                    .unwrap()
            });
            assert!(hits.is_empty());
            eprintln!("TFS_ALLOC docs={count} empty_scope logical={logical} allocated={allocated}");
            assert!(allocated < 8192, "empty scope allocated {allocated} bytes");
        }
    }

    // One bucket also needs memory maintenance even when no repacking is needed.
    let index = BM25Index::new("reclamation".into(), RawTokenizer::default(), None);
    for id in 0..20_000 {
        index.insert(id, "common", 0).unwrap();
    }
    index.purge_ids(&(1000..20_000).collect::<BTreeSet<_>>(), 1);
    futures::executor::block_on(index.flush_with(
        1,
        |_| std::future::ready(Ok(())),
        |_, _| std::future::ready(Ok(())),
    ))
    .unwrap();
    let version = index.stats().version;
    let before = index.search("common", 10, None);
    let (_, allocated, freed) = measure(|| index.compact_buckets());
    eprintln!("TFS_RECLAIM allocated={allocated} freed={freed}");
    assert!(
        freed > allocated + 200_000,
        "maintenance did not release excess posting capacity"
    );
    assert_eq!(index.search("common", 10, None), before);
    assert!(!index.has_dirty_buckets());
    assert_eq!(index.stats().version, version);
    // Space is still reusable after maintenance.
    index.insert(20_000, "common", 2).unwrap();
    assert_eq!(index.len(), 1001);
}
