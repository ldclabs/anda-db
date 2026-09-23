//! Small standalone benchmark harness; no production allocator changes.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Instant,
};
struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static COUNT: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static LARGEST: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
fn record(bytes: usize) {
    if ACTIVE.load(Ordering::Relaxed) {
        COUNT.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
        LARGEST.fetch_max(bytes as u64, Ordering::Relaxed);
    }
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        let ptr = unsafe { System.realloc(ptr, layout, size) };
        if !ptr.is_null() {
            if size >= layout.size() {
                LIVE_BYTES.fetch_add((size - layout.size()) as u64, Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub((layout.size() - size) as u64, Ordering::Relaxed);
            }
        }
        ptr
    }
}

/// Report outstanding allocator bytes while retaining one result, separately
/// from measure's cumulative allocation counters. Warm cache/runtime state first.
pub fn retained(name: &str, mut run: impl FnMut() -> bytes::Bytes) {
    for _ in 0..2 {
        drop(run());
    }
    let before = LIVE_BYTES.load(Ordering::Relaxed);
    let result = run();
    let held = LIVE_BYTES.load(Ordering::Relaxed).saturating_sub(before);
    eprintln!(
        "{name}: result_bytes={}, retained_allocation_bytes={held}",
        result.len()
    );
    drop(result);
}
pub fn header() {
    println!("case,median_us,p95_us,allocations,allocated_bytes,largest_allocation");
}
pub fn measure(name: &str, mut setup: impl FnMut(), mut run: impl FnMut()) {
    for _ in 0..2 {
        setup();
        run();
    }
    let mut times = Vec::new();
    let mut counts = 0;
    let mut bytes = 0;
    let mut largest = 0;
    for _ in 0..15 {
        setup();
        COUNT.store(0, Ordering::Relaxed);
        BYTES.store(0, Ordering::Relaxed);
        LARGEST.store(0, Ordering::Relaxed);
        ACTIVE.store(true, Ordering::SeqCst);
        let start = Instant::now();
        run();
        let elapsed = start.elapsed().as_secs_f64() * 1e6;
        ACTIVE.store(false, Ordering::SeqCst);
        times.push(elapsed);
        counts += COUNT.load(Ordering::Relaxed);
        bytes += BYTES.load(Ordering::Relaxed);
        largest = largest.max(LARGEST.load(Ordering::Relaxed));
    }
    times.sort_by(f64::total_cmp);
    println!(
        "{name},{:.3},{:.3},{},{},{largest}",
        times[7],
        times[14],
        counts / 15,
        bytes / 15
    );
}
