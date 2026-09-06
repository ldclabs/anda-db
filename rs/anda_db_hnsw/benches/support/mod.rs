use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
pub struct CountingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
fn add(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
}
// SAFETY: every allocation is forwarded unchanged to System; only requested
// sizes are counted. This instrumentation exists exclusively in the benchmark.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, size) };
        if !new.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            add(size);
        }
        new
    }
}
#[derive(Clone, Copy)]
pub struct Snapshot {
    pub live: usize,
    pub allocations: usize,
    pub peak: usize,
}
impl Snapshot {
    pub fn read() -> Self {
        Self {
            live: LIVE.load(Ordering::Relaxed),
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            peak: PEAK.load(Ordering::Relaxed),
        }
    }
    pub fn start() -> Self {
        PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
        Self::read()
    }
}
