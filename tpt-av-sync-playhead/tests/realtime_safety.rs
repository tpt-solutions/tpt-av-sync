//! Real-time-safety audit for the playhead hot path.
//!
//! `PlayheadSync::set_local_position` and `PlayheadSync::synchronized_position`
//! run on the audio/render thread and must never allocate or block. This is
//! enforced here, not just claimed in a doc comment: a counting global
//! allocator (the only one this binary installs, so the count is exact)
//! observes zero allocations across many calls to both methods, exercising
//! every branch (master, follower-with-no-master, follower-playing,
//! follower-paused) and a populated `remotes` map so `HashMap::get` runs a
//! real probe rather than hitting an empty table.
//!
//! Lock-freedom is structural rather than something a test can observe:
//! `PlayheadSync` holds no `Mutex`/`RwLock`/atomic-with-contention — see
//! its field list in `src/sync.rs`. `HashMap::get` and arithmetic are the
//! only operations on the hot path, and neither can block.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use tpt_av_sync_playhead::{PlayheadSync, PlayheadUpdate};
use tpt_av_sync_utils::PeerId;

thread_local! {
    // Per-thread, so parallel test threads (the default test harness
    // behavior) don't pollute each other's counts.
    static ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOC_COUNT.try_with(|c| c.set(c.get() + 1));
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOC_COUNT.try_with(|c| c.set(c.get() + 1));
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn allocs_during<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOC_COUNT.with(Cell::get);
    let result = f();
    let after = ALLOC_COUNT.with(Cell::get);
    (result, after - before)
}

#[test]
fn set_local_position_never_allocates() {
    let mut sync = PlayheadSync::new(PeerId::from_u64(1), 48_000);
    // Warm up (first call to any method may lazily touch e.g. thread-local
    // state unrelated to our code); only the loop below is measured.
    sync.set_local_position(0);

    let (_, allocs) = allocs_during(|| {
        for position in 0..10_000_u64 {
            sync.set_local_position(position);
        }
    });
    assert_eq!(allocs, 0, "set_local_position allocated on the hot path");
}

#[test]
fn synchronized_position_never_allocates_as_master() {
    let mut sync = PlayheadSync::new(PeerId::from_u64(1), 48_000);
    sync.set_master(true);
    sync.set_local_position(123);
    // Warm-up call outside the measured region.
    let _ = sync.synchronized_position();

    let (_, allocs) = allocs_during(|| {
        for _ in 0..10_000 {
            std::hint::black_box(sync.synchronized_position());
        }
    });
    assert_eq!(allocs, 0, "synchronized_position (master) allocated on the hot path");
}

#[test]
fn synchronized_position_never_allocates_as_playing_follower() {
    let mut sync = PlayheadSync::new(PeerId::from_u64(1), 48_000);
    let master = PeerId::from_u64(2);
    // Populate remotes (and a real clock-offset/drift sample) so the hot
    // path exercises a non-empty HashMap probe and the drift-correction
    // arithmetic, not just the early-return branches.
    sync.observe_clock_sample(50.0, 5.0, 0.0);
    sync.follow_master(master);
    sync.receive_update(&PlayheadUpdate {
        peer_id: master,
        position: 48_000,
        timestamp_ms: 0,
        playing: true,
    });
    let _ = sync.synchronized_position();

    let (_, allocs) = allocs_during(|| {
        for _ in 0..10_000 {
            std::hint::black_box(sync.synchronized_position());
        }
    });
    assert_eq!(
        allocs, 0,
        "synchronized_position (playing follower) allocated on the hot path"
    );
}

#[test]
fn synchronized_position_never_allocates_as_paused_follower() {
    let mut sync = PlayheadSync::new(PeerId::from_u64(1), 48_000);
    let master = PeerId::from_u64(2);
    sync.follow_master(master);
    sync.receive_update(&PlayheadUpdate {
        peer_id: master,
        position: 5_000,
        timestamp_ms: 0,
        playing: false,
    });
    let _ = sync.synchronized_position();

    let (_, allocs) = allocs_during(|| {
        for _ in 0..10_000 {
            std::hint::black_box(sync.synchronized_position());
        }
    });
    assert_eq!(
        allocs, 0,
        "synchronized_position (paused follower) allocated on the hot path"
    );
}

#[test]
fn synchronized_position_never_allocates_as_follower_without_master() {
    let mut sync = PlayheadSync::new(PeerId::from_u64(1), 48_000);
    sync.set_local_position(7);
    let _ = sync.synchronized_position();

    let (_, allocs) = allocs_during(|| {
        for _ in 0..10_000 {
            std::hint::black_box(sync.synchronized_position());
        }
    });
    assert_eq!(
        allocs, 0,
        "synchronized_position (no master) allocated on the hot path"
    );
}
