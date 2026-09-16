//! Benchmarks for the playhead synchronization hot path.
//!
//! The real-time-safety contract for these methods is: no allocation, no
//! locking, no syscalls. The benchmarks pin down the throughput side;
//! the allocation/lock-free property is enforced by keeping the
//! implementations arithmetic-only (see `src/sync.rs`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tpt_av_sync_playhead::sync::ClockFn;
use tpt_av_sync_playhead::PlayheadSync;
use tpt_av_sync_utils::PeerId;

fn fake_clock(start: u64) -> (ClockFn, Arc<AtomicU64>) {
    let t = Arc::new(AtomicU64::new(start));
    let t2 = t.clone();
    let f = Arc::new(move || t2.load(Ordering::Relaxed) as f64) as ClockFn;
    (f, t)
}

fn bench_hot_path(c: &mut Criterion) {
    let (clock, t) = fake_clock(1_000);
    let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
    let master = PeerId::from_u64(2);
    sync.follow_master(master);
    sync.clock.observe(7.5, 12.0);
    sync.receive_update(&tpt_av_sync_playhead::PlayheadUpdate {
        peer_id: master,
        position: 480_000,
        timestamp_ms: 1_000,
        playing: true,
    });

    c.bench_function("synchronized_position (playing, extrapolated)", |b| {
        b.iter(|| sync.synchronized_position())
    });

    c.bench_function("set_local_position", |b| {
        b.iter(|| sync.set_local_position(black_box(123_456)))
    });

    c.bench_function("generate_update", |b| b.iter(|| sync.generate_update()));

    let _ = t;
}

fn bench_clock_sync(c: &mut Criterion) {
    let mut sync = tpt_av_sync_playhead::ClockSynchronizer::new();
    let (clock, _t) = fake_clock(0);
    let mut player = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
    let req = tpt_av_sync_playhead::ClockSyncMessage::request(PeerId::from_u64(2), 100);

    c.bench_function("clock measure + observe", |b| {
        b.iter(|| {
            let resp = req.respond(250);
            sync.process_response(resp, 350);
        })
    });

    c.bench_function("process_clock_sync (request → response)", |b| {
        b.iter(|| player.process_clock_sync(req))
    });
}

criterion_group!(benches, bench_hot_path, bench_clock_sync);
criterion_main!(benches);
