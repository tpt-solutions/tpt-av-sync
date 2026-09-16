//! Synchronized playback: a master clock and a follower stay aligned via
//! NTP-style clock sync and latency-compensated playhead updates.
//!
//! Run with: `cargo run -p tpt-av-sync-examples --bin playhead_sync`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tpt_av_sync_crdt::TimelineCrdt;
use tpt_av_sync_net::{LoopbackTransport, SyncEngine, SyncEvent, SyncMessage};
use tpt_av_sync_playhead::{PlayheadSync, TransportState, TransportSync};
use tpt_av_sync_utils::PeerId;

fn main() {
    println!("=== tpt-av-sync: playhead sync demo ===\n");

    // Truth for the demo: the master clock runs 5 ms ahead of the follower.
    let true_offset_ms = 5.0_f64;

    let (ta, tb) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));
    let mut master_engine = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(ta));
    let mut follower_engine =
        SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(tb));

    // Virtual clocks: both peers share one advancing simulation clock.
    // (Real apps use their own monotonic clocks and correct the skew via
    // ClockSynchronizer.)
    let sim = Arc::new(AtomicU64::new(0));
    let sim_master = sim.clone();
    let sim_follower = sim.clone();

    // The master's clock runs 5 ms ahead of the follower's (the skew the
    // follower must measure and compensate).
    let mut master = PlayheadSync::with_clock(
        PeerId::from_u64(1),
        48_000,
        Arc::new(move || sim_master.load(Ordering::Relaxed) as f64 + true_offset_ms),
    );
    let mut follower = PlayheadSync::with_clock(
        PeerId::from_u64(2),
        48_000,
        Arc::new(move || sim_follower.load(Ordering::Relaxed) as f64),
    );
    master.set_master(true);
    follower.follow_master(PeerId::from_u64(1));

    // The engine owns the transport, so playhead updates go through a
    // small helper that rebroadcasts via the loopback flag flip.
    //
    // (In a real application you would route the update through the sync
    // engine by storing it in application state and sending it from the
    // engine's event loop.)

    // 1. Clock sync round: the offset estimate should land near truth.
    let request = follower.send_clock_sync_request(PeerId::from_u64(1));
    // The master's clock is 5 ms ahead; the one-way link is ~1 ms here.
    let response = request.respond((request.t1 as f64 + true_offset_ms + 1.0) as u64);
    let reply_at = (response.t2.unwrap() as f64 - true_offset_ms) as u64 + 1;
    follower.process_clock_sync(response.with_t4(reply_at));
    println!(
        "clock sync: follower offset {:.1} ms (true +{true_offset_ms} ms), rtt {:.1} ms",
        follower.clock_offset_ms(),
        follower.clock_rtt_ms()
    );

    // 2. Master starts playback at frame 0; the follower obeys.
    let mut master_transport = TransportSync::new();
    master_transport.set_master(true);
    let mut follower_transport = TransportSync::new();
    let play = master_transport.play(0);
    follower_transport.on_remote_control(&play, PeerId::from_u64(1));
    master.set_playing(true);
    follower.set_playing(true);
    println!(
        "transport: {:?} at frame {}",
        follower_transport.state(),
        play.position()
    );

    // 3. Playback loop: the master broadcasts playhead updates every 10 ms
    //    of simulated time; the follower tracks within a millisecond.
    let sample_rate = 48_000_u32;
    let mut worst_error_ms = 0.0_f64;
    for tick in 0..100 {
        let now = 10 * (tick + 1);
        sim.store(now, Ordering::Relaxed);
        let now_remote = now as f64 + true_offset_ms;

        let master_pos = (now_remote * f64::from(sample_rate) / 1_000.0).floor() as u64;
        master.set_local_position(master_pos);
        let update = master.generate_update();
        master_engine
            .transport_mut()
            .broadcast(SyncMessage::PlayheadUpdate(update))
            .expect("send");
        follower_engine.process_messages();

        for event in follower_engine.take_events() {
            if let SyncEvent::Playhead(update) = event {
                follower.receive_update(&update);
            }
        }
        let estimated = follower.synchronized_position();
        let error = (estimated as f64 - master_pos as f64).abs() / f64::from(sample_rate) * 1_000.0;
        worst_error_ms = worst_error_ms.max(error);
    }

    println!(
        "playback: 100 updates at 48 kHz; worst follower error {worst_error_ms:.3} ms (< 1 ms target)"
    );
    assert!(worst_error_ms < 1.0, "playhead sync precision target missed");

    // 4. Stop propagates too.
    let stop = master_transport.stop(master.local_position());
    follower_transport.on_remote_control(&stop, PeerId::from_u64(1));
    assert_eq!(follower_transport.state(), TransportState::Stopped);
    println!(
        "transport: {:?} — transport control sync ✓",
        follower_transport.state()
    );
}
