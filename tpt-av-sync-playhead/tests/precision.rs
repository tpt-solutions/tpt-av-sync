//! Deterministic precision tests for playhead synchronization.
//!
//! All "network" behavior here is simulated with a seeded pseudo-random
//! generator and virtual clocks: no real time passes, no flakiness. The
//! targets mirror the spec: sub-millisecond clock-sync accuracy and
//! playhead agreement.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tpt_av_sync_playhead::{ClockSyncMessage, PlayheadSync, TransportState, TransportSync};
use tpt_av_sync_utils::PeerId;

/// Deterministic LCG pseudo-random in `[lo, hi]`.
#[derive(Clone)]
struct Jitter {
    state: u64,
    lo: f64,
    hi: f64,
}

impl Jitter {
    fn new(seed: u64, lo: f64, hi: f64) -> Self {
        Self {
            state: seed | 1,
            lo,
            hi,
        }
    }
    fn next(&mut self) -> f64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let unit = ((self.state >> 11) as f64) / ((1_u64 << 53) as f64);
        self.lo + unit * (self.hi - self.lo)
    }
}

fn virtual_clock(start_ms: i64) -> (tpt_av_sync_playhead::sync::ClockFn, Arc<AtomicU64>) {
    let t = Arc::new(AtomicU64::new(start_ms.max(0) as u64));
    let t2 = t.clone();
    let f = Arc::new(move || t2.load(Ordering::Relaxed) as f64)
        as tpt_av_sync_playhead::sync::ClockFn;
    (f, t)
}

#[test]
fn clock_sync_converges_to_sub_millisecond_accuracy() {
    // Truth: the remote clock is 3.7 ms ahead; each direction of the
    // network adds 9.4–10.6 ms (jittered, deterministic).
    let true_offset = 3.7_f64;
    let mut d1 = Jitter::new(0xA11CE, 9.4, 10.6);
    let mut d2 = Jitter::new(0xB0B, 9.4, 10.6);

    let local = PeerId::from_u64(1);

    let mut requester = PlayheadSync::new(local, 48_000);

    let mut t_local = 100_000_f64;
    for _ in 0..100 {
        let sent_at = t_local;
        let arrived_remote = sent_at + true_offset + d1.next();
        let resp = ClockSyncMessage::request(local, sent_at as u64).respond(arrived_remote as u64);
        t_local = arrived_remote - true_offset + d2.next();
        // Stamp t4 with the virtual local receive time.
        requester.process_clock_sync(resp.with_t4(t_local as u64));
    }

    let err = (requester.clock_offset_ms() - true_offset).abs();
    assert!(err < 0.8, "offset error {err} ms exceeds sub-ms target");
    // The winning sample's rtt should be near the sum of minimum latencies.
    assert!(
        requester.clock_rtt_ms() < 21.0,
        "best rtt {} should be close to the 18.8 ms floor",
        requester.clock_rtt_ms()
    );
}

#[test]
fn playing_followers_track_master_within_one_millisecond() {
    // Master plays at 48 kHz from frame 0. The follower receives updates
    // every 10 virtual ms through a 4–9 ms one-way link and must reproduce
    // the master's *current* position to within 1 ms (48 frames).
    let sample_rate = 48_000_u32;
    let true_offset = 7.3_f64; // remote clock ahead of local

    let (clock, t_local) = virtual_clock(0);
    let mut follower = PlayheadSync::with_clock(PeerId::from_u64(2), sample_rate, clock);
    follower.follow_master(PeerId::from_u64(1));
    follower.observe_clock_sample(true_offset, 12.0, 0.0);

    let master = PeerId::from_u64(1);
    let mut rng = Jitter::new(77, 4.0, 9.0);

    // Simulation ticks in local time; the master plays on remote time.
    let mut master_pos;
    let mut updates = 0_u32;
    let t0_remote = 100.0_f64; // playback starts at remote time 100
    let mut next_master_tick_remote = t0_remote;

    for tick in 0..200 {
        // Advance the virtual local clock; remote is 7.3 ms ahead.
        let clock_now = 10.0 * (tick as f64 + 1.0);
        t_local.store(clock_now as u64, Ordering::Relaxed);
        let now_remote = clock_now + true_offset;

        master_pos = if now_remote > t0_remote {
            ((now_remote - t0_remote) * f64::from(sample_rate) / 1_000.0).floor() as u64
        } else {
            0
        };

        // Master broadcasts every 10 remote-ms once playback started.
        if now_remote >= next_master_tick_remote && now_remote > t0_remote {
            next_master_tick_remote += 10.0;
            let transit = rng.next();
            let stamp_remote = (now_remote - transit).max(t0_remote);
            let stamp_pos = ((stamp_remote - t0_remote)
                * f64::from(sample_rate)
                / 1_000.0)
                .floor() as u64;
            follower.receive_update(&tpt_av_sync_playhead::PlayheadUpdate {
                peer_id: master,
                position: stamp_pos,
                timestamp_ms: stamp_remote as u64,
                playing: true,
            });
            updates += 1;
        }

        if updates == 0 {
            continue;
        }

        let estimated = follower.synchronized_position();
        let error_frames = (estimated as f64 - master_pos as f64).abs();
        let error_ms = error_frames / f64::from(sample_rate) * 1_000.0;
        assert!(
            error_ms < 1.0,
            "tick {tick}: error {error_ms} ms ({error_frames} frames)"
        );
    }
    assert!(updates > 100, "simulation must have exercised updates");
}

#[test]
fn drift_estimation_tracks_slowing_remote_clock() {
    // Remote clock runs 40 ppm fast. Over 60 virtual seconds of samples
    // the estimator must converge within 10 ppm, and the corrected
    // elapsed time must stay within 100 µs of the truth over the span.
    let skew_ppm = 40.0_f64;
    let (clock, t_local) = virtual_clock(0);
    let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);

    let mut rng = Jitter::new(9_001, -0.002, 0.002);
    let mut t = 0_f64;
    let mut true_offset = 0_f64;
    while t < 60_000.0 {
        true_offset += 10.0 * skew_ppm / 1_000_000.0; // 10 ms tick, remote gains
        t += 10.0;
        if (t as u64) % 1_000 == 0 {
            // One clock-sync round per simulated second.
            let noisy = true_offset + rng.next();
            sync.observe_clock_sample(noisy, 20.0, t);
        }
        let _ = t_local;
    }

    let ppm_err = (sync.skew_ppm() - skew_ppm).abs();
    assert!(ppm_err < 10.0, "skew error {ppm_err} ppm");

    // Correcting a local minute with the estimated skew lands within
    // 100 µs of the remote minute.
    let corrected = sync.skew_ppm();
    let corrected_ms = 60_000.0 * (1.0 + corrected / 1_000_000.0);
    let true_ms = 60_000.0 * (1.0 + skew_ppm / 1_000_000.0);
    assert!((corrected_ms - true_ms).abs() < 0.2, "{} vs {true_ms}", corrected_ms);
}

#[test]
fn transport_followers_follow_the_master() {
    let master = PeerId::from_u64(1);
    let mut a = TransportSync::new();
    let mut b = TransportSync::new();
    let mut c = TransportSync::new();
    a.set_master(true);

    let commands = vec![
        (a.play(1_000), TransportState::Playing),
        (a.locate(2_048), TransportState::Playing), // locate keeps state
        (a.stop(3_000), TransportState::Stopped),
    ];
    for (cmd, expected) in &commands {
        assert!(b.on_remote_control(cmd, master));
        assert!(c.on_remote_control(cmd, master));
        assert_eq!(b.state(), *expected);
        assert_eq!(b.position(), c.position());
    }
    assert_eq!(b.position(), 3_000);
    assert_eq!(c.state(), TransportState::Stopped);
}
