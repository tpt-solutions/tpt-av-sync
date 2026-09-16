//! The playhead synchronization engine.

use crate::clock::{ClockSyncMessage, ClockSynchronizer};
use crate::drift::DriftCompensator;
use crate::latency::LatencyEstimator;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tpt_av_sync_utils::PeerId;

/// A playhead position update broadcast by a peer.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PlayheadUpdate {
    /// Peer that produced the update.
    pub peer_id: PeerId,
    /// Playhead position in frames/samples.
    pub position: u64,
    /// When the position was captured, in the *sender's* clock domain
    /// (ms). Receivers convert with their clock offset.
    pub timestamp_ms: u64,
    /// Whether the sender's transport is playing.
    pub playing: bool,
}

/// Clock source: monotonic milliseconds in an arbitrary domain.
pub type ClockFn = Arc<dyn Fn() -> f64 + Send + Sync>;

/// A tracked remote playhead.
#[derive(Debug, Clone, Copy)]
struct RemoteTrack {
    position: u64,
    /// Capture time converted into the local clock domain.
    at_local_ms: f64,
    playing: bool,
}

/// Playhead synchronization engine.
///
/// Ensures that all peers see a synchronized playhead position with
/// sub-millisecond precision, accounting for network latency and clock
/// drift.
///
/// # Real-time safety
///
/// [`set_local_position`](Self::set_local_position) and
/// [`synchronized_position`](Self::synchronized_position) are the
/// audio/render-thread hot path: both perform only arithmetic and a
/// hash-map probe — no allocation, no locking, no syscalls.
pub struct PlayheadSync {
    peer_id: PeerId,
    local_position: u64,
    playing: bool,
    is_master: bool,
    master_peer: Option<PeerId>,
    sample_rate: u32,
    clock: ClockSynchronizer,
    latency: LatencyEstimator,
    drift: DriftCompensator,
    now: ClockFn,
    last_offset_sample_at: Option<f64>,
    remotes: HashMap<PeerId, RemoteTrack>,
}

impl PlayheadSync {
    /// Creates a playhead sync engine driven by the real monotonic clock.
    #[must_use]
    pub fn new(peer_id: PeerId, sample_rate: u32) -> Self {
        let start = std::time::Instant::now();
        Self::with_clock(peer_id, sample_rate, Arc::new(move || {
            start.elapsed().as_secs_f64() * 1_000.0
        }))
    }

    /// Creates a playhead sync engine with an injected clock (ms in any
    /// monotonic domain) — used for deterministic tests and simulations.
    #[must_use]
    pub fn with_clock(peer_id: PeerId, sample_rate: u32, now: ClockFn) -> Self {
        Self {
            peer_id,
            local_position: 0,
            playing: false,
            is_master: false,
            master_peer: None,
            sample_rate,
            clock: ClockSynchronizer::new(),
            latency: LatencyEstimator::default(),
            drift: DriftCompensator::default(),
            now,
            last_offset_sample_at: None,
            remotes: HashMap::new(),
        }
    }

    /// This peer's id.
    #[must_use]
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The configured sample rate (frames per second).
    #[must_use]
    pub const fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Updates the local playhead position. Hot path: allocation-free.
    #[inline]
    pub fn set_local_position(&mut self, position: u64) {
        self.local_position = position;
    }

    /// The local playhead position.
    #[must_use]
    pub const fn local_position(&self) -> u64 {
        self.local_position
    }

    /// Marks the local transport playing/stopped.
    pub const fn set_playing(&mut self, playing: bool) {
        self.playing = playing;
    }

    /// Whether the local transport is playing.
    #[must_use]
    pub const fn playing(&self) -> bool {
        self.playing
    }

    /// Promotes/demotes this peer to master clock.
    pub const fn set_master(&mut self, is_master: bool) {
        self.is_master = is_master;
        if is_master {
            self.master_peer = Some(self.peer_id);
        }
    }

    /// Whether this peer is the master clock.
    #[must_use]
    pub const fn is_master(&self) -> bool {
        self.is_master
    }

    /// Follows the given peer's clock as master.
    pub const fn follow_master(&mut self, peer: PeerId) {
        self.master_peer = Some(peer);
    }

    /// The peer currently followed as master, if any.
    #[must_use]
    pub const fn master_peer(&self) -> Option<PeerId> {
        self.master_peer
    }

    /// Receives a playhead update from a peer.
    ///
    /// `position` is the peer's playhead in frames, `timestamp_ms` the
    /// sender-clock capture time of that position. The update is stored
    /// with its capture time converted into the local clock domain;
    /// extrapolation to "now" happens on read so stale updates never
    /// compound error. The peer is assumed to be playing; use
    /// [`Self::receive_update`] to convey the paused state.
    pub fn receive_playhead_update(&mut self, peer_id: PeerId, position: u64, timestamp_ms: u64) {
        let playing = self
            .remotes
            .get(&peer_id)
            .map(|t| t.playing)
            .unwrap_or(true);
        self.store_update(peer_id, position, timestamp_ms, playing);
    }

    /// Receives a full playhead update message (see [`PlayheadUpdate`]).
    pub fn receive_update(&mut self, update: &PlayheadUpdate) {
        self.store_update(
            update.peer_id,
            update.position,
            update.timestamp_ms,
            update.playing,
        );
    }

    fn store_update(
        &mut self,
        peer_id: PeerId,
        position: u64,
        timestamp_ms: u64,
        playing: bool,
    ) {
        let at_local_ms = self.clock.to_local(timestamp_ms as f64);
        self.remotes.insert(
            peer_id,
            RemoteTrack {
                position,
                at_local_ms,
                playing,
            },
        );
    }

    /// Returns the synchronized playhead position.
    ///
    /// When this peer is the master (or no master is being followed) the
    /// local position is returned. Otherwise the tracked master position
    /// is extrapolated to the current instant — including while playing —
    /// which inherently compensates one-way transit, and drift-corrected.
    ///
    /// Hot path: allocation-free, lock-free.
    #[must_use]
    pub fn synchronized_position(&self) -> u64 {
        if self.is_master {
            return self.local_position;
        }
        let Some(master) = self.master_peer else {
            return self.local_position;
        };
        let Some(track) = self.remotes.get(&master) else {
            return self.local_position;
        };
        if !track.playing {
            return track.position;
        }
        let now = (self.now)();
        let elapsed_ms = (now - track.at_local_ms).max(0.0);
        let corrected = self.drift.correct_elapsed_ms(elapsed_ms);
        let advance = (corrected * f64::from(self.sample_rate) / 1_000.0).round() as u64;
        track.position.saturating_add(advance)
    }

    /// Builds the playhead update this peer should broadcast.
    ///
    /// The capture timestamp is expressed in the *sender's local* domain;
    /// receivers convert it once with their clock offset
    /// ([`ClockSynchronizer::to_local`](crate::ClockSynchronizer::to_local)).
    /// Pre-converting here would apply the offset twice — once by the
    /// sender and once by the receiver.
    #[must_use]
    pub fn generate_update(&self) -> PlayheadUpdate {
        let now = (self.now)();
        PlayheadUpdate {
            peer_id: self.peer_id,
            position: self.local_position,
            timestamp_ms: now.round().max(0.0) as u64,
            playing: self.playing,
        }
    }

    /// Processes an incoming clock-sync message.
    ///
    /// Requests are answered with a response (return it to the sender);
    /// responses feed the offset/latency/drift estimators and return
    /// `None`.
    pub fn process_clock_sync(&mut self, message: ClockSyncMessage) -> Option<ClockSyncMessage> {
        match message.message_type {
            crate::clock::ClockSyncType::Request => Some(message.respond(self.now_ms_u64())),
            crate::clock::ClockSyncType::Response => {
                let now = (self.now)();
                if self.clock.process_response(message, self.now_ms_u64()) {
                    self.latency.on_rtt_sample(self.clock.rtt_ms());
                    self.drift.on_offset_sample(self.clock.offset_ms(), now);
                    self.last_offset_sample_at = Some(now);
                }
                None
            }
        }
    }

    /// Sends a clock-sync request to `peer`: returns the message to
    /// transmit.
    #[must_use]
    pub fn send_clock_sync_request(&self, _peer: PeerId) -> ClockSyncMessage {
        ClockSyncMessage::request(self.peer_id, self.now_ms_u64())
    }

    /// Feeds a clock-offset sample obtained outside the built-in T1–T4
    /// exchange (e.g. from the application's own sync loop). Updates the
    /// offset, latency, and drift estimators.
    pub fn observe_clock_sample(&mut self, offset_ms: f64, rtt_ms: f64, at_local_ms: f64) {
        self.clock.observe(offset_ms, rtt_ms);
        self.latency.on_rtt_sample(rtt_ms);
        self.drift.on_offset_sample(offset_ms, at_local_ms);
        self.last_offset_sample_at = Some(at_local_ms);
    }

    /// The accepted clock offset estimate (ms; remote ahead of local).
    #[must_use]
    pub const fn clock_offset_ms(&self) -> f64 {
        self.clock.offset_ms()
    }

    /// The accepted round-trip estimate (ms).
    #[must_use]
    pub const fn clock_rtt_ms(&self) -> f64 {
        self.clock.rtt_ms()
    }

    /// The smoothed one-way latency estimate (ms).
    #[must_use]
    pub fn one_way_latency_ms(&self) -> f64 {
        self.latency.rtt_ms() / 2.0
    }

    /// The clock-skew estimate between local and master clocks (ppm).
    #[must_use]
    pub const fn skew_ppm(&self) -> f64 {
        self.drift.skew_ppm()
    }

    /// When the last clock-offset sample was accepted, in local ms.
    #[must_use]
    pub const fn last_clock_sample_at(&self) -> Option<f64> {
        self.last_offset_sample_at
    }

    fn now_ms_u64(&self) -> u64 {
        let now = (self.now)();
        if now.is_finite() && now >= 0.0 {
            now as u64
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn virtual_clock(start: u64) -> (ClockFn, Arc<AtomicU64>) {
        let t = Arc::new(AtomicU64::new(start));
        let t2 = t.clone();
        let f = Arc::new(move || t2.load(Ordering::Relaxed) as f64) as ClockFn;
        (f, t)
    }

    #[test]
    fn master_returns_local_position() {
        let (clock, _t) = virtual_clock(0);
        let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
        sync.set_master(true);
        sync.set_local_position(123_456);
        assert_eq!(sync.synchronized_position(), 123_456);
    }

    #[test]
    fn paused_master_position_is_adopted_directly() {
        let (clock, _t) = virtual_clock(0);
        let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
        let master = PeerId::from_u64(2);
        sync.follow_master(master);
        sync.receive_update(&PlayheadUpdate {
            peer_id: master,
            position: 5_000,
            timestamp_ms: 1_000,
            playing: false,
        });
        assert_eq!(sync.synchronized_position(), 5_000);
    }

    #[test]
    fn update_without_offset_is_used_verbatim() {
        // No clock-sync samples yet: offset 0, capture time = sender time.
        let (clock, _t) = virtual_clock(0);
        let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
        let master = PeerId::from_u64(2);
        sync.follow_master(master);
        sync.receive_update(&PlayheadUpdate {
            peer_id: master,
            position: 10_000,
            timestamp_ms: 500,
            playing: false,
        });
        assert_eq!(sync.synchronized_position(), 10_000);
    }

    #[test]
    fn follower_without_master_uses_local() {
        let (clock, _t) = virtual_clock(0);
        let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
        sync.set_local_position(42);
        assert_eq!(sync.synchronized_position(), 42);
    }

    #[test]
    fn clock_offset_shifts_capture_time() {
        // Master clock runs 250 ms ahead; paused master at 1_000_000.
        let (clock, _t) = virtual_clock(0);
        let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
        sync.clock.observe(250.0, 10.0);
        let master = PeerId::from_u64(2);
        sync.follow_master(master);
        sync.receive_update(&PlayheadUpdate {
            peer_id: master,
            position: 1_000_000,
            timestamp_ms: 2_000,
            playing: false,
        });
        // Capture converts to local 1750; paused => position used as-is.
        assert_eq!(sync.synchronized_position(), 1_000_000);
        assert!((sync.clock_offset_ms() - 250.0).abs() < f64::EPSILON);
    }

    #[test]
    fn generate_update_stamps_local_clock() {
        let (clock, t) = virtual_clock(1_000);
        let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
        sync.clock.observe(300.0, 5.0);
        let update = sync.generate_update();
        assert_eq!(update.peer_id, PeerId::from_u64(1));
        // Stamps stay in the sender's local domain; receivers convert.
        assert_eq!(update.timestamp_ms, 1_000);
        assert_eq!(t.load(Ordering::Relaxed), 1_000);
    }

    #[test]
    fn request_is_answered_response_is_ingested() {
        let (clock, t) = virtual_clock(100);
        let mut sync = PlayheadSync::with_clock(PeerId::from_u64(1), 48_000, clock);
        let remote = PeerId::from_u64(2);

        // Requests from others are answered with the local clock time.
        let incoming = ClockSyncMessage::request(remote, 10);
        let answer = sync.process_clock_sync(incoming).expect("response");
        assert_eq!(answer.message_type, crate::clock::ClockSyncType::Response);
        assert_eq!(answer.t2, Some(100));

        // Remote is 250 ms ahead; both links add 25 ms. t1=100, t2=t3=375,
        // t4=150 → offset 250, rtt 50.
        let req = sync.send_clock_sync_request(remote);
        assert_eq!(req.t1, 100);
        let resp = req.respond(375);
        t.store(150, Ordering::Relaxed);
        sync.process_clock_sync(resp);
        assert!(sync.clock.has_sample());
        assert!(
            (sync.clock_offset_ms() - 250.0).abs() < 1.0,
            "{}",
            sync.clock_offset_ms()
        );
        assert!((sync.clock_rtt_ms() - 50.0).abs() < 1.0);
    }
}
