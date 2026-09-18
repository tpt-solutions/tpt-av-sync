//! NTP-style network clock synchronization (T1–T4 offset estimation).
//!
//! The classic four-timestamp exchange:
//!
//! ```text
//! A                          B
//! | --- t1 (request) ------> |
//! |                     t2 = B receive time
//! |                     t3 = B send time
//! | <------ (response) ----  |
//! t4 = A receive time
//! ```
//!
//! - round-trip time `rtt = (t4 - t1) - (t3 - t2)`
//! - clock offset `offset = ((t2 - t1) + (t3 - t4)) / 2`
//!   (how far B's clock is ahead of A's)
//!
//! The synchronizer keeps the offset measured from the lowest-rtt sample
//! seen, which is the standard way to reject queuing jitter.

use serde::{Deserialize, Serialize};
use tpt_av_sync_utils::PeerId;

/// Role of a clock-sync message in the exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClockSyncType {
    /// Initial request (carries `t1`).
    Request,
    /// Reply from the receiver (carries `t1`, `t2`, `t3`).
    Response,
}

/// A clock synchronization message (NTP-like algorithm).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ClockSyncMessage {
    /// Sender peer id.
    pub peer_id: PeerId,
    /// Message type (request or response).
    pub message_type: ClockSyncType,
    /// Timestamp T1 — sender's send time (ms).
    pub t1: u64,
    /// Timestamp T2 — receiver's receive time (ms, responses only).
    pub t2: Option<u64>,
    /// Timestamp T3 — receiver's send time (ms, responses only).
    pub t3: Option<u64>,
    /// Timestamp T4 — original sender's receive time (ms, filled by the
    /// requester when the response arrives).
    pub t4: Option<u64>,
}

impl ClockSyncMessage {
    /// Builds a sync request stamped with the sender's current time.
    #[must_use]
    pub fn request(peer_id: PeerId, now_ms: u64) -> Self {
        Self {
            peer_id,
            message_type: ClockSyncType::Request,
            t1: now_ms,
            t2: None,
            t3: None,
            t4: None,
        }
    }

    /// Builds the response a receiver sends back, stamping receive/send
    /// times. Processing time between `t2` and `t3` is folded into the
    /// measurement; keeping it tiny improves accuracy.
    #[must_use]
    pub fn respond(&self, now_ms: u64) -> Self {
        Self {
            peer_id: self.peer_id,
            message_type: ClockSyncType::Response,
            t1: self.t1,
            t2: Some(now_ms),
            t3: Some(now_ms),
            t4: None,
        }
    }

    /// Completes a received response with the requester's receive time.
    #[must_use]
    pub const fn with_t4(mut self, now_ms: u64) -> Self {
        self.t4 = Some(now_ms);
        self
    }

    /// Computes `(offset_ms, rtt_ms)` from a completed response, or `None`
    /// when timestamps are missing.
    #[must_use]
    pub fn measure(&self) -> Option<(f64, f64)> {
        let (t2, t3, t4) = (self.t2?, self.t3?, self.t4?);
        let t1 = self.t1 as f64;
        let (t2, t3, t4) = (t2 as f64, t3 as f64, t4 as f64);
        let rtt = (t4 - t1) - (t3 - t2);
        let offset = ((t2 - t1) + (t3 - t4)) / 2.0;
        Some((offset, rtt.max(0.0)))
    }
}

/// Running NTP-style clock synchronizer.
///
/// Retains the offset measured from the best (lowest-rtt) sample seen,
/// which is robust against queuing jitter. All methods are allocation-free.
#[derive(Debug, Clone)]
pub struct ClockSynchronizer {
    best_rtt_ms: f64,
    best_offset_ms: f64,
    has_sample: bool,
}

impl Default for ClockSynchronizer {
    fn default() -> Self {
        Self::new()
    }
}

impl ClockSynchronizer {
    /// Creates a synchronizer with no samples.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            best_rtt_ms: f64::INFINITY,
            best_offset_ms: 0.0,
            has_sample: false,
        }
    }

    /// Feeds a measurement. Constant-time, allocation-free.
    pub fn observe(&mut self, offset_ms: f64, rtt_ms: f64) {
        if !self.has_sample || rtt_ms < self.best_rtt_ms {
            self.best_rtt_ms = rtt_ms;
            self.best_offset_ms = offset_ms;
        }
        self.has_sample = true;
    }

    /// Consumes a response message: fills `t4` if not already stamped and
    /// observes the result. Returns `false` when the message is not a
    /// complete response. (Pre-stamping `t4` lets callers with virtual or
    /// higher-resolution receive timestamps drive the estimator.)
    pub fn process_response(&mut self, response: ClockSyncMessage, now_ms: u64) -> bool {
        if response.message_type != ClockSyncType::Response {
            return false;
        }
        let completed = if response.t4.is_some() {
            response
        } else {
            response.with_t4(now_ms)
        };
        match completed.measure() {
            Some((offset, rtt)) => {
                self.observe(offset, rtt);
                true
            }
            None => false,
        }
    }

    /// The accepted clock offset in ms (remote clock minus local clock).
    #[must_use]
    pub const fn offset_ms(&self) -> f64 {
        self.best_offset_ms
    }

    /// The round-trip time of the winning sample.
    #[must_use]
    pub const fn rtt_ms(&self) -> f64 {
        self.best_rtt_ms
    }

    /// Whether any sample has been observed.
    #[must_use]
    pub const fn has_sample(&self) -> bool {
        self.has_sample
    }

    /// Converts a remote-clock timestamp to local-clock time.
    #[must_use]
    pub const fn to_local(&self, remote_ms: f64) -> f64 {
        remote_ms - self.best_offset_ms
    }

    /// Converts a local-clock timestamp to remote-clock time.
    #[must_use]
    pub const fn to_remote(&self, local_ms: f64) -> f64 {
        local_ms + self.best_offset_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_av_sync_utils::wire;

    fn peer() -> PeerId {
        PeerId::from_u64(1)
    }

    #[test]
    fn request_response_measure() {
        // Truth: B's clock is 250 ms ahead of A's; one-way latency is 20 ms.
        let local_send = 1_000_u64;
        let remote_recv = local_send + 250 + 20; // 1270 on B's clock
        let remote_send = remote_recv + 1; // 1 ms of processing on B
        let local_recv = (remote_send - 250) + 20; // back on A's clock

        let req = ClockSyncMessage::request(peer(), local_send);
        let resp = req.respond(remote_send);
        assert_eq!(resp.t2, Some(remote_send));
        assert_eq!(resp.t3, Some(remote_send));
        let (offset, rtt) = resp.with_t4(local_recv).measure().unwrap();
        assert!((offset - 250.5).abs() < 0.6, "offset {offset}");
        assert!((rtt - 41.0).abs() < 0.01, "rtt {rtt}");
    }

    #[test]
    fn synchronizer_prefers_lowest_rtt_sample() {
        let mut sync = ClockSynchronizer::new();
        sync.observe(400.0, 50.0); // jittery sample
        sync.observe(250.0, 10.0); // clean sample
        sync.observe(300.0, 40.0);
        assert!((sync.offset_ms() - 250.0).abs() < f64::EPSILON);
        assert!((sync.rtt_ms() - 10.0).abs() < f64::EPSILON);
        assert!(sync.has_sample());
    }

    #[test]
    fn conversions_use_offset() {
        let mut sync = ClockSynchronizer::new();
        sync.observe(250.0, 10.0);
        assert!((sync.to_remote(1_000.0) - 1_250.0).abs() < f64::EPSILON);
        assert!((sync.to_local(1_250.0) - 1_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn process_response_rejects_requests_and_incomplete() {
        let mut sync = ClockSynchronizer::new();
        let req = ClockSyncMessage::request(peer(), 5);
        assert!(!sync.process_response(req, 10));
        assert!(!sync.has_sample());

        let half = ClockSyncMessage {
            peer_id: peer(),
            message_type: ClockSyncType::Response,
            t1: 5,
            t2: None,
            t3: None,
            t4: None,
        };
        assert!(!sync.process_response(half, 10));
    }

    #[test]
    fn measure_requires_all_timestamps() {
        let req = ClockSyncMessage::request(peer(), 5);
        assert!(req.measure().is_none());
    }

    #[test]
    fn message_serde_roundtrip() {
        let msg = ClockSyncMessage::request(peer(), 42);
        let bytes = wire::encode(&msg).unwrap();
        let back: ClockSyncMessage = wire::decode(&bytes).unwrap();
        assert_eq!(back, msg);
    }
}
