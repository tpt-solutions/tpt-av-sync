//! Session replay: deterministically replays a recorded operation log
//! (from [`TimelineCrdt::operation_log`](crate::TimelineCrdt::operation_log)
//! or a persisted store) into a `TimelineCrdt`, at full speed, at the
//! original real-time pace (optionally scaled), or up to a target point in
//! time.
//!
//! Replay only *paces* application; correctness comes from
//! [`TimelineCrdt::apply_remote`](crate::TimelineCrdt::apply_remote)'s
//! idempotent, order-tolerant semantics, exactly as it does for live
//! network delivery. A recording is therefore just an operation log played
//! back through the same path a peer would use.

use crate::operation::TaggedOperation;
use crate::timeline_crdt::TimelineCrdt;
use std::time::{Duration, SystemTime};

/// Controls the pacing of [`SessionRecording::replay_all`] /
/// [`SessionRecording::replay_until`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplaySpeed {
    /// Apply every operation immediately, with no delay between them.
    Instant,
    /// Reproduce the original wall-clock spacing between consecutive
    /// operations' [`TaggedOperation::timestamp`], scaled by this factor
    /// (`2.0` plays twice as fast, `0.5` half as fast). Non-positive
    /// factors behave like [`ReplaySpeed::Instant`]. Blocks the calling
    /// thread — call from a dedicated replay task, not the render/audio
    /// hot path.
    Realtime(f64),
}

/// A recorded operation log, ready to be replayed.
///
/// Operations are sorted by [`TaggedOperation::timestamp`] on construction.
/// That field is wall-clock capture time — display/pacing only, per its own
/// doc comment — so this ordering governs *when* a replay applies each
/// operation, not whether the result is correct; `apply_remote` converges
/// regardless of the order it receives operations in.
#[derive(Debug, Clone, Default)]
pub struct SessionRecording {
    ops: Vec<TaggedOperation>,
}

impl SessionRecording {
    /// Wraps a recorded operation log, sorting it into replay order.
    #[must_use]
    pub fn new(mut ops: Vec<TaggedOperation>) -> Self {
        ops.sort_by_key(|op| op.timestamp);
        Self { ops }
    }

    /// Number of recorded operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// True when there is nothing recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// The recorded operations, in replay order.
    #[must_use]
    pub fn operations(&self) -> &[TaggedOperation] {
        &self.ops
    }

    /// The capture-time span of the recording (first, last), if non-empty.
    #[must_use]
    pub fn time_range(&self) -> Option<(SystemTime, SystemTime)> {
        Some((self.ops.first()?.timestamp, self.ops.last()?.timestamp))
    }

    /// Replays every recorded operation into `crdt`.
    pub fn replay_all(&self, crdt: &mut TimelineCrdt, speed: ReplaySpeed) {
        self.replay_matching(crdt, speed, |_| true);
    }

    /// Replays only operations captured at or before `cutoff` — "what the
    /// session looked like at this point in time".
    pub fn replay_until(&self, crdt: &mut TimelineCrdt, speed: ReplaySpeed, cutoff: SystemTime) {
        self.replay_matching(crdt, speed, |op| op.timestamp <= cutoff);
    }

    fn replay_matching(
        &self,
        crdt: &mut TimelineCrdt,
        speed: ReplaySpeed,
        include: impl Fn(&TaggedOperation) -> bool,
    ) {
        let mut previous: Option<SystemTime> = None;
        for op in &self.ops {
            if !include(op) {
                continue;
            }
            if let (ReplaySpeed::Realtime(factor), Some(prev)) = (speed, previous) {
                if factor > 0.0 {
                    if let Ok(gap) = op.timestamp.duration_since(prev) {
                        let scaled = gap.div_f64(factor);
                        if scaled > Duration::ZERO {
                            std::thread::sleep(scaled);
                        }
                    }
                }
            }
            previous = Some(op.timestamp);
            // A recorded operation was valid when it was produced; on
            // replay, only a truncated/corrupted log entry could be
            // rejected (e.g. a target deleted later in a log that was cut
            // short). Skip it and keep the rest of the session replaying
            // rather than aborting on one bad entry.
            let _ = crdt.apply_remote(op.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{ClipData, TimelineOperation};
    use crate::{ClipId, TrackData, TrackId};
    use std::time::Duration;
    use tpt_av_sync_utils::{OperationId, PeerId, VectorClock};

    fn tagged_at(lamport: u64, peer: PeerId, op: TimelineOperation, at: SystemTime) -> TaggedOperation {
        TaggedOperation {
            op_id: OperationId::new(lamport, peer),
            operation: op,
            lamport_ts: lamport,
            vector_clock: VectorClock::new(),
            peer_id: peer,
            timestamp: at,
        }
    }

    fn sample_ops(peer: PeerId, base: SystemTime) -> Vec<TaggedOperation> {
        let track = TrackId::from_u64(1);
        let clip = ClipId::from_u64(1);
        vec![
            tagged_at(
                1,
                peer,
                TimelineOperation::InsertTrack {
                    track_id: track,
                    track: TrackData::new("A1"),
                    position: 0,
                },
                base,
            ),
            tagged_at(
                2,
                peer,
                TimelineOperation::InsertClip {
                    clip_id: clip,
                    track_id: track,
                    clip: ClipData::new("a.wav", 0, 1_000),
                    position: 0,
                },
                base + Duration::from_secs(1),
            ),
            tagged_at(
                3,
                peer,
                TimelineOperation::TrimClip {
                    clip_id: clip,
                    new_start_frame: 0,
                    new_duration: 500,
                    edge: crate::TrimEdge::End,
                },
                base + Duration::from_secs(2),
            ),
        ]
    }

    #[test]
    fn replay_all_reproduces_final_state() {
        let peer = PeerId::from_u64(1);
        let base = SystemTime::UNIX_EPOCH;
        let mut source = TimelineCrdt::new(peer);
        for op in sample_ops(peer, base) {
            source.apply_remote(op).unwrap();
        }

        let recording = SessionRecording::new(source.operation_log().to_vec());
        let mut replayed = TimelineCrdt::new(PeerId::from_u64(2));
        recording.replay_all(&mut replayed, ReplaySpeed::Instant);

        assert_eq!(replayed.view(), source.view());
    }

    #[test]
    fn replay_until_stops_at_cutoff() {
        let peer = PeerId::from_u64(1);
        let base = SystemTime::UNIX_EPOCH;
        let recording = SessionRecording::new(sample_ops(peer, base));

        let mut at_insert_only = TimelineCrdt::new(PeerId::from_u64(2));
        recording.replay_until(
            &mut at_insert_only,
            ReplaySpeed::Instant,
            base + Duration::from_secs(1),
        );
        assert_eq!(at_insert_only.view().clips.len(), 1);
        assert_eq!(at_insert_only.view().clips[0].duration_frames, 1_000);

        let mut full = TimelineCrdt::new(PeerId::from_u64(3));
        recording.replay_all(&mut full, ReplaySpeed::Instant);
        assert_eq!(full.view().clips[0].duration_frames, 500);
    }

    #[test]
    fn recording_sorts_out_of_order_input_by_timestamp() {
        let peer = PeerId::from_u64(1);
        let base = SystemTime::UNIX_EPOCH;
        let mut ops = sample_ops(peer, base);
        ops.reverse();
        let recording = SessionRecording::new(ops);
        let timestamps: Vec<_> = recording.operations().iter().map(|o| o.timestamp).collect();
        let mut sorted = timestamps.clone();
        sorted.sort();
        assert_eq!(timestamps, sorted);
    }

    #[test]
    fn realtime_replay_paces_by_scaled_gaps() {
        let peer = PeerId::from_u64(1);
        let base = SystemTime::UNIX_EPOCH;
        // Two ops 100ms apart in capture time; replay at 50x speed should
        // take on the order of 2ms, not 100ms — bound generously to avoid
        // flaking on a loaded CI box.
        let ops = vec![
            tagged_at(
                1,
                peer,
                TimelineOperation::InsertTrack {
                    track_id: TrackId::from_u64(1),
                    track: TrackData::new("A1"),
                    position: 0,
                },
                base,
            ),
            tagged_at(
                2,
                peer,
                TimelineOperation::InsertClip {
                    clip_id: ClipId::from_u64(1),
                    track_id: TrackId::from_u64(1),
                    clip: ClipData::new("a.wav", 0, 1_000),
                    position: 0,
                },
                base + Duration::from_millis(100),
            ),
        ];
        let recording = SessionRecording::new(ops);
        let mut crdt = TimelineCrdt::new(PeerId::from_u64(2));
        let start = std::time::Instant::now();
        recording.replay_all(&mut crdt, ReplaySpeed::Realtime(50.0));
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "50x replay of a 100ms gap must finish well under the original duration"
        );
        assert_eq!(crdt.view().clips.len(), 1);
    }

    #[test]
    fn empty_recording_replays_to_nothing() {
        let recording = SessionRecording::new(Vec::new());
        assert!(recording.is_empty());
        assert_eq!(recording.time_range(), None);
        let mut crdt = TimelineCrdt::new(PeerId::from_u64(1));
        recording.replay_all(&mut crdt, ReplaySpeed::Instant);
        assert_eq!(crdt.view().clips.len(), 0);
    }
}
