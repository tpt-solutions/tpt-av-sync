//! Timeline operations — the replicated vocabulary of the CRDT.
//!
//! Every variant of [`TimelineOperation`] is designed to be **commutative**
//! (any application order yields the same state) and **idempotent**
//! (applying it twice changes nothing). All payload values are absolute
//! (never deltas relative to current state), which is what makes remote
//! application order-independent.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use tpt_av_sync_utils::{OperationId, PeerId, SyncError, VectorClock};

/// Unique identifier for a clip.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct ClipId(u64);

impl ClipId {
    /// Wraps a raw value; useful for deterministic tests.
    #[must_use]
    pub const fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Generates a fresh id (same generator as [`tpt_av_sync_utils::PeerId::generate`]).
    #[must_use]
    pub fn generate() -> Self {
        Self(tpt_av_sync_utils::peer_id::raw_generated_u64())
    }
}

impl std::fmt::Display for ClipId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "clip-{:016x}", self.0)
    }
}

/// Unique identifier for a track.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct TrackId(u64);

impl TrackId {
    /// Wraps a raw value; useful for deterministic tests.
    #[must_use]
    pub const fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Generates a fresh id.
    #[must_use]
    pub fn generate() -> Self {
        Self(tpt_av_sync_utils::peer_id::raw_generated_u64())
    }
}

impl std::fmt::Display for TrackId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "track-{:016x}", self.0)
    }
}

/// Which edge of a clip a trim operation grabs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrimEdge {
    /// The in-point (left edge) of the clip.
    Start,
    /// The out-point (right edge) of the clip.
    End,
}

/// The kind of content a track carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TrackKind {
    /// Audio track.
    Audio,
    /// Video track.
    Video,
    /// MIDI / instrument track.
    Midi,
    /// Bus / mix bus track.
    Bus,
}

/// Data describing a newly created clip.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClipData {
    /// Display name.
    pub name: String,
    /// Media source (path, URL, or asset id).
    pub source: String,
    /// First frame of the clip on the timeline.
    pub start_frame: u64,
    /// Length of the clip in frames/samples.
    pub duration_frames: u64,
    /// UI color (packed RGBA).
    pub color: u32,
}

impl ClipData {
    /// Creates clip data with default styling.
    #[must_use]
    pub fn new(name: impl Into<String>, start_frame: u64, duration_frames: u64) -> Self {
        Self {
            name: name.into(),
            source: String::new(),
            start_frame,
            duration_frames,
            color: 0xFF_AA_CC_88,
        }
    }
}

/// Per-field metadata updates for a clip. `None` fields are left unchanged.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ClipMetadataUpdate {
    /// New display name.
    pub name: Option<String>,
    /// New media source.
    pub source: Option<String>,
    /// New UI color.
    pub color: Option<u32>,
    /// New gain (linear).
    pub gain: Option<f32>,
    /// Muted flag.
    pub muted: Option<bool>,
    /// Locked flag.
    pub locked: Option<bool>,
}

/// Data describing a newly created track.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackData {
    /// Display name.
    pub name: String,
    /// Content kind.
    pub kind: TrackKind,
    /// Fader position in dB.
    pub volume_db: f32,
    /// Muted flag.
    pub muted: bool,
    /// Solo flag.
    pub solo: bool,
}

impl TrackData {
    /// Creates track data of the given kind with unity gain.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: TrackKind::Audio,
            volume_db: 0.0,
            muted: false,
            solo: false,
        }
    }
}

/// Per-field metadata updates for a track.
///
/// This operation family extends the original spec (which only had track
/// insert/delete); see DESIGN.md §"Deviations from spec.txt".
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TrackMetadataUpdate {
    /// New display name.
    pub name: Option<String>,
    /// New fader position in dB.
    pub volume_db: Option<f32>,
    /// Muted flag.
    pub muted: Option<bool>,
    /// Solo flag.
    pub solo: Option<bool>,
    /// New ordering position in the session.
    pub position: Option<u64>,
}

/// Per-field updates for session-level metadata.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionMetadataUpdate {
    /// Session name.
    pub name: Option<String>,
    /// Sample rate in Hz.
    pub sample_rate: Option<u32>,
    /// Tempo in beats per minute.
    pub tempo_bpm: Option<f64>,
    /// Time signature numerator.
    pub time_signature_numerator: Option<u32>,
    /// Time signature denominator.
    pub time_signature_denominator: Option<u32>,
}

/// What an automation envelope is attached to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TargetId {
    /// A clip-level envelope.
    Clip(ClipId),
    /// A track-level envelope.
    Track(TrackId),
}

/// The parameter an automation envelope controls.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EnvelopeType {
    /// Volume automation.
    Volume,
    /// Pan automation.
    Pan,
    /// Mute automation.
    Mute,
    /// Any custom parameter.
    Custom(String),
}

/// Curve shape between automation points.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub enum Interpolation {
    /// Straight line between points.
    #[default]
    Linear,
    /// Hold the previous value until the next point.
    Step,
    /// Smooth (spline) transition.
    Smooth,
}

/// A single point on an automation envelope.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EnvelopePoint {
    /// Timeline frame of the point.
    pub frame: u64,
    /// Parameter value at that frame.
    pub value: f32,
    /// Curve shape leaving this point.
    pub interpolation: Interpolation,
}

/// A timeline operation that can be replicated across peers.
///
/// All operations are commutative and idempotent: no matter what order they
/// arrive in, the final state is identical on every peer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimelineOperation {
    /// Insert a new clip into a track. Re-inserting an existing clip id
    /// overwrites its fields and resurrects it if it was deleted.
    InsertClip {
        /// Unique clip id (generated by the peer creating the clip).
        clip_id: ClipId,
        /// Target track id.
        track_id: TrackId,
        /// Clip data.
        clip: ClipData,
        /// Ordering hint within the track (purely informational; clips are
        /// ordered by start time).
        position: u64,
    },

    /// Move a clip to a new track and/or start time.
    MoveClip {
        /// Clip id.
        clip_id: ClipId,
        /// New track id (can be the same track).
        new_track_id: TrackId,
        /// New start time in frames/samples.
        new_start_frame: u64,
        /// New ordering hint within the track.
        new_position: u64,
    },

    /// Delete (tombstone) a clip. Like every mutating operation this
    /// requires the clip to exist (arrival before the insert is buffered by
    /// the CRDT engine, keeping the tombstone order-independent).
    DeleteClip {
        /// Clip id.
        clip_id: ClipId,
    },

    /// Split a clip at a frame offset relative to the clip start.
    ///
    /// The original clip keeps the range before `split_frame`; a new clip
    /// with id `new_clip_id` materializes the range from `split_frame` to
    /// the original's end. Concurrent splits of the same clip compose: the
    /// original plus one clip per distinct split point (spec §5.1).
    SplitClip {
        /// Original clip id.
        clip_id: ClipId,
        /// Split offset in frames, relative to the clip start.
        split_frame: u64,
        /// Id for the new (right-hand) clip.
        new_clip_id: ClipId,
    },

    /// Trim a clip by writing its absolute geometry.
    ///
    /// Both resulting values are carried explicitly (computed by the
    /// originating peer), which keeps the operation order-independent.
    /// `edge` records which edge the user grabbed — `Start` trims keep the
    /// out-point anchored, `End` trims keep the in-point anchored.
    TrimClip {
        /// Clip id.
        clip_id: ClipId,
        /// Resulting start frame (unchanged for `End`-edge trims).
        new_start_frame: u64,
        /// Resulting duration in frames.
        new_duration: u64,
        /// Which edge is being trimmed.
        edge: TrimEdge,
    },

    /// Update clip metadata fields.
    UpdateClipMetadata {
        /// Clip id.
        clip_id: ClipId,
        /// Metadata updates.
        updates: ClipMetadataUpdate,
    },

    /// Insert a new track. Re-inserting an existing track id overwrites its
    /// fields and resurrects it if it was deleted.
    InsertTrack {
        /// Track id.
        track_id: TrackId,
        /// Track data.
        track: TrackData,
        /// Ordering position in the session.
        position: u64,
    },

    /// Update track metadata fields (spec extension — see DESIGN.md).
    UpdateTrackMetadata {
        /// Track id.
        track_id: TrackId,
        /// Metadata updates.
        updates: TrackMetadataUpdate,
    },

    /// Delete (tombstone) a track. Clips on the track are hidden until the
    /// track is re-inserted, but keep their state.
    DeleteTrack {
        /// Track id.
        track_id: TrackId,
    },

    /// Replace the points of an automation envelope (LWW per
    /// `(target, envelope_type)`).
    UpdateEnvelope {
        /// Clip or track the envelope is attached to.
        target_id: TargetId,
        /// Envelope type (volume, pan, custom).
        envelope_type: EnvelopeType,
        /// The complete new set of envelope points.
        points: Vec<EnvelopePoint>,
    },

    /// Update session-level metadata.
    UpdateSessionMetadata {
        /// Metadata updates.
        updates: SessionMetadataUpdate,
    },
}

impl TimelineOperation {
    /// Validates structural payload limits for untrusted inputs: string
    /// lengths and envelope sizes (see
    /// [`tpt_av_sync_utils::security`] for the constants).
    ///
    /// The [`crate::TimelineCrdt`] applies this on every inbound operation;
    /// relays must apply it before forwarding or persisting. Cheap — no
    /// allocation beyond the error.
    ///
    /// # Errors
    ///
    /// [`SyncError::InvalidOperation`] when a field exceeds its limit.
    pub fn validate(&self) -> Result<(), SyncError> {
        use tpt_av_sync_utils::security::{validate_string, MAX_ENVELOPE_POINTS};
        match self {
            TimelineOperation::InsertClip { clip, .. } => {
                validate_string(&clip.name, "clip name")?;
                validate_string(&clip.source, "clip source")
            }
            TimelineOperation::UpdateClipMetadata { updates, .. } => {
                if let Some(v) = &updates.name {
                    validate_string(v, "clip name")?;
                }
                if let Some(v) = &updates.source {
                    validate_string(v, "clip source")?;
                }
                Ok(())
            }
            TimelineOperation::InsertTrack { track, .. } => {
                validate_string(&track.name, "track name")
            }
            TimelineOperation::UpdateTrackMetadata { updates, .. } => {
                if let Some(v) = &updates.name {
                    validate_string(v, "track name")?;
                }
                Ok(())
            }
            TimelineOperation::UpdateEnvelope {
                envelope_type, points, ..
            } => {
                if points.len() > MAX_ENVELOPE_POINTS {
                    return Err(SyncError::invalid(format!(
                        "envelope point count {} exceeds limit {MAX_ENVELOPE_POINTS}",
                        points.len()
                    )));
                }
                if let EnvelopeType::Custom(name) = envelope_type {
                    validate_string(name, "envelope parameter name")?;
                }
                Ok(())
            }
            TimelineOperation::UpdateSessionMetadata { updates } => {
                if let Some(v) = &updates.name {
                    validate_string(v, "session name")?;
                }
                Ok(())
            }
            TimelineOperation::MoveClip { .. }
            | TimelineOperation::DeleteClip { .. }
            | TimelineOperation::SplitClip { .. }
            | TimelineOperation::TrimClip { .. }
            | TimelineOperation::DeleteTrack { .. } => Ok(()),
        }
    }

    /// Returns the ids this operation reads or mutates, used for causal
    /// buffering of operations that arrive before their targets.
    #[must_use]
    pub fn required_targets(&self) -> Vec<RequiredTarget> {
        match self {
            TimelineOperation::InsertClip { .. } => Vec::new(),
            TimelineOperation::MoveClip { clip_id, .. }
            | TimelineOperation::SplitClip { clip_id, .. }
            | TimelineOperation::TrimClip { clip_id, .. }
            | TimelineOperation::UpdateClipMetadata { clip_id, .. } => {
                vec![RequiredTarget::Clip(*clip_id)]
            }
            TimelineOperation::DeleteClip { clip_id } => {
                vec![RequiredTarget::Clip(*clip_id)]
            }
            TimelineOperation::InsertTrack { .. } => Vec::new(),
            TimelineOperation::UpdateTrackMetadata { track_id, .. } => {
                vec![RequiredTarget::Track(*track_id)]
            }
            TimelineOperation::DeleteTrack { track_id } => {
                vec![RequiredTarget::Track(*track_id)]
            }
            TimelineOperation::UpdateEnvelope { target_id, .. } => match *target_id {
                TargetId::Clip(c) => vec![RequiredTarget::Clip(c)],
                TargetId::Track(t) => vec![RequiredTarget::Track(t)],
            },
            TimelineOperation::UpdateSessionMetadata { .. } => Vec::new(),
        }
    }
}

/// A target entity an operation requires to already exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredTarget {
    /// A clip that must exist.
    Clip(ClipId),
    /// A track that must exist.
    Track(TrackId),
}

/// An operation with the metadata needed for ordering and conflict
/// resolution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaggedOperation {
    /// Unique operation id (Lamport timestamp + peer id).
    pub op_id: OperationId,
    /// The operation itself.
    pub operation: TimelineOperation,
    /// Lamport timestamp for causal ordering.
    pub lamport_ts: u64,
    /// Vector clock of the creating peer at operation time.
    pub vector_clock: VectorClock,
    /// Peer id of the creator.
    pub peer_id: PeerId,
    /// Wall-clock timestamp (for display only, never for ordering).
    pub timestamp: SystemTime,
}

impl TaggedOperation {
    /// The deterministic application rank of this operation:
    /// `(lamport, peer)`.
    #[must_use]
    pub fn rank(&self) -> (u64, PeerId) {
        (self.lamport_ts, self.peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_av_sync_utils::wire;

    #[test]
    fn serde_roundtrip_of_every_operation() {
        let ops = vec![
            TimelineOperation::InsertClip {
                clip_id: ClipId::from_u64(1),
                track_id: TrackId::from_u64(2),
                clip: ClipData::new("a", 0, 10),
                position: 0,
            },
            TimelineOperation::MoveClip {
                clip_id: ClipId::from_u64(1),
                new_track_id: TrackId::from_u64(2),
                new_start_frame: 42,
                new_position: 1,
            },
            TimelineOperation::DeleteClip { clip_id: ClipId::from_u64(1) },
            TimelineOperation::SplitClip {
                clip_id: ClipId::from_u64(1),
                split_frame: 5,
                new_clip_id: ClipId::from_u64(3),
            },
            TimelineOperation::TrimClip {
                clip_id: ClipId::from_u64(1),
                new_start_frame: 2,
                new_duration: 7,
                edge: TrimEdge::Start,
            },
            TimelineOperation::UpdateClipMetadata {
                clip_id: ClipId::from_u64(1),
                updates: ClipMetadataUpdate {
                    name: Some("b".into()),
                    gain: Some(0.5),
                    ..Default::default()
                },
            },
            TimelineOperation::InsertTrack {
                track_id: TrackId::from_u64(2),
                track: TrackData::new("t"),
                position: 0,
            },
            TimelineOperation::UpdateTrackMetadata {
                track_id: TrackId::from_u64(2),
                updates: TrackMetadataUpdate {
                    name: Some("u".into()),
                    ..Default::default()
                },
            },
            TimelineOperation::DeleteTrack { track_id: TrackId::from_u64(2) },
            TimelineOperation::UpdateEnvelope {
                target_id: TargetId::Clip(ClipId::from_u64(1)),
                envelope_type: EnvelopeType::Volume,
                points: vec![EnvelopePoint {
                    frame: 1,
                    value: 0.5,
                    interpolation: Interpolation::Smooth,
                }],
            },
            TimelineOperation::UpdateSessionMetadata {
                updates: SessionMetadataUpdate {
                    name: Some("s".into()),
                    sample_rate: Some(48_000),
                    ..Default::default()
                },
            },
        ];
        for op in ops {
            let bytes = wire::encode(&op).expect("serialize");
            let back: TimelineOperation = wire::decode(&bytes).expect("deserialize");
            assert_eq!(back, op);
        }
    }

    #[test]
    fn required_targets_match_operation_shapes() {
        let insert = TimelineOperation::InsertClip {
            clip_id: ClipId::from_u64(1),
            track_id: TrackId::from_u64(2),
            clip: ClipData::new("a", 0, 1),
            position: 0,
        };
        assert!(insert.required_targets().is_empty());

        let move_op = TimelineOperation::MoveClip {
            clip_id: ClipId::from_u64(1),
            new_track_id: TrackId::from_u64(2),
            new_start_frame: 0,
            new_position: 0,
        };
        assert_eq!(
            move_op.required_targets(),
            vec![RequiredTarget::Clip(ClipId::from_u64(1))]
        );
    }

    #[test]
    fn tagged_operation_serde_roundtrip() {
        let tagged = TaggedOperation {
            op_id: OperationId::new(3, PeerId::from_u64(9)),
            operation: TimelineOperation::DeleteClip { clip_id: ClipId::from_u64(1) },
            lamport_ts: 3,
            vector_clock: VectorClock::new(),
            peer_id: PeerId::from_u64(9),
            timestamp: SystemTime::UNIX_EPOCH,
        };
        let bytes = wire::encode(&tagged).expect("serialize");
        let back: TaggedOperation = wire::decode(&bytes).expect("deserialize");
        assert_eq!(back, tagged);
    }
}
