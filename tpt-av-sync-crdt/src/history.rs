//! Operation history: undo/redo support.
//!
//! Every successfully applied *local* operation records an inverse. Undoing
//! re-applies the inverse as a fresh local operation (with a new Lamport
//! timestamp), which keeps undo replication-safe: other peers receive it as
//! an ordinary operation and converge.

use crate::operation::{ClipData, ClipMetadataUpdate, TimelineOperation, TrackMetadataUpdate};
use crate::state::Session;

/// A recorded operation and its inverse.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryEntry {
    /// The user-facing operation (re-applied by *redo*).
    pub forward: TimelineOperation,
    /// The operation that undoes `forward` (applied by *undo*).
    pub inverse: TimelineOperation,
}

/// Bounded undo/redo stack.
#[derive(Debug, Default)]
pub struct History {
    undo_stack: Vec<HistoryEntry>,
    redo_stack: Vec<HistoryEntry>,
    capacity: usize,
}

impl History {
    /// Creates a history with the given undo depth.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            capacity: capacity.max(1),
        }
    }

    /// Records a new operation: pushes onto the undo stack and clears redo.
    pub fn record(&mut self, entry: HistoryEntry) {
        self.redo_stack.clear();
        if self.undo_stack.len() >= self.capacity {
            self.undo_stack.remove(0);
        }
        self.undo_stack.push(entry);
    }

    /// Pops the most recent entry for undo.
    pub fn pop_undo(&mut self) -> Option<HistoryEntry> {
        self.undo_stack.pop()
    }

    /// Parks a popped entry on the redo stack.
    pub fn push_redo(&mut self, entry: HistoryEntry) {
        self.redo_stack.push(entry);
    }

    /// Pops the most recent entry for redo.
    pub fn pop_redo(&mut self) -> Option<HistoryEntry> {
        self.redo_stack.pop()
    }

    /// Returns a popped entry back onto the undo stack (after redo).
    pub fn push_undo(&mut self, entry: HistoryEntry) {
        self.undo_stack.push(entry);
    }

    /// Number of undoable operations.
    #[must_use]
    pub fn undo_len(&self) -> usize {
        self.undo_stack.len()
    }

    /// Number of redoable operations.
    #[must_use]
    pub fn redo_len(&self) -> usize {
        self.redo_stack.len()
    }

    /// Clears all history.
    pub fn clear(&mut self) {
        self.undo_stack.clear();
        self.redo_stack.clear();
    }
}

/// Computes the inverse of `op` against the session state *as it was before
/// the operation was applied*. Returns `None` when the operation is a no-op
/// (nothing to undo).
#[must_use]
pub fn compute_inverse(op: &TimelineOperation, session: &Session) -> Option<TimelineOperation> {
    match op {
        TimelineOperation::InsertClip {
            clip_id,
            track_id,
            clip,
            position,
        } => {
            if session.clip(clip_id).is_some() {
                // Overwrite of an existing clip: restore its prior values.
                let prev = session.resolve_clip(clip_id)?;
                Some(TimelineOperation::InsertClip {
                    clip_id: *clip_id,
                    track_id: prev.track_id,
                    clip: ClipData {
                        name: prev.name,
                        source: prev.source,
                        start_frame: prev.start_frame,
                        duration_frames: prev.duration_frames,
                        color: prev.color,
                    },
                    position: prev.order_hint,
                })
            } else {
                let _ = (track_id, clip, position);
                Some(TimelineOperation::DeleteClip { clip_id: *clip_id })
            }
        }
        TimelineOperation::MoveClip { clip_id, .. } => {
            let prev = session.resolve_clip(clip_id)?;
            Some(TimelineOperation::MoveClip {
                clip_id: *clip_id,
                new_track_id: prev.track_id,
                new_start_frame: prev.start_frame,
                new_position: prev.order_hint,
            })
        }
        TimelineOperation::DeleteClip { clip_id } => {
            let prev = session.resolve_clip(clip_id)?;
            Some(TimelineOperation::InsertClip {
                clip_id: *clip_id,
                track_id: prev.track_id,
                clip: ClipData {
                    name: prev.name,
                    source: prev.source,
                    start_frame: prev.start_frame,
                    duration_frames: prev.duration_frames,
                    color: prev.color,
                },
                position: prev.order_hint,
            })
        }
        TimelineOperation::SplitClip {
            clip_id: _,
            new_clip_id,
            ..
        } => {
            if session.clip(new_clip_id).is_some() {
                // Child id already existed: the split was a no-op.
                None
            } else {
                Some(TimelineOperation::DeleteClip { clip_id: *new_clip_id })
            }
        }
        TimelineOperation::TrimClip { clip_id, edge, .. } => {
            let prev = session.resolve_clip(clip_id)?;
            Some(TimelineOperation::TrimClip {
                clip_id: *clip_id,
                new_start_frame: prev.start_frame,
                new_duration: prev.duration_frames,
                edge: *edge,
            })
        }
        TimelineOperation::UpdateClipMetadata { clip_id, updates } => {
            let prev = session.resolve_clip(clip_id)?;
            Some(TimelineOperation::UpdateClipMetadata {
                clip_id: *clip_id,
                updates: ClipMetadataUpdate {
                    name: updates.name.clone().map(|_| prev.name),
                    source: updates.source.clone().map(|_| prev.source),
                    color: updates.color.map(|_| prev.color),
                    gain: updates.gain.map(|_| prev.gain),
                    muted: updates.muted.map(|_| prev.muted),
                    locked: updates.locked.map(|_| prev.locked),
                },
            })
        }
        TimelineOperation::InsertTrack {
            track_id,
            track,
            position,
        } => {
            if session.track(track_id).is_some() {
                let prev = session.track(track_id)?;
                let data = prev.to_track_data();
                Some(TimelineOperation::InsertTrack {
                    track_id: *track_id,
                    track: data,
                    position: *prev.order_hint.get(),
                })
            } else {
                let _ = (track, position);
                Some(TimelineOperation::DeleteTrack { track_id: *track_id })
            }
        }
        TimelineOperation::UpdateTrackMetadata { track_id, updates } => {
            let prev = session.track(track_id)?;
            Some(TimelineOperation::UpdateTrackMetadata {
                track_id: *track_id,
                updates: TrackMetadataUpdate {
                    name: updates.name.clone().map(|_| prev.name.get().clone()),
                    volume_db: updates.volume_db.map(|_| *prev.volume_db.get()),
                    muted: updates.muted.map(|_| *prev.muted.get()),
                    solo: updates.solo.map(|_| *prev.solo.get()),
                    position: updates.position.map(|_| *prev.order_hint.get()),
                },
            })
        }
        TimelineOperation::DeleteTrack { track_id } => {
            let prev = session.track(track_id)?;
            let data = prev.to_track_data();
            Some(TimelineOperation::InsertTrack {
                track_id: *track_id,
                track: data,
                position: *prev.order_hint.get(),
            })
        }
        TimelineOperation::UpdateEnvelope {
            target_id,
            envelope_type,
            ..
        } => {
            let old_points = session
                .envelopes()
                .get(target_id, envelope_type)
                .unwrap_or(&[])
                .to_vec();
            Some(TimelineOperation::UpdateEnvelope {
                target_id: *target_id,
                envelope_type: envelope_type.clone(),
                points: old_points,
            })
        }
        TimelineOperation::UpdateSessionMetadata { updates } => {
            let current = session.materialize().metadata;
            Some(compute_session_metadata_inverse(updates, &current))
        }
    }
}

/// Inverse for [`TimelineOperation::UpdateSessionMetadata`], which needs
/// the materialized metadata — kept as a separate function because it
/// cannot fall back to per-entity resolution.
#[must_use]
pub fn compute_session_metadata_inverse(
    updates: &crate::operation::SessionMetadataUpdate,
    current: &crate::state::SessionMetadata,
) -> TimelineOperation {
    use crate::operation::SessionMetadataUpdate;
    TimelineOperation::UpdateSessionMetadata {
        updates: SessionMetadataUpdate {
            name: updates.name.clone().map(|_| current.name.clone()),
            sample_rate: updates.sample_rate.map(|_| current.sample_rate),
            tempo_bpm: updates.tempo_bpm.map(|_| current.tempo_bpm),
            time_signature_numerator: updates
                .time_signature_numerator
                .map(|_| current.time_signature.0),
            time_signature_denominator: updates
                .time_signature_denominator
                .map(|_| current.time_signature.1),
        },
    }
}
