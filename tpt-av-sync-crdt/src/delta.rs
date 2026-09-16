//! Delta compression: diff two materialized sessions into a minimal set of
//! operations (spec §7.2).
//!
//! Instead of shipping a full snapshot when states diverge,
//! [`compute_delta`] produces the operations that transform `old` into
//! `new`. All emitted operations carry absolute values, so they are safe to
//! apply in any order after their inserts.

use crate::operation::{
    ClipData, ClipMetadataUpdate, SessionMetadataUpdate, TimelineOperation, TrackData,
    TrackMetadataUpdate, TrimEdge,
};
use crate::state::{Session, SessionView};

/// Computes the difference between two timeline views as a list of
/// operations that, when applied to a session in `old`'s state, produce a
/// state equal to `new`.
///
/// Track inserts come first, then per-entity mutations, then deletes, so
/// the list can be applied naïvely without dependency buffering.
#[must_use]
pub fn compute_delta(old: &SessionView, new: &SessionView) -> Vec<TimelineOperation> {
    let mut delta = Vec::new();

    // --- tracks: inserts and field updates ---
    for track in &new.tracks {
        match old.track(&track.track_id) {
            None => delta.push(TimelineOperation::InsertTrack {
                track_id: track.track_id,
                track: TrackData {
                    name: track.name.clone(),
                    kind: track.kind,
                    volume_db: track.volume_db,
                    muted: track.muted,
                    solo: track.solo,
                },
                position: track.position,
            }),
            Some(prev) => {
                let mut updates = TrackMetadataUpdate::default();
                if prev.name != track.name {
                    updates.name = Some(track.name.clone());
                }
                if prev.volume_db != track.volume_db {
                    updates.volume_db = Some(track.volume_db);
                }
                if prev.muted != track.muted {
                    updates.muted = Some(track.muted);
                }
                if prev.solo != track.solo {
                    updates.solo = Some(track.solo);
                }
                if prev.position != track.position {
                    updates.position = Some(track.position);
                }
                if updates != TrackMetadataUpdate::default() {
                    delta.push(TimelineOperation::UpdateTrackMetadata {
                        track_id: track.track_id,
                        updates,
                    });
                }
            }
        }
    }
    for track in &old.tracks {
        if new.track(&track.track_id).is_none() {
            delta.push(TimelineOperation::DeleteTrack {
                track_id: track.track_id,
            });
        }
    }

    // --- session metadata ---
    {
        let mut updates = SessionMetadataUpdate::default();
        if old.metadata.name != new.metadata.name {
            updates.name = Some(new.metadata.name.clone());
        }
        if old.metadata.sample_rate != new.metadata.sample_rate {
            updates.sample_rate = Some(new.metadata.sample_rate);
        }
        if old.metadata.tempo_bpm != new.metadata.tempo_bpm {
            updates.tempo_bpm = Some(new.metadata.tempo_bpm);
        }
        if old.metadata.time_signature != new.metadata.time_signature {
            updates.time_signature_numerator = Some(new.metadata.time_signature.0);
            updates.time_signature_denominator = Some(new.metadata.time_signature.1);
        }
        if updates != SessionMetadataUpdate::default() {
            delta.push(TimelineOperation::UpdateSessionMetadata { updates });
        }
    }

    // --- clips: inserts and mutations ---
    for clip in &new.clips {
        match old.clip(&clip.clip_id) {
            None => delta.push(TimelineOperation::InsertClip {
                clip_id: clip.clip_id,
                track_id: clip.track_id,
                clip: ClipData {
                    name: clip.name.clone(),
                    source: clip.source.clone(),
                    start_frame: clip.start_frame,
                    duration_frames: clip.duration_frames,
                    color: clip.color,
                },
                position: clip.order_hint,
            }),
            Some(prev) => {
                if prev.track_id != clip.track_id
                    || prev.start_frame != clip.start_frame
                    || prev.order_hint != clip.order_hint
                {
                    delta.push(TimelineOperation::MoveClip {
                        clip_id: clip.clip_id,
                        new_track_id: clip.track_id,
                        new_start_frame: clip.start_frame,
                        new_position: clip.order_hint,
                    });
                }
                if prev.duration_frames != clip.duration_frames {
                    delta.push(TimelineOperation::TrimClip {
                        clip_id: clip.clip_id,
                        new_start_frame: clip.start_frame,
                        new_duration: clip.duration_frames,
                        edge: TrimEdge::End,
                    });
                }
                let mut updates = ClipMetadataUpdate::default();
                if prev.name != clip.name {
                    updates.name = Some(clip.name.clone());
                }
                if prev.source != clip.source {
                    updates.source = Some(clip.source.clone());
                }
                if prev.color != clip.color {
                    updates.color = Some(clip.color);
                }
                if prev.gain != clip.gain {
                    updates.gain = Some(clip.gain);
                }
                if prev.muted != clip.muted {
                    updates.muted = Some(clip.muted);
                }
                if prev.locked != clip.locked {
                    updates.locked = Some(clip.locked);
                }
                if updates != ClipMetadataUpdate::default() {
                    delta.push(TimelineOperation::UpdateClipMetadata {
                        clip_id: clip.clip_id,
                        updates,
                    });
                }
            }
        }
    }
    for clip in &old.clips {
        if new.clip(&clip.clip_id).is_none() {
            delta.push(TimelineOperation::DeleteClip {
                clip_id: clip.clip_id,
            });
        }
    }

    // --- envelopes ---
    for ((target, env_type), points) in &new.envelopes {
        if old.envelopes.get(&(*target, env_type.clone())) != Some(points) {
            delta.push(TimelineOperation::UpdateEnvelope {
                target_id: target.clone(),
                envelope_type: env_type.clone(),
                points: points.clone(),
            });
        }
    }

    delta
}

/// Applies a delta directly to a session state.
///
/// Each operation is applied with a synthetic tag of
/// `(base_tag.lamport + index, base_tag.peer)`, giving the delta strictly
/// increasing ranks. Pass a `base_tag` whose Lamport component exceeds
/// every tag already recorded in the session so the delta wins all
/// last-writer-wins comparisons.
///
/// Prefer routing deltas through [`crate::TimelineCrdt::apply_local`] when
/// the delta should also be replicated and undoable — this function mutates
/// raw state only.
pub fn apply_delta(
    session: &mut Session,
    delta: &[TimelineOperation],
    base_tag: crate::merge::OpTag,
) {
    for (index, op) in delta.iter().enumerate() {
        let tag = crate::merge::OpTag {
            lamport: base_tag.lamport.saturating_add(index as u64),
            peer: base_tag.peer,
        };
        let _ = session.apply(op, tag);
    }
}
