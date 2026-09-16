//! CRDT structure for an individual track.

use crate::merge::{LwwReg, OpTag};
use crate::operation::{TimelineOperation, TrackData};
use serde::{Deserialize, Serialize};
use tpt_av_sync_utils::SyncError;

/// CRDT state for one track.
///
/// All fields are last-writer-wins registers; `alive` gates visibility of
/// the track itself *and* of the clips that sit on it (deleting a track
/// hides its clips without destroying them).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackCrdt {
    /// Liveness register: `true` while the track exists.
    pub alive: LwwReg<bool>,
    /// Display name.
    pub name: LwwReg<String>,
    /// Content kind.
    pub kind: LwwReg<crate::operation::TrackKind>,
    /// Fader position in dB.
    pub volume_db: LwwReg<f32>,
    /// Muted flag.
    pub muted: LwwReg<bool>,
    /// Solo flag.
    pub solo: LwwReg<bool>,
    /// Ordering position within the session (informational hint).
    pub order_hint: LwwReg<u64>,
}

impl TrackCrdt {
    /// Creates a track from an `InsertTrack` operation.
    #[must_use]
    pub fn from_insert(track: &TrackData, position: u64, tag: OpTag) -> Self {
        Self {
            alive: LwwReg::new(true, tag),
            name: LwwReg::new(track.name.clone(), tag),
            kind: LwwReg::new(track.kind, tag),
            volume_db: LwwReg::new(track.volume_db, tag),
            muted: LwwReg::new(track.muted, tag),
            solo: LwwReg::new(track.solo, tag),
            order_hint: LwwReg::new(position, tag),
        }
    }

    /// Applies an operation to this track.
    pub fn apply(&mut self, op: &TimelineOperation, tag: OpTag) -> Result<(), SyncError> {
        match op {
            TimelineOperation::InsertTrack { track, position, .. } => {
                self.alive.set(true, tag);
                self.name.set(track.name.clone(), tag);
                self.kind.set(track.kind, tag);
                self.volume_db.set(track.volume_db, tag);
                self.muted.set(track.muted, tag);
                self.solo.set(track.solo, tag);
                self.order_hint.set(*position, tag);
            }
            TimelineOperation::UpdateTrackMetadata { updates, .. } => {
                if let Some(v) = &updates.name {
                    self.name.set(v.clone(), tag);
                }
                if let Some(v) = updates.volume_db {
                    self.volume_db.set(v, tag);
                }
                if let Some(v) = updates.muted {
                    self.muted.set(v, tag);
                }
                if let Some(v) = updates.solo {
                    self.solo.set(v, tag);
                }
                if let Some(v) = updates.position {
                    self.order_hint.set(v, tag);
                }
            }
            TimelineOperation::DeleteTrack { .. } => {
                self.alive.set(false, tag);
            }
            _ => {}
        }
        Ok(())
    }

    /// Rebuilds this track's creation payload as it currently stands — used
    /// to compute undo inverses and deltas.
    #[must_use]
    pub fn to_track_data(&self) -> TrackData {
        TrackData {
            name: self.name.get().clone(),
            kind: *self.kind.get(),
            volume_db: *self.volume_db.get(),
            muted: *self.muted.get(),
            solo: *self.solo.get(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{TrackId, TrackKind, TrackMetadataUpdate};

    fn tag(l: u64) -> OpTag {
        OpTag::new(l, tpt_av_sync_utils::PeerId::from_u64(1))
    }

    #[test]
    fn delete_hides_track_and_stale_insert_does_not_resurrect() {
        let mut track = TrackCrdt::from_insert(&TrackData::new("drums"), 0, tag(1));
        track.apply(&TimelineOperation::DeleteTrack { track_id: TrackId::from_u64(7) }, tag(4))
            .unwrap();
        track.apply(
            &TimelineOperation::InsertTrack {
                track_id: TrackId::from_u64(7),
                track: TrackData::new("drums"),
                position: 0,
            },
            tag(2),
        )
        .unwrap();
        assert_eq!(track.alive.get(), &false);
    }

    #[test]
    fn metadata_update_changes_only_present_fields() {
        let mut track = TrackCrdt::from_insert(&TrackData::new("drums"), 2, tag(1));
        track.apply(
            &TimelineOperation::UpdateTrackMetadata {
                track_id: TrackId::from_u64(7),
                updates: TrackMetadataUpdate {
                    solo: Some(true),
                    position: Some(5),
                    ..Default::default()
                },
            },
            tag(2),
        )
        .unwrap();
        assert_eq!(track.solo.get(), &true);
        assert_eq!(track.order_hint.get(), &5);
        assert_eq!(track.name.get(), "drums");
    }

    #[test]
    fn to_track_data_reflects_state() {
        let mut track = TrackCrdt::from_insert(&TrackData::new("gtr"), 0, tag(1));
        track.apply(
            &TimelineOperation::UpdateTrackMetadata {
                track_id: TrackId::from_u64(1),
                updates: TrackMetadataUpdate {
                    volume_db: Some(-6.0),
                    ..Default::default()
                },
            },
            tag(2),
        )
        .unwrap();
        let data = track.to_track_data();
        assert_eq!(data.kind, TrackKind::Audio);
        assert_eq!(data.volume_db, -6.0);
    }
}
