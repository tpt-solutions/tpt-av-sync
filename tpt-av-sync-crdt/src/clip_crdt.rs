//! CRDT structure for an individual clip.
//!
//! A clip is a bag of [`LwwReg`] fields plus:
//!
//! - an optional `parent` link — clips created by a split remember which
//!   clip they were split from and at which offset;
//! - a `splits` map — the set of split *points* recorded on this clip
//!   (offset → new clip id), from which segment extents are derived;
//! - a `detached` flag — set once the clip receives its own explicit
//!   geometry (`MoveClip` / `TrimClip`), after which it no longer follows
//!   its parent's geometry.
//!
//! Because every field is an LWW register and `splits` is a set, applying
//! the same operations in any order converges to the same state.

use crate::merge::{LwwReg, OpTag};
use crate::operation::{
    ClipData, ClipMetadataUpdate, TimelineOperation, TrackId, TrimEdge,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tpt_av_sync_utils::SyncError;

/// The parent link of a clip created by a split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClipParent {
    /// The clip this clip was split from.
    pub clip: crate::operation::ClipId,
    /// Offset of this clip's start, relative to the parent clip's start.
    pub offset: u64,
}

/// CRDT state for one clip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipCrdt {
    /// Present when this clip was created by a split.
    pub parent: Option<ClipParent>,
    /// Liveness register: `true` while the clip exists.
    pub alive: LwwReg<bool>,
    /// The track the clip sits on.
    pub track: LwwReg<TrackId>,
    /// Start frame (authoritative only while `detached`).
    pub start_frame: LwwReg<u64>,
    /// Duration in frames (authoritative only while `detached`).
    pub duration_frames: LwwReg<u64>,
    /// Display name.
    pub name: LwwReg<String>,
    /// Media source.
    pub source: LwwReg<String>,
    /// UI color.
    pub color: LwwReg<u32>,
    /// Linear gain.
    pub gain: LwwReg<f32>,
    /// Muted flag.
    pub muted: LwwReg<bool>,
    /// Locked flag.
    pub locked: LwwReg<bool>,
    /// Informational ordering hint within the track.
    pub order_hint: LwwReg<u64>,
    /// True once the clip carries its own explicit geometry.
    pub detached: bool,
    /// Split points recorded on this clip: offset → new clip id.
    pub splits: BTreeMap<u64, crate::operation::ClipId>,
    /// The `(parent, offset)` claim that currently owns this clip — the
    /// smallest claim among all split operations targeting this id (see
    /// `Session::apply_split`). `None` for root clips.
    pub origin_claim: Option<(crate::operation::ClipId, u64)>,
}

impl ClipCrdt {
    /// Creates a clip from an `InsertClip` operation.
    #[must_use]
    pub(crate) fn from_insert(
        track_id: TrackId,
        clip: &ClipData,
        position: u64,
        tag: OpTag,
    ) -> Self {
        Self {
            parent: None,
            alive: LwwReg::new(true, tag),
            track: LwwReg::new(track_id, tag),
            start_frame: LwwReg::new(clip.start_frame, tag),
            duration_frames: LwwReg::new(clip.duration_frames, tag),
            name: LwwReg::new(clip.name.clone(), tag),
            source: LwwReg::new(clip.source.clone(), tag),
            color: LwwReg::new(clip.color, tag),
            gain: LwwReg::new(1.0, tag),
            muted: LwwReg::new(false, tag),
            locked: LwwReg::new(false, tag),
            order_hint: LwwReg::new(position, tag),
            detached: false,
            splits: BTreeMap::new(),
            origin_claim: None,
        }
    }

    /// Creates the right-hand clip produced by a split.
    ///
    /// With the exception of the liveness register, all field registers
    /// start *unwritten*: their values are inherited from the parent clip
    /// chain until this clip receives a direct write. Resolving lazily like
    /// this (rather than copying the parent's values at creation time)
    /// keeps the result independent of operation arrival order.
    #[must_use]
    pub fn from_split(tag: OpTag) -> Self {
        Self {
            parent: None, // filled in by the caller (needs the parent id)
            alive: LwwReg::new(true, tag),
            track: LwwReg::new_initial(TrackId::from_u64(0)),
            start_frame: LwwReg::new_initial(0),
            duration_frames: LwwReg::new_initial(0),
            name: LwwReg::new_initial(String::new()),
            source: LwwReg::new_initial(String::new()),
            color: LwwReg::new_initial(0),
            gain: LwwReg::new_initial(1.0),
            muted: LwwReg::new_initial(false),
            locked: LwwReg::new_initial(false),
            order_hint: LwwReg::new_initial(0),
            detached: false,
            splits: BTreeMap::new(),
            origin_claim: None,
        }
    }

    /// Applies an operation to this clip.
    ///
    /// Returns [`SyncError::UnknownTarget`] when the operation references a
    /// clip that does not exist (the caller decides whether to buffer it).
    /// Split-child creation is orchestrated by the session; this method only
    /// records the split point.
    pub fn apply(&mut self, op: &TimelineOperation, tag: OpTag) -> Result<(), SyncError> {
        match op {
            TimelineOperation::InsertClip {
                clip_id: _,
                track_id,
                clip,
                position,
            } => {
                // Every field is written at the op's tag — including the
                // defaults — so re-inserting an existing id converges to
                // the highest-ranked insert regardless of arrival order.
                self.alive.set(true, tag);
                self.track.set(*track_id, tag);
                self.start_frame.set(clip.start_frame, tag);
                self.duration_frames.set(clip.duration_frames, tag);
                self.name.set(clip.name.clone(), tag);
                self.source.set(clip.source.clone(), tag);
                self.color.set(clip.color, tag);
                self.gain.set(1.0, tag);
                self.muted.set(false, tag);
                self.locked.set(false, tag);
                self.order_hint.set(*position, tag);
            }
            TimelineOperation::MoveClip {
                new_track_id,
                new_start_frame,
                new_position,
                ..
            } => {
                self.track.set(*new_track_id, tag);
                self.start_frame.set(*new_start_frame, tag);
                self.order_hint.set(*new_position, tag);
                self.detached = true;
            }
            TimelineOperation::DeleteClip { .. } => {
                self.alive.set(false, tag);
            }
            TimelineOperation::SplitClip { split_frame, new_clip_id, .. } => {
                self.record_split(*split_frame, *new_clip_id);
            }
            TimelineOperation::TrimClip {
                new_start_frame,
                new_duration,
                ..
            } => {
                self.start_frame.set(*new_start_frame, tag);
                self.duration_frames.set(*new_duration, tag);
                self.detached = true;
            }
            TimelineOperation::UpdateClipMetadata { updates, .. } => {
                self.apply_metadata(updates, tag);
            }
            _ => {}
        }
        Ok(())
    }

    /// Records a split point; deterministic when two peers split at the
    /// same offset with different new clip ids (the smaller id wins).
    pub fn record_split(&mut self, offset: u64, new_clip_id: crate::operation::ClipId) {
        use std::collections::btree_map::Entry;
        match self.splits.entry(offset) {
            Entry::Occupied(mut slot) => {
                if new_clip_id < *slot.get() {
                    slot.insert(new_clip_id);
                }
            }
            Entry::Vacant(slot) => {
                slot.insert(new_clip_id);
            }
        }
    }

    /// Applies per-field metadata updates.
    pub fn apply_metadata(&mut self, updates: &ClipMetadataUpdate, tag: OpTag) {
        if let Some(v) = &updates.name {
            self.name.set(v.clone(), tag);
        }
        if let Some(v) = &updates.source {
            self.source.set(v.clone(), tag);
        }
        if let Some(v) = updates.color {
            self.color.set(v, tag);
        }
        if let Some(v) = updates.gain {
            self.gain.set(v, tag);
        }
        if let Some(v) = updates.muted {
            self.muted.set(v, tag);
        }
        if let Some(v) = updates.locked {
            self.locked.set(v, tag);
        }
    }

    /// Rebuilds this clip's creation payload as it currently stands — used
    /// to compute undo inverses and deltas.
    #[must_use]
    pub fn to_clip_data(&self) -> ClipData {
        ClipData {
            name: self.name.get().clone(),
            source: self.source.get().clone(),
            start_frame: *self.start_frame.get(),
            duration_frames: *self.duration_frames.get(),
            color: *self.color.get(),
        }
    }

    /// Whether the clip's earliest own split point bounds its extent, and
    /// at which offset. `None` when the clip has no split points.
    #[must_use]
    pub fn first_split_offset(&self) -> Option<u64> {
        self.splits.keys().next().copied()
    }

    /// The boundary (in parent-local frames) of the next split point after
    /// `offset`, i.e. where the segment starting at `offset` ends.
    #[must_use]
    pub fn next_split_boundary_after(&self, offset: u64) -> Option<u64> {
        self.splits.range(offset.saturating_add(1)..).next().map(|(k, _)| *k)
    }

    /// Trim-edge accessor kept for symmetry with the operation shape.
    #[must_use]
    pub fn trim_edge_is_start(edge: TrimEdge) -> bool {
        matches!(edge, TrimEdge::Start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{ClipId, TrackId};

    fn tag(l: u64) -> OpTag {
        OpTag::new(l, tpt_av_sync_utils::PeerId::from_u64(1))
    }

    fn inserted() -> ClipCrdt {
        ClipCrdt::from_insert(
            TrackId::from_u64(1),
            &ClipData::new("take", 100, 1000),
            0,
            tag(1),
        )
    }

    #[test]
    fn insert_then_move_then_stale_move_keeps_latest() {
        let mut clip = inserted();
        clip.apply(
            &TimelineOperation::MoveClip {
                clip_id: ClipId::from_u64(1),
                new_track_id: TrackId::from_u64(1),
                new_start_frame: 500,
                new_position: 0,
            },
            tag(2),
        )
        .unwrap();
        clip.apply(
            &TimelineOperation::MoveClip {
                clip_id: ClipId::from_u64(1),
                new_track_id: TrackId::from_u64(1),
                new_start_frame: 300,
                new_position: 0,
            },
            tag(1),
        )
        .unwrap();
        assert_eq!(clip.start_frame.get(), &500, "stale move must lose");
        assert!(clip.detached);
    }

    #[test]
    fn delete_then_stale_insert_stays_deleted() {
        let mut clip = inserted();
        clip.apply(&TimelineOperation::DeleteClip { clip_id: ClipId::from_u64(1) }, tag(5))
            .unwrap();
        clip.apply(
            &TimelineOperation::InsertClip {
                clip_id: ClipId::from_u64(1),
                track_id: TrackId::from_u64(1),
                clip: ClipData::new("take", 100, 1000),
                position: 0,
            },
            tag(3),
        )
        .unwrap();
        assert_eq!(clip.alive.get(), &false, "stale insert must not resurrect");
    }

    #[test]
    fn newer_insert_resurrects_deleted_clip() {
        let mut clip = inserted();
        clip.apply(&TimelineOperation::DeleteClip { clip_id: ClipId::from_u64(1) }, tag(2))
            .unwrap();
        clip.apply(
            &TimelineOperation::InsertClip {
                clip_id: ClipId::from_u64(1),
                track_id: TrackId::from_u64(1),
                clip: ClipData::new("revived", 0, 5),
                position: 0,
            },
            tag(3),
        )
        .unwrap();
        assert_eq!(clip.alive.get(), &true);
        assert_eq!(clip.name.get(), "revived");
    }

    #[test]
    fn same_offset_split_keeps_smaller_id() {
        let mut clip = inserted();
        clip.record_split(100, ClipId::from_u64(9));
        clip.record_split(100, ClipId::from_u64(4));
        assert_eq!(clip.splits.get(&100), Some(&ClipId::from_u64(4)));
        clip.record_split(100, ClipId::from_u64(50));
        assert_eq!(clip.splits.get(&100), Some(&ClipId::from_u64(4)));
    }

    #[test]
    fn split_boundaries_are_strictly_after_offset() {
        let mut clip = inserted();
        clip.record_split(100, ClipId::from_u64(2));
        clip.record_split(200, ClipId::from_u64(3));
        assert_eq!(clip.next_split_boundary_after(0), Some(100));
        assert_eq!(clip.next_split_boundary_after(100), Some(200));
        assert_eq!(clip.next_split_boundary_after(200), None);
        assert_eq!(clip.first_split_offset(), Some(100));
    }

    #[test]
    fn metadata_update_only_touches_present_fields() {
        let mut clip = inserted();
        clip.apply_metadata(
            &ClipMetadataUpdate {
                gain: Some(0.25),
                ..Default::default()
            },
            tag(2),
        );
        assert_eq!(clip.gain.get(), &0.25);
        assert_eq!(clip.name.get(), "take");
    }

    #[test]
    fn to_clip_data_roundtrips_values() {
        let clip = inserted();
        let data = clip.to_clip_data();
        assert_eq!(data.name, "take");
        assert_eq!(data.start_frame, 100);
        assert_eq!(data.duration_frames, 1000);
    }
}
