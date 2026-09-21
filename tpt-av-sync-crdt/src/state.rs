//! The timeline state model: a session of tracks, clips, and envelopes.
//!
//! [`Session`] is a plain data structure that knows how to *apply*
//! [`TimelineOperation`]s and how to *materialize* the visible timeline
//! ([`Session::materialize`]). It holds no networking and no clocks —
//! [`crate::TimelineCrdt`] wraps it with clocks, the operation log, and
//! causal buffering.
//!
//! # Split inheritance
//!
//! A clip created by a split starts with most of its registers
//! *unwritten* (tagged [`OpTag::initial`]). While a register is unwritten,
//! its value is inherited from the parent clip chain: rename the original
//! and every un-renamed split piece follows; trim a piece and it keeps its
//! own geometry from then on. Resolving values lazily this way — instead of
//! copying them at creation time — is what makes concurrent splits
//! deterministic regardless of operation arrival order.

use crate::clip_crdt::{ClipCrdt, ClipParent};
use crate::envelope_crdt::EnvelopeStore;
use crate::merge::{LwwReg, OpTag};
use crate::operation::{
    ClipId, EnvelopePoint, EnvelopeType, TargetId, TimelineOperation, TrackId, TrackKind,
};
use crate::track_crdt::TrackCrdt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tpt_av_sync_utils::{PeerId, SyncError};

/// Session-level metadata as LWW registers.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionMetadataState {
    name: LwwReg<String>,
    sample_rate: LwwReg<u32>,
    tempo_bpm: LwwReg<f64>,
    ts_numerator: LwwReg<u32>,
    ts_denominator: LwwReg<u32>,
}

impl Default for SessionMetadataState {
    fn default() -> Self {
        Self {
            name: LwwReg::new_initial("Untitled Session".to_string()),
            sample_rate: LwwReg::new_initial(48_000),
            tempo_bpm: LwwReg::new_initial(120.0),
            ts_numerator: LwwReg::new_initial(4),
            ts_denominator: LwwReg::new_initial(4),
        }
    }
}

/// The current timeline state, as a bag of per-entity CRDTs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Session {
    metadata: SessionMetadataState,
    tracks: BTreeMap<TrackId, TrackCrdt>,
    clips: BTreeMap<ClipId, ClipCrdt>,
    envelopes: EnvelopeStore,
}

impl Session {
    /// Creates an empty session with default metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies an operation to the session state.
    ///
    /// The only error this can return is [`SyncError::UnknownTarget`] —
    /// operations that reference entities that do not (yet) exist. The
    /// caller (usually [`crate::TimelineCrdt`]) buffers such operations
    /// until their dependencies arrive; buffering deletes as well is what
    /// makes delete-before-insert delivery order-independent.
    pub fn apply(&mut self, op: &TimelineOperation, tag: OpTag) -> Result<(), SyncError> {
        match op {
            TimelineOperation::InsertClip {
                clip_id,
                track_id,
                clip,
                position,
            } => match self.clips.entry(*clip_id) {
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    slot.get_mut().apply(op, tag)?;
                }
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(ClipCrdt::from_insert(*track_id, clip, *position, tag));
                }
            },
            TimelineOperation::DeleteClip { clip_id } => {
                let rec = self.clips.get_mut(clip_id).ok_or(SyncError::UnknownTarget {
                    kind: "clip",
                    id: clip_id.as_u64(),
                })?;
                rec.apply(op, tag)?;
            }
            TimelineOperation::MoveClip { clip_id, .. }
            | TimelineOperation::TrimClip { clip_id, .. }
            | TimelineOperation::UpdateClipMetadata { clip_id, .. } => {
                let rec = self.clips.get_mut(clip_id).ok_or(SyncError::UnknownTarget {
                    kind: "clip",
                    id: clip_id.as_u64(),
                })?;
                rec.apply(op, tag)?;
            }
            TimelineOperation::SplitClip {
                clip_id,
                split_frame,
                new_clip_id,
            } => {
                self.apply_split(*clip_id, *split_frame, *new_clip_id, tag)?;
            }
            TimelineOperation::InsertTrack {
                track_id,
                track,
                position,
            } => match self.tracks.entry(*track_id) {
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    slot.get_mut().apply(op, tag)?;
                }
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(TrackCrdt::from_insert(track, *position, tag));
                }
            },
            TimelineOperation::UpdateTrackMetadata { track_id, .. } => {
                let rec = self.tracks.get_mut(track_id).ok_or(SyncError::UnknownTarget {
                    kind: "track",
                    id: track_id.as_u64(),
                })?;
                rec.apply(op, tag)?;
            }
            TimelineOperation::DeleteTrack { track_id } => {
                let rec = self.tracks.get_mut(track_id).ok_or(SyncError::UnknownTarget {
                    kind: "track",
                    id: track_id.as_u64(),
                })?;
                rec.apply(op, tag)?;
            }
            TimelineOperation::UpdateEnvelope {
                target_id,
                envelope_type,
                points,
            } => {
                let missing = match target_id {
                    TargetId::Clip(c) if !self.clips.contains_key(c) => {
                        Some(SyncError::UnknownTarget {
                            kind: "clip",
                            id: c.as_u64(),
                        })
                    }
                    TargetId::Track(t) if !self.tracks.contains_key(t) => {
                        Some(SyncError::UnknownTarget {
                            kind: "track",
                            id: t.as_u64(),
                        })
                    }
                    _ => None,
                };
                if let Some(err) = missing {
                    return Err(err);
                }
                self.envelopes
                    .set(*target_id, envelope_type.clone(), points.clone(), tag);
            }
            TimelineOperation::UpdateSessionMetadata { updates } => {
                if let Some(v) = &updates.name {
                    self.metadata.name.set(v.clone(), tag);
                }
                if let Some(v) = updates.sample_rate {
                    self.metadata.sample_rate.set(v, tag);
                }
                if let Some(v) = updates.tempo_bpm {
                    self.metadata.tempo_bpm.set(v, tag);
                }
                if let Some(v) = updates.time_signature_numerator {
                    self.metadata.ts_numerator.set(v, tag);
                }
                if let Some(v) = updates.time_signature_denominator {
                    self.metadata.ts_denominator.set(v, tag);
                }
            }
        }
        Ok(())
    }

    /// Records a split point on the parent clip and materializes the child.
    ///
    /// # Child-id ownership
    ///
    /// Two split operations may (pathologically or via undo/redo cycles)
    /// target the same child id from different parents. Ownership is
    /// resolved deterministically: the split with the smallest
    /// `(parent_id, offset)` claim owns the child. A losing operation is a
    /// no-op; when a smaller claim arrives after creation the child is
    /// re-parented and the stale split point is removed from the previous
    /// parent. Every replica therefore converges regardless of arrival
    /// order. Equal re-delivery resurrects the child at the operation's
    /// tag, which is what redo-after-undo relies on.
    fn apply_split(
        &mut self,
        clip_id: ClipId,
        split_frame: u64,
        new_clip_id: ClipId,
        tag: OpTag,
    ) -> Result<(), SyncError> {
        if clip_id == new_clip_id {
            return Ok(()); // degenerate self-split: deterministic no-op
        }
        let claim = (clip_id, split_frame);
        if let Some(current) = self
            .clips
            .get(&new_clip_id)
            .map(|child| child.origin_claim.unwrap_or(claim))
        {
            if claim > current {
                return Ok(()); // this split loses ownership: no-op
            }
            if claim < current {
                // Re-parent: drop the previous owner's split point.
                if let Some(old_parent) = self.clips.get_mut(&current.0) {
                    if old_parent.splits.get(&current.1) == Some(&new_clip_id) {
                        old_parent.splits.remove(&current.1);
                    }
                }
                let child = self.clips.get_mut(&new_clip_id).expect("checked above");
                child.origin_claim = Some(claim);
                child.parent = Some(ClipParent {
                    clip: clip_id,
                    offset: split_frame,
                });
            }
            let child = self.clips.get_mut(&new_clip_id).expect("checked above");
            child.alive.set(true, tag);
            let rec = self.clips.get_mut(&clip_id).ok_or(SyncError::UnknownTarget {
                kind: "clip",
                id: clip_id.as_u64(),
            })?;
            rec.record_split(split_frame, new_clip_id);
            return Ok(());
        }
        {
            let rec = self.clips.get_mut(&clip_id).ok_or(SyncError::UnknownTarget {
                kind: "clip",
                id: clip_id.as_u64(),
            })?;
            rec.apply(
                &TimelineOperation::SplitClip {
                    clip_id,
                    split_frame,
                    new_clip_id,
                },
                tag,
            )?;
        }
        let mut child = ClipCrdt::from_split(tag);
        child.parent = Some(ClipParent {
            clip: clip_id,
            offset: split_frame,
        });
        child.origin_claim = Some(claim);
        self.clips.insert(new_clip_id, child);
        Ok(())
    }

    /// Read-only access to a track's CRDT record.
    #[must_use]
    pub fn track(&self, track_id: &TrackId) -> Option<&TrackCrdt> {
        self.tracks.get(track_id)
    }

    /// Read-only access to a clip's CRDT record.
    #[must_use]
    pub fn clip(&self, clip_id: &ClipId) -> Option<&ClipCrdt> {
        self.clips.get(clip_id)
    }

    /// All track records, keyed by id.
    pub fn tracks(&self) -> impl Iterator<Item = (&TrackId, &TrackCrdt)> {
        self.tracks.iter()
    }

    /// All clip records, keyed by id.
    pub fn clips(&self) -> impl Iterator<Item = (&ClipId, &ClipCrdt)> {
        self.clips.iter()
    }

    /// The automation envelope store.
    #[must_use]
    pub const fn envelopes(&self) -> &EnvelopeStore {
        &self.envelopes
    }

    /// The LWW tags of the five session-metadata registers (name,
    /// sample_rate, tempo_bpm, ts_numerator, ts_denominator), for
    /// compaction (see `crate::compaction`). Order is not meaningful to
    /// callers — only the set of tags is.
    #[must_use]
    pub fn session_metadata_tags(&self) -> [crate::merge::OpTag; 5] {
        [
            self.metadata.name.tag(),
            self.metadata.sample_rate.tag(),
            self.metadata.tempo_bpm.tag(),
            self.metadata.ts_numerator.tag(),
            self.metadata.ts_denominator.tag(),
        ]
    }

    /// Mutable access to the automation envelope store (used by delta
    /// application).
    pub fn envelopes_mut(&mut self) -> &mut EnvelopeStore {
        &mut self.envelopes
    }

    /// Fully resolves a clip's inherited field values, geometry, and
    /// visibility. Returns `None` if the clip record does not exist.
    #[must_use]
    pub fn resolve_clip(&self, clip_id: &ClipId) -> Option<ResolvedClip> {
        let rec = self.clips.get(clip_id)?;
        let mut name = rec.name.get().clone();
        let mut source = rec.source.get().clone();
        let mut color = *rec.color.get();
        let mut gain = *rec.gain.get();
        let mut muted = *rec.muted.get();
        let mut locked = *rec.locked.get();
        let mut order_hint = *rec.order_hint.get();
        let mut track_id = *rec.track.get();

        // Inherit unwritten metadata fields from the parent chain.
        let mut cur = rec;
        while let Some(parent_link) = cur.parent {
            let parent = match self.clips.get(&parent_link.clip) {
                Some(p) => p,
                None => break,
            };
            if cur.name.tag() == OpTag::initial() {
                name = parent.name.get().clone();
            }
            if cur.source.tag() == OpTag::initial() {
                source = parent.source.get().clone();
            }
            if cur.color.tag() == OpTag::initial() {
                color = *parent.color.get();
            }
            if cur.gain.tag() == OpTag::initial() {
                gain = *parent.gain.get();
            }
            if cur.muted.tag() == OpTag::initial() {
                muted = *parent.muted.get();
            }
            if cur.locked.tag() == OpTag::initial() {
                locked = *parent.locked.get();
            }
            if cur.order_hint.tag() == OpTag::initial() {
                order_hint = *parent.order_hint.get();
            }
            if cur.track.tag() == OpTag::initial() {
                track_id = *parent.track.get();
            }
            cur = parent;
        }

        let geometry = self.resolve_geometry(rec);
        let alive = *rec.alive.get();
        let track_ok = self.tracks.get(&track_id).is_some_and(|t| *t.alive.get());
        let (start_frame, duration_frames) = geometry.unwrap_or((0, 0));
        let visible = alive && geometry.is_some() && track_ok && duration_frames > 0;

        Some(ResolvedClip {
            clip_id: *clip_id,
            track_id,
            name,
            source,
            color,
            gain,
            muted,
            locked,
            order_hint,
            start_frame,
            duration_frames,
            visible,
        })
    }

    /// Resolves a clip's visible geometry `(start, duration)`.
    ///
    /// The *full* range `[start, tail)` is resolved first (see
    /// [`Self::resolve_full_range`]); the visible extent is that range
    /// capped by the clip's own earliest split point, so a split always
    /// carves the tail off its target.
    fn resolve_geometry(&self, rec: &ClipCrdt) -> Option<(u64, u64)> {
        let (start, tail) = self.resolve_full_range(rec)?;
        let full = tail.saturating_sub(start);
        Some((start, rec.first_split_offset().unwrap_or(full).min(full)))
    }

    /// Resolves a clip's full `[start, tail)` range: the segment this clip
    /// would occupy if it had no split points of its own. Split children
    /// derive their ranges from the parent's *full* range, so the parent's
    /// own visible cap never shortens them.
    fn resolve_full_range(&self, rec: &ClipCrdt) -> Option<(u64, u64)> {
        match rec.parent {
            None => {
                // Root clip: registers are always written by InsertClip.
                if rec.start_frame.tag() == OpTag::initial()
                    || rec.duration_frames.tag() == OpTag::initial()
                {
                    return None;
                }
                let start = *rec.start_frame.get();
                Some((start, start.saturating_add(*rec.duration_frames.get())))
            }
            Some(parent_link) => {
                if rec.detached {
                    let start = *rec.start_frame.get();
                    return Some((start, start.saturating_add(*rec.duration_frames.get())));
                }
                let parent = self.clips.get(&parent_link.clip)?;
                if !*parent.alive.get() {
                    return None;
                }
                let (p_start, p_tail) = self.resolve_full_range(parent)?;
                let my_start = p_start.saturating_add(parent_link.offset);
                let boundary = p_start.saturating_add(
                    parent
                        .next_split_boundary_after(parent_link.offset)
                        .unwrap_or(p_tail.saturating_sub(p_start)),
                );
                Some((my_start, boundary.min(p_tail).max(my_start)))
            }
        }
    }

    /// Materializes the session into the plain, ordered view that
    /// applications render (and that tests compare for convergence).
    #[must_use]
    pub fn materialize(&self) -> SessionView {
        let metadata = SessionMetadata {
            name: self.metadata.name.get().clone(),
            sample_rate: *self.metadata.sample_rate.get(),
            tempo_bpm: *self.metadata.tempo_bpm.get(),
            time_signature: (
                *self.metadata.ts_numerator.get(),
                *self.metadata.ts_denominator.get(),
            ),
        };

        let mut tracks: Vec<TrackView> = self
            .tracks
            .iter()
            .filter(|(_, t)| *t.alive.get())
            .map(|(id, t)| TrackView {
                track_id: *id,
                name: t.name.get().clone(),
                kind: *t.kind.get(),
                volume_db: *t.volume_db.get(),
                muted: *t.muted.get(),
                solo: *t.solo.get(),
                position: *t.order_hint.get(),
            })
            .collect();
        tracks.sort_by_key(|t| (t.position, t.track_id.as_u64()));
        let track_order: BTreeMap<TrackId, u64> =
            tracks.iter().map(|t| (t.track_id, t.position)).collect();

        let mut clips: Vec<ClipView> = self
            .clips
            .keys()
            .filter_map(|id| self.resolve_clip(id))
            .filter(|c| c.visible)
            .map(|c| ClipView {
                clip_id: c.clip_id,
                track_id: c.track_id,
                name: c.name,
                source: c.source,
                color: c.color,
                gain: c.gain,
                muted: c.muted,
                locked: c.locked,
                start_frame: c.start_frame,
                duration_frames: c.duration_frames,
                order_hint: c.order_hint,
            })
            .collect();
        clips.sort_by_key(|c| {
            (
                track_order.get(&c.track_id).copied().unwrap_or(u64::MAX),
                c.track_id.as_u64(),
                c.start_frame,
                c.order_hint,
                c.clip_id.as_u64(),
            )
        });

        let envelopes: BTreeMap<(TargetId, EnvelopeType), Vec<EnvelopePoint>> = self
            .envelopes
            .iter()
            .map(|(t, e, pts)| ((*t, e.clone()), pts.to_vec()))
            .collect();

        SessionView {
            metadata,
            tracks,
            clips,
            envelopes,
        }
    }
}

/// A fully resolved clip: inherited values applied, geometry computed,
/// visibility decided.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedClip {
    /// The clip id.
    pub clip_id: ClipId,
    /// Resolved track (following the parent chain while unwritten).
    pub track_id: TrackId,
    /// Resolved display name.
    pub name: String,
    /// Resolved media source.
    pub source: String,
    /// Resolved UI color.
    pub color: u32,
    /// Resolved linear gain.
    pub gain: f32,
    /// Resolved muted flag.
    pub muted: bool,
    /// Resolved locked flag.
    pub locked: bool,
    /// Resolved ordering hint.
    pub order_hint: u64,
    /// Resolved start frame.
    pub start_frame: u64,
    /// Resolved duration (0 when the clip is not visible).
    pub duration_frames: u64,
    /// Whether the clip appears on the materialized timeline.
    pub visible: bool,
}

/// Materialized session metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMetadata {
    /// Session name.
    pub name: String,
    /// Sample rate in Hz.
    pub sample_rate: u32,
    /// Tempo in BPM.
    pub tempo_bpm: f64,
    /// Time signature `(numerator, denominator)`.
    pub time_signature: (u32, u32),
}

/// One materialized track.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackView {
    /// Track id.
    pub track_id: TrackId,
    /// Display name.
    pub name: String,
    /// Content kind.
    pub kind: TrackKind,
    /// Fader in dB.
    pub volume_db: f32,
    /// Muted flag.
    pub muted: bool,
    /// Solo flag.
    pub solo: bool,
    /// Ordering position.
    pub position: u64,
}

/// One materialized (visible) clip.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClipView {
    /// Clip id.
    pub clip_id: ClipId,
    /// Track the clip sits on.
    pub track_id: TrackId,
    /// Display name.
    pub name: String,
    /// Media source.
    pub source: String,
    /// UI color.
    pub color: u32,
    /// Linear gain.
    pub gain: f32,
    /// Muted flag.
    pub muted: bool,
    /// Locked flag.
    pub locked: bool,
    /// Start frame on the timeline.
    pub start_frame: u64,
    /// Duration in frames.
    pub duration_frames: u64,
    /// Ordering hint.
    pub order_hint: u64,
}

/// The materialized, ordered view of a session — what applications render.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionView {
    /// Session metadata.
    pub metadata: SessionMetadata,
    /// Visible tracks, ordered.
    pub tracks: Vec<TrackView>,
    /// Visible clips, ordered by track then time.
    pub clips: Vec<ClipView>,
    /// All automation envelopes.
    pub envelopes: BTreeMap<(TargetId, EnvelopeType), Vec<EnvelopePoint>>,
}

impl SessionView {
    /// Finds a clip by id.
    #[must_use]
    pub fn clip(&self, clip_id: &ClipId) -> Option<&ClipView> {
        self.clips.iter().find(|c| &c.clip_id == clip_id)
    }

    /// Finds a track by id.
    #[must_use]
    pub fn track(&self, track_id: &TrackId) -> Option<&TrackView> {
        self.tracks.iter().find(|t| &t.track_id == track_id)
    }
}

/// Convenience constructor for applying operations without a full
/// `TimelineCrdt` (tests, deltas): `(lamport, peer 0)`.
#[must_use]
pub fn tag_for(lamport: u64) -> OpTag {
    OpTag::new(lamport, PeerId::from_u64(0))
}
