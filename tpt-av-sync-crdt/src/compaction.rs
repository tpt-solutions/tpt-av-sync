//! Operation-log compaction: bounds `TimelineCrdt::operation_log`'s growth
//! for long-running sessions, without changing materialized state.
//!
//! `TimelineSnapshot` is replay-based (see `timeline_crdt.rs`), so its size
//! — and the in-memory operation log it's built from — grows with every
//! operation ever applied, even ones every currently-live field has long
//! since superseded. [`compact`] drops exactly those: an operation is kept
//! only if it is still the reason some field holds its current value.
//!
//! # How retention is decided
//!
//! Every mutable field in the CRDT (`ClipCrdt`/`TrackCrdt`/`EnvelopeStore`/
//! session metadata) is an [`crate::merge::LwwReg`] that remembers the
//! [`OpTag`] (`(lamport, peer)`) of whichever write currently wins it. That
//! pair is exactly an [`OperationId`] — globally unique, since Lamport
//! counters only increase per peer. So for any field, "the operation that
//! produced its current value" can be found by exact `OperationId` lookup
//! against the full log, no per-operation-kind knowledge required: build
//! `OperationId -> &TaggedOperation` once, then look up every field's
//! current tag across every entity. Only the operations reached this way
//! survive compaction; a rank-ordered replay of the survivors reproduces
//! the exact original session, because it always applies to each field
//! whichever value the tag comparison says wins — trivially, if all we
//! keep for a field is a no-op subset of an already-idempotent register's
//! writes, we necessarily keep at least the winning one, and the loser
//! writes contribute nothing on replay whether they are present or not.
//!
//! Two things additionally need explicit handling, because they aren't
//! LWW-register writes:
//!
//! - **Bootstrap.** `Session::apply` only creates a `ClipCrdt`/`TrackCrdt`
//!   entry when it sees an `InsertClip`/`InsertTrack` on a vacant slot,
//!   before any per-field tag lookup has anything to find. One such
//!   operation per non-split-child entity is always kept regardless of
//!   whether any of *its own* fields are still the current tag-owner.
//! - **Splits are never compacted.** A split's outcome depends on
//!   `record_split`'s per-offset "smaller id wins" and `apply_split`'s
//!   per-child `origin_claim` — deterministic, but tag-agnostic tie-breaks,
//!   not registers with a single "current tag" to look up. Reconstructing
//!   those correctly from a filtered subset would need to re-derive rules
//!   that already live in `state.rs`, which risks quietly diverging from
//!   them. Splits are a small fraction of most sessions' operations next
//!   to routine moves/trims/renames, so keeping every `SplitClip` verbatim
//!   still bounds the dominant source of log growth while adding zero risk
//!   to the trickiest part of the CRDT.

use crate::merge::OpTag;
use crate::operation::{ClipId, TaggedOperation, TimelineOperation, TrackId};
use crate::state::Session;
use std::collections::{HashMap, HashSet};
use tpt_av_sync_utils::OperationId;

fn op_id_of(tag: OpTag) -> OperationId {
    OperationId::new(tag.lamport, tag.peer)
}

fn retain_tag(
    keep: &mut HashSet<OperationId>,
    by_id: &HashMap<OperationId, &TaggedOperation>,
    tag: OpTag,
) {
    if tag == OpTag::initial() {
        return; // never written; no corresponding operation exists.
    }
    let id = op_id_of(tag);
    if by_id.contains_key(&id) {
        keep.insert(id);
    }
}

/// Computes the compacted form of `ops` for the current state of `session`.
///
/// `ops` should be `session`'s own history (typically
/// `TimelineCrdt::operation_log()`); the result is safe to replay from
/// scratch via `TimelineCrdt::apply_remote` in place of the original log —
/// see the module docs for why this reproduces identical state.
#[must_use]
pub fn compact(ops: &[TaggedOperation], session: &Session) -> Vec<TaggedOperation> {
    let by_id: HashMap<OperationId, &TaggedOperation> =
        ops.iter().map(|op| (op.op_id, op)).collect();
    let mut keep: HashSet<OperationId> = HashSet::new();

    // Splits: keep every one, unconditionally (see module docs).
    for op in ops {
        if matches!(op.operation, TimelineOperation::SplitClip { .. }) {
            keep.insert(op.op_id);
        }
    }

    // Bootstrap: one InsertClip per root (non-split-child) clip, one
    // InsertTrack per track. Split-created clips are bootstrapped by their
    // (already unconditionally kept) creating SplitClip instead.
    let mut seen_clip_insert: HashSet<ClipId> = HashSet::new();
    let mut seen_track_insert: HashSet<TrackId> = HashSet::new();
    for op in ops {
        match &op.operation {
            TimelineOperation::InsertClip { clip_id, .. } if seen_clip_insert.insert(*clip_id) => {
                keep.insert(op.op_id);
            }
            TimelineOperation::InsertTrack { track_id, .. }
                if seen_track_insert.insert(*track_id) =>
            {
                keep.insert(op.op_id);
            }
            _ => {}
        }
    }

    // Every clip's LWW fields.
    for (_, clip) in session.clips() {
        for tag in [
            clip.alive.tag(),
            clip.track.tag(),
            clip.start_frame.tag(),
            clip.duration_frames.tag(),
            clip.name.tag(),
            clip.source.tag(),
            clip.color.tag(),
            clip.gain.tag(),
            clip.muted.tag(),
            clip.locked.tag(),
            clip.order_hint.tag(),
        ] {
            retain_tag(&mut keep, &by_id, tag);
        }
    }

    // Every track's LWW fields.
    for (_, track) in session.tracks() {
        for tag in [
            track.alive.tag(),
            track.name.tag(),
            track.kind.tag(),
            track.volume_db.tag(),
            track.muted.tag(),
            track.solo.tag(),
            track.order_hint.tag(),
        ] {
            retain_tag(&mut keep, &by_id, tag);
        }
    }

    // Every envelope's LWW tag.
    for tag in session.envelopes().tags() {
        retain_tag(&mut keep, &by_id, tag);
    }

    // Session metadata's LWW tags.
    for tag in session.session_metadata_tags() {
        retain_tag(&mut keep, &by_id, tag);
    }

    // Preserve original relative order (rank order, not filter order, is
    // what a fresh replay needs — `operation_log` is already in a valid
    // apply order, and filtering preserves that order).
    ops.iter().filter(|op| keep.contains(&op.op_id)).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{ClipData, ClipMetadataUpdate, TrackData, TrimEdge};
    use crate::timeline_crdt::TimelineCrdt;
    use tpt_av_sync_utils::PeerId;

    fn peer(n: u64) -> PeerId {
        PeerId::from_u64(n)
    }

    #[test]
    fn compaction_preserves_materialized_state() {
        let mut crdt = TimelineCrdt::new(peer(1));
        let track = TrackId::from_u64(1);
        let clip = ClipId::from_u64(1);
        crdt.apply_local(TimelineOperation::InsertTrack {
            track_id: track,
            track: TrackData::new("A1"),
            position: 0,
        });
        crdt.apply_local(TimelineOperation::InsertClip {
            clip_id: clip,
            track_id: track,
            clip: ClipData::new("a.wav", 0, 1_000),
            position: 0,
        });
        // Churn: several edits, some fully superseding earlier ones.
        crdt.apply_local(TimelineOperation::MoveClip {
            clip_id: clip,
            new_track_id: track,
            new_start_frame: 100,
            new_position: 1,
        });
        crdt.apply_local(TimelineOperation::UpdateClipMetadata {
            clip_id: clip,
            updates: ClipMetadataUpdate { name: Some("renamed.wav".into()), ..Default::default() },
        });
        crdt.apply_local(TimelineOperation::TrimClip {
            clip_id: clip,
            new_start_frame: 100,
            new_duration: 500,
            edge: TrimEdge::End,
        });
        crdt.apply_local(TimelineOperation::MoveClip {
            clip_id: clip,
            new_track_id: track,
            new_start_frame: 200,
            new_position: 2,
        });

        let before = crdt.view();
        let compacted_ops = compact(crdt.operation_log(), crdt.session());
        assert!(
            compacted_ops.len() < crdt.operation_log().len(),
            "superseded moves must actually be dropped"
        );

        let mut rebuilt = TimelineCrdt::new(peer(2));
        for op in compacted_ops {
            rebuilt.apply_remote(op).unwrap();
        }
        assert_eq!(rebuilt.view(), before, "compaction must not change materialized state");
    }

    #[test]
    fn compaction_preserves_a_deleted_clip_as_a_tombstone() {
        let mut crdt = TimelineCrdt::new(peer(1));
        let track = TrackId::from_u64(1);
        let clip = ClipId::from_u64(1);
        crdt.apply_local(TimelineOperation::InsertTrack {
            track_id: track,
            track: TrackData::new("A1"),
            position: 0,
        });
        crdt.apply_local(TimelineOperation::InsertClip {
            clip_id: clip,
            track_id: track,
            clip: ClipData::new("a.wav", 0, 1_000),
            position: 0,
        });
        crdt.apply_local(TimelineOperation::DeleteClip { clip_id: clip });

        let compacted_ops = compact(crdt.operation_log(), crdt.session());
        let mut rebuilt = TimelineCrdt::new(peer(2));
        for op in compacted_ops {
            rebuilt.apply_remote(op).unwrap();
        }
        assert_eq!(rebuilt.view(), crdt.view());
        assert!(rebuilt.view().clip(&clip).is_none(), "still deleted after compaction");

        // A late re-insert must still resurrect it (alive's real tag must
        // not have been lost).
        let mut a = rebuilt;
        a.apply_local(TimelineOperation::InsertClip {
            clip_id: clip,
            track_id: track,
            clip: ClipData::new("restored.wav", 0, 1_000),
            position: 0,
        });
        assert!(a.view().clip(&clip).is_some());
    }

    #[test]
    fn compaction_never_drops_any_split() {
        let mut crdt = TimelineCrdt::new(peer(1));
        let track = TrackId::from_u64(1);
        let clip = ClipId::from_u64(1);
        crdt.apply_local(TimelineOperation::InsertTrack {
            track_id: track,
            track: TrackData::new("A1"),
            position: 0,
        });
        crdt.apply_local(TimelineOperation::InsertClip {
            clip_id: clip,
            track_id: track,
            clip: ClipData::new("a.wav", 0, 1_000),
            position: 0,
        });
        let piece = ClipId::from_u64(2);
        crdt.apply_local(TimelineOperation::SplitClip {
            clip_id: clip,
            split_frame: 500,
            new_clip_id: piece,
        });
        // Rename the parent after the split — this write fully supersedes
        // the InsertClip's own name field, but the split itself must
        // survive compaction regardless.
        crdt.apply_local(TimelineOperation::UpdateClipMetadata {
            clip_id: clip,
            updates: ClipMetadataUpdate { name: Some("renamed.wav".into()), ..Default::default() },
        });

        let split_count = crdt
            .operation_log()
            .iter()
            .filter(|op| matches!(op.operation, TimelineOperation::SplitClip { .. }))
            .count();
        let compacted_ops = compact(crdt.operation_log(), crdt.session());
        let compacted_split_count = compacted_ops
            .iter()
            .filter(|op| matches!(op.operation, TimelineOperation::SplitClip { .. }))
            .count();
        assert_eq!(compacted_split_count, split_count, "no split may be dropped");

        let mut rebuilt = TimelineCrdt::new(peer(2));
        for op in compacted_ops {
            rebuilt.apply_remote(op).unwrap();
        }
        assert_eq!(rebuilt.view(), crdt.view());
        assert_eq!(rebuilt.view().clips.len(), 2, "both split pieces must still be present");
    }

    #[test]
    fn compaction_is_a_no_op_on_an_empty_session() {
        let crdt = TimelineCrdt::new(peer(1));
        assert!(compact(crdt.operation_log(), crdt.session()).is_empty());
    }

    #[test]
    fn compacting_twice_is_idempotent() {
        let mut crdt = TimelineCrdt::new(peer(1));
        let track = TrackId::from_u64(1);
        crdt.apply_local(TimelineOperation::InsertTrack {
            track_id: track,
            track: TrackData::new("A1"),
            position: 0,
        });
        for i in 0..5 {
            crdt.apply_local(TimelineOperation::UpdateTrackMetadata {
                track_id: track,
                updates: crate::operation::TrackMetadataUpdate {
                    volume_db: Some(i as f32),
                    ..Default::default()
                },
            });
        }
        let once = compact(crdt.operation_log(), crdt.session());
        let twice = compact(&once, crdt.session());
        assert_eq!(once, twice);
    }
}
