//! Scenario tests for spec §5.1 — concurrent operations from multiple
//! peers must converge identically on every replica.


use tpt_av_sync_crdt::{
    ClipData, ClipId, TaggedOperation, TimelineCrdt, TimelineOperation, TrackData, TrackId,
    TrimEdge,
};

use tpt_av_sync_utils::PeerId;

fn setup_clip(crdt: &mut TimelineCrdt, clip_id: ClipId, start: u64, dur: u64) {
    let track = TrackId::from_u64(1);
    crdt.apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("A1"),
        position: 0,
    });
    crdt.apply_local(TimelineOperation::InsertClip {
        clip_id,
        track_id: track,
        clip: ClipData::new("take1", start, dur),
        position: 0,
    });
}

/// Sync all operations that `from` has but `to` does not.
fn sync(from: &TimelineCrdt, to: &mut TimelineCrdt) {
    for op in from.operation_log() {
        to.apply_remote(op.clone()).expect("remote apply");
    }
}

#[test]
fn concurrent_clip_moves_converge_by_lamport() {
    // Spec §5.1: A moves Clip X to 100, B moves Clip X to 200. Both
    // replicas must converge; the higher (lamport, peer) write wins.
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 10_000);
    sync(&a, &mut b);

    // Concurrent moves at the same Lamport timestamp (neither saw the
    // other's edit before issuing its own).
    a.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 100,
        new_position: 0,
    });
    b.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 200,
        new_position: 0,
    });

    sync(&a, &mut b);
    sync(&b, &mut a);

    let va = a.view();
    let vb = b.view();
    assert_eq!(va, vb, "replicas must converge");
    assert_eq!(
        va.clip(&clip).expect("clip present").start_frame,
        200,
        "equal lamports: higher peer id (B) wins"
    );
}

#[test]
fn higher_lamport_move_wins_regardless_of_delivery_order() {
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 10_000);
    sync(&a, &mut b);

    // B edits first (lamport 3), then issues its move (lamport 4). A's
    // move is issued at lamport 3 concurrently with B's first edit.
    b.apply_local(TimelineOperation::UpdateSessionMetadata {
        updates: tpt_av_sync_crdt::SessionMetadataUpdate {
            name: Some("b was here".into()),
            ..Default::default()
        },
    });
    let b_move = b.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 200,
        new_position: 0,
    });
    let a_move = a.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 100,
        new_position: 0,
    });
    assert!(a_move.lamport_ts < b_move.lamport_ts, "B's move must rank higher");

    // Deliver B's higher-ranked move to A before A has seen it land, then
    // fully sync both ways.
    sync(&b, &mut a);
    sync(&a, &mut b);

    let view = a.view();
    assert_eq!(
        view.clip(&clip).unwrap().start_frame,
        200,
        "higher-ranked move wins even when delivered first"
    );
    assert_eq!(a.view(), b.view());
}

#[test]
fn concurrent_clip_deletes_are_idempotent() {
    // Spec §5.1: both peers delete Clip X. The clip is deleted exactly
    // once and replicas converge.
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 10_000);
    sync(&a, &mut b);

    a.apply_local(TimelineOperation::DeleteClip { clip_id: clip });
    b.apply_local(TimelineOperation::DeleteClip { clip_id: clip });

    sync(&a, &mut b);
    sync(&b, &mut a);

    let va = a.view();
    assert_eq!(va, b.view());
    assert!(va.clip(&clip).is_none(), "clip must be gone");
    assert_eq!(va.clips.len(), 0);
}

#[test]
fn delete_concurrent_with_move_has_deterministic_outcome() {
    // A deletes X while B moves X. Deleting tombstones the liveness
    // register; a concurrent or later move writes geometry only and cannot
    // resurrect the clip (only a re-insert can). Both replicas must agree.
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 10_000);
    sync(&a, &mut b);

    a.apply_local(TimelineOperation::DeleteClip { clip_id: clip });
    b.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 4_000,
        new_position: 0,
    });

    sync(&a, &mut b);
    sync(&b, &mut a);

    let va = a.view();
    assert_eq!(va, b.view(), "delete vs. move must converge");
    assert!(
        va.clip(&clip).is_none(),
        "a move cannot resurrect a deleted clip"
    );

    // A later re-insert on top of the converged state does resurrect it.
    a.apply_local(TimelineOperation::InsertClip {
        clip_id: clip,
        track_id: TrackId::from_u64(1),
        clip: ClipData::new("restored", 4_000, 6_000),
        position: 0,
    });
    sync(&a, &mut b);
    assert_eq!(a.view(), b.view());
    let view = a.view();
    let restored = view.clip(&clip).expect("re-insert must resurrect");
    assert_eq!(restored.start_frame, 4_000);
}

#[test]
fn concurrent_clip_splits_yield_three_clips() {
    // Spec §5.1: A splits X at 500, B splits X at 1000. Result: three
    // clips — original (0-500), middle (500-1000), right (1000-end).
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);
    let clip_a2 = ClipId::from_u64(101);
    let clip_b2 = ClipId::from_u64(102);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 2_000);
    sync(&a, &mut b);

    a.apply_local(TimelineOperation::SplitClip {
        clip_id: clip,
        split_frame: 500,
        new_clip_id: clip_a2,
    });
    b.apply_local(TimelineOperation::SplitClip {
        clip_id: clip,
        split_frame: 1_000,
        new_clip_id: clip_b2,
    });

    sync(&a, &mut b);
    sync(&b, &mut a);

    let va = a.view();
    assert_eq!(va, b.view(), "replicas must converge");
    assert_eq!(va.clips.len(), 3, "two concurrent splits -> three clips");

    let original = va.clip(&clip).unwrap();
    let middle = va.clip(&clip_a2).unwrap();
    let right = va.clip(&clip_b2).unwrap();
    assert_eq!((original.start_frame, original.duration_frames), (0, 500));
    assert_eq!((middle.start_frame, middle.duration_frames), (500, 500));
    assert_eq!((right.start_frame, right.duration_frames), (1_000, 1_000));
}

#[test]
fn split_then_rename_propagates_to_split_pieces() {
    // Split pieces inherit fields until they receive their own writes:
    // renaming the original must rename un-renamed pieces identically on
    // every replica, regardless of delivery order.
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);
    let piece = ClipId::from_u64(101);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 2_000);
    sync(&a, &mut b);

    let split = a.apply_local(TimelineOperation::SplitClip {
        clip_id: clip,
        split_frame: 500,
        new_clip_id: piece,
    });
    let rename = a.apply_local(TimelineOperation::UpdateClipMetadata {
        clip_id: clip,
        updates: tpt_av_sync_crdt::ClipMetadataUpdate {
            name: Some("master_take".into()),
            ..Default::default()
        },
    });

    // Deliver to B in reverse order (rename before the split).
    b.apply_remote(rename).unwrap();
    b.apply_remote(split).unwrap();

    let va = a.view();
    let vb = b.view();
    assert_eq!(va, vb);
    assert_eq!(va.clip(&piece).unwrap().name, "master_take");
}

#[test]
fn trim_and_move_compose_across_replicas() {
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 10_000);
    sync(&a, &mut b);

    a.apply_local(TimelineOperation::TrimClip {
        clip_id: clip,
        new_start_frame: 0,
        new_duration: 6_000,
        edge: TrimEdge::End,
    });
    b.apply_local(TimelineOperation::TrimClip {
        clip_id: clip,
        new_start_frame: 0,
        new_duration: 4_000,
        edge: TrimEdge::End,
    });

    sync(&a, &mut b);
    sync(&b, &mut a);

    let va = a.view();
    assert_eq!(va, b.view());
    // B's trim has the higher peer id at the same Lamport level -> wins.
    assert_eq!(va.clip(&clip).unwrap().duration_frames, 4_000);
}

#[test]
fn operations_arriving_out_of_order_are_buffered_until_ready() {
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);
    let track = TrackId::from_u64(1);

    let mut a = TimelineCrdt::new(peer_a);
    setup_clip(&mut a, clip, 0, 1_000);
    a.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: track,
        new_start_frame: 500,
        new_position: 0,
    });

    // B receives everything in reverse order.
    let log: Vec<TaggedOperation> = a.operation_log().to_vec();
    let mut b = TimelineCrdt::new(peer_b);
    for op in log.iter().rev() {
        b.apply_remote(op.clone()).unwrap();
    }

    assert_eq!(a.view(), b.view());
    assert_eq!(b.pending_len(), 0, "all buffered ops must have applied");
}

#[test]
fn snapshot_replay_reproduces_state_exactly() {
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let mut a = TimelineCrdt::new(peer_a);
    setup_clip(&mut a, ClipId::from_u64(100), 0, 2_000);
    a.apply_local(TimelineOperation::SplitClip {
        clip_id: ClipId::from_u64(100),
        split_frame: 500,
        new_clip_id: ClipId::from_u64(101),
    });
    a.apply_local(TimelineOperation::UpdateSessionMetadata {
        updates: tpt_av_sync_crdt::SessionMetadataUpdate {
            name: Some("Episode 12".into()),
            tempo_bpm: Some(96.0),
            ..Default::default()
        },
    });

    let snapshot = a.snapshot();
    let b = TimelineCrdt::from_snapshot(snapshot, peer_b);
    assert_eq!(a.view(), b.view());
    assert_eq!(a.operation_log().len(), b.operation_log().len());
}

#[test]
fn merge_snapshot_into_diverged_replica() {
    // Offline-first groundwork: two replicas edit independently while
    // disconnected; merging B's snapshot into A converges both.
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(100);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 5_000);
    // B got the base state before divergence.
    b.merge_snapshot(&a.snapshot());

    a.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 1_000,
        new_position: 0,
    });
    b.apply_local(TimelineOperation::TrimClip {
        clip_id: clip,
        new_start_frame: 0,
        new_duration: 3_000,
        edge: TrimEdge::End,
    });

    let b_snapshot = b.snapshot();
    a.merge_snapshot(&b_snapshot);
    b.merge_snapshot(&a.snapshot()); // make sure B sees A's op too

    assert_eq!(a.view(), b.view(), "merge must converge both replicas");
    // A's move (lamport 3) and B's trim (lamport 3, peer 2) both rank at 3;
    // peer 2 wins per-register, so the trim geometry stands.
    let view = a.view();
    let clip_view = view.clip(&clip).unwrap();
    assert_eq!(clip_view.start_frame, 0);
    assert_eq!(clip_view.duration_frames, 3_000);
}
