//! Undo/redo and delta-compression tests.

use tpt_av_sync_crdt::{
    compute_delta, apply_delta, ClipData, ClipId, ClipMetadataUpdate, EnvelopePoint,
    EnvelopeType, OpTag, Session, SessionMetadataUpdate, TargetId, TimelineCrdt,
    TimelineOperation, TrackData, TrackId, TrimEdge,
};
use tpt_av_sync_utils::wire;
use tpt_av_sync_utils::PeerId;

fn setup(crdt: &mut TimelineCrdt) -> (TrackId, ClipId) {
    let track = TrackId::from_u64(1);
    let clip = ClipId::from_u64(100);
    crdt.apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("A1"),
        position: 0,
    });
    crdt.apply_local(TimelineOperation::InsertClip {
        clip_id: clip,
        track_id: track,
        clip: ClipData::new("take1", 0, 1_000),
        position: 0,
    });
    (track, clip)
}

#[test]
fn undo_redo_move_round_trip() {
    let mut crdt = TimelineCrdt::new(PeerId::from_u64(1));
    let (track, clip) = setup(&mut crdt);

    crdt.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: track,
        new_start_frame: 400,
        new_position: 0,
    });
    assert_eq!(crdt.view().clip(&clip).unwrap().start_frame, 400);

    crdt.undo().expect("undo must exist");
    assert_eq!(crdt.view().clip(&clip).unwrap().start_frame, 0);

    crdt.redo().expect("redo must exist");
    assert_eq!(crdt.view().clip(&clip).unwrap().start_frame, 400);
}

#[test]
fn undo_delete_resurrects_clip_with_previous_data() {
    let mut crdt = TimelineCrdt::new(PeerId::from_u64(1));
    let (_track, clip) = setup(&mut crdt);
    crdt.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 250,
        new_position: 0,
    });
    crdt.apply_local(TimelineOperation::DeleteClip { clip_id: clip });
    assert!(crdt.view().clip(&clip).is_none());

    crdt.undo().expect("undo delete");
    let view = crdt.view();
    let restored = view.clip(&clip).expect("clip restored");
    assert_eq!(restored.start_frame, 250);
    assert_eq!(restored.duration_frames, 1_000);
}

#[test]
fn undo_split_removes_piece_and_redo_restores_it() {
    let mut crdt = TimelineCrdt::new(PeerId::from_u64(1));
    let (track, clip) = setup(&mut crdt);
    let piece = ClipId::from_u64(101);
    crdt.apply_local(TimelineOperation::SplitClip {
        clip_id: clip,
        split_frame: 400,
        new_clip_id: piece,
    });
    assert_eq!(crdt.view().clips.len(), 2);

    crdt.undo().expect("undo split");
    assert_eq!(crdt.view().clips.len(), 1, "piece must be gone");

    crdt.redo().expect("redo split");
    assert_eq!(crdt.view().clips.len(), 2);
    let view = crdt.view();
    assert_eq!(view.clip(&clip).unwrap().duration_frames, 400);
    assert_eq!(
        view.clip(&piece).unwrap().start_frame,
        400,
        "track {} intact",
        track.as_u64()
    );
}

#[test]
fn undo_metadata_and_session_metadata_round_trip() {
    let mut crdt = TimelineCrdt::new(PeerId::from_u64(1));
    let (_track, clip) = setup(&mut crdt);

    crdt.apply_local(TimelineOperation::UpdateClipMetadata {
        clip_id: clip,
        updates: ClipMetadataUpdate {
            name: Some("renamed".into()),
            gain: Some(0.5),
            ..Default::default()
        },
    });
    crdt.apply_local(TimelineOperation::UpdateSessionMetadata {
        updates: SessionMetadataUpdate {
            name: Some("Episode".into()),
            sample_rate: Some(44_100),
            ..Default::default()
        },
    });
    assert_eq!(crdt.view().metadata.name, "Episode");

    crdt.undo().expect("undo session metadata");
    assert_eq!(crdt.view().metadata.name, "Untitled Session");
    assert_eq!(crdt.view().metadata.sample_rate, 48_000);

    crdt.undo().expect("undo clip metadata");
    let view = crdt.view();
    assert_eq!(view.clip(&clip).unwrap().name, "take1");
    assert_eq!(view.clip(&clip).unwrap().gain, 1.0);

    crdt.redo();
    crdt.redo();
    let view = crdt.view();
    assert_eq!(view.metadata.name, "Episode");
    assert_eq!(view.clip(&clip).unwrap().name, "renamed");
}

#[test]
fn undo_of_remote_viewed_on_second_replica_converges() {
    // The undo is a normal operation: a second replica that receives the
    // full log (edit, undo) converges with the editor.
    let editor = PeerId::from_u64(1);
    let viewer = PeerId::from_u64(2);
    let mut a = TimelineCrdt::new(editor);
    let (track, clip) = setup(&mut a);
    a.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: track,
        new_start_frame: 777,
        new_position: 0,
    });
    a.undo().expect("undo");

    let mut b = TimelineCrdt::new(viewer);
    for op in a.operation_log() {
        b.apply_remote(op.clone()).unwrap();
    }
    assert_eq!(a.view(), b.view());
    assert_eq!(b.view().clip(&clip).unwrap().start_frame, 0);
}

#[test]
fn delta_between_two_states_transforms_one_into_the_other() {
    let base_track = TrackId::from_u64(1);
    let clip_a = ClipId::from_u64(100);
    let clip_b = ClipId::from_u64(101);

    let mut old = Session::new();
    apply_delta(
        &mut old,
        &[
            TimelineOperation::InsertTrack {
                track_id: base_track,
                track: TrackData::new("A1"),
                position: 0,
            },
            TimelineOperation::InsertClip {
                clip_id: clip_a,
                track_id: base_track,
                clip: ClipData::new("a", 0, 1_000),
                position: 0,
            },
        ],
        OpTag::new(1_000, PeerId::from_u64(1)),
    );

    // New state: clip_a moved+trimmed, clip_b added, session renamed.
    let mut new = Session::new();
    apply_delta(
        &mut new,
        &[
            TimelineOperation::InsertTrack {
                track_id: base_track,
                track: TrackData::new("A1"),
                position: 0,
            },
            TimelineOperation::InsertClip {
                clip_id: clip_a,
                track_id: base_track,
                clip: ClipData::new("a", 0, 1_000),
                position: 0,
            },
            TimelineOperation::MoveClip {
                clip_id: clip_a,
                new_track_id: base_track,
                new_start_frame: 500,
                new_position: 0,
            },
            TimelineOperation::TrimClip {
                clip_id: clip_a,
                new_start_frame: 500,
                new_duration: 700,
                edge: TrimEdge::End,
            },
            TimelineOperation::InsertClip {
                clip_id: clip_b,
                track_id: base_track,
                clip: ClipData::new("b", 2_000, 100),
                position: 1,
            },
            TimelineOperation::UpdateSessionMetadata {
                updates: SessionMetadataUpdate {
                    name: Some("v2".into()),
                    ..Default::default()
                },
            },
        ],
        OpTag::new(2_000, PeerId::from_u64(1)),
    );

    let delta = compute_delta(&old.materialize(), &new.materialize());
    assert!(
        delta.len() < 6,
        "delta should be minimal, got {} ops",
        delta.len()
    );

    apply_delta(&mut old, &delta, OpTag::new(5_000, PeerId::from_u64(1)));
    assert_eq!(old.materialize(), new.materialize());
}

#[test]
fn delta_detects_envelope_and_metadata_changes() {
    let track = TrackId::from_u64(1);
    let clip = ClipId::from_u64(100);

    let mut old = Session::new();
    let mut new = Session::new();
    let base = vec![
        TimelineOperation::InsertTrack {
            track_id: track,
            track: TrackData::new("A1"),
            position: 0,
        },
        TimelineOperation::InsertClip {
            clip_id: clip,
            track_id: track,
            clip: ClipData::new("a", 0, 100),
            position: 0,
        },
    ];
    apply_delta(&mut old, &base, OpTag::new(1, PeerId::from_u64(1)));
    apply_delta(&mut new, &base, OpTag::new(1, PeerId::from_u64(1)));

    new.apply(
        &TimelineOperation::UpdateEnvelope {
            target_id: TargetId::Clip(clip),
            envelope_type: EnvelopeType::Volume,
            points: vec![EnvelopePoint {
                frame: 0,
                value: 1.0,
                interpolation: tpt_av_sync_crdt::Interpolation::Linear,
            }],
        },
        OpTag::new(2, PeerId::from_u64(1)),
    )
    .unwrap();
    new.apply(
        &TimelineOperation::UpdateClipMetadata {
            clip_id: clip,
            updates: ClipMetadataUpdate {
                color: Some(0xFF_00_00_00),
                ..Default::default()
            },
        },
        OpTag::new(3, PeerId::from_u64(1)),
    )
    .unwrap();

    let delta = compute_delta(&old.materialize(), &new.materialize());
    assert_eq!(delta.len(), 2, "one metadata + one envelope op");
    apply_delta(&mut old, &delta, OpTag::new(100, PeerId::from_u64(1)));
    assert_eq!(old.materialize(), new.materialize());
}

#[test]
fn tagged_operations_flow_through_bincode() {
    // Snapshot wire-format smoke test: snapshots serialize losslessly.
    let mut crdt = TimelineCrdt::new(PeerId::from_u64(7));
    setup(&mut crdt);
    let bytes = wire::encode(&crdt.snapshot()).expect("serialize");
    let snap: tpt_av_sync_crdt::TimelineSnapshot = wire::decode(&bytes).unwrap();
    let restored = TimelineCrdt::from_snapshot(snap, PeerId::from_u64(8));
    assert_eq!(crdt.view(), restored.view());
}
