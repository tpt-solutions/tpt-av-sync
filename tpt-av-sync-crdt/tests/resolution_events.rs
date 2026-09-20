//! Conflict/merge visualizer: `TimelineCrdt::take_resolution_events`
//! reports which write won a concurrent move/delete/split, and why.

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
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

fn sync(from: &TimelineCrdt, to: &mut TimelineCrdt) {
    for op in from.operation_log() {
        to.apply_remote(op.clone()).expect("remote apply");
    }
}

#[test]
fn uncontested_solo_editing_reports_no_events() {
    let mut a = TimelineCrdt::new(PeerId::from_u64(1));
    let clip = ClipId::from_u64(1);
    setup_clip(&mut a, clip, 0, 1_000);
    a.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 500,
        new_position: 0,
    });
    assert!(
        a.take_resolution_events().is_empty(),
        "editing your own uncontested writes must not report a conflict"
    );
}

#[test]
fn sequential_cross_peer_edit_reports_no_event() {
    // Alice creates and moves a clip; Bob syncs, sees her move, and *then*
    // moves it himself. Bob's move causally follows Alice's (he observed
    // it before issuing his own), so this is ordinary sequential editing,
    // not a conflict — even though two different peers touched the same
    // field. This guards against a naive "different peer than last
    // writer" heuristic, which would misfire on every first edit anyone
    // other than the creator makes.
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(1);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 1_000);
    a.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 100,
        new_position: 0,
    });
    sync(&a, &mut b);
    assert!(
        b.take_resolution_events().is_empty(),
        "receiving Alice's own sequential edits is not a conflict"
    );

    b.apply_local(TimelineOperation::MoveClip {
        clip_id: clip,
        new_track_id: TrackId::from_u64(1),
        new_start_frame: 200,
        new_position: 0,
    });
    assert!(
        b.take_resolution_events().is_empty(),
        "Bob's move, issued after observing Alice's, must not be flagged as concurrent"
    );

    sync(&b, &mut a);
    assert!(
        a.take_resolution_events().is_empty(),
        "Alice receiving Bob's causally-later move is not a conflict either"
    );
    assert_eq!(a.view(), b.view());
}

#[test]
fn concurrent_move_reports_the_winner() {
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(1);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 1_000);
    sync(&a, &mut b);
    b.take_resolution_events(); // discard events from the setup sync

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

    // B applies A's concurrent move on top of its own: B's move (peer 2)
    // outranks A's (peer 1) at an equal lamport, so A's incoming write
    // loses.
    sync(&a, &mut b);
    let events = b.take_resolution_events();
    assert_eq!(events.len(), 1, "one contested move must be reported");
    let event = events[0];
    assert_eq!(event.kind, "move");
    assert_eq!(event.clip_id, clip);
    assert!(!event.op_won, "A's move must lose to B's higher-ranked move");
    assert_eq!(b.view().clip(&clip).unwrap().start_frame, 200);
}

#[test]
fn concurrent_delete_reports_a_contested_event() {
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(1);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 1_000);
    sync(&a, &mut b);
    b.take_resolution_events();

    a.apply_local(TimelineOperation::DeleteClip { clip_id: clip });
    b.apply_local(TimelineOperation::DeleteClip { clip_id: clip });

    sync(&a, &mut b);
    let events = b.take_resolution_events();
    assert_eq!(events.len(), 1, "the second delete of the same clip is contested");
    assert_eq!(events[0].kind, "delete");
    assert!(b.view().clip(&clip).is_none());
}

#[test]
fn concurrent_split_reports_the_smaller_id_winning() {
    let peer_a = PeerId::from_u64(1);
    let peer_b = PeerId::from_u64(2);
    let clip = ClipId::from_u64(1);

    let mut a = TimelineCrdt::new(peer_a);
    let mut b = TimelineCrdt::new(peer_b);
    setup_clip(&mut a, clip, 0, 1_000);
    sync(&a, &mut b);
    b.take_resolution_events();

    // Both peers split at the same offset but propose different new ids.
    a.apply_local(TimelineOperation::SplitClip {
        clip_id: clip,
        split_frame: 500,
        new_clip_id: ClipId::from_u64(100),
    });
    b.apply_local(TimelineOperation::SplitClip {
        clip_id: clip,
        split_frame: 500,
        new_clip_id: ClipId::from_u64(50),
    });

    sync(&a, &mut b);
    let events = b.take_resolution_events();
    assert_eq!(events.len(), 1, "same split point, different ids is contested");
    let event = events[0];
    assert_eq!(event.kind, "split");
    assert!(
        !event.op_won,
        "A's id (100) loses to B's already-recorded smaller id (50)"
    );
    assert_eq!(event.reason, "same split point, smaller clip id wins");
}
