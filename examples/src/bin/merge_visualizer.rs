//! Conflict/merge visualizer: shows *which* concurrent edit won and *why*
//! for each of the three documented conflict classes (move, delete,
//! split), using `TimelineCrdt::take_resolution_events`.
//!
//! Unlike the other examples, this one deliberately creates real
//! concurrency — Alice and Bob each issue edits before either has seen the
//! other's — for all three conflict classes at once, then prints the
//! resulting resolution log.
//!
//! Run with: `cargo run -p tpt-av-sync-examples --bin merge_visualizer`

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_utils::PeerId;

fn sync(from: &TimelineCrdt, to: &mut TimelineCrdt) {
    for op in from.operation_log() {
        to.apply_remote(op.clone()).expect("remote apply");
    }
}

fn main() {
    println!("=== tpt-av-sync: conflict/merge visualizer ===\n");

    let alice_id = PeerId::from_u64(1);
    let bob_id = PeerId::from_u64(2);
    let mut alice = TimelineCrdt::new(alice_id);
    let mut bob = TimelineCrdt::new(bob_id);

    let track = TrackId::from_u64(1);
    let moved = ClipId::from_u64(1);
    let deleted = ClipId::from_u64(2);
    let split = ClipId::from_u64(3);

    alice.apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("A1"),
        position: 0,
    });
    for (id, name) in [(moved, "clip_a.wav"), (deleted, "clip_b.wav"), (split, "clip_c.wav")] {
        alice.apply_local(TimelineOperation::InsertClip {
            clip_id: id,
            track_id: track,
            clip: ClipData::new(name, 0, 96_000),
            position: 0,
        });
    }
    sync(&alice, &mut bob);
    bob.take_resolution_events(); // discard: not a conflict, just catching up.
    println!("session set up: 3 clips, Bob synced.\n");

    // Three concurrent conflicts, issued with neither peer aware of the
    // other's edit:
    println!("concurrent edits, issued with no coordination:");
    alice.apply_local(TimelineOperation::MoveClip {
        clip_id: moved,
        new_track_id: track,
        new_start_frame: 100,
        new_position: 0,
    });
    bob.apply_local(TimelineOperation::MoveClip {
        clip_id: moved,
        new_track_id: track,
        new_start_frame: 200,
        new_position: 0,
    });
    println!("  Alice: move clip_a.wav to 100   |  Bob: move clip_a.wav to 200");

    alice.apply_local(TimelineOperation::DeleteClip { clip_id: deleted });
    bob.apply_local(TimelineOperation::DeleteClip { clip_id: deleted });
    println!("  Alice: delete clip_b.wav        |  Bob: delete clip_b.wav");

    alice.apply_local(TimelineOperation::SplitClip {
        clip_id: split,
        split_frame: 48_000,
        new_clip_id: ClipId::from_u64(100),
    });
    bob.apply_local(TimelineOperation::SplitClip {
        clip_id: split,
        split_frame: 48_000,
        new_clip_id: ClipId::from_u64(50),
    });
    println!("  Alice: split clip_c.wav -> id 100 |  Bob: split clip_c.wav -> id 50\n");

    // Reconcile: sync in both directions and collect what each side
    // observed as it applied the other's operations.
    sync(&alice, &mut bob);
    let bob_events = bob.take_resolution_events();
    sync(&bob, &mut alice);
    let alice_events = alice.take_resolution_events();

    println!("resolution log (as observed while applying the incoming operations):");
    for event in bob_events.iter().chain(alice_events.iter()) {
        println!("  {event}");
    }

    assert_eq!(alice.view(), bob.view(), "replicas must converge");
    assert_eq!(bob_events.len(), 3, "three genuine conflicts must be reported");
    println!("\nconverged: both peers agree, and every conflict's outcome was explained ✓");
}
