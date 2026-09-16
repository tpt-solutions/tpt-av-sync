//! Two peers editing the same timeline concurrently over TCP, meeting in
//! the same state without a central server.
//!
//! Run with: `cargo run -p tpt-av-sync-examples --bin collaborative_editor`

use std::thread;
use std::time::Duration;

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId, TrimEdge};
use tpt_av_sync_net::{SyncEngine, TcpTransport, Transport};
use tpt_av_sync_utils::PeerId;

fn main() {
    println!("=== tpt-av-sync: collaborative editor demo ===\n");

    let bind = "127.0.0.1:0".parse().unwrap();
    let (t1, addr) = TcpTransport::listen(bind, PeerId::from_u64(1)).expect("listen");
    let t2 = TcpTransport::listen(bind, PeerId::from_u64(2)).expect("listen").0;
    t2.connect(addr).expect("connect");
    println!("peers connected over TCP ({addr})\n");

    let mut alice = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(t1));
    let mut bob = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(t2));

    // Alice sets up the session and inserts a clip.
    let track = TrackId::from_u64(1);
    let clip = ClipId::from_u64(100);
    alice.apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("Dialog"),
        position: 0,
    });
    alice.apply_local(TimelineOperation::InsertClip {
        clip_id: clip,
        track_id: track,
        clip: ClipData::new("take_03.wav", 0, 96_000),
        position: 0,
    });

    // Both sides pump the network until Alice's edits land on Bob.
    pump(&mut alice, &mut bob);
    println!("after setup, Bob sees {} clip(s)", bob.crdt().view().clips.len());

    // CONCURRENT edits: neither peer has seen the other's change yet.
    // Alice splits the clip at 1s; Bob trims its tail at the same moment.
    let piece = ClipId::from_u64(101);
    alice.apply_local(TimelineOperation::SplitClip {
        clip_id: clip,
        split_frame: 48_000,
        new_clip_id: piece,
    });
    bob.apply_local(TimelineOperation::TrimClip {
        clip_id: clip,
        new_start_frame: 0,
        new_duration: 80_000,
        edge: TrimEdge::End,
    });

    println!("concurrent edits issued:");
    println!("  Alice: split take_03.wav at 1.000s");
    println!("  Bob:   trim take_03.wav tail to 80,000 frames");

    pump(&mut alice, &mut bob);

    // Convergence: both peers agree, and the concurrent split + trim both
    // survived.
    let va = alice.crdt().view();
    let vb = bob.crdt().view();
    assert_eq!(va, vb, "CRDT replicas must converge");
    println!("\nconverged without a conflict:");
    for c in &va.clips {
        println!(
            "  clip {} \"{}\" start={} dur={}",
            c.clip_id.as_u64(),
            c.name,
            c.start_frame,
            c.duration_frames
        );
    }
    assert_eq!(va.clips.len(), 2, "split produced a second clip");
    println!("\nboth peers now see the same timeline. CRDT conflict resolution ✓");
}

/// Pumps both engines until their views match (or a generous cap elapses).
fn pump(alice: &mut SyncEngine, bob: &mut SyncEngine) {
    for _ in 0..200 {
        alice.process_messages();
        bob.process_messages();
        if alice.crdt().view() == bob.crdt().view()
            && alice.crdt().view().clips.len() == bob.crdt().view().clips.len()
            && alice.pending_acks() == 0
            && bob.pending_acks() == 0
        {
            thread::sleep(Duration::from_millis(5));
            alice.process_messages();
            bob.process_messages();
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
}
