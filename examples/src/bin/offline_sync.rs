//! Offline-first: edits made while disconnected queue locally and merge
//! automatically on reconnect.
//!
//! Run with: `cargo run -p tpt-av-sync-examples --bin offline_sync`

use std::thread;
use std::time::Duration;

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_net::{LoopbackTransport, SyncEngine};
use tpt_av_sync_utils::PeerId;

fn main() {
    println!("=== tpt-av-sync: offline-first demo ===\n");

    let (ta, tb) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));

    // Alice starts "offline": sends fail, edits queue.
    let offline_flag = ta.fail_handle();
    offline_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut alice = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(ta));
    let track = TrackId::from_u64(1);

    alice.apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("Foley"),
        position: 0,
    });
    alice.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(10),
        track_id: track,
        clip: ClipData::new("door_slam.wav", 0, 12_000),
        position: 0,
    });
    alice.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(11),
        track_id: track,
        clip: ClipData::new("rain_loop.wav", 24_000, 48_000),
        position: 1,
    });

    println!(
        "offline: {} edit(s) queued locally",
        alice.offline_queue_len()
    );
    assert_eq!(alice.offline_queue_len(), 3);

    // Meanwhile Bob works on his copy of the session (seeded earlier).
    let mut bob = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(tb));
    bob.apply_local(TimelineOperation::UpdateTrackMetadata {
        track_id: track,
        updates: tpt_av_sync_crdt::TrackMetadataUpdate {
            name: Some("Foley (renamed)".into()),
            ..Default::default()
        },
    });
    println!("bob, working alone, renamed the track");

    // Reconnect: the queue flushes and both peers converge.
    println!("\nreconnecting...");
    offline_flag.store(false, std::sync::atomic::Ordering::Relaxed);

    for _ in 0..50 {
        alice.process_messages();
        bob.process_messages();
        if alice.crdt().view() == bob.crdt().view()
            && alice.offline_queue_len() == 0
            && bob.crdt().view().clips.len() == 2
        {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }

    println!("queue drained: {} message(s) left", alice.offline_queue_len());
    let va = alice.crdt().view();
    let vb = bob.crdt().view();
    assert_eq!(va, vb, "offline edits must merge on reconnect");
    assert_eq!(va.clips.len(), 2);
    println!("converged state:");
    println!("  track \"{}\"", va.tracks[0].name);
    for c in &va.clips {
        println!(
            "  clip {} \"{}\" start={} dur={}",
            c.clip_id.as_u64(),
            c.name,
            c.start_frame,
            c.duration_frames
        );
    }
    println!("\nno data lost while disconnected. Offline-first ✓");
}
