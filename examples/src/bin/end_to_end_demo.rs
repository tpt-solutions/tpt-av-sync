//! End-to-end demo: three peers, real TCP sockets, one live session.
//!
//! This is the "everything together" demo: unlike the single-feature
//! examples, it drives one `SyncEngine` + `PlayheadSync` + `PresenceManager`
//! per peer, over an actual TCP mesh (not the in-process loopback
//! transport), through a session shaped like a real one:
//!
//!   1. Alice starts a session and adds a track and two clips.
//!   2. Bob and Carol join *after* editing has already happened and catch
//!      up via snapshot-on-join.
//!   3. All three edit concurrently, unaware of each other's changes.
//!   4. Alice becomes playback master; Bob and Carol track her playhead to
//!      sub-millisecond precision.
//!   5. Everyone publishes presence (name, cursor); peers see each other.
//!
//! It ends by asserting all three replicas hold the identical timeline.
//!
//! Run with: `cargo run -p tpt-av-sync-examples --bin end_to_end_demo`

use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_net::{SyncEngine, SyncEvent, SyncMessage, TcpTransport};
use tpt_av_sync_playhead::PlayheadSync;
use tpt_av_sync_presence::{CursorState, PresenceManager, UserInfo};
use tpt_av_sync_utils::PeerId;

struct Peer {
    name: &'static str,
    engine: SyncEngine,
    presence: PresenceManager,
    playhead: PlayheadSync,
}

fn pump(peers: &mut [Peer]) {
    for _ in 0..40 {
        for peer in peers.iter_mut() {
            peer.engine.process_messages();
            for event in peer.engine.take_events() {
                match event {
                    SyncEvent::Presence(update) => peer.presence.receive_update(update),
                    SyncEvent::Playhead(update) => peer.playhead.receive_update(&update),
                    _ => {}
                }
            }
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn converged(peers: &[Peer]) -> bool {
    let first = peers[0].engine.crdt().view();
    peers[1..].iter().all(|p| p.engine.crdt().view() == first)
}

fn main() {
    println!("=== tpt-av-sync: end-to-end demo (3 peers, real TCP) ===\n");

    // 1. Alice listens first and starts editing solo.
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (t_alice, addr_alice) = TcpTransport::listen(bind, PeerId::from_u64(1)).expect("listen");
    let mut alice = Peer {
        name: "Alice",
        engine: SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(t_alice)),
        presence: PresenceManager::new(
            UserInfo::online(PeerId::from_u64(1), "Alice", 0),
            Default::default(),
        ),
        playhead: PlayheadSync::new(PeerId::from_u64(1), 48_000),
    };

    let track = TrackId::from_u64(1);
    let clip_a = ClipId::from_u64(100);
    let clip_b = ClipId::from_u64(101);
    alice.engine.apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("Dialog"),
        position: 0,
    });
    alice.engine.apply_local(TimelineOperation::InsertClip {
        clip_id: clip_a,
        track_id: track,
        clip: ClipData::new("take_03.wav", 0, 96_000),
        position: 0,
    });
    alice.engine.apply_local(TimelineOperation::InsertClip {
        clip_id: clip_b,
        track_id: track,
        clip: ClipData::new("take_07.wav", 100_000, 48_000),
        position: 1,
    });
    println!("Alice starts the session solo: 1 track, 2 clips.\n");

    // 2. Bob and Carol join in progress: they connect and catch up via
    //    snapshot-on-join, not by replaying every historical operation.
    let (t_bob, addr_bob) = TcpTransport::listen(bind, PeerId::from_u64(2)).expect("listen");
    t_bob.connect(addr_alice).expect("bob dials alice");
    let bob = Peer {
        name: "Bob",
        engine: SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(t_bob)),
        presence: PresenceManager::new(
            UserInfo::online(PeerId::from_u64(2), "Bob", 0),
            Default::default(),
        ),
        playhead: PlayheadSync::new(PeerId::from_u64(2), 48_000),
    };

    let (t_carol, _addr_carol) = TcpTransport::listen(bind, PeerId::from_u64(3)).expect("listen");
    t_carol.connect(addr_alice).expect("carol dials alice");
    t_carol.connect(addr_bob).expect("carol dials bob");
    let carol = Peer {
        name: "Carol",
        engine: SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(3)), Box::new(t_carol)),
        presence: PresenceManager::new(
            UserInfo::online(PeerId::from_u64(3), "Carol", 0),
            Default::default(),
        ),
        playhead: PlayheadSync::new(PeerId::from_u64(3), 48_000),
    };

    let mut peers = vec![alice, bob, carol];
    pump(&mut peers);
    println!(
        "Bob and Carol join in progress; after sync each sees {} clip(s).\n",
        peers[1].engine.crdt().view().clips.len()
    );
    assert!(converged(&peers), "join-in-progress must converge before concurrent edits");

    // 3. Everyone publishes presence.
    for peer in &mut peers {
        peer.presence.update_local_cursor(CursorState::new(0));
        let update = peer.presence.generate_update();
        peer.engine
            .transport_mut()
            .broadcast(SyncMessage::PresenceUpdate(update))
            .expect("broadcast presence");
    }
    pump(&mut peers);
    println!(
        "presence: Alice sees {} other peer(s) online.\n",
        peers[0].presence.remote_users().len()
    );

    // 4. Concurrent edits — none of the three has seen another's change.
    peers[0].engine.apply_local(TimelineOperation::TrimClip {
        clip_id: clip_a,
        new_start_frame: 0,
        new_duration: 80_000,
        edge: tpt_av_sync_crdt::TrimEdge::End,
    });
    peers[1].engine.apply_local(TimelineOperation::MoveClip {
        clip_id: clip_b,
        new_track_id: track,
        new_start_frame: 120_000,
        new_position: 1,
    });
    let clip_c = ClipId::from_u64(102);
    peers[2].engine.apply_local(TimelineOperation::InsertClip {
        clip_id: clip_c,
        track_id: track,
        clip: ClipData::new("carol_add.wav", 200_000, 24_000),
        position: 2,
    });
    println!("concurrent edits, issued with no coordination:");
    println!("  Alice: trim take_03.wav tail to 80,000 frames");
    println!("  Bob:   move take_07.wav to 120,000");
    println!("  Carol: insert carol_add.wav at 200,000");

    pump(&mut peers);
    assert!(converged(&peers), "all three replicas must converge");
    let final_view = peers[0].engine.crdt().view();
    assert_eq!(final_view.clips.len(), 3, "all three concurrent edits must survive");
    println!(
        "\nconverged: all 3 peers agree on {} clips after 3-way concurrent editing ✓\n",
        final_view.clips.len()
    );

    // 5. Alice plays; Bob and Carol track her playhead live.
    peers[0].playhead.set_master(true);
    peers[1].playhead.follow_master(PeerId::from_u64(1));
    peers[2].playhead.follow_master(PeerId::from_u64(1));

    let sample_rate = 48_000_u64;
    let mut worst_error_ms = 0.0_f64;
    for tick in 0..20_u64 {
        let position = tick * sample_rate / 10; // 100 ms of playback per tick
        peers[0].playhead.set_local_position(position);
        let update = peers[0].playhead.generate_update();
        peers[0]
            .engine
            .transport_mut()
            .broadcast(SyncMessage::PlayheadUpdate(update))
            .expect("broadcast playhead");
        pump(&mut peers);

        for follower in &peers[1..] {
            let estimated = follower.playhead.synchronized_position();
            let error_ms =
                (estimated as f64 - position as f64).abs() / sample_rate as f64 * 1_000.0;
            worst_error_ms = worst_error_ms.max(error_ms);
        }
    }
    println!(
        "playhead: Alice played 2.0s of audio; Bob/Carol tracked within {worst_error_ms:.3} ms worst-case."
    );

    println!("\n=== live session complete: CRDT + transport + playhead + presence, one stack ===");
    for peer in &peers {
        println!(
            "  {}: {} clip(s), {} remote user(s) visible",
            peer.name,
            peer.engine.crdt().view().clips.len(),
            peer.presence.remote_users().len()
        );
    }
}
