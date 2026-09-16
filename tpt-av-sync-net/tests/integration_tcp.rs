//! Integration tests over real TCP sockets: operation exchange, snapshot
//! on join, and reconnect with the offline queue.

use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_net::{SyncEngine, TcpTransport};
use tpt_av_sync_utils::PeerId;

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    cond()
}

fn seed_track(crdt: &mut TimelineCrdt) -> TrackId {
    let track = TrackId::from_u64(1);
    crdt.apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("A1"),
        position: 0,
    });
    track
}

fn pump(engines: &mut [&mut SyncEngine]) {
    for _ in 0..20 {
        for engine in engines.iter_mut() {
            engine.process_messages();
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn two_peers_exchange_operations_over_tcp() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (t1, l1) = TcpTransport::listen(bind, PeerId::from_u64(1)).unwrap();
    let t2 = TcpTransport::listen(bind, PeerId::from_u64(2)).unwrap().0;
    t2.connect(l1).expect("dial 2 -> 1");

    let mut a = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(t1));
    let mut b = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(t2));

    let track = seed_track(a.crdt_mut());
    a.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(100),
        track_id: track,
        clip: ClipData::new("tcp_take", 0, 2_000),
        position: 0,
    });
    b.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(101),
        track_id: track,
        clip: ClipData::new("b_take", 5_000, 1_000),
        position: 1,
    });
    pump(&mut [&mut a, &mut b]);

    assert!(wait_until(Duration::from_secs(5), || {
        a.crdt().view() == b.crdt().view() && a.crdt().view().clips.len() == 2
    }));
}

#[test]
fn joining_peer_requests_snapshot_and_converges() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (t1, l1) = TcpTransport::listen(bind, PeerId::from_u64(1)).unwrap();
    let mut a = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(t1));

    let track = seed_track(a.crdt_mut());
    for i in 0..3_u64 {
        a.apply_local(TimelineOperation::InsertClip {
            clip_id: ClipId::from_u64(200 + i),
            track_id: track,
            clip: ClipData::new("clip", i * 1_000, 900),
            position: i,
        });
    }

    // Late joiner: connects and asks for state.
    let t2 = TcpTransport::listen(bind, PeerId::from_u64(2)).unwrap().0;
    t2.connect(l1).expect("dial 2 -> 1");
    let mut b = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(t2));
    b.request_snapshot();
    pump(&mut [&mut a, &mut b]);

    assert!(wait_until(Duration::from_secs(5), || {
        a.crdt().view() == b.crdt().view() && b.crdt().operation_log().len() >= 4
    }));
}

#[test]
fn offline_edits_flush_on_reconnect() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (t1, l1) = TcpTransport::listen(bind, PeerId::from_u64(1)).unwrap();
    let mut a = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(t1));
    let track = seed_track(a.crdt_mut());

    // A is alone: broadcast fails and the edit queues offline.
    a.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(300),
        track_id: track,
        clip: ClipData::new("while_offline", 0, 500),
        position: 0,
    });
    assert_eq!(a.offline_queue_len(), 1);

    // A connects to a fresh peer B who never saw the edit; the queue
    // flushes on join.
    let t2 = TcpTransport::listen(bind, PeerId::from_u64(2)).unwrap().0;
    t2.connect(l1).expect("dial 2 -> 1");
    let mut b = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(t2));
    pump(&mut [&mut a, &mut b]);

    assert!(wait_until(Duration::from_secs(5), || {
        a.crdt().view() == b.crdt().view() && b.crdt().view().clips.len() == 1
    }));
    assert_eq!(
        b.crdt().view().clips[0].name, "while_offline",
        "offline edit must reach the reconnecting peer"
    );
    assert_eq!(a.offline_queue_len(), 0);
}

#[test]
fn three_peers_converge_over_tcp_mesh() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (t1, l1) = TcpTransport::listen(bind, PeerId::from_u64(1)).unwrap();
    let (t2, l2) = TcpTransport::listen(bind, PeerId::from_u64(2)).unwrap();
    let t3 = TcpTransport::listen(bind, PeerId::from_u64(3)).unwrap().0;

    // Full mesh: every transport knows every peer. Clones share the same
    // underlying peer map, so dialing before boxing the engines is enough.
    let t1_dial = t1.clone();
    t1_dial.connect(l2).expect("dial 1 -> 2");
    let t2_dial = t2.clone();
    t2_dial.connect(l1).expect("dial 2 -> 1");
    let t3_dial = t3.clone();
    t3_dial.connect(l1).expect("dial 3 -> 1");
    t3_dial.connect(l2).expect("dial 3 -> 2");

    let mut e1 = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(t1));
    let mut e2 = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(t2));
    let mut e3 = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(3)), Box::new(t3));

    let track = seed_track(e1.crdt_mut());
    e1.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(400),
        track_id: track,
        clip: ClipData::new("from-1", 0, 1_000),
        position: 0,
    });
    e2.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(401),
        track_id: track,
        clip: ClipData::new("from-2", 5_000, 1_000),
        position: 1,
    });
    pump(&mut [&mut e1, &mut e2, &mut e3]);

    let ok = wait_until(Duration::from_secs(5), || {
        e1.crdt().view() == e2.crdt().view()
            && e2.crdt().view() == e3.crdt().view()
            && e1.crdt().view().clips.len() == 2
    });
    if !ok {
        eprintln!("e1 peers {:?} log {}", e1.transport().peers(), e1.crdt().operation_log().len());
        eprintln!("e2 peers {:?} log {}", e2.transport().peers(), e2.crdt().operation_log().len());
        eprintln!("e3 peers {:?} log {}", e3.transport().peers(), e3.crdt().operation_log().len());
        for (name, e) in [("e1", &e1), ("e2", &e2), ("e3", &e3)] {
            eprintln!("{name} clips: {:?}", e.crdt().view().clips.iter().map(|c| (c.clip_id.as_u64(), c.name.clone())).collect::<Vec<_>>());
        }
    }
    assert!(ok);
}
