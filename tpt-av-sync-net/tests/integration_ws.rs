//! Integration tests over WebSocket (default `websocket` feature).

use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_net::{SyncEngine, WebsocketTransport};
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

fn url(addr: SocketAddr) -> String {
    format!("ws://{addr}")
}

#[test]
fn two_peers_exchange_operations_over_websocket() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (t1, l1) = WebsocketTransport::serve(bind, PeerId::from_u64(1)).expect("serve");
    let t2 = WebsocketTransport::serve(bind, PeerId::from_u64(2))
        .expect("serve")
        .0;
    t2.connect(&url(l1)).expect("dial 2 -> 1");

    let mut a = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(t1));
    let mut b = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(t2));

    let track = TrackId::from_u64(1);
    a.crdt_mut().apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("A1"),
        position: 0,
    });
    a.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(500),
        track_id: track,
        clip: ClipData::new("ws_take", 0, 1_000),
        position: 0,
    });

    for _ in 0..20 {
        a.process_messages();
        b.process_messages();
        thread::sleep(Duration::from_millis(5));
    }

    assert!(wait_until(Duration::from_secs(5), || {
        a.crdt().view() == b.crdt().view() && b.crdt().view().clips.len() == 1
    }));
}
