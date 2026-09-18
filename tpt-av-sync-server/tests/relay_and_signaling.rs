//! Integration tests: relay server routing, persistence-backed snapshot
//! bootstrap, and the signaling server fan-out.

use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_net::SyncEngine;
use tpt_av_sync_server::{RelayClientTransport, RelayServer, SessionStore};
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

#[test]
fn two_peers_sync_through_the_relay() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (_server, addr) = RelayServer::serve(bind, None).expect("serve relay");

    let ta = RelayClientTransport::connect(addr, "room-1", PeerId::from_u64(1))
        .expect("client 1");
    let tb = RelayClientTransport::connect(addr, "room-1", PeerId::from_u64(2))
        .expect("client 2");

    let mut a = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(ta));
    let mut b = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(tb));

    let track = TrackId::from_u64(1);
    a.crdt_mut().apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("A1"),
        position: 0,
    });
    a.apply_local(TimelineOperation::InsertClip {
        clip_id: ClipId::from_u64(600),
        track_id: track,
        clip: ClipData::new("via relay", 0, 1_000),
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

#[test]
fn relay_persists_and_bootstraps_lone_joiner() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let dir = std::env::temp_dir().join(format!("relay-store-{}", PeerId::generate().as_u64()));
    let store = SessionStore::open(&dir).expect("store");
    let (_server, addr) = RelayServer::serve(bind, Some(store)).expect("serve relay");

    // Editor A records two operations through the relay.
    {
        let ta = RelayClientTransport::connect(addr, "episode", PeerId::from_u64(1))
            .expect("client 1");
        let mut a = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(ta));
        let track = TrackId::from_u64(1);
        a.apply_local(TimelineOperation::InsertTrack {
            track_id: track,
            track: TrackData::new("A1"),
            position: 0,
        });
        a.apply_local(TimelineOperation::InsertClip {
            clip_id: ClipId::from_u64(700),
            track_id: track,
            clip: ClipData::new("recorded", 0, 1_000),
            position: 0,
        });
        for _ in 0..5 {
            a.process_messages();
            thread::sleep(Duration::from_millis(5));
        }
    } // A disconnects.

    // Give the relay a moment to persist; then a fresh peer bootstraps
    // from the server alone.
    thread::sleep(Duration::from_millis(100));
    let tb = RelayClientTransport::connect(addr, "episode", PeerId::from_u64(2))
        .expect("client 2");
    let mut b = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(tb));
    b.request_snapshot();

    let ok = wait_until(Duration::from_secs(5), || {
        b.process_messages();
        b.crdt().operation_log().len() >= 2 && b.crdt().view().clips.len() == 1
    });
    if !ok {
        eprintln!("b log {}", b.crdt().operation_log().len());
        eprintln!("events {:?}", b.take_events());
    }
    assert!(ok);
    assert_eq!(b.crdt().view().clips[0].name, "recorded");
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn signaling_server_fans_out_to_the_room() {
    

    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (_server, addr) = tpt_av_sync_server::SignalingServer::serve(bind).expect("serve signaling");

    async fn connect(
        addr: SocketAddr,
        _local: PeerId,
    ) -> tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    > {
        let (ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .expect("ws connect");
        ws
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        use futures_util::{SinkExt, StreamExt};
        let mut wa = connect(addr, PeerId::from_u64(1)).await;
        let mut wb = connect(addr, PeerId::from_u64(2)).await;

        // Both join "room-x".
        for (ws, peer) in [(&mut wa, 1_u64), (&mut wb, 2_u64)] {
            let frame = tpt_av_sync_server::SignalFrame {
                room: "room-x".into(),
                from: PeerId::from_u64(peer),
                to: None,
                payload: tpt_av_sync_server::SignalPayload::Join { token_proof: None },
            };
            ws.send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::to_string(&frame).unwrap(),
            ))
            .await
            .unwrap();
        }
        // Give the server a beat to register both members.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // A broadcasts an offer; B must receive it.
        let offer = tpt_av_sync_server::SignalFrame {
            room: "room-x".into(),
            from: PeerId::from_u64(1),
            to: None,
            payload: tpt_av_sync_server::SignalPayload::Offer {
                sdp: "v=0 fake".into(),
            },
        };
        wa.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&offer).unwrap(),
        ))
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(remaining > Duration::ZERO, "B must receive the offer");
            match tokio::time::timeout(remaining, wb.next()).await {
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                    let frame: tpt_av_sync_server::SignalFrame =
                        serde_json::from_str(&text).unwrap();
                    assert_eq!(frame.from, PeerId::from_u64(1));
                    assert!(matches!(
                        frame.payload,
                        tpt_av_sync_server::SignalPayload::Offer { .. }
                    ));
                    break;
                }
                Ok(Some(Ok(_))) => continue,
                _ => panic!("B must receive the offer"),
            }
        }
    });
}
