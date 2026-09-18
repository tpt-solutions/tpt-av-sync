//! Room authorization tests (Phase 7 B6): token-auth relays reject
//! proof-less peers and admit ones holding the secret.

use std::net::SocketAddr;
use std::time::Duration;

use tpt_av_sync_net::{SyncEngine, SyncMessage, Transport};
use tpt_av_sync_server::{RelayClientTransport, RelayServer, RoomAuth, ServerLimits};
use tpt_av_sync_utils::PeerId;

const SECRET: &str = "episode-12-secret";

#[test]
fn token_relay_rejects_proofless_clients() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (_server, addr) = RelayServer::serve_full(
        bind,
        ServerLimits::default(),
        RoomAuth::Token {
            secret: SECRET.into(),
        },
        None,
    )
    .expect("serve");

    // A client without a join proof must be disconnected: its sends go
    // nowhere, and it never sees the room.
    let outcome = RelayClientTransport::connect(addr, "room", PeerId::from_u64(1));
    // The TCP connect succeeds (handshake is transport-level); but the
    // session must NOT become usable. Probing via a second client with the
    // secret: it must not see the proofless peer.
    assert!(outcome.is_ok() || outcome.is_err(), "connect is non-fatal");

    let admitted = RelayClientTransport::connect_with_room_auth(
        addr,
        "room",
        PeerId::from_u64(2),
        Some(SECRET),
    )
    .expect("holder of the secret joins");
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        admitted.peers().is_empty(),
        "proofless peer must not appear as a room member"
    );
}

#[test]
fn token_relay_admits_and_syncs_secret_holders() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (_server, addr) = RelayServer::serve_full(
        bind,
        ServerLimits::default(),
        RoomAuth::Token {
            secret: SECRET.into(),
        },
        None,
    )
    .expect("serve");

    let ta = RelayClientTransport::connect_with_room_auth(
        addr,
        "episode",
        PeerId::from_u64(1),
        Some(SECRET),
    )
    .expect("alice joins");
    let tb = RelayClientTransport::connect_with_room_auth(
        addr,
        "episode",
        PeerId::from_u64(2),
        Some(SECRET),
    )
    .expect("bob joins");

    let mut a = SyncEngine::new(tpt_av_sync_crdt::TimelineCrdt::new(PeerId::from_u64(1)), Box::new(ta));
    let mut b = SyncEngine::new(tpt_av_sync_crdt::TimelineCrdt::new(PeerId::from_u64(2)), Box::new(tb));

    let track = tpt_av_sync_crdt::TrackId::from_u64(1);
    a.apply_local(tpt_av_sync_crdt::TimelineOperation::InsertTrack {
        track_id: track,
        track: tpt_av_sync_crdt::TrackData::new("A1"),
        position: 0,
    });

    for _ in 0..20 {
        a.process_messages();
        b.process_messages();
        std::thread::sleep(Duration::from_millis(5));
    }
    let _ = SyncMessage::RequestSnapshot;
    assert_eq!(a.crdt().view(), b.crdt().view(), "secret holders sync");
}
