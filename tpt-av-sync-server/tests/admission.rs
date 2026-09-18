//! Integration tests for server admission control (Phase 7 B3): Origin
//! allowlist, per-IP connection caps, and rate limiting.

use std::net::SocketAddr;
use std::time::Duration;

use tpt_av_sync_net::{SyncMessage, Transport};
use tpt_av_sync_server::{RelayClientTransport, RelayServer, ServerLimits};
use tpt_av_sync_utils::{OperationId, PeerId};

#[test]
fn origin_allowlist_rejects_clients_without_origin() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let limits = ServerLimits {
        allowed_origins: vec!["https://studio.example".into()],
        ..ServerLimits::default()
    };
    let (_server, addr) = RelayServer::serve_with_limits(bind, limits, None).expect("serve");

    // tokio-tungstenite sends no Origin header: with a non-empty allowlist
    // the handshake must be refused.
    let result = RelayClientTransport::connect(addr, "room", PeerId::from_u64(1));
    assert!(
        result.is_err(),
        "client without an allowed Origin must be rejected"
    );
}

#[test]
fn per_ip_connection_cap_admits_up_to_the_limit() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let limits = ServerLimits {
        max_connections: 8,
        max_connections_per_ip: 2,
        ..ServerLimits::default()
    };
    let (_server, addr) = RelayServer::serve_with_limits(bind, limits, None).expect("serve");

    let first = RelayClientTransport::connect(addr, "room", PeerId::from_u64(1));
    let second = RelayClientTransport::connect(addr, "room", PeerId::from_u64(2));
    let third = RelayClientTransport::connect(addr, "room", PeerId::from_u64(3));

    assert!(first.is_ok(), "first connection admitted");
    assert!(second.is_ok(), "second connection admitted");
    assert!(third.is_err(), "third connection from one IP must be refused");
}

#[test]
fn flooding_connection_is_rate_limited() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let limits = ServerLimits {
        rate_burst: 3,
        rate_refill_per_sec: 1,
        ..ServerLimits::default()
    };
    let (_server, addr) = RelayServer::serve_with_limits(bind, limits, None).expect("serve");

    // A peer that blasts more than `rate_burst` frames gets its server-side
    // connection closed; only the initial burst reaches the receiver.
    let mut sender = RelayClientTransport::connect(addr, "room", PeerId::from_u64(1))
        .expect("sender admitted");
    let mut receiver = RelayClientTransport::connect(addr, "room", PeerId::from_u64(2))
        .expect("receiver admitted");
    std::thread::sleep(Duration::from_millis(100)); // both joins processed

    for i in 0..64_u64 {
        sender
            .broadcast(SyncMessage::Ack(OperationId::new(i, PeerId::from_u64(1))))
            .ok();
    }
    std::thread::sleep(Duration::from_millis(200));

    let mut delivered = 0_usize;
    while let Ok(Some((_, SyncMessage::Ack(_)))) = receiver.try_recv() {
        delivered += 1;
    }
    assert!(
        delivered < 64,
        "rate limiter must cut a flooding sender short (delivered {delivered})"
    );
}
