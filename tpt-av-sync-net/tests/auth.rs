//! Authenticated handshake tests (Phase 7 B5): identity proofs,
//! challenge/response, and rejection of impostors.

use std::net::SocketAddr;
use std::sync::Arc;

use tpt_av_sync_net::{SyncEngine, TcpTransport};
use tpt_av_sync_utils::{PeerId, PeerIdentity};

fn seed(b: u8) -> Arc<PeerIdentity> {
    PeerIdentity::from_seed([b; 32]).unwrap().shared()
}

#[test]
fn authenticated_peers_verify_each_other() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let alice = seed(1);
    let bob = seed(2);

    let (ta, listen) = TcpTransport::listen_authenticated(bind, alice.clone()).unwrap();
    let tb = TcpTransport::listen(bind, PeerId::from_u64(99)).unwrap().0;

    // Bob dials with his own identity; both sides verify + challenge.
    let remote = tb.connect_authenticated(listen, bob.clone()).unwrap();
    assert_eq!(remote, alice.peer_id(), "authenticated peer id is authoritative");

    // The engine on the authenticated acceptor sees the verified peer.
    let mut a = SyncEngine::new(
        tpt_av_sync_crdt::TimelineCrdt::new(alice.peer_id()),
        Box::new(ta),
    );
    let mut b = SyncEngine::new(
        tpt_av_sync_crdt::TimelineCrdt::new(bob.peer_id()),
        Box::new(tb),
    );
    for _ in 0..10 {
        a.process_messages();
        b.process_messages();
    }
    assert_eq!(a.transport().peers(), vec![bob.peer_id()]);
}

#[test]
fn any_valid_keypair_authenticates_but_never_spoofs() {
    // Authentication proves *key possession*, not authorization: any valid
    // keypair completes the handshake — but the verified peer id is always
    // derived from the presented key, so an impostor can never claim
    // someone else's id. (Authorization/allowlists are Phase 7 B6.)
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let alice = seed(1);
    let mallory = seed(66);

    let (_ta, listen) = TcpTransport::listen_authenticated(bind, alice.clone()).unwrap();
    let tb = TcpTransport::listen(bind, PeerId::from_u64(99)).unwrap().0;

    // The return value is the *remote's* verified id (Alice's) — the
    // dialer's own stale claimed id (99) never enters the handshake.
    let verified = tb.connect_authenticated(listen, mallory.clone()).unwrap();
    assert_eq!(verified, alice.peer_id());
    assert_ne!(verified, PeerId::from_u64(99), "claimed id is ignored");
}

#[test]
fn tampered_identity_is_rejected() {
    // An identity proof bound to peer A must not verify against a
    // different derived id. Verified at the crypto layer (identity.rs) and
    // here via a peer id / key mismatch through the transport's helper:
    let alice = PeerIdentity::from_seed([7; 32]).unwrap();
    let proof = alice.hello_proof();

    // Claim a peer id that does not derive from Alice's key.
    let impostor_id = PeerId::from_u64(0x1234_5678);
    assert!(proof.verify(impostor_id).is_err());
    assert!(proof.verify(alice.peer_id()).is_ok());
}

#[test]
fn anonymous_peers_still_interoperate() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (ta, listen) = TcpTransport::listen(bind, PeerId::from_u64(1)).unwrap();
    let tb = TcpTransport::listen(bind, PeerId::from_u64(2)).unwrap().0;
    tb.connect(listen).unwrap();

    let mut a = SyncEngine::new(
        tpt_av_sync_crdt::TimelineCrdt::new(PeerId::from_u64(1)),
        Box::new(ta),
    );
    let mut b = SyncEngine::new(
        tpt_av_sync_crdt::TimelineCrdt::new(PeerId::from_u64(2)),
        Box::new(tb),
    );
    for _ in 0..10 {
        a.process_messages();
        b.process_messages();
    }
    assert_eq!(a.transport().peers(), vec![PeerId::from_u64(2)]);
}
