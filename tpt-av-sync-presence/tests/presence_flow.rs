//! End-to-end presence flow: two managers exchange wire updates, cursors
//! flow through serialization, and idle transitions age both sides.

use tpt_av_sync_presence::{
    CursorState, IdleConfig, PresenceManager, PresenceState, PresenceUpdate, UserInfo,
};
use tpt_av_sync_utils::wire;
use tpt_av_sync_utils::PeerId;

fn manager(peer: u64, name: &str) -> PresenceManager {
    PresenceManager::new(
        UserInfo::online(PeerId::from_u64(peer), name, 1_000),
        IdleConfig {
            idle_after_ms: 10,
            offline_after_ms: 100,
        },
    )
}

#[test]
fn two_peers_exchange_cursors_both_directions() {
    let mut alice = manager(1, "Alice");
    let mut bob = manager(2, "Bob");

    // Alice moves her cursor; the update goes over the wire.
    alice.update_local_cursor(CursorState::new(1_100).with_playhead(24_000));
    let wire = alice.generate_update();
    let bytes = wire::encode(&wire).unwrap();
    let received: PresenceUpdate = wire::decode(&bytes).unwrap();
    bob.receive_update(received);

    // Bob moves his; the update comes back the other way.
    bob.update_local_cursor(
        CursorState::new(1_200)
            .with_playhead(24_500)
            .with_selection(0, 480),
    );
    let wire = bob.generate_update();
    alice.receive_update(wire::decode(&wire::encode(&wire).unwrap()).unwrap());

    // Both see each other's cursor.
    assert_eq!(alice.remote_cursors().len(), 1);
    assert_eq!(bob.remote_cursors().len(), 1);
    let (id, cursor) = bob.remote_cursors()[0];
    assert_eq!(*id, PeerId::from_u64(1));
    assert_eq!(cursor.playhead, Some(24_000));
}

#[test]
fn idle_aging_applies_to_remote_users_via_tick() {
    let mut alice = manager(1, "Alice");
    let mut bob = manager(2, "Bob");

    bob.update_local_cursor(CursorState::new(1_050));
    let update = bob.generate_update();
    alice.receive_update(update);

    // Some time later Alice ticks; Bob has gone silent past idle_after.
    alice.tick(1_070);
    assert_eq!(alice.local_user().presence, PresenceState::Idle);
    assert_eq!(alice.remote_users()[0].presence, PresenceState::Idle);

    // Much later: offline on both sides.
    alice.tick(1_500);
    bob.tick(1_500);
    assert_eq!(alice.remote_users()[0].presence, PresenceState::Offline);
    assert_eq!(bob.local_user().presence, PresenceState::Offline);
    assert!(
        alice.remote_cursors().is_empty(),
        "offline users expose no cursor"
    );
}
