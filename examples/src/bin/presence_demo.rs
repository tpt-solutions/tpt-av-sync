//! Remote cursors and presence: two users see each other's playhead,
//! selection, and online/idle state through the sync engine.
//!
//! Run with: `cargo run -p tpt-av-sync-examples --bin presence_demo`

use tpt_av_sync_crdt::TimelineCrdt;
use tpt_av_sync_net::{LoopbackTransport, SyncEngine, SyncEvent, SyncMessage, Transport};
use tpt_av_sync_presence::{
    AvatarData, Color, CursorState, IdleConfig, PresenceManager, PresenceState, UserInfo,
};
use tpt_av_sync_utils::PeerId;

fn main() {
    println!("=== tpt-av-sync: presence demo ===\n");

    let (mut ta, mut tb) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));

    // Alice publishes her identity before connecting.
    let mut alice_presence = PresenceManager::new(
        UserInfo::online(PeerId::from_u64(1), "Alice", 0)
            .with_avatar(AvatarData::from_url(
                "https://cdn.example.test/alice.png",
                Color::rgb(230, 90, 90),
            )),
        IdleConfig::default(),
    );
    let mut bob_presence = PresenceManager::new(
        UserInfo::online(PeerId::from_u64(2), "Bob", 0)
            .with_avatar(AvatarData::from_url(
                "https://cdn.example.test/bob.png",
                Color::rgb(90, 130, 230),
            )),
        IdleConfig::default(),
    );

    // Exchange presence + cursors through the sync engine.
    ta.broadcast(SyncMessage::PresenceUpdate(alice_presence.generate_update()))
        .expect("send");
    bob_presence.update_local_cursor(
        CursorState::new(1_000)
            .with_playhead(48_000)
            .with_selection(0, 960),
    );
    tb.broadcast(SyncMessage::PresenceUpdate(bob_presence.generate_update()))
        .expect("send");

    let mut alice_engine = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(ta));
    let mut bob_engine = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(tb));
    alice_engine.process_messages();
    bob_engine.process_messages();

    for event in alice_engine.take_events() {
        if let SyncEvent::Presence(update) = event {
            alice_presence.receive_update(update);
        }
    }
    for event in bob_engine.take_events() {
        if let SyncEvent::Presence(update) = event {
            bob_presence.receive_update(update);
        }
    }

    println!("Alice sees:");
    for user in alice_presence.remote_users() {
        println!(
            "  {} ({}), avatar accent #{:06x}, state {:?}",
            user.name,
            user.peer_id,
            user.avatar
                .as_ref()
                .map(|a| a.color.to_rgba_u32() >> 8)
                .unwrap_or(0),
            user.presence
        );
    }
    for (peer, cursor) in alice_presence.remote_cursors() {
        println!(
            "  cursor of {peer}: playhead {:?}, selection {:?}",
            cursor.playhead, cursor.selection
        );
    }

    // Bob goes idle (no activity past the idle threshold); both sides age.
    bob_presence.tick(60_000);
    alice_presence.tick(60_000);
    println!("\nafter an hour of silence:");
    println!("  Alice: {:?}", alice_presence.local_user().presence);
    println!("  Bob (as seen by Alice): {:?}",
        alice_presence.remote_users()[0].presence);

    // Bob disconnects without saying goodbye.
    alice_presence.handle_peer_leave(PeerId::from_u64(2));
    assert_eq!(
        alice_presence.remote_users()[0].presence,
        PresenceState::Offline
    );
    assert!(
        alice_presence.remote_cursors().is_empty(),
        "offline users expose no cursor"
    );
    println!("  Bob dropped: state Offline, cursor hidden ✓");
}
