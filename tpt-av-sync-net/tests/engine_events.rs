//! Engine event pass-through: playhead, presence, clock sync, and
//! transport-control messages surface as [`SyncEvent`]s.

use std::time::Duration;

use tpt_av_sync_crdt::TimelineCrdt;
use tpt_av_sync_net::{LoopbackTransport, SyncEngine, SyncEvent, SyncMessage};
use tpt_av_sync_playhead::{ClockSyncMessage, PlayheadUpdate, TransportControl};
use tpt_av_sync_presence::{CursorState, PresenceUpdate, UserInfo};
use tpt_av_sync_utils::PeerId;

#[test]
fn playhead_presence_and_clock_events_pass_through() {
    let (mut ta, mut tb) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));

    // A sends a playhead update, transport control, and clock sync.
    use tpt_av_sync_net::Transport as _;
    ta.broadcast(SyncMessage::PlayheadUpdate(PlayheadUpdate {
        peer_id: PeerId::from_u64(1),
        position: 48_000,
        timestamp_ms: 1_000,
        playing: true,
    }))
    .unwrap();
    ta.broadcast(SyncMessage::TransportControl(TransportControl::Play {
        position: 48_000,
    }))
    .unwrap();
    ta.broadcast(SyncMessage::ClockSync(ClockSyncMessage::request(
        PeerId::from_u64(1),
        100,
    )))
    .unwrap();

    // B sends presence.
    tb.broadcast(SyncMessage::PresenceUpdate(PresenceUpdate {
        peer_id: PeerId::from_u64(2),
        user: UserInfo::online(PeerId::from_u64(2), "Bob", 0),
        cursor: Some(CursorState::new(10).with_playhead(96_000)),
        leaving: false,
    }))
    .unwrap();

    let mut a = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(ta));
    let mut b = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(tb));

    for _ in 0..10 {
        a.process_messages();
        b.process_messages();
        std::thread::sleep(Duration::from_millis(2));
    }

    let b_events = b.take_events();
    assert!(
        b_events.iter().any(|e| matches!(e, SyncEvent::Playhead(u)
            if u.position == 48_000 && u.playing)),
        "playhead event expected: {b_events:?}"
    );
    assert!(
        b_events.iter().any(|e| matches!(e, SyncEvent::TransportControl(c)
            if *c == TransportControl::Play { position: 48_000 })),
        "transport control expected: {b_events:?}"
    );
    assert!(
        b_events.iter().any(|e| matches!(e, SyncEvent::ClockSync(_))),
        "clock sync expected: {b_events:?}"
    );

    let a_events = a.take_events();
    assert!(
        a_events.iter().any(|e| matches!(e, SyncEvent::Presence(p)
            if p.peer_id == PeerId::from_u64(2) && p.user.name == "Bob")),
        "presence event expected: {a_events:?}"
    );
}
