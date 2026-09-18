# tpt-av-sync-presence

User presence and awareness for [`tpt-av-sync`](https://github.com/tpt-solutions/tpt-av-sync): who else is in the session, where their cursors are, and whether they're active — the "Figma bar" for media timelines.

## What's inside

| Module | Contents |
| :--- | :--- |
| `presence` | `PresenceManager` (local identity, remote users, cursors), `UserInfo`, `PresenceState` (Online/Idle/Offline), `PresenceUpdate` wire message. |
| `cursor` | `CursorState` — playhead, selection range, focused clip, timestamp. Builder-style constructors. |
| `avatar` | `AvatarData` (URL or inline PNG), `Color` (RGBA, packs to `0xAARRGGBB`). |
| `activity` | `ActivityTracker` + `IdleConfig` — Online → Idle → Offline transitions computed from injected timestamps. |

## Design notes

- **No hidden clocks.** Every time-dependent behavior takes `now_ms` as a parameter, so state transitions are deterministic, testable, and can be driven by the host application's clock or the session's synchronized clock.
- **Graceful and ungraceful departure.** A `leaving` update (or `handle_peer_leave` after a silent disconnect) marks a user Offline but keeps their entry — the UI can show "away" — and hides their cursor.
- **Wire-ready.** `PresenceUpdate` and everything it carries serialize losslessly through `bincode`; ship it via `SyncMessage::PresenceUpdate` in `tpt-av-sync-net`.

## Usage

```rust
use tpt_av_sync_presence::{
    AvatarData, Color, CursorState, IdleConfig, PresenceManager, PresenceState, UserInfo,
};
use tpt_av_sync_utils::PeerId;

let mut alice = PresenceManager::new(
    UserInfo::online(PeerId::from_u64(1), "Alice", 1_000)
        .with_avatar(AvatarData::from_url(
            "https://cdn.example/alice.png",
            Color::rgb(230, 90, 90),
        )),
    IdleConfig::default(), // 30 s idle, 5 min offline
);

// Local cursor moves; the update to broadcast:
alice.update_local_cursor(
    CursorState::new(1_500).with_playhead(48_000).with_selection(0, 960),
);
let update = alice.generate_update(); // -> SyncMessage::PresenceUpdate

// Receiving a peer's update:
// alice.receive_update(update_from_bob);
for (peer, cursor) in alice.remote_cursors() {
    println!("{peer} is looking at frame {:?}", cursor.playhead);
}

// Drive the state machine with your clock:
alice.tick(60_000); // an hour of silence -> Idle/Offline transitions
assert_eq!(alice.local_user().presence, PresenceState::Offline);
```

Round-trip flows (two managers exchanging updates over bincode, idle aging on both sides) are covered in `tests/presence_flow.rs`.

## Feature flags

None.

## Minimum supported Rust

1.75 (workspace MSRV).

## License

Dual-licensed under [MIT](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-MIT) OR [Apache-2.0](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-APACHE).
