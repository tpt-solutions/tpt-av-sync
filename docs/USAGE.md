# tpt-av-sync — Usage Guide

A practical tour of the collaboration stack: from a single-process CRDT to a multi-peer, playhead-synchronized, presence-aware session. For architecture and design rationale see [DESIGN.md](../DESIGN.md); runnable versions of everything here live in [`examples/`](../examples).

- **Crate docs:** each crate's README (linked below) documents its full API surface.
- **MSRV:** Rust 1.75+.

---

## 1. Local CRDT only (`tpt-av-sync-crdt`)

No networking involved — replicate however you like (files, HTTP, a string and a dream).

```toml
[dependencies]
tpt-av-sync-crdt = "0.1"
```

```rust
use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId, TrimEdge};
use tpt_av_sync_utils::PeerId;

let mut crdt = TimelineCrdt::new(PeerId::generate());

// Build the session.
let vo = TrackId::from_u64(1);
crdt.apply_local(TimelineOperation::InsertTrack {
    track_id: vo,
    track: TrackData::new("Dialog"),
    position: 0,
});
let take = ClipId::from_u64(100);
crdt.apply_local(TimelineOperation::InsertClip {
    clip_id: take,
    track_id: vo,
    clip: ClipData::new("take_03.wav", 0, 480_000),
    position: 0,
});

// Edit.
crdt.apply_local(TimelineOperation::SplitClip {
    clip_id: take,
    split_frame: 96_000,
    new_clip_id: ClipId::from_u64(101),
});
crdt.apply_local(TimelineOperation::TrimClip {
    clip_id: take,
    new_start_frame: 0,
    new_duration: 80_000,
    edge: TrimEdge::End,
});

// Undo/redo (replicates like any other edit).
crdt.undo();
crdt.redo();

// Render.
for clip in &crdt.view().clips {
    println!("{} @ {} (+{})", clip.name, clip.start_frame, clip.duration_frames);
}
```

**Syncing two replicas:** every `apply_local` returns a `TaggedOperation` — ship it to peers and feed received ones into `apply_remote`. Application is idempotent (duplicates are dropped) and order-tolerant (ops referencing unseen entities buffer until they arrive):

```rust
// on peer A:
let op = a.apply_local(/* ... */);
send_to_peer_b(op);

// on peer B:
b.apply_remote(op_received)?;
assert_eq!(a.view(), b.view()); // eventually, and in any order
```

**New peers / reconnects:** exchange `snapshot()`s — `TimelineCrdt::from_snapshot` builds a replica by replaying; `merge_snapshot` folds a snapshot into existing state (the offline-resync path).

## 2. Adding the network (`tpt-av-sync-net`)

```toml
[dependencies]
tpt-av-sync-net = "0.1"   # TCP + WebSocket; add feature "webrtc" for P2P data channels
```

The `SyncEngine` pairs a `TimelineCrdt` with a `Transport` and handles dispatch, acks/resends, snapshots on join, and the offline queue. Pick a transport:

```rust
use std::net::SocketAddr;
use tpt_av_sync_crdt::TimelineCrdt;
use tpt_av_sync_net::{SyncEngine, TcpTransport, WebsocketTransport};
use tpt_av_sync_utils::PeerId;

// LAN / direct (mesh: dial every peer).
let addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
let (tcp, listen) = TcpTransport::listen(addr, PeerId::generate())?;
// another process: TcpTransport::listen(..)?.0.connect(listen)?;

// Or server-starred (studio/WAN):
let (ws, ws_listen) = WebsocketTransport::serve(addr, PeerId::generate())?;
// others: WebsocketTransport::serve(..)?.0.connect(&format!("ws://{ws_listen}"))?;

let mut engine = SyncEngine::new(TimelineCrdt::new(PeerId::generate()), Box::new(tcp));
```

The application loop:

```rust
loop {
    // 1. user edits
    // engine.apply_local(TimelineOperation::MoveClip { .. })?;  // auto-broadcast

    // 2. pump the network
    engine.process_messages();

    // 3. react
    for event in engine.take_events() {
        use tpt_av_sync_net::SyncEvent::*;
        match event {
            PeerJoined(peer) => { /* someone arrived; state was pushed to them */ }
            RemoteOperation(op) => { /* update undo/UI state */ }
            SnapshotMerged { applied, .. } => { /* resync progress */ }
            _ => {}
        }
    }

    std::thread::sleep(std::time::Duration::from_millis(4));
}
```

Notes:

- **Offline-first:** if nobody is connected, `apply_local` queues instead of failing; the queue flushes when a peer joins. `engine.offline_queue_len()` shows the backlog.
- **Reliability:** broadcasts are tracked until acked and resent with a bounded budget (tunable via `EngineConfig`).
- **Batching:** wrap `OperationBatcher` around high-frequency edit streams (16 ms default) and broadcast `SyncMessage::Batch`es.
- **Discovery:** `MulticastDiscovery::new(..)` finds LAN peers (name/session/port beacons); dial what it advertises with `TcpTransport`.
- **WebRTC** (feature `webrtc`): `WebRtcTransport::new` + `dial`, shuffle `take_outgoing_signal()` / `handle_signal` envelopes with the remote side (any relay works — see §5). `is_ready(peer)` tells you when the data channel is live.

## 3. Synchronized playback (`tpt-av-sync-playhead`)

```toml
[dependencies]
tpt-av-sync-playhead = "0.1"
```

```rust
use tpt_av_sync_playhead::{PlayheadSync, TransportControl, TransportSync};
use tpt_av_sync_utils::PeerId;

let mut me = PlayheadSync::new(PeerId::generate(), 48_000);
let mut transport = TransportSync::new();

// Elect/follow a master (lowest peer id is a fine deterministic rule).
let master: PeerId = /* session master */;
if master == me.peer_id() { me.set_master(true); transport.set_master(true); }
else { me.follow_master(master); }

// Periodically: NTP-style clock sync.
let req = me.send_clock_sync_request(master);
// ... transport.send(SyncMessage::ClockSync(req)); on the response:
// me.process_clock_sync(response_stamped_with_t4);

// On transport commands from the master (SyncEvent::TransportControl):
// transport.on_remote_control(&control, master)?;

// Playback loop (UI thread): broadcast every ~10 ms while playing.
// me.set_local_position(app_playhead); me.set_playing(true);
// let update = me.generate_update();  // -> SyncMessage::PlayheadUpdate

// Audio/render thread — hot path, allocation-free and lock-free:
let frame = me.synchronized_position();
```

Clock-sync round trips should repeat every few seconds; `DriftCompensator` keeps positions aligned in between. Precision and drift behavior are proven in `tpt-av-sync-playhead/tests/precision.rs`.

## 4. Presence and cursors (`tpt-av-sync-presence`)

```toml
[dependencies]
tpt-av-sync-presence = "0.1"
```

```rust
use tpt_av_sync_presence::{
    AvatarData, Color, CursorState, IdleConfig, PresenceManager, UserInfo,
};
use tpt_av_sync_utils::PeerId;

let mut presence = PresenceManager::new(
    UserInfo::online(PeerId::from_u64(1), "Alice", now_ms)
        .with_avatar(AvatarData::from_url("https://cdn/alice.png", Color::rgb(230, 90, 90))),
    IdleConfig::default(),
);

// On cursor/selection/playhead interaction (~ every frame or on change):
presence.update_local_cursor(CursorState::new(now_ms).with_playhead(frame).with_selection(a, b));
let update = presence.generate_update(); // -> SyncMessage::PresenceUpdate via the engine

// On SyncEvent::Presence(update): presence.receive_update(update);
// On peer disconnect: presence.handle_peer_leave(peer);

// Drive transitions (Online -> Idle -> Offline) from your clock:
presence.tick(now_ms);
for (peer, cursor) in presence.remote_cursors() {
    render_cursor(peer, cursor); // skip when they're offline — the list already hides them
}
```

## 5. Server assist (`tpt-av-sync-server`)

Everything so far is serverless. Add the server crate when you need a rendezvous or a fallback:

- **Signaling** (`SignalingServer`): peers exchange WebRTC `SignalEnvelope`s through JSON rooms; the media path stays P2P.
- **Relay** (`RelayServer` + `RelayClientTransport`): when direct connections are impossible, the engine runs unmodified over the relay.
- **Persistence** (`SessionStore`): attach to the relay and operations are journaled per room; joining peers bootstrap from history even if every editor left.

```rust
use std::net::SocketAddr;
use tpt_av_sync_net::SyncEngine;
use tpt_av_sync_server::{RelayClientTransport, RelayServer, SessionStore};
use tpt_av_sync_crdt::TimelineCrdt;
use tpt_av_sync_utils::PeerId;

let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
let store = SessionStore::open("./sessions")?;
let (_server, bound) = RelayServer::serve(addr, Some(store))?;

let transport = RelayClientTransport::connect(bound, "episode-12", PeerId::generate())?;
let engine = SyncEngine::new(TimelineCrdt::new(PeerId::generate()), Box::new(transport));
// ... same loop as §2
```

## 6. End-to-end demos

```sh
cargo run -p tpt-av-sync-examples --bin collaborative_editor  # concurrent split+trim converging over TCP
cargo run -p tpt-av-sync-examples --bin playhead_sync         # NTP clock sync + <1 ms playback tracking
cargo run -p tpt-av-sync-examples --bin presence_demo         # cursors, idle aging, departure
cargo run -p tpt-av-sync-examples --bin offline_sync          # offline queue → reconnect merge
```

## 7. Real-time-safety checklist (audio threads)

Safe on the render thread:

- `PlayheadSync::synchronized_position` / `set_local_position` — arithmetic + one hash probe, no allocation/locking/syscalls.

Keep off the render thread:

- `SyncEngine` (I/O, allocation) — poll it on a UI/worker thread and hand results to the audio thread through your own lock-free bridge.
- CRDT application — do it on the control thread; ship the resulting position/state to the audio thread.

## 8. Licensing and dependencies

The workspace is dual-licensed MIT OR Apache-2.0. `deny.toml` allows only permissive licenses (MIT, Apache-2.0, BSD, ISC, Zlib, + Unicode/CC0 in the transitive tree) and denies GPL/LGPL/AGPL/MPL; CI runs `cargo deny check` on every push, including the optional WebRTC feature's tree. Contributions must not introduce copyleft dependencies or C/C++ bindings.
