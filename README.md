# tpt-av-sync

**A pure-Rust, CRDT-based real-time collaboration engine for media timelines.**
Multi-user editing, playhead synchronization, and conflict-free state replication — the collaboration layer of the TPT AV stack.

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE-MIT)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE-APACHE)
[![CI](https://github.com/tpt-solutions/tpt-av-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/tpt-solutions/tpt-av-sync/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/tpt-av-sync-crdt.svg)](https://crates.io/crates/tpt-av-sync-crdt)

## Vision

`tpt-av-sync` lets multiple users edit the same audio or video timeline simultaneously, with automatic conflict resolution and real-time synchronization of edits, playhead positions, and parameter changes.

- **Media-specific CRDTs** — conflict-free data structures designed for timeline operations (clip insert / move / delete / split / trim).
- **Real-time playhead synchronization** — sub-millisecond precision across network connections, safe for audio/render threads (allocation-free, lock-free hot paths).
- **Peer-to-peer architecture** — WebRTC data channels for low-latency collaboration; TCP and WebSocket transports for LAN and server-based setups.
- **Offline-first** — edits are queued locally while disconnected and merge automatically on reconnect.
- **Pure Rust** — no C/C++ bindings anywhere in the sync stack.
- **Permissive licensing** — dual-licensed MIT OR Apache-2.0, enforced with `cargo-deny` in CI (GPL/LGPL/MPL/AGPL dependencies are rejected).

## Ecosystem

`tpt-av-sync` sits in the **collaboration layer** of the TPT AV stack, alongside `tpt-audio` and `tpt-visual`.

| Crate | Role | Relationship to `tpt-av-sync` |
| :--- | :--- | :--- |
| `tpt-audio` | Audio timeline, processing | `tpt-av-sync` replicates the audio timeline state across peers. |
| `tpt-visual` | Video timeline, compositing | `tpt-av-sync` replicates the video timeline state across peers. |
| **`tpt-av-sync`** | **Collaboration (this repo)** | CRDT engine, network sync, playhead synchronization, presence. |
| External peers | Other users' applications | Exchange timeline operations over WebRTC / WebSocket / TCP. |

## Workspace layout

| Crate | Purpose |
| :--- | :--- |
| [`tpt-av-sync-utils`](tpt-av-sync-utils) | Shared types: `PeerId`, `OperationId`, Lamport & vector clocks, time helpers, `SyncError`. |
| [`tpt-av-sync-crdt`](tpt-av-sync-crdt) | The CRDT engine: timeline operations, per-clip/track/envelope CRDTs, conflict resolution, undo/redo, snapshots, delta compression. |
| [`tpt-av-sync-net`](tpt-av-sync-net) | Network transport: `Transport` trait, TCP + WebSocket implementations, reliability (ack/retry), message batching, `SyncEngine`, peer discovery. Optional WebRTC data-channel transport (feature `webrtc`). |
| [`tpt-av-sync-playhead`](tpt-av-sync-playhead) | NTP-style clock sync, latency estimation, drift compensation, playhead & transport-control sync. Real-time safe. |
| [`tpt-av-sync-presence`](tpt-av-sync-presence) | User presence (online/idle/offline), remote cursors, avatars, activity tracking. |
| [`tpt-av-sync-server`](tpt-av-sync-server) | Optional server: WebRTC signaling, message relay fallback, session persistence. |
| [`examples`](examples) | Runnable demos: collaborative editor, playhead sync, presence, offline sync. |

Each crate is independently useful — use just the CRDT engine, just the network layer, or just the playhead sync.

## Quick start

Add the CRDT engine to your `Cargo.toml`:

```toml
[dependencies]
tpt-av-sync-crdt = "0.1"
tpt-av-sync-utils = "0.1"
```

Apply a local edit and get a replicable operation back:

```rust
use tpt_av_sync_crdt::{TimelineCrdt, TimelineOperation, ClipData};
use tpt_av_sync_utils::PeerId;

let mut crdt = TimelineCrdt::new(PeerId::generate());

// Create a track, then insert a clip on it.
let track_id = tpt_av_sync_crdt::TrackId::generate();
crdt.apply_local(tpt_av_sync_crdt::TimelineOperation::InsertTrack {
    track_id,
    track: tpt_av_sync_crdt::TrackData::new("Dialog"),
    position: 0,
});

let op = crdt.apply_local(TimelineOperation::InsertClip {
    clip_id: tpt_av_sync_crdt::ClipId::generate(),
    track_id,
    clip: ClipData::new("take_03.wav", 0, 48_000 * 10),
    position: 0,
});
// Send `op` to peers via tpt-av-sync-net; peers call `crdt.apply_remote(op)`.
```

See [DESIGN.md](DESIGN.md) for the full architecture and [docs/USAGE.md](docs/USAGE.md) for a getting-started guide covering networking, playhead sync, and presence.

## Status

Early-stage, pre-1.0. The phase plan lives in [todo.md](todo.md); the design in [DESIGN.md](DESIGN.md).

## Contributing & License

Dual-licensed under [MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE). By contributing you agree your contributions are licensed under the same terms, that no GPL/LGPL/AGPL/MPL dependencies may be introduced, and that all CRDT operations must remain provably commutative and idempotent (property tests enforce this in CI).

TPT Solutions Open Source — [opensource.tptsolutions.co.nz](https://opensource.tptsolutions.co.nz/)
