# tpt-av-sync — Project Todo

CRDT-based real-time collaboration engine for media timelines. Dual-licensed MIT OR Apache-2.0. TPT Solutions Open Source.

---

## Phase 0 — Repo & Tooling Bootstrap

- [ ] Create GitHub repo `github.com/tpt-solutions/tpt-av-sync`
- [ ] `git init` locally, initial commit
- [ ] Add `LICENSE-MIT`
- [ ] Add `LICENSE-APACHE`
- [ ] Root `README.md` (vision, ecosystem table, dual-license badges)
- [ ] Root `Cargo.toml` workspace manifest
  - [ ] `resolver = "2"`, `members` list for all sub-crates
  - [ ] `[workspace.package]` with `license = "MIT OR Apache-2.0"`, `edition`, `rust-version`, `repository`
  - [ ] `[workspace.dependencies]` (tokio, tokio-tungstenite, serde, bincode, log, webrtc)
- [ ] `deny.toml` (cargo-deny)
  - [ ] Allow: MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Zlib
  - [ ] Deny: GPL-2.0, GPL-3.0, LGPL-2.1, LGPL-3.0, AGPL-3.0, MPL-2.0
- [ ] GitHub Actions CI workflow
  - [ ] `cargo build --workspace`
  - [ ] `cargo test --workspace`
  - [ ] `cargo clippy --workspace -- -D warnings`
  - [ ] `cargo deny check`
- [ ] `DESIGN.md` (carry over spec.txt as living design doc)
- [ ] `.gitignore` for Rust (`/target`, etc.)

---

## Phase 1 — Foundation & CRDT Core

### `tpt-av-sync-utils`
- [ ] Scaffold crate (`Cargo.toml`, `src/lib.rs`)
- [ ] `peer_id.rs` — `PeerId` type
- [ ] `operation_id.rs` — `OperationId` (Lamport timestamp + peer id)
- [ ] `clock.rs` — Lamport clock
- [ ] `clock.rs` — `VectorClock` (`increment`, `merge`, `happens_before`, `is_concurrent`)
- [ ] `time.rs` — network time sync types
- [ ] `error.rs` — `SyncError` enum
- [ ] Unit tests: Lamport clock ordering
- [ ] Unit tests: vector clock merge / happens-before / concurrency detection

### `tpt-av-sync-crdt`
- [ ] Scaffold crate (`Cargo.toml`, `src/lib.rs`, `tests/`)
- [ ] `operation.rs` — `TimelineOperation` enum
  - [ ] `InsertClip`, `MoveClip`, `DeleteClip`, `SplitClip`, `TrimClip`
  - [ ] `UpdateClipMetadata`, `InsertTrack`, `DeleteTrack`
  - [ ] `UpdateEnvelope`, `UpdateSessionMetadata`
  - [ ] `TaggedOperation` struct (op_id, operation, lamport_ts, vector_clock, peer_id, timestamp)
- [ ] `state.rs` — `Session` state model (tracks, clips, envelopes, session metadata)
- [ ] `clip_crdt.rs` — per-clip CRDT structure
- [ ] `track_crdt.rs` — per-track CRDT structure
- [ ] `envelope_crdt.rs` — automation envelope CRDT structure
- [ ] `merge.rs` — conflict resolution logic
  - [ ] Last-writer-wins via Lamport timestamp (concurrent moves)
  - [ ] Idempotent delete handling (concurrent deletes)
  - [ ] Split/merge semantics (concurrent splits → 3-clip result)
- [ ] `history.rs` — operation history log, undo/redo
- [ ] `timeline_crdt.rs` — `TimelineCrdt` struct
  - [ ] `new(local_peer_id)`
  - [ ] `apply_local(operation) -> TaggedOperation`
  - [ ] `apply_remote(tagged_op) -> Result<(), SyncError>`
  - [ ] `session()`, `operation_log()`
  - [ ] `snapshot() -> TimelineSnapshot`
  - [ ] `from_snapshot(snapshot, local_peer_id) -> Self`
- [ ] Property-based tests (`proptest`) — commutativity for each operation type
- [ ] Property-based tests (`proptest`) — idempotency for each operation type
- [ ] Scenario test: concurrent clip moves (spec §5.1)
- [ ] Scenario test: concurrent clip deletes (spec §5.1)
- [ ] Scenario test: concurrent clip splits (spec §5.1)
- [ ] `cargo-deny` CI gate green for this crate's dependency tree

---

## Phase 2 — Network Transport

### `tpt-av-sync-net`
- [ ] Scaffold crate (`Cargo.toml`, `src/lib.rs`, `tests/`)
- [ ] `message.rs` — `SyncMessage` enum (Operation, RequestSnapshot, Snapshot, PlayheadUpdate, TransportControl, PresenceUpdate, ClockSync, Ack)
- [ ] Message (de)serialization via `serde` + `bincode`
- [ ] `transport.rs` — `Transport` trait (`send_to`, `broadcast`, `recv`, `try_recv`, `peers`)
- [ ] `peer.rs` — peer management/state tracking
- [ ] `tcp.rs` — raw TCP transport implementation (LAN testing)
- [ ] `websocket.rs` — WebSocket transport (`tokio-tungstenite`, server-based)
- [ ] `reliability.rs` — message ordering, ack tracking, retry
- [ ] `discovery.rs` — peer discovery stub (interface only, impl deferred to Phase 5)
- [ ] `SyncEngine`
  - [ ] `new(crdt, transport)`
  - [ ] `apply_local(operation)` — apply + broadcast + track pending
  - [ ] `process_messages()` — dispatch Operation/Ack/Snapshot/PlayheadUpdate/PresenceUpdate
  - [ ] `handle_peer_join(peer_id)` — send snapshot to new peer
  - [ ] `handle_peer_leave(peer_id)`
- [ ] Integration test: 2 peers exchange operations over TCP
- [ ] Integration test: 2 peers exchange operations over WebSocket
- [ ] Integration test: snapshot request/response on peer join
- [ ] Integration test: reconnect after disconnect (offline-first groundwork)

---

## Phase 3 — Playhead Synchronization

### `tpt-av-sync-playhead`
- [ ] Scaffold crate (`Cargo.toml`, `src/lib.rs`, `tests/`)
- [ ] `clock.rs` — NTP-like clock sync (`ClockSyncMessage`, T1–T4 offset/latency calculation)
- [ ] `latency.rs` — round-trip latency estimation and compensation
- [ ] `drift.rs` — clock drift compensation over time
- [ ] `sync.rs` — `PlayheadSync` struct
  - [ ] `new(sample_rate)`
  - [ ] `set_local_position(position)`
  - [ ] `receive_playhead_update(peer_id, position, timestamp)`
  - [ ] `synchronized_position()` — master vs. latency-adjusted remote position
  - [ ] `generate_update() -> PlayheadUpdate`
  - [ ] `process_clock_sync(message)`
- [ ] `transport.rs` (playhead) — transport control sync (play/stop/record) across peers
- [ ] Real-time-safety audit: no heap allocation on `synchronized_position` / `set_local_position` hot path
- [ ] Real-time-safety audit: no locking on hot path (lock-free data access)
- [ ] Benchmark: playhead sync precision (target sub-millisecond)
- [ ] Benchmark: clock drift correction under simulated jitter

---

## Phase 4 — Presence and Awareness

### `tpt-av-sync-presence`
- [ ] Scaffold crate (`Cargo.toml`, `src/lib.rs`, `tests/`)
- [ ] `presence.rs` — `PresenceManager`
  - [ ] `new(local_user)`
  - [ ] `update_local_cursor(cursor)`
  - [ ] `receive_update(peer_id, update)`
  - [ ] `remote_users()`, `remote_cursors()`
  - [ ] `generate_update() -> PresenceUpdate`
- [ ] `presence.rs` — `UserInfo`, `PresenceState` (Online/Idle/Offline)
- [ ] `cursor.rs` — `CursorState` (playhead, selection, focused clip, timestamp)
- [ ] `avatar.rs` — `AvatarData` (avatar URL/data, color)
- [ ] `activity.rs` — idle-timeout / activity indicator logic
- [ ] Wire `PresenceUpdate` handling into `SyncEngine::process_messages`
- [ ] Test: presence state transitions (Online → Idle → Offline)
- [ ] Test: remote cursor broadcast/receive round-trip

---

## Phase 5 — WebRTC and Advanced Features

- [ ] `tpt-av-sync-net`: `webrtc.rs` — `WebRtcTransport` (peer connection + data channel) implementing `Transport`
- [ ] Scaffold `tpt-av-sync-server` crate (optional relay, feature-gated)
  - [ ] `signaling.rs` — WebRTC SDP offer/answer + ICE candidate exchange server
  - [ ] `relay.rs` — message relay fallback for NAT-blocked peers
  - [ ] `persistence.rs` — session persistence for relay/signaling server
- [ ] `discovery.rs` — flesh out mDNS-based LAN peer discovery
- [ ] `discovery.rs` — broadcast-based discovery fallback
- [ ] Offline-first sync: local pending-operation queue while disconnected
- [ ] Offline-first sync: resync/merge flow on reconnect
- [ ] `OperationBatcher` — batch operations at fixed interval (e.g. 16ms) in `tpt-av-sync-net`
- [ ] Delta compression: `compute_delta(old, new) -> Vec<TimelineOperation>`
- [ ] Delta compression: `apply_delta(session, delta)`
- [ ] Example: `collaborative_editor.rs` (two peers editing same timeline)
- [ ] Example: `playhead_sync.rs` (synchronized playback across peers)
- [ ] Example: `presence_demo.rs` (remote cursors and presence)
- [ ] Example: `offline_sync.rs` (offline editing with later sync)

---

## Phase 6 — Release Readiness

- [ ] Full `cargo doc` coverage for every public type across all crates
- [ ] Per-crate README polish
- [ ] Top-level usage guide / getting-started docs
- [ ] End-to-end demo: 2+ peers collaboratively editing the same timeline live
- [ ] Fuzz/stress testing for CRDT merge edge cases
- [ ] Version bump to 1.0.0-ready across workspace
- [ ] CHANGELOG.md
- [ ] Publish `tpt-av-sync-utils` to crates.io
- [ ] Publish `tpt-av-sync-crdt` to crates.io
- [ ] Publish `tpt-av-sync-net` to crates.io
- [ ] Publish `tpt-av-sync-playhead` to crates.io
- [ ] Publish `tpt-av-sync-presence` to crates.io
- [ ] Publish `tpt-av-sync-server` to crates.io (if stabilized)
- [ ] Final `cargo-deny` + full dependency-tree license audit
