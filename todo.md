# tpt-av-sync — Project Todo

CRDT-based real-time collaboration engine for media timelines. Dual-licensed MIT OR Apache-2.0. TPT Solutions Open Source.

---

## Phase 0 — Repo & Tooling Bootstrap

- [ ] Create GitHub repo `github.com/tpt-solutions/tpt-av-sync`
- [x] `git init` locally, initial commit
- [x] Add `LICENSE-MIT`
- [x] Add `LICENSE-APACHE`
- [x] Root `README.md` (vision, ecosystem table, dual-license badges)
- [x] Root `Cargo.toml` workspace manifest
  - [x] `resolver = "2"`, `members` list for all sub-crates
  - [x] `[workspace.package]` with `license = "MIT OR Apache-2.0"`, `edition`, `rust-version`, `repository`
  - [x] `[workspace.dependencies]` (tokio, tokio-tungstenite, serde, bincode, log, webrtc)
- [x] `deny.toml` (cargo-deny)
  - [x] Allow: MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Zlib
  - [x] Deny: GPL-2.0, GPL-3.0, LGPL-2.1, LGPL-3.0, AGPL-3.0, MPL-2.0
- [x] GitHub Actions CI workflow
  - [x] `cargo build --workspace`
  - [x] `cargo test --workspace`
  - [x] `cargo clippy --workspace -- -D warnings`
  - [x] `cargo deny check`
- [x] `DESIGN.md` (carry over spec.txt as living design doc)
- [x] `.gitignore` for Rust (`/target`, etc.)

---

## Phase 1 — Foundation & CRDT Core

### `tpt-av-sync-utils`
- [x] Scaffold crate (`Cargo.toml`, `src/lib.rs`)
- [x] `peer_id.rs` — `PeerId` type
- [x] `operation_id.rs` — `OperationId` (Lamport timestamp + peer id)
- [x] `clock.rs` — Lamport clock
- [x] `clock.rs` — `VectorClock` (`increment`, `merge`, `happens_before`, `is_concurrent`)
- [x] `time.rs` — network time sync types
- [x] `error.rs` — `SyncError` enum
- [x] Unit tests: Lamport clock ordering
- [x] Unit tests: vector clock merge / happens-before / concurrency detection

### `tpt-av-sync-crdt`
- [x] Scaffold crate (`Cargo.toml`, `src/lib.rs`, `tests/`)
- [x] `operation.rs` — `TimelineOperation` enum
  - [x] `InsertClip`, `MoveClip`, `DeleteClip`, `SplitClip`, `TrimClip`
  - [x] `UpdateClipMetadata`, `InsertTrack`, `DeleteTrack`
  - [x] `UpdateEnvelope`, `UpdateSessionMetadata`
  - [x] `TaggedOperation` struct (op_id, operation, lamport_ts, vector_clock, peer_id, timestamp)
- [x] `state.rs` — `Session` state model (tracks, clips, envelopes, session metadata)
- [x] `clip_crdt.rs` — per-clip CRDT structure
- [x] `track_crdt.rs` — per-track CRDT structure
- [x] `envelope_crdt.rs` — automation envelope CRDT structure
- [x] `merge.rs` — conflict resolution logic
  - [x] Last-writer-wins via Lamport timestamp (concurrent moves)
  - [x] Idempotent delete handling (concurrent deletes)
  - [x] Split/merge semantics (concurrent splits → 3-clip result)
- [x] `history.rs` — operation history log, undo/redo
- [x] `timeline_crdt.rs` — `TimelineCrdt` struct
  - [x] `new(local_peer_id)`
  - [x] `apply_local(operation) -> TaggedOperation`
  - [x] `apply_remote(tagged_op) -> Result<(), SyncError>`
  - [x] `session()`, `operation_log()`
  - [x] `snapshot() -> TimelineSnapshot`
  - [x] `from_snapshot(snapshot, local_peer_id) -> Self`
- [x] Property-based tests (`proptest`) — commutativity for each operation type
- [x] Property-based tests (`proptest`) — idempotency for each operation type
- [x] Scenario test: concurrent clip moves (spec §5.1)
- [x] Scenario test: concurrent clip deletes (spec §5.1)
- [x] Scenario test: concurrent clip splits (spec §5.1)
- [x] `cargo-deny` CI gate green for this crate's dependency tree

---

## Phase 2 — Network Transport

### `tpt-av-sync-net`
- [x] Scaffold crate (`Cargo.toml`, `src/lib.rs`, `tests/`)
- [x] `message.rs` — `SyncMessage` enum (Operation, RequestSnapshot, Snapshot, PlayheadUpdate, TransportControl, PresenceUpdate, ClockSync, Ack)
- [x] Message (de)serialization via `serde` + `bincode`
- [x] `transport.rs` — `Transport` trait (`send_to`, `broadcast`, `recv`, `try_recv`, `peers`)
- [x] `peer.rs` — peer management/state tracking
- [x] `tcp.rs` — raw TCP transport implementation (LAN testing)
- [x] `websocket.rs` — WebSocket transport (`tokio-tungstenite`, server-based)
- [x] `reliability.rs` — message ordering, ack tracking, retry
- [x] `discovery.rs` — peer discovery stub (interface only, impl deferred to Phase 5)
- [x] `SyncEngine`
  - [x] `new(crdt, transport)`
  - [x] `apply_local(operation)` — apply + broadcast + track pending
  - [x] `process_messages()` — dispatch Operation/Ack/Snapshot/PlayheadUpdate/PresenceUpdate
  - [x] `handle_peer_join(peer_id)` — send snapshot to new peer
  - [x] `handle_peer_leave(peer_id)`
- [x] Integration test: 2 peers exchange operations over TCP
- [x] Integration test: 2 peers exchange operations over WebSocket
- [x] Integration test: snapshot request/response on peer join
- [x] Integration test: reconnect after disconnect (offline-first groundwork)

---

## Phase 3 — Playhead Synchronization

### `tpt-av-sync-playhead`
- [x] Scaffold crate (`Cargo.toml`, `src/lib.rs`, `tests/`)
- [x] `clock.rs` — NTP-like clock sync (`ClockSyncMessage`, T1–T4 offset/latency calculation)
- [x] `latency.rs` — round-trip latency estimation and compensation
- [x] `drift.rs` — clock drift compensation over time
- [x] `sync.rs` — `PlayheadSync` struct
  - [x] `new(sample_rate)`
  - [x] `set_local_position(position)`
  - [x] `receive_playhead_update(peer_id, position, timestamp)`
  - [x] `synchronized_position()` — master vs. latency-adjusted remote position
  - [x] `generate_update() -> PlayheadUpdate`
  - [x] `process_clock_sync(message)`
- [x] `transport.rs` (playhead) — transport control sync (play/stop/record) across peers
- [ ] Real-time-safety audit: no heap allocation on `synchronized_position` / `set_local_position` hot path
- [ ] Real-time-safety audit: no locking on hot path (lock-free data access)
- [x] Benchmark: playhead sync precision (target sub-millisecond)
- [x] Benchmark: clock drift correction under simulated jitter

---

## Phase 4 — Presence and Awareness

### `tpt-av-sync-presence`
- [x] Scaffold crate (`Cargo.toml`, `src/lib.rs`, `tests/`)
- [x] `presence.rs` — `PresenceManager`
  - [x] `new(local_user)`
  - [x] `update_local_cursor(cursor)`
  - [x] `receive_update(peer_id, update)`
  - [x] `remote_users()`, `remote_cursors()`
  - [x] `generate_update() -> PresenceUpdate`
- [x] `presence.rs` — `UserInfo`, `PresenceState` (Online/Idle/Offline)
- [x] `cursor.rs` — `CursorState` (playhead, selection, focused clip, timestamp)
- [x] `avatar.rs` — `AvatarData` (avatar URL/data, color)
- [x] `activity.rs` — idle-timeout / activity indicator logic
- [x] Wire `PresenceUpdate` handling into `SyncEngine::process_messages`
- [x] Test: presence state transitions (Online → Idle → Offline)
- [x] Test: remote cursor broadcast/receive round-trip

---

## Phase 5 — WebRTC and Advanced Features

- [x] `tpt-av-sync-net`: `webrtc.rs` — `WebRtcTransport` (peer connection + data channel) implementing `Transport`
- [x] Scaffold `tpt-av-sync-server` crate (optional relay, feature-gated)
  - [x] `signaling.rs` — WebRTC SDP offer/answer + ICE candidate exchange server
  - [x] `relay.rs` — message relay fallback for NAT-blocked peers
  - [x] `persistence.rs` — session persistence for relay/signaling server
- [x] `discovery.rs` — LAN peer discovery via UDP beacon (`MulticastDiscovery`); deliberate alternative to full mDNS/DNS-SD — see `DESIGN.md` §"Deviations from spec.txt"
- [x] `discovery.rs` — broadcast-based discovery fallback (`BroadcastDiscovery`)
- [x] Offline-first sync: local pending-operation queue while disconnected
- [x] Offline-first sync: resync/merge flow on reconnect
- [x] `OperationBatcher` — batch operations at fixed interval (e.g. 16ms) in `tpt-av-sync-net`
- [x] Delta compression: `compute_delta(old, new) -> Vec<TimelineOperation>`
- [x] Delta compression: `apply_delta(session, delta)`
- [x] Example: `collaborative_editor.rs` (two peers editing same timeline)
- [x] Example: `playhead_sync.rs` (synchronized playback across peers)
- [x] Example: `presence_demo.rs` (remote cursors and presence)
- [x] Example: `offline_sync.rs` (offline editing with later sync)

---

## Phase 6 — Release Readiness

- [x] Full `cargo doc` coverage for every public type across all crates (`#![warn(missing_docs)]` in every crate's `lib.rs`, clean)
- [x] Per-crate README polish
- [x] Top-level usage guide / getting-started docs (`docs/USAGE.md`)
- [ ] End-to-end demo: 2+ peers collaboratively editing the same timeline live
- [x] Fuzz/stress testing for CRDT merge edge cases
- [ ] Version bump to 1.0.0-ready across workspace
- [ ] CHANGELOG.md
- [ ] Publish `tpt-av-sync-utils` to crates.io
- [ ] Publish `tpt-av-sync-crdt` to crates.io
- [ ] Publish `tpt-av-sync-net` to crates.io
- [ ] Publish `tpt-av-sync-playhead` to crates.io
- [ ] Publish `tpt-av-sync-presence` to crates.io
- [ ] Publish `tpt-av-sync-server` to crates.io (if stabilized)
- [ ] Final `cargo-deny` + full dependency-tree license audit

---

## Phase 7 — Security Hardening

Findings from a security recon pass: no transport encryption on TCP/WebSocket (plaintext bincode), no peer authentication anywhere (`PeerId` is self-asserted in every handshake and in the relay/signaling server's per-frame `from` field), no rate limiting/connection caps, unbounded per-room op-log growth, bincode deserializing untrusted bytes with no size guard beyond the flat 64 MiB frame cap, and WebRTC signaling leans on a public Google STUN server (leaks participant IPs). Land in this order — each step is independently testable:

- [x] **B1 — Bounded deserialization + field validation** (no protocol changes)
  - [x] `tpt_av_sync_utils::security::bounded_decode<T>` (byte-limit check before `wire::decode`)
  - [x] Replace direct `bincode::deserialize` calls in `tcp.rs`, `websocket.rs`, `webrtc.rs`, `relay.rs`, `signaling.rs`, `persistence.rs`
  - [x] `TimelineOperation::validate()` in `operation.rs` (max string lengths, max envelope-point count)
  - [x] Call `validate()` from `SyncEngine`'s inbound path (`engine.rs`) and from `relay.rs` before forwarding/persisting
  - [x] Reject-path tests (oversized field, oversized frame)
- [x] **B2 — Bounded persistence** (`persistence.rs`)
  - [x] `SessionStore::open_with_limits(dir, max_bytes_per_room, max_ops_per_room)`
  - [x] v1: size-triggered truncate/rotate
- [x] **B3 — Relay/signaling connection caps, rate limiting, Origin check**
  - [x] Global + per-IP connection caps; per-room member caps (`limits.rs`)
  - [x] Hand-rolled per-connection token-bucket rate limit
  - [x] `Origin` header allowlist check (no-op for LAN mode)
- [x] **B4 — Transport encryption (TLS/wss)**
  - [x] `TlsIdentityConfig`/`TlsTrust` types in `tpt_av_sync_utils::security`; `tls.rs` in `tpt-av-sync-net`
  - [x] `tcp.rs`: TLS-wrapped `TcpStream` via `rustls`
  - [x] `websocket.rs`: `tokio-rustls` wrap + `tokio_tungstenite` for `wss://`
  - [x] Secure-by-default wrappers over existing `listen`/`connect`/`serve`, self-signed cert via `rcgen` + TOFU fingerprint pinning
  - [x] Explicit loudly-named plaintext opt-outs with warning logs
  - [x] `TlsIdentityConfig::Pem` path for real CA certs (public deployment)
  - [x] Add workspace deps: `tokio-rustls`, `rustls-pemfile`, `rcgen`; pin `rustls` to match `webrtc`'s transitive version
- [x] **B5 — Peer authentication (keypair identity)**
  - [x] `PeerIdentity` (Ed25519) in `tpt_av_sync_utils::identity`; `PeerId` derived from public key
  - [x] Bump `PROTOCOL_VERSION` to 2 and enforce it
  - [x] Challenge/response handshake (nonce + signature) in `message.rs`/`tcp.rs`/`websocket.rs`
  - [x] Add workspace dep: `ed25519-dalek`
- [x] **B6 — Room authorization (shared token, bound peer identity)**
  - [x] Room token verified via keyed hash (`room_token_proof`, not a raw passphrase over the wire)
  - [x] Join/token-proof + verifying-key first-frame requirement in `relay.rs`/`signaling.rs`
  - [x] Bind peer_id to connection server-side; stop trusting client-supplied `from` on subsequent frames
  - [x] `RoomAuth` open opt-out for LAN/trusted use
- [x] Re-run `cargo deny check` after new deps land (advisories `RUSTSEC-2025-0141`, `RUSTSEC-2025-0134` — unmaintained, no safe upgrade — explicitly acknowledged in `deny.toml`)

---

## Phase 8 — Innovative Features

- [ ] Session recording & replay: record full tagged-op log, replay at controllable speed/to a target timestamp via `TimelineCrdt::apply_remote` (build on `history.rs`/`persistence.rs`/`TimelineSnapshot`)
- [ ] Conflict/merge visualizer: emit a structured "resolution event" from `merge.rs` (which op won a concurrent move/delete/split and why); surface it in `examples/collaborative_editor.rs` or a new example
- [ ] Live session inspector/dashboard (TUI via `ratatui`): connected peers, presence state, playhead positions, op throughput — hook into `SyncEngine::process_messages`

---

## Phase 9 — Adoption Tooling

- [ ] `cargo-generate` project template wiring `tpt-av-sync-{utils,crdt,net,playhead,presence}` with a minimal working `SyncEngine` setup
- [ ] New `tpt-av-sync-cli` binary crate: inspect/replay/dump a `SessionStore` op-log; run a local relay server for testing (reuse `RelayServer` directly)
- [ ] `Dockerfile` + `docker-compose.yml` for `tpt-av-sync-server` (relay/signaling ports + persistent volume for op-logs)
