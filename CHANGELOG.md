# Changelog

All notable changes to the `tpt-av-sync` workspace are documented here. Per-crate details live in each crate's `CHANGELOG.md`; this file summarizes the workspace.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added — Phase 0 (bootstrap)
- Cargo workspace with six crates + examples, dual MIT OR Apache-2.0 licensing (`LICENSE-MIT`, `LICENSE-APACHE`), `deny.toml` license audit (GPL/AGPL/MPL denied, enforced in CI), GitHub Actions CI (build/test/clippy on Linux + Windows, all-features job, `cargo-deny`).

### Added — Phase 1 (foundation & CRDT core)
- `tpt-av-sync-utils`: `PeerId`, `OperationId`, `LamportClock`, `VectorClock` (merge / happens-before / concurrency), `MonotonicClock`, `NetworkTime`, `SyncError`.
- `tpt-av-sync-crdt`: `TimelineOperation` (11 variants), `TaggedOperation`, `LwwReg`/`OpTag` conflict resolution, per-clip/track/envelope CRDTs, `Session` with materialized `SessionView`, `TimelineCrdt` (local/remote apply with dependency buffering), replay-based `TimelineSnapshot`, undo/redo (`compute_inverse`), delta compression (`compute_delta`/`apply_delta`).
- Correctness proof: proptest suites for commutativity, idempotency, snapshot fidelity; spec §5.1 scenario tests (concurrent moves/deletes/splits); stress tests (5 replicas × permutations, triple delivery, snapshot chains).

### Added — Phase 2 (network transport)
- `tpt-av-sync-net`: `Transport` trait; `LoopbackTransport`, `TcpTransport` (framed, handshaked), `WebsocketTransport` (default feature); `SyncMessage`/`WireFrame` wire format; `SyncEngine` with events, ack/resend reliability, offline queue, snapshot-on-join; `OperationBatcher`; UDP multicast/broadcast discovery; `PeerRegistry`.
- Integration tests over real sockets: TCP two-peer exchange, three-peer mesh convergence, snapshot bootstrap, reconnect flush, WebSocket exchange, multicast discovery.

### Added — Phase 3 (playhead sync)
- `tpt-av-sync-playhead`: NTP-style T1–T4 `ClockSyncMessage` + `ClockSynchronizer`, EWMA `LatencyEstimator`, ppm-scale `DriftCompensator` with NTP-step rejection, `PlayheadSync` (allocation-free, lock-free hot path; injectable clock), `TransportSync` play/stop/record/locate replication.
- Deterministic precision tests (virtual clocks, jitter simulation): sub-ms offset accuracy, <1 ms tracking at 48 kHz, 40 ppm drift convergence; Criterion benches for the hot path.
- Real-time-safety audit: `tests/realtime_safety.rs` verifies zero heap allocation across every branch of `set_local_position`/`synchronized_position` with a counting `GlobalAlloc`, rather than relying on the doc comment's claim alone. Lock-freedom is structural (no `Mutex`/`RwLock` on `PlayheadSync`).

### Added — Phase 4 (presence)
- `tpt-av-sync-presence`: `PresenceManager`, `UserInfo`, `PresenceState` (Online/Idle/Offline), `CursorState`, `AvatarData`/`Color`, `ActivityTracker`/`IdleConfig`; two-peer wire round-trip tests.

### Added — Phase 5 (WebRTC, server, advanced)
- `WebRtcTransport` (feature `webrtc`): SCTP data-channel transport with signaling envelopes (`SignalEnvelope`), pre-description ICE buffering, queue-until-open sends, loopback-only constructor.
- `tpt-av-sync-server`: `SignalingServer` (JSON WebSocket room routing), `RelayServer` + `RelayClientTransport` (sync-message relay usable directly by the engine), `SessionStore` (append-only per-room operation logs; relay answers snapshots from history, enabling lone-joiner bootstrap).
- Examples: `collaborative_editor`, `playhead_sync`, `presence_demo`, `offline_sync`.

### Added — Phase 6 (release readiness)
- `DESIGN.md` living design doc (including documented deviations from the original spec), this changelog set, per-crate READMEs, `docs/USAGE.md` getting-started guide.
- `examples/end_to_end_demo`: three peers over a real TCP mesh (not the in-process loopback transport the other examples use) — join-in-progress via snapshot, three-way concurrent CRDT edits, live playhead tracking, and presence, all through one stack; asserts full convergence.
- Full `cargo doc` coverage (`#![warn(missing_docs)]` in every crate) and a clean `cargo-deny check` (two unmaintained-but-no-safe-upgrade advisories, `bincode`'s and `rustls-pemfile`'s, explicitly acknowledged in `deny.toml`).

### Added — Phase 7 (security hardening)
- `tpt_av_sync_utils::security`: bounded deserialization (`bounded_decode`/`decode_message`) ahead of every wire decode; field-length/point-count validation (`validate_string(s)`) enforced by `TimelineOperation::validate()` on the engine's inbound path and by the relay before forwarding/persisting.
- `tpt_av_sync_server::persistence`: `SessionStore::open_with_limits` — size- and op-count-triggered truncation per room, bounding unbounded op-log growth.
- `tpt_av_sync_server::limits`: `ConnectionGuard` (global + per-IP + per-room caps), `TokenBucket` per-connection rate limiting, `Origin` header allowlist (LAN mode when empty).
- Transport encryption: `TlsIdentityConfig`/`TlsTrust` (self-signed + TOFU fingerprint pinning, or loaded PEM for public deployment); `tcp.rs`/`websocket.rs` gain TLS-wrapped listen/connect alongside explicit, loudly-named plaintext opt-outs.
- Peer authentication: `tpt_av_sync_utils::identity::PeerIdentity` (Ed25519; `PeerId` derived from the public key), hello + liveness challenge/response handshake, `PROTOCOL_VERSION` bumped to 2 and enforced.
- Room authorization: keyed-hash `room_token_proof` (the passphrase itself never crosses the wire), a `Join` first frame binding peer identity server-side in `relay.rs`/`signaling.rs` — client-supplied `from` is no longer trusted on later frames — with an explicit `RoomAuth::Open` opt-out for LAN/trusted use.

### Added — Path to 1.0
- `tpt_av_sync_crdt::compaction::compact` / `TimelineCrdt::compact()`: drops operations no longer needed to reconstruct current session state (found via exact `OperationId` lookup against each field's current LWW tag), bounding operation-log growth for long-running sessions. Splits are deliberately exempt (see the module doc). Covered by a 256-case property test plus targeted tombstone/split tests.

### Added — Phase 8 (innovative features)
- `tpt_av_sync_crdt::replay`: `SessionRecording` replays a recorded operation log through `TimelineCrdt::apply_remote`, instantly, at a scaled real-time pace, or only up to a target timestamp.
- `tpt_av_sync_crdt::merge`: `ResolutionEvent` + `TimelineCrdt::take_resolution_events()` — a conflict/merge visualizer reporting which write won a *genuinely concurrent* (vector-clock checked, not just "different peer") move/delete/split, and why. `examples/merge_visualizer` demonstrates all three conflict kinds side by side.

### Added — Phase 9 (adoption tooling)
- `tpt-av-sync-cli` binary crate: `inspect` (list rooms / dump a room's op log from a `SessionStore`), `replay` (replay a room via `SessionRecording`, instant or `<N>x` realtime), `relay`/`signaling` (run either server locally for testing), `dashboard` (a live `ratatui` TUI — connected peers, presence, playhead positions, operation throughput — driven by `SyncEngine::process_messages`/`take_events`).
- `Dockerfile` (multi-stage) + `docker-compose.yml` running a persisted relay and a signaling server together.

## [0.1.0] — initial development release

First public snapshot of all six crates. Pre-1.0: wire formats may still change; see the [1.0 criteria in DESIGN.md](DESIGN.md#11-path-to-10).

[Unreleased]: https://github.com/tpt-solutions/tpt-av-sync/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tpt-solutions/tpt-av-sync/releases/tag/v0.1.0
