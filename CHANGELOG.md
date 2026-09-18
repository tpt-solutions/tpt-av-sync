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

### Added — Phase 4 (presence)
- `tpt-av-sync-presence`: `PresenceManager`, `UserInfo`, `PresenceState` (Online/Idle/Offline), `CursorState`, `AvatarData`/`Color`, `ActivityTracker`/`IdleConfig`; two-peer wire round-trip tests.

### Added — Phase 5 (WebRTC, server, advanced)
- `WebRtcTransport` (feature `webrtc`): SCTP data-channel transport with signaling envelopes (`SignalEnvelope`), pre-description ICE buffering, queue-until-open sends, loopback-only constructor.
- `tpt-av-sync-server`: `SignalingServer` (JSON WebSocket room routing), `RelayServer` + `RelayClientTransport` (sync-message relay usable directly by the engine), `SessionStore` (append-only per-room operation logs; relay answers snapshots from history, enabling lone-joiner bootstrap).
- Examples: `collaborative_editor`, `playhead_sync`, `presence_demo`, `offline_sync`.

### Added — Phase 6 (release readiness)
- `DESIGN.md` living design doc (including documented deviations from the original spec), this changelog set, per-crate READMEs, `docs/USAGE.md` getting-started guide.

## [0.1.0] — initial development release

First public snapshot of all six crates. Pre-1.0: wire formats may still change; see the [1.0 criteria in DESIGN.md](DESIGN.md#11-path-to-10).

[Unreleased]: https://github.com/tpt-solutions/tpt-av-sync/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tpt-solutions/tpt-av-sync/releases/tag/v0.1.0
