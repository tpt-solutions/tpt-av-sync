# Changelog — tpt-av-sync-net

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) · SemVer.

## [Unreleased]

### Added
- `Transport` trait (`send_to` / `broadcast` / `recv` / `try_recv` / `peers`) with sorted, deterministic peer lists.
- `LoopbackTransport`: in-memory pair/N-way fabric with a shared offline-failure flag (`fail_handle`) for reconnect simulations.
- `TcpTransport`: listener + dialer, length-prefixed bincode `WireFrame`s (64 MiB cap), `Hello` protocol handshake, per-connection reader threads, clean shutdown.
- `WebsocketTransport` (feature `websocket`, default): serve/connect over tokio-tungstenite with a private runtime; binary frames; synchronous engine-facing API.
- `WebRtcTransport` (feature `webrtc`): SCTP data-channel transport; `SignalEnvelope` (Offer/Answer/IceCandidate) in/out with pre-description ICE buffering; sends queue until the channel opens; `is_ready`; `loopback_only` constructor (NAT 1-to-1 to 127.0.0.1); mDNS candidate names disabled for LAN reliability.
- `SyncMessage` (Operation, Batch, RequestSnapshot, Snapshot, PlayheadUpdate, TransportControl, PresenceUpdate, ClockSync, Ack) + `WireFrame` + `PROTOCOL_VERSION`.
- `SyncEngine`: `apply_local` with auto-broadcast, `process_messages` poll loop (bounded), `SyncEvent` stream (PeerJoined/PeerLeft, RemoteOperation, SnapshotMerged, Playhead, TransportControl, Presence, ClockSync), snapshot push on join, `request_snapshot`, offline queueing + flush-on-join, `transport_mut` for app-level sends.
- `ReliabilityManager`: ack tracking with timeout + bounded resends; duplicate acks absorbed.
- `OperationBatcher`: fixed-interval batch flushing; failed flushes retain the batch.
- `OfflineQueue`: bounded FIFO with oldest-drop.
- `MulticastDiscovery` / `BroadcastDiscovery`: UDP beacons (peer id, name, session, port), TTL expiry, `SO_REUSEADDR` binding (multiple peers per host, Windows-compatible).
- `MdnsDiscovery` (feature `mdns`): standard mDNS/DNS-SD responder (via `mdns-sd`) implementing the same `Discovery` trait, for interop with non-`tpt-av-sync` mDNS tooling.
- `PeerRegistry`: join/see/leave bookkeeping.

### Fixed
- Accepted TCP sockets inheriting the listener's non-blocking mode on Windows broke the handshake/reader (now forced to blocking; idle read timeouts are retried).
- Loopback `send_to` now delivers with the sender's identity (correct source attribution).
- `WrtcInner::on_channel_open`'s flush of writes queued while the data channel was still opening dropped the `channel.send(...)` future without awaiting it, so that data was silently never sent; now routed through the same `rt_block_on` path as every other send.

### Verified
- 19 unit tests; integration tests over real sockets: TCP two-peer exchange, snapshot bootstrap, offline-reconnect flush, three-peer mesh convergence, WebSocket exchange, multicast discovery, mDNS discovery; engine event pass-through suite.
