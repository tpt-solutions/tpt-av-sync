# Changelog — tpt-av-sync-server

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) · SemVer.

## [Unreleased]

### Added
- `SignalingServer`: room-routed WebRTC signaling over JSON WebSocket frames (`SignalFrame`/`SignalPayload`: Join, Leave, Offer, Answer, IceCandidate); direct (`to`) and fan-out (`to: None`) delivery; membership cleanup on disconnect; clean shutdown.
- `RelayServer`: binary relay for `SyncMessage`s by room (`RelayFrame`), direct + fan-out routing; persists `Operation`s when a store is attached; answers `RequestSnapshot` from persisted history as peer `0`, enabling lone-joiner bootstrap without any other peer online; `persisted_ops` introspection.
- `RelayClientTransport`: `tpt_av_sync_net::Transport` implementation tunneling through the relay (joins a room on connect, tracks peers from inbound frames, broadcast always viable via the server hop).
- `SessionStore`: append-only per-room operation logs (length-prefixed bincode frames, crash-tolerant reads that skip truncated tails), room listing.

### Fixed
- Relay snapshot replies originally echoed the requester's own peer id as sender and were dropped by the client's self-frame filter (now sent as peer `0`).
- Relay membership is now registered from any inbound frame (clients that never sent `Join` still route).

### Verified
- Persistence round-trip unit test; integration tests: two peers syncing entirely through the relay, persist-and-bootstrap (editor leaves, fresh peer recovers the session from the server alone), signaling fan-out delivery.
