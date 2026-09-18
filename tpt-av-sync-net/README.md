# tpt-av-sync-net

Network transport and replication engine for [`tpt-av-sync`](https://github.com/tpt-solutions/tpt-av-sync): carry CRDT timeline operations between peers over TCP, WebSocket, or WebRTC data channels, with reliability, batching, LAN discovery, and an offline-first message queue.

## What's inside

| Module | Contents |
| :--- | :--- |
| `transport` | The `Transport` trait plus `LoopbackTransport` (in-memory, for tests/demos). |
| `tcp` | `TcpTransport` — length-prefixed bincode frames, Hello handshake, one reader thread per connection. Great for LAN use. |
| `websocket` | `WebsocketTransport` (feature `websocket`, default) — server-based collaboration; internal tokio runtime, synchronous engine-facing API. |
| `webrtc` | `WebRtcTransport` (feature `webrtc`) — direct peer-to-peer SCTP data channels; you relay `SignalEnvelope`s (SDP/ICE) via any channel (e.g. `tpt-av-sync-server`). |
| `message` | `SyncMessage` (operations, snapshots, playhead, transport control, presence, clock sync, acks), `WireFrame`, protocol version. |
| `engine` | `SyncEngine` + `SyncEvent` — apply local edits, poll inbound traffic, automatic snapshots on peer join, offline queue, event pass-through. |
| `reliability` | `ReliabilityManager` — ack tracking with bounded resend (duplicate-safe: the CRDT is idempotent). |
| `batcher` | `OperationBatcher` — coalesce operations on a fixed interval (16 ms default). |
| `offline` | `OfflineQueue` — bounded FIFO captured while disconnected, flushed on join. |
| `discovery` | UDP multicast + broadcast LAN beacons with TTL expiry (`SO_REUSEADDR`-safe). |
| `peer` | `PeerRegistry` — join/leave/last-seen bookkeeping. |

## Choosing a transport

| Transport | Topology | Needs a server | Use when |
| :--- | :--- | :--- | :--- |
| `LoopbackTransport` | N-way in-memory | no | tests, demos, benchmarks |
| `TcpTransport` | mesh (dial each peer) | no | trusted LAN, simplest real setup |
| `WebsocketTransport` | star via server | yes | studio/WAN deployments |
| `WebRtcTransport` | P2P data channels | signaling only | low-latency WAN, NAT traversal |
| `RelayClientTransport` (server crate) | star via relay | yes | WebRTC blocked; also persists history |

## Usage

Two peers over TCP:

```rust
use std::net::SocketAddr;
use tpt_av_sync_crdt::{TimelineCrdt, TimelineOperation, ClipData, ClipId, TrackData, TrackId};
use tpt_av_sync_net::{SyncEngine, TcpTransport};
use tpt_av_sync_utils::PeerId;

let addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
let (t1, listen) = TcpTransport::listen(addr, PeerId::generate())?;
let t2 = TcpTransport::listen(addr, PeerId::generate())?.0;
t2.connect(listen)?; // dials peer 1

let mut alice = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(1)), Box::new(t1));
let mut bob = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(t2));

let track = TrackId::from_u64(1);
alice.crdt_mut().apply_local(TimelineOperation::InsertTrack {
    track_id: track, track: TrackData::new("A1"), position: 0,
});
let clip = ClipId::from_u64(2);
alice.apply_local(TimelineOperation::InsertClip {
    clip_id: clip, track_id: track,
    clip: ClipData::new("take.wav", 0, 48_000), position: 0,
});

// Application loop:
loop {
    alice.process_messages();
    bob.process_messages();
    for event in bob.take_events() {
        // SyncEvent::RemoteOperation(..), PeerJoined(..), Playhead(..), ...
#       let _ = event;
    }
#   break;
}
```

Offline-first: when `broadcast` fails (no peers), the engine queues automatically and flushes on `handle_peer_join` / the next connection — no data loss while disconnected.

WebRTC: create the transport, exchange `SignalEnvelope`s through any relay (see `tpt-av-sync-server`'s signaling server), then use it like any other transport:

```rust
# #[cfg(feature = "webrtc")]
# fn demo() -> Result<(), tpt_av_sync_utils::SyncError> {
use tpt_av_sync_net::{WebRtcTransport, SyncMessage};
use tpt_av_sync_utils::{PeerId, SyncError};

let t = WebRtcTransport::new(PeerId::generate())?;
t.dial(remote_peer)?;
// pump: while let Some(env) = t.take_outgoing_signal() { /* relay to peer */ }
//       t.handle_signal(env_received_from_peer)?;
// t.send_to(remote_peer, SyncMessage::RequestSnapshot)?;
# Ok(())
# }
```

## Platform notes

- Accepted TCP sockets are forced into blocking mode (they inherit the listener's non-blocking flag on Windows).
- Discovery binds with `SO_REUSEADDR` so several peers can share the port on one host (required on Windows).
- WebRTC disables mDNS candidate names and (in tests/demos) can advertise `127.0.0.1` via `WebRtcTransport::loopback_only`.

## Feature flags

| Feature | Default | Pulls in |
| :--- | :--- | :--- |
| `websocket` | ✓ | `tokio`, `tokio-tungstenite`, `futures-util` |
| `webrtc` | — | `webrtc`, `bytes` (+ tokio stack) |

`cargo build --no-default-features` gives you TCP + loopback only (no async runtime).

## Minimum supported Rust

1.75 (workspace MSRV).

## License

Dual-licensed under [MIT](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-MIT) OR [Apache-2.0](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-APACHE).
