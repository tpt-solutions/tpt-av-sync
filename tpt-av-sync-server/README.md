# tpt-av-sync-server

Optional server for [`tpt-av-sync`](https://github.com/tpt-solutions/tpt-av-sync) sessions: WebRTC signaling, a sync-message relay for peers that cannot connect directly, and durable session persistence. Nothing here sits in the CRDT's correctness path — the engine is fully peer-to-peer without this crate.

## What's inside

| Module | Contents |
| :--- | :--- |
| `signaling` | `SignalingServer` — SDP offer/answer + ICE candidate exchange over JSON WebSocket frames, routed by room (`to: Some(peer)` for direct, `None` for fan-out). |
| `relay` | `RelayServer` — binary relay for `SyncMessage`s by room; `RelayClientTransport` implements `tpt_av_sync_net::Transport` so the `SyncEngine` runs unmodified through the server. |
| `persistence` | `SessionStore` — append-only per-room operation logs (length-prefixed bincode, crash-tolerant reads); the relay uses it to persist operations and answer `RequestSnapshot` from history. |

## When to use what

- **Peers can reach each other** → no server needed (TCP mesh or WebRTC directly).
- **WebRTC handshake needs a rendezvous** → run `SignalingServer`; relay its `SignalFrame`s to the two `WebRtcTransport`s; the data path stays P2P.
- **WebRTC/TCP blocked entirely** → run `RelayServer` and connect clients with `RelayClientTransport`; all traffic flows through the server.
- **Sessions should outlive their peers** → attach a `SessionStore` to the relay: operations are persisted per room and replayed to later joiners, even if every editor went offline.

## Usage

Relay with persistence (clients need only this crate's client + the engine):

```rust
use std::net::SocketAddr;
use tpt_av_sync_server::{RelayClientTransport, RelayServer, SessionStore};
use tpt_av_sync_utils::PeerId;

# fn demo() -> Result<(), tpt_av_sync_utils::SyncError> {
let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
let store = SessionStore::open("/var/lib/tpt-av-sync/sessions")?;
let (_server, addr) = RelayServer::serve(addr, Some(store))?;

let transport = RelayClientTransport::connect(addr, "episode-12", PeerId::generate())?;
// hand `transport` to tpt_av_sync_net::SyncEngine as usual
# Ok(())
# }
```

Signaling server: peers join a room with `SignalPayload::Join` and exchange `Offer`/`Answer`/`IceCandidate` JSON frames; the server routes them unchanged. See `tests/relay_and_signaling.rs` for a complete round trip.

## Operational notes

- Both servers are plain `ws://` WebSocket listeners; terminate TLS in front (nginx/caddy) for WAN use.
- `SessionStore` appends per operation and tolerates truncated tails (a crash mid-write loses at most that frame).
- The relay answers snapshot requests from persisted history as peer `0`; a lone joiner can bootstrap from the server alone.

## Feature flags

None.

## Minimum supported Rust

1.75 (workspace MSRV).

## License

Dual-licensed under [MIT](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-MIT) OR [Apache-2.0](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-APACHE).
