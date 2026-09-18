//! Optional relay and signaling server for `tpt-av-sync` sessions.
//!
//! Three cooperating pieces:
//!
//! - [`signaling`] — WebRTC SDP/ICE exchange for peers that cannot reach
//!   each other directly. Frames are JSON (easy to bridge from browsers).
//! - [`relay`] — a message relay for sync traffic (`SyncMessage`s routed
//!   by room), plus a client-side [`RelayTransport`] implementing
//!   [`Transport`](tpt_av_sync_net::Transport) so the engine can run
//!   entirely through the server when peer-to-peer fails.
//! - [`persistence`] — optional on-disk session history: operations are
//!   appended per room and replayed to joining peers.

#![deny(missing_docs)]

pub mod limits;
pub mod persistence;
pub mod relay;
pub mod signaling;

pub use limits::{ConnectionGuard, ServerLimits, TokenBucket};
pub use persistence::{SessionStore, StoreLimits};
pub use relay::{RelayClientTransport, RelayFrame, RelayJoinProof, RelayServer, RoomAuth};
pub use signaling::{SignalFrame, SignalPayload, SignalingServer};
