//! Network transport and replication for the `tpt-av-sync` engine.
//!
//! This crate carries the CRDT's operations between peers:
//!
//! - [`Transport`] — the transport seam; [`LoopbackTransport`] for tests,
//!   [`TcpTransport`] for LAN, [`WebsocketTransport`] for server-based
//!   setups, and an optional WebRTC data-channel transport under the
//!   `webrtc` feature.
//! - [`SyncEngine`] — apply local edits, poll inbound traffic, receive
//!   [`SyncEvent`]s; handles snapshots on join, acks/resends, and the
//!   offline-first queue.
//! - [`reliability`] — ack tracking and bounded resend.
//! - [`batcher`] — fixed-interval operation batching.
//! - [`discovery`] — UDP multicast/broadcast LAN beacons, plus an optional
//!   standard mDNS/DNS-SD responder (`mdns_discovery`, feature `mdns`) for
//!   interop with non-`tpt-av-sync` mDNS tooling.
//!
//! ```no_run
//! use std::net::SocketAddr;
//! use tpt_av_sync_crdt::TimelineCrdt;
//! use tpt_av_sync_net::{SyncEngine, TcpTransport};
//! use tpt_av_sync_utils::PeerId;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let addr: SocketAddr = "127.0.0.1:0".parse()?;
//! let transport = TcpTransport::listen(addr, PeerId::generate())?;
//! let engine = SyncEngine::new(
//!     TimelineCrdt::new(PeerId::generate()),
//!     Box::new(transport.0),
//! );
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]

#[cfg(feature = "websocket")]
pub mod websocket;

pub mod batcher;
pub mod discovery;
pub mod engine;
pub mod message;
pub mod offline;
pub mod peer;
pub mod reliability;
pub mod tcp;
pub mod transport;

#[cfg(feature = "mdns")]
pub mod mdns_discovery;

#[cfg(feature = "webrtc")]
pub mod webrtc;

#[cfg(feature = "webrtc")]
pub use webrtc::{SignalEnvelope, WebRtcTransport};

pub use batcher::OperationBatcher;
pub use discovery::{
    BroadcastDiscovery, Discovery, MulticastDiscovery, PeerAdvertisement,
    DEFAULT_BROADCAST_ADDR, DEFAULT_MULTICAST_ADDR,
};
pub use engine::{EngineConfig, SyncEngine, SyncEvent};

#[cfg(feature = "mdns")]
pub use mdns_discovery::MdnsDiscovery;
pub use message::{SyncMessage, WireFrame, PROTOCOL_VERSION};
pub use offline::OfflineQueue;
pub use peer::{PeerInfo, PeerRegistry};
pub use reliability::ReliabilityManager;
pub use tcp::TcpTransport;
pub use transport::{LoopbackTransport, Transport};

#[cfg(feature = "websocket")]
pub use websocket::WebsocketTransport;
