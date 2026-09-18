//! Shared types, logical clocks, time helpers, and error handling for the
//! `tpt-av-sync` real-time collaboration engine.
//!
//! This crate is the dependency-free foundation of the [`tpt-av-sync`]
//! workspace. Every other crate builds on it:
//!
//! - [`PeerId`] — unique identifier for a collaborating peer.
//! - [`OperationId`] — unique identifier for a replicated operation
//!   (Lamport timestamp + peer id).
//! - [`LamportClock`] / [`VectorClock`] — logical clocks for causal
//!   ordering and conflict resolution.
//! - [`time`] — wall-clock and monotonic time helpers used across the
//!   network and playhead crates.
//! - [`SyncError`] — the error type shared by all crates.
//!
//! # Timestamp convention
//!
//! Wall-clock timestamps crossing the wire are unsigned 64-bit integers of
//! **milliseconds since the Unix epoch**. `std::time::SystemTime` is used
//! only where the spec mandates it (see
//! `tpt_av_sync_crdt::TaggedOperation::timestamp`).
//!
//! [`tpt-av-sync`]: https://github.com/tpt-solutions/tpt-av-sync

#![deny(missing_docs)]

pub mod clock;
pub mod error;
pub mod identity;
pub mod operation_id;
pub mod peer_id;
pub mod security;
pub mod time;
pub mod wire;

pub use clock::{LamportClock, VectorClock};
pub use identity::{
    derive_peer_id, random_nonce, room_token_proof, IdentityError, PeerIdentity,
    PeerIdentityProof, NONCE_LEN,
};
pub use error::SyncError;
pub use operation_id::OperationId;
pub use peer_id::PeerId;
