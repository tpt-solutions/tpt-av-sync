//! Real-time-safe playhead synchronization for the `tpt-av-sync` engine.
//!
//! This crate keeps every peer's playhead aligned to a shared master clock
//! with sub-millisecond precision. It contains no I/O: timestamps come in
//! as `u64` milliseconds, messages come in as plain data, and all hot-path
//! methods ([`PlayheadSync::set_local_position`],
//! [`PlayheadSync::synchronized_position`]) are allocation-free and
//! lock-free — safe to call from audio/render threads.
//!
//! Modules:
//!
//! - [`clock`] — NTP-style T1–T4 clock offset estimation.
//! - [`latency`] — round-trip latency estimation and compensation.
//! - [`drift`] — clock-drift (skew) compensation over time.
//! - [`sync`] — the [`PlayheadSync`] engine tying it all together.
//! - [`transport`] — shared transport-control state (play/stop/record).

#![deny(missing_docs)]

pub mod clock;
pub mod drift;
pub mod latency;
pub mod sync;
pub mod transport;

pub use clock::{ClockSyncMessage, ClockSyncType, ClockSynchronizer};
pub use drift::DriftCompensator;
pub use latency::LatencyEstimator;
pub use sync::{ClockFn, PlayheadSync, PlayheadUpdate};
pub use transport::{TransportControl, TransportState, TransportSync};
