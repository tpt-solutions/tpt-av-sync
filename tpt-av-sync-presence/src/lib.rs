//! User presence and awareness for the `tpt-av-sync` engine.
//!
//! This crate models the "who else is here and what are they looking at"
//! layer of a collaborative session:
//!
//! - [`PresenceManager`] — local user state, remote users, remote cursors.
//! - [`UserInfo`], [`PresenceState`] — identity and online/idle/offline.
//! - [`CursorState`] — playhead, selection, and focused clip of a user.
//! - [`AvatarData`] — avatar payloads and UI colors.
//! - [`activity`] — idle-timeout transitions (Online → Idle → Offline).
//! - [`PresenceUpdate`] — the wire message exchanged via
//!   `tpt-av-sync-net`'s `SyncMessage::PresenceUpdate`.
//!
//! Like the rest of the engine there is no I/O here and no hidden clocks:
//! time enters as `u64` milliseconds, making behavior deterministic and
//! testable.

#![deny(missing_docs)]

pub mod activity;
pub mod avatar;
pub mod cursor;
pub mod presence;

pub use activity::{ActivityTracker, IdleConfig};
pub use avatar::{AvatarData, AvatarKind, Color};
pub use cursor::CursorState;
pub use presence::{PresenceManager, PresenceState, PresenceUpdate, RemoteUser, UserInfo};
