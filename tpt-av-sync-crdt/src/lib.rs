//! CRDT engine for conflict-free real-time collaboration on media
//! timelines.
//!
//! This crate implements the replicated timeline state machine of the
//! `tpt-av-sync` engine:
//!
//! - [`TimelineOperation`] — the replicated edit vocabulary (insert, move,
//!   delete, split, trim, metadata, envelopes).
//! - [`TimelineCrdt`] — stamps local edits, applies remote edits
//!   idempotently and order-tolerantly, and produces/installs snapshots.
//! - [`Session`] — the state model, with conflict resolution implemented as
//!   per-field last-writer-wins registers ([`merge`]).
//! - [`history`] — undo/redo of local edits.
//! - [`delta`] — diffing two materialized states into operations.
//!
//! # Guarantees
//!
//! Any two replicas that observe the same set of operations converge to
//! identical state, regardless of arrival order or duplicates — enforced by
//! property-based tests in this crate's test suite.
//!
//! ```rust
//! use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackId};
//! use tpt_av_sync_utils::PeerId;
//!
//! let mut crdt = TimelineCrdt::new(PeerId::generate());
//! let track = TrackId::from_u64(1);
//! crdt.apply_local(TimelineOperation::InsertTrack {
//!     track_id: track,
//!     track: tpt_av_sync_crdt::TrackData::new("VO"),
//!     position: 0,
//! });
//! let clip = ClipId::from_u64(2);
//! crdt.apply_local(TimelineOperation::InsertClip {
//!     clip_id: clip,
//!     track_id: track,
//!     clip: ClipData::new("take1", 0, 48_000),
//!     position: 0,
//! });
//! assert_eq!(crdt.view().clips.len(), 1);
//! ```

#![deny(missing_docs)]

pub mod clip_crdt;
pub mod delta;
pub mod envelope_crdt;
pub mod history;
pub mod merge;
pub mod operation;
pub mod replay;
pub mod state;
pub mod timeline_crdt;
pub mod track_crdt;

pub use clip_crdt::{ClipCrdt, ClipParent};
pub use delta::{apply_delta, compute_delta};
pub use envelope_crdt::EnvelopeStore;
pub use history::{compute_inverse, History, HistoryEntry};
pub use merge::{resolve_tag_conflict, LwwReg, OpTag, ResolutionEvent};
pub use operation::{
    ClipData, ClipId, ClipMetadataUpdate, EnvelopePoint, EnvelopeType, Interpolation,
    RequiredTarget, SessionMetadataUpdate, TaggedOperation, TargetId, TimelineOperation,
    TrackData, TrackId, TrackKind, TrackMetadataUpdate, TrimEdge,
};
pub use replay::{ReplaySpeed, SessionRecording};
pub use state::{
    tag_for, ClipView, ResolvedClip, Session, SessionMetadata, SessionView, TrackView,
};
pub use timeline_crdt::{missing_targets, TimelineCrdt, TimelineSnapshot};
