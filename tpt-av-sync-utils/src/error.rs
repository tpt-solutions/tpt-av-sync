//! The shared error type for the `tpt-av-sync` engine.

use crate::operation_id::OperationId;
use crate::peer_id::PeerId;
use std::fmt;

/// Errors produced across the `tpt-av-sync` crates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncError {
    /// A message could not be serialized or deserialized.
    Serialization(String),
    /// A transport-level failure (I/O, connection refused, protocol error).
    Transport(String),
    /// The addressed peer is not connected.
    PeerNotFound(PeerId),
    /// An operation with this id has already been applied (informational —
    /// duplicate delivery is expected and idempotent).
    DuplicateOperation(OperationId),
    /// The operation references an entity that has not been observed yet.
    ///
    /// The sync engine buffers such operations until their dependencies
    /// arrive; this error only surfaces when dependencies cannot arrive
    /// (e.g. the creating peer will never connect).
    UnknownTarget {
        /// Kind of the missing entity (`"clip"`, `"track"`, …).
        kind: &'static str,
        /// Raw id of the missing entity.
        id: u64,
    },
    /// The operation is malformed or violates an invariant.
    InvalidOperation(String),
    /// The connection has been closed.
    Disconnected,
    /// A blocking or awaited operation exceeded its deadline.
    Timeout,
}

impl SyncError {
    /// Convenience constructor for [`SyncError::Transport`].
    pub fn transport(msg: impl Into<String>) -> Self {
        SyncError::Transport(msg.into())
    }

    /// Convenience constructor for [`SyncError::Serialization`].
    pub fn serialization(msg: impl Into<String>) -> Self {
        SyncError::Serialization(msg.into())
    }

    /// Convenience constructor for [`SyncError::InvalidOperation`].
    pub fn invalid(msg: impl Into<String>) -> Self {
        SyncError::InvalidOperation(msg.into())
    }
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncError::Serialization(m) => write!(f, "serialization error: {m}"),
            SyncError::Transport(m) => write!(f, "transport error: {m}"),
            SyncError::PeerNotFound(p) => write!(f, "peer not connected: {p}"),
            SyncError::DuplicateOperation(op) => write!(f, "duplicate operation: {op}"),
            SyncError::UnknownTarget { kind, id } => {
                write!(f, "unknown {kind} target: {id:#x}")
            }
            SyncError::InvalidOperation(m) => write!(f, "invalid operation: {m}"),
            SyncError::Disconnected => write!(f, "disconnected"),
            SyncError::Timeout => write!(f, "operation timed out"),
        }
    }
}

impl std::error::Error for SyncError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_informative() {
        let peer = PeerId::from_u64(1);
        assert_eq!(
            SyncError::PeerNotFound(peer).to_string(),
            "peer not connected: peer-0000000000000001"
        );
        assert_eq!(
            SyncError::UnknownTarget { kind: "clip", id: 5 }.to_string(),
            "unknown clip target: 0x5"
        );
        assert_eq!(SyncError::Timeout.to_string(), "operation timed out");
        assert_eq!(
            SyncError::transport("refused").to_string(),
            "transport error: refused"
        );
    }

    #[test]
    fn is_std_error() {
        let boxed: Box<dyn std::error::Error> = Box::new(SyncError::Disconnected);
        assert_eq!(boxed.to_string(), "disconnected");
    }
}
