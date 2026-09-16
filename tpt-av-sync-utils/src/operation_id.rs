//! Unique operation identifiers.

use crate::peer_id::PeerId;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for a replicated timeline operation.
///
/// An operation id is the pair *(Lamport timestamp, peer id)* of the peer
/// that created the operation. Because Lamport counters are strictly
/// increasing per peer and peer ids are unique, the pair is globally unique
/// — which makes it a perfect key for idempotent operation application:
/// applying an operation whose `OperationId` has already been seen is a
/// no-op.
///
/// The pair is also totally ordered by `(lamport, peer)`, giving a global,
/// deterministic total order over all operations. The CRDT uses this order
/// as the last-writer-wins ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OperationId {
    /// Lamport timestamp of the operation.
    pub lamport: u64,
    /// Peer that created the operation.
    pub peer: PeerId,
}

impl OperationId {
    /// Creates an operation id from a Lamport timestamp and peer id.
    #[must_use]
    pub const fn new(lamport: u64, peer: PeerId) -> Self {
        Self { lamport, peer }
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "op({}@{})", self.lamport, self.peer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_lamport_then_peer() {
        let a = PeerId::from_u64(1);
        let b = PeerId::from_u64(2);
        let id1 = OperationId::new(5, a);
        let id2 = OperationId::new(5, b);
        let id3 = OperationId::new(6, a);
        assert!(id1 < id2, "same lamport: lower peer id sorts first");
        assert!(id2 < id3, "higher lamport sorts after");
    }

    #[test]
    fn ids_from_different_peers_never_collide() {
        let a = OperationId::new(10, PeerId::from_u64(1));
        let b = OperationId::new(10, PeerId::from_u64(2));
        assert_ne!(a, b);
    }

    #[test]
    fn display_format() {
        let id = OperationId::new(7, PeerId::from_u64(0xAB));
        assert_eq!(id.to_string(), "op(7@peer-00000000000000ab)");
    }
}
