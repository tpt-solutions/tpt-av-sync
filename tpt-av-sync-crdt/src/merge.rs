//! Conflict resolution primitives: tagged last-writer-wins registers.
//!
//! Every mutable piece of CRDT state (a clip's start frame, a track's name,
//! …) is stored in an [`LwwReg`]. A register write is tagged with an
//! [`OpTag`] — the *(Lamport timestamp, peer id)* pair of the operation that
//! produced it. Because `(lamport, peer)` is totally ordered, any two
//! concurrent writes to the same field resolve to the same winner on every
//! peer, which makes the whole state space commutative and idempotent.
//!
//! # Resolution rules used across the CRDT
//!
//! | Conflict | Resolution |
//! | :--- | :--- |
//! | Concurrent moves of one clip | LWW per geometry register (spec §5.1). |
//! | Concurrent deletes of one clip | Idempotent: the `alive` register flips to `false` once. |
//! | Delete vs. edit of the same clip | Independent registers: a delete hides the clip; a later re-insert (higher tag) resurrects it. |
//! | Concurrent splits of one clip | Split *points* merge as a set; segments are derived deterministically (spec §5.1: two splits → three clips). |
//! | Same split offset, different new ids | The smaller clip id wins (deterministic, order-independent). |

use serde::{Deserialize, Serialize};
use std::fmt;
use tpt_av_sync_utils::PeerId;

/// The total-order tag attached to every CRDT register write.
///
/// Ordering is by Lamport timestamp first, peer id second — exactly the
/// ordering of [`tpt_av_sync_utils::OperationId`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct OpTag {
    /// Lamport timestamp of the writing operation.
    pub lamport: u64,
    /// Peer that performed the writing operation.
    pub peer: PeerId,
}

impl OpTag {
    /// Creates a tag.
    #[must_use]
    pub const fn new(lamport: u64, peer: PeerId) -> Self {
        Self { lamport, peer }
    }

    /// The tag carried by a register's initial value: `(0, peer 0)`. Any
    /// real operation supersedes it.
    #[must_use]
    pub const fn initial() -> Self {
        Self {
            lamport: 0,
            peer: PeerId::from_u64(0),
        }
    }
}

impl fmt::Display for OpTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}@{})", self.lamport, self.peer)
    }
}

/// A last-writer-wins register: a value paired with the tag of the
/// operation that last set it.
///
/// A write is accepted only if its tag is *strictly greater* than the
/// current tag, so re-applying the same operation is a no-op (idempotency)
/// and application order never matters (commutativity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwReg<T> {
    value: T,
    tag: OpTag,
}

impl<T> LwwReg<T> {
    /// Creates a register with an initial value tagged [`OpTag::initial`].
    #[must_use]
    pub const fn new_initial(value: T) -> Self {
        Self {
            value,
            tag: OpTag::initial(),
        }
    }

    /// Creates a register with an explicit tag.
    #[must_use]
    pub const fn new(value: T, tag: OpTag) -> Self {
        Self { value, tag }
    }

    /// The current value.
    #[must_use]
    pub const fn get(&self) -> &T {
        &self.value
    }

    /// The tag of the last accepted write.
    #[must_use]
    pub const fn tag(&self) -> OpTag {
        self.tag
    }

    /// Attempts to write `value` at `tag`.
    ///
    /// Returns `true` if the write won (tag strictly greater than current)
    /// and the value was replaced.
    pub fn set(&mut self, value: T, tag: OpTag) -> bool {
        if tag > self.tag {
            self.value = value;
            self.tag = tag;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(l: u64, p: u64) -> OpTag {
        OpTag::new(l, PeerId::from_u64(p))
    }

    #[test]
    fn higher_lamport_wins() {
        let mut reg = LwwReg::new_initial(10_u64);
        assert!(reg.set(20, tag(5, 1)));
        assert_eq!(reg.get(), &20);
        assert!(!reg.set(30, tag(3, 1)), "stale write must lose");
        assert_eq!(reg.get(), &20);
    }

    #[test]
    fn equal_lamport_higher_peer_wins() {
        let mut reg = LwwReg::new_initial(1_u64);
        assert!(reg.set(2, tag(7, 2)));
        assert!(reg.set(3, tag(7, 5)), "higher peer id breaks the tie");
        assert_eq!(reg.get(), &3);
        assert!(!reg.set(4, tag(7, 2)), "lower peer id loses the tie");
    }

    #[test]
    fn equal_tag_write_is_rejected() {
        let mut reg = LwwReg::new(1_u64, tag(4, 9));
        assert!(!reg.set(2, tag(4, 9)), "exact duplicate write is a no-op");
        assert_eq!(reg.get(), &1);
    }

    #[test]
    fn resolution_is_commutative() {
        let mut a = LwwReg::new_initial("x".to_string());
        let mut b = a.clone();
        a.set("from-a".to_string(), tag(9, 1));
        b.set("from-b".to_string(), tag(9, 2));
        a.set("from-b".to_string(), tag(9, 2));
        b.set("from-a".to_string(), tag(9, 1));
        assert_eq!(a, b, "both orders must converge");
        assert_eq!(a.get(), "from-b");
    }

    #[test]
    fn initial_tag_is_beaten_by_any_real_tag() {
        let mut reg = LwwReg::new_initial(0_u64);
        assert!(reg.set(1, OpTag::new(1, PeerId::from_u64(1))));
    }
}
