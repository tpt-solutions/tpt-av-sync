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

use crate::operation::ClipId;
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

/// A structured record of how a conflict on a clip was resolved: which
/// write won a *genuinely concurrent* move, delete, or split, and why.
///
/// "Concurrent" here means causally concurrent — neither operation's vector
/// clock happened-before the other's — not merely "a different peer wrote
/// this field previously". Sequential cross-peer edits (Alice creates a
/// clip, Bob edits it after seeing that create) are the common case and are
/// *not* conflicts; only two edits issued without knowledge of each other
/// are. See [`crate::TimelineCrdt::take_resolution_events`], which does the
/// concurrency check (it has the operation log to check against) and calls
/// [`resolve_tag_conflict`] to decide the winner once concurrency is
/// established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolutionEvent {
    /// What kind of conflict this is: `"move"`, `"delete"`, or `"split"`.
    pub kind: &'static str,
    /// The clip the conflict is about.
    pub clip_id: ClipId,
    /// The tag of the operation being applied when this conflict was
    /// observed.
    pub op: OpTag,
    /// Whether `op` is the winner of the conflict (`false` means it lost
    /// to the concurrent write already present, or to the other side of a
    /// split tie).
    pub op_won: bool,
    /// One-line explanation of why, suitable for a UI or log line.
    pub reason: &'static str,
}

impl fmt::Display for ResolutionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} on clip {}: op {} {} — {}",
            self.kind,
            self.clip_id.as_u64(),
            self.op,
            if self.op_won { "won" } else { "lost" },
            self.reason
        )
    }
}

/// Decides the winner between two tagged writes already known to be
/// concurrent, and builds the [`ResolutionEvent`] describing it: the
/// higher `(lamport, peer)` tag wins, exactly as [`LwwReg::set`] decides.
#[must_use]
pub fn resolve_tag_conflict(
    kind: &'static str,
    clip_id: ClipId,
    previous: OpTag,
    incoming: OpTag,
) -> ResolutionEvent {
    ResolutionEvent {
        kind,
        clip_id,
        op: incoming,
        op_won: incoming > previous,
        reason: "higher (lamport, peer) tag wins",
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

    #[test]
    fn resolve_tag_conflict_reports_the_actual_winner() {
        let clip = ClipId::from_u64(1);
        let winner = resolve_tag_conflict("move", clip, tag(3, 1), tag(5, 2));
        assert!(winner.op_won, "higher lamport must win");
        let loser = resolve_tag_conflict("move", clip, tag(9, 1), tag(5, 2));
        assert!(!loser.op_won, "lower lamport must lose");
    }
}
