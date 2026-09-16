//! Logical clocks: Lamport clocks and vector clocks.

use crate::peer_id::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A [Lamport clock](https://en.wikipedia.org/wiki/Lamport_timestamp) for
/// establishing a partial order over events in a distributed system.
///
/// Every local event calls [`tick`](LamportClock::tick); every incoming
/// message carrying a remote timestamp calls
/// [`observe`](LamportClock::observe). The resulting counter values give a
/// *happens-before* order: if event A causally precedes event B, then
/// `A.lamport < B.lamport`.
///
/// Lamport timestamps alone cannot distinguish causally-unrelated
/// (concurrent) events; pair them with [`VectorClock`] when concurrency
/// detection is needed. Ties between equal timestamps are broken by peer id
/// (see [`crate::OperationId`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LamportClock {
    counter: u64,
}

impl LamportClock {
    /// Creates a Lamport clock starting at zero (or a known value, e.g.
    /// restored from persistence).
    #[must_use]
    pub const fn new(start: u64) -> Self {
        Self { counter: start }
    }

    /// Allocates the next local timestamp and advances the clock.
    pub fn tick(&mut self) -> u64 {
        self.counter = self.counter.saturating_add(1);
        self.counter
    }

    /// Merges a timestamp observed from a remote peer, keeping the local
    /// clock ahead of everything seen so far.
    pub fn observe(&mut self, remote: u64) {
        if remote > self.counter {
            self.counter = remote;
        }
    }

    /// Returns the current counter value without advancing the clock.
    #[must_use]
    pub const fn get(&self) -> u64 {
        self.counter
    }
}

/// A [vector clock](https://en.wikipedia.org/wiki/Vector_clock) tracking a
/// per-peer event counter, used to detect causal ordering and concurrency
/// between operations.
///
/// - `a.happens_before(b)` — every counter in `a` is `<=` the matching
///   counter in `b` and at least one is strictly less.
/// - `a.is_concurrent(b)` — neither happens before the other; the events
///   are causally unrelated.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    /// One entry per peer that has performed a local event.
    clocks: BTreeMap<PeerId, u64>,
}

impl VectorClock {
    /// Creates an empty vector clock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the counter for `peer_id` (the peer performing an event).
    pub fn increment(&mut self, peer_id: PeerId) {
        *self.clocks.entry(peer_id).or_insert(0) += 1;
    }

    /// Sets the counter for `peer_id` to at least `value`.
    pub fn witness(&mut self, peer_id: PeerId, value: u64) {
        let slot = self.clocks.entry(peer_id).or_insert(0);
        if value > *slot {
            *slot = value;
        }
    }

    /// Merges `other` into this clock, taking the component-wise maximum.
    pub fn merge(&mut self, other: &VectorClock) {
        for (&peer, &value) in &other.clocks {
            self.witness(peer, value);
        }
    }

    /// Returns the counter value for `peer_id` (0 if absent).
    #[must_use]
    pub fn get(&self, peer_id: &PeerId) -> u64 {
        self.clocks.get(peer_id).copied().unwrap_or(0)
    }

    /// Returns true if every event counted by `self` has also been counted
    /// by `other`, i.e. `self` causally precedes `other`.
    #[must_use]
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut strictly_less = false;
        for (&peer, &value) in &self.clocks {
            let other_value = other.get(&peer);
            if value > other_value {
                return false;
            }
            if value < other_value {
                strictly_less = true;
            }
        }
        // Also counts when `other` knows peers that `self` does not know at all.
        if !strictly_less {
            for peer in other.clocks.keys() {
                if !self.clocks.contains_key(peer) {
                    strictly_less = true;
                    break;
                }
            }
        }
        strictly_less
    }

    /// Returns true if `self` and `other` are causally unrelated: neither
    /// happens before the other. Equal clocks are *not* concurrent.
    #[must_use]
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self) && self != other
    }

    /// Returns the set of tracked peers.
    #[must_use]
    pub fn peers(&self) -> impl Iterator<Item = &PeerId> {
        self.clocks.keys()
    }

    /// Number of peers tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.clocks.len()
    }

    /// True when no peers are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lamport_tick_is_monotonic() {
        let mut clock = LamportClock::new(0);
        assert_eq!(clock.tick(), 1);
        assert_eq!(clock.tick(), 2);
        assert_eq!(clock.get(), 2);
    }

    #[test]
    fn lamport_observe_jumps_ahead() {
        let mut a = LamportClock::new(5);
        a.observe(100);
        assert_eq!(a.tick(), 101);
        a.observe(40); // stale observation is ignored
        assert_eq!(a.tick(), 102);
    }

    #[test]
    fn lamport_observe_preserves_happens_before() {
        let mut a = LamportClock::new(0);
        let mut b = LamportClock::new(0);
        let t1 = a.tick(); // A's event
        b.observe(t1); // B receives A's event
        let t2 = b.tick(); // B's event causally after A's
        assert!(t2 > t1);
    }

    fn vc(entries: &[(u64, u64)]) -> VectorClock {
        let mut clock = VectorClock::new();
        for &(peer, value) in entries {
            let peer = PeerId::from_u64(peer);
            for _ in 0..value {
                clock.increment(peer);
            }
        }
        clock
    }

    #[test]
    fn vector_clock_happens_before() {
        let a = vc(&[(1, 1)]);
        let ab = vc(&[(1, 1), (2, 1)]);
        assert!(a.happens_before(&ab));
        assert!(!ab.happens_before(&a));
    }

    #[test]
    fn vector_clock_concurrent() {
        let a = vc(&[(1, 1)]);
        let b = vc(&[(2, 1)]);
        assert!(a.is_concurrent(&b));
        assert!(b.is_concurrent(&a));
        assert!(!a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }

    #[test]
    fn vector_clock_merge_takes_max() {
        let mut a = vc(&[(1, 3)]);
        let b = vc(&[(1, 2), (2, 5)]);
        a.merge(&b);
        assert_eq!(a.get(&PeerId::from_u64(1)), 3);
        assert_eq!(a.get(&PeerId::from_u64(2)), 5);
        assert!(b.happens_before(&a));
    }

    #[test]
    fn vector_clock_equal_is_not_concurrent() {
        let a = vc(&[(1, 2), (2, 1)]);
        let b = vc(&[(1, 2), (2, 1)]);
        assert_eq!(a, b);
        assert!(!a.is_concurrent(&b));
        assert!(!a.happens_before(&b));
    }

    #[test]
    fn vector_clock_peer_absent_means_zero() {
        let a = vc(&[(1, 1)]);
        let ab = vc(&[(1, 1), (2, 1)]);
        assert_eq!(a.get(&PeerId::from_u64(2)), 0);
        assert!(a.happens_before(&ab), "unknown peer counts as zero");
    }

    #[test]
    fn vector_clock_serde_roundtrip() {
        let clock = vc(&[(7, 3), (9, 1)]);
        let bytes = bincode::serialize(&clock).expect("serialize");
        let back: VectorClock = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(back, clock);
    }
}
