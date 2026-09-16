//! Unique peer identifiers.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for a peer participating in a collaborative session.
///
/// A `PeerId` is a 64-bit value. [`PeerId::generate`] derives one from the
/// wall clock, the process id, and a process-local counter, which is
/// collision-free for any realistic session lifetime. Applications that
/// require stronger guarantees (e.g. sessions spanning untrusted networks)
/// may construct a `PeerId` from their own randomness via [`PeerId::from_u64`].
///
/// `PeerId` is totally ordered; the ordering is used as a deterministic
/// tie-breaker in last-writer-wins conflict resolution and for master-clock
/// election.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerId(u64);

static PEER_COUNTER: AtomicU64 = AtomicU64::new(0);

/// SplitMix64 finalizer — cheap, well-distributed bit mixer.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// Generates a raw, well-mixed 64-bit identifier from the wall clock,
/// process id, and a process-local counter.
///
/// Shared by `PeerId::generate` and the clip/track id generators in the
/// CRDT crate. Not cryptographically unique.
#[doc(hidden)]
#[must_use]
pub fn raw_generated_u64() -> u64 {
    let counter = PEER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = u64::from(std::process::id());
    mix64(nanos ^ pid.rotate_left(32) ^ (counter << 1).rotate_left(17))
}

impl PeerId {
    /// Wraps a raw 64-bit value into a `PeerId`.
    #[must_use]
    pub const fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw 64-bit value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Generates a new peer id from the wall clock, the process id, and a
    /// process-local counter.
    ///
    /// This is not cryptographically unique, but collisions are practically
    /// impossible for the lifetime of a collaboration session.
    #[must_use]
    pub fn generate() -> Self {
        let counter = PEER_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let pid = u64::from(std::process::id());
        Self(mix64(nanos ^ pid.rotate_left(32) ^ (counter << 1).rotate_left(17)))
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peer-{:016x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_unique_and_nonzero() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let id = PeerId::generate();
            assert_ne!(id, PeerId::from_u64(0));
            assert!(seen.insert(id), "duplicate peer id generated");
        }
    }

    #[test]
    fn ordering_is_total() {
        let mut ids = vec![PeerId::from_u64(30), PeerId::from_u64(10), PeerId::from_u64(20)];
        ids.sort();
        assert_eq!(
            ids,
            vec![PeerId::from_u64(10), PeerId::from_u64(20), PeerId::from_u64(30)]
        );
    }

    #[test]
    fn display_is_hex() {
        assert_eq!(PeerId::from_u64(0xAB).to_string(), "peer-00000000000000ab");
    }

    #[test]
    fn serde_roundtrip_matches_raw_u64() {
        let id = PeerId::from_u64(0xdead_beef_cafe);
        let bytes = bincode::serialize(&id).expect("serialize");
        assert_eq!(bytes, 0xdead_beef_cafe_u64.to_le_bytes());
        let back: PeerId = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(back, id);
    }
}
