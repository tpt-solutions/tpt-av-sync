//! Peer management and state tracking.

use std::collections::HashMap;
use tpt_av_sync_utils::PeerId;

/// What is known about a connected peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerInfo {
    /// The peer's id.
    pub peer_id: PeerId,
    /// When the peer joined (unix ms).
    pub first_seen_ms: u64,
    /// When the peer was last observed (unix ms).
    pub last_seen_ms: u64,
}

/// A registry of peers observed by a transport or engine.
#[derive(Debug, Default)]
pub struct PeerRegistry {
    peers: HashMap<PeerId, PeerInfo>,
}

impl PeerRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a peer joining (or being first observed).
    pub fn note_join(&mut self, peer: PeerId, now_ms: u64) {
        self.peers.entry(peer).or_insert(PeerInfo {
            peer_id: peer,
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
        });
    }

    /// Refreshes a peer's last-seen timestamp, registering unknown peers.
    pub fn note_seen(&mut self, peer: PeerId, now_ms: u64) {
        let slot = self.peers.entry(peer).or_insert(PeerInfo {
            peer_id: peer,
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
        });
        slot.last_seen_ms = now_ms;
    }

    /// Records a peer leaving. Returns `true` when it was tracked.
    pub fn note_leave(&mut self, peer: &PeerId) -> bool {
        self.peers.remove(peer).is_some()
    }

    /// A peer's info, if tracked.
    #[must_use]
    pub fn get(&self, peer: &PeerId) -> Option<&PeerInfo> {
        self.peers.get(peer)
    }

    /// All tracked peers, sorted by id.
    #[must_use]
    pub fn peers(&self) -> Vec<PeerInfo> {
        let mut infos: Vec<PeerInfo> = self.peers.values().copied().collect();
        infos.sort_by_key(|p| p.peer_id);
        infos
    }

    /// Number of tracked peers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// True when no peers are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_seen_leave_lifecycle() {
        let mut reg = PeerRegistry::new();
        reg.note_join(PeerId::from_u64(2), 100);
        reg.note_join(PeerId::from_u64(2), 150); // idempotent join keeps first_seen
        reg.note_seen(PeerId::from_u64(2), 200);

        let info = reg.get(&PeerId::from_u64(2)).unwrap();
        assert_eq!(info.first_seen_ms, 100);
        assert_eq!(info.last_seen_ms, 200);

        assert!(reg.note_leave(&PeerId::from_u64(2)));
        assert!(!reg.note_leave(&PeerId::from_u64(2)));
        assert!(reg.is_empty());
    }

    #[test]
    fn peers_sorted_by_id() {
        let mut reg = PeerRegistry::new();
        reg.note_join(PeerId::from_u64(30), 0);
        reg.note_join(PeerId::from_u64(10), 0);
        reg.note_join(PeerId::from_u64(20), 0);
        let ids: Vec<u64> = reg.peers().into_iter().map(|p| p.peer_id.as_u64()).collect();
        assert_eq!(ids, vec![10, 20, 30]);
    }
}
