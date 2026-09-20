//! Message reliability: operation acknowledgments and retry.

use std::collections::HashMap;
use std::time::Duration;
use tpt_av_sync_crdt::TaggedOperation;
use tpt_av_sync_utils::{OperationId, PeerId};

/// Tracks operations awaiting peer acknowledgments and resends the ones
/// that go unacknowledged.
///
/// The engine tracks broadcast operations until every peer has acked; an
/// operation that times out is resent (up to the configured `max_resends`) and
/// finally dropped from tracking. Thanks to CRDT idempotency, duplicate
/// delivery is always safe.
#[derive(Debug)]
pub struct ReliabilityManager {
    pending: HashMap<OperationId, PendingEntry>,
    ack_timeout_ms: u64,
    max_resends: u32,
    acked_peers: HashMap<OperationId, Vec<PeerId>>,
}

#[derive(Debug)]
struct PendingEntry {
    op: TaggedOperation,
    last_sent_ms: u64,
    attempts: u32,
}

impl ReliabilityManager {
    /// Creates a manager with the given ack timeout and resend budget.
    #[must_use]
    pub fn new(ack_timeout: Duration, max_resends: u32) -> Self {
        Self {
            pending: HashMap::new(),
            acked_peers: HashMap::new(),
            ack_timeout_ms: ack_timeout.as_millis() as u64,
            max_resends,
        }
    }

    /// Starts tracking a broadcast operation.
    pub fn track(&mut self, op: TaggedOperation, now_ms: u64) {
        self.pending.insert(
            op.op_id,
            PendingEntry {
                op,
                last_sent_ms: now_ms,
                attempts: 1,
            },
        );
    }

    /// Records an ack. Returns `true` when the operation was being tracked
    /// (first ack) — later acks from other peers are absorbed.
    pub fn on_ack(&mut self, op_id: &OperationId, from: PeerId) -> bool {
        if let Some(peers) = self.acked_peers.get_mut(op_id) {
            if !peers.contains(&from) {
                peers.push(from);
            }
            return false;
        }
        self.acked_peers.insert(*op_id, vec![from]);
        self.pending.remove(op_id).is_some()
    }

    /// Operations whose ack timed out: resends each (bumping attempt
    /// counters) and drops entries that exhausted `max_resends`.
    /// Also returns operations that were dropped for exceeding the budget.
    pub fn due_for_resend(&mut self, now_ms: u64) -> Vec<TaggedOperation> {
        let timeout = self.ack_timeout_ms;
        let max = self.max_resends;
        let mut due = Vec::new();
        self.pending.retain(|_, entry| {
            if now_ms.saturating_sub(entry.last_sent_ms) < timeout {
                return true;
            }
            if entry.attempts > max {
                return false;
            }
            entry.attempts += 1;
            entry.last_sent_ms = now_ms;
            due.push(entry.op.clone());
            true
        });
        due
    }

    /// Stops tracking an operation without an ack (e.g. after a give-up).
    pub fn forget(&mut self, op_id: &OperationId) {
        self.pending.remove(op_id);
        self.acked_peers.remove(op_id);
    }

    /// Number of operations currently awaiting acks.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// True when nothing is pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tpt_av_sync_utils::VectorClock;

    fn op(lamport: u64) -> TaggedOperation {
        let peer = PeerId::from_u64(1);
        TaggedOperation {
            op_id: OperationId::new(lamport, peer),
            operation: tpt_av_sync_crdt::TimelineOperation::DeleteClip {
                clip_id: tpt_av_sync_crdt::ClipId::from_u64(1),
            },
            lamport_ts: lamport,
            vector_clock: VectorClock::new(),
            peer_id: peer,
            timestamp: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn ack_clears_pending() {
        let mut rel = ReliabilityManager::new(Duration::from_millis(50), 3);
        rel.track(op(1), 1_000);
        assert_eq!(rel.pending_len(), 1);
        assert!(rel.on_ack(&OperationId::new(1, PeerId::from_u64(1)), PeerId::from_u64(2)));
        assert!(rel.is_empty());
        // Duplicate acks absorb silently.
        assert!(!rel.on_ack(&OperationId::new(1, PeerId::from_u64(1)), PeerId::from_u64(3)));
    }

    #[test]
    fn unacked_operations_are_resent_then_dropped() {
        let mut rel = ReliabilityManager::new(Duration::from_millis(100), 2);
        rel.track(op(1), 0);
        assert!(rel.due_for_resend(50).is_empty(), "not due yet");
        assert_eq!(rel.due_for_resend(150).len(), 1, "first resend");
        assert_eq!(rel.due_for_resend(300).len(), 1, "second resend");
        assert!(
            rel.due_for_resend(1_000).is_empty(),
            "budget exhausted: dropped"
        );
        assert!(rel.is_empty());
    }
}
