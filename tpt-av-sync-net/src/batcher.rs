//! Operation batching to reduce network overhead (spec §7.1).

use crate::message::SyncMessage;
use crate::transport::Transport;
use std::time::{Duration, Instant};
use tpt_av_sync_crdt::TaggedOperation;
use tpt_av_sync_utils::SyncError;

/// Collects operations and sends them as a single
/// [`SyncMessage::Batch`] at a fixed interval (e.g. every 16 ms for
/// 60 fps edit streams).
#[derive(Debug)]
pub struct OperationBatcher {
    pending: Vec<TaggedOperation>,
    interval: Duration,
    last_flush: Instant,
}

impl Default for OperationBatcher {
    fn default() -> Self {
        Self::new(Duration::from_millis(16))
    }
}

impl OperationBatcher {
    /// Creates a batcher with the given flush interval.
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            pending: Vec::new(),
            interval,
            last_flush: Instant::now(),
        }
    }

    /// Adds an operation to the batch.
    pub fn add(&mut self, op: TaggedOperation) {
        self.pending.push(op);
    }

    /// Number of operations waiting to be flushed.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// True when nothing is waiting.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Broadcasts the pending batch if the interval has elapsed. Returns
    /// the number of operations flushed (0 when the interval has not
    /// elapsed or the batch is empty).
    ///
    /// On a broadcast failure (e.g. offline) the batch is retained and can
    /// be retried later.
    pub fn flush_if_ready(&mut self, transport: &mut dyn Transport) -> Result<usize, SyncError> {
        if self.pending.is_empty() || self.last_flush.elapsed() < self.interval {
            return Ok(0);
        }
        let count = self.pending.len();
        let batch = std::mem::take(&mut self.pending);
        match transport.broadcast(SyncMessage::Batch(batch.clone())) {
            Ok(()) => {
                self.last_flush = Instant::now();
                Ok(count)
            }
            Err(err) => {
                self.pending = batch;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LoopbackTransport;
    use std::time::SystemTime;
    use tpt_av_sync_crdt::{
        ClipId, TimelineOperation, TaggedOperation,
    };
    use tpt_av_sync_utils::{OperationId, PeerId, VectorClock};

    fn op(i: u64) -> TaggedOperation {
        let peer = PeerId::from_u64(1);
        TaggedOperation {
            op_id: OperationId::new(i, peer),
            operation: TimelineOperation::DeleteClip {
                clip_id: ClipId::from_u64(i),
            },
            lamport_ts: i,
            vector_clock: VectorClock::new(),
            peer_id: peer,
            timestamp: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn flush_honors_interval() {
        let mut batcher = OperationBatcher::new(Duration::from_millis(10_000));
        let (mut a, mut b) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));
        batcher.add(op(1));
        batcher.add(op(2));
        assert_eq!(batcher.pending_len(), 2);
        // Interval (10 s) has not elapsed since construction.
        assert_eq!(batcher.flush_if_ready(&mut a).unwrap(), 0);
        assert_eq!(batcher.pending_len(), 2);
        let _ = b;
    }

    #[test]
    fn flush_delivers_batch() {
        let mut batcher = OperationBatcher::new(Duration::from_millis(0));
        let (mut a, mut b) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));
        batcher.add(op(1));
        batcher.add(op(2));
        let flushed = batcher
            .flush_if_ready(&mut a)
            .unwrap_or_else(|e| panic!("flush: {e}"));
        assert_eq!(flushed, 2);
        let (_, msg) = b.try_recv().unwrap().unwrap();
        match msg {
            SyncMessage::Batch(ops) => assert_eq!(ops.len(), 2),
            other => panic!("expected batch, got {other:?}"),
        }
        assert!(batcher.is_empty());
    }

    #[test]
    fn failed_flush_retains_batch() {
        let mut batcher = OperationBatcher::new(Duration::from_millis(0));
        let (mut a, _b) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));
        a.set_fail_sends(true);
        batcher.add(op(1));
        assert!(batcher.flush_if_ready(&mut a).is_err());
        assert_eq!(batcher.pending_len(), 1, "batch must survive failures");
    }
}
