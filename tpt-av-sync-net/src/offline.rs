//! Offline-first support: queuing messages while disconnected.

use crate::message::SyncMessage;
use std::collections::VecDeque;

/// A bounded FIFO of messages captured while the peer has no connections.
///
/// When connectivity returns, [`Self::drain`] hands them to the transport
/// (the engine does this on peer-join). Because CRDT operations are
/// idempotent and order-tolerant, replaying the queue on reconnect needs no
/// coordination with receivers.
#[derive(Debug)]
pub struct OfflineQueue {
    queue: VecDeque<SyncMessage>,
    capacity: usize,
}

impl OfflineQueue {
    /// Creates a queue holding at most `capacity` messages; the oldest are
    /// dropped beyond that.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Enqueues a message, dropping the oldest when full.
    pub fn enqueue(&mut self, message: SyncMessage) {
        if self.queue.len() >= self.capacity {
            self.queue.pop_front();
        }
        self.queue.push_back(message);
    }

    /// Removes and returns all queued messages, in order.
    pub fn drain(&mut self) -> Vec<SyncMessage> {
        self.queue.drain(..).collect()
    }

    /// Number of queued messages.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::SyncMessage;

    #[test]
    fn fifo_with_bounded_capacity() {
        let mut q = OfflineQueue::new(2);
        q.enqueue(SyncMessage::RequestSnapshot);
        q.enqueue(SyncMessage::RequestSnapshot);
        q.enqueue(SyncMessage::RequestSnapshot);
        assert_eq!(q.len(), 2, "oldest dropped beyond capacity");
        assert_eq!(q.drain().len(), 2);
        assert!(q.is_empty());
    }
}
