//! The [`Transport`] trait — the seam between the sync engine and the
//! network.
//!
//! The trait is deliberately synchronous and blocking-friendly (matching
//! the engine's pull-based [`crate::SyncEngine::process_messages`] loop);
//! async transports (WebSocket, WebRTC) run their I/O on background tasks
//! and bridge into the engine through internal queues.

use crate::message::SyncMessage;
use std::sync::mpsc;
use std::sync::Mutex;
use tpt_av_sync_utils::{PeerId, SyncError};

/// A message transport connecting this peer to others.
///
/// Implementations must be thread-safe (`Send + Sync`). `recv` blocks;
/// engines normally use [`try_recv`](Transport::try_recv) in a poll loop.
pub trait Transport: Send + Sync {
    /// Sends a message to a specific peer.
    fn send_to(&mut self, peer_id: PeerId, message: SyncMessage) -> Result<(), SyncError>;

    /// Broadcasts a message to all connected peers.
    ///
    /// Returns [`SyncError::Transport`] when no peers are connected — the
    /// engine's offline-first flow treats that as a queue signal.
    fn broadcast(&mut self, message: SyncMessage) -> Result<(), SyncError>;

    /// Receives the next message (blocking).
    fn recv(&mut self) -> Result<(PeerId, SyncMessage), SyncError>;

    /// Receives the next message (non-blocking). `Ok(None)` means the inbox
    /// is currently empty.
    fn try_recv(&mut self) -> Result<Option<(PeerId, SyncMessage)>, SyncError>;

    /// Returns the list of connected peers (sorted for determinism).
    fn peers(&self) -> Vec<PeerId>;
}

/// An in-memory transport pair connected by a channel — for tests, demos,
/// and benchmarks without sockets.
///
/// Both ends share one channel; a message broadcast by either end is
/// delivered to the other. Peer membership is symmetric and fixed. Each
/// end can simulate being offline via [`Self::set_fail_sends`]; use
/// [`Self::fail_handle`] to keep control of that flag after handing the
/// transport to an engine.
#[derive(Debug)]
pub struct LoopbackTransport {
    local: PeerId,
    inbox: Mutex<mpsc::Receiver<(PeerId, SyncMessage)>>,
    outbox: Vec<mpsc::SyncSender<(PeerId, SyncMessage)>>,
    others: Vec<PeerId>,
    fail_sends: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl LoopbackTransport {
    fn new(
        local: PeerId,
        inbox: Mutex<mpsc::Receiver<(PeerId, SyncMessage)>>,
        outbox: Vec<mpsc::SyncSender<(PeerId, SyncMessage)>>,
        others: Vec<PeerId>,
    ) -> Self {
        Self {
            local,
            inbox,
            outbox,
            others,
            fail_sends: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Creates a pair of connected transports.
    pub fn pair(a: PeerId, b: PeerId) -> (LoopbackTransport, LoopbackTransport) {
        let (tx_a, rx_a) = mpsc::sync_channel(4096);
        let (tx_b, rx_b) = mpsc::sync_channel(4096);
        let ta = Self::new(a, Mutex::new(rx_a), vec![tx_b], vec![b]);
        let tb = Self::new(b, Mutex::new(rx_b), vec![tx_a], vec![a]);
        (ta, tb)
    }

    /// Creates an N-way loopback fabric (any transport delivers to all the
    /// others).
    #[must_use]
    pub fn fabric(ids: &[PeerId]) -> Vec<LoopbackTransport> {
        let mut senders: Vec<(PeerId, mpsc::SyncSender<(PeerId, SyncMessage)>)> = Vec::new();
        let mut receivers = Vec::new();
        for id in ids {
            let (tx, rx) = mpsc::sync_channel(4096);
            senders.push((*id, tx));
            receivers.push(rx);
        }
        let mut transports = Vec::with_capacity(ids.len());
        for (i, id) in ids.iter().enumerate() {
            let inbox = Mutex::new(std::mem::replace(&mut receivers[i], mpsc::sync_channel(1).1));
            transports.push(Self::new(
                *id,
                inbox,
                senders
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, tx)| tx.1.clone())
                    .collect(),
                ids.iter().filter(|o| **o != *id).copied().collect(),
            ));
        }
        transports
    }

    /// Makes every send fail (simulates being offline).
    pub fn set_fail_sends(&mut self, fail: bool) {
        self.fail_sends
            .store(fail, std::sync::atomic::Ordering::Relaxed);
    }

    /// The shared fail flag for this transport — flip it after the
    /// transport has been moved into an engine (reconnect simulation).
    #[must_use]
    pub fn fail_handle(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.fail_sends.clone()
    }

    /// This transport's peer id.
    #[must_use]
    pub const fn local(&self) -> PeerId {
        self.local
    }
}

impl Transport for LoopbackTransport {
    fn send_to(&mut self, peer_id: PeerId, message: SyncMessage) -> Result<(), SyncError> {
        if self.fail_sends.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(SyncError::transport("loopback send failure"));
        }
        let _ = peer_id; // loopback delivers to all connected ends
        for tx in &self.outbox {
            tx.send((self.local, message.clone()))
                .map_err(|_| SyncError::Disconnected)?;
        }
        Ok(())
    }

    fn broadcast(&mut self, message: SyncMessage) -> Result<(), SyncError> {
        if self.fail_sends.load(std::sync::atomic::Ordering::Relaxed)
            || self.outbox.is_empty()
        {
            return Err(SyncError::transport("no connected peers"));
        }
        for tx in &self.outbox {
            tx.send((self.local, message.clone()))
                .map_err(|_| SyncError::Disconnected)?;
        }
        Ok(())
    }

    fn recv(&mut self) -> Result<(PeerId, SyncMessage), SyncError> {
        self.inbox
            .lock()
            .expect("inbox lock")
            .recv()
            .map_err(|_| SyncError::Disconnected)
    }

    fn try_recv(&mut self) -> Result<Option<(PeerId, SyncMessage)>, SyncError> {
        match self.inbox.lock().expect("inbox lock").try_recv() {
            Ok(item) => Ok(Some(item)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(SyncError::Disconnected),
        }
    }

    fn peers(&self) -> Vec<PeerId> {
        if self.fail_sends.load(std::sync::atomic::Ordering::Relaxed) {
            Vec::new()
        } else {
            self.others.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_av_sync_crdt::TimelineSnapshot;

    fn ids() -> (PeerId, PeerId) {
        (PeerId::from_u64(1), PeerId::from_u64(2))
    }

    #[test]
    fn pair_delivers_both_directions() {
        let (mut a, mut b) = LoopbackTransport::pair(ids().0, ids().1);
        assert_eq!(a.peers(), vec![ids().1]);
        a.broadcast(SyncMessage::RequestSnapshot).unwrap();
        let (from, msg) = b.try_recv().unwrap().expect("message");
        assert_eq!(from, ids().0);
        assert_eq!(msg, SyncMessage::RequestSnapshot);

        b.send_to(ids().0, SyncMessage::Snapshot(TimelineSnapshot::from_ops(Vec::new())))
            .unwrap();
        let (from, msg) = a.try_recv().unwrap().expect("message");
        assert_eq!(from, ids().1);
        assert!(matches!(msg, SyncMessage::Snapshot(_)));
    }

    #[test]
    fn fail_sends_simulates_offline() {
        let (mut a, _b) = LoopbackTransport::pair(ids().0, ids().1);
        a.set_fail_sends(true);
        assert!(a.broadcast(SyncMessage::RequestSnapshot).is_err());
        assert!(a.peers().is_empty());
    }

    #[test]
    fn fabric_delivers_to_all_others() {
        let ids = vec![
            PeerId::from_u64(1),
            PeerId::from_u64(2),
            PeerId::from_u64(3),
        ];
        let mut fabric = LoopbackTransport::fabric(&ids);
        fabric[0].broadcast(SyncMessage::RequestSnapshot).unwrap();
        for end in &mut fabric[1..] {
            let (from, _) = end.try_recv().unwrap().expect("message");
            assert_eq!(from, ids[0]);
        }
    }
}
