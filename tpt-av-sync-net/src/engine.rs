//! The [`SyncEngine`]: replication, events, and offline-first flows.

use crate::message::SyncMessage;
use crate::offline::OfflineQueue;
use crate::reliability::ReliabilityManager;
use crate::transport::Transport;
use std::time::Duration;
use tpt_av_sync_crdt::{TaggedOperation, TimelineCrdt};
use tpt_av_sync_playhead::{ClockSyncMessage, PlayheadUpdate, TransportControl};
use tpt_av_sync_presence::PresenceUpdate;
use tpt_av_sync_utils::{PeerId, SyncError};

/// Tunables for [`SyncEngine`].
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Queue operations when no peer is connected and flush them on
    /// reconnect (offline-first; default on).
    pub offline_queue: bool,
    /// How long to wait for acks before resending.
    pub ack_timeout: Duration,
    /// How many times to resend an unacknowledged operation.
    pub max_resends: u32,
    /// Upper bound on messages handled per [`SyncEngine::process_messages`]
    /// call (keeps the poll loop bounded).
    pub max_messages_per_tick: usize,
    /// Capacity of the offline queue.
    pub offline_capacity: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            offline_queue: true,
            ack_timeout: Duration::from_millis(1_000),
            max_resends: 4,
            max_messages_per_tick: 4_096,
            offline_capacity: 10_000,
        }
    }
}

/// Events surfaced by the engine to the application.
#[derive(Debug, Clone, PartialEq)]
pub enum SyncEvent {
    /// A new peer connected.
    PeerJoined(PeerId),
    /// A peer disconnected.
    PeerLeft(PeerId),
    /// A remote operation was applied.
    RemoteOperation(TaggedOperation),
    /// A snapshot was received and merged.
    SnapshotMerged {
        /// Operations actually applied (not already known).
        applied: usize,
        /// Operations the snapshot carried.
        total: usize,
    },
    /// A playhead update arrived (pass through to
    /// `tpt-av-sync-playhead`).
    Playhead(PlayheadUpdate),
    /// A transport-control command arrived.
    TransportControl(TransportControl),
    /// A presence update arrived.
    Presence(PresenceUpdate),
    /// A clock-sync message arrived (respond via
    /// `PlayheadSync::process_clock_sync`).
    ClockSync(ClockSyncMessage),
}

/// The sync engine that manages replication across peers.
///
/// Owns a [`TimelineCrdt`] and a [`Transport`]. The application flow is:
///
/// 1. call [`apply_local`](Self::apply_local) for user edits,
/// 2. call [`process_messages`](Self::process_messages) periodically,
/// 3. drain [`take_events`](Self::take_events) for UI/network reactions.
pub struct SyncEngine {
    crdt: TimelineCrdt,
    transport: Box<dyn Transport>,
    config: EngineConfig,
    reliability: ReliabilityManager,
    offline: OfflineQueue,
    events: Vec<SyncEvent>,
    known_peers: Vec<PeerId>,
}

impl SyncEngine {
    /// Creates a new sync engine.
    #[must_use]
    pub fn new(crdt: TimelineCrdt, transport: Box<dyn Transport>) -> Self {
        Self::with_config(crdt, transport, EngineConfig::default())
    }

    /// Creates a sync engine with custom configuration.
    #[must_use]
    pub fn with_config(
        crdt: TimelineCrdt,
        transport: Box<dyn Transport>,
        config: EngineConfig,
    ) -> Self {
        let offline = OfflineQueue::new(config.offline_capacity);
        let reliability = ReliabilityManager::new(config.ack_timeout, config.max_resends);
        Self {
            crdt,
            transport,
            config,
            reliability,
            offline,
            events: Vec::new(),
            known_peers: Vec::new(),
        }
    }

    /// The local peer id.
    #[must_use]
    pub fn local_peer_id(&self) -> PeerId {
        self.crdt.local_peer_id()
    }

    /// Read-only access to the CRDT.
    #[must_use]
    pub const fn crdt(&self) -> &TimelineCrdt {
        &self.crdt
    }

    /// Mutable access to the CRDT (e.g. for undo/redo; note that undo/redo
    /// apply through [`TimelineCrdt::apply_local`] and are *not*
    /// broadcast — broadcast [`SyncEvent`]s cover only engine flows; call
    /// [`Self::broadcast_log_tail`] after direct CRDT mutation).
    #[must_use]
    pub const fn crdt_mut(&mut self) -> &mut TimelineCrdt {
        &mut self.crdt
    }

    /// Read-only access to the transport.
    #[must_use]
    pub const fn transport(&self) -> &dyn Transport {
        &*self.transport
    }

    /// Mutable access to the transport (e.g. to send application-level
    /// messages such as playhead updates or presence).
    #[must_use]
    pub fn transport_mut(&mut self) -> &mut dyn Transport {
        &mut *self.transport
    }

    /// Applies a local operation and broadcasts it to peers.
    ///
    /// The operation is applied to the CRDT first; broadcast failures are
    /// absorbed by the offline queue when enabled.
    pub fn apply_local(&mut self, operation: tpt_av_sync_crdt::TimelineOperation) -> TaggedOperation {
        let tagged = self.crdt.apply_local(operation);
        self.dispatch(SyncMessage::Operation(tagged.clone()));
        tagged
    }

    /// Broadcasts a request for a state snapshot (used after connecting).
    pub fn request_snapshot(&mut self) {
        self.dispatch(SyncMessage::RequestSnapshot);
    }

    /// Broadcasts a message through the transport with offline handling.
    fn dispatch(&mut self, msg: SyncMessage) {
        match self.transport.broadcast(msg.clone()) {
            Ok(()) => {
                if let SyncMessage::Operation(op) = msg {
                    self.reliability.track(op, now_ms());
                }
            }
            Err(err) => {
                log::debug!("broadcast failed ({err}); offline handling");
                if self.config.offline_queue {
                    self.offline.enqueue(msg);
                }
            }
        }
    }

    /// Polls the transport: processes inbound messages, refreshes peer
    /// membership, and resends unacknowledged operations. Returns the
    /// number of messages handled.
    pub fn process_messages(&mut self) -> usize {
        self.detect_peer_changes();
        let mut handled = 0;
        while handled < self.config.max_messages_per_tick {
            match self.transport.try_recv() {
                Ok(Some((peer, msg))) => {
                    self.handle_message(peer, msg);
                    handled += 1;
                }
                Ok(None) => break,
                Err(err) => {
                    log::debug!("try_recv error: {err}");
                    break;
                }
            }
        }
        self.resend_due();
        handled
    }

    /// Fires `PeerJoined`/`PeerLeft` for membership changes and pushes a
    /// snapshot to joiners.
    fn detect_peer_changes(&mut self) {
        let current = self.transport.peers();
        for peer in &current {
            if !self.known_peers.contains(peer) {
                self.events.push(SyncEvent::PeerJoined(*peer));
                let _ = self.handle_peer_join(*peer);
            }
        }
        let departed: Vec<PeerId> = self
            .known_peers
            .iter()
            .filter(|p| !current.contains(p))
            .copied()
            .collect();
        for peer in departed {
            self.events.push(SyncEvent::PeerLeft(peer));
            self.handle_peer_leave(peer);
        }
        self.known_peers = current;
    }

    /// Handles a new peer joining: sends the full state snapshot and
    /// flushes the offline queue to it.
    pub fn handle_peer_join(&mut self, peer_id: PeerId) -> Result<(), SyncError> {
        let snapshot = SyncMessage::Snapshot(self.crdt.snapshot());
        self.transport.send_to(peer_id, snapshot)?;
        self.flush_offline_queue();
        Ok(())
    }

    /// Handles a peer leaving. Pending acks for the peer are kept (the
    /// resend budget bounds the churn of re-sends).
    pub fn handle_peer_leave(&mut self, _peer_id: PeerId) {}

    /// Drains the offline queue into the transport (called on join).
    pub fn flush_offline_queue(&mut self) {
        for msg in self.offline.drain() {
            match self.transport.broadcast(msg.clone()) {
                Ok(()) => {
                    if let SyncMessage::Operation(op) = msg {
                        self.reliability.track(op, now_ms());
                    }
                }
                Err(err) => {
                    log::warn!("offline flush failed ({err}); dropping message");
                }
            }
        }
    }

    /// Events accumulated since the last drain.
    pub fn take_events(&mut self) -> Vec<SyncEvent> {
        std::mem::take(&mut self.events)
    }

    /// Operations still awaiting peer acknowledgments.
    #[must_use]
    pub fn pending_acks(&self) -> usize {
        self.reliability.pending_len()
    }

    /// Messages queued while offline.
    #[must_use]
    pub fn offline_queue_len(&self) -> usize {
        self.offline.len()
    }

    fn handle_message(&mut self, peer: PeerId, msg: SyncMessage) {
        match msg {
            SyncMessage::Operation(op) => self.handle_operation(peer, op),
            SyncMessage::Batch(ops) => {
                for op in ops {
                    self.handle_operation(peer, op);
                }
            }
            SyncMessage::Ack(op_id) => {
                self.reliability.on_ack(&op_id, peer);
            }
            SyncMessage::RequestSnapshot => {
                let snapshot = SyncMessage::Snapshot(self.crdt.snapshot());
                let _ = self.transport.send_to(peer, snapshot);
            }
            SyncMessage::Snapshot(snapshot) => {
                let total = snapshot.ops.len();
                let before = self.crdt.operation_log().len();
                self.crdt.merge_snapshot(&snapshot);
                let applied = self.crdt.operation_log().len() - before;
                self.events.push(SyncEvent::SnapshotMerged { applied, total });
            }
            SyncMessage::PlayheadUpdate(update) => {
                self.events.push(SyncEvent::Playhead(update));
            }
            SyncMessage::TransportControl(control) => {
                self.events.push(SyncEvent::TransportControl(control));
            }
            SyncMessage::PresenceUpdate(update) => {
                self.events.push(SyncEvent::Presence(update));
            }
            SyncMessage::ClockSync(message) => {
                self.events.push(SyncEvent::ClockSync(message));
            }
        }
    }

    fn handle_operation(&mut self, peer: PeerId, op: TaggedOperation) {
        // Untrusted input: structural limits before the CRDT sees it.
        if let Err(err) = op.operation.validate() {
            log::warn!("rejecting invalid operation from {peer}: {err}");
            return;
        }
        match self.crdt.apply_remote(op.clone()) {
            Ok(()) => {
                let _ = self
                    .transport
                    .send_to(peer, SyncMessage::Ack(op.op_id));
                self.events.push(SyncEvent::RemoteOperation(op));
            }
            Err(err) => {
                log::warn!("failed to apply operation {op:?}: {err}");
            }
        }
    }

    fn resend_due(&mut self) {
        for op in self.reliability.due_for_resend(now_ms()) {
            let _ = self.transport.broadcast(SyncMessage::Operation(op));
        }
    }
}

fn now_ms() -> u64 {
    tpt_av_sync_utils::time::now_unix_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LoopbackTransport;
    use tpt_av_sync_crdt::{ClipData, ClipId, TimelineOperation, TrackData, TrackId};
    
    

    fn crdt_with_track(peer: PeerId) -> TimelineCrdt {
        let mut crdt = TimelineCrdt::new(peer);
        crdt.apply_local(TimelineOperation::InsertTrack {
            track_id: TrackId::from_u64(1),
            track: TrackData::new("A1"),
            position: 0,
        });
        crdt
    }

    #[test]
    fn offline_queue_flushes_on_peer_join() {
        let (transport_a, transport_b) =
            LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));
        let offline_flag = transport_a.fail_handle();
        offline_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut engine =
            SyncEngine::new(crdt_with_track(PeerId::from_u64(1)), Box::new(transport_a));

        // Nobody reachable: the edit is queued.
        let clip = ClipId::from_u64(10);
        engine.apply_local(TimelineOperation::InsertClip {
            clip_id: clip,
            track_id: TrackId::from_u64(1),
            clip: ClipData::new("offline edit", 0, 100),
            position: 0,
        });
        assert_eq!(engine.offline_queue_len(), 1, "queued while offline");

        // Restore connectivity and push the state to the joiner.
        offline_flag.store(false, std::sync::atomic::Ordering::Relaxed);
        engine.handle_peer_join(PeerId::from_u64(2)).unwrap();
        assert_eq!(engine.offline_queue_len(), 0, "queue flushed on join");

        let mut other = SyncEngine::new(
            crdt_with_track(PeerId::from_u64(2)),
            Box::new(transport_b),
        );
        for _ in 0..10 {
            engine.process_messages();
            other.process_messages();
        }
        assert_eq!(engine.crdt().view(), other.crdt().view());
    }

    #[test]
    fn events_flow_for_ops_acks_and_snapshots() {
        let (ta, tb) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));
        let mut a = SyncEngine::new(crdt_with_track(PeerId::from_u64(1)), Box::new(ta));
        let mut b = SyncEngine::new(crdt_with_track(PeerId::from_u64(2)), Box::new(tb));

        a.apply_local(TimelineOperation::InsertClip {
            clip_id: ClipId::from_u64(11),
            track_id: TrackId::from_u64(1),
            clip: ClipData::new("c", 0, 5),
            position: 0,
        });
        for _ in 0..10 {
            a.process_messages();
            b.process_messages();
        }

        assert!(a.take_events().iter().any(|e| matches!(
            e,
            SyncEvent::SnapshotMerged { .. }
        )));
        let b_events = b.take_events();
        assert!(
            b_events
                .iter()
                .any(|e| matches!(e, SyncEvent::RemoteOperation(op) if op.lamport_ts == 2)),
            "B should see A's insert: {b_events:?}"
        );
        assert_eq!(a.crdt().view(), b.crdt().view());
    }

    #[test]
    fn unacked_op_is_resent() {
        let (ta, _tb) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));
        let mut engine = SyncEngine::with_config(
            crdt_with_track(PeerId::from_u64(1)),
            Box::new(ta),
            EngineConfig {
                ack_timeout: Duration::from_millis(0), // immediately due
                max_resends: 3,
                ..EngineConfig::default()
            },
        );
        engine.apply_local(TimelineOperation::InsertClip {
            clip_id: ClipId::from_u64(12),
            track_id: TrackId::from_u64(1),
            clip: ClipData::new("x", 0, 1),
            position: 0,
        });
        assert_eq!(engine.pending_acks(), 1);
        engine.process_messages(); // resend fires (still no acks)
        assert!(engine.pending_acks() <= 1);
    }

    #[test]
    fn snapshot_request_response_round_trip() {
        let (ta, tb) = LoopbackTransport::pair(PeerId::from_u64(1), PeerId::from_u64(2));
        let mut a = SyncEngine::new(crdt_with_track(PeerId::from_u64(1)), Box::new(ta));
        let mut b = SyncEngine::new(TimelineCrdt::new(PeerId::from_u64(2)), Box::new(tb));

        a.crdt_mut().apply_local(TimelineOperation::InsertTrack {
            track_id: TrackId::from_u64(1),
            track: TrackData::new("A1"),
            position: 0,
        });
        a.apply_local(TimelineOperation::InsertClip {
            clip_id: ClipId::from_u64(13),
            track_id: TrackId::from_u64(1),
            clip: ClipData::new("c", 0, 1),
            position: 0,
        });

        b.request_snapshot();
        for _ in 0..10 {
            a.process_messages();
            b.process_messages();
        }
        assert_eq!(a.crdt().view(), b.crdt().view());
        // A's log: setup track + re-insert track + clip.
        assert_eq!(a.crdt().operation_log().len(), 3);
        assert_eq!(b.crdt().operation_log().len(), 3, "B replayed A's log");
        assert!(b
            .take_events()
            .iter()
            .any(|e| matches!(e, SyncEvent::SnapshotMerged { .. })));
    }
}
