//! The [`TimelineCrdt`]: the CRDT engine that ties state, clocks, history,
//! and snapshots together.

use crate::history::{compute_inverse, History, HistoryEntry};
use crate::merge::OpTag;
use crate::operation::{RequiredTarget, TaggedOperation, TimelineOperation};
use crate::state::{Session, SessionView};
use std::collections::HashSet;
use std::time::SystemTime;
use tpt_av_sync_utils::{LamportClock, OperationId, PeerId, SyncError, VectorClock};

/// A replication snapshot: the full operation log.
///
/// Snapshots are replay-based: a peer restoring a snapshot deterministically
/// replays the operations in `(lamport, peer)` order, which reproduces the
/// exact CRDT state. This keeps snapshots compact (duplicated clip data is
/// not carried twice) and correct by construction.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TimelineSnapshot {
    /// The operation log to replay.
    pub ops: Vec<TaggedOperation>,
}

impl TimelineSnapshot {
    /// Creates a snapshot from an operation log.
    #[must_use]
    pub fn from_ops(ops: Vec<TaggedOperation>) -> Self {
        Self { ops }
    }
}

/// The CRDT timeline state.
///
/// This is the core data structure that maintains a conflict-free
/// representation of the timeline across all peers.
///
/// # Replication contract
///
/// - [`TimelineCrdt::apply_local`] stamps and applies a local edit and
///   returns the [`TaggedOperation`] to broadcast.
/// - [`TimelineCrdt::apply_remote`] applies a received operation. It is
///   **idempotent** (duplicate delivery is detected by operation id) and
///   **order-tolerant** (operations referencing unknown clips/tracks are
///   buffered until their dependencies arrive).
///
/// Any two replicas that eventually observe the same set of operations
/// converge to identical state, regardless of delivery order or duplicates.
pub struct TimelineCrdt {
    session: Session,
    operation_log: Vec<TaggedOperation>,
    seen: HashSet<OperationId>,
    vector_clock: VectorClock,
    lamport: LamportClock,
    local_peer_id: PeerId,
    pending: Vec<TaggedOperation>,
    history: History,
}

impl TimelineCrdt {
    /// Creates a new empty timeline CRDT.
    #[must_use]
    pub fn new(local_peer_id: PeerId) -> Self {
        Self {
            session: Session::new(),
            operation_log: Vec::new(),
            seen: HashSet::new(),
            vector_clock: VectorClock::new(),
            lamport: LamportClock::new(0),
            local_peer_id,
            pending: Vec::new(),
            history: History::new(256),
        }
    }

    /// The local peer id.
    #[must_use]
    pub const fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    /// The local vector clock.
    #[must_use]
    pub const fn vector_clock(&self) -> &VectorClock {
        &self.vector_clock
    }

    /// Applies a local operation (from the current user).
    ///
    /// Increments the Lamport clock and vector clock, applies the operation
    /// to the session state, records it in the log and undo history, and
    /// returns the tagged operation to be sent to peers.
    ///
    /// Operations whose targets do not (yet) exist are recorded and buffered
    /// exactly like remote operations; they apply whenever the target
    /// appears.
    pub fn apply_local(&mut self, operation: TimelineOperation) -> TaggedOperation {
        self.apply_local_internal(operation, true)
    }

    /// Undo the most recent local operation. Returns the compensating
    /// operation that was applied (already reflected in the log), or `None`
    /// when there is nothing to undo.
    pub fn undo(&mut self) -> Option<TaggedOperation> {
        let entry = self.history.pop_undo()?;
        let tagged = self.apply_local_internal(entry.inverse.clone(), false);
        self.history.push_redo(entry);
        Some(tagged)
    }

    /// Redo the most recently undone local operation.
    pub fn redo(&mut self) -> Option<TaggedOperation> {
        let entry = self.history.pop_redo()?;
        let tagged = self.apply_local_internal(entry.forward.clone(), false);
        self.history.push_undo(entry);
        Some(tagged)
    }

    /// Applies a remote operation (from another peer).
    ///
    /// Handles idempotency (duplicates are dropped), causal bookkeeping
    /// (vector clock merge, Lamport observation), and dependency buffering.
    pub fn apply_remote(&mut self, tagged_op: TaggedOperation) -> Result<(), SyncError> {
        if self.seen.contains(&tagged_op.op_id) {
            return Ok(());
        }
        self.vector_clock.merge(&tagged_op.vector_clock);
        self.vector_clock
            .witness(tagged_op.peer_id, tagged_op.lamport_ts);
        self.lamport.observe(tagged_op.lamport_ts);
        self.seen.insert(tagged_op.op_id);
        self.operation_log.push(tagged_op.clone());

        match self.session.apply(
            &tagged_op.operation,
            OpTag::new(tagged_op.lamport_ts, tagged_op.peer_id),
        ) {
            Ok(()) => {}
            Err(SyncError::UnknownTarget { .. }) => {
                self.pending.push(tagged_op);
            }
            Err(err) => return Err(err),
        }
        self.drain_pending();
        Ok(())
    }

    /// Returns the current session state.
    #[must_use]
    pub const fn session(&self) -> &Session {
        &self.session
    }

    /// Returns the operation log (for persistence or replication).
    #[must_use]
    pub fn operation_log(&self) -> &[TaggedOperation] {
        &self.operation_log
    }

    /// The materialized view of the session (convenience for
    /// `self.session().materialize()`).
    #[must_use]
    pub fn view(&self) -> SessionView {
        self.session.materialize()
    }

    /// Whether an operation with this id has already been applied.
    #[must_use]
    pub fn has_operation(&self, op_id: &OperationId) -> bool {
        self.seen.contains(op_id)
    }

    /// Number of buffered operations still waiting for their dependencies.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Generates a snapshot of the current state (for new peers joining).
    #[must_use]
    pub fn snapshot(&self) -> TimelineSnapshot {
        TimelineSnapshot {
            ops: self.operation_log.clone(),
        }
    }

    /// Restores state from a snapshot by replaying its operations in
    /// deterministic `(lamport, peer)` order.
    #[must_use]
    pub fn from_snapshot(snapshot: TimelineSnapshot, local_peer_id: PeerId) -> Self {
        let mut crdt = TimelineCrdt::new(local_peer_id);
        let mut ops = snapshot.ops;
        ops.sort_by_key(|op| op.rank());
        for op in ops {
            let _ = crdt.apply_remote(op);
        }
        crdt
    }

    /// Merges a snapshot into existing state, applying only operations this
    /// replica has not seen. Used by the offline-first resync flow.
    pub fn merge_snapshot(&mut self, snapshot: &TimelineSnapshot) {
        let mut ops = snapshot.ops.clone();
        ops.sort_by_key(|op| op.rank());
        for op in ops {
            let _ = self.apply_remote(op);
        }
    }

    /// Internal apply used by both local edits and undo/redo.
    fn apply_local_internal(
        &mut self,
        operation: TimelineOperation,
        record_history: bool,
    ) -> TaggedOperation {
        let lamport = self.lamport.tick();
        self.vector_clock.increment(self.local_peer_id);
        let op_id = OperationId::new(lamport, self.local_peer_id);
        let inverse =
            if record_history { compute_inverse(&operation, &self.session) } else { None };

        let tagged = TaggedOperation {
            op_id,
            operation: operation.clone(),
            lamport_ts: lamport,
            vector_clock: self.vector_clock.clone(),
            peer_id: self.local_peer_id,
            timestamp: SystemTime::now(),
        };

        match self
            .session
            .apply(&operation, OpTag::new(lamport, self.local_peer_id))
        {
            Ok(()) => {
                if let Some(inverse) = inverse {
                    self.history.record(HistoryEntry { forward: operation, inverse });
                }
            }
            Err(SyncError::UnknownTarget { .. }) => {
                self.pending.push(tagged.clone());
            }
            Err(_) => unreachable!("Session::apply only returns UnknownTarget"),
        }

        self.operation_log.push(tagged.clone());
        self.seen.insert(op_id);
        tagged
    }

    /// Repeatedly retries buffered operations until no more progress is
    /// possible. All writes are absolute, so the retry order among pending
    /// operations cannot affect the final state.
    fn drain_pending(&mut self) {
        loop {
            let mut progress = false;
            let mut still_waiting = Vec::new();
            for op in self.pending.drain(..) {
                match self.session.apply(
                    &op.operation,
                    OpTag::new(op.lamport_ts, op.peer_id),
                ) {
                    Ok(()) => progress = true,
                    Err(SyncError::UnknownTarget { .. }) => still_waiting.push(op),
                    Err(_) => unreachable!("Session::apply only returns UnknownTarget"),
                }
            }
            self.pending = still_waiting;
            if !progress {
                break;
            }
        }
    }
}

impl std::fmt::Debug for TimelineCrdt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimelineCrdt")
            .field("local_peer_id", &self.local_peer_id)
            .field("ops_in_log", &self.operation_log.len())
            .field("pending", &self.pending.len())
            .field("lamport", &self.lamport.get())
            .finish_non_exhaustive()
    }
}

/// Classifies which required targets of an operation are missing from a
/// session. Exposed for tests and diagnostics.
#[must_use]
pub fn missing_targets(op: &TimelineOperation, session: &Session) -> Vec<RequiredTarget> {
    op.required_targets()
        .into_iter()
        .filter(|target| match target {
            RequiredTarget::Clip(id) => session.clip(id).is_none(),
            RequiredTarget::Track(id) => session.track(id).is_none(),
        })
        .collect()
}
