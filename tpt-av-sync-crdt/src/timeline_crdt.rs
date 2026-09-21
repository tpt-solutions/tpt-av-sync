//! The [`TimelineCrdt`]: the CRDT engine that ties state, clocks, history,
//! and snapshots together.

use crate::history::{compute_inverse, History, HistoryEntry};
use crate::merge::{resolve_tag_conflict, OpTag, ResolutionEvent};
use crate::operation::{ClipId, RequiredTarget, TaggedOperation, TimelineOperation};
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
    resolution_events: Vec<ResolutionEvent>,
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
            resolution_events: Vec::new(),
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

        self.observe_resolution(&tagged_op);
        let tag = OpTag::new(tagged_op.lamport_ts, tagged_op.peer_id);
        match self.session.apply(&tagged_op.operation, tag) {
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

    /// Drains the queue of conflict-resolution events observed since the
    /// last call: which write won a concurrent move, delete, or split, and
    /// why. Empty most of the time — only genuinely contested edits (two
    /// different peers touching the same clip) produce one. Intended for a
    /// UI/log conflict visualizer; has no effect on CRDT state.
    pub fn take_resolution_events(&mut self) -> Vec<ResolutionEvent> {
        std::mem::take(&mut self.resolution_events)
    }

    /// Compacts the operation log in place: drops operations that no
    /// longer contribute to the current session state (see
    /// `crate::compaction`). Materialized state (`view()`) is unchanged; a
    /// fresh replica built by replaying the compacted log converges to the
    /// exact same state a full replay would have produced.
    ///
    /// This also shrinks the idempotency-tracking set to match, so a very
    /// late duplicate of an operation this call just dropped will be
    /// reprocessed rather than short-circuited — harmless (CRDT operations
    /// are idempotent by construction) but not free, so don't compact on
    /// every operation; call it periodically (e.g. session checkpoints, or
    /// every N operations) on a session old enough that such duplicates
    /// are no longer in flight.
    pub fn compact(&mut self) {
        let compacted = crate::compaction::compact(&self.operation_log, &self.session);
        self.seen = compacted.iter().map(|op| op.op_id).collect();
        self.operation_log = compacted;
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

        // Local edits are never flagged for `resolution_events`: the
        // Lamport clock always ticks past everything this replica has
        // already observed, so a local write can never lose to (or be
        // concurrent with) state already in `self.session` — see
        // `observe_resolution`, which only runs for incoming remote
        // operations.
        let tag = OpTag::new(lamport, self.local_peer_id);
        match self.session.apply(&operation, tag) {
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
            for op in std::mem::take(&mut self.pending) {
                self.observe_resolution(&op);
                let tag = OpTag::new(op.lamport_ts, op.peer_id);
                match self.session.apply(&op.operation, tag) {
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

    /// Checks whether `incoming` (an operation about to be applied, already
    /// present in `operation_log`) is genuinely concurrent with an earlier
    /// move/delete/split on the same clip, and if so records a
    /// [`ResolutionEvent`]. "Concurrent" is checked with vector clocks, not
    /// by comparing tags: two edits where one causally follows the other
    /// (the ordinary case — Bob edits a clip Alice created, after seeing
    /// her create it) are not a conflict, no matter how many peers have
    /// touched the clip. Only used for remote/pending application: a local
    /// edit's Lamport tick always dominates everything the replica has
    /// already observed, so it can never be concurrent with existing state.
    fn observe_resolution(&mut self, incoming: &TaggedOperation) {
        let (kind, clip_id) = match &incoming.operation {
            TimelineOperation::MoveClip { clip_id, .. } => ("move", *clip_id),
            TimelineOperation::DeleteClip { clip_id } => ("delete", *clip_id),
            TimelineOperation::SplitClip { clip_id, .. } => ("split", *clip_id),
            _ => return,
        };

        let already_logged = self.operation_log.len().saturating_sub(1);
        let Some(prior) = self.operation_log[..already_logged]
            .iter()
            .rev()
            .find(|op| op.op_id != incoming.op_id && matches_conflict_class(&op.operation, kind, clip_id))
            .cloned()
        else {
            return;
        };
        if !incoming.vector_clock.is_concurrent(&prior.vector_clock) {
            return; // one causally follows the other: not a conflict.
        }

        let incoming_tag = OpTag::new(incoming.lamport_ts, incoming.peer_id);
        let prior_tag = OpTag::new(prior.lamport_ts, prior.peer_id);

        let event = if kind == "split" {
            let (TimelineOperation::SplitClip { new_clip_id: incoming_new, split_frame: incoming_frame, .. },
                 TimelineOperation::SplitClip { new_clip_id: prior_new, split_frame: prior_frame, .. }) =
                (&incoming.operation, &prior.operation)
            else {
                unreachable!("matches_conflict_class guarantees SplitClip for kind \"split\"");
            };
            if incoming_frame != prior_frame || incoming_new == prior_new {
                return; // different split point, or the same proposal: no conflict.
            }
            ResolutionEvent {
                kind,
                clip_id,
                op: incoming_tag,
                op_won: incoming_new < prior_new,
                reason: "same split point, smaller clip id wins",
            }
        } else {
            resolve_tag_conflict(kind, clip_id, prior_tag, incoming_tag)
        };
        self.resolution_events.push(event);
    }
}

/// Whether `op` is the same conflict class (kind + target clip) as the one
/// being checked — used by [`TimelineCrdt::observe_resolution`] to find the
/// most recent prior operation to compare against.
fn matches_conflict_class(op: &TimelineOperation, kind: &str, clip_id: ClipId) -> bool {
    match (op, kind) {
        (TimelineOperation::MoveClip { clip_id: c, .. }, "move")
        | (TimelineOperation::DeleteClip { clip_id: c }, "delete")
        | (TimelineOperation::SplitClip { clip_id: c, .. }, "split") => *c == clip_id,
        _ => false,
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
