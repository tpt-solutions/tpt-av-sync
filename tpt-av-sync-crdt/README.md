# tpt-av-sync-crdt

The CRDT engine for media timelines: conflict-free replication of clip/track/envelope edits for the [`tpt-av-sync`](https://github.com/tpt-solutions/tpt-av-sync) collaboration stack — and a general-purpose LWW-register CRDT toolkit for any document with spatial-temporal entities.

**Guarantee:** any two replicas that observe the same set of operations converge to identical state, regardless of arrival order or duplicates. Enforced by property-based tests (commutativity, idempotency, snapshot fidelity) and deterministic stress tests in this crate's CI.

## What's inside

| Module | Contents |
| :--- | :--- |
| `operation` | `TimelineOperation` (insert / move / delete / split / trim / metadata / envelope / session ops), `TaggedOperation`, id types (`ClipId`, `TrackId`). |
| `merge` | `LwwReg<T>` + `OpTag` — the last-writer-wins register everything is built from. |
| `clip_crdt` / `track_crdt` | Per-entity CRDT records (registers + split points + parent links). |
| `envelope_crdt` | Automation envelopes: whole-curve LWW per `(target, type)`. |
| `state` | `Session` — apply operations, resolve inherited values, `materialize()` into the ordered `SessionView` apps render. |
| `timeline_crdt` | `TimelineCrdt` — stamps local ops, applies remote ops idempotently + order-tolerantly (dependency buffering), snapshots, undo/redo. |
| `history` | `compute_inverse`, bounded undo/redo stacks. |
| `delta` | `compute_delta` / `apply_delta` — diff two materialized states into minimal operations. |

## Conflict resolution (the short version)

- Every field is an LWW register keyed by `(lamport_timestamp, peer_id)` — concurrent writes converge to the same winner everywhere.
- Concurrent deletes are idempotent; delete vs. edit converges (a later re-insert resurrects, a move cannot).
- **Concurrent splits compose**: splitting at 500 and 1000 yields three clips, on every replica, in any delivery order. Splits record *points* on the parent; pieces inherit fields from the parent chain until they receive their own writes.
- All operation payloads are absolute, so application is order-independent by construction.

See [DESIGN.md §4](https://github.com/tpt-solutions/tpt-av-sync/blob/master/DESIGN.md#4-the-crdt-tpt-av-sync-crdt) for the full model.

## Usage

```rust
use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_utils::PeerId;

let mut crdt = TimelineCrdt::new(PeerId::generate());

let track = TrackId::from_u64(1);
crdt.apply_local(TimelineOperation::InsertTrack {
    track_id: track,
    track: TrackData::new("Dialog"),
    position: 0,
});

let clip = ClipId::from_u64(2);
let tagged = crdt.apply_local(TimelineOperation::InsertClip {
    clip_id: clip,
    track_id: track,
    clip: ClipData::new("take_03.wav", 0, 48_000 * 10),
    position: 0,
});
// Send `tagged` to peers (tpt-av-sync-net); they call `crdt.apply_remote(tagged)`.

// Replicate a remote op — idempotent and order-tolerant:
// crdt.apply_remote(remote_tagged_op)?;

let view = crdt.view(); // materialized, ordered view for rendering
assert_eq!(view.clips.len(), 1);
```

Two-replica convergence:

```rust
let mut a = TimelineCrdt::new(PeerId::from_u64(1));
let mut b = TimelineCrdt::new(PeerId::from_u64(2));
for op in a.operation_log() {
    b.apply_remote(op.clone()).unwrap();
}
assert_eq!(a.view(), b.view());
```

Undo/redo (replicates as ordinary operations):

```rust
crdt.apply_local(TimelineOperation::MoveClip { clip_id: clip, new_track_id: track, new_start_frame: 500, new_position: 0 });
crdt.undo(); // compensating op applied + logged
crdt.redo();
```

## Testing the guarantees yourself

```sh
cargo test -p tpt-av-sync-crdt
# property tests:  cargo test -p tpt-av-sync-crdt --test property_ops
# scenarios:      cargo test -p tpt-av-sync-crdt --test scenarios
# stress:         cargo test -p tpt-av-sync-crdt --test stress
```

## Feature flags

None.

## Minimum supported Rust

1.75 (workspace MSRV).

## License

Dual-licensed under [MIT](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-MIT) OR [Apache-2.0](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-APACHE).
