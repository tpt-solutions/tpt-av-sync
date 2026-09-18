# Changelog — tpt-av-sync-crdt

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) · SemVer.

## [Unreleased]

### Added
- `TimelineOperation`: `InsertClip`, `MoveClip`, `DeleteClip`, `SplitClip`, `TrimClip`, `UpdateClipMetadata`, `InsertTrack`, `UpdateTrackMetadata`, `DeleteTrack`, `UpdateEnvelope`, `UpdateSessionMetadata` — all with absolute payloads; bincode round-trips for every variant.
- `TaggedOperation` (op id, lamport, vector clock, peer, wall-clock stamp) with deterministic `rank()`.
- `LwwReg<T>` + `OpTag` last-writer-wins registers: strictly-greater write acceptance gives commutativity + idempotency.
- `ClipCrdt` / `TrackCrdt` / `EnvelopeStore`: per-entity registers; clip split points with parent links; tracks gate visibility of their clips.
- `Session`: operation application with `SyncError::UnknownTarget` dependency signaling, lazy split inheritance (rename/trim propagation), geometry resolution (full range + visible extent), `materialize()` → ordered `SessionView`; deterministic split child-id ownership (smallest `(parent, offset)` claim wins, re-parenting on late claims, resurrect on re-delivery for redo).
- `TimelineCrdt`: `apply_local` (clock stamping + history + log), `apply_remote` (duplicate detection, clock merge, dependency buffering with drain), `undo`/`redo`, `snapshot`/`from_snapshot`/`merge_snapshot` (replay-based, order-sorted).
- `history`: `compute_inverse` for every operation (incl. session metadata), bounded undo/redo stacks.
- `delta`: `compute_delta` (minimal diffs between materialized views) and `apply_delta` (synthetic-tag application).

### Fixed
- Creation-order divergence on duplicate inserts (all default fields now written at the op's tag).
- Delete-before-insert delivery losing tombstones (deletes now buffer like all mutations).
- Split child-id races across parents (ownership claims, see above).
- Root clip extent not capped by split points when registers were already written.

### Verified
- 21 unit tests; 10 spec §5.1 scenario tests; proptest suites (commutativity, idempotency, snapshot fidelity, 256 cases each); stress tests (100+ op sessions, 5 replicas × 5 permutations, triple delivery, 4-generation snapshot chains).
