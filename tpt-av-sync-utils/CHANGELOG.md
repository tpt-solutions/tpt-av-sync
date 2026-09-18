# Changelog — tpt-av-sync-utils

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) · SemVer.

## [Unreleased]

### Added
- `PeerId`: 64-bit peer identity with `generate()` (clock ⊕ pid ⊕ counter, SplitMix64-finished), `from_u64`/`as_u64`, total ordering, hex display.
- `OperationId`: `(lamport, peer)` pair; globally unique, totally ordered; display as `op(lamport@peer-…)`.
- `LamportClock`: `tick` / `observe` / `get`.
- `VectorClock`: `increment`, `witness`, `merge` (component-wise max), `happens_before`, `is_concurrent`, serde round-trip.
- `time`: `now_unix_ms`, `MonotonicClock` (offset + elapsed), `NetworkTime` (offset/rtt ↔ local/remote conversion), `Timestamped<T>`.
- `SyncError`: `Serialization`, `Transport`, `PeerNotFound`, `DuplicateOperation`, `UnknownTarget`, `InvalidOperation`, `Disconnected`, `Timeout` + convenience constructors.
- Shared raw id generator (`raw_generated_u64`) used by downstream id types.

### Verified
- 22 unit tests: clock ordering/concurrency, serde round-trips, display formats, monotonic elapsed time.
