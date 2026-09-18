# tpt-av-sync-utils

Shared types, logical clocks, time helpers, and error handling for the [`tpt-av-sync`](https://github.com/tpt-av-sync) real-time collaboration engine.

This is the dependency-free foundation crate of the workspace: every other crate builds on it, and it is useful standalone in any distributed system that needs hybrid logical identity and causal bookkeeping.

## What's inside

| Module | Contents |
| :--- | :--- |
| [`peer_id`](https://docs.rs/tpt-av-sync-utils) | `PeerId` — 64-bit peer identity, generated or caller-supplied, totally ordered. |
| `operation_id` | `OperationId` — `(lamport, peer)` pair; globally unique, totally ordered. |
| `clock` | `LamportClock` (tick/observe) and `VectorClock` (increment / merge / happens-before / is-concurrent). |
| `time` | `now_unix_ms`, `MonotonicClock`, `NetworkTime` (offset/rtt conversion), `Timestamped<T>`. |
| `error` | `SyncError` — the one error type shared across all `tpt-av-sync` crates. |

## Design notes

- **Timestamps on the wire are `u64` milliseconds since the Unix epoch.** `std::time::SystemTime` appears only where the CRDT spec mandates it. Monotonic measurements go through `MonotonicClock` (wraps `Instant`).
- **Ordering is load-bearing.** `PeerId` and `OperationId` implement `Ord` and their order is the deterministic tie-breaker used by last-writer-wins conflict resolution in `tpt-av-sync-crdt`. Never rely on `Ord` for display sorting.
- **`PeerId::generate` is not cryptographic.** It mixes wall clock, process id, and a process-local counter — collision-free for any realistic session, but if you need adversarially-unique ids, supply your own randomness through `PeerId::from_u64`.
- **Serde-first.** Every public type implements `Serialize`/`Deserialize` and round-trips through `bincode` (unit-tested).

## Usage

```rust
use tpt_av_sync_utils::{LamportClock, OperationId, PeerId, VectorClock};

// Identity
let alice = PeerId::generate();

// Lamport clock: causally-ordered timestamps across peers
let mut a = LamportClock::new(0);
let mut b = LamportClock::new(0);
let t1 = a.tick();          // A performs an event
b.observe(t1);              // B receives it
let t2 = b.tick();          // B's next event is causally after A's
assert!(t2 > t1);

// Vector clock: detect concurrency
let mut va = VectorClock::new();
let mut vb = VectorClock::new();
va.increment(alice);
let bob = PeerId::generate();
vb.increment(bob);
assert!(va.is_concurrent(&vb));

let id = OperationId::new(t2, bob); // unique + ordered
```

## Feature flags

None — the crate is dependency-light by design (`serde` only).

## Minimum supported Rust

1.75 (workspace MSRV).

## License

Dual-licensed under [MIT](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-MIT) OR [Apache-2.0](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-APACHE).
