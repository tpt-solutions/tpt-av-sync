# tpt-av-sync-playhead

Real-time-safe playhead synchronization for [`tpt-av-sync`](https://github.com/tpt-solutions/tpt-av-sync): NTP-style clock sync, latency estimation, drift compensation, and shared transport control — so every machine in a session plays at the same instant position, within a millisecond.

**Real-time safety:** `PlayheadSync::set_local_position` and `synchronized_position` — the audio/render-thread hot path — perform arithmetic and a single hash probe. No allocation, no locking, no syscalls. Enforced by construction and pinned by Criterion benches.

## What's inside

| Module | Contents |
| :--- | :--- |
| `clock` | `ClockSyncMessage` (T1–T4), `ClockSynchronizer` — NTP-style offset with lowest-rtt sample selection. |
| `latency` | `LatencyEstimator` — EWMA round-trip smoothing, one-way estimate, event-time compensation. |
| `drift` | `DriftCompensator` — skew estimation in ppm from successive offsets; rejects NTP-step jumps; corrects elapsed time between sync rounds. |
| `sync` | `PlayheadSync` — the engine: local/remote positions, latency-adjusted extrapolation, `PlayheadUpdate` generation, clock-sync message processing. Takes an injectable `ClockFn` so tests are fully deterministic. |
| `transport` | `TransportSync`, `TransportControl`, `TransportState` — play/stop/record/locate replicated across peers (followers apply, masters ignore). |

## How it stays accurate

1. **Clock offset** via the classic four-timestamp exchange (`rtt = (t4−t1)−(t3−t2)`, `offset = ((t2−t1)+(t3−t4))/2`); the lowest-rtt sample wins, which rejects queuing jitter.
2. **Latency** — smoothed rtt/2 compensates transit when interpreting remote events.
3. **Drift** — clocks diverge by tens of ppm; the compensator tracks how the offset changes and extrapolates positions at the corrected rate between sync rounds (50 ppm ≈ 180 ms/hour if ignored).
4. **Playhead updates** are stamped in the sender's local domain; receivers convert once with their offset and extrapolate to "now" at the sample rate — stale updates never compound error.

## Usage

```rust
use tpt_av_sync_playhead::{PlayheadSync, ClockSyncMessage};
use tpt_av_sync_utils::PeerId;

let mut me = PlayheadSync::new(PeerId::generate(), 48_000);
let master = PeerId::from_u64(42);

// Learn the clock offset (repeats periodically in a real app).
me.follow_master(master);
let request = me.send_clock_sync_request(master);
// ... send `request`; when the response arrives:
// me.process_clock_sync(response_with_t4);

// Receive the master's playhead broadcast:
// me.receive_update(&playhead_update);

// On the audio thread:
let frame = me.synchronized_position(); // allocation-free, lock-free

// Drive the local transport from the master's commands:
// transport.on_remote_control(&control, master)?;
```

Two peers converge on a jittered link within 1 ms at 48 kHz — `tests/precision.rs` proves it against simulated networks with virtual clocks (no flakiness, no real sleeping):

```sh
cargo test -p tpt-av-sync-playhead
cargo bench -p tpt-av-sync-playhead   # hot-path + clock-sync benches
```

## Feature flags

None.

## Minimum supported Rust

1.75 (workspace MSRV).

## License

Dual-licensed under [MIT](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-MIT) OR [Apache-2.0](https://github.com/tpt-solutions/tpt-av-sync/blob/master/LICENSE-APACHE).
