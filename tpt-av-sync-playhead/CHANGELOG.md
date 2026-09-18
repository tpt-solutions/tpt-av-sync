# Changelog — tpt-av-sync-playhead

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) · SemVer.

## [Unreleased]

### Added
- `ClockSyncMessage` (T1–T4 NTP exchange) with `request` / `respond` / `with_t4` / `measure` (offset + rtt); `ClockSyncType`; serde round-trip.
- `ClockSynchronizer`: lowest-rtt sample selection for jitter rejection; local↔remote conversions; accepts pre-stamped `t4` (virtual/higher-resolution clocks).
- `LatencyEstimator`: EWMA rtt smoothing (`alpha` configurable), one-way estimate, event-time compensation.
- `DriftCompensator`: skew estimation in ppm from successive offsets, EWMA smoothing, NTP-step jump rejection (>10 000 ppm), elapsed-time correction.
- `PlayheadSync`: master/follower playhead engine with injectable `ClockFn` (`new` real-time, `with_clock` deterministic); `PlayheadUpdate` stamps in the *sender's* domain (receivers convert once — no double offsets); `synchronized_position` extrapolates paused/playing masters with drift correction; clock-sync message processing feeds offset/latency/drift estimators; `observe_clock_sample` for external sync loops.
- `TransportSync` / `TransportControl` / `TransportState`: play/stop/record/locate replication (followers apply, masters ignore; `Locate` preserves state).

### Fixed
- Playhead update timestamps were pre-converted into the remote domain, double-applying the clock offset on receive (senders now stamp local time).

### Verified
- 25 unit tests; deterministic precision suite (virtual clocks + seeded jitter): sub-ms clock-offset accuracy over 100 exchanges, <1 ms playhead tracking at 48 kHz through a 4–9 ms one-way link, 40 ppm drift convergence within 10 ppm, transport follower agreement; Criterion benches for `synchronized_position`, `set_local_position`, `generate_update`, and clock-sync processing.
