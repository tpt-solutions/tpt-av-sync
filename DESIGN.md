# DESIGN.md — tpt-av-sync

**CRDT-based real-time collaboration engine for media timelines.**
This is the living design document; `spec.txt` is the original specification this document carries forward. Where this document and the spec disagree, this document wins and the difference is recorded in [Deviations from spec.txt](#deviations-from-spectxt).

Status: pre-1.0 · Dual-licensed MIT OR Apache-2.0 · TPT Solutions Open Source

---

## 1. Goals and non-goals

### Goals

1. **Conflict-free replication.** Any two replicas that observe the same set of operations converge to identical state, regardless of delivery order, duplicates, or partitions.
2. **Real-time safety on hot paths.** Playhead synchronization and per-frame parameter updates are allocation-free and lock-free; safe to call from audio/render threads.
3. **Offline-first.** Edits work with zero connectivity; state merges automatically on reconnect.
4. **Pure Rust.** No C/C++ bindings anywhere in the sync stack (webrtc-rs is pure Rust).
5. **Permissive licensing.** Dual MIT OR Apache-2.0; GPL/LGPL/AGPL/MPL dependencies are rejected by `cargo-deny` in CI.
6. **Composability.** Every crate is independently useful: use just the CRDT, just the playhead sync, or just presence.

### Non-goals

- **Media decoding/processing.** `tpt-audio` / `tpt-visual` own the media; this stack replicates *state about* media.
- **Rolling your own crypto.** TLS transport, peer authentication, and room authorization are built in (§10) — a deployment still owns key/token distribution and network topology, but not the primitives.
- **CRDT garbage collection.** Tombstones are retained for the session lifetime; GC of dead operation logs is future work.

---

## 2. Crate architecture

```text
                    ┌────────────────────────────┐
                    │  examples/ (demo binaries) │
                    └─────────────┬──────────────┘
              ┌───────────────────┼────────────────────┐
              │                   │                    │
     ┌────────▼────────┐  ┌───────▼────────┐  ┌────────▼────────┐
     │ tpt-av-sync-net │  │  playhead      │  │    presence     │
     │  transports,    │  │  clock sync,   │  │  cursors, idle  │
     │  SyncEngine     │  │  latency/drift │  │  avatars        │
     └────────┬────────┘  └───────┬────────┘  ┌────────▼────────┐
              │                   │           │                 │
              │            ┌──────▼───────────▼─────────┐       │
              │            │     tpt-av-sync-crdt       │◄──────┘
              │            │ operations, LWW registers, │
              │            │ Session, snapshots, deltas │
              │            └──────────┬─────────────────┘
              │                       │
              │            ┌──────────▼─────────────────┐
              └───────────►│     tpt-av-sync-utils      │
                           │ PeerId, OperationId,       │
                           │ Lamport/VectorClock,       │
                           │ SyncError, time            │
                           └────────────────────────────┘

  optional server-side:
     tpt-av-sync-server  (signaling, relay + RelayClientTransport, persistence)
```

Dependency edges point downward only. `tpt-av-sync-net` depends on playhead and presence because its `SyncMessage` enum carries their wire types.

## 3. Identity and ordering (utils)

- **`PeerId`** — 64-bit, generated from wall clock ⊕ process id ⊕ counter through a SplitMix64 finalizer. Totally ordered; the ordering is used as a tie-breaker everywhere. Applications may supply their own values via `PeerId::from_u64`.
- **`OperationId` = (lamport, peer)** — globally unique, totally ordered. The CRDT uses it for idempotency (duplicate detection) and as the LWW ranking.
- **`LamportClock`** — `tick()` for local events, `observe(remote)` on receive. Gives happens-before for causally related events.
- **`VectorClock`** — per-peer counters; `merge` (max), `happens_before`, `is_concurrent`. Maintained by `TimelineCrdt` and stamped onto every `TaggedOperation` (the engine currently uses it causally-lite: merge + witness on receive; full concurrency detection is available but conflict resolution does not require it — see §4).

## 4. The CRDT (tpt-av-sync-crdt)

### 4.1 State model

Every mutable field of every entity (clip, track, envelope, session metadata) is an **`LwwReg<T>`** — a value plus the `(lamport, peer)` tag of the writing operation. A write wins only if its tag is strictly greater. This single rule makes application:

- **commutative** — `max` is commutative, so field values converge under any order;
- **idempotent** — re-delivery loses the tie against itself.

### 4.2 Operations

`TimelineOperation` variants: `InsertClip`, `MoveClip`, `DeleteClip`, `SplitClip`, `TrimClip`, `UpdateClipMetadata`, `InsertTrack`, `UpdateTrackMetadata`, `DeleteTrack`, `UpdateEnvelope`, `UpdateSessionMetadata`. All payloads are **absolute** (no deltas relative to current state) — this is what makes remote application order-independent.

### 4.3 Deletes and causality

Deletes tombstone the `alive` register. Like every mutating operation, a delete **requires its target to exist**; if it arrives before the insert, the `TimelineCrdt` buffers it (`pending`) and re-drains whenever new state arrives. Buffering deletes — rather than accepting them as no-ops against unknown ids — is what makes delete-before-insert delivery order-independent: the tombstone lands after the insert regardless of arrival order.

Re-inserting a tombstoned id at a higher tag resurrects the entity (undo-of-delete relies on this). A concurrent move cannot resurrect; only `InsertClip` writes the liveness register.

### 4.4 Splits: derived segments, not copies

A split never copies data. `SplitClip { clip_id, split_frame, new_clip_id }`:

1. records `(split_frame → new_clip_id)` in the parent's `splits` map (a set — concurrent splits compose);
2. creates the child clip with **unwritten** field registers and a `parent = (clip_id, offset)` link.

Values are then **resolved lazily**: while a child's register is unwritten it inherits from the parent chain. Rename the original and every un-renamed piece follows; trim a piece and it keeps its own geometry from then on. Resolving lazily instead of copying at creation time is what makes concurrent splits deterministic regardless of arrival order.

Geometry resolution works on two levels:

- **full range** `[start, tail)` — derived from own registers (roots and detached clips) or from the parent chain (offset into parent, bounded by the parent's next split boundary);
- **visible extent** — the full range capped by the clip's *own* earliest split point.

Spec §5.1 falls out directly: A splits X at 500, B splits X at 1000 → X's visible extent is 0–500, the child at offset 500 is bounded by B's split point at 1000, and the child at offset 1000 runs to the end — three clips.

**Child-id ownership.** Two split ops may target the same child id from different parents (pathological ids, undo/redo cycles). Ownership goes to the smallest `(parent_id, offset)` claim; losers are no-ops; a winning claim arriving later re-parents the child and removes the stale split point from the old parent. Equal re-delivery resurrects the child at the op's tag (redo-after-undo relies on that).

### 4.5 Undo/redo

`compute_inverse(op, session_before)` derives the inverse from pre-operation state. Undo applies the inverse as a **fresh local operation** (new Lamport timestamp), so undo replicates like any other edit. History is a bounded stack (default 256 deep).

### 4.6 Snapshots and deltas

- `TimelineSnapshot` is the **operation log**; `from_snapshot` replays in `(lamport, peer)` order. Replay-based snapshots are compact and correct by construction (no duplicated state to keep consistent). `merge_snapshot` applies only unknown ops — the offline resync path.
- `compute_delta(old, new)` diffs two materialized views into minimal operations; `apply_delta` applies them with synthetic tags. Used for bandwidth-lean resync of visible state.

### 4.7 What the property tests enforce

In `tests/property_ops.rs` and `tests/stress.rs`, over generated op sequences (insert/move/delete/split/trim/metadata/envelope/track ops):

- **commutativity** — every permutation of an op set yields the identical materialized view;
- **idempotency** — delivering everything twice (in different orders) changes nothing and does not grow the log;
- **snapshot fidelity** — replay reproduces the exact state;
- **stress** — 100+ clip sessions across 5 replicas with 5 permutations, triple delivery, and 4-generation snapshot chains.

## 5. Networking (tpt-av-sync-net)

### 5.1 The `Transport` seam

```rust
pub trait Transport: Send + Sync {
    fn send_to(&mut self, peer: PeerId, msg: SyncMessage) -> Result<(), SyncError>;
    fn broadcast(&mut self, msg: SyncMessage) -> Result<(), SyncError>;
    fn recv(&mut self) -> Result<(PeerId, SyncMessage), SyncError>;
    fn try_recv(&mut self) -> Result<Option<(PeerId, SyncMessage)>, SyncError>;
    fn peers(&self) -> Vec<PeerId>;
}
```

Deliberately synchronous/poll-based: the engine pulls with `try_recv`, and async transports (WebSocket, WebRTC) run their I/O on internal runtimes that bridge into per-transport inboxes. This keeps the engine runtime-agnostic and simple to embed.

Implementations: `LoopbackTransport` (in-memory; tests/demos), `TcpTransport` (length-prefixed bincode `WireFrame`s, Hello handshake, one reader thread per connection), `WebsocketTransport` (tokio-tungstenite; binary frames), `WebRtcTransport` (feature `webrtc`; SCTP data channels, signaling via `SignalEnvelope`s the application relays), and `RelayClientTransport` (server crate; tunnels through a relay).

**Platform notes baked into the code:** accepted TCP sockets inherit the listener's non-blocking mode on Windows, so the transport forces blocking mode on connections; UDP discovery binds with `SO_REUSEADDR` (required on Windows for two peers on one host); WebRTC candidate gathering disables mDNS names (`.local`) because LAN sessions cannot rely on mDNS resolution.

### 5.2 `SyncEngine`

Owns a `TimelineCrdt` + `Box<dyn Transport>`. The app loop: `apply_local` for edits → `process_messages` on a timer → drain `take_events`. The engine handles: op dispatch + ack tracking (`ReliabilityManager`, bounded resend), snapshot push on peer join, `RequestSnapshot` answers, batch delivery, and the offline queue (`OfflineQueue`; bounded FIFO flushed on join). Events surface application-level messages (playhead, transport control, presence, clock sync) without the engine interpreting them.

### 5.3 Batching and discovery

`OperationBatcher` coalesces operations on a fixed interval (16 ms default; failed flushes retain the batch). `MulticastDiscovery` / `BroadcastDiscovery` beacon a small UDP packet (peer id, name, session, connect port) with TTL-based expiry. A full mDNS/DNS-SD responder (`MdnsDiscovery`, feature `mdns`, built on the pure-Rust `mdns-sd` crate) is also available, implementing the same `Discovery` trait, for interop with non-`tpt-av-sync` mDNS tooling on the LAN — see §8.

## 6. Playhead synchronization (tpt-av-sync-playhead)

No I/O, no hidden clocks: all methods take timestamps as parameters, and the engine's time source is an injected `ClockFn` (making tests fully deterministic).

- **Clock sync** — NTP-style T1–T4 exchange (`ClockSyncMessage`): `rtt = (t4−t1)−(t3−t2)`, `offset = ((t2−t1)+(t3−t4))/2`. `ClockSynchronizer` keeps the lowest-rtt sample (jitter rejection).
- **Latency** — `LatencyEstimator` EWMA over rtt; one-way ≈ rtt/2; `compensate` shifts remote event times.
- **Drift** — `DriftCompensator` estimates skew (ppm) from successive offsets and rejects NTP-step-sized jumps; positions are extrapolated with the corrected rate between clock-sync rounds.
- **`PlayheadSync`** — master publishes positions stamped in its local domain; followers convert once with their offset and extrapolate `(now − capture) × sample_rate` with drift correction. `synchronized_position` / `set_local_position` are the RT hot path: arithmetic + one hash probe — no allocation, no locking, no syscalls.
- **Transport control** — `TransportSync` replicates play/stop/record/locate; followers apply, masters ignore (`Locate` preserves the playing/stopped state).

Deterministic precision tests (`tests/precision.rs`) simulate jittered networks with virtual clocks: sub-ms clock-offset accuracy, <1 ms playhead tracking at 48 kHz over a 4–9 ms link, and 40-ppm drift convergence. Criterion benchmarks pin hot-path throughput (`cargo bench -p tpt-av-sync-playhead`).

## 7. Presence (tpt-av-sync-presence)

`PresenceManager` holds local identity (`UserInfo`, avatar, accent `Color`), the local `CursorState` (playhead, selection, focused clip), and remote users' last-known state. Idle transitions (Online → Idle → Offline) are computed from injected timestamps (`IdleConfig`, `ActivityTracker`), never from wall-clock reads, so behavior is testable and replicable. `PresenceUpdate` is the wire message; a `leaving` flag marks graceful departure, and `handle_peer_leave` covers silent disconnects. Offline users keep their entry (UI can show "away") but expose no cursor.

## 8. Deviations from spec.txt

1. **Dual license.** The spec says "pure MIT"; the repository is dual-licensed **MIT OR Apache-2.0** (matching the Rust ecosystem convention and the todo checklist). `deny.toml` still enforces the no-copyleft rule.
2. **`TrimClip` carries absolute geometry.** The spec's `new_duration + edge` is ambiguous under reordering; the operation carries `(new_start_frame, new_duration, edge)` with the originator computing both values, keeping application order-independent.
3. **`UpdateTrackMetadata` added.** The spec only had track insert/delete; mute/solo/fader moves are core DAW interactions.
4. **Envelope granularity is whole-curve LWW** per `(target, envelope_type)`. Per-point merging produces noisy results for drawing automation; whole-curve LWW matches user expectations. The spec's `compute_delta`/`apply_delta` (§7.2) are implemented for state transfer.
5. **Splits are split *points*, not per-op materialization**, with lazy inheritance from the parent chain — this is what makes spec §5.1's "concurrent splits → 3 clips" hold under arbitrary delivery order (the naive per-op copy does not).
6. **Deletes require their target** and buffer when it is missing (the spec's "delete unknown = no-op" is not order-independent; see §4.3).
7. **`SyncMessage::Batch` added** for the spec's §7.1 batching; `Ack(OperationId)` unchanged.
8. **Discovery uses a custom UDP beacon** (multicast + broadcast) as the *default* instead of full mDNS/DNS-SD; same LAN function, far less machinery, no external dependency by default. A standard mDNS/DNS-SD responder is available as an opt-in (`mdns` feature, `MdnsDiscovery`) for interop with other mDNS-aware tooling on the LAN — see §11.
9. **Session metadata fields extended** (tempo, time signature) beyond name/sample-rate.
10. **WebRTC e2e integration test is `#[ignore]`** by default: ICE between host candidates requires unrestricted local UDP, which CI sandboxes and some host firewalls block. Run with `cargo test --features webrtc -- --ignored` on a permissive network. Signaling, channel registration, and framing are exercised by unit tests and the relay path.

## 9. Performance notes

- CRDT apply is O(log n) per op (BTreeMap registers); convergence work is bounded by delivered ops, not by re-merge sweeps.
- Snapshot size is O(operations) by default; `TimelineCrdt::compact()` (§11) drops operations no longer needed to reconstruct current state, bounding this for the dominant case (routine edits) — call it periodically on long-running sessions. Splits are exempt from compaction and remain O(splits ever performed).
- Playhead hot path: ~arithmetic-only (see benches). Target: comfortably under 100 ns per call on x86-64.
- Message path: bincode; batcher amortizes per-message overhead to ~1 frame at 60 fps edit streams.

## 10. Security considerations

Phase 7 landed transport security, peer authentication, and room authorization directly in the engine, in six independently-testable steps (`todo.md` §Phase 7 has the full breakdown):

- **Bounded deserialization + field validation.** `tpt_av_sync_utils::security::bounded_decode`/`decode_message` refuse to decode an oversized frame before touching the buffer; `TimelineOperation::validate()` bounds string lengths and envelope-point counts, enforced on `SyncEngine`'s inbound path and by the relay before forwarding/persisting.
- **Bounded persistence.** `SessionStore::open_with_limits` truncates/rotates a room's on-disk op-log past configured size/count limits — unbounded growth is no longer possible by default.
- **Admission control.** `ConnectionGuard` (global/per-IP/per-room caps), a per-connection `TokenBucket` rate limit, and an `Origin` allowlist (empty = LAN mode) guard the relay/signaling servers.
- **Transport encryption.** `tcp.rs`/`websocket.rs` support TLS-wrapped listen/connect via `rustls`, with self-signed certs + TOFU fingerprint pinning for LAN use or loaded PEM material for public deployment. Plaintext remains available through explicit, loudly-named opt-outs.
- **Peer authentication.** `PeerIdentity` (Ed25519; `PeerId` is derived from the public key, not self-asserted) proves ownership via a hello signature and liveness via a nonce challenge/response. `PROTOCOL_VERSION` (2) is enforced, not ignored.
- **Room authorization.** A room token never crosses the wire — only a keyed-hash proof (`room_token_proof`) does — and the relay/signaling servers bind a connection's peer id server-side from its first `Join` frame, rather than trusting a client-supplied `from` field on every subsequent frame. `RoomAuth::Open` is an explicit opt-out for LAN/trusted use.

Deployment still owns key distribution, room-token distribution, and network topology — the engine gives you the primitives, not a PKI or a directory service. Discovery beacons still announce peer name/session/port on the LAN; suppress discovery when that is sensitive.

## 11. Path to 1.0

- [x] Snapshot compaction / operation GC (bounded memory for week-scale sessions): `tpt_av_sync_crdt::compaction::compact` / `TimelineCrdt::compact()` drop operations no longer needed to reconstruct current state, found by exact `OperationId` lookup against each field's current LWW tag (no per-operation-kind logic needed for the common case). Splits are deliberately never compacted — see the module doc for why — so this bounds the dominant source of log growth (routine moves/trims/renames) without touching the trickiest, tag-agnostic part of the CRDT. Covered by a property test (`compaction_never_changes_materialized_state`, 256 randomized cases) plus targeted tests for tombstones and splits.
- [~] Vector-clock concurrency surfacing in the public API: `TimelineCrdt::take_resolution_events()` now reports genuine (vector-clock checked) concurrency for the three documented conflict classes — move, delete, split (`tpt-av-sync-crdt/src/merge.rs`, `ResolutionEvent`). Not yet generalized to every field (e.g. concurrent metadata-field writes aren't reported).
- [x] mDNS/DNS-SD responder option: `tpt_av_sync_net::mdns_discovery::MdnsDiscovery` (feature `mdns`, built on the pure-Rust `mdns-sd` crate), implementing the same `Discovery` trait as the default UDP-beacon discoveries. Peer identity/session travel in the service's TXT record. Since the mDNS daemon runs its own background thread that outlives a dropped handle, `Discovery::stop`/`Drop` explicitly unregister and shut it down.
- [ ] Long-run soak: 24 h simulated multi-peer session in CI.
