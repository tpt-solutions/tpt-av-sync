# minimal-sync-app

A [`cargo-generate`](https://github.com/cargo-generate/cargo-generate) template wiring up `tpt-av-sync-{utils,crdt,net,playhead,presence}` into a minimal working peer with a `SyncEngine`, presence, and playhead sync already connected — a starting point for a collaborative timeline app, not a finished one.

## Use it

```sh
cargo generate --path path/to/tpt-av-sync/templates/minimal-sync-app --name my-app
cd my-app
cargo run -- 127.0.0.1:9000                    # first peer
cargo run -- 127.0.0.1:9001 127.0.0.1:9000      # second peer, dials the first
```

You'll see both peers converge on the starter track/clip the first peer inserts, and log each other's edits as they arrive.

## What it wires up

- `TcpTransport` + `SyncEngine` — swap the transport for `WebsocketTransport`, `WebRtcTransport`, or `RelayClientTransport` as your deployment needs; nothing else in the template depends on which one you pick.
- `PresenceManager` — publishes local identity, receives remote presence via `SyncEvent::Presence`.
- `PlayheadSync` — receives remote playhead updates via `SyncEvent::Playhead`; wire `generate_update()`/`set_local_position()` to your own audio/render clock.

From here: replace the sleep loop in `main()` with your app's real event loop (audio callback, UI frame tick, ...), and call `engine.apply_local(...)` from your editing code instead of the hardcoded starter track/clip.

## Before this builds

The template's `Cargo.toml` points at `tpt-av-sync`'s `main` branch on GitHub (`git = "..."` dependencies), since the crates aren't on crates.io yet. Once they are, switch those to ordinary version dependencies.
