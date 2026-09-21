//! Smoke tests for the `inspect` and `replay` subcommands: runs the real
//! binary against a `SessionStore` populated with recorded operations and
//! checks its stdout.

use std::process::Command;
use tpt_av_sync_crdt::{ClipData, ClipId, TaggedOperation, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_server::SessionStore;
use tpt_av_sync_utils::{OperationId, PeerId, VectorClock};

fn tagged(lamport: u64, peer: PeerId, op: TimelineOperation) -> TaggedOperation {
    TaggedOperation {
        op_id: OperationId::new(lamport, peer),
        operation: op,
        lamport_ts: lamport,
        vector_clock: VectorClock::new(),
        peer_id: peer,
        timestamp: std::time::SystemTime::now(),
    }
}

fn populate_store(dir: &std::path::Path) {
    let store = SessionStore::open(dir).expect("open store");
    let peer = PeerId::from_u64(1);
    let track = TrackId::from_u64(1);
    let clip = ClipId::from_u64(1);
    store.append_op(
        "studio-a",
        &tagged(
            1,
            peer,
            TimelineOperation::InsertTrack {
                track_id: track,
                track: TrackData::new("A1"),
                position: 0,
            },
        ),
    );
    store.append_op(
        "studio-a",
        &tagged(
            2,
            peer,
            TimelineOperation::InsertClip {
                clip_id: clip,
                track_id: track,
                clip: ClipData::new("take1.wav", 0, 48_000),
                position: 0,
            },
        ),
    );
}

fn run_cli(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tpt-av-sync"))
        .args(args)
        .output()
        .expect("run cli")
}

/// A directory under the OS temp dir, uniquely named and removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("tpt-cli-test-{label}-{}", PeerId::generate().as_u64()));
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn inspect_lists_rooms_and_dumps_operations() {
    let dir = TempDir::new("inspect");
    populate_store(dir.path());
    let dir_str = dir.path().to_str().unwrap();

    let listing = run_cli(&["inspect", dir_str]);
    assert!(listing.status.success());
    let stdout = String::from_utf8_lossy(&listing.stdout);
    assert!(stdout.contains("studio-a: 2 operation(s)"), "{stdout}");

    let dump = run_cli(&["inspect", dir_str, "studio-a"]);
    assert!(dump.status.success());
    let stdout = String::from_utf8_lossy(&dump.stdout);
    assert!(stdout.contains("2 operation(s) total"), "{stdout}");
    assert!(stdout.contains("InsertClip"), "{stdout}");
}

#[test]
fn replay_reconstructs_the_timeline() {
    let dir = TempDir::new("replay");
    populate_store(dir.path());
    let dir_str = dir.path().to_str().unwrap();

    let output = run_cli(&["replay", dir_str, "studio-a"]);
    assert!(output.status.success(), "{:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("1 track(s)"), "{stdout}");
    assert!(stdout.contains("1 clip(s)"), "{stdout}");
    assert!(stdout.contains("take1.wav"), "{stdout}");
}

#[test]
fn missing_room_reports_a_clean_error() {
    let dir = TempDir::new("missing-room");
    populate_store(dir.path());
    let dir_str = dir.path().to_str().unwrap();

    let output = run_cli(&["replay", dir_str, "no-such-room"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no-such-room"), "{stderr}");
}
