//! Session persistence: append-only operation logs per room.
//!
//! Used by the relay server to (a) durably record operations and
//! (b) replay them to joining peers, so sessions survive server restarts
//! and peers that connect after the editors went offline.
//!
//! Stores can be **bounded** ([`SessionStore::open_with_limits`]): when a
//! room's log exceeds the byte or operation limit, the log rotates — the
//! current file is moved aside (one retained generation) and a fresh one
//! starts. Reads span both generations, oldest first. Disk usage is
//! thereby capped at roughly twice the per-room byte limit.

use std::collections::HashMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tpt_av_sync_crdt::TaggedOperation;
use tpt_av_sync_utils::wire;
use tpt_av_sync_utils::SyncError;

/// Limits bounding one room's on-disk log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreLimits {
    /// Rotate when the current log would exceed this many bytes.
    pub max_bytes_per_room: u64,
    /// Rotate after this many operations in the current log.
    pub max_ops_per_room: u32,
}

impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_bytes_per_room: 64 * 1024 * 1024,
            max_ops_per_room: 1_000_000,
        }
    }
}

/// A directory-backed store of per-room operation logs.
#[derive(Debug)]
pub struct SessionStore {
    dir: PathBuf,
    limits: Option<StoreLimits>,
    /// Per-room operation counts for the *current* log segment.
    counts: Mutex<HashMap<String, u32>>,
}

impl SessionStore {
    /// Creates (or opens) an unbounded store rooted at `dir`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, SyncError> {
        Self::open_inner(dir, None)
    }

    /// Creates (or opens) a bounded store: rooms rotate when they exceed
    /// either limit. See the module docs for the rotation semantics.
    pub fn open_with_limits(
        dir: impl AsRef<Path>,
        limits: StoreLimits,
    ) -> Result<Self, SyncError> {
        Self::open_inner(dir, Some(limits))
    }

    fn open_inner(dir: impl AsRef<Path>, limits: Option<StoreLimits>) -> Result<Self, SyncError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .map_err(|e| SyncError::transport(format!("store {}: {e}", dir.display())))?;
        Ok(Self {
            dir,
            limits,
            counts: Mutex::new(HashMap::new()),
        })
    }

    fn stem_for(&self, room: &str) -> String {
        // Rooms are file-name safe by construction (callers control them);
        // sanitize anything unexpected anyway.
        room.chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect()
    }

    fn path_for(&self, room: &str) -> PathBuf {
        self.dir.join(format!("{}.opslog", self.stem_for(room)))
    }

    fn rotated_path_for(&self, room: &str) -> PathBuf {
        self.dir.join(format!("{}.opslog.1", self.stem_for(room)))
    }

    /// Appends one operation to the room's log (bincode-framed).
    ///
    /// With limits in force, the log rotates first if the frame would push
    /// it past [`StoreLimits::max_bytes_per_room`] or the segment already
    /// holds [`StoreLimits::max_ops_per_room`] operations.
    pub fn append_op(&self, room: &str, op: &TaggedOperation) {
        let Ok(mut bytes) = wire::encode(op) else {
            return;
        };
        let mut framed = (bytes.len() as u32).to_be_bytes().to_vec();
        framed.append(&mut bytes);

        if let Some(limits) = self.limits {
            let mut count = {
                let mut counts = self.counts.lock().expect("store counts");
                *counts.entry(room.to_string()).or_insert_with(|| {
                    // Resume counting from the current segment if the store
                    // was reopened.
                    count_frames(&self.path_for(room)).unwrap_or(0)
                })
            };
            let current_len = fs::metadata(self.path_for(room)).map(|m| m.len()).unwrap_or(0);
            if count >= limits.max_ops_per_room
                || current_len + framed.len() as u64 > limits.max_bytes_per_room
            {
                self.rotate(room);
                count = 0;
                counts_entry(&self.counts, room, 0);
            }
            count += 1;
            counts_entry(&self.counts, room, count);
        }

        if let Ok(mut file) = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path_for(room))
        {
            let _ = file.write_all(&framed);
        }
    }

    /// Moves the current log to the single retained rotated generation,
    /// discarding the previous one.
    fn rotate(&self, room: &str) {
        let current = self.path_for(room);
        let rotated = self.rotated_path_for(room);
        if rotated.exists() {
            let _ = fs::remove_file(&rotated);
        }
        if current.exists() {
            let _ = fs::rename(&current, &rotated);
        }
    }

    /// Loads every operation recorded for `room`, oldest first
    /// (rotated generation, then current).
    #[must_use]
    pub fn load_ops(&self, room: &str) -> Vec<TaggedOperation> {
        let mut ops = read_frames(&self.rotated_path_for(room));
        ops.extend(read_frames(&self.path_for(room)));
        ops
    }

    /// Rooms that have logs in this store.
    #[must_use]
    pub fn rooms(&self) -> Vec<String> {
        let mut rooms = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let name = name.strip_suffix(".opslog").unwrap_or(name);
                if !rooms.iter().any(|r: &String| r == name) {
                    rooms.push(name.to_string());
                }
            }
        }
        rooms.sort();
        rooms
    }
}

fn counts_entry(counts: &Mutex<HashMap<String, u32>>, room: &str, value: u32) {
    counts.lock().expect("store counts").insert(room.to_string(), value);
}

/// Counts length-prefixed frames in a log file (cheap scan of the prefix
/// bytes only — reads sequentially, keeps no payloads).
fn count_frames(path: &Path) -> Option<u32> {
    let bytes = fs::read(path).ok()?;
    let mut count = 0_u32;
    let mut cursor = 0_usize;
    while cursor + 4 <= bytes.len() {
        let len = u32::from_be_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        let end = cursor + 4 + len;
        if end > bytes.len() {
            break;
        }
        count += 1;
        cursor = end;
    }
    Some(count)
}

/// Decodes every complete frame in a log file, skipping a truncated tail.
fn read_frames(path: &Path) -> Vec<TaggedOperation> {
    let Ok(bytes) = fs::read(path) else {
        return Vec::new();
    };
    let mut ops = Vec::new();
    let mut cursor = 0_usize;
    while cursor + 4 <= bytes.len() {
        let len = u32::from_be_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        let start = cursor + 4;
        let end = start + len;
        if end > bytes.len() {
            break; // truncated tail (crash mid-write)
        }
        if let Ok(op) = wire::decode::<TaggedOperation>(&bytes[start..end]) {
            ops.push(op);
        }
        cursor = end;
    }
    ops
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tpt_av_sync_crdt::{ClipId, TimelineOperation};
    use tpt_av_sync_utils::{OperationId, PeerId, VectorClock};

    fn op(i: u64) -> TaggedOperation {
        let peer = PeerId::from_u64(1);
        TaggedOperation {
            op_id: OperationId::new(i, peer),
            operation: TimelineOperation::DeleteClip {
                clip_id: ClipId::from_u64(i),
            },
            lamport_ts: i,
            vector_clock: VectorClock::new(),
            peer_id: peer,
            timestamp: SystemTime::UNIX_EPOCH,
        }
    }

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("tpt-store-{}", PeerId::generate().as_u64()))
    }

    #[test]
    fn append_load_round_trip() {
        let dir = temp_dir();
        let store = SessionStore::open(&dir).unwrap();
        store.append_op("room-a", &op(1));
        store.append_op("room-a", &op(2));
        store.append_op("room-b", &op(3));

        let a = store.load_ops("room-a");
        assert_eq!(a.len(), 2);
        assert_eq!(a[0].lamport_ts, 1);
        assert_eq!(a[1].lamport_ts, 2);
        assert_eq!(store.load_ops("room-b").len(), 1);
        assert_eq!(store.rooms(), vec!["room-a", "room-b"]);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn limits_trigger_rotation_and_reads_span_generations() {
        let dir = temp_dir();
        let store = SessionStore::open_with_limits(
            &dir,
            StoreLimits {
                max_bytes_per_room: 4096,
                max_ops_per_room: 3,
            },
        )
        .unwrap();

        // Four ops in one room: the op-count limit forces a rotation after
        // the third.
        for i in 1..=4_u64 {
            store.append_op("ep", &op(i));
        }
        assert!(
            !store.path_for("ep").exists() || store.rotated_path_for("ep").exists(),
            "rotation must have produced a rotated generation"
        );

        let ops = store.load_ops("ep");
        assert_eq!(ops.len(), 4, "reads must span both generations");
        assert_eq!(ops[0].lamport_ts, 1, "oldest first");
        assert_eq!(ops[3].lamport_ts, 4);

        // The *current* segment restarted: appending again goes to a fresh
        // file whose count resumes independently.
        store.append_op("ep", &op(5));
        assert_eq!(store.load_ops("ep").len(), 5);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn byte_limit_caps_disk_usage_and_keeps_newest_window() {
        let dir = temp_dir();
        let store = SessionStore::open_with_limits(
            &dir,
            StoreLimits {
                max_bytes_per_room: 1024,
                max_ops_per_room: 10_000,
            },
        )
        .unwrap();

        for i in 1..=50_u64 {
            store.append_op("big", &op(i));
        }
        let total: u64 = ["big.opslog", "big.opslog.1"]
            .iter()
            .map(|name| fs::metadata(dir.join(name)).map(|m| m.len()).unwrap_or(0))
            .sum();
        // Two retained generations, each under the byte limit (plus at most
        // one overshoot frame).
        assert!(total <= 2048 + 256, "disk usage {total} not bounded");

        // Reads return the newest retained window, in order, ending at the
        // newest operation. Rotation drops older generations by design.
        let ops = store.load_ops("big");
        assert!(!ops.is_empty());
        assert_eq!(ops.last().unwrap().lamport_ts, 50);
        for pair in ops.windows(2) {
            assert!(pair[0].lamport_ts < pair[1].lamport_ts, "order kept");
        }
        fs::remove_dir_all(dir).ok();
    }
}
