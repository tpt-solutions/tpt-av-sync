//! Session persistence: append-only operation logs per room.
//!
//! Used by the relay server to (a) durably record operations and
//! (b) replay them to joining peers, so sessions survive server restarts
//! and peers that connect after the editors went offline.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use tpt_av_sync_crdt::TaggedOperation;
use tpt_av_sync_utils::SyncError;

/// A directory-backed store of per-room operation logs.
#[derive(Debug, Clone)]
pub struct SessionStore {
    dir: PathBuf,
}

impl SessionStore {
    /// Creates (or opens) a store rooted at `dir`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, SyncError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .map_err(|e| SyncError::transport(format!("store {}: {e}", dir.display())))?;
        Ok(Self { dir })
    }

    fn path_for(&self, room: &str) -> PathBuf {
        // Rooms are file-name safe by construction (callers control them);
        // sanitize anything unexpected anyway.
        let safe: String = room
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect();
        self.dir.join(format!("{safe}.opslog"))
    }

    /// Appends one operation to the room's log (bincode-framed).
    pub fn append_op(&self, room: &str, op: &TaggedOperation) {
        let Ok(mut bytes) = bincode::serialize(op) else {
            return;
        };
        let len = (bytes.len() as u32).to_be_bytes();
        let mut framed = len.to_vec();
        framed.append(&mut bytes);
        if let Ok(mut file) = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path_for(room))
        {
            let _ = file.write_all(&framed);
        }
    }

    /// Loads every operation recorded for `room`, in append order.
    #[must_use]
    pub fn load_ops(&self, room: &str) -> Vec<TaggedOperation> {
        let Ok(bytes) = fs::read(self.path_for(room)) else {
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
            if let Ok(op) = bincode::deserialize::<TaggedOperation>(&bytes[start..end]) {
                ops.push(op);
            }
            cursor = end;
        }
        ops
    }

    /// Rooms that have logs in this store.
    #[must_use]
    pub fn rooms(&self) -> Vec<String> {
        let mut rooms = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.path().file_stem().and_then(|s| s.to_str()) {
                    rooms.push(name.to_string());
                }
            }
        }
        rooms.sort();
        rooms
    }
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

    #[test]
    fn append_load_round_trip() {
        let dir = std::env::temp_dir().join(format!("tpt-store-{}", PeerId::generate().as_u64()));
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
}
