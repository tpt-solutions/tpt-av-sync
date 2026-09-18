//! Remote user cursors: what a user is looking at or selecting.

use serde::{Deserialize, Serialize};
use tpt_av_sync_crdt::ClipId;

/// A user's cursor state (what they are looking at or selecting).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct CursorState {
    /// Playhead position in frames, if visible to the user.
    pub playhead: Option<u64>,
    /// Selection range `(start, end)` in frames, if any.
    pub selection: Option<(u64, u64)>,
    /// The clip being hovered or edited.
    pub focused_clip: Option<ClipId>,
    /// When the cursor was captured (unix ms).
    pub timestamp_ms: u64,
}

impl CursorState {
    /// Creates a cursor state stamped with `timestamp_ms`.
    #[must_use]
    pub fn new(timestamp_ms: u64) -> Self {
        Self {
            timestamp_ms,
            ..Self::default()
        }
    }

    /// Builder: sets the playhead.
    #[must_use]
    pub const fn with_playhead(mut self, playhead: u64) -> Self {
        self.playhead = Some(playhead);
        self
    }

    /// Builder: sets the selection.
    #[must_use]
    pub const fn with_selection(mut self, start: u64, end: u64) -> Self {
        self.selection = Some((start, end));
        self
    }

    /// Builder: sets the focused clip.
    #[must_use]
    pub const fn with_focused_clip(mut self, clip: ClipId) -> Self {
        self.focused_clip = Some(clip);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_av_sync_utils::wire;

    #[test]
    fn builder_chain_and_serde() {
        let cursor = CursorState::new(123)
            .with_playhead(48_000)
            .with_selection(0, 960)
            .with_focused_clip(ClipId::from_u64(7));
        assert_eq!(cursor.playhead, Some(48_000));
        assert_eq!(cursor.selection, Some((0, 960)));
        assert_eq!(cursor.focused_clip, Some(ClipId::from_u64(7)));

        let bytes = wire::encode(&cursor).unwrap();
        let back: CursorState = wire::decode(&bytes).unwrap();
        assert_eq!(back, cursor);
    }
}
