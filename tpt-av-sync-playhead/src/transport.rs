//! Shared transport control (play / stop / record / locate) across peers.

use serde::{Deserialize, Serialize};
use tpt_av_sync_utils::PeerId;

/// The state of a transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportState {
    /// Stopped.
    Stopped,
    /// Playing.
    Playing,
    /// Recording.
    Recording,
}

/// A transport-control command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportControl {
    /// Start playback from a position.
    Play {
        /// Start position in frames.
        position: u64,
    },
    /// Stop playback parked at a position.
    Stop {
        /// Park position in frames.
        position: u64,
    },
    /// Move the playhead without changing state.
    Locate {
        /// Target position in frames.
        position: u64,
    },
    /// Start recording from a position.
    Record {
        /// Start position in frames.
        position: u64,
    },
}

impl TransportControl {
    /// The position the command refers to.
    #[must_use]
    pub const fn position(&self) -> u64 {
        match *self {
            TransportControl::Play { position }
            | TransportControl::Stop { position }
            | TransportControl::Locate { position }
            | TransportControl::Record { position } => position,
        }
    }

    /// The resulting transport state.
    #[must_use]
    pub const fn state(&self) -> TransportState {
        match self {
            TransportControl::Play { .. } => TransportState::Playing,
            TransportControl::Stop { .. } => TransportState::Stopped,
            TransportControl::Locate { .. } => TransportState::Stopped,
            TransportControl::Record { .. } => TransportState::Recording,
        }
    }
}

/// Tracks shared transport state across peers.
///
/// The master issues commands locally; followers apply remote commands and
/// ignore their own (or optionally reject remote commands while they hold
/// the token). This module is pure state — the networking lives in
/// `tpt-av-sync-net`.
#[derive(Debug, Clone)]
pub struct TransportSync {
    state: TransportState,
    position: u64,
    is_master: bool,
    master_peer: Option<PeerId>,
}

impl Default for TransportSync {
    fn default() -> Self {
        Self {
            state: TransportState::Stopped,
            position: 0,
            is_master: false,
            master_peer: None,
        }
    }
}

impl TransportSync {
    /// Creates a transport-sync state machine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current transport state.
    #[must_use]
    pub const fn state(&self) -> TransportState {
        self.state
    }

    /// Current parked/playing position.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Whether this peer commands the transport.
    #[must_use]
    pub const fn is_master(&self) -> bool {
        self.is_master
    }

    /// Sets master status.
    pub const fn set_master(&mut self, is_master: bool) {
        self.is_master = is_master;
    }

    /// The peer followed as transport master.
    #[must_use]
    pub const fn master_peer(&self) -> Option<PeerId> {
        self.master_peer
    }

    /// Local command: play. Returns the control message to broadcast.
    pub const fn play(&mut self, position: u64) -> TransportControl {
        self.state = TransportState::Playing;
        self.position = position;
        TransportControl::Play { position }
    }

    /// Local command: stop.
    pub const fn stop(&mut self, position: u64) -> TransportControl {
        self.state = TransportState::Stopped;
        self.position = position;
        TransportControl::Stop { position }
    }

    /// Local command: locate.
    ///
    /// Moves the playhead without changing the current state.
    pub const fn locate(&mut self, position: u64) -> TransportControl {
        self.position = position;
        TransportControl::Locate { position }
    }

    /// Local command: record.
    pub const fn record(&mut self, position: u64) -> TransportControl {
        self.state = TransportState::Recording;
        self.position = position;
        TransportControl::Record { position }
    }

    /// Applies a remote transport command.
    ///
    /// Returns `true` when the command was applied (this peer is a
    /// follower); a master ignores remote commands and returns `false`.
    /// `Locate` moves the playhead without touching the current state.
    pub fn on_remote_control(&mut self, control: &TransportControl, from: PeerId) -> bool {
        if self.is_master {
            return false;
        }
        self.master_peer = Some(from);
        self.position = control.position();
        if !matches!(control, TransportControl::Locate { .. }) {
            self.state = control.state();
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_av_sync_utils::wire;

    #[test]
    fn local_commands_update_state() {
        let mut t = TransportSync::new();
        let msg = t.play(1_000);
        assert_eq!(msg, TransportControl::Play { position: 1_000 });
        assert_eq!(t.state(), TransportState::Playing);
        assert_eq!(t.position(), 1_000);

        let msg = t.stop(2_000);
        assert_eq!(msg, TransportControl::Stop { position: 2_000 });
        assert_eq!(t.state(), TransportState::Stopped);

        t.record(500);
        assert_eq!(t.state(), TransportState::Recording);

        let msg = t.locate(9_000);
        assert_eq!(msg, TransportControl::Locate { position: 9_000 });
        assert_eq!(t.position(), 9_000);
    }

    #[test]
    fn followers_apply_remote_commands() {
        let mut a = TransportSync::new();
        let mut b = TransportSync::new();
        a.set_master(true);

        let cmd = a.play(4_000);
        assert!(b.on_remote_control(&cmd, PeerId::from_u64(1)));
        assert_eq!(b.state(), TransportState::Playing);
        assert_eq!(b.position(), 4_000);

        let cmd = a.stop(4_500);
        assert!(b.on_remote_control(&cmd, PeerId::from_u64(1)));
        assert_eq!(b.state(), TransportState::Stopped);
    }

    #[test]
    fn master_ignores_remote_commands() {
        let mut a = TransportSync::new();
        a.set_master(true);
        let cmd = TransportControl::Play { position: 1 };
        assert!(!a.on_remote_control(&cmd, PeerId::from_u64(2)));
        assert_eq!(a.state(), TransportState::Stopped);
    }

    #[test]
    fn control_serde_roundtrip() {
        for cmd in [
            TransportControl::Play { position: 1 },
            TransportControl::Stop { position: 2 },
            TransportControl::Locate { position: 3 },
            TransportControl::Record { position: 4 },
        ] {
            let bytes = wire::encode(&cmd).unwrap();
            let back: TransportControl = wire::decode(&bytes).unwrap();
            assert_eq!(back, cmd);
        }
    }
}
