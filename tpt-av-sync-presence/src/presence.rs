//! The presence manager: local user state, remote users, and cursors.

use crate::activity::{ActivityTracker, IdleConfig};
use crate::avatar::AvatarData;
use crate::cursor::CursorState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tpt_av_sync_utils::PeerId;

/// A user's presence state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PresenceState {
    /// The user is online and active.
    Online,
    /// The user is online but has been inactive for a while.
    Idle,
    /// The user is offline (or disconnected).
    Offline,
}

/// Information about a user in the session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserInfo {
    /// The user's peer id.
    pub peer_id: PeerId,
    /// Display name.
    pub name: String,
    /// Avatar, if the user published one.
    pub avatar: Option<AvatarData>,
    /// Presence state.
    pub presence: PresenceState,
    /// When the user was last active (unix ms).
    pub last_active_ms: u64,
}

impl UserInfo {
    /// Creates user info in the [`PresenceState::Online`] state.
    #[must_use]
    pub fn online(peer_id: PeerId, name: impl Into<String>, now_ms: u64) -> Self {
        Self {
            peer_id,
            name: name.into(),
            avatar: None,
            presence: PresenceState::Online,
            last_active_ms: now_ms,
        }
    }

    /// Attaches avatar data (builder).
    #[must_use]
    pub fn with_avatar(mut self, avatar: AvatarData) -> Self {
        self.avatar = Some(avatar);
        self
    }
}

/// The wire message exchanged between peers to share presence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PresenceUpdate {
    /// The publishing peer.
    pub peer_id: PeerId,
    /// The publisher's user info (name, avatar, state).
    pub user: UserInfo,
    /// The publisher's cursor, if it has moved since the last update.
    pub cursor: Option<CursorState>,
    /// Set when the peer is leaving the session.
    pub leaving: bool,
}

/// A remote user and their cursor, as known locally.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteUser {
    /// The remote user's identity and state.
    pub info: UserInfo,
    /// The remote user's last cursor, if any.
    pub cursor: Option<CursorState>,
}

/// Presence and awareness manager.
///
/// Holds the local user's identity, cursor, and activity tracker, plus the
/// last known state of every remote user. The local presence is
/// recomputed lazily from the activity tracker on [`tick`](Self::tick) and
/// [`generate_update`](Self::generate_update).
#[derive(Debug, Clone)]
pub struct PresenceManager {
    local_peer: PeerId,
    local_user: UserInfo,
    tracker: ActivityTracker,
    cursor: Option<CursorState>,
    remotes: HashMap<PeerId, RemoteUser>,
}

impl PresenceManager {
    /// Creates a manager for the local user.
    #[must_use]
    pub fn new(local_user: UserInfo, idle_config: IdleConfig) -> Self {
        let local_peer = local_user.peer_id;
        let tracker = ActivityTracker::new(idle_config, local_user.last_active_ms);
        Self {
            local_peer,
            local_user,
            tracker,
            cursor: None,
            remotes: HashMap::new(),
        }
    }

    /// The local user's peer id.
    #[must_use]
    pub const fn local_peer(&self) -> PeerId {
        self.local_peer
    }

    /// Read-only view of the local user.
    #[must_use]
    pub fn local_user(&self) -> &UserInfo {
        &self.local_user
    }

    /// Attaches or replaces the local avatar.
    pub fn set_avatar(&mut self, avatar: AvatarData) {
        self.local_user.avatar = Some(avatar);
    }

    /// Renames the local user.
    pub fn set_name(&mut self, name: impl Into<String>) {
        self.local_user.name = name.into();
    }

    /// Updates the local cursor state and marks the user active.
    pub fn update_local_cursor(&mut self, cursor: CursorState) {
        self.cursor = Some(cursor);
        self.tracker.note_activity(cursor.timestamp_ms);
        self.local_user.last_active_ms = cursor.timestamp_ms;
    }

    /// Records local activity (any input), stamping `now_ms`.
    pub fn note_local_activity(&mut self, now_ms: u64) {
        self.tracker.note_activity(now_ms);
        self.local_user.last_active_ms = now_ms;
    }

    /// Applies a presence update from a remote peer.
    ///
    /// A `leaving` update marks the peer offline instead of removing the
    /// entry, so the UI can keep showing the user as "away".
    pub fn receive_update(&mut self, update: PresenceUpdate) {
        let mut info = update.user;
        if update.leaving {
            info.presence = PresenceState::Offline;
        }
        let entry = self.remotes.entry(update.peer_id).or_insert_with(|| {
            let _ = &info;
            RemoteUser {
                info: info.clone(),
                cursor: None,
            }
        });
        entry.info = info;
        if let Some(cursor) = update.cursor {
            entry.cursor = Some(cursor);
        }
    }

    /// Marks a remote peer gone (disconnect without a goodbye message).
    pub fn handle_peer_leave(&mut self, peer: PeerId) {
        if let Some(entry) = self.remotes.get_mut(&peer) {
            entry.info.presence = PresenceState::Offline;
        }
    }

    /// All tracked remote users (including offline ones).
    #[must_use]
    pub fn remote_users(&self) -> Vec<&UserInfo> {
        self.remotes.values().map(|r| &r.info).collect()
    }

    /// All remote cursors of users that are not offline.
    #[must_use]
    pub fn remote_cursors(&self) -> Vec<(&PeerId, &CursorState)> {
        self.remotes
            .iter()
            .filter(|(_, r)| r.info.presence != PresenceState::Offline)
            .filter_map(|(id, r)| r.cursor.as_ref().map(|c| (id, c)))
            .collect()
    }

    /// A specific remote user, if tracked.
    #[must_use]
    pub fn remote_user(&self, peer: &PeerId) -> Option<&RemoteUser> {
        self.remotes.get(peer)
    }

    /// Refreshes the local presence from the activity tracker and ages
    /// remote users by the same rules.
    pub fn tick(&mut self, now_ms: u64) {
        self.local_user.presence = self.tracker.presence_at(now_ms);
        self.local_user.last_active_ms = self.tracker.last_active_ms();
        let idle_after_ms = self.tracker.config().idle_after_ms;
        let offline_after_ms = self.tracker.config().offline_after_ms;
        let config = IdleConfig {
            idle_after_ms,
            offline_after_ms,
        };
        for entry in self.remotes.values_mut() {
            entry.info.presence = config.presence_at(now_ms, entry.info.last_active_ms);
        }
    }

    /// Builds the presence update this peer should broadcast.
    #[must_use]
    pub fn generate_update(&self) -> PresenceUpdate {
        PresenceUpdate {
            peer_id: self.local_peer,
            user: self.local_user.clone(),
            cursor: self.cursor,
            leaving: false,
        }
    }

    /// Builds the presence update announcing this peer is leaving.
    #[must_use]
    pub fn generate_leave_update(&mut self) -> PresenceUpdate {
        self.local_user.presence = PresenceState::Offline;
        PresenceUpdate {
            peer_id: self.local_peer,
            user: self.local_user.clone(),
            cursor: None,
            leaving: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::avatar::Color;

    fn alice() -> UserInfo {
        UserInfo::online(PeerId::from_u64(1), "Alice", 1_000)
            .with_avatar(AvatarData::from_url("https://example.test/a.png", Color::rgb(255, 0, 0)))
    }

    #[test]
    fn local_cursor_updates_and_generates_updates() {
        let mut mgr = PresenceManager::new(alice(), IdleConfig::default());
        let cursor = CursorState::new(1_500).with_playhead(48_000);
        mgr.update_local_cursor(cursor);

        let update = mgr.generate_update();
        assert_eq!(update.peer_id, PeerId::from_u64(1));
        assert_eq!(update.cursor, Some(cursor));
        assert_eq!(update.user.presence, PresenceState::Online);
        assert_eq!(mgr.local_user().last_active_ms, 1_500);
    }

    #[test]
    fn remote_updates_and_leave_round_trip() {
        let mut mgr = PresenceManager::new(alice(), IdleConfig::default());
        let bob = UserInfo::online(PeerId::from_u64(2), "Bob", 1_000);
        mgr.receive_update(PresenceUpdate {
            peer_id: bob.peer_id,
            user: bob.clone(),
            cursor: Some(CursorState::new(1_100).with_selection(0, 10)),
            leaving: false,
        });
        assert_eq!(mgr.remote_users().len(), 1);
        assert_eq!(mgr.remote_cursors().len(), 1);

        mgr.receive_update(PresenceUpdate {
            peer_id: bob.peer_id,
            user: bob,
            cursor: None,
            leaving: true,
        });
        assert_eq!(mgr.remote_users()[0].presence, PresenceState::Offline);
        assert_eq!(mgr.remote_cursors().len(), 0, "offline cursors are hidden");
    }

    #[test]
    fn peer_leave_marks_offline() {
        let mut mgr = PresenceManager::new(alice(), IdleConfig::default());
        let bob = UserInfo::online(PeerId::from_u64(2), "Bob", 1_000);
        mgr.receive_update(PresenceUpdate {
            peer_id: bob.peer_id,
            user: bob,
            cursor: None,
            leaving: false,
        });
        mgr.handle_peer_leave(PeerId::from_u64(2));
        assert_eq!(mgr.remote_users()[0].presence, PresenceState::Offline);
    }

    #[test]
    fn local_tick_goes_idle_then_offline() {
        let config = IdleConfig {
            idle_after_ms: 10,
            offline_after_ms: 100,
        };
        let mut mgr = PresenceManager::new(alice(), config);
        mgr.note_local_activity(1_000);
        mgr.tick(1_005);
        assert_eq!(mgr.local_user().presence, PresenceState::Online);
        mgr.tick(1_010);
        assert_eq!(mgr.local_user().presence, PresenceState::Idle);
        mgr.tick(1_200);
        assert_eq!(mgr.local_user().presence, PresenceState::Offline);
    }

    #[test]
    fn leave_update_announces_departure() {
        let mut mgr = PresenceManager::new(alice(), IdleConfig::default());
        let update = mgr.generate_leave_update();
        assert!(update.leaving);
        assert_eq!(update.user.presence, PresenceState::Offline);
    }

    #[test]
    fn presence_update_serde_roundtrip() {
        let mut mgr = PresenceManager::new(alice(), IdleConfig::default());
        mgr.update_local_cursor(CursorState::new(2_000).with_playhead(96_000));
        let update = mgr.generate_update();
        let bytes = bincode::serialize(&update).unwrap();
        let back: PresenceUpdate = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, update);
    }
}
