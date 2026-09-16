//! Idle-timeout and activity indicator logic.
//!
//! Presence transitions are driven entirely by injected timestamps:
//!
//! - active input moves the user to [`PresenceState::Online`]
//! - `idle_after_ms` without activity → [`PresenceState::Idle`]
//! - `offline_after_ms` without activity → [`PresenceState::Offline`]

use crate::presence::PresenceState;

/// Timeouts controlling the Online → Idle → Offline transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdleConfig {
    /// Inactivity after which a user counts as idle (ms).
    pub idle_after_ms: u64,
    /// Inactivity after which a user counts as offline (ms).
    pub offline_after_ms: u64,
}

impl Default for IdleConfig {
    fn default() -> Self {
        Self {
            idle_after_ms: 30_000,
            offline_after_ms: 300_000,
        }
    }
}

impl IdleConfig {
    /// Computes the presence implied by the gap between `now_ms` and
    /// `last_active_ms`.
    #[must_use]
    pub const fn presence_at(&self, now_ms: u64, last_active_ms: u64) -> PresenceState {
        let idle_for = now_ms.saturating_sub(last_active_ms);
        if idle_for >= self.offline_after_ms {
            PresenceState::Offline
        } else if idle_for >= self.idle_after_ms {
            PresenceState::Idle
        } else {
            PresenceState::Online
        }
    }
}

/// Tracks the local user's last activity and derives their presence state.
#[derive(Debug, Clone)]
pub struct ActivityTracker {
    config: IdleConfig,
    last_active_ms: u64,
}

impl ActivityTracker {
    /// Creates a tracker; the user counts as active at `now_ms`.
    #[must_use]
    pub fn new(config: IdleConfig, now_ms: u64) -> Self {
        Self {
            config,
            last_active_ms: now_ms,
        }
    }

    /// Records user activity (input event, edit, cursor move…).
    pub const fn note_activity(&mut self, now_ms: u64) {
        self.last_active_ms = now_ms;
    }

    /// The last recorded activity time.
    #[must_use]
    pub const fn last_active_ms(&self) -> u64 {
        self.last_active_ms
    }

    /// The current presence state at `now_ms`.
    #[must_use]
    pub const fn presence_at(&self, now_ms: u64) -> PresenceState {
        self.config.presence_at(now_ms, self.last_active_ms)
    }

    /// The configuration in force.
    #[must_use]
    pub const fn config(&self) -> &IdleConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transitions_online_idle_offline() {
        let config = IdleConfig {
            idle_after_ms: 10,
            offline_after_ms: 100,
        };
        let mut tracker = ActivityTracker::new(config, 1_000);
        assert_eq!(tracker.presence_at(1_005), PresenceState::Online);
        assert_eq!(tracker.presence_at(1_010), PresenceState::Idle);
        assert_eq!(tracker.presence_at(1_100), PresenceState::Offline);

        // Activity resurrects an idle/offline user.
        tracker.note_activity(1_090);
        assert_eq!(tracker.presence_at(1_095), PresenceState::Online);
    }
}
