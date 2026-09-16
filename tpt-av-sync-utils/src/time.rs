//! Network time synchronization types and time helpers.
//!
//! All wall-clock values in `tpt-av-sync` are `u64` **milliseconds since
//! the Unix epoch** unless stated otherwise.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the current wall-clock time as milliseconds since the Unix epoch.
#[must_use]
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A monotonic clock relative to an arbitrary epoch, safe for measuring
/// elapsed time across clock adjustments.
///
/// Wraps [`std::time::Instant`]; the epoch starts when `MonotonicClock::new`
/// is first called (or at the provided offset), so values are small and
/// stable within a process.
#[derive(Debug)]
pub struct MonotonicClock {
    start: std::time::Instant,
    offset_ms: u64,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl MonotonicClock {
    /// Creates a monotonic clock whose epoch starts now, reporting values
    /// shifted by `offset_ms`.
    #[must_use]
    pub fn new(offset_ms: u64) -> Self {
        Self {
            start: std::time::Instant::now(),
            offset_ms,
        }
    }

    /// Milliseconds elapsed on this clock since its epoch.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.offset_ms + self.start.elapsed().as_millis() as u64
    }

    /// Milliseconds elapsed since `earlier` was returned by [`now_ms`](Self::now_ms).
    ///
    /// Returns 0 if `earlier` is in the future of this clock.
    #[must_use]
    pub fn elapsed_since(&self, earlier: u64) -> u64 {
        self.now_ms().saturating_sub(earlier)
    }
}

/// The estimated relationship between the local clock and a reference
/// (master) clock, as computed by an NTP-style exchange.
///
/// `offset` is how much the *remote* clock is ahead of the local clock:
/// `remote_ms ≈ local_ms + offset_ms`. `rtt` is the measured round-trip
/// time; the smaller the rtt of the winning sample, the more trustworthy
/// the offset.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NetworkTime {
    /// Estimated clock offset in milliseconds (remote ahead of local).
    pub offset_ms: f64,
    /// Round-trip time of the sample the offset was derived from.
    pub rtt_ms: f64,
}

impl NetworkTime {
    /// Creates a network time estimate.
    #[must_use]
    pub const fn new(offset_ms: f64, rtt_ms: f64) -> Self {
        Self { offset_ms, rtt_ms }
    }

    /// Converts a remote-clock timestamp to an estimated local-clock
    /// timestamp.
    #[must_use]
    pub const fn to_local(&self, remote_ms: f64) -> f64 {
        remote_ms - self.offset_ms
    }

    /// Converts a local-clock timestamp to an estimated remote-clock
    /// timestamp.
    #[must_use]
    pub const fn to_remote(&self, local_ms: f64) -> f64 {
        local_ms + self.offset_ms
    }
}

/// A value paired with the (wall-clock) time it was captured at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timestamped<T> {
    /// The value.
    pub value: T,
    /// Unix milliseconds at which the value was captured.
    pub at_ms: u64,
}

impl<T> Timestamped<T> {
    /// Pairs `value` with the current wall-clock time.
    #[must_use]
    pub fn now(value: T) -> Self {
        Self {
            value,
            at_ms: now_unix_ms(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_unix_ms_is_plausible() {
        // ~2026-09. If this fails the machine clock is badly off.
        assert!(now_unix_ms() > 1_700_000_000_000);
    }

    #[test]
    fn monotonic_clock_measures_elapsed() {
        let clock = MonotonicClock::new(1_000);
        let t0 = clock.now_ms();
        std::thread::sleep(std::time::Duration::from_millis(15));
        let t1 = clock.now_ms();
        assert!(t0 >= 1_000);
        assert!(t1 - t0 >= 10, "expected >= 10ms elapsed, got {}", t1 - t0);
    }

    #[test]
    fn network_time_converts_both_directions() {
        let nt = NetworkTime::new(250.0, 10.0);
        assert!((nt.to_local(1_500.0) - 1_250.0).abs() < f64::EPSILON);
        assert!((nt.to_remote(1_250.0) - 1_500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn timestamped_now_pairs_value_with_time() {
        let stamped = Timestamped::now(42_u32);
        assert_eq!(stamped.value, 42);
        assert!(stamped.at_ms <= now_unix_ms());
    }
}
