//! Clock-drift compensation over time.
//!
//! Two machines' clocks run at slightly different rates — typically tens of
//! parts per million (ppm). A clock offset measured once goes stale: after
//! an hour, 50 ppm is 180 ms of error, catastrophic for playhead alignment.
//! The compensator tracks how the offset *changes* over time and estimates
//! the skew rate, so positions can be corrected between clock-sync rounds.

/// Estimates the relative skew rate between two clocks (in ppm) from
/// successive offset measurements.
///
/// Given offsets `o1` at time `t1` and `o2` at time `t2` (same clock
/// domain for `t`), the rate is `(o2 - o1) / (t2 - t1)` — i.e. ms of drift
/// per ms of elapsed time, reported as ppm. The estimate is smoothed with
/// an EWMA. All methods are allocation-free.
#[derive(Debug, Clone, Copy)]
pub struct DriftCompensator {
    last_offset_ms: Option<f64>,
    last_time_ms: Option<f64>,
    skew_ppm_ewma: f64,
    alpha: f64,
    rate_samples: u32,
}

impl Default for DriftCompensator {
    fn default() -> Self {
        Self::new(0.2)
    }
}

impl DriftCompensator {
    /// Creates a compensator with EWMA smoothing factor `alpha` in `[0,1]`.
    #[must_use]
    pub fn new(alpha: f64) -> Self {
        Self {
            last_offset_ms: None,
            last_time_ms: None,
            skew_ppm_ewma: 0.0,
            alpha: alpha.clamp(0.0, 1.0),
            rate_samples: 0,
        }
    }

    /// Feeds a fresh offset measurement (`offset_ms` observed at `now_ms`,
    /// both in the same local time domain).
    pub fn on_offset_sample(&mut self, offset_ms: f64, now_ms: f64) {
        if let (Some(last_offset), Some(last_time)) = (self.last_offset_ms, self.last_time_ms) {
            let dt = now_ms - last_time;
            if dt > 0.0 {
                let ppm = ((offset_ms - last_offset) / dt) * 1_000_000.0;
                // Ignore absurd jumps (resyncs, NTP steps on the remote).
                if ppm.abs() < 10_000.0 {
                    self.skew_ppm_ewma = if self.rate_samples == 0 {
                        ppm
                    } else {
                        self.skew_ppm_ewma + self.alpha * (ppm - self.skew_ppm_ewma)
                    };
                    self.rate_samples = self.rate_samples.saturating_add(1);
                }
            }
        }
        self.last_offset_ms = Some(offset_ms);
        self.last_time_ms = Some(now_ms);
    }

    /// Current skew estimate in parts per million (positive: the remote
    /// clock runs faster than the local clock).
    #[must_use]
    pub const fn skew_ppm(&self) -> f64 {
        self.skew_ppm_ewma
    }

    /// Number of rate samples contributing to the estimate.
    #[must_use]
    pub const fn rate_samples(&self) -> u32 {
        self.rate_samples
    }

    /// Corrects an elapsed duration measured on the local clock so it
    /// corresponds to the remote clock's elapsed time.
    ///
    /// For a playhead that advances with the local clock but must match a
    /// remote master, pass the local elapsed ms and add the result.
    #[must_use]
    pub fn correct_elapsed_ms(&self, local_elapsed_ms: f64) -> f64 {
        local_elapsed_ms * (1.0 + self.skew_ppm_ewma / 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_constant_skew() {
        // Remote gains 0.05 ms per second (50 ppm).
        let mut drift = DriftCompensator::default();
        drift.on_offset_sample(10.0, 0.0);
        drift.on_offset_sample(10.05, 1_000.0);
        drift.on_offset_sample(10.10, 2_000.0);
        assert!((drift.skew_ppm() - 50.0).abs() < 1.0, "{}", drift.skew_ppm());
    }

    #[test]
    fn rejects_ntp_step_jumps() {
        let mut drift = DriftCompensator::default();
        drift.on_offset_sample(10.0, 0.0);
        drift.on_offset_sample(10.05, 1_000.0);
        // Massive step: NTP correction on the remote.
        drift.on_offset_sample(500.0, 2_000.0);
        // Skew estimate should be unaffected.
        assert!((drift.skew_ppm() - 50.0).abs() < 1.0, "{}", drift.skew_ppm());
    }

    #[test]
    fn correct_elapsed_applies_ppm() {
        let mut drift = DriftCompensator::default();
        drift.on_offset_sample(0.0, 0.0);
        drift.on_offset_sample(1.0, 1_000.0); // 1000 ppm
        let corrected = drift.correct_elapsed_ms(60_000.0);
        assert!((corrected - 60_060.0).abs() < 0.01, "{}", corrected);
    }

    #[test]
    fn no_rate_before_two_samples() {
        let mut drift = DriftCompensator::default();
        drift.on_offset_sample(3.0, 100.0);
        assert_eq!(drift.rate_samples(), 0);
        assert_eq!(drift.skew_ppm(), 0.0);
    }
}
