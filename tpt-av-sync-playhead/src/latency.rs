//! Round-trip latency estimation and compensation.

/// Estimates one-way latency from rtt samples with an exponentially
/// weighted moving average.
///
/// A single rtt sample can be wildly unrepresentative (Wi-Fi retry bursts,
/// GC pauses); the EWMA smooths the estimate while still tracking real
/// network changes. All methods are allocation-free.
#[derive(Debug, Clone, Copy)]
pub struct LatencyEstimator {
    ewma_rtt_ms: f64,
    alpha: f64,
    samples: u32,
}

impl Default for LatencyEstimator {
    fn default() -> Self {
        Self::new(0.25)
    }
}

impl LatencyEstimator {
    /// Creates an estimator with smoothing factor `alpha` in `[0, 1]`.
    /// Smaller values smooth more; 0.25 is a good default for media
    /// collaboration traffic.
    #[must_use]
    pub fn new(alpha: f64) -> Self {
        Self {
            ewma_rtt_ms: 0.0,
            alpha: alpha.clamp(0.0, 1.0),
            samples: 0,
        }
    }

    /// Feeds one rtt sample (ms).
    pub fn on_rtt_sample(&mut self, rtt_ms: f64) {
        if self.samples == 0 {
            self.ewma_rtt_ms = rtt_ms;
        } else {
            self.ewma_rtt_ms += self.alpha * (rtt_ms - self.ewma_rtt_ms);
        }
        self.samples = self.samples.saturating_add(1);
    }

    /// The smoothed round-trip estimate in ms (0 before any samples).
    #[must_use]
    pub const fn rtt_ms(&self) -> f64 {
        self.ewma_rtt_ms
    }

    /// Estimated one-way latency in ms (half the rtt).
    #[must_use]
    pub fn one_way_ms(&self) -> f64 {
        self.ewma_rtt_ms / 2.0
    }

    /// Number of samples observed.
    #[must_use]
    pub const fn samples(&self) -> u32 {
        self.samples
    }

    /// Compensates a remote timestamp: estimates when the event actually
    /// happened on local clocks by subtracting one-way latency.
    ///
    /// `remote_event_ms` is the remote timestamp of the event; the caller
    /// is responsible for converting between clocks first (see
    /// [`crate::clock::ClockSynchronizer`]).
    #[must_use]
    pub fn compensate(&self, remote_event_ms: f64) -> f64 {
        remote_event_ms - self.ewma_rtt_ms / 2.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_smooths_spikes() {
        let mut est = LatencyEstimator::new(0.25);
        for _ in 0..10 {
            est.on_rtt_sample(20.0);
        }
        assert!((est.rtt_ms() - 20.0).abs() < f64::EPSILON);
        est.on_rtt_sample(200.0); // spike
        assert!(est.rtt_ms() > 20.0 && est.rtt_ms() < 70.0, "{}", est.rtt_ms());
        for _ in 0..20 {
            est.on_rtt_sample(20.0);
        }
        assert!(est.rtt_ms() < 25.0, "spike must wash out: {}", est.rtt_ms());
    }

    #[test]
    fn one_way_is_half_rtt() {
        let mut est = LatencyEstimator::default();
        est.on_rtt_sample(40.0);
        assert!((est.one_way_ms() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn compensate_subtracts_half_rtt() {
        let mut est = LatencyEstimator::default();
        est.on_rtt_sample(30.0);
        assert!((est.compensate(1_000.0) - 985.0).abs() < f64::EPSILON);
    }

    #[test]
    fn first_sample_seeds_estimate() {
        let mut est = LatencyEstimator::default();
        est.on_rtt_sample(80.0);
        assert!((est.rtt_ms() - 80.0).abs() < f64::EPSILON);
        assert_eq!(est.samples(), 1);
    }
}
