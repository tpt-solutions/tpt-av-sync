//! Server-side admission control: connection caps, per-connection rate
//! limiting, and Origin allowlist checks.
//!
//! Shared by the relay and signaling servers. Defaults are generous for a
//! studio LAN; public deployments should tighten them and set an Origin
//! allowlist (and terminate TLS in front — see DESIGN.md §10).

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
#[cfg(test)]
use std::time::Duration;

/// Admission-control configuration for a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerLimits {
    /// Maximum simultaneously connected clients (global).
    pub max_connections: usize,
    /// Maximum simultaneously connected clients from one IP address.
    pub max_connections_per_ip: usize,
    /// Maximum members in one room.
    pub max_room_members: usize,
    /// Per-connection token-bucket capacity (frames).
    pub rate_burst: u32,
    /// Per-connection token refill rate (frames per second).
    pub rate_refill_per_sec: u32,
    /// Allowed `Origin` header values; empty allows everything (LAN mode).
    pub allowed_origins: Vec<String>,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_connections: 1_024,
            max_connections_per_ip: 64,
            max_room_members: 256,
            rate_burst: 512,
            rate_refill_per_sec: 256,
            allowed_origins: Vec::new(),
        }
    }
}

/// Tracks live connections globally and per source IP.
#[derive(Debug)]
pub struct ConnectionGuard {
    limits: ServerLimits,
    total: AtomicUsize,
    per_ip: Mutex<HashMap<IpAddr, usize>>,
}

/// An admitted connection. Dropping it releases the capacity.
#[derive(Debug)]
pub struct ConnectionLease {
    guard: Arc<ConnectionGuard>,
    ip: IpAddr,
    closed: bool,
}

impl ConnectionGuard {
    /// Creates a guard enforcing `limits`.
    #[must_use]
    pub fn new(limits: ServerLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            total: AtomicUsize::new(0),
            per_ip: Mutex::new(HashMap::new()),
        })
    }

    /// The limits in force.
    #[must_use]
    pub const fn limits(&self) -> &ServerLimits {
        &self.limits
    }

    /// Admits a connection from `ip`, or returns `None` when the global or
    /// per-IP cap is already saturated. Release by dropping the lease.
    pub fn try_admit(self: &Arc<Self>, ip: IpAddr) -> Option<ConnectionLease> {
        let mut per_ip = self.per_ip.lock().ok()?;
        if self.total.load(Ordering::Relaxed) >= self.limits.max_connections {
            return None;
        }
        if *per_ip.get(&ip).unwrap_or(&0) >= self.limits.max_connections_per_ip {
            return None;
        }
        self.total.fetch_add(1, Ordering::Relaxed);
        *per_ip.entry(ip).or_insert(0) += 1;
        Some(ConnectionLease {
            guard: self.clone(),
            ip,
            closed: false,
        })
    }

    /// Live connection count (global).
    #[must_use]
    pub fn live(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.guard.total.fetch_sub(1, Ordering::Relaxed);
        if let Ok(mut per_ip) = self.guard.per_ip.lock() {
            if let Some(count) = per_ip.get_mut(&self.ip) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    per_ip.remove(&self.ip);
                }
            }
        }
    }
}

/// Per-connection token bucket over inbound frames.
#[derive(Debug)]
pub struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    /// Creates a bucket starting full.
    #[must_use]
    pub fn new(capacity: u32, refill_per_sec: u32) -> Self {
        Self {
            tokens: f64::from(capacity),
            capacity: f64::from(capacity),
            refill_per_sec: f64::from(refill_per_sec),
            last_refill: Instant::now(),
        }
    }

    /// Refills by elapsed time and consumes one token, or returns `false`
    /// when the sender is over budget.
    pub fn try_take(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed.as_secs_f64() * self.refill_per_sec)
            .min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Checks a request's `Origin` against the allowlist. An empty allowlist
/// allows everything (LAN mode); with a list, missing or unmatched Origins
/// are rejected.
#[must_use]
pub fn origin_allowed(origin: Option<&str>, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    match origin {
        Some(origin) => allowed.iter().any(|a| a == origin),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_total: usize, per_ip: usize) -> ServerLimits {
        ServerLimits {
            max_connections: max_total,
            max_connections_per_ip: per_ip,
            ..ServerLimits::default()
        }
    }

    #[test]
    fn guard_enforces_global_and_per_ip_caps() {
        let guard = ConnectionGuard::new(limits(4, 2));
        let ip_a = "10.0.0.1".parse().unwrap();
        let ip_b = "10.0.0.2".parse().unwrap();

        let l1 = guard.try_admit(ip_a).expect("a1");
        let _l2 = guard.try_admit(ip_a).expect("a2");
        assert!(guard.try_admit(ip_a).is_none(), "per-ip cap");
        let _l3 = guard.try_admit(ip_b).expect("b1");
        assert_eq!(guard.live(), 3);
        let _l4 = guard.try_admit(ip_b).expect("b2");
        assert!(guard.try_admit(ip_b).is_none(), "global cap");
        drop(l1);
        assert!(guard.try_admit(ip_a).is_some(), "lease drop frees capacity");
    }

    #[test]
    fn bucket_allows_burst_then_throttles() {
        let mut bucket = TokenBucket::new(4, 1); // 4 burst, 1/s refill
        for _ in 0..4 {
            assert!(bucket.try_take(), "burst tokens");
        }
        assert!(!bucket.try_take(), "empty bucket throttles");
        std::thread::sleep(Duration::from_millis(50));
        assert!(!bucket.try_take(), "refill too slow within 50 ms");
    }

    #[test]
    fn origin_policy() {
        let allow = vec!["https://studio.example".to_string()];
        assert!(origin_allowed(Some("https://studio.example"), &allow));
        assert!(!origin_allowed(Some("https://evil.example"), &allow));
        assert!(!origin_allowed(None, &allow), "no header + list => reject");
        assert!(origin_allowed(Some("anything"), &[]), "empty list = LAN mode");
    }

}
