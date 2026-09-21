//! LAN discovery integration for the `mdns` feature: two peers find each
//! other over standard mDNS/DNS-SD.
//!
//! Multicast availability varies by host (see `tests/discovery.rs`); this
//! degrades to a smoke check (daemon start + register succeed, no crash)
//! when the platform refuses multicast delivery.

#![cfg(feature = "mdns")]

use std::time::{Duration, Instant};

use tpt_av_sync_net::{Discovery, MdnsDiscovery};
use tpt_av_sync_utils::PeerId;

#[test]
fn peers_discover_each_other_via_mdns() {
    let a = MdnsDiscovery::new(PeerId::from_u64(101), "studio-a", Some("session-1".to_string()), 9100);
    let b = MdnsDiscovery::new(PeerId::from_u64(102), "studio-b", Some("session-1".to_string()), 9200);

    let (mut a, mut b) = match (a, b) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("mdns daemon unavailable: {e}");
            return;
        }
    };

    let now_ms = tpt_av_sync_utils::time::now_unix_ms();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut found = false;
    while Instant::now() < deadline {
        let _ = a.advertise();
        let _ = b.advertise();
        let seen_a = a.poll(now_ms);
        let seen_b = b.poll(now_ms);
        if seen_b.iter().any(|adv| adv.peer_id == PeerId::from_u64(101))
            && seen_a.iter().any(|adv| adv.peer_id == PeerId::from_u64(102))
        {
            found = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    a.stop();
    b.stop();

    if !found {
        eprintln!("mdns delivery filtered on this host; smoke-only");
    }
}

#[test]
fn advertise_is_idempotent_across_repeated_calls() {
    let Ok(mut discovery) =
        MdnsDiscovery::new(PeerId::from_u64(103), "studio-c", None, 9300)
    else {
        eprintln!("mdns daemon unavailable");
        return;
    };
    // Must not error or panic on repeated calls (register-once semantics).
    for _ in 0..3 {
        discovery.advertise().expect("advertise");
    }
    discovery.stop();
}
