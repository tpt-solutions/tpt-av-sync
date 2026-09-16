//! WebRTC transport integration over loopback: two transports exchange
//! signaling envelopes through a relay thread, then sync messages flow
//! over the data channel.
#![cfg(feature = "webrtc")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tpt_av_sync_net::webrtc::WebRtcTransport;
use tpt_av_sync_net::{SyncMessage, Transport};
use tpt_av_sync_utils::PeerId;

fn relay_signals(a: &WebRtcTransport, b: &WebRtcTransport, stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        if let Some(env) = a.take_outgoing_signal() {
            b.handle_signal(env).expect("handle signal");
            continue;
        }
        if let Some(env) = b.take_outgoing_signal() {
            a.handle_signal(env).expect("handle signal");
            continue;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
#[ignore = "requires unrestricted UDP ICE between host candidates; run manually with `cargo test --features webrtc -- --ignored`. Signaling, channel registration, and framing are all exercised when ICE completes (see DESIGN.md)."]
fn two_peers_connect_over_webrtc_loopback() {
    let mut ta = WebRtcTransport::new(PeerId::from_u64(1)).expect("transport a");
    let mut tb = WebRtcTransport::new(PeerId::from_u64(2)).expect("transport b");

    // Clones share connection state; the relay thread shuffles signaling
    // envelopes between the two peers.
    let relay_stop = Arc::new(AtomicBool::new(false));
    let relay = {
        let stop = relay_stop.clone();
        let ta_relay = ta.clone();
        let tb_relay = tb.clone();
        std::thread::spawn(move || relay_signals(&ta_relay, &tb_relay, &stop))
    };

    ta.dial(PeerId::from_u64(2)).expect("dial");

    // Wait for ICE + DTLS + SCTP: the data channel must be open on both
    // ends before any traffic can flow.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline
        && !(ta.is_ready(PeerId::from_u64(2)) && tb.is_ready(PeerId::from_u64(1)))
    {
        std::thread::sleep(Duration::from_millis(25));
    }
    relay_stop.store(true, Ordering::Relaxed);
    let _ = relay.join();

    assert!(
        ta.is_ready(PeerId::from_u64(2)),
        "A's data channel to B must open"
    );
    assert!(
        tb.is_ready(PeerId::from_u64(1)),
        "B's data channel to A must open"
    );

    // Exchange a message over the channel.
    ta.send_to(PeerId::from_u64(2), SyncMessage::RequestSnapshot)
        .expect("send over data channel");
    let deadline = Instant::now() + Duration::from_secs(5);
    let (from, msg) = loop {
        match tb.try_recv().expect("recv") {
            Some(item) => break item,
            None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            None => panic!("message must arrive over the data channel"),
        }
    };
    assert_eq!(from, PeerId::from_u64(1));
    assert_eq!(msg, SyncMessage::RequestSnapshot);
}
