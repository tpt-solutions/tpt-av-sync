//! {{project-name}}: a minimal `tpt-av-sync` collaborative app.
//!
//! This is a starting point, not a finished app: it wires up the sync
//! engine, presence, and a playhead so you have a running peer-to-peer
//! session to build a UI on top of. Run two copies to see it converge:
//!
//! ```sh
//! cargo run -- 127.0.0.1:9000
//! cargo run -- 127.0.0.1:9001 127.0.0.1:9000   # second peer dials the first
//! ```

use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_net::{SyncEngine, SyncEvent, TcpTransport};
use tpt_av_sync_playhead::PlayheadSync;
use tpt_av_sync_presence::{PresenceManager, UserInfo};
use tpt_av_sync_utils::PeerId;

/// Sample rate used for playhead position math (frames/second).
const SAMPLE_RATE: u32 = {{sample_rate}};

fn main() {
    let mut args = std::env::args().skip(1);
    let bind: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:0".to_string())
        .parse()
        .expect("first argument must be a bind address, e.g. 127.0.0.1:9000");
    let peer_addr: Option<SocketAddr> = args.next().map(|s| s.parse().expect("invalid peer address"));

    // 1. Identity and transport. Swap TcpTransport for WebsocketTransport,
    //    WebRtcTransport, or a RelayClientTransport as your deployment
    //    needs — SyncEngine only depends on the Transport trait.
    let local_peer = PeerId::generate();
    let (transport, addr) = TcpTransport::listen(bind, local_peer).expect("listen");
    println!("{{project-name}}: peer {local_peer} listening on {addr}");
    if let Some(peer_addr) = peer_addr {
        transport.connect(peer_addr).expect("connect to peer");
        println!("dialed {peer_addr}");
    }

    // 2. The CRDT + sync engine. `apply_local` is how *your* UI/editing
    //    code makes changes; `take_events()` after `process_messages()` is
    //    how you find out what changed remotely.
    let mut engine = SyncEngine::new(TimelineCrdt::new(local_peer), Box::new(transport));

    // 3. Presence: publish who you are so peers can show you in their UI.
    let mut presence = PresenceManager::new(
        UserInfo::online(local_peer, "anonymous", 0),
        Default::default(),
    );

    // 4. Playhead sync, for audio/video transport position — wire this to
    //    your actual clock/transport if you're building a media app.
    let mut playhead = PlayheadSync::new(local_peer, SAMPLE_RATE);

    // Starter content, so there's something to see converge. Delete this
    // once you have real UI-driven edits.
    if peer_addr.is_none() {
        let track = TrackId::from_u64(1);
        engine.apply_local(TimelineOperation::InsertTrack {
            track_id: track,
            track: TrackData::new("Track 1"),
            position: 0,
        });
        engine.apply_local(TimelineOperation::InsertClip {
            clip_id: ClipId::from_u64(1),
            track_id: track,
            clip: ClipData::new("example.wav", 0, SAMPLE_RATE as u64),
            position: 0,
        });
    }

    // 5. The event loop. In a real app this runs on your app's own timer
    //    (audio callback, UI frame tick, ...) instead of a sleep loop.
    loop {
        engine.process_messages();
        for event in engine.take_events() {
            match event {
                SyncEvent::PeerJoined(peer) => println!("peer joined: {peer}"),
                SyncEvent::PeerLeft(peer) => println!("peer left: {peer}"),
                SyncEvent::RemoteOperation(op) => {
                    println!("remote edit from {}: {:?}", op.peer_id, op.operation);
                }
                SyncEvent::Presence(update) => presence.receive_update(update),
                SyncEvent::Playhead(update) => playhead.receive_update(&update),
                _ => {}
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
}
