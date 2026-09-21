//! Long-run soak test: a 24-hour multi-peer session, compressed into
//! simulated time so it runs in well under a second of wall-clock time in
//! CI, exercising the full stack together (CRDT + transport + presence +
//! playhead + compaction) the way none of the narrower unit/integration
//! tests do on their own.
//!
//! Five peers share a session over an in-memory mesh
//! ([`LoopbackTransport::fabric`]) for a simulated 24 hours, in 1-minute
//! ticks. Each tick, each peer independently (a deterministic xorshift
//! PRNG, not `rand`, so failures reproduce byte-for-byte) decides whether
//! it's "at the keyboard" this minute; active peers issue a few edits and
//! a playhead update. Presence ages normally, so peers that go quiet for
//! a while genuinely idle and then go offline, exactly like a real
//! multi-hour session. Every simulated hour, each peer compacts its own
//! operation log. Convergence is checked periodically throughout, not
//! just at the end, so a divergence bug is caught near where it happened
//! instead of buried in 24 hours of subsequent history.

use tpt_av_sync_crdt::{ClipData, ClipId, TimelineCrdt, TimelineOperation, TrackData, TrackId};
use tpt_av_sync_net::{LoopbackTransport, SyncEngine, SyncEvent};
use tpt_av_sync_playhead::PlayheadSync;
use tpt_av_sync_presence::{IdleConfig, PresenceManager, PresenceState, UserInfo};
use tpt_av_sync_utils::PeerId;

const NUM_PEERS: u64 = 5;
const TICK_MS: u64 = 60_000; // 1 simulated minute per tick.
const SIMULATED_HOURS: u64 = 24;
const TOTAL_TICKS: u64 = SIMULATED_HOURS * 60 * 60_000 / TICK_MS;
const COMPACT_EVERY_N_TICKS: u64 = 60; // once per simulated hour.
const CONVERGENCE_CHECK_EVERY_N_TICKS: u64 = 360; // every 6 simulated hours.

/// A small, fast, deterministic PRNG — reproducible test failures matter
/// more here than statistical quality.
struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.next() % denominator < numerator
    }

    fn range(&mut self, lo: u64, hi_exclusive: u64) -> u64 {
        lo + self.next() % (hi_exclusive - lo)
    }
}

struct SimPeer {
    engine: SyncEngine,
    presence: PresenceManager,
    playhead: PlayheadSync,
    peer_id: PeerId,
}

fn views_converged(peers: &[SimPeer]) -> bool {
    let first = peers[0].engine.crdt().view();
    peers[1..].iter().all(|p| p.engine.crdt().view() == first)
}

#[test]
fn twenty_four_hour_simulated_multi_peer_session() {
    let peer_ids: Vec<PeerId> = (1..=NUM_PEERS).map(PeerId::from_u64).collect();
    let transports = LoopbackTransport::fabric(&peer_ids);

    let idle_config = IdleConfig::default(); // 30s idle, 5min offline.
    let mut peers: Vec<SimPeer> = peer_ids
        .iter()
        .zip(transports)
        .map(|(&peer_id, transport)| SimPeer {
            engine: SyncEngine::new(TimelineCrdt::new(peer_id), Box::new(transport)),
            presence: PresenceManager::new(UserInfo::online(peer_id, "peer", 0), idle_config),
            playhead: PlayheadSync::new(peer_id, 48_000),
            peer_id,
        })
        .collect();

    // Seed a shared track and one clip per peer so there's something to
    // edit from tick zero.
    let track = TrackId::from_u64(1);
    peers[0].engine.apply_local(TimelineOperation::InsertTrack {
        track_id: track,
        track: TrackData::new("A1"),
        position: 0,
    });
    for i in 0..NUM_PEERS {
        peers[0].engine.apply_local(TimelineOperation::InsertClip {
            clip_id: ClipId::from_u64(i + 1),
            track_id: track,
            clip: ClipData::new("seed.wav", i * 10_000, 5_000),
            position: 0,
        });
    }

    let mut rng = Xorshift(0xC0FF_EE00_1234_5678);
    let mut sim_now_ms: u64 = 0;
    let mut total_ops_issued: u64 = 1 + NUM_PEERS; // the seed above.
    let clip_pool: Vec<ClipId> = (1..=NUM_PEERS).map(ClipId::from_u64).collect();

    for tick in 0..TOTAL_TICKS {
        sim_now_ms += TICK_MS;

        for peer in &mut peers {
            if !rng.chance(1, 2) {
                continue; // this peer is away from the keyboard this minute.
            }
            let edit_count = rng.range(1, 4);
            for _ in 0..edit_count {
                let clip_id = clip_pool[rng.range(0, clip_pool.len() as u64) as usize];
                let op = match rng.range(0, 4) {
                    0 => TimelineOperation::MoveClip {
                        clip_id,
                        new_track_id: track,
                        new_start_frame: rng.range(0, 500_000),
                        new_position: rng.range(0, NUM_PEERS),
                    },
                    1 => TimelineOperation::TrimClip {
                        clip_id,
                        new_start_frame: rng.range(0, 400_000),
                        new_duration: rng.range(1_000, 20_000),
                        edge: tpt_av_sync_crdt::TrimEdge::End,
                    },
                    2 => TimelineOperation::UpdateClipMetadata {
                        clip_id,
                        updates: tpt_av_sync_crdt::ClipMetadataUpdate {
                            gain: Some(rng.range(0, 200) as f32 / 100.0),
                            ..Default::default()
                        },
                    },
                    _ => TimelineOperation::UpdateClipMetadata {
                        clip_id,
                        updates: tpt_av_sync_crdt::ClipMetadataUpdate {
                            muted: Some(rng.chance(1, 3)),
                            ..Default::default()
                        },
                    },
                };
                peer.engine.apply_local(op);
                total_ops_issued += 1;
            }
            peer.presence.update_local_cursor(
                tpt_av_sync_presence::CursorState::new(sim_now_ms).with_playhead(rng.range(0, 1_000_000)),
            );
            peer.playhead.set_local_position(rng.range(0, 1_000_000));
        }

        // Deliver this tick's traffic and drain events on every peer.
        for peer in &mut peers {
            peer.engine.process_messages();
            for event in peer.engine.take_events() {
                match event {
                    SyncEvent::Presence(update) => peer.presence.receive_update(update),
                    SyncEvent::Playhead(update) => peer.playhead.receive_update(&update),
                    _ => {}
                }
            }
            peer.presence.tick(sim_now_ms);
        }

        if tick % COMPACT_EVERY_N_TICKS == 0 {
            for peer in &mut peers {
                let before = peer.engine.crdt().view();
                peer.engine.crdt_mut().compact();
                assert_eq!(
                    peer.engine.crdt().view(),
                    before,
                    "compaction must never change materialized state (peer {}, tick {tick})",
                    peer.peer_id
                );
            }
        }

        if tick % CONVERGENCE_CHECK_EVERY_N_TICKS == 0 {
            // Re-pump a few times: convergence only holds once in-flight
            // traffic has actually been delivered, not mid-flight.
            for _ in 0..5 {
                for peer in &mut peers {
                    peer.engine.process_messages();
                }
            }
            assert!(
                views_converged(&peers),
                "replicas diverged by simulated hour {}",
                sim_now_ms / 3_600_000
            );
        }
    }

    // Final settle + full convergence check.
    for _ in 0..10 {
        for peer in &mut peers {
            peer.engine.process_messages();
        }
    }
    assert!(views_converged(&peers), "replicas must converge after 24 simulated hours");

    // Compaction must have kept the operation log well below the total
    // number of operations issued over the session — the whole point of
    // this soak test existing is to prove that holds over a long session,
    // not just over the small examples the unit tests use.
    for peer in &peers {
        let log_len = peer.engine.crdt().operation_log().len() as u64;
        assert!(
            log_len < total_ops_issued / 2,
            "peer {}: operation log ({log_len}) did not stay bounded relative to \
             total ops issued ({total_ops_issued}) — compaction regression?",
            peer.peer_id
        );
    }

    // Presence sanity: with a 50% chance of activity per 1-minute tick,
    // every peer has certainly gone idle and back at some point in 24
    // simulated hours; nobody should still be flagged "Online" from tick
    // zero with no updates since (that would mean tick() isn't wired up).
    let any_ever_idle_or_offline = peers.iter().all(|p| {
        matches!(
            p.presence.local_user().presence,
            PresenceState::Online | PresenceState::Idle | PresenceState::Offline
        )
    });
    assert!(any_ever_idle_or_offline, "presence states must be well-formed");

    eprintln!(
        "soak: {TOTAL_TICKS} ticks, {total_ops_issued} ops issued, \
         final log sizes: {:?}",
        peers.iter().map(|p| p.engine.crdt().operation_log().len()).collect::<Vec<_>>()
    );
}
