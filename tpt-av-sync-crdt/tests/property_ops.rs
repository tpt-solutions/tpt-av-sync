//! Property-based tests (`proptest`): for arbitrary operation sequences,
//! the CRDT must converge under any delivery order (commutativity),
//! tolerate duplicate delivery (idempotency), and reproduce state from
//! snapshots.

use proptest::prelude::*;
use std::time::SystemTime;
use tpt_av_sync_crdt::{
    ClipData, ClipId, ClipMetadataUpdate, EnvelopePoint, EnvelopeType, SessionMetadataUpdate,
    TaggedOperation, TargetId, TimelineCrdt, TimelineOperation, TrackData, TrackId,
    TrackKind, TrackMetadataUpdate, TrimEdge,
};
use tpt_av_sync_utils::{OperationId, PeerId, VectorClock};

/// A deterministic xorshift shuffle so failures reproduce exactly.
fn shuffle<T>(v: &mut [T], seed: u64) {
    let mut state = seed | 1;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    if v.len() < 2 {
        return;
    }
    for i in (1..v.len()).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        v.swap(i, j);
    }
}

fn tagged(lamport: u64, peer: PeerId, op: TimelineOperation) -> TaggedOperation {
    TaggedOperation {
        op_id: OperationId::new(lamport, peer),
        operation: op,
        lamport_ts: lamport,
        vector_clock: VectorClock::new(),
        peer_id: peer,
        timestamp: SystemTime::UNIX_EPOCH,
    }
}

/// Stamps each op in the list with a unique `(lamport, peer)` tag.
/// Lamport equals the position in the list; peer alternates so
/// equal-lamport tie-breaks are exercised.
fn tag_ops(ops: &[TimelineOperation]) -> Vec<TaggedOperation> {
    ops.iter()
        .enumerate()
        .map(|(i, op)| tagged(i as u64 + 1, PeerId::from_u64((i % 2) as u64 + 1), op.clone()))
        .collect()
}

fn arb_op() -> impl Strategy<Value = TimelineOperation> {
    let clip_pool = 10_u64..15;
    let split_pool = 50_u64..55;
    let track_pool = 1_u64..4;
    prop_oneof![
        3 => (clip_pool.clone(), track_pool.clone(), 0_u64..500, 1_u64..2_000).prop_map(
            |(c, t, s, d)| TimelineOperation::InsertClip {
                clip_id: ClipId::from_u64(c),
                track_id: TrackId::from_u64(t),
                clip: ClipData::new("clip", s, d),
                position: 0,
            }
        ),
        3 => (clip_pool.clone(), track_pool.clone(), 0_u64..1_000).prop_map(
            |(c, t, s)| TimelineOperation::MoveClip {
                clip_id: ClipId::from_u64(c),
                new_track_id: TrackId::from_u64(t),
                new_start_frame: s,
                new_position: 0,
            }
        ),
        2 => clip_pool.clone().prop_map(|c| TimelineOperation::DeleteClip {
            clip_id: ClipId::from_u64(c),
        }),
        2 => (clip_pool.clone(), 1_u64..999, split_pool.clone()).prop_map(
            |(c, f, n)| TimelineOperation::SplitClip {
                clip_id: ClipId::from_u64(c),
                split_frame: f,
                new_clip_id: ClipId::from_u64(n),
            }
        ),
        2 => (clip_pool.clone(), 0_u64..300, 1_u64..1_500).prop_map(
            |(c, s, d)| TimelineOperation::TrimClip {
                clip_id: ClipId::from_u64(c),
                new_start_frame: s,
                new_duration: d,
                edge: TrimEdge::End,
            }
        ),
        2 => (clip_pool.clone(), 0_u32..1_000, any::<bool>()).prop_map(
            |(c, gain_x100, muted)| TimelineOperation::UpdateClipMetadata {
                clip_id: ClipId::from_u64(c),
                updates: ClipMetadataUpdate {
                    name: Some(format!("clip-{c}")),
                    gain: Some(gain_x100 as f32 / 100.0),
                    muted: Some(muted),
                    ..Default::default()
                },
            }
        ),
        2 => (track_pool.clone(), 0_u64..3).prop_map(
            |(t, p)| TimelineOperation::InsertTrack {
                track_id: TrackId::from_u64(t),
                track: TrackData {
                    name: format!("track-{t}"),
                    kind: TrackKind::Audio,
                    volume_db: 0.0,
                    muted: false,
                    solo: false,
                },
                position: p,
            }
        ),
        1 => (track_pool.clone(), -12_i32..12, any::<bool>()).prop_map(
            |(t, vol, solo)| TimelineOperation::UpdateTrackMetadata {
                track_id: TrackId::from_u64(t),
                updates: TrackMetadataUpdate {
                    volume_db: Some(vol as f32),
                    solo: Some(solo),
                    ..Default::default()
                },
            }
        ),
        1 => track_pool.clone().prop_map(|t| TimelineOperation::DeleteTrack {
            track_id: TrackId::from_u64(t),
        }),
        1 => (clip_pool.clone(), 0_u64..50, 0_u32..100).prop_map(
            |(c, frame, value_x100)| TimelineOperation::UpdateEnvelope {
                target_id: TargetId::Clip(ClipId::from_u64(c)),
                envelope_type: EnvelopeType::Volume,
                points: vec![EnvelopePoint {
                    frame,
                    value: value_x100 as f32 / 100.0,
                    interpolation: tpt_av_sync_crdt::Interpolation::Linear,
                }],
            }
        ),
        1 => prop_oneof![
            Just(TimelineOperation::UpdateSessionMetadata {
                updates: SessionMetadataUpdate {
                    name: Some("session".into()),
                    ..Default::default()
                }
            }),
            Just(TimelineOperation::UpdateSessionMetadata {
                updates: SessionMetadataUpdate {
                    sample_rate: Some(44_100),
                    tempo_bpm: Some(140.0),
                    ..Default::default()
                }
            }),
        ],
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Commutativity: any permutation of the same tagged operation set
    /// converges to the identical materialized state.
    #[test]
    fn convergence_under_arbitrary_reordering(
        ops in proptest::collection::vec(arb_op(), 0..40),
        seed in any::<u64>(),
    ) {
        let tagged_ops = tag_ops(&ops);
        let mut reference = TimelineCrdt::new(PeerId::from_u64(99));
        for op in &tagged_ops {
            reference.apply_remote(op.clone()).unwrap();
        }

        let mut permuted: Vec<TaggedOperation> = tagged_ops.clone();
        shuffle(&mut permuted, seed);
        let mut replica = TimelineCrdt::new(PeerId::from_u64(99));
        for op in &permuted {
            replica.apply_remote(op.clone()).unwrap();
        }

        prop_assert_eq!(reference.view(), replica.view());
    }

    /// Idempotency: duplicate delivery of every operation changes nothing.
    #[test]
    fn duplicate_delivery_is_idempotent(
        ops in proptest::collection::vec(arb_op(), 0..40),
    ) {
        let tagged_ops = tag_ops(&ops);
        let mut once = TimelineCrdt::new(PeerId::from_u64(99));
        let mut twice = TimelineCrdt::new(PeerId::from_u64(99));
        for op in &tagged_ops {
            once.apply_remote(op.clone()).unwrap();
        }
        // Deliver everything twice, in different orders.
        let mut permuted = tagged_ops.clone();
        shuffle(&mut permuted, 0xDEAD_BEEF);
        for op in tagged_ops.iter().chain(permuted.iter()) {
            twice.apply_remote(op.clone()).unwrap();
        }
        prop_assert_eq!(once.view(), twice.view());
        prop_assert_eq!(once.operation_log().len(), tagged_ops.len());
        prop_assert_eq!(twice.operation_log().len(), tagged_ops.len());
    }

    /// Snapshot round-trip: replaying the log reproduces the exact state,
    /// and merging a snapshot into a fresh replica converges.
    #[test]
    fn snapshot_replay_is_faithful(
        ops in proptest::collection::vec(arb_op(), 0..40),
    ) {
        let mut origin = TimelineCrdt::new(PeerId::from_u64(1));
        for op in tag_ops(&ops) {
            origin.apply_remote(op).unwrap();
        }
        let restored = TimelineCrdt::from_snapshot(origin.snapshot(), PeerId::from_u64(2));
        prop_assert_eq!(origin.view(), restored.view());

        let mut merger = TimelineCrdt::new(PeerId::from_u64(3));
        merger.merge_snapshot(&origin.snapshot());
        prop_assert_eq!(origin.view(), merger.view());
    }

    /// Compaction: for arbitrary operation streams (any mix of
    /// inserts/moves/deletes/splits/trims/metadata/envelope/session-meta
    /// edits), compacting must never change materialized state, and a
    /// fresh replica built only from the compacted log must converge to
    /// the same state as one built from the full history.
    #[test]
    fn compaction_never_changes_materialized_state(
        ops in proptest::collection::vec(arb_op(), 0..80),
    ) {
        let mut origin = TimelineCrdt::new(PeerId::from_u64(1));
        for op in tag_ops(&ops) {
            origin.apply_remote(op).unwrap();
        }
        let before = origin.view();

        let compacted_ops = tpt_av_sync_crdt::compact(origin.operation_log(), origin.session());
        prop_assert!(compacted_ops.len() <= origin.operation_log().len());

        let mut rebuilt = TimelineCrdt::new(PeerId::from_u64(2));
        for op in compacted_ops {
            rebuilt.apply_remote(op).unwrap();
        }
        prop_assert_eq!(rebuilt.view(), before.clone());

        // compact() itself (in place) must agree with the free function.
        origin.compact();
        prop_assert_eq!(origin.view(), before);
    }
}
