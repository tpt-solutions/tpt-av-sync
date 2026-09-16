//! Stress tests: convergence under large, duplicated, reordered, and
//! adversarially interleaved operation streams across many replicas.
//!
//! These complement the property tests in `property_ops.rs` with
//! deterministic heavy scenarios that run as ordinary tests in CI.

use std::time::SystemTime;

use tpt_av_sync_crdt::{
    ClipData, ClipId, TaggedOperation, TimelineCrdt, TimelineOperation, TrackData, TrackId,
    TrimEdge,
};
use tpt_av_sync_utils::{OperationId, PeerId, VectorClock};

fn tagged(lamport: u64, peer: u64, op: TimelineOperation) -> TaggedOperation {
    let peer = PeerId::from_u64(peer);
    TaggedOperation {
        op_id: OperationId::new(lamport, peer),
        operation: op,
        lamport_ts: lamport,
        vector_clock: VectorClock::new(),
        peer_id: peer,
        timestamp: SystemTime::UNIX_EPOCH,
    }
}

/// Deterministic xorshift shuffle.
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

/// Builds a long, varied editing session over a fixed id universe.
fn build_session_ops() -> Vec<TimelineOperation> {
    let mut ops = Vec::new();
    let tracks = [1_u64, 2, 3];
    for (i, t) in tracks.iter().enumerate() {
        ops.push(TimelineOperation::InsertTrack {
            track_id: TrackId::from_u64(*t),
            track: TrackData::new(format!("track-{t}")),
            position: i as u64,
        });
    }
    // 100 clips with inserts, moves, trims, metadata, splits, deletes.
    for i in 0..100_u64 {
        let clip = ClipId::from_u64(100 + i);
        let track = TrackId::from_u64(tracks[(i % 3) as usize]);
        ops.push(TimelineOperation::InsertClip {
            clip_id: clip,
            track_id: track,
            clip: ClipData::new(format!("clip-{i}"), i * 1_000, 10_000),
            position: i,
        });
        if i % 3 == 0 {
            ops.push(TimelineOperation::MoveClip {
                clip_id: clip,
                new_track_id: TrackId::from_u64(tracks[((i + 1) % 3) as usize]),
                new_start_frame: i * 1_500,
                new_position: i,
            });
        }
        if i % 5 == 0 {
            ops.push(TimelineOperation::TrimClip {
                clip_id: clip,
                new_start_frame: i * 1_500,
                new_duration: 4_000 + i,
                edge: TrimEdge::End,
            });
        }
        if i % 7 == 0 {
            ops.push(TimelineOperation::SplitClip {
                clip_id: clip,
                split_frame: 1_000 + i,
                new_clip_id: ClipId::from_u64(500 + i),
            });
        }
        if i % 11 == 0 {
            ops.push(TimelineOperation::DeleteClip { clip_id: clip });
        }
    }
    ops
}

#[test]
fn stress_five_replicas_random_delivery_order() {
    let ops = build_session_ops();
    let tagged_ops: Vec<TaggedOperation> = ops
        .iter()
        .enumerate()
        .map(|(i, op)| tagged(i as u64 + 1, (i % 3) as u64 + 1, op.clone()))
        .collect();

    // Reference replica: generation order.
    let mut reference = TimelineCrdt::new(PeerId::from_u64(9));
    for op in &tagged_ops {
        reference.apply_remote(op.clone()).unwrap();
    }

    // 5 replicas, each a different deterministic permutation.
    for seed in 1..=5_u64 {
        let mut permuted = tagged_ops.clone();
        shuffle(&mut permuted, seed * 7_919);
        let mut replica = TimelineCrdt::new(PeerId::from_u64(100 + seed));
        for op in &permuted {
            replica.apply_remote(op.clone()).unwrap();
        }
        assert_eq!(
            reference.view(),
            replica.view(),
            "replica with seed {seed} diverged"
        );
    }
}

#[test]
fn stress_heavy_duplicate_and_partial_delivery() {
    let ops = build_session_ops();
    let tagged_ops: Vec<TaggedOperation> = ops
        .iter()
        .enumerate()
        .map(|(i, op)| tagged(i as u64 + 1, (i % 2) as u64 + 1, op.clone()))
        .collect();

    let mut once = TimelineCrdt::new(PeerId::from_u64(1));
    for op in &tagged_ops {
        once.apply_remote(op.clone()).unwrap();
    }

    // A replica that receives everything three times in shuffled orders.
    let mut thrice = TimelineCrdt::new(PeerId::from_u64(2));
    for seed in [11_u64, 22, 33] {
        let mut batch = tagged_ops.clone();
        shuffle(&mut batch, seed);
        for op in &batch {
            thrice.apply_remote(op.clone()).unwrap();
        }
    }
    assert_eq!(once.view(), thrice.view());
    assert_eq!(
        once.operation_log().len(),
        thrice.operation_log().len(),
        "duplicates must not grow the log"
    );
}

#[test]
fn stress_snapshot_chain_round_trips() {
    // Snapshot → restore → more edits → snapshot again, across three
    // generations, must remain faithful.
    let mut gen1 = TimelineCrdt::new(PeerId::from_u64(1));
    let ops = build_session_ops();
    for (i, op) in ops.iter().enumerate() {
        let _ = gen1.apply_remote(tagged(i as u64 + 1, 1, op.clone()));
    }

    let gen2 = TimelineCrdt::from_snapshot(gen1.snapshot(), PeerId::from_u64(2));
    assert_eq!(gen1.view(), gen2.view());

    let track = TrackId::from_u64(1);
    let mut gen3 = TimelineCrdt::from_snapshot(gen2.snapshot(), PeerId::from_u64(3));
    gen3.apply_local(TimelineOperation::InsertTrack {
        track_id: TrackId::from_u64(99),
        track: TrackData::new("gen3"),
        position: 9,
    });
    gen3.apply_local(TimelineOperation::MoveClip {
        clip_id: ClipId::from_u64(100),
        new_track_id: track,
        new_start_frame: 123_456,
        new_position: 0,
    });

    let gen4 = TimelineCrdt::from_snapshot(gen3.snapshot(), PeerId::from_u64(4));
    assert_eq!(gen3.view(), gen4.view());
    assert_eq!(gen3.operation_log().len(), gen4.operation_log().len());
}
