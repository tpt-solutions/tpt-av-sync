//! CRDT convergence properties wired to the shared `tpt-av-test` harness.
//!
//! `LwwReg` already carries proptest coverage in `property_ops.rs`; this
//! test runs the same invariant through `tpt-av-test-fuzz`'s
//! `assert_crdt_commutative` / `assert_crdt_idempotent`, so every TPT
//! repository expresses convergence with one shared vocabulary.

use tpt_av_sync_crdt::merge::{LwwReg, OpTag};
use tpt_av_sync_utils::PeerId;
use tpt_av_test_fuzz::crdt::{assert_crdt_commutative, assert_crdt_idempotent};

/// Applies one `(value, tag)` write to the register.
fn apply_write(mut register: LwwReg<String>, write: (String, OpTag)) -> LwwReg<String> {
    let (value, tag) = write;
    register.set(value, tag);
    register
}

#[test]
fn lww_register_writes_are_commutative() {
    let write_a = (
        "from peer 1".to_string(),
        OpTag::new(7, PeerId::from_u64(1)),
    );
    let write_b = (
        "from peer 2".to_string(),
        OpTag::new(9, PeerId::from_u64(2)),
    );

    // A then B must land on the same state as B then A.
    assert_crdt_commutative(LwwReg::new_initial("initial".into()), write_a, write_b, apply_write);
}

#[test]
fn lww_register_writes_are_idempotent() {
    let write = (
        "same edit twice".to_string(),
        OpTag::new(4, PeerId::from_u64(3)),
    );
    assert_crdt_idempotent(LwwReg::new_initial("initial".into()), write, apply_write);
}

#[test]
fn tied_tags_converge_regardless_of_arrival_order() {
    // Same Lamport timestamp from different peers: total order over
    // (lamport, peer) must still make A;B == B;A.
    let first = ("peer one wins ties".to_string(), OpTag::new(5, PeerId::from_u64(1)));
    let second = ("peer two wins ties".to_string(), OpTag::new(5, PeerId::from_u64(2)));
    assert_crdt_commutative(
        LwwReg::new_initial("initial".into()),
        first,
        second,
        apply_write,
    );
}
