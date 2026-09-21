//! CRDT structure for automation envelopes.
//!
//! An envelope is stored as one last-writer-wins register per
//! `(target, envelope_type)` pair holding the *complete* point list.
//! Envelope editing is coarse-grained by design: parameters move constantly
//! (drawing automation), so per-point merging produces noisy results and
//! last-writer-wins of the whole curve matches user expectations. The
//! registers keep application commutative and idempotent like everything
//! else in the CRDT.

use crate::merge::{LwwReg, OpTag};
use crate::operation::{EnvelopePoint, EnvelopeType, TargetId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Store of all automation envelopes in a session.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnvelopeStore {
    envelopes: BTreeMap<(TargetId, EnvelopeType), LwwReg<Vec<EnvelopePoint>>>,
}

impl EnvelopeStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the points of an envelope (LWW).
    pub fn set(
        &mut self,
        target: TargetId,
        envelope_type: EnvelopeType,
        points: Vec<EnvelopePoint>,
        tag: OpTag,
    ) {
        self.envelopes
            .entry((target, envelope_type))
            .or_insert_with(|| LwwReg::new_initial(Vec::new()))
            .set(points, tag);
    }

    /// The current points of an envelope, if it has ever been set.
    #[must_use]
    pub fn get(&self, target: &TargetId, envelope_type: &EnvelopeType) -> Option<&[EnvelopePoint]> {
        self.envelopes
            .get(&(*target, envelope_type.clone()))
            .map(|reg| reg.get().as_slice())
    }

    /// Iterates over all envelopes that have been set.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&TargetId, &EnvelopeType, &[EnvelopePoint])> {
        self.envelopes.iter().map(|((t, e), reg)| (t, e, reg.get().as_slice()))
    }

    /// The LWW tag of each stored envelope's last write (for compaction:
    /// see `crate::compaction`).
    pub fn tags(&self) -> impl Iterator<Item = OpTag> + '_ {
        self.envelopes.values().map(LwwReg::tag)
    }

    /// Number of stored envelopes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.envelopes.len()
    }

    /// True when no envelopes have been set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.envelopes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{ClipId, TrackId};

    fn tag(l: u64) -> OpTag {
        OpTag::new(l, tpt_av_sync_utils::PeerId::from_u64(1))
    }

    fn pt(frame: u64, value: f32) -> EnvelopePoint {
        EnvelopePoint {
            frame,
            value,
            interpolation: crate::operation::Interpolation::Linear,
        }
    }

    #[test]
    fn higher_tag_replaces_points() {
        let mut store = EnvelopeStore::new();
        let target = TargetId::Clip(ClipId::from_u64(1));
        store.set(target, EnvelopeType::Volume, vec![pt(0, 0.0)], tag(1));
        store.set(target, EnvelopeType::Volume, vec![pt(0, 0.5), pt(10, 1.0)], tag(2));
        store.set(target, EnvelopeType::Volume, vec![pt(0, 0.9)], tag(1));
        assert_eq!(
            store.get(&target, &EnvelopeType::Volume),
            Some(&[pt(0, 0.5), pt(10, 1.0)][..])
        );
    }

    #[test]
    fn concurrent_writes_converge_regardless_of_order() {
        let target = TargetId::Track(TrackId::from_u64(3));
        let mut a = EnvelopeStore::new();
        let mut b = EnvelopeStore::new();
        let early = vec![pt(0, 0.0)];
        let late = vec![pt(5, 1.0)];
        a.set(target, EnvelopeType::Pan, early.clone(), tag(4));
        a.set(target, EnvelopeType::Pan, late.clone(), tag(6));
        b.set(target, EnvelopeType::Pan, late.clone(), tag(6));
        b.set(target, EnvelopeType::Pan, early, tag(4));
        assert_eq!(a.get(&target, &EnvelopeType::Pan), b.get(&target, &EnvelopeType::Pan));
    }

    #[test]
    fn different_envelope_types_are_independent() {
        let mut store = EnvelopeStore::new();
        let target = TargetId::Clip(ClipId::from_u64(2));
        store.set(target, EnvelopeType::Volume, vec![pt(0, 1.0)], tag(1));
        store.set(
            target,
            EnvelopeType::Custom("reverb-mix".into()),
            vec![pt(0, 0.2)],
            tag(2),
        );
        assert_eq!(store.len(), 2);
        assert_eq!(store.get(&target, &EnvelopeType::Mute), None);
    }
}
