//! Network message types and the wire framing used by all transports.

use serde::{Deserialize, Serialize};
use tpt_av_sync_crdt::{TaggedOperation, TimelineSnapshot};
use tpt_av_sync_playhead::{ClockSyncMessage, PlayheadUpdate, TransportControl};
use tpt_av_sync_presence::PresenceUpdate;
use tpt_av_sync_utils::{OperationId, PeerId};

/// Protocol version negotiated in the handshake. Bumping this number breaks
/// wire compatibility; transports reject mismatched peers (enforced since
/// v2, which introduced peer-identity proofs and challenge/response).
pub const PROTOCOL_VERSION: u16 = 2;

/// A message exchanged between synced peers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SyncMessage {
    /// A timeline operation to replicate.
    Operation(TaggedOperation),
    /// Several operations batched together (see [`crate::batcher`]).
    Batch(Vec<TaggedOperation>),
    /// A request for the current state snapshot (sent by joining peers).
    RequestSnapshot,
    /// A state snapshot (sent to new peers, or on reconnect).
    Snapshot(TimelineSnapshot),
    /// Playhead position update.
    PlayheadUpdate(PlayheadUpdate),
    /// Transport control (play, stop, record, locate).
    TransportControl(TransportControl),
    /// Presence update (user info, cursor, leaving).
    PresenceUpdate(PresenceUpdate),
    /// Clock synchronization message (NTP-style).
    ClockSync(ClockSyncMessage),
    /// Acknowledgment that an operation was received and applied.
    Ack(OperationId),
}

/// The outermost wire frame of the framing transports (TCP).
///
/// WebSocket transports wrap [`SyncMessage`] directly (messages are
/// already delimited), while the TCP transport length-prefixes
/// [`WireFrame`]s.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireFrame {
    /// First frame on a newly established connection.
    Hello {
        /// Sender's peer id.
        peer_id: PeerId,
        /// Sender's protocol version.
        protocol: u16,
        /// Ed25519 ownership proof, present when the sender authenticates.
        identity: Option<tpt_av_sync_utils::PeerIdentityProof>,
    },
    /// Liveness challenge (authenticated mode): the dialer challenges the
    /// acceptor to sign a fresh nonce.
    Challenge {
        /// Random nonce.
        nonce: [u8; tpt_av_sync_utils::NONCE_LEN],
    },
    /// Response to a [`WireFrame::Challenge`].
    ChallengeResponse {
        /// Signature over the challenge context and nonce.
        signature: tpt_av_sync_utils::wire::Signature64,
    },
    /// A sync message.
    Message(SyncMessage),
    /// Sent before closing a connection cleanly.
    Goodbye,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_av_sync_utils::wire;
    use tpt_av_sync_crdt::{ClipData, ClipId, TimelineOperation, TrackId};
    use std::time::SystemTime;

    #[test]
    fn every_message_variant_roundtrips() {
        let messages = vec![
            SyncMessage::Operation(TaggedOperation {
                op_id: OperationId::new(1, PeerId::from_u64(2)),
                operation: TimelineOperation::InsertClip {
                    clip_id: ClipId::from_u64(3),
                    track_id: TrackId::from_u64(4),
                    clip: ClipData::new("a", 0, 1),
                    position: 0,
                },
                lamport_ts: 1,
                vector_clock: Default::default(),
                peer_id: PeerId::from_u64(2),
                timestamp: SystemTime::UNIX_EPOCH,
            }),
            SyncMessage::Batch(Vec::new()),
            SyncMessage::RequestSnapshot,
            SyncMessage::Snapshot(TimelineSnapshot::from_ops(Vec::new())),
            SyncMessage::PlayheadUpdate(PlayheadUpdate {
                peer_id: PeerId::from_u64(1),
                position: 10,
                timestamp_ms: 20,
                playing: false,
            }),
            SyncMessage::TransportControl(TransportControl::Play { position: 1 }),
            SyncMessage::PresenceUpdate(PresenceUpdate {
                peer_id: PeerId::from_u64(1),
                user: tpt_av_sync_presence::UserInfo::online(PeerId::from_u64(1), "x", 0),
                cursor: None,
                leaving: false,
            }),
            SyncMessage::ClockSync(ClockSyncMessage::request(PeerId::from_u64(1), 5)),
            SyncMessage::Ack(OperationId::new(9, PeerId::from_u64(9))),
        ];
        for msg in messages {
            let bytes = wire::encode(&msg).unwrap();
            let back: SyncMessage = wire::decode(&bytes).unwrap();
            assert_eq!(back, msg);
        }
    }

    #[test]
    fn wire_frame_roundtrips() {
        let frame = WireFrame::Hello {
            peer_id: PeerId::from_u64(7),
            protocol: PROTOCOL_VERSION,
            identity: None,
        };
        let bytes = wire::encode(&frame).unwrap();
        let back: WireFrame = wire::decode(&bytes).unwrap();
        assert_eq!(back, frame);
    }
}
