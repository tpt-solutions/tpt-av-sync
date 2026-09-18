//! WebRTC data-channel transport (feature `webrtc`).
//!
//! Peers connect directly over ordered SCTP data channels — no server in
//! the data path. Signaling (SDP offers/answers and ICE candidates) is
//! surfaced to the application through [`SignalEnvelope`]s: the transport
//! emits envelopes via [`take_outgoing_signal`](Self::take_outgoing_signal)
//! and consumes delivered envelopes via
//! [`handle_signal`](Self::handle_signal). Any out-of-band channel works —
//! e.g. the WebSocket signaling server in `tpt-av-sync-server`.
//!
//! ICE candidates that arrive before the remote description is applied are
//! buffered, so envelope delivery ordering does not matter.

use crate::message::WireFrame;
use crate::transport::Transport;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tpt_av_sync_utils::{PeerId, SyncError, wire};
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidate;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::RTCPeerConnection;

use crate::SyncMessage;

/// The data channel name used by the engine.
pub const DATA_CHANNEL_LABEL: &str = "tpt-av-sync";

/// A signaling message to be relayed between peers by the application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalEnvelope {
    /// SDP offer from `from` to `to`.
    Offer {
        /// Sending peer.
        from: PeerId,
        /// Target peer.
        to: PeerId,
        /// The SDP blob.
        sdp: String,
    },
    /// SDP answer from `from` to `to`.
    Answer {
        /// Sending peer.
        from: PeerId,
        /// Target peer.
        to: PeerId,
        /// The SDP blob.
        sdp: String,
    },
    /// An ICE candidate from `from` for `to`.
    IceCandidate {
        /// Sending peer.
        from: PeerId,
        /// Target peer.
        to: PeerId,
        /// Candidate string.
        candidate: String,
        /// Media stream id (may be empty for some candidates).
        mid: String,
    },
}

/// One remote peer's connection state.
struct PeerConn {
    pc: Arc<RTCPeerConnection>,
    channel: Mutex<Option<Arc<RTCDataChannel>>>,
    /// ICE candidates that arrived before the remote description.
    pending_candidates: Mutex<Vec<webrtc::ice_transport::ice_candidate::RTCIceCandidateInit>>,
    /// Frames queued before the data channel opened.
    pending_writes: Mutex<Vec<Vec<u8>>>,
    open: AtomicBool,
}

struct WrtcInner {
    local: PeerId,
    api: webrtc::api::API,
    peers: Mutex<HashMap<PeerId, Arc<PeerConn>>>,
    inbox: Mutex<mpsc::Receiver<(PeerId, SyncMessage)>>,
    inbound_tx: mpsc::Sender<(PeerId, SyncMessage)>,
    outgoing: Mutex<std::collections::VecDeque<SignalEnvelope>>,
    shutdown: AtomicBool,
    _rt: Arc<tokio::runtime::Runtime>,
}

/// A WebRTC data-channel transport implementing [`Transport`].
#[derive(Clone)]
pub struct WebRtcTransport {
    inner: Arc<WrtcInner>,
}

impl std::fmt::Debug for WebRtcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebRtcTransport")
            .field("local", &self.inner.local)
            .finish_non_exhaustive()
    }
}

impl WebRtcTransport {
    /// Creates a transport. Tokio runs on a private runtime owned by the
    /// transport; ICE uses plain host candidates plus a public STUN server.
    pub fn new(local: PeerId) -> Result<Self, SyncError> {
        Self::build(local, false)
    }

    /// Creates a transport that only advertises `127.0.0.1` — for two
    /// peers on the same machine (tests, demos). Bypasses interface
    /// selection and firewall quirks on host adapters.
    pub fn loopback_only(local: PeerId) -> Result<Self, SyncError> {
        Self::build(local, true)
    }

    fn build(local: PeerId, loopback: bool) -> Result<Self, SyncError> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| SyncError::transport(format!("tokio runtime: {e}")))?,
        );

        let mut media = MediaEngine::default();
        media
            .register_default_codecs()
            .map_err(|e| SyncError::transport(format!("media engine: {e}")))?;

        // Plain host candidates (no `.local` mDNS names): collaborative
        // sessions run on LANs and loopbacks where mDNS resolution is not
        // guaranteed.
        let mut settings = SettingEngine::default();
        settings.set_interface_filter(Box::new(|_name| true));
        settings.set_ice_multicast_dns_mode(webrtc::ice::mdns::MulticastDnsMode::Disabled);
        if loopback {
            settings.set_nat_1to1_ips(
                vec!["127.0.0.1".to_string()],
                webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType::Host,
            );
        }

        let api = APIBuilder::new()
            .with_media_engine(media)
            .with_setting_engine(settings)
            .build();

        let (inbound_tx, inbox_rx) = mpsc::channel();
        Ok(Self {
            inner: Arc::new(WrtcInner {
                local,
                api,
                peers: Mutex::new(HashMap::new()),
                inbox: Mutex::new(inbox_rx),
                inbound_tx,
                outgoing: Mutex::new(std::collections::VecDeque::new()),
                shutdown: AtomicBool::new(false),
                _rt: rt,
            }),
        })
    }

    /// This peer's id.
    #[must_use]
    pub fn local_peer_id(&self) -> PeerId {
        self.inner.local
    }

    /// Whether the data channel to `peer` is open (ICE connected, DTLS
    /// finished, SCTP established). Sends to unready peers are queued.
    #[must_use]
    pub fn is_ready(&self, peer: PeerId) -> bool {
        self.inner
            .peers
            .lock()
            .expect("peers lock")
            .get(&peer)
            .map(|conn| conn.open.load(AtomicOrdering::Relaxed))
            .unwrap_or(false)
    }

    /// Takes the next signaling envelope that should be relayed to a
    /// remote peer (non-blocking).
    pub fn take_outgoing_signal(&self) -> Option<SignalEnvelope> {
        self.inner.outgoing.lock().ok()?.pop_front()
    }

    /// Handles a signaling envelope received from the remote side.
    pub fn handle_signal(&self, envelope: SignalEnvelope) -> Result<(), SyncError> {
        if envelope.to() != self.inner.local {
            return Ok(()); // not for us
        }
        let from = envelope.from();
        self.inner.rt_block_on(async move {
            match envelope {
                SignalEnvelope::Offer { sdp, .. } => {
                    self.answer_offer(from, sdp).await
                }
                SignalEnvelope::Answer { sdp, .. } => {
                    let offer_type = RTCSessionDescription::answer(sdp)
                        .map_err(|e| SyncError::transport(format!("bad answer sdp: {e}")))?;
                    let pc = self.pc_for(from)?;
                    pc.set_remote_description(offer_type)
                        .await
                        .map_err(|e| SyncError::transport(format!("set remote: {e}")))?;
                    self.flush_candidates(from).await;
                    Ok(())
                }
                SignalEnvelope::IceCandidate { candidate, mid, .. } => {
                    let init = webrtc::ice_transport::ice_candidate::RTCIceCandidateInit {
                        candidate,
                        sdp_mid: if mid.is_empty() { None } else { Some(mid) },
                        ..Default::default()
                    };
                    let has_remote = self
                        .pc_for(from)?
                        .remote_description()
                        .await
                        .is_some();
                    if has_remote {
                        self.pc_for(from)?
                            .add_ice_candidate(init)
                            .await
                            .map_err(|e| SyncError::transport(format!("add candidate: {e}")))?;
                    } else {
                        let conn = self.peers_entry(from)?;
                        conn.pending_candidates.lock().expect("cand lock").push(init);
                    }
                    Ok(())
                }
            }
        })
    }

    /// Dials `peer`: creates a peer connection and emits an
    /// [`SignalEnvelope::Offer`] for the application to relay.
    pub fn dial(&self, peer: PeerId) -> Result<(), SyncError> {
        self.inner.rt_block_on(async move {
            let pc = self.create_peer_connection(peer).await?;

            let channel = pc
                .create_data_channel(DATA_CHANNEL_LABEL, None)
                .await
                .map_err(|e| SyncError::transport(format!("data channel: {e}")))?;
            let conn = self.peers_entry(peer)?;
            *conn.channel.lock().expect("channel lock") = Some(channel.clone());

            let channel_for_open = channel.clone();
            let inner = self.inner.clone();
            let ready_peer = peer;
            channel.on_open(Box::new(move || {
                Box::pin(async move {
                    inner.on_channel_open(ready_peer, &channel_for_open);
                })
            }));
            let inner = self.inner.clone();
            let ready_peer = peer;
            channel.on_message(Box::new(move |msg: DataChannelMessage| {
                let inner = inner.clone();
                Box::pin(async move {
                    inner.dispatch_frame(ready_peer, &msg);
                })
            }));

            let offer = pc
                .create_offer(None)
                .await
                .map_err(|e| SyncError::transport(format!("offer: {e}")))?;
            pc.set_local_description(offer)
                .await
                .map_err(|e| SyncError::transport(format!("set local: {e}")))?;
            let sdp = pc
                .local_description()
                .await
                .ok_or_else(|| SyncError::transport("no local description"))?
                .sdp;
            self.emit(SignalEnvelope::Offer {
                from: self.inner.local,
                to: peer,
                sdp,
            });
            Ok(())
        })
    }

    // ---- async internals ----

    async fn answer_offer(&self, from: PeerId, sdp: String) -> Result<(), SyncError> {
        let offer = RTCSessionDescription::offer(sdp)
            .map_err(|e| SyncError::transport(format!("bad offer sdp: {e}")))?;
        let pc = self.create_peer_connection(from).await?;

        pc.set_remote_description(offer)
            .await
            .map_err(|e| SyncError::transport(format!("set remote: {e}")))?;
        self.flush_candidates(from).await;

        let answer = pc
            .create_answer(None)
            .await
            .map_err(|e| SyncError::transport(format!("answer: {e}")))?;
        pc.set_local_description(answer)
            .await
            .map_err(|e| SyncError::transport(format!("set local: {e}")))?;
        let sdp = pc
            .local_description()
            .await
            .ok_or_else(|| SyncError::transport("no local description"))?
            .sdp;
        self.emit(SignalEnvelope::Answer {
            from: self.inner.local,
            to: from,
            sdp,
        });
        Ok(())
    }

    async fn create_peer_connection(&self, peer: PeerId) -> Result<Arc<RTCPeerConnection>, SyncError> {
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let pc = self
            .inner
            .api
            .new_peer_connection(config)
            .await
            .map_err(|e| SyncError::transport(format!("peer connection: {e}")))?;
        let pc = Arc::new(pc);

        let conn = Arc::new(PeerConn {
            pc: pc.clone(),
            channel: Mutex::new(None),
            pending_candidates: Mutex::new(Vec::new()),
            pending_writes: Mutex::new(Vec::new()),
            open: AtomicBool::new(false),
        });
        self.inner
            .peers
            .lock()
            .expect("peers lock")
            .insert(peer, conn);

        // Outgoing ICE candidates go through signaling.
        let signal_inner = self.inner.clone();
        let signal_peer = peer;
        pc.on_ice_candidate(Box::new(move |candidate: Option<RTCIceCandidate>| {
            let inner = signal_inner.clone();
            let peer = signal_peer;
            Box::pin(async move {
                if let Some(candidate) = candidate {
                    if let Ok(init) = candidate.to_json() {
                        inner.emit(SignalEnvelope::IceCandidate {
                            from: inner.local,
                            to: peer,
                            candidate: init.candidate,
                            mid: init.sdp_mid.unwrap_or_default(),
                        });
                    }
                }
            })
        }));

        // Channels created by the remote side arrive here (answerer path).
        let channel_inner = self.inner.clone();
        let channel_peer = peer;
        pc.on_data_channel(Box::new(move |channel: Arc<RTCDataChannel>| {
            let inner = channel_inner.clone();
            let peer = channel_peer;
            Box::pin(async move {
                let conn = inner.peers.lock().expect("peers lock").get(&peer).cloned();
                if let Some(conn) = conn {
                    *conn.channel.lock().expect("channel lock") = Some(channel.clone());
                }
                let open_inner = inner.clone();
                let open_peer = peer;
                let open_channel = channel.clone();
                channel.on_open(Box::new(move || {
                    Box::pin(async move {
                        open_inner.on_channel_open(open_peer, &open_channel);
                    })
                }));
                let msg_inner = inner.clone();
                let msg_peer = peer;
                channel.on_message(Box::new(move |msg: DataChannelMessage| {
                    let inner = msg_inner.clone();
                    let peer = msg_peer;
                    Box::pin(async move {
                        inner.dispatch_frame(peer, &msg);
                    })
                }));
            })
        }));

        let state_inner = self.inner.clone();
        pc.on_peer_connection_state_change(Box::new(move |state: RTCPeerConnectionState| {
            let inner = state_inner.clone();
            Box::pin(async move {
                if state == RTCPeerConnectionState::Failed
                    || state == RTCPeerConnectionState::Closed
                {
                    // Keep the entry (channel is dead; sends will fail) so
                    // the engine observes the outage rather than a silent
                    // peer disappearance.
                    let _ = inner;
                }
            })
        }));

        Ok(pc)
    }

    fn pc_for(&self, peer: PeerId) -> Result<Arc<RTCPeerConnection>, SyncError> {
        self.inner
            .peers
            .lock()
            .expect("peers lock")
            .get(&peer)
            .map(|conn| conn.pc.clone())
            .ok_or(SyncError::PeerNotFound(peer))
    }

    fn peers_entry(&self, peer: PeerId) -> Result<Arc<PeerConn>, SyncError> {
        self.inner
            .peers
            .lock()
            .expect("peers lock")
            .get(&peer)
            .cloned()
            .ok_or(SyncError::PeerNotFound(peer))
    }

    async fn flush_candidates(&self, peer: PeerId) {
        let Ok(conn) = self.peers_entry(peer) else {
            return;
        };
        let pending: Vec<_> = std::mem::take(
            &mut *conn.pending_candidates.lock().expect("cand lock"),
        );
        for init in pending {
            let _ = conn.pc.add_ice_candidate(init).await;
        }
    }

    fn emit(&self, envelope: SignalEnvelope) {
        if let Ok(mut out) = self.inner.outgoing.lock() {
            out.push_back(envelope);
        }
    }

    /// Serializes and transmits `frame`, queueing it while the channel is
    /// still opening.
    fn send_frame(
        inner: &Arc<WrtcInner>,
        conn: &Arc<PeerConn>,
        frame: &WireFrame,
    ) -> Result<(), SyncError> {
        let bytes = wire::encode(frame)
            .map_err(|e| SyncError::serialization(e.to_string()))?;
        if !conn.open.load(AtomicOrdering::Relaxed) {
            conn.pending_writes.lock().expect("writes lock").push(bytes);
            return Ok(());
        }
        let channel = conn
            .channel
            .lock()
            .expect("channel lock")
            .clone()
            .ok_or_else(|| SyncError::transport("data channel missing"))?;
        inner
            ._rt
            .block_on(async move { channel.send(&::bytes::Bytes::from(bytes)).await })
            .map(|_| ())
            .map_err(|e| SyncError::transport(format!("channel send: {e}")))
    }
}

impl WrtcInner {
    fn rt_block_on<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        self._rt.block_on(fut)
    }

    fn emit(&self, envelope: SignalEnvelope) {
        if let Ok(mut out) = self.outgoing.lock() {
            out.push_back(envelope);
        }
    }

    fn on_channel_open(&self, peer: PeerId, channel: &Arc<RTCDataChannel>) {
        if let Some(conn) = self.peers.lock().expect("peers lock").get(&peer) {
            conn.open.store(true, AtomicOrdering::Relaxed);
            let pending: Vec<Vec<u8>> = std::mem::take(
                &mut *conn.pending_writes.lock().expect("writes lock"),
            );
            for bytes in pending {
                let _ = channel.send(&::bytes::Bytes::from(bytes));
            }
        }
    }

    fn dispatch_frame(&self, peer: PeerId, msg: &DataChannelMessage) {
        if msg.data.len() > tpt_av_sync_utils::security::MAX_WIRE_MESSAGE_BYTES {
            return;
        }
        if let Ok(WireFrame::Message(message)) = wire::decode::<WireFrame>(&msg.data) {
            let _ = self.inbound_tx.send((peer, message));
        }
    }
}

impl SignalEnvelope {
    /// The peer this envelope is addressed to.
    #[must_use]
    pub const fn to(&self) -> PeerId {
        match *self {
            SignalEnvelope::Offer { to, .. }
            | SignalEnvelope::Answer { to, .. }
            | SignalEnvelope::IceCandidate { to, .. } => to,
        }
    }

    /// The peer this envelope originated from.
    #[must_use]
    pub const fn from(&self) -> PeerId {
        match *self {
            SignalEnvelope::Offer { from, .. }
            | SignalEnvelope::Answer { from, .. }
            | SignalEnvelope::IceCandidate { from, .. } => from,
        }
    }
}

impl Transport for WebRtcTransport {
    fn send_to(&mut self, peer_id: PeerId, message: SyncMessage) -> Result<(), SyncError> {
        let conn = self.peers_entry(peer_id)?;
        WebRtcTransport::send_frame(&self.inner, &conn, &WireFrame::Message(message))
    }

    fn broadcast(&mut self, message: SyncMessage) -> Result<(), SyncError> {
        let conns: Vec<Arc<PeerConn>> = {
            self.inner
                .peers
                .lock()
                .expect("peers lock")
                .values()
                .cloned()
                .collect()
        };
        if conns.is_empty() {
            return Err(SyncError::transport("no connected peers"));
        }
        for conn in conns {
            WebRtcTransport::send_frame(&self.inner, &conn, &WireFrame::Message(message.clone()))?;
        }
        Ok(())
    }

    fn recv(&mut self) -> Result<(PeerId, SyncMessage), SyncError> {
        self.inner
            .inbox
            .lock()
            .expect("inbox lock")
            .recv()
            .map_err(|_| SyncError::Disconnected)
    }

    fn try_recv(&mut self) -> Result<Option<(PeerId, SyncMessage)>, SyncError> {
        match self.inner.inbox.lock().expect("inbox lock").try_recv() {
            Ok(item) => Ok(Some(item)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(SyncError::Disconnected),
        }
    }

    fn peers(&self) -> Vec<PeerId> {
        let mut ids: Vec<PeerId> = self
            .inner
            .peers
            .lock()
            .expect("peers lock")
            .keys()
            .copied()
            .collect();
        ids.sort();
        ids
    }
}

impl Drop for WebRtcTransport {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, AtomicOrdering::Relaxed);
        let conns: Vec<Arc<PeerConn>> = self
            .inner
            .peers
            .lock()
            .expect("peers lock")
            .values()
            .cloned()
            .collect();
        for conn in conns {
            let _ = self.inner.rt_block_on(conn.pc.close());
        }
    }
}
