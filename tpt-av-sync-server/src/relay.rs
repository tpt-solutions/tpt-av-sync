//! Message relay: `SyncMessage`s routed by room, with a client-side
//! [`RelayClientTransport`] implementing the engine's [`Transport`] trait.
//!
//! The relay is the fallback path when direct peer-to-peer connections
//! (WebRTC) cannot be established: all traffic flows through the server,
//! which forwards frames by room. With a [`SessionStore`] attached the
//! relay also persists operations per room and answers `RequestSnapshot`
//! from that history, letting a lone joiner bootstrap.

use crate::limits::{origin_allowed, ConnectionGuard, ServerLimits, TokenBucket};
use tpt_av_sync_utils::room_token_proof;

/// Room admission policy for the relay/signaling servers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomAuth {
    /// Anyone may join (trusted/LAN deployments).
    Open,
    /// Peers must present a valid [`room_token_proof`] in their join frame.
    Token {
        /// The shared room secret.
        secret: String,
    },
}
use crate::persistence::SessionStore;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tpt_av_sync_crdt::{TaggedOperation, TimelineSnapshot};
use tpt_av_sync_net::{SyncMessage, Transport};
use tpt_av_sync_utils::wire;
use tpt_av_sync_utils::{PeerId, SyncError};
use tokio::net::TcpListener;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_tungstenite::tungstenite::http::{StatusCode};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// A relayed sync message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayFrame {
    /// The relay room.
    pub room: String,
    /// Sending peer.
    pub from: PeerId,
    /// Target peer, or `None` to fan out to the room (minus sender).
    pub to: Option<PeerId>,
    /// The payload.
    pub message: SyncMessage,
    /// Join proof (room authorization, B6): required as the FIRST frame
    /// when the server runs `RoomAuth::Token`.
    pub join: Option<RelayJoinProof>,
}

/// A room-join authorization proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayJoinProof {
    /// The joining peer (must match the frame's `from`).
    pub peer_id: PeerId,
    /// The room (must match the frame's `room`).
    pub room: String,
    /// `room_token_proof(secret, room)` under the room's secret.
    pub token_proof: [u8; 32],
}

#[derive(Default)]
struct RelayRoom {
    members: HashMap<PeerId, tokio_mpsc::UnboundedSender<Vec<u8>>>,
}

struct RelayInner {
    addr: SocketAddr,
    limits: ServerLimits,
    auth: RoomAuth,
    guard: Arc<ConnectionGuard>,
    rooms: Mutex<HashMap<String, RelayRoom>>,
    shutdown: AtomicBool,
    store: Mutex<Option<SessionStore>>,
    _rt: Arc<tokio::runtime::Runtime>,
}

/// A sync-message relay server.
#[derive(Clone)]
pub struct RelayServer {
    inner: Arc<RelayInner>,
}

impl RelayServer {
    /// Starts the relay on `bind`, optionally persisting operations per
    /// room into `store`, with default admission limits.
    pub fn serve(
        bind: SocketAddr,
        store: Option<SessionStore>,
    ) -> Result<(Self, SocketAddr), SyncError> {
        Self::serve_with_limits(bind, ServerLimits::default(), store)
    }

    /// Starts the relay with explicit admission limits (connection caps,
    /// rate limiting, Origin allowlist — see [`ServerLimits`]) and open
    /// room admission.
    pub fn serve_with_limits(
        bind: SocketAddr,
        limits: ServerLimits,
        store: Option<SessionStore>,
    ) -> Result<(Self, SocketAddr), SyncError> {
        Self::serve_full(bind, limits, RoomAuth::Open, store)
    }

    /// Starts the relay with full configuration, including room
    /// authorization (see [`RoomAuth`]).
    pub fn serve_full(
        bind: SocketAddr,
        limits: ServerLimits,
        auth: RoomAuth,
        store: Option<SessionStore>,
    ) -> Result<(Self, SocketAddr), SyncError> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| SyncError::transport(format!("tokio runtime: {e}")))?,
        );
        let listener = rt
            .block_on(async { TcpListener::bind(bind).await })
            .map_err(|e| SyncError::transport(format!("bind {bind}: {e}")))?;
        let addr = listener
            .local_addr()
            .map_err(|e| SyncError::transport(e.to_string()))?;

        let guard = ConnectionGuard::new(limits.clone());
        let inner = Arc::new(RelayInner {
            addr,
            limits,
            auth,
            guard,
            rooms: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            store: Mutex::new(store),
            _rt: rt.clone(),
        });

        let accept_inner = inner.clone();
        rt.spawn(async move {
            while !accept_inner.shutdown.load(Ordering::Relaxed) {
                let Ok((stream, peer_addr)) = listener.accept().await else {
                    break;
                };
                let task_inner = accept_inner.clone();
                tokio::spawn(async move {
                    // Admission control: global + per-IP caps.
                    let Some(_lease) = task_inner.guard.try_admit(peer_addr.ip()) else {
                        return;
                    };
                    // Origin allowlist (empty list = LAN mode, allow all).
                    let allowed = task_inner.limits.allowed_origins.clone();
                    // The `Err` variant's size is dictated by tungstenite's
                    // `Callback` trait (`Response<Option<String>>`), not by
                    // us; there's nothing here to box.
                    #[allow(clippy::result_large_err)]
                    let ws = tokio_tungstenite::accept_hdr_async(
                        stream,
                        move |req: &Request, resp: Response| {
                            let origin = req
                                .headers()
                                .get("Origin")
                                .and_then(|v| v.to_str().ok());
                            if origin_allowed(origin, &allowed) {
                                Ok(resp)
                            } else {
                                Err(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Some("origin not allowed".to_string()))
                                    .expect("static error response"))
                            }
                        },
                    )
                    .await;
                    let Ok(ws) = ws else {
                        return;
                    };
                    let mut bucket = TokenBucket::new(
                        task_inner.limits.rate_burst,
                        task_inner.limits.rate_refill_per_sec,
                    );
                    RelayServer::serve_connection(task_inner, ws, &mut bucket).await;
                });
            }
        });

        Ok((Self { inner }, addr))
    }

    /// The bound address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.inner.addr
    }

    /// Stops accepting connections.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);
    }

    /// Operations persisted for `room` (when a store is attached).
    #[must_use]
    pub fn persisted_ops(&self, room: &str) -> Vec<TaggedOperation> {
        let guard = self.inner.store.lock().expect("store lock");
        match &*guard {
            Some(store) => store.load_ops(room),
            None => Vec::new(),
        }
    }

    async fn serve_connection<S>(
        inner: Arc<RelayInner>,
        ws: tokio_tungstenite::WebSocketStream<S>,
        bucket: &mut TokenBucket,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sink, mut reader) = ws.split();
        let (tx, mut rx) = tokio_mpsc::unbounded_channel::<Vec<u8>>();
        let mut joined: Vec<(String, PeerId)> = Vec::new();
        // Set once the connection proves its identity+room (B6): frames
        // from any other identity are dropped.
        let mut bound: Option<(String, PeerId)> = None;

        let write_task = tokio::spawn(async move {
            while let Some(bytes) = rx.recv().await {
                if sink.send(WsMessage::Binary(bytes)).await.is_err() {
                    break;
                }
            }
        });

        while let Some(msg) = reader.next().await {
            let bytes = match msg {
                Ok(WsMessage::Binary(bytes)) => bytes,
                Ok(WsMessage::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            };
            if !bucket.try_take() {
                break; // sender over budget: close the connection
            }
            let Ok(frame) = tpt_av_sync_utils::security::decode_message::<RelayFrame>(&bytes)
            else {
                continue;
            };

            // Room authorization (B6): under token auth, the first frame
            // must be a verified join; afterwards only the bound identity
            // may speak.
            match (&inner.auth, bound.as_ref()) {
                (RoomAuth::Token { secret }, None) => {
                    let proof = frame.join.as_ref().ok_or_else(|| {
                        SyncError::transport("missing join proof under token auth")
                    });
                    let proof = match proof {
                        Ok(p) => p,
                        Err(_) => break,
                    };
                    if proof.room != frame.room
                        || proof.peer_id != frame.from
                        || proof.token_proof != room_token_proof(secret, &frame.room)
                    {
                        break; // bad proof: close
                    }
                    bound = Some((frame.room.clone(), frame.from));
                }
                (RoomAuth::Token { .. }, Some((bound_room, bound_peer))) => {
                    if *bound_room != frame.room || *bound_peer != frame.from {
                        break; // identity/room mismatch after binding
                    }
                }
                (RoomAuth::Open, _) => {}
            }

            // Any frame from a peer registers (or refreshes) its membership
            // in the addressed room — bounded by the room-member cap.
            {
                let mut rooms = inner.rooms.lock().expect("rooms lock");
                let room = rooms.entry(frame.room.clone()).or_default();
                let known = room.members.contains_key(&frame.from);
                if !known && room.members.len() >= inner.limits.max_room_members {
                    break; // room full: close the connection
                }
                if room.members.insert(frame.from, tx.clone()).is_none() {
                    joined.push((frame.room.clone(), frame.from));
                }
            }

            // Untrusted input: reject operations that violate structural
            // limits before forwarding or persisting them.
            if let SyncMessage::Operation(op) = &frame.message {
                if op.operation.validate().is_err() {
                    continue;
                }
            }

            match &frame.message {
                SyncMessage::Operation(op) => {
                    if let Some(store) = inner.store.lock().expect("store lock").as_ref() {
                        store.append_op(&frame.room, op);
                    }
                }
                // Snapshot requests are answered from persisted history so
                // a lone joiner can bootstrap from the server alone.
                SyncMessage::RequestSnapshot => {
                    let stored = inner
                        .store
                        .lock()
                        .expect("store lock")
                        .as_ref()
                        .map(|store| store.load_ops(&frame.room))
                        .unwrap_or_default();
                    if !stored.is_empty() {
                        if let Ok(reply_bytes) = wire::encode(&RelayFrame {
                            room: frame.room.clone(),
                            // Sent on behalf of the server itself (peer 0);
                            // clients drop frames that appear self-sent.
                            from: PeerId::from_u64(0),
                            to: Some(frame.from),
                            message: SyncMessage::Snapshot(TimelineSnapshot::from_ops(stored)),
                            join: None,
                        }) {
                            let member = inner
                                .rooms
                                .lock()
                                .expect("rooms lock")
                                .get(&frame.room)
                                .and_then(|room| room.members.get(&frame.from))
                                .cloned();
                            if let Some(member) = member {
                                let _ = member.send(reply_bytes);
                            }
                        }
                        continue;
                    }
                }
                _ => {}
            }

            let rooms = inner.rooms.lock().expect("rooms lock");
            if let Some(room) = rooms.get(&frame.room) {
                match frame.to {
                    Some(target) => {
                        if let Some(member) = room.members.get(&target) {
                            let _ = member.send(bytes.to_vec());
                        }
                    }
                    None => {
                        for (peer, member) in &room.members {
                            if *peer != frame.from {
                                let _ = member.send(bytes.to_vec());
                            }
                        }
                    }
                }
            }
        }

        for (room, peer) in joined {
            if let Some(room_mut) = inner.rooms.lock().expect("rooms lock").get_mut(&room) {
                room_mut.members.remove(&peer);
            }
        }
        write_task.abort();
    }
}

/// Client-side transport that tunnels `SyncMessage`s through a
/// [`RelayServer`]. Joins `room` as `local` upon construction.
pub struct RelayClientTransport {
    inner: Arc<RelayClientInner>,
}

struct RelayClientInner {
    local: PeerId,
    room: String,
    inbox: Mutex<mpsc::Receiver<(PeerId, SyncMessage)>>,
    outbound: tokio_mpsc::UnboundedSender<Vec<u8>>,
    peers: Arc<Mutex<Vec<PeerId>>>,
    _rt: Arc<tokio::runtime::Runtime>,
}

impl RelayClientTransport {
    /// Connects to the relay at `addr` and joins `room` (open admission).
    pub fn connect(
        addr: SocketAddr,
        room: impl Into<String>,
        local: PeerId,
    ) -> Result<Self, SyncError> {
        Self::connect_with_room_auth(addr, room, local, None)
    }

    /// Connects and joins `room` presenting a proof derived from the room
    /// secret (for servers running [`RoomAuth::Token`]).
    pub fn connect_with_room_auth(
        addr: SocketAddr,
        room: impl Into<String>,
        local: PeerId,
        room_secret: Option<&str>,
    ) -> Result<Self, SyncError> {
        let room = room.into();
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|e| SyncError::transport(format!("tokio runtime: {e}")))?,
        );
        let url = format!("ws://{addr}");
        let (outbound_tx, mut outbound_rx) = tokio_mpsc::unbounded_channel::<Vec<u8>>();
        let (inbound_tx, inbox_rx) = mpsc::channel();
        let peers: Arc<Mutex<Vec<PeerId>>> = Arc::new(Mutex::new(Vec::new()));

        let (ws, _resp) = rt
            .block_on(async { tokio_tungstenite::connect_async(&url).await })
            .map_err(|e| SyncError::transport(format!("relay connect {url}: {e}")))?;
        let (mut sink, mut reader) = ws.split();

        // Join the room (fan-out frame; also serves as presence and, in
        // token-auth mode, carries the join proof).
        let join_bytes = wire::encode(&RelayFrame {
            room: room.clone(),
            from: local,
            to: None,
            message: SyncMessage::RequestSnapshot,
            join: room_secret.map(|secret| RelayJoinProof {
                peer_id: local,
                room: room.clone(),
                token_proof: tpt_av_sync_utils::room_token_proof(secret, &room),
            }),
        })
        .map_err(|e| SyncError::serialization(e.to_string()))?;
        rt.block_on(async { sink.send(WsMessage::Binary(join_bytes)).await })
            .map_err(|e| SyncError::transport(e.to_string()))?;

        // One task owns the socket: outbound frames from the engine, inbound
        // frames to the engine.
        let reader_peers = peers.clone();
        let reader_inbound = inbound_tx;
        let reader_local = local;
        rt.spawn(async move {
            loop {
                tokio::select! {
                    maybe_out = outbound_rx.recv() => {
                        match maybe_out {
                            Some(bytes) => {
                                if sink.send(WsMessage::Binary(bytes)).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    maybe_in = reader.next() => {
                        match maybe_in {
                            Some(Ok(WsMessage::Binary(bytes))) => {
                                if bytes.len()
                                    <= tpt_av_sync_utils::security::MAX_WIRE_MESSAGE_BYTES
                                {
                                if let Ok(frame) = wire::decode::<RelayFrame>(&bytes) {
                                    if frame.from != reader_local {
                                        let mut known =
                                            reader_peers.lock().expect("peers lock");
                                        if !known.contains(&frame.from) {
                                            known.push(frame.from);
                                            known.sort();
                                        }
                                        drop(known);
                                        let _ = reader_inbound.send((frame.from, frame.message));
                                    }
                                }
                                }
                            }
                            Some(Ok(_)) => {}
                            Some(Err(_)) | None => break,
                        }
                    }
                }
            }
        });

        Ok(Self {
            inner: Arc::new(RelayClientInner {
                local,
                room,
                inbox: Mutex::new(inbox_rx),
                outbound: outbound_tx,
                peers,
                _rt: rt,
            }),
        })
    }

    /// The room this transport joined.
    #[must_use]
    pub fn room(&self) -> &str {
        &self.inner.room
    }

    fn frame(&self, message: SyncMessage, to: Option<PeerId>) -> Result<Vec<u8>, SyncError> {
        wire::encode(&RelayFrame {
            room: self.inner.room.clone(),
            from: self.inner.local,
            to,
            message,
            join: None,
        })
        .map_err(|e| SyncError::serialization(e.to_string()))
    }
}

impl Transport for RelayClientTransport {
    fn send_to(&mut self, peer_id: PeerId, message: SyncMessage) -> Result<(), SyncError> {
        self.inner
            .outbound
            .send(self.frame(message, Some(peer_id))?)
            .map_err(|_| SyncError::Disconnected)
    }

    fn broadcast(&mut self, message: SyncMessage) -> Result<(), SyncError> {
        // Unlike direct transports, an empty peer list is fine here: the
        // relay server is always a viable next hop (it persists and fans
        // out), so sends succeed while the connection is alive.
        self.inner
            .outbound
            .send(self.frame(message, None)?)
            .map_err(|_| SyncError::Disconnected)
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
        self.inner.peers.lock().expect("peers lock").clone()
    }
}
