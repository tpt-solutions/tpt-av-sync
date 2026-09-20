//! WebRTC signaling: SDP offer/answer + ICE candidate exchange.
//!
//! Peers join a room and exchange [`SignalFrame`]s as JSON text frames.
//! The server never interprets SDP — it routes envelopes:
//! `to = Some(peer)` is a direct delivery, `to = None` fans out to the
//! rest of the room (minus the sender).

use crate::limits::{origin_allowed, ConnectionGuard, ServerLimits, TokenBucket};
use crate::relay::RoomAuth;
use tpt_av_sync_utils::room_token_proof;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tpt_av_sync_utils::{PeerId, SyncError};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// The payload of a signaling frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SignalPayload {
    /// Announce presence in the room. Under [`RoomAuth::Token`] the proof
    /// must match `room_token_proof(secret, room)`.
    Join {
        /// Proof-of-membership under the room secret (Open mode: `None`).
        token_proof: Option<[u8; 32]>,
    },
    /// Leave the room.
    Leave,
    /// An SDP offer.
    Offer {
        /// The SDP blob.
        sdp: String,
    },
    /// An SDP answer.
    Answer {
        /// The SDP blob.
        sdp: String,
    },
    /// An ICE candidate.
    IceCandidate {
        /// Candidate string.
        candidate: String,
        /// Media stream id (may be empty).
        mid: String,
    },
}

/// One signaling envelope routed by the server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalFrame {
    /// The signaling room.
    pub room: String,
    /// Sending peer.
    pub from: PeerId,
    /// Target peer, or `None` to fan out to the room.
    pub to: Option<PeerId>,
    /// The payload.
    pub payload: SignalPayload,
}

#[derive(Default)]
struct Room {
    members: HashMap<PeerId, tokio_mpsc::UnboundedSender<String>>,
}

/// A WebRTC signaling server.
#[derive(Clone)]
pub struct SignalingServer {
    inner: Arc<SignalingInner>,
}

struct SignalingInner {
    addr: SocketAddr,
    limits: ServerLimits,
    auth: RoomAuth,
    guard: Arc<ConnectionGuard>,
    rooms: Mutex<HashMap<String, Room>>,
    shutdown: AtomicBool,
    _rt: Arc<tokio::runtime::Runtime>,
}

impl SignalingServer {
    /// Starts the signaling server on `bind` with default admission
    /// limits. Returns the server handle and the bound address.
    pub fn serve(bind: SocketAddr) -> Result<(Self, SocketAddr), SyncError> {
        Self::serve_with_limits(bind, ServerLimits::default())
    }

    /// Starts the signaling server with explicit admission limits
    /// (connection caps, rate limiting, Origin allowlist) and open room
    /// admission.
    pub fn serve_with_limits(
        bind: SocketAddr,
        limits: ServerLimits,
    ) -> Result<(Self, SocketAddr), SyncError> {
        Self::serve_full(bind, limits, RoomAuth::Open)
    }

    /// Starts the signaling server with full configuration, including
    /// room authorization (see [`RoomAuth`]).
    pub fn serve_full(bind: SocketAddr, limits: ServerLimits, auth: RoomAuth) -> Result<(Self, SocketAddr), SyncError> {
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
        let inner = Arc::new(SignalingInner {
            addr,
            limits,
            auth,
            guard,
            rooms: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
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
                    SignalingServer::serve_connection(task_inner, ws, &mut bucket).await;
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

    /// Stops accepting new connections.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);
    }

    async fn serve_connection<S>(
        inner: Arc<SignalingInner>,
        ws: tokio_tungstenite::WebSocketStream<S>,
        bucket: &mut TokenBucket,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sink, mut reader) = ws.split();
        let (tx, mut rx) = tokio_mpsc::unbounded_channel::<String>();
        let mut joined: Vec<(String, PeerId)> = Vec::new();

        // Writer pump.
        let write_task = tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                if sink.send(WsMessage::Text(text)).await.is_err() {
                    break;
                }
            }
        });

        while let Some(msg) = reader.next().await {
            let text = match msg {
                Ok(WsMessage::Text(text)) => {
                    if !bucket.try_take() {
                        break; // sender over budget: close the connection
                    }
                    text
                }
                Ok(WsMessage::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            };
            let Ok(frame) = serde_json::from_str::<SignalFrame>(&text) else {
                continue;
            };
            match frame.payload {
                SignalPayload::Leave => break,
                SignalPayload::Join { token_proof } => {
                    // Room authorization (B6): under token auth the join
                    // must carry a valid proof.
                    if let RoomAuth::Token { secret } = &inner.auth {
                        match token_proof {
                            Some(proof)
                                if proof == room_token_proof(secret, &frame.room) => {}
                            _ => break, // missing or bad proof: close
                        }
                    }
                    let mut rooms = inner.rooms.lock().expect("rooms lock");
                    let room = rooms.entry(frame.room.clone()).or_default();
                    let known = room.members.contains_key(&frame.from);
                    if !known && room.members.len() >= inner.limits.max_room_members {
                        break; // room full: close the connection
                    }
                    room.members.insert(frame.from, tx.clone());
                    joined.push((frame.room.clone(), frame.from));
                }
                _ => {
                    let rooms = inner.rooms.lock().expect("rooms lock");
                    if let Some(room) = rooms.get(&frame.room) {
                        let text = text.clone();
                        match frame.to {
                            Some(target) => {
                                if let Some(member) = room.members.get(&target) {
                                    let _ = member.send(text);
                                }
                            }
                            None => {
                                for (peer, member) in &room.members {
                                    if *peer != frame.from {
                                        let _ = member.send(text.clone());
                                    }
                                }
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
