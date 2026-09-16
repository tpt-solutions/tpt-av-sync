//! WebSocket transport (`tokio-tungstenite`, server-based).
//!
//! A `WebsocketTransport` either listens for peers
//! ([`WebsocketTransport::serve`]) or dials out
//! ([`WebsocketTransport::connect`]); a full mesh is built by combining
//! both. Each connection performs a [`WireFrame::Hello`] handshake with
//! binary frames — one bincode frame per WebSocket message. Tokio runs on
//! a private runtime inside the transport; the engine-facing API stays
//! synchronous.

use crate::message::{WireFrame, PROTOCOL_VERSION};
use crate::transport::Transport;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tpt_av_sync_utils::{PeerId, SyncError};
use tokio::net::TcpListener;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::SyncMessage;

#[derive(Clone)]
struct PeerConn {
    tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
}

struct WsInner {
    local: PeerId,
    peers: Mutex<HashMap<PeerId, PeerConn>>,
    inbox: Mutex<mpsc::Receiver<(PeerId, SyncMessage)>>,
    inbound_tx: mpsc::Sender<(PeerId, SyncMessage)>,
    shutdown: AtomicBool,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    listen_addr: Mutex<Option<SocketAddr>>,
    _rt: Arc<tokio::runtime::Runtime>,
}

/// A WebSocket-based sync transport.
#[derive(Clone)]
pub struct WebsocketTransport {
    inner: Arc<WsInner>,
}

impl std::fmt::Debug for WebsocketTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketTransport")
            .field("local", &self.inner.local)
            .finish_non_exhaustive()
    }
}

impl WebsocketTransport {
    /// Starts a WebSocket server for incoming peers. Returns the transport
    /// and bound address.
    pub fn serve(bind: SocketAddr, local: PeerId) -> Result<(Self, SocketAddr), SyncError> {
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

        let (tx, rx) = mpsc::channel();
        let inner = Arc::new(WsInner {
            local,
            peers: Mutex::new(HashMap::new()),
            inbox: Mutex::new(rx),
            inbound_tx: tx,
            shutdown: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            listen_addr: Mutex::new(Some(addr)),
            _rt: rt.clone(),
        });

        let accept_inner = inner.clone();
        let accept_task = rt.spawn(async move {
            while !accept_inner.shutdown.load(Ordering::Relaxed) {
                let stream = match listener.accept().await {
                    Ok((stream, _)) => stream,
                    Err(_) => break,
                };
                let task_inner = accept_inner.clone();
                tokio::spawn(async move {
                    if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                        let _ = WebsocketTransport::handshake_and_register(&task_inner, ws).await;
                    }
                });
            }
        });
        inner.tasks.lock().expect("tasks lock").push(accept_task);

        Ok((Self { inner }, addr))
    }

    /// Connects to a WebSocket sync server (`ws://…`). Returns the remote
    /// peer id.
    pub fn connect(&self, url: &str) -> Result<PeerId, SyncError> {
        let inner = self.inner.clone();
        self.inner._rt.block_on(async move {
            let (ws, _resp) = tokio_tungstenite::connect_async(url)
                .await
                .map_err(|e| SyncError::transport(format!("ws connect {url}: {e}")))?;
            WebsocketTransport::handshake_and_register(&inner, ws).await
        })
    }

    async fn handshake_and_register<S>(
        inner: &Arc<WsInner>,
        ws: tokio_tungstenite::WebSocketStream<S>,
    ) -> Result<PeerId, SyncError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sink, mut reader) = ws.split();

        // Send our hello, then wait for theirs.
        let hello = bincode::serialize(&WireFrame::Hello {
            peer_id: inner.local,
            protocol: PROTOCOL_VERSION,
        })
        .map_err(|e| SyncError::serialization(e.to_string()))?;
        sink.send(WsMessage::Binary(hello))
            .await
            .map_err(|e| SyncError::transport(e.to_string()))?;

        let peer = loop {
            match reader.next().await {
                Some(Ok(WsMessage::Binary(bytes))) => {
                    if let Ok(WireFrame::Hello { peer_id, .. }) =
                        bincode::deserialize::<WireFrame>(&bytes)
                    {
                        break peer_id;
                    }
                }
                Some(Ok(WsMessage::Ping(p))) => {
                    let _ = sink.send(WsMessage::Pong(p)).await;
                }
                Some(Err(e)) => return Err(SyncError::transport(e.to_string())),
                Some(Ok(WsMessage::Close(_))) | None => {
                    return Err(SyncError::Disconnected);
                }
                Some(Ok(_)) => continue,
            }
        };

        let (task_tx, mut task_rx) = tokio_mpsc::unbounded_channel::<Vec<u8>>();
        inner
            .peers
            .lock()
            .expect("peers lock")
            .insert(peer, PeerConn { tx: task_tx });

        // Writer task.
        let writer_task = tokio::spawn(async move {
            while let Some(bytes) = task_rx.recv().await {
                if sink.send(WsMessage::Binary(bytes)).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });
        inner
            .tasks
            .lock()
            .expect("tasks lock")
            .push(writer_task);

        // Reader task.
        let read_inner = inner.clone();
        let inbound = inner.inbound_tx.clone();
        let reader_task = tokio::spawn(async move {
            let mut reader = reader;
            while let Some(msg) = reader.next().await {
                match msg {
                    Ok(WsMessage::Binary(bytes)) => {
                        if let Ok(WireFrame::Message(m)) =
                            bincode::deserialize::<WireFrame>(&bytes)
                        {
                            if inbound.send((peer, m)).is_err() {
                                break;
                            }
                        }
                    }
                    Ok(WsMessage::Close(_)) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            read_inner.peers.lock().expect("peers lock").remove(&peer);
        });
        inner
            .tasks
            .lock()
            .expect("tasks lock")
            .push(reader_task);

        Ok(peer)
    }

    /// This peer's id.
    #[must_use]
    pub fn local_peer_id(&self) -> PeerId {
        self.inner.local
    }

    /// The listening address, when serving.
    #[must_use]
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        *self.inner.listen_addr.lock().expect("addr lock")
    }

    fn send_frame(&self, peer: PeerId, frame: &WireFrame) -> Result<(), SyncError> {
        let bytes =
            bincode::serialize(frame).map_err(|e| SyncError::serialization(e.to_string()))?;
        let conn = self
            .inner
            .peers
            .lock()
            .expect("peers lock")
            .get(&peer)
            .cloned()
            .ok_or(SyncError::PeerNotFound(peer))?;
        conn.tx
            .send(bytes)
            .map_err(|_| SyncError::transport("peer write channel closed"))
    }
}

impl Transport for WebsocketTransport {
    fn send_to(&mut self, peer_id: PeerId, message: SyncMessage) -> Result<(), SyncError> {
        self.send_frame(peer_id, &WireFrame::Message(message))
    }

    fn broadcast(&mut self, message: SyncMessage) -> Result<(), SyncError> {
        let peers = Transport::peers(self);
        if peers.is_empty() {
            return Err(SyncError::transport("no connected peers"));
        }
        for peer in peers {
            if let Err(e) = self.send_frame(peer, &WireFrame::Message(message.clone())) {
                log::warn!("broadcast to {peer} failed: {e}");
            }
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

impl Drop for WebsocketTransport {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);
        let tasks: Vec<tokio::task::JoinHandle<()>> =
            self.inner.tasks.lock().expect("tasks lock").drain(..).collect();
        for task in tasks {
            task.abort();
        }
    }
}
