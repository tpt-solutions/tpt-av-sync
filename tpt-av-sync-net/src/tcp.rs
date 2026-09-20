//! Raw TCP transport (length-prefixed frames over blocking sockets).
//!
//! Intended for LAN use and tests. Each connection performs a
//! [`WireFrame::Hello`] handshake in both directions, then carries
//! [`WireFrame::Message`]s. One background reader thread per connection;
//! writes are serialized behind a per-connection mutex.

use crate::message::{WireFrame, PROTOCOL_VERSION};
use crate::transport::Transport;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tpt_av_sync_utils::identity::IdentityError;
use tpt_av_sync_utils::wire::Signature64;
use tpt_av_sync_utils::{PeerId, PeerIdentity, PeerIdentityProof, SyncError, wire};

use crate::SyncMessage;

/// Largest accepted frame (64 MiB) — guards against corrupt/garbage length
/// prefixes.
pub const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

struct PeerConn {
    writer: Mutex<TcpStream>,
}

struct Inner {
    local: PeerId,
    /// Some => authenticated mode: this side presents a proof, challenges
    /// the remote, and (when `require_remote_identity`) demands one back.
    /// Interior-mutable so dial-side handles can attach an identity.
    identity: std::sync::Mutex<Option<Arc<PeerIdentity>>>,
    require_remote_identity: AtomicBool,
    peers: Mutex<HashMap<PeerId, Arc<PeerConn>>>,
    inbox: Mutex<mpsc::Receiver<(PeerId, SyncMessage)>>,
    inbound_tx: mpsc::Sender<(PeerId, SyncMessage)>,
    shutdown: AtomicBool,
    listen_addr: Mutex<Option<SocketAddr>>,
}

impl Inner {
    fn register(inner: &Arc<Self>, peer: PeerId, conn: Arc<PeerConn>, mut stream: TcpStream) {
        inner.peers.lock().expect("peers lock").insert(peer, conn);
        let inner = inner.clone();
        thread::spawn(move || {
            loop {
                match read_frame(&mut stream) {
                    Ok(Some(WireFrame::Message(msg))) => {
                        if inner.inbound_tx.send((peer, msg)).is_err() {
                            break;
                        }
                    }
                    Ok(Some(WireFrame::Goodbye)) | Ok(None) => break,
                    Ok(Some(WireFrame::Hello { .. }))
                    | Ok(Some(WireFrame::Challenge { .. }))
                    | Ok(Some(WireFrame::ChallengeResponse { .. })) => continue,
                    Err(SyncError::Timeout) => continue, // idle read timeout
                    Err(_) => break,
                }
            }
            inner.peers.lock().expect("peers lock").remove(&peer);
        });
    }

    fn hello_frame(&self) -> WireFrame {
        // In authenticated mode the identity's derived id is authoritative.
        let local = self
            .identity()
            .map(|id| id.peer_id())
            .unwrap_or(self.local);
        WireFrame::Hello {
            peer_id: local,
            protocol: PROTOCOL_VERSION,
            identity: self.identity().as_ref().map(|id| id.hello_proof()),
        }
    }

    fn identity(&self) -> Option<Arc<PeerIdentity>> {
        self.identity
            .lock()
            .expect("identity lock")
            .clone()
    }

    fn requires_remote_identity(&self) -> bool {
        self.require_remote_identity
            .load(Ordering::Relaxed)
    }

    /// Validates the remote `Hello`: protocol version, and — in
    /// authenticated mode — the ownership proof. Returns the remote peer id
    /// (authoritative when a proof is presented).
    fn verify_remote_hello(
        &self,
        protocol: u16,
        claimed: PeerId,
        remote_identity: Option<PeerIdentityProof>,
    ) -> Result<PeerId, SyncError> {
        if protocol != PROTOCOL_VERSION {
            return Err(SyncError::transport(format!(
                "protocol mismatch: peer speaks v{protocol}, we speak v{PROTOCOL_VERSION}"
            )));
        }
        match (self.requires_remote_identity(), remote_identity) {
            (_, Some(proof)) => {
                proof.verify(claimed).map_err(|err| match err {
                    IdentityError::PeerIdMismatch | IdentityError::Malformed => {
                        SyncError::transport(format!("peer identity rejected: {err}"))
                    }
                    IdentityError::BadSignature => {
                        SyncError::transport("peer identity rejected: bad signature")
                    }
                })?;
                Ok(claimed)
            }
            (false, None) => Ok(claimed),
            (true, None) => Err(SyncError::transport(
                "peer did not present an identity (authenticated mode)",
            )),
        }
    }

    /// Issues a liveness challenge to a remote that presented an identity.
    fn challenge_and_verify(
        &self,
        stream: &mut TcpStream,
        remote_proof: &PeerIdentityProof,
    ) -> Result<(), SyncError> {
        let nonce = tpt_av_sync_utils::random_nonce();
        write_frame(stream, &WireFrame::Challenge { nonce })?;
        match read_frame(stream)? {
            Some(WireFrame::ChallengeResponse { signature }) => {
                tpt_av_sync_utils::identity::verify_challenge_response(
                    remote_proof,
                    &nonce,
                    &signature.0,
                )
                .map_err(|_| SyncError::transport("challenge response rejected"))?;
                Ok(())
            }
            _ => Err(SyncError::transport(
                "expected challenge response during handshake",
            )),
        }
    }

    /// Answers the dialer's liveness challenge (authenticated acceptor).
    fn answer_challenge(&self, stream: &mut TcpStream) -> Result<(), SyncError> {
        match read_frame(stream)? {
            Some(WireFrame::Challenge { nonce }) => {
                let id = self
                    .identity()
                    .ok_or_else(|| SyncError::transport("challenged without an identity"))?;
                write_frame(
                    stream,
                    &WireFrame::ChallengeResponse {
                        signature: Signature64(id.sign_challenge(&nonce)),
                    },
                )
            }
            _ => Err(SyncError::transport("expected challenge during handshake")),
        }
    }

    /// Completes the server side of the handshake on an accepted socket
    /// and attaches it to the peer map.
    fn accept_incoming(inner: &Arc<Self>, stream: TcpStream) {
        let mut stream = stream;
        configure(&mut stream);
        if write_frame(&mut stream, &inner.hello_frame()).is_err() {
            return;
        }
        let remote = match read_frame(&mut stream) {
            Ok(Some(WireFrame::Hello {
                peer_id,
                protocol,
                identity,
            })) => match inner.verify_remote_hello(protocol, peer_id, identity) {
                Ok(peer) => (peer, identity),
                Err(_) => return,
            },
            _ => return,
        };
        let (peer, remote_identity) = remote;
        if remote_identity.is_some() && inner.answer_challenge(&mut stream).is_err() {
            return;
        }
        let writer = match stream.try_clone() {
            Ok(w) => w,
            Err(_) => return,
        };
        let conn = Arc::new(PeerConn {
            writer: Mutex::new(writer),
        });
        Inner::register(inner, peer, conn, stream);
    }
}

/// A peer-to-peer transport over raw TCP.
#[derive(Clone)]
pub struct TcpTransport {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for TcpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpTransport")
            .field("local", &self.inner.local)
            .finish_non_exhaustive()
    }
}

impl TcpTransport {
    /// Starts listening for incoming peers (anonymous mode: no identity
    /// checks — fine for trusted LANs). Returns the transport and the
    /// bound address (useful when binding port 0).
    pub fn listen(bind: SocketAddr, local: PeerId) -> Result<(Self, SocketAddr), SyncError> {
        Self::listen_with_identity(bind, local, None)
    }

    /// Starts listening in authenticated mode: the local identity presents
    /// an Ed25519 proof, is challenged for liveness, and every remote must
    /// present a valid identity of its own. The local peer id is derived
    /// from the identity's key.
    pub fn listen_authenticated(
        bind: SocketAddr,
        identity: Arc<PeerIdentity>,
    ) -> Result<(Self, SocketAddr), SyncError> {
        let local = identity.peer_id();
        Self::listen_with_identity(bind, local, Some(identity))
    }

    fn listen_with_identity(
        bind: SocketAddr,
        local: PeerId,
        identity: Option<Arc<PeerIdentity>>,
    ) -> Result<(Self, SocketAddr), SyncError> {
        let listener =
            TcpListener::bind(bind).map_err(|e| SyncError::transport(format!("bind {bind}: {e}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| SyncError::transport(e.to_string()))?;
        let addr = listener
            .local_addr()
            .map_err(|e| SyncError::transport(e.to_string()))?;

        let require = identity.is_some();
        let (tx, rx) = mpsc::channel();
        let inner = Arc::new(Inner {
            local,
            require_remote_identity: AtomicBool::new(require),
            identity: std::sync::Mutex::new(identity),
            peers: Mutex::new(HashMap::new()),
            inbox: Mutex::new(rx),
            inbound_tx: tx,
            shutdown: AtomicBool::new(false),
            listen_addr: Mutex::new(Some(addr)),
        });

        let accept_inner = inner.clone();
        thread::spawn(move || {
            while !accept_inner.shutdown.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => Inner::accept_incoming(&accept_inner, stream),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Ok((Self { inner }, addr))
    }

    /// Connects to a listening peer (anonymous mode). Returns the remote
    /// peer's id.
    pub fn connect(&self, addr: SocketAddr) -> Result<PeerId, SyncError> {
        self.dial(addr)
    }

    /// Connects in authenticated mode: the peer accepted via
    /// [`TcpTransport::listen_authenticated`] must present a valid identity
    /// and answer a liveness challenge.
    pub fn connect_authenticated(
        &self,
        addr: SocketAddr,
        identity: Arc<PeerIdentity>,
    ) -> Result<PeerId, SyncError> {
        *self.inner.identity.lock().expect("identity lock") = Some(identity);
        self.inner
            .require_remote_identity
            .store(true, Ordering::Relaxed);
        self.dial(addr)
    }

    fn dial(&self, addr: SocketAddr) -> Result<PeerId, SyncError> {
        let mut stream =
            TcpStream::connect_timeout(&addr, Duration::from_secs(5))
                .map_err(|e| SyncError::transport(format!("connect {addr}: {e}")))?;
        configure(&mut stream);
        write_frame(&mut stream, &self.inner.hello_frame())?;
        let remote = match read_frame(&mut stream)? {
            Some(WireFrame::Hello {
                peer_id,
                protocol,
                identity,
            }) => self
                .inner
                .verify_remote_hello(protocol, peer_id, identity)
                .map(|peer| (peer, identity))?,
            _ => return Err(SyncError::transport("expected Hello handshake")),
        };
        let (peer, remote_identity) = remote;
        if let Some(proof) = remote_identity {
            // The remote authenticated: prove it is live before trusting it.
            self.inner.challenge_and_verify(&mut stream, &proof)?;
        }
        let writer = stream
            .try_clone()
            .map_err(|e| SyncError::transport(e.to_string()))?;
        let conn = Arc::new(PeerConn {
            writer: Mutex::new(writer),
        });
        Inner::register(&self.inner, peer, conn, stream);
        Ok(peer)
    }


    /// This peer's id.
    #[must_use]
    pub fn local_peer_id(&self) -> PeerId {
        self.inner.local
    }

    /// The listening address, when this transport listens.
    #[must_use]
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        *self.inner.listen_addr.lock().expect("addr lock")
    }

    fn send_frame(&self, peer: PeerId, frame: &WireFrame) -> Result<(), SyncError> {
        let conn = self
            .inner
            .peers
            .lock()
            .expect("peers lock")
            .get(&peer)
            .cloned()
            .ok_or(SyncError::PeerNotFound(peer))?;
        let mut writer = conn.writer.lock().expect("writer lock");
        let _ = writer.set_write_timeout(Some(Duration::from_secs(5)));
        write_frame(&mut writer, frame)
    }
}

impl Transport for TcpTransport {
    fn send_to(&mut self, peer_id: PeerId, message: SyncMessage) -> Result<(), SyncError> {
        self.send_frame(peer_id, &WireFrame::Message(message))
    }

    fn broadcast(&mut self, message: SyncMessage) -> Result<(), SyncError> {
        let peers = Transport::peers(self);
        if peers.is_empty() {
            return Err(SyncError::transport("no connected peers"));
        }
        let mut last_err = None;
        for peer in peers {
            if let Err(e) = self.send_frame(peer, &WireFrame::Message(message.clone())) {
                log::warn!("broadcast to {peer} failed: {e}");
                last_err = Some(e);
            }
        }
        match last_err {
            Some(e) if !Transport::peers(self).is_empty() => Err(e),
            Some(_) | None => Ok(()),
        }
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
        let mut ids: Vec<PeerId> = self.inner.peers.lock().expect("peers lock").keys().copied().collect();
        ids.sort();
        ids
    }
}

impl Drop for TcpTransport {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);
        // Tear down live connections so reader threads unblock.
        let conns: Vec<Arc<PeerConn>> = self
            .inner
            .peers
            .lock()
            .expect("peers lock")
            .values()
            .cloned()
            .collect();
        for conn in conns {
            if let Ok(writer) = conn.writer.lock() {
                let _ = writer.shutdown(Shutdown::Both);
            }
        }
    }
}

fn configure(stream: &mut TcpStream) {
    // Accepted sockets inherit the listener's non-blocking mode on some
    // platforms; the handshake and reader need blocking reads.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
}

fn write_frame(stream: &mut TcpStream, frame: &WireFrame) -> Result<(), SyncError> {
    let bytes =
        wire::encode(frame).map_err(|e| SyncError::serialization(e.to_string()))?;
    let len = u32::try_from(bytes.len()).map_err(|_| SyncError::transport("frame too large"))?;
    stream
        .write_all(&len.to_be_bytes())
        .and_then(|()| stream.write_all(&bytes))
        .and_then(|()| stream.flush())
        .map_err(|e| SyncError::transport(e.to_string()))
}

fn read_frame(stream: &mut TcpStream) -> Result<Option<WireFrame>, SyncError> {
    let mut len_buf = [0_u8; 4];
    match stream.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
        {
            return Err(SyncError::Timeout)
        }
        Err(e) => return Err(SyncError::transport(e.to_string())),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_BYTES {
        return Err(SyncError::transport(format!(
            "frame of {len} bytes exceeds limit"
        )));
    }
    let mut buf = vec![0_u8; len as usize];
    stream
        .read_exact(&mut buf)
        .map_err(|e| SyncError::transport(e.to_string()))?;
    tpt_av_sync_utils::security::bounded_decode(&buf, MAX_FRAME_BYTES as usize)
        .map(Some)
        .map_err(|e| SyncError::serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_transports_exchange_frames_over_loopback() {
        let (mut ta, addr) = TcpTransport::listen(
            "127.0.0.1:0".parse().unwrap(),
            PeerId::from_u64(1),
        )
        .unwrap();
        let (mut tb, _) = TcpTransport::listen("127.0.0.1:0".parse().unwrap(), PeerId::from_u64(2))
            .unwrap();

        let remote = tb.connect(addr).unwrap();
        assert_eq!(remote, PeerId::from_u64(1));

        // Wait for the accept-side handshake to register peer 2 on A.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while Transport::peers(&ta).is_empty() && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(Transport::peers(&ta), vec![PeerId::from_u64(2)]);

        ta.send_to(
            PeerId::from_u64(2),
            SyncMessage::RequestSnapshot,
        )
        .unwrap();
        // Reader threads are scheduled asynchronously; poll briefly.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let (from, msg) = loop {
            match tb.try_recv().unwrap() {
                Some(item) => break item,
                None if std::time::Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                None => panic!("message arrives"),
            }
        };
        assert_eq!(from, PeerId::from_u64(1));
        assert_eq!(msg, SyncMessage::RequestSnapshot);
    }
}
