//! LAN peer discovery.
//!
//! Two discovery transports are provided, both built on a tiny
//! self-describing UDP beacon (no external dependencies):
//!
//! - [`MulticastDiscovery`] — beacons to a multicast group
//!   (default `239.255.42.98:51820`); the primary LAN mechanism and the
//!   equivalent of the planned mDNS-based discovery.
//! - [`BroadcastDiscovery`] — beacons to the subnet broadcast address;
//!   the fallback for networks where multicast is filtered.
//!
//! A full DNS-SD (mDNS) responder is deliberately deferred: the beacon
//! carries the same information (peer id, name, session, connect port)
//! with none of the protocol machinery. See DESIGN.md §"Deviations".

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::Duration;
use tpt_av_sync_utils::{PeerId, SyncError, wire};

/// Default multicast group and port for LAN discovery.
pub const DEFAULT_MULTICAST_ADDR: &str = "239.255.42.98:51820";
/// Default subnet broadcast address and port (fallback discovery).
pub const DEFAULT_BROADCAST_ADDR: &str = "255.255.255.255:51821";
/// How long a peer stays discoverable without a fresh beacon (ms).
pub const DEFAULT_TTL_MS: u64 = 10_000;

/// The UDP beacon payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryPacket {
    /// Beaconing peer.
    pub peer_id: PeerId,
    /// Human-readable peer/application name.
    pub name: String,
    /// Collaborative session id, if the peer hosts one.
    pub session: Option<String>,
    /// The peer's TCP listen port (where to connect).
    pub port: u16,
}

/// A peer discovered on the LAN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAdvertisement {
    /// The peer's id.
    pub peer_id: PeerId,
    /// Human-readable name.
    pub name: String,
    /// Session id, if advertised.
    pub session: Option<String>,
    /// Where to connect (`ip:port` for the TCP transport).
    pub addr: SocketAddr,
    /// When the peer was last seen (unix ms).
    pub last_seen_ms: u64,
}

/// The discovery interface (spec §"discovery.rs").
pub trait Discovery: Send {
    /// Sends one beacon announcing this peer.
    fn advertise(&mut self) -> Result<(), SyncError>;
    /// Collects beacons, refreshes the peer table, and expires stale
    /// entries. Returns currently-live peers.
    fn poll(&mut self, now_ms: u64) -> Vec<PeerAdvertisement>;
    /// Stops the discovery transport.
    fn stop(&mut self);
}

struct DiscoveryCore {
    local: PeerId,
    name: String,
    session: Option<String>,
    listen_port: u16,
    socket: UdpSocket,
    target: SocketAddr,
    known: HashMap<PeerId, (PeerAdvertisement, u64)>,
    ttl_ms: u64,
}

impl DiscoveryCore {
    fn bind(
        local: PeerId,
        name: impl Into<String>,
        session: Option<String>,
        listen_port: u16,
        target: SocketAddr,
        ttl_ms: u64,
    ) -> Result<Self, SyncError> {
        let is_multicast = target.ip().is_multicast();
        let bind_addr = SocketAddr::new(
            std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            target.port(),
        );
        // SO_REUSEADDR lets several peers on one host share the discovery
        // port (required on Windows, conventional for multicast elsewhere).
        let socket2_sock = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .map_err(|e| SyncError::transport(format!("discovery socket: {e}")))?;
        socket2_sock
            .set_reuse_address(true)
            .map_err(|e| SyncError::transport(e.to_string()))?;
        socket2_sock
            .bind(&bind_addr.into())
            .map_err(|e| SyncError::transport(format!("discovery bind: {e}")))?;
        let socket: UdpSocket = socket2_sock.into();
        socket
            .set_broadcast(true)
            .map_err(|e| SyncError::transport(e.to_string()))?;
        socket
            .set_read_timeout(Some(Duration::from_millis(1)))
            .ok();
        if is_multicast {
            let v4 = match target.ip() {
                std::net::IpAddr::V4(v4) => v4,
                std::net::IpAddr::V6(_) => {
                    return Err(SyncError::transport("IPv6 multicast unsupported"));
                }
            };
            socket
                .join_multicast_v4(&v4, &Ipv4Addr::UNSPECIFIED)
                .map_err(|e| SyncError::transport(format!("multicast join: {e}")))?;
            socket
                .set_multicast_loop_v4(true)
                .map_err(|e| SyncError::transport(e.to_string()))?;
        }
        Ok(Self {
            local,
            name: name.into(),
            session,
            listen_port,
            socket,
            target,
            known: HashMap::new(),
            ttl_ms,
        })
    }

    fn advertise(&mut self) -> Result<(), SyncError> {
        let packet = DiscoveryPacket {
            peer_id: self.local,
            name: self.name.clone(),
            session: self.session.clone(),
            port: self.listen_port,
        };
        let bytes = wire::encode(&packet)
            .map_err(|e| SyncError::serialization(e.to_string()))?;
        self.socket
            .send_to(&bytes, self.target)
            .map_err(|e| SyncError::transport(format!("discovery send: {e}")))?;
        Ok(())
    }

    fn poll(&mut self, now_ms: u64) -> Vec<PeerAdvertisement> {
        let mut buf = [0_u8; 1024];
        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((n, from)) => {
                    if let Ok(packet) = wire::decode::<DiscoveryPacket>(&buf[..n]) {
                        if packet.peer_id == self.local {
                            continue;
                        }
                        let addr = SocketAddr::new(from.ip(), packet.port);
                        self.known.insert(
                            packet.peer_id,
                            (
                                PeerAdvertisement {
                                    peer_id: packet.peer_id,
                                    name: packet.name,
                                    session: packet.session,
                                    addr,
                                    last_seen_ms: now_ms,
                                },
                                now_ms,
                            ),
                        );
                    }
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break;
                }
                Err(_) => break,
            }
        }
        self.known
            .retain(|_, (_, seen)| now_ms.saturating_sub(*seen) <= self.ttl_ms);
        self.known.values().map(|(a, _)| a.clone()).collect()
    }
}

/// LAN discovery over a multicast group.
pub struct MulticastDiscovery {
    core: DiscoveryCore,
}

impl std::fmt::Debug for MulticastDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MulticastDiscovery")
            .field("local", &self.core.local)
            .field("target", &self.core.target)
            .finish_non_exhaustive()
    }
}

impl MulticastDiscovery {
    /// Binds to [`DEFAULT_MULTICAST_ADDR`] with the given TTL.
    pub fn new(
        local: PeerId,
        name: impl Into<String>,
        session: Option<String>,
        listen_port: u16,
    ) -> Result<Self, SyncError> {
        let target = DEFAULT_MULTICAST_ADDR
            .to_socket_addrs()
            .map_err(|e| SyncError::transport(e.to_string()))?
            .next()
            .ok_or_else(|| SyncError::transport("multicast address unresolved"))?;
        Ok(Self {
            core: DiscoveryCore::bind(local, name, session, listen_port, target, DEFAULT_TTL_MS)?,
        })
    }
}

impl Discovery for MulticastDiscovery {
    fn advertise(&mut self) -> Result<(), SyncError> {
        self.core.advertise()
    }

    fn poll(&mut self, now_ms: u64) -> Vec<PeerAdvertisement> {
        self.core.poll(now_ms)
    }

    fn stop(&mut self) {}
}

/// LAN discovery over subnet broadcast (fallback when multicast is
/// filtered).
pub struct BroadcastDiscovery {
    core: DiscoveryCore,
}

impl std::fmt::Debug for BroadcastDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastDiscovery")
            .field("local", &self.core.local)
            .field("target", &self.core.target)
            .finish_non_exhaustive()
    }
}

impl BroadcastDiscovery {
    /// Binds to [`DEFAULT_BROADCAST_ADDR`].
    pub fn new(
        local: PeerId,
        name: impl Into<String>,
        session: Option<String>,
        listen_port: u16,
    ) -> Result<Self, SyncError> {
        let target = DEFAULT_BROADCAST_ADDR
            .to_socket_addrs()
            .map_err(|e| SyncError::transport(e.to_string()))?
            .next()
            .ok_or_else(|| SyncError::transport("broadcast address unresolved"))?;
        Ok(Self {
            core: DiscoveryCore::bind(local, name, session, listen_port, target, DEFAULT_TTL_MS)?,
        })
    }
}

impl Discovery for BroadcastDiscovery {
    fn advertise(&mut self) -> Result<(), SyncError> {
        self.core.advertise()
    }

    fn poll(&mut self, now_ms: u64) -> Vec<PeerAdvertisement> {
        self.core.poll(now_ms)
    }

    fn stop(&mut self) {}
}

/// Shared packet/advertisement behavior tests live in the integration
/// suite (multicast support varies by host).
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_serde_roundtrip() {
        let packet = DiscoveryPacket {
            peer_id: PeerId::from_u64(5),
            name: "studio-a".into(),
            session: Some("session-1".into()),
            port: 9000,
        };
        let bytes = wire::encode(&packet).unwrap();
        let back: DiscoveryPacket = wire::decode(&bytes).unwrap();
        assert_eq!(back, packet);
    }
}
