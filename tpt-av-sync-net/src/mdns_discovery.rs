//! LAN peer discovery via standard mDNS/DNS-SD (RFC 6762/6763), using
//! [`mdns_sd`] — feature-gated (`mdns`) since `MulticastDiscovery`/
//! `BroadcastDiscovery` already cover the same LAN function without an
//! extra dependency (see `discovery.rs`'s module doc and DESIGN.md
//! §"Deviations from spec.txt"). Use this when interop with other,
//! non-`tpt-av-sync` mDNS-aware tooling on the LAN matters (e.g. showing
//! up in a general "services on this network" browser); use the beacon
//! discoveries otherwise.
//!
//! Peer identity and session info travel in the service's TXT record
//! (`id`, `name`, optionally `session`) — the same fields the UDP beacon
//! carries in [`DiscoveryPacket`](crate::discovery::DiscoveryPacket).

use crate::discovery::{Discovery, PeerAdvertisement};
use mdns_sd::{Receiver, ServiceDaemon, ServiceEvent, ServiceInfo};
use std::collections::HashMap;
use tpt_av_sync_utils::{PeerId, SyncError};

/// The mDNS/DNS-SD service type every `tpt-av-sync` peer advertises under.
pub const SERVICE_TYPE: &str = "_tpt-av-sync._udp.local.";

/// How long a peer stays discoverable without a fresh resolve (ms). mDNS
/// itself re-announces on TTL expiry/changes, so this is a safety net,
/// not the primary staleness signal (contrast with the beacon
/// discoveries, which have no underlying protocol-level liveness).
pub const DEFAULT_TTL_MS: u64 = 30_000;

/// LAN discovery over standard mDNS/DNS-SD.
pub struct MdnsDiscovery {
    daemon: ServiceDaemon,
    receiver: Receiver<ServiceEvent>,
    local: PeerId,
    name: String,
    session: Option<String>,
    listen_port: u16,
    fullname: String,
    registered: bool,
    ttl_ms: u64,
    known: HashMap<PeerId, (PeerAdvertisement, String, u64)>,
}

impl std::fmt::Debug for MdnsDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdnsDiscovery")
            .field("local", &self.local)
            .field("fullname", &self.fullname)
            .finish_non_exhaustive()
    }
}

impl MdnsDiscovery {
    /// Starts an mDNS daemon, browsing for [`SERVICE_TYPE`]. Registration
    /// (announcing this peer) happens on the first [`Discovery::advertise`]
    /// call, not here.
    pub fn new(
        local: PeerId,
        name: impl Into<String>,
        session: Option<String>,
        listen_port: u16,
    ) -> Result<Self, SyncError> {
        let daemon =
            ServiceDaemon::new().map_err(|e| SyncError::transport(format!("mdns daemon: {e}")))?;
        let receiver = daemon
            .browse(SERVICE_TYPE)
            .map_err(|e| SyncError::transport(format!("mdns browse: {e}")))?;
        let instance_name = format!("tpt-{:016x}", local.as_u64());
        let fullname = format!("{instance_name}.{SERVICE_TYPE}");
        Ok(Self {
            daemon,
            receiver,
            local,
            name: name.into(),
            session,
            listen_port,
            fullname,
            registered: false,
            ttl_ms: DEFAULT_TTL_MS,
            known: HashMap::new(),
        })
    }

    fn register(&mut self) -> Result<(), SyncError> {
        let instance_name = format!("tpt-{:016x}", self.local.as_u64());
        let host_name = format!("{instance_name}.local.");
        let mut properties = vec![
            ("id".to_string(), self.local.as_u64().to_string()),
            ("name".to_string(), self.name.clone()),
        ];
        if let Some(session) = &self.session {
            properties.push(("session".to_string(), session.clone()));
        }
        let info = ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name,
            &host_name,
            (),
            self.listen_port,
            &properties[..],
        )
        .map_err(|e| SyncError::transport(format!("mdns service info: {e}")))?
        .enable_addr_auto();
        self.daemon
            .register(info)
            .map_err(|e| SyncError::transport(format!("mdns register: {e}")))?;
        self.registered = true;
        Ok(())
    }

    fn handle_event(&mut self, event: ServiceEvent, now_ms: u64) {
        match event {
            ServiceEvent::ServiceResolved(resolved) => {
                if resolved.fullname == self.fullname {
                    return; // our own advertisement, echoed back.
                }
                let Some(Some(id_bytes)) = resolved.txt_properties.get_property_val("id") else {
                    return;
                };
                let Ok(id_str) = std::str::from_utf8(id_bytes) else { return };
                let Ok(peer_id_raw) = id_str.parse::<u64>() else { return };
                let peer_id = PeerId::from_u64(peer_id_raw);
                if peer_id == self.local {
                    return;
                }
                let Some(addr) = resolved
                    .addresses
                    .iter()
                    .map(mdns_sd::ScopedIp::to_ip_addr)
                    .find(|ip| !ip.is_loopback())
                else {
                    return;
                };
                let name = resolved
                    .txt_properties
                    .get_property_val("name")
                    .flatten()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("peer")
                    .to_string();
                let session = resolved
                    .txt_properties
                    .get_property_val("session")
                    .flatten()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(str::to_string);
                self.known.insert(
                    peer_id,
                    (
                        PeerAdvertisement {
                            peer_id,
                            name,
                            session,
                            addr: std::net::SocketAddr::new(addr, resolved.port),
                            last_seen_ms: now_ms,
                        },
                        resolved.fullname.clone(),
                        now_ms,
                    ),
                );
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                self.known.retain(|_, (_, known_fullname, _)| known_fullname != &fullname);
            }
            _ => {}
        }
    }
}

impl Discovery for MdnsDiscovery {
    fn advertise(&mut self) -> Result<(), SyncError> {
        // mDNS announces and re-announces in the background once
        // registered; unlike the beacon discoveries, repeated calls don't
        // need to (and shouldn't) re-send a fresh packet each time.
        if !self.registered {
            self.register()?;
        }
        Ok(())
    }

    fn poll(&mut self, now_ms: u64) -> Vec<PeerAdvertisement> {
        while let Ok(event) = self.receiver.try_recv() {
            self.handle_event(event, now_ms);
        }
        self.known
            .retain(|_, (_, _, seen)| now_ms.saturating_sub(*seen) <= self.ttl_ms);
        self.known.values().map(|(a, ..)| a.clone()).collect()
    }

    fn stop(&mut self) {
        // Unlike the UDP-socket-based discoveries (whose sockets clean up
        // via Drop), the mDNS daemon runs its own background thread that
        // outlives a dropped `ServiceDaemon` handle — it must be shut down
        // explicitly, or it leaks for the life of the process.
        if self.registered {
            let _ = self.daemon.unregister(&self.fullname);
        }
        let _ = self.daemon.shutdown();
    }
}

impl Drop for MdnsDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}
