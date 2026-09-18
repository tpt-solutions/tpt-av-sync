//! TLS support for the TCP and WebSocket transports (feature `tls`).
//!
//! Security model (Phase 7 B4):
//!
//! - Servers present a certificate from a [`TlsIdentityConfig`]: a freshly
//!   generated self-signed cert (dev/LAN) or loaded PEM material
//!   (production behind a real CA).
//! - Clients trust via [`TlsTrust`]: pin an expected SHA-256 certificate
//!   fingerprint, or accept-any-on-first-use (TOFU — compare
//!   [`Self::observed_fingerprints`] after the first connect and pin them
//!   from then on).
//!
//! Plaintext transports keep loudly-named constructors
//! (`listen_plaintext_insecure`, `connect_plaintext_insecure`) so an
//! insecure deployment is visible at the call site and in logs.
//!
//! ```no_run
//! # #[cfg(feature = "tls")]
//! # fn main() -> Result<(), tpt_av_sync_utils::SyncError> {
//! use std::net::SocketAddr;
//! use tpt_av_sync_net::{TcpTransport, TlsIdentityConfig, TlsTrust};
//! use tpt_av_sync_utils::PeerId;
//!
//! let addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
//! let identity = TlsIdentityConfig::SelfSigned {
//!     common_name: "studio-a".into(),
//! };
//! let (_transport, listen) = TcpTransport::listen_tls(addr, PeerId::generate(), &identity)?;
//! // On the client — first use (TOFU):
//! let (_t2, fingerprint) =
//!     TcpTransport::connect_tls(listen, PeerId::generate(), TlsTrust::AnyFirstUse)?;
//! // Later connects pin what was observed:
//! let _t3 = TcpTransport::connect_tls(listen, PeerId::generate(), TlsTrust::PinnedSha256(fingerprint))?;
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "tls"))]
//! # fn main() {}
//! ```

#![cfg(feature = "tls")]

use std::sync::Arc;
use std::sync::Mutex;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::DigitallySignedStruct;
use rustls::SignatureScheme;
use tpt_av_sync_utils::security::{certificate_fingerprint, TlsIdentityConfig, TlsTrust};
use tpt_av_sync_utils::SyncError;

/// A loaded TLS server identity (rustls config, ready to use).
#[derive(Clone)]
pub struct TlsServerIdentity {
    config: Arc<rustls::ServerConfig>,
}

/// A loaded TLS client trust configuration.
#[derive(Clone)]
pub struct TlsClientTrust {
    config: Arc<rustls::ClientConfig>,
    /// Fingerprints seen so far (TOFU observation record).
    observed: Arc<Mutex<Vec<[u8; 32]>>>,
    trust: TlsTrust,
}

impl TlsServerIdentity {
    /// Builds a server identity: generates a self-signed certificate or
    /// loads PEM material per `config`.
    pub fn new(config: &TlsIdentityConfig) -> Result<Self, SyncError> {
        let (certs, key) = load_certificate_chain(config)?;
        let mut server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| SyncError::transport(format!("tls server config: {e}")))?;
        server.alpn_protocols = vec![b"tpt-av-sync".to_vec()];
        Ok(Self {
            config: Arc::new(server),
        })
    }

    /// The rustls server configuration.
    #[must_use]
    pub fn config(&self) -> Arc<rustls::ServerConfig> {
        self.config.clone()
    }
}

impl TlsClientTrust {
    /// Builds client trust per `trust` (TOFU or pinned).
    pub fn new(trust: TlsTrust) -> Result<Self, SyncError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = Arc::new(FingerprintVerifier::new(trust.clone()));
        let mut config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(rustls::ALL_VERSIONS)
            .expect("supported versions")
            .with_custom_certificate_verifier(verifier);
        config.alpn_protocols = vec![b"tpt-av-sync".to_vec()];
        Ok(Self {
            config: Arc::new(config),
            observed: Arc::new(Mutex::new(Vec::new())),
            trust,
        })
    }

    /// The rustls client configuration.
    #[must_use]
    pub fn config(&self) -> Arc<rustls::ClientConfig> {
        self.config.clone()
    }

    /// Certificates observed on connections made with this trust object
    /// (TOFU record).
    #[must_use]
    pub fn observed_fingerprints(&self) -> Vec<[u8; 32]> {
        self.observed.lock().expect("observed lock").clone()
    }

    /// The trust policy in force.
    #[must_use]
    pub const fn trust(&self) -> &TlsTrust {
        &self.trust
    }
}

fn load_certificate_chain(
    config: &TlsIdentityConfig,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), SyncError> {
    match config {
        TlsIdentityConfig::SelfSigned { common_name } => {
            let pair = rcgen::generate_simple_self_signed(vec![common_name.clone()])
                .map_err(|e| SyncError::transport(format!("self-signed cert: {e}")))?;
            let cert = CertificateDer::from(pair.cert);
            let key = PrivateKeyDer::try_from(pair.key_pair.serialize_pem())
                .map_err(|e| SyncError::transport(format!("self-signed key: {e}")))?;
            Ok((vec![cert], key))
        }
        TlsIdentityConfig::Pem { cert_pem, key_pem } => {
            let mut certs = Vec::new();
            for cert in rustls_pemfile::certs(&mut cert_pem.as_bytes()) {
                certs.push(cert.map_err(|e| SyncError::transport(format!("cert pem: {e}")))?);
            }
            let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
                .map_err(|e| SyncError::transport(format!("key pem: {e}")))?
                .ok_or_else(|| SyncError::transport("no private key in pem"))?;
            Ok((certs, key))
        }
    }
}

/// A `ServerCertVerifier` that pins fingerprints (or accepts anything on
/// first use). **It deliberately skips chain and host-name validation** —
/// that is the pinning model: the fingerprint IS the trust root.
#[derive(Debug)]
struct FingerprintVerifier {
    trust: TlsTrust,
    observed: Arc<Mutex<Vec<[u8; 32]>>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl FingerprintVerifier {
    fn new(trust: TlsTrust) -> Self {
        Self {
            trust,
            observed: Arc::new(Mutex::new(Vec::new())),
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
    }

    fn record(&self, fingerprint: [u8; 32]) {
        self.observed
            .lock()
            .expect("observed lock")
            .push(fingerprint);
    }
}

impl ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fingerprint = certificate_fingerprint(end_entity.as_ref());
        match &self.trust {
            TlsTrust::AnyFirstUse => {
                self.record(fingerprint);
                Ok(ServerCertVerified::assertion())
            }
            TlsTrust::PinnedSha256(expected) => {
                if &fingerprint == expected {
                    self.record(fingerprint);
                    Ok(ServerCertVerified::assertion())
                } else {
                    Err(rustls::Error::General(format!(
                        "certificate fingerprint {fingerprint:02x?} does not match pin"
                    )))
                }
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
