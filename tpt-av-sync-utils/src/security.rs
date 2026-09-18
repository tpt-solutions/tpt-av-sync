//! Security primitives shared across the engine: bounded deserialization
//! and operation field validation.
//!
//! Wire inputs are untrusted. Two layers of defense live here:
//!
//! 1. [`bounded_decode`] — every deserialization goes through a byte-limit
//!    check before the decoder touches the buffer (and the transports cap
//!    frames *before* reading them off the socket, so an attacker cannot
//!    make us allocate the buffer in the first place).
//! 2. the limit constants ([`MAX_STRING_BYTES`], [`MAX_ENVELOPE_POINTS`],
//!    …) against which `TimelineOperation::validate()` (in the CRDT crate,
//!    where the type lives) checks payload fields — applied by the engine
//!    on every inbound operation and by the relay before forwarding or
//!    persisting.
//!
//! These bound memory amplification; they are not authentication. See
//! DESIGN.md §10 and the Phase 7 checklist in todo.md.

use crate::wire;
use crate::SyncError;

/// Default byte ceiling for a single decoded wire message (64 MiB).
///
/// Snapshot and batch messages legitimately scale with session size, so
/// this ceiling matches the transports' frame cap and bounds total
/// allocation per message; the *per-operation* limits
/// (`TimelineOperation::validate`, `MAX_STRING_BYTES`,
/// `MAX_ENVELOPE_POINTS`) are what bound the cost of any single edit.
/// Tighten per-deployment with [`bounded_decode`] if sessions are small.
pub const MAX_WIRE_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Longest accepted string field (clip/track names, media sources, custom
/// envelope parameters, session names) in bytes.
pub const MAX_STRING_BYTES: usize = 4 * 1024;

/// Most points accepted in one envelope update.
pub const MAX_ENVELOPE_POINTS: usize = 100_000;

/// Server identity for TLS transports: either a freshly generated
/// self-signed certificate (development, LAN — pair with TOFU pinning on
/// the client) or loaded PEM material (production).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsIdentityConfig {
    /// Generate a self-signed certificate for this common name.
    SelfSigned {
        /// Certificate common name (e.g. the host name).
        common_name: String,
    },
    /// PEM-encoded certificate chain and private key (PEM/PKCS8).
    Pem {
        /// Certificate chain, PEM encoded.
        cert_pem: String,
        /// Private key, PEM encoded.
        key_pem: String,
    },
}

/// How a TLS client decides to trust the server certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsTrust {
    /// Accept the first certificate seen and record its SHA-256
    /// fingerprint (trust-on-first-use). Combine with
    /// [`TlsTrust::PinnedSha256`] on subsequent connections.
    AnyFirstUse,
    /// Only accept a certificate whose SHA-256 fingerprint matches.
    PinnedSha256([u8; 32]),
}

/// SHA-256 over a DER-encoded certificate.
#[must_use]
pub fn certificate_fingerprint(der: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(der);
    hasher.finalize().into()
}

/// Deserializes `bytes` as `T`, refusing to decode anything larger than
/// `max_bytes`.
pub fn bounded_decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<T, SyncError> {
    if bytes.len() > max_bytes {
        return Err(SyncError::serialization(format!(
            "message of {} bytes exceeds the {} byte limit",
            bytes.len(),
            max_bytes
        )));
    }
    wire::decode(bytes)
}

/// Deserializes with the default [`MAX_WIRE_MESSAGE_BYTES`] ceiling.
pub fn decode_message<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, SyncError> {
    bounded_decode(bytes, MAX_WIRE_MESSAGE_BYTES)
}

/// Checks one string field against [`MAX_STRING_BYTES`].
///
/// Shared vocabulary for `TimelineOperation::validate` and relay-side
/// validation of signaling frames.
pub fn validate_string(value: &str, field: &'static str) -> Result<(), SyncError> {
    if value.len() > MAX_STRING_BYTES {
        return Err(SyncError::invalid(format!(
            "{field} of {} bytes exceeds the {MAX_STRING_BYTES} byte limit",
            value.len()
        )));
    }
    Ok(())
}

/// Checks several optional string fields at once (used for update structs).
pub fn validate_strings<'a>(
    values: impl IntoIterator<Item = (&'a str, &'static str)>,
) -> Result<(), SyncError> {
    for (value, field) in values {
        validate_string(value, field)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::encode;

    #[test]
    fn oversized_message_rejected_before_decode() {
        let value = vec![1_u64; 16];
        let bytes = encode(&value).unwrap();
        assert!(bounded_decode::<Vec<u64>>(&bytes, 3).is_err());
        assert!(decode_message::<Vec<u64>>(&bytes).is_ok());
    }

    #[test]
    fn limit_constants_are_sane() {
        assert!(MAX_STRING_BYTES < MAX_WIRE_MESSAGE_BYTES);
        assert!(MAX_ENVELOPE_POINTS > 0);
    }
}
