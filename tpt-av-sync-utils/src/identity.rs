//! Peer authentication: Ed25519 keypair identity with `PeerId` derivation.
//!
//! A [`PeerIdentity`] binds a `PeerId` to a signing key: the peer id is
//! derived from the public key (SHA-256, truncated to 64 bits), so a
//! claimed identity cannot be separated from the key that owns it. Two
//! proofs travel the wire:
//!
//! - **ownership** — a signature over a domain-separated hello tag and the
//!   peer id, proving the sender controls the key;
//! - **liveness** — a challenge/response over a fresh nonce, proving the
//!   key is present *now* (anti-replay).
//!
//! See the Phase 7 (B5) checklist in todo.md and DESIGN.md §10.

use crate::PeerId;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Domain separator for hello (ownership) signatures.
const HELLO_CONTEXT: &[u8] = b"tpt-av-sync/identity/hello/v2";
/// Domain separator for challenge (liveness) signatures.
const CHALLENGE_CONTEXT: &[u8] = b"tpt-av-sync/identity/challenge/v2";
/// Length of a challenge nonce.
pub const NONCE_LEN: usize = 32;

/// Errors produced by identity verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    /// The claimed peer id does not derive from the presented key.
    PeerIdMismatch,
    /// A signature did not verify.
    BadSignature,
    /// Key or signature bytes had the wrong length.
    Malformed,
}

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PeerIdMismatch => write!(f, "peer id does not derive from verifying key"),
            Self::BadSignature => write!(f, "signature verification failed"),
            Self::Malformed => write!(f, "malformed key or signature"),
        }
    }
}

impl std::error::Error for IdentityError {}

/// Derives the [`PeerId`] for a verifying key (first 8 bytes of
/// SHA-256 `"tpt-av-sync/peer-id/v2" || vk`).
#[must_use]
pub fn derive_peer_id(verifying_key: &[u8; 32]) -> PeerId {
    let mut hasher = Sha256::new();
    hasher.update(b"tpt-av-sync/peer-id/v2");
    hasher.update(verifying_key);
    let digest = hasher.finalize();
    PeerId::from_u64(u64::from_be_bytes(digest[..8].try_into().expect("8 bytes")))
}

/// The public half of an identity as it travels the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerIdentityProof {
    /// Ed25519 verifying key.
    pub verifying_key: [u8; 32],
    /// Signature over [`HELLO_CONTEXT`] `||` peer id, proving the sender
    /// controls `verifying_key` and that the key derives its claimed id.
    pub hello_signature: [u8; 64],
}

// serde has no array impls beyond length 32; the proof is fixed-size, so
// encode it as one 96-byte blob.
impl Serialize for PeerIdentityProof {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut blob = [0_u8; 96];
        blob[..32].copy_from_slice(&self.verifying_key);
        blob[32..].copy_from_slice(&self.hello_signature);
        serializer.serialize_bytes(&blob)
    }
}

impl<'de> Deserialize<'de> for PeerIdentityProof {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let blob: &[u8] = serde::Deserialize::deserialize(deserializer)?;
        if blob.len() != 96 {
            return Err(serde::de::Error::invalid_length(
                blob.len(),
                &"a 96-byte identity proof",
            ));
        }
        let mut key = [0_u8; 32];
        let mut sig = [0_u8; 64];
        key.copy_from_slice(&blob[..32]);
        sig.copy_from_slice(&blob[32..]);
        Ok(Self {
            verifying_key: key,
            hello_signature: sig,
        })
    }
}

impl PeerIdentityProof {
    /// Verifies the proof against a claimed peer id.
    pub fn verify(&self, claimed: PeerId) -> Result<(), IdentityError> {
        let vk = VerifyingKey::from_bytes(&self.verifying_key)
            .map_err(|_| IdentityError::Malformed)?;
        if derive_peer_id(&self.verifying_key) != claimed {
            return Err(IdentityError::PeerIdMismatch);
        }
        let mut message = Vec::with_capacity(HELLO_CONTEXT.len() + 8);
        message.extend_from_slice(HELLO_CONTEXT);
        message.extend_from_slice(&claimed.as_u64().to_be_bytes());
        let signature = ed25519_dalek::Signature::from_bytes(&self.hello_signature);
        vk.verify(&message, &signature).map_err(|_| IdentityError::BadSignature)
    }
}

/// An Ed25519 signing identity. `PeerId` is derived from the public key.
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    signing: SigningKey,
    peer_id: PeerId,
}

impl PeerIdentity {
    /// Generates a fresh identity from OS randomness.
    pub fn generate() -> Self {
        let mut seed = [0_u8; 32];
        rand_core::OsRng.fill_bytes(&mut seed);
        Self::from_seed(seed).expect("random seed is a valid key")
    }

    /// Builds an identity from a 32-byte seed (deterministic tests).
    pub fn from_seed(seed: [u8; 32]) -> Result<Self, IdentityError> {
        let signing = SigningKey::from_bytes(&seed);
        let vk = signing.verifying_key().to_bytes();
        Ok(Self {
            signing,
            peer_id: derive_peer_id(&vk),
        })
    }

    /// The derived peer id.
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The verifying (public) key bytes.
    #[must_use]
    pub fn verifying_key(&self) -> [u8; 32] {
        self.signing.verifying_key().to_bytes()
    }

    /// Produces the ownership proof for this identity.
    #[must_use]
    pub fn hello_proof(&self) -> PeerIdentityProof {
        let mut message = Vec::with_capacity(HELLO_CONTEXT.len() + 8);
        message.extend_from_slice(HELLO_CONTEXT);
        message.extend_from_slice(&self.peer_id.as_u64().to_be_bytes());
        PeerIdentityProof {
            verifying_key: self.verifying_key(),
            hello_signature: self.signing.sign(&message).to_bytes(),
        }
    }

    /// Signs a challenge nonce (liveness proof).
    #[must_use]
    pub fn sign_challenge(&self, nonce: &[u8; NONCE_LEN]) -> [u8; 64] {
        let mut message = Vec::with_capacity(CHALLENGE_CONTEXT.len() + NONCE_LEN);
        message.extend_from_slice(CHALLENGE_CONTEXT);
        message.extend_from_slice(nonce);
        self.signing.sign(&message).to_bytes()
    }

    /// Wraps the identity for sharing across threads.
    #[must_use]
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }
}

/// Computes the proof-of-membership token for `room` under `secret`
/// (SHA-256 over a domain-separated prefix, the secret, and the room).
///
/// The secret itself never travels the wire — only this derivation does
/// (B6 room authorization). Know the secret to join; know nothing useful
/// from joining.
#[must_use]
pub fn room_token_proof(secret: &str, room: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"tpt-av-sync/room-token/v2");
    hasher.update((secret.len() as u64).to_be_bytes());
    hasher.update(secret.as_bytes());
    hasher.update((room.len() as u64).to_be_bytes());
    hasher.update(room.as_bytes());
    hasher.finalize().into()
}

/// Generates a fresh random nonce.
#[must_use]
pub fn random_nonce() -> [u8; NONCE_LEN] {
    let mut nonce = [0_u8; NONCE_LEN];
    rand_core::OsRng.fill_bytes(&mut nonce);
    nonce
}

/// Verifies a challenge response against a nonce and a verified identity
/// proof.
pub fn verify_challenge_response(
    proof: &PeerIdentityProof,
    nonce: &[u8; NONCE_LEN],
    signature: &[u8; 64],
) -> Result<(), IdentityError> {
    let vk =
        VerifyingKey::from_bytes(&proof.verifying_key).map_err(|_| IdentityError::Malformed)?;
    let mut message = Vec::with_capacity(CHALLENGE_CONTEXT.len() + NONCE_LEN);
    message.extend_from_slice(CHALLENGE_CONTEXT);
    message.extend_from_slice(nonce);
    let signature = ed25519_dalek::Signature::from_bytes(signature);
    vk.verify(&message, &signature)
        .map_err(|_| IdentityError::BadSignature)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(b: u8) -> [u8; 32] {
        [b; 32]
    }

    #[test]
    fn peer_id_is_deterministic_per_key_and_distinct_across_keys() {
        let a = PeerIdentity::from_seed(seed(1)).unwrap();
        let b = PeerIdentity::from_seed(seed(1)).unwrap();
        let c = PeerIdentity::from_seed(seed(2)).unwrap();
        assert_eq!(a.peer_id(), b.peer_id(), "same seed, same id");
        assert_ne!(a.peer_id(), c.peer_id(), "different seed, different id");
    }

    #[test]
    fn hello_proof_verifies_and_binds_peer_id() {
        let id = PeerIdentity::from_seed(seed(3)).unwrap();
        let proof = id.hello_proof();
        proof.verify(id.peer_id()).expect("valid proof");

        // A different claimed id must fail the key→id binding first.
        assert_eq!(
            proof.verify(PeerId::from_u64(0xDEAD)).unwrap_err(),
            IdentityError::PeerIdMismatch
        );

        // A tampered signature must fail verification.
        let mut tampered = proof;
        tampered.hello_signature[0] ^= 0xFF;
        assert_eq!(
            tampered.verify(id.peer_id()).unwrap_err(),
            IdentityError::BadSignature
        );

        // A key that derives a different peer id must fail the binding.
        let other = PeerIdentity::from_seed(seed(4)).unwrap();
        assert_eq!(
            other.hello_proof().verify(id.peer_id()).unwrap_err(),
            IdentityError::PeerIdMismatch
        );
    }

    #[test]
    fn challenge_response_is_liveness_bound() {
        let id = PeerIdentity::from_seed(seed(5)).unwrap();
        let proof = id.hello_proof();
        let nonce = random_nonce();
        let sig = id.sign_challenge(&nonce);
        verify_challenge_response(&proof, &nonce, &sig).expect("valid liveness");

        let mut other_nonce = nonce;
        other_nonce[0] ^= 1;
        assert!(verify_challenge_response(&proof, &other_nonce, &sig).is_err());
    }

    #[test]
    fn proofs_serialize_compactly() {
        let id = PeerIdentity::from_seed(seed(6)).unwrap();
        let bytes = crate::wire::encode(&id.hello_proof()).unwrap();
        // 32-byte key + 64-byte signature + bincode framing.
        assert_eq!(bytes.len(), 32 + 64 + 8);
    }
}
