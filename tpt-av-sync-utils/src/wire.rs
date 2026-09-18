//! The wire codec: the single serialization seam for everything that
//! crosses a transport (or a test).
//!
//! Bincode 2 with the `legacy` configuration — byte-for-byte the 1.x wire
//! format, without depending on the unmaintained bincode 1.x line
//! (RUSTSEC-2025-0141). Application code should never call `bincode`
//! directly; use [`encode`] / [`decode`] so the format stays swappable.

use crate::SyncError;

/// Serializes `value` into the wire format.
pub fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, SyncError> {
    bincode::serde::encode_to_vec(value, bincode::config::legacy())
        .map_err(|e| SyncError::serialization(e.to_string()))
}

/// Deserializes a value from the wire format (trailing bytes are rejected).
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, SyncError> {
    let (value, len) = bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
        .map_err(|e| SyncError::serialization(e.to_string()))?;
    if len != bytes.len() {
        return Err(SyncError::serialization(format!(
            "trailing bytes after value: {} of {}",
            bytes.len() - len,
            bytes.len()
        )));
    }
    Ok(value)
}

/// An Ed25519 signature as it travels the wire (serde has no impls for
/// arrays beyond length 32, so it is encoded as a 64-byte blob).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Signature64(
    /// The raw signature bytes.
    pub [u8; 64],
);

impl serde::Serialize for Signature64 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for Signature64 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let blob: &[u8] = serde::Deserialize::deserialize(deserializer)?;
        if blob.len() != 64 {
            return Err(serde::de::Error::invalid_length(
                blob.len(),
                &"a 64-byte signature",
            ));
        }
        let mut out = [0_u8; 64];
        out.copy_from_slice(blob);
        Ok(Self(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let value = vec![1_u64, 2, 3];
        let bytes = encode(&value).unwrap();
        assert_eq!(decode::<Vec<u64>>(&bytes).unwrap(), value);
    }

    #[test]
    fn trailing_bytes_rejected() {
        let bytes = encode(&42_u32).unwrap();
        let mut padded = bytes.clone();
        padded.push(0);
        assert!(decode::<u32>(&padded).is_err());
    }

    #[test]
    fn signature_roundtrip() {
        let sig = Signature64([7; 64]);
        let bytes = encode(&sig).unwrap();
        assert_eq!(decode::<Signature64>(&bytes).unwrap(), sig);
    }

    #[test]
    fn legacy_format_matches_bincode_1() {
        // bincode 1.x defaults (little-endian, fixed-int, u64 lengths):
        // scalars are bare LE bytes; collections carry a u64 length prefix.
        assert_eq!(encode(&1_u32).unwrap(), vec![1, 0, 0, 0]);
        assert_eq!(
            encode(&vec![1_u64, 2]).unwrap(),
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
