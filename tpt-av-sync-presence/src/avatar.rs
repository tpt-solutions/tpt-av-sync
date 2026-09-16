//! User avatars and UI colors.

use serde::{Deserialize, Serialize};

/// An RGBA color (each channel 0–255).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Color {
    /// Red channel.
    pub r: u8,
    /// Green channel.
    pub g: u8,
    /// Blue channel.
    pub b: u8,
    /// Alpha channel (255 = opaque).
    pub a: u8,
}

impl Color {
    /// Creates a color from channels.
    #[must_use]
    pub const fn rgba(r: u8, g: u8, b: u8, a: u8) -> Self {
        Self { r, g, b, a }
    }

    /// Creates an opaque color.
    #[must_use]
    pub const fn rgb(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b, a: 255 }
    }

    /// Packs into `0xAARRGGBB`.
    #[must_use]
    pub const fn to_rgba_u32(self) -> u32 {
        ((self.a as u32) << 24) | ((self.r as u32) << 16) | ((self.g as u32) << 8) | self.b as u32
    }

    /// Unpacks from `0xAARRGGBB`.
    #[must_use]
    pub const fn from_rgba_u32(v: u32) -> Self {
        Self {
            r: (v >> 16) as u8,
            g: (v >> 8) as u8,
            b: v as u8,
            a: (v >> 24) as u8,
        }
    }
}

/// The payload of a user avatar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AvatarKind {
    /// Avatar fetched from a URL.
    Url(String),
    /// Inline image bytes (e.g. a small PNG).
    Png(Vec<u8>),
}

/// A user's avatar and associated UI color.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvatarData {
    /// The avatar payload.
    pub kind: AvatarKind,
    /// Accent color used for cursors, selections, and highlights.
    pub color: Color,
}

impl AvatarData {
    /// Creates avatar data from a URL with an accent color.
    #[must_use]
    pub fn from_url(url: impl Into<String>, color: Color) -> Self {
        Self {
            kind: AvatarKind::Url(url.into()),
            color,
        }
    }

    /// Creates avatar data from inline PNG bytes with an accent color.
    #[must_use]
    pub fn from_png(bytes: Vec<u8>, color: Color) -> Self {
        Self {
            kind: AvatarKind::Png(bytes),
            color,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn color_pack_roundtrip() {
        let c = Color::rgba(0x12, 0x34, 0x56, 0xAB);
        assert_eq!(c.to_rgba_u32(), 0xAB_12_34_56);
        assert_eq!(Color::from_rgba_u32(0xAB_12_34_56), c);
    }

    #[test]
    fn avatar_kinds_serialize() {
        let data = AvatarData::from_png(vec![1, 2, 3], Color::rgb(255, 0, 0));
        let bytes = bincode::serialize(&data).unwrap();
        let back: AvatarData = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, data);
    }
}
