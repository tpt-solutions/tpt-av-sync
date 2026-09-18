# Changelog — tpt-av-sync-presence

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) · SemVer.

## [Unreleased]

### Added
- `PresenceManager`: local identity + cursor + activity, remote user/cursor tracking, `tick` aging for local and remote users, `generate_update` / `generate_leave_update`, `handle_peer_leave` (silent disconnect).
- `UserInfo` (peer, name, avatar, presence, last-active) with builder helpers; `PresenceState` (Online / Idle / Offline).
- `PresenceUpdate` wire message (user info + optional cursor + `leaving` flag), bincode round-trip verified.
- `CursorState`: playhead, selection range, focused clip, timestamp; builder chain.
- `AvatarData` (URL or inline PNG) + `Color` (RGBA ↔ `0xAARRGGBB` packing).
- `ActivityTracker` + `IdleConfig`: Online → Idle → Offline transitions computed from injected timestamps (deterministic, host-clock-independent).

### Verified
- 10 unit tests + 2 flow tests: two-peer cursor exchange through bincode, idle aging on both sides, offline users hidden from `remote_cursors`, leave announcements.
