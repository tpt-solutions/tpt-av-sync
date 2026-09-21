//! `replay` subcommand.

use tpt_av_sync_crdt::{ReplaySpeed, SessionRecording, TimelineCrdt};
use tpt_av_sync_server::SessionStore;
use tpt_av_sync_utils::PeerId;

/// Parses a speed argument: `"instant"` (default) or `"<factor>x"`
/// (e.g. `"2x"`, `"0.5x"`).
fn parse_speed(arg: Option<&String>) -> Result<ReplaySpeed, String> {
    let Some(arg) = arg else {
        return Ok(ReplaySpeed::Instant);
    };
    if arg.eq_ignore_ascii_case("instant") {
        return Ok(ReplaySpeed::Instant);
    }
    let factor = arg
        .strip_suffix(['x', 'X'])
        .ok_or_else(|| format!("invalid speed \"{arg}\": expected \"instant\" or e.g. \"2x\""))?
        .parse::<f64>()
        .map_err(|_| format!("invalid speed \"{arg}\": not a number"))?;
    Ok(ReplaySpeed::Realtime(factor))
}

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let dir = args
        .first()
        .ok_or_else(|| "usage: replay <store-dir> <room> [speed]".to_string())?;
    let room = args
        .get(1)
        .ok_or_else(|| "usage: replay <store-dir> <room> [speed]".to_string())?;
    let speed = parse_speed(args.get(2))?;

    let store = SessionStore::open(dir).map_err(|e| format!("opening store: {e}"))?;
    let ops = store.load_ops(room);
    if ops.is_empty() {
        return Err(format!("no operations recorded for room \"{room}\""));
    }
    let recording = SessionRecording::new(ops);
    println!(
        "replaying {} operation(s) from room \"{room}\" ({speed:?})...",
        recording.len()
    );

    let mut crdt = TimelineCrdt::new(PeerId::from_u64(0));
    recording.replay_all(&mut crdt, speed);

    let view = crdt.view();
    println!("\n{} track(s):", view.tracks.len());
    for track in &view.tracks {
        println!(
            "  [{}] \"{}\" ({:?}, {:.1} dB{}{})",
            track.track_id.as_u64(),
            track.name,
            track.kind,
            track.volume_db,
            if track.muted { ", muted" } else { "" },
            if track.solo { ", solo" } else { "" },
        );
    }
    println!("\n{} clip(s):", view.clips.len());
    for clip in &view.clips {
        println!(
            "  [{}] \"{}\" track={} start={} dur={}",
            clip.clip_id.as_u64(),
            clip.name,
            clip.track_id.as_u64(),
            clip.start_frame,
            clip.duration_frames,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_instant_and_realtime_speeds() {
        assert_eq!(parse_speed(None), Ok(ReplaySpeed::Instant));
        assert_eq!(
            parse_speed(Some(&"instant".to_string())),
            Ok(ReplaySpeed::Instant)
        );
        assert_eq!(
            parse_speed(Some(&"2x".to_string())),
            Ok(ReplaySpeed::Realtime(2.0))
        );
        assert_eq!(
            parse_speed(Some(&"0.5x".to_string())),
            Ok(ReplaySpeed::Realtime(0.5))
        );
        assert!(parse_speed(Some(&"fast".to_string())).is_err());
        assert!(parse_speed(Some(&"x".to_string())).is_err());
    }
}
