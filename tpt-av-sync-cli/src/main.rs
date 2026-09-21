//! `tpt-av-sync`: command-line tooling for the sync engine.
//!
//! Subcommands:
//!
//! - `inspect <store-dir> [room]` — list rooms in a persisted
//!   [`tpt_av_sync_server::SessionStore`], or dump one room's operation
//!   log.
//! - `replay <store-dir> <room> [speed]` — replay a room's recorded
//!   operations into a fresh [`tpt_av_sync_crdt::TimelineCrdt`] and print
//!   the resulting timeline. `speed` is `instant` (default) or a
//!   realtime multiplier such as `2x` or `0.5x`.
//! - `relay <bind-addr> [store-dir]` — run a [`tpt_av_sync_server::RelayServer`]
//!   for local testing, optionally persisting to `store-dir`.
//! - `signaling <bind-addr>` — run a [`tpt_av_sync_server::SignalingServer`]
//!   for local testing (WebRTC SDP/ICE exchange).
//! - `dashboard <bind-addr> [peer-addr]` — a live terminal dashboard:
//!   listens on `bind-addr` (and dials `peer-addr` if given), showing
//!   connected peers, presence, playhead positions, and operation
//!   throughput as they change.

mod dashboard;
mod inspect;
mod relay_cmd;
mod replay_cmd;
mod signaling_cmd;

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("inspect") => inspect::run(&args[1..]),
        Some("replay") => replay_cmd::run(&args[1..]),
        Some("relay") => relay_cmd::run(&args[1..]),
        Some("signaling") => signaling_cmd::run(&args[1..]),
        Some("dashboard") => dashboard::run(&args[1..]),
        _ => Err(usage()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn usage() -> String {
    "usage: tpt-av-sync <inspect|replay|relay|signaling|dashboard> [args...]\n\n\
     inspect <store-dir> [room]           list rooms, or dump one room's ops\n\
     replay <store-dir> <room> [speed]    replay a room (speed: instant|<N>x)\n\
     relay <bind-addr> [store-dir]        run a local relay server\n\
     signaling <bind-addr>                run a local signaling server\n\
     dashboard <bind-addr> [peer-addr]    live session dashboard (TUI)"
        .to_string()
}
