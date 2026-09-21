//! `signaling` subcommand: runs a [`SignalingServer`] for local testing
//! (WebRTC SDP/ICE exchange for peers that cannot reach each other
//! directly).

use std::io::BufRead;
use std::net::SocketAddr;
use std::time::Duration;
use tpt_av_sync_server::SignalingServer;

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let bind: SocketAddr = args
        .first()
        .ok_or_else(|| "usage: signaling <bind-addr>".to_string())?
        .parse()
        .map_err(|e| format!("invalid bind address: {e}"))?;

    let (server, addr) = SignalingServer::serve(bind).map_err(|e| format!("starting signaling server: {e}"))?;
    println!("signaling server listening on {addr}");
    println!("press Enter to stop (or send SIGINT/SIGTERM, e.g. Ctrl+C or `docker stop`)...");

    // See relay_cmd::run for why EOF (no `-i` / no TTY, as in a plain
    // container run) must not be treated as a stop signal.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        if let Ok(n) = std::io::stdin().lock().read_line(&mut line) {
            if n > 0 {
                let _ = tx.send(());
            }
        }
    });
    loop {
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(()) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                std::thread::sleep(Duration::from_secs(10));
            }
        }
    }

    server.shutdown();
    println!("signaling server stopped");
    Ok(())
}
