//! `relay` subcommand: runs a [`RelayServer`] for local testing.

use std::io::BufRead;
use std::net::SocketAddr;
use std::time::Duration;
use tpt_av_sync_server::{RelayServer, SessionStore};

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let bind: SocketAddr = args
        .first()
        .ok_or_else(|| "usage: relay <bind-addr> [store-dir]".to_string())?
        .parse()
        .map_err(|e| format!("invalid bind address: {e}"))?;

    let store = match args.get(1) {
        Some(dir) => Some(SessionStore::open(dir).map_err(|e| format!("opening store: {e}"))?),
        None => None,
    };
    let persisting = store.is_some();

    let (server, addr) = RelayServer::serve(bind, store).map_err(|e| format!("starting relay: {e}"))?;
    println!("relay listening on {addr}{}", if persisting {
        " (persisting)"
    } else {
        " (in-memory only)"
    });
    println!("press Enter to stop...");

    // Block until the operator stops the server; a background thread on
    // the relay's own tokio runtime keeps accepting connections the whole
    // time. Also nudge stdout periodically so a piped/redirected session
    // (no interactive Enter) still shows the process is alive.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().lock().read_line(&mut line);
        let _ = tx.send(());
    });
    loop {
        if rx.recv_timeout(Duration::from_secs(10)).is_ok() {
            break;
        }
    }

    server.shutdown();
    println!("relay stopped");
    Ok(())
}
