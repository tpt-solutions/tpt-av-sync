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
    println!("press Enter to stop (or send SIGINT/SIGTERM, e.g. Ctrl+C or `docker stop`)...");

    // Block until the operator stops the server; a background thread on
    // the relay's own tokio runtime keeps accepting connections the whole
    // time. Reading a real line (n > 0 bytes, including the newline) means
    // the operator pressed Enter. EOF (n == 0) means stdin was closed
    // without any input — the common case in a container with no `-i` —
    // and must NOT be treated as "stop": otherwise the process would exit
    // immediately after starting. In that case we just wait forever, and
    // the container relies on SIGTERM/SIGINT to stop it instead.
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
            // Stdin closed without an Enter keypress (no TTY attached, as
            // in a container run without `-i`): there is no stop signal
            // coming from this channel, so just keep waiting — the process
            // relies on SIGINT/SIGTERM instead. `recv_timeout` returns
            // immediately once the sender is dropped, so without this
            // sleep the loop would spin.
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                std::thread::sleep(Duration::from_secs(10));
            }
        }
    }

    server.shutdown();
    println!("relay stopped");
    Ok(())
}
