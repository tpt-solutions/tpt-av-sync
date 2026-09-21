//! `inspect` subcommand.

use tpt_av_sync_server::SessionStore;

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let dir = args
        .first()
        .ok_or_else(|| "usage: inspect <store-dir> [room]".to_string())?;
    let store = SessionStore::open(dir).map_err(|e| format!("opening store: {e}"))?;

    match args.get(1) {
        None => {
            let rooms = store.rooms();
            if rooms.is_empty() {
                println!("(no rooms in {dir})");
                return Ok(());
            }
            for room in rooms {
                let ops = store.load_ops(&room);
                println!("{room}: {} operation(s)", ops.len());
            }
        }
        Some(room) => {
            let ops = store.load_ops(room);
            if ops.is_empty() {
                println!("(no operations recorded for room \"{room}\")");
                return Ok(());
            }
            for op in &ops {
                println!(
                    "[{:>6}@{}] {:?}",
                    op.lamport_ts,
                    op.peer_id,
                    op.operation
                );
            }
            println!("\n{} operation(s) total", ops.len());
        }
    }
    Ok(())
}
