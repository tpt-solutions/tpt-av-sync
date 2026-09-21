//! `dashboard` subcommand: a live terminal view of a running session —
//! connected peers, presence, playhead positions, and operation
//! throughput — driven by `SyncEngine::process_messages`.

use std::collections::BTreeMap;
use std::io::Stdout;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Constraint;
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::Frame;
use ratatui::Terminal;

use tpt_av_sync_crdt::TimelineCrdt;
use tpt_av_sync_net::{SyncEngine, SyncEvent, TcpTransport};
use tpt_av_sync_presence::PresenceState;
use tpt_av_sync_utils::PeerId;

/// One row of the dashboard: what's known about a peer.
#[derive(Debug, Clone, Default)]
struct PeerRow {
    name: Option<String>,
    presence: Option<PresenceState>,
    playhead: Option<u64>,
    ops_seen: u64,
}

/// Everything the dashboard renders, decoupled from the terminal and
/// network so the layout can be unit tested without either.
#[derive(Debug, Default)]
struct DashboardState {
    rows: BTreeMap<PeerId, PeerRow>,
    total_ops: u64,
    ops_per_sec: f64,
}

impl DashboardState {
    fn apply(&mut self, event: SyncEvent) {
        match event {
            SyncEvent::PeerJoined(peer) => {
                self.rows.entry(peer).or_default();
            }
            SyncEvent::PeerLeft(peer) => {
                self.rows.entry(peer).or_default().presence = Some(PresenceState::Offline);
            }
            SyncEvent::RemoteOperation(op) => {
                self.total_ops += 1;
                self.rows.entry(op.peer_id).or_default().ops_seen += 1;
            }
            SyncEvent::SnapshotMerged { applied, .. } => {
                self.total_ops += applied as u64;
            }
            SyncEvent::Playhead(update) => {
                self.rows.entry(update.peer_id).or_default().playhead = Some(update.position);
            }
            SyncEvent::Presence(update) => {
                let row = self.rows.entry(update.peer_id).or_default();
                row.name = Some(update.user.name.clone());
                row.presence = Some(update.user.presence);
            }
            SyncEvent::TransportControl(_) | SyncEvent::ClockSync(_) => {}
        }
    }

    /// Recomputes the throughput window: `ops_per_sec` over `elapsed`,
    /// resetting the per-peer counters that fed it.
    fn tick_throughput(&mut self, elapsed: Duration) {
        let ops_this_tick: u64 = self.rows.values().map(|r| r.ops_seen).sum();
        if elapsed.as_secs_f64() > 0.0 {
            self.ops_per_sec = ops_this_tick as f64 / elapsed.as_secs_f64();
        }
        for row in self.rows.values_mut() {
            row.ops_seen = 0;
        }
    }
}

fn render(frame: &mut Frame, state: &DashboardState, local_peer: PeerId, addr: SocketAddr) {
    let area = frame.area();
    let header = Paragraph::new(Line::from(format!(
        "tpt-av-sync dashboard — local peer {local_peer}, listening on {addr} — 'q' to quit"
    )))
    .block(Block::default().borders(Borders::BOTTOM));

    let header_height = 2;
    let footer_height = 2;
    let table_height = area.height.saturating_sub(header_height + footer_height);

    let header_area = ratatui::layout::Rect { height: header_height, ..area };
    let table_area = ratatui::layout::Rect {
        y: area.y + header_height,
        height: table_height,
        ..area
    };
    let footer_area = ratatui::layout::Rect {
        y: area.y + header_height + table_height,
        height: footer_height,
        ..area
    };

    frame.render_widget(header, header_area);

    let rows: Vec<Row> = state
        .rows
        .iter()
        .map(|(peer, row)| {
            let presence = row
                .presence
                .map(|p| format!("{p:?}"))
                .unwrap_or_else(|| "unknown".to_string());
            let playhead = row
                .playhead
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".to_string());
            Row::new(vec![
                Cell::from(peer.to_string()),
                Cell::from(row.name.clone().unwrap_or_else(|| "-".to_string())),
                Cell::from(presence),
                Cell::from(playhead),
                Cell::from(row.ops_seen.to_string()),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(20),
            Constraint::Length(16),
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Length(10),
        ],
    )
    .header(Row::new(vec!["peer", "name", "presence", "playhead", "ops/tick"]))
    .block(Block::default().borders(Borders::ALL).title("peers"));

    frame.render_widget(table, table_area);

    let footer = Paragraph::new(Line::from(format!(
        "total ops applied: {}   throughput: {:.1} ops/s",
        state.total_ops, state.ops_per_sec
    )));
    frame.render_widget(footer, footer_area);
}

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let bind: SocketAddr = args
        .first()
        .ok_or_else(|| "usage: dashboard <bind-addr> [peer-addr]".to_string())?
        .parse()
        .map_err(|e| format!("invalid bind address: {e}"))?;
    let peer_addr: Option<SocketAddr> = match args.get(1) {
        Some(s) => Some(s.parse().map_err(|e| format!("invalid peer address: {e}"))?),
        None => None,
    };

    let local_peer = PeerId::generate();
    let (transport, addr) =
        TcpTransport::listen(bind, local_peer).map_err(|e| format!("listen: {e}"))?;
    if let Some(peer_addr) = peer_addr {
        transport
            .connect(peer_addr)
            .map_err(|e| format!("connecting to {peer_addr}: {e}"))?;
    }
    let mut engine = SyncEngine::new(TimelineCrdt::new(local_peer), Box::new(transport));

    let mut stdout = std::io::stdout();
    enable_raw_mode().map_err(|e| e.to_string())?;
    stdout.execute(EnterAlternateScreen).map_err(|e| e.to_string())?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).map_err(|e| e.to_string())?;

    let result = run_loop(&mut terminal, &mut engine, local_peer, addr);

    disable_raw_mode().map_err(|e| e.to_string())?;
    terminal
        .backend_mut()
        .execute(LeaveAlternateScreen)
        .map_err(|e| e.to_string())?;
    result
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    engine: &mut SyncEngine,
    local_peer: PeerId,
    addr: SocketAddr,
) -> Result<(), String> {
    let mut state = DashboardState::default();
    let tick_rate = Duration::from_millis(250);
    let mut last_tick = Instant::now();

    loop {
        engine.process_messages();
        for event in engine.take_events() {
            state.apply(event);
        }

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout).map_err(|e| e.to_string())? {
            if let Event::Key(key) = event::read().map_err(|e| e.to_string())? {
                if matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                    || (key.code == KeyCode::Char('c')
                        && key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL))
                {
                    return Ok(());
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            state.tick_throughput(last_tick.elapsed());
            last_tick = Instant::now();
            terminal
                .draw(|frame| render(frame, &state, local_peer, addr))
                .map_err(|e| e.to_string())?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    #[test]
    fn presence_and_playhead_events_populate_rows() {
        let mut state = DashboardState::default();
        let peer = PeerId::from_u64(1);
        state.apply(SyncEvent::PeerJoined(peer));
        state.apply(SyncEvent::Playhead(tpt_av_sync_playhead::PlayheadUpdate {
            peer_id: peer,
            position: 48_000,
            timestamp_ms: 0,
            playing: true,
        }));
        let row = state.rows.get(&peer).expect("row exists");
        assert_eq!(row.playhead, Some(48_000));
    }

    #[test]
    fn throughput_counts_ops_and_resets() {
        let mut state = DashboardState::default();
        let peer = PeerId::from_u64(1);
        for lamport in 1..=5_u64 {
            state.apply(SyncEvent::RemoteOperation(tpt_av_sync_crdt::TaggedOperation {
                op_id: tpt_av_sync_utils::OperationId::new(lamport, peer),
                operation: tpt_av_sync_crdt::TimelineOperation::InsertTrack {
                    track_id: tpt_av_sync_crdt::TrackId::from_u64(1),
                    track: tpt_av_sync_crdt::TrackData::new("A1"),
                    position: 0,
                },
                lamport_ts: lamport,
                vector_clock: tpt_av_sync_utils::VectorClock::new(),
                peer_id: peer,
                timestamp: std::time::SystemTime::UNIX_EPOCH,
            }));
        }
        assert_eq!(state.total_ops, 5);
        state.tick_throughput(Duration::from_secs(1));
        assert!((state.ops_per_sec - 5.0).abs() < f64::EPSILON);
        assert_eq!(state.rows[&peer].ops_seen, 0, "per-tick counter resets");
    }

    #[test]
    fn render_does_not_panic_on_a_small_terminal() {
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut state = DashboardState::default();
        state.apply(SyncEvent::PeerJoined(PeerId::from_u64(1)));
        terminal
            .draw(|frame| {
                render(
                    frame,
                    &state,
                    PeerId::from_u64(0),
                    "127.0.0.1:9000".parse().unwrap(),
                )
            })
            .unwrap();
    }
}
