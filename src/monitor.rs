use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde::Serialize;
use tokio::time;
use tracing::warn;

#[derive(Debug, Default, Clone)]
pub struct SymbolStats {
    pub last_event_at: Option<Instant>,
    pub gaps_today: u64,
}

#[derive(Debug, Default, Clone)]
pub struct ConnStats {
    pub connected: bool,
    pub reconnects_today: u64,
    pub symbols: HashMap<String, SymbolStats>,
}

/// Shared state updated by connection tasks.
pub type MonitorState = Arc<Mutex<HashMap<String, ConnStats>>>;

pub fn new_state() -> MonitorState {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Lock MonitorState, recovering from poison (another task panicked).
/// This is always safe: the HashMap is append-only metadata — a poisoned
/// lock just means one snapshot of ConnStats may be stale.
pub fn lock_state(state: &MonitorState) -> std::sync::MutexGuard<'_, HashMap<String, ConnStats>> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Serialize)]
struct StatusJson {
    updated_at: String,
    uptime_s: u64,
    connections: HashMap<String, ConnStatusJson>,
}

#[derive(Serialize)]
struct ConnStatusJson {
    connected: bool,
    reconnects_today: u64,
    symbols: HashMap<String, SymbolStatusJson>,
}

#[derive(Serialize)]
struct SymbolStatusJson {
    last_event_age_s: f64,
    gaps_today: u64,
}

/// Tokio task: writes status.json every 30s.
pub async fn run_monitor(data_dir: PathBuf, state: MonitorState, start: Instant) {
    let status_dir = data_dir.join("metadata");
    let _ = std::fs::create_dir_all(&status_dir);
    let status_path = status_dir.join("status.json");
    let mut interval = time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        let now = Instant::now();
        let uptime_s = now.duration_since(start).as_secs();
        let updated_at = chrono::Utc::now().to_rfc3339();

        let conns_snap = {
            let guard = lock_state(&state);
            guard.clone()
        };

        let mut connections = HashMap::new();
        for (conn_name, conn) in &conns_snap {
            let mut symbols = HashMap::new();
            for (sym, stats) in &conn.symbols {
                let last_event_age_s = stats
                    .last_event_at
                    .map(|t| now.duration_since(t).as_secs_f64())
                    .unwrap_or(f64::INFINITY);

                if last_event_age_s > 60.0 {
                    warn!(
                        conn = %conn_name,
                        symbol = %sym,
                        age_s = last_event_age_s,
                        "stale symbol — no events for >60s"
                    );
                }
                if stats.gaps_today > 3 {
                    warn!(
                        conn = %conn_name,
                        symbol = %sym,
                        gaps = stats.gaps_today,
                        "high gap count today"
                    );
                }

                symbols.insert(
                    sym.clone(),
                    SymbolStatusJson {
                        last_event_age_s,
                        gaps_today: stats.gaps_today,
                    },
                );
            }

            connections.insert(
                conn_name.clone(),
                ConnStatusJson {
                    connected: conn.connected,
                    reconnects_today: conn.reconnects_today,
                    symbols,
                },
            );
        }

        let status = StatusJson {
            updated_at,
            uptime_s,
            connections,
        };

        match serde_json::to_string_pretty(&status) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&status_path, json) {
                    warn!(error = %e, "failed to write status.json");
                }
            }
            Err(e) => warn!(error = %e, "failed to serialize status"),
        }
    }
}
