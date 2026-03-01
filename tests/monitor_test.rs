use std::time::Instant;

use fathom::monitor::{ConnStats, MonitorState, SymbolStats, new_state, run_monitor};

// ── MonitorState ────────────────────────────────────────────────────────────

#[test]
fn test_new_state_empty() {
    let state = new_state();
    let guard = state.lock().unwrap();
    assert!(guard.is_empty());
}

#[test]
fn test_conn_stats_default() {
    let cs = ConnStats::default();
    assert!(!cs.connected);
    assert_eq!(cs.reconnects_today, 0);
    assert!(cs.symbols.is_empty());
}

#[test]
fn test_symbol_stats_default() {
    let ss = SymbolStats::default();
    assert!(ss.last_event_at.is_none());
    assert_eq!(ss.gaps_today, 0);
}

#[test]
fn test_state_update_and_read() {
    let state: MonitorState = new_state();

    // Simulate connection task updating state
    {
        let mut guard = state.lock().unwrap();
        let cs = guard.entry("spot".to_string()).or_default();
        cs.connected = true;
        cs.reconnects_today = 3;
        let ss = cs.symbols.entry("ETHUSDT".to_string()).or_default();
        ss.last_event_at = Some(Instant::now());
        ss.gaps_today = 2;
    }

    // Read back
    let guard = state.lock().unwrap();
    let cs = guard.get("spot").unwrap();
    assert!(cs.connected);
    assert_eq!(cs.reconnects_today, 3);
    let ss = cs.symbols.get("ETHUSDT").unwrap();
    assert_eq!(ss.gaps_today, 2);
    assert!(ss.last_event_at.is_some());
}

#[test]
fn test_state_multiple_connections() {
    let state = new_state();

    {
        let mut guard = state.lock().unwrap();
        guard.entry("spot".to_string()).or_default().connected = true;
        guard.entry("perp".to_string()).or_default().connected = false;
    }

    let guard = state.lock().unwrap();
    assert_eq!(guard.len(), 2);
    assert!(guard["spot"].connected);
    assert!(!guard["perp"].connected);
}

// ── run_monitor writes status.json ──────────────────────────────────────────

#[tokio::test]
async fn test_monitor_writes_status_json() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state();
    let start = Instant::now();

    {
        let mut guard = state.lock().unwrap();
        let cs = guard.entry("test_conn".to_string()).or_default();
        cs.connected = true;
        cs.symbols
            .entry("BTCUSDT".to_string())
            .or_default()
            .last_event_at = Some(Instant::now());
    }

    // Spawn monitor, let it tick once, then abort
    let handle = tokio::spawn(run_monitor(dir.path().to_path_buf(), state.clone(), start));
    // First tick is immediate due to interval behavior
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    handle.abort();
    let _ = handle.await;

    let status_path = dir.path().join("metadata").join("status.json");
    assert!(status_path.exists(), "status.json should be written");

    let content = std::fs::read_to_string(&status_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(json.get("updated_at").is_some());
    assert!(json.get("uptime_s").is_some());
    assert!(json.get("connections").is_some());

    let conns = json["connections"].as_object().unwrap();
    assert!(conns.contains_key("test_conn"));
    assert!(conns["test_conn"]["connected"].as_bool().unwrap());
}
