/// E2E tests using MockBinanceServer (axum) for realistic reconnect/multi-symbol/failure scenarios.
///
/// These tests complement `integration_test.rs` (raw TCP mocks) with:
///  - Proper HTTP routing per symbol (GET /depth?symbol=X)
///  - Multiple WS connections in one test (reconnect, gap)
///  - HTTP error injection for retry testing
///  - Snap 1s parquet creation via flush-boundary timing
mod helpers;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use tokio::sync::mpsc;

use fathom::{
    accumulator::Snapshot1s,
    config::{ConnectionConfig, Exchange},
    connection::connection_task,
    exchange::BinanceSpot,
    monitor,
    writer::raw::{run_raw_writer, RawDiff},
    writer::snap_1s::run_snap_writer,
};
use helpers::MockBinanceServer;

// ── Message helpers ────────────────────────────────────────────────────────────

/// Build a Binance combined-stream depth update JSON string.
fn ws_msg(
    sym: &str,
    event_ms: i64,
    first_update_id: i64,
    final_update_id: i64,
    bids: Vec<(&str, &str)>,
    asks: Vec<(&str, &str)>,
) -> String {
    let bids_json: Vec<[&str; 2]> = bids.iter().map(|(p, q)| [*p, *q]).collect();
    let asks_json: Vec<[&str; 2]> = asks.iter().map(|(p, q)| [*p, *q]).collect();
    serde_json::json!({
        "stream": format!("{}@depth@100ms", sym.to_lowercase()),
        "data": {
            "E": event_ms,
            "U": first_update_id,
            "u": final_update_id,
            "b": bids_json,
            "a": asks_json,
        }
    })
    .to_string()
}

// ── Parquet helpers ────────────────────────────────────────────────────────────

fn find_parquets(dir: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    collect_parquets(dir, &mut result);
    result
}

fn collect_parquets(dir: &Path, acc: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_parquets(&path, acc);
            } else if path.extension().map_or(false, |e| e == "parquet") {
                acc.push(path);
            }
        }
    }
}

fn count_parquet_rows(path: &Path) -> usize {
    let file = std::fs::File::open(path).expect("parquet file should exist");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

fn raw_parquets(dir: &Path) -> Vec<PathBuf> {
    find_parquets(dir)
        .into_iter()
        .filter(|p| p.components().any(|c| c.as_os_str() == "raw"))
        .collect()
}

fn snap_parquets(dir: &Path) -> Vec<PathBuf> {
    find_parquets(dir)
        .into_iter()
        .filter(|p| p.components().any(|c| c.as_os_str() == "1s"))
        .collect()
}

// ── Infrastructure helpers ────────────────────────────────────────────────────

fn make_conn(name: &str, symbols: Vec<&str>, server: &MockBinanceServer) -> ConnectionConfig {
    ConnectionConfig {
        name: name.to_string(),
        exchange: Exchange::BinanceSpot,
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        depth_ms: 100,
        ws_url_override: Some(server.ws_url()),
        snapshot_url_override: Some(server.snapshot_url_template()),
    }
}

// ── Test 1: reconnect after WS close ─────────────────────────────────────────

/// WS closes twice.  connection_task should reconnect, accumulate 6 raw rows total.
///
/// Round 1: sync (U=100,u=101) + 2 post-sync → 3 raw rows → WS close
///          → connection_task: reconnects_today += 1 → backoff ~1 s
/// Round 2: same sequence (fresh book, same snapshot) → 3 raw rows → WS close
/// Total: 6 raw rows, 2 WS connections.
#[tokio::test]
async fn test_e2e_reconnect_after_ws_close() {
    let server = MockBinanceServer::new().await;

    // Snapshot persists across both reconnects (lastUpdateId = 100).
    server.push_snapshot(
        "ETHUSDT",
        100,
        vec![("3000.00", "5.00"), ("2999.00", "3.00")],
        vec![("3001.00", "4.00"), ("3002.00", "2.00")],
    );

    // Round 1 messages (3 applied events)
    let round1 = vec![
        ws_msg("ethusdt", 1_700_000_001_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_002_000, 102, 102, vec![("3000.00", "6.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_003_000, 103, 103, vec![], vec![("3001.00", "2.00")]),
    ];
    // Round 2: same sequence — after reset/re-sync, book starts fresh
    let round2 = vec![
        ws_msg("ethusdt", 1_700_000_010_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_011_000, 102, 102, vec![("3000.00", "6.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_012_000, 103, 103, vec![], vec![("3001.00", "2.00")]),
    ];
    server.push_ws_round(round1);
    server.push_ws_round(round2);

    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(128);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(128);
    let raw_w = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60));
    let snap_w = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    let conn = make_conn("reconnect_test", vec!["ETHUSDT"], &server);
    let task = tokio::spawn(connection_task(
        conn,
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
    ));

    // Round 1 (~50 ms) + backoff (~1 000–1 250 ms) + round 2 (~50 ms) + slack.
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    assert!(
        server.connected_count() >= 2,
        "should have opened at least 2 WS connections (got {})",
        server.connected_count()
    );

    let raws = raw_parquets(dir.path());
    assert_eq!(raws.len(), 1, "one raw parquet file for ETHUSDT");

    let rows = count_parquet_rows(&raws[0]);
    assert_eq!(rows, 6, "3 raw events per round × 2 rounds = 6");
}

// ── Test 2: two symbols on one connection ─────────────────────────────────────

/// Single ConnectionConfig with ETHUSDT + BTCUSDT.
/// WS sends interleaved messages for both symbols.
/// Expected: two separate raw parquet files (one per symbol).
#[tokio::test]
async fn test_e2e_multi_symbol_single_connection() {
    let server = MockBinanceServer::new().await;

    server.push_snapshot(
        "ETHUSDT",
        100,
        vec![("3000.00", "5.00")],
        vec![("3001.00", "4.00")],
    );
    server.push_snapshot(
        "BTCUSDT",
        200,
        vec![("50000.00", "1.00")],
        vec![("50001.00", "1.00")],
    );

    // Interleaved: ETH sync, BTC sync, ETH post, BTC post → 2 rows each
    let round = vec![
        ws_msg("ethusdt", 1_700_000_001_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("btcusdt", 1_700_000_001_001, 200, 201, vec![("50000.00", "1.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_002_000, 102, 102, vec![("3000.00", "6.00")], vec![]),
        ws_msg("btcusdt", 1_700_000_002_001, 202, 202, vec![("50000.00", "1.20")], vec![]),
    ];
    server.push_ws_round(round);

    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(128);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(128);
    let raw_w = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60));
    let snap_w = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    let conn = make_conn("multi_sym", vec!["ETHUSDT", "BTCUSDT"], &server);
    let task = tokio::spawn(connection_task(
        conn,
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    let raws = raw_parquets(dir.path());
    assert_eq!(
        raws.len(),
        2,
        "one raw parquet per symbol (ETHUSDT + BTCUSDT); found: {:?}",
        raws
    );

    let total_rows: usize = raws.iter().map(|p| count_parquet_rows(p)).sum();
    assert_eq!(total_rows, 4, "2 rows per symbol × 2 symbols = 4 total");
}

// ── Test 3: snapshot HTTP 500 → backoff → retry succeeds ─────────────────────

/// First snapshot request returns 500.  connection_task backs off and retries.
/// The second request succeeds and data flows normally.
///
/// Round 1: WS connects, but snapshot returns 500 → WS is dropped before any
///          messages are processed.
/// Round 2: WS connects after backoff, snapshot succeeds → data flows.
#[tokio::test]
async fn test_e2e_snapshot_http_error_then_retry() {
    let server = MockBinanceServer::new().await;

    // First request → 500 (consumed), subsequent → snapshot JSON.
    server.push_http_error("ETHUSDT", 500);
    server.push_snapshot(
        "ETHUSDT",
        100,
        vec![("3000.00", "5.00")],
        vec![("3001.00", "4.00")],
    );

    // Round 1: no useful messages (connection drops after snapshot fails).
    server.push_ws_round(vec![]);
    // Round 2: sync + post-sync messages.
    server.push_ws_round(vec![
        ws_msg("ethusdt", 1_700_000_001_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_002_000, 102, 102, vec![("3000.00", "6.00")], vec![]),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(128);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(128);
    let raw_w = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60));
    let snap_w = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    let conn = make_conn("retry_test", vec!["ETHUSDT"], &server);
    let task = tokio::spawn(connection_task(
        conn,
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
    ));

    // Attempt 1 fails fast + backoff (~1 s) + attempt 2 (~100 ms) + slack.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    let raws = raw_parquets(dir.path());
    assert!(!raws.is_empty(), "raw parquet should exist after successful retry");
    let rows: usize = raws.iter().map(|p| count_parquet_rows(p)).sum();
    assert!(rows > 0, "at least some raw events should have been written");
}

// ── Test 4: OrderBook gap triggers reconnect ──────────────────────────────────

/// A deliberately crafted gap in update IDs causes connection_task to break
/// out of the inner loop, increment gaps_today, and reconnect.
///
/// Round 1: sync (U=100,u=101) → gap (U=200, expected 102) → reconnect
/// Round 2: sync + normal stream → connection_task processes cleanly
#[tokio::test]
async fn test_e2e_gap_triggers_reconnect() {
    let server = MockBinanceServer::new().await;

    server.push_snapshot(
        "ETHUSDT",
        100,
        vec![("3000.00", "5.00")],
        vec![("3001.00", "4.00")],
    );

    // Round 1: one sync event, then a gap (U=200 when 102 expected)
    server.push_ws_round(vec![
        ws_msg("ethusdt", 1_700_000_001_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_002_000, 200, 200, vec![("3000.00", "9.00")], vec![]),
    ]);
    // Round 2: fresh sync after reconnect
    server.push_ws_round(vec![
        ws_msg("ethusdt", 1_700_000_010_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_011_000, 102, 102, vec![("3000.00", "6.00")], vec![]),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(128);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(128);
    let raw_w = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60));
    let snap_w = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    let state_check = Arc::clone(&state);
    let conn = make_conn("gap_test", vec!["ETHUSDT"], &server);
    let task = tokio::spawn(connection_task(
        conn,
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
    ));

    // Gap detected in round 1 + backoff (~1 s) + round 2 + slack.
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    let guard = state_check.lock().unwrap();
    let cs = guard.get("gap_test").expect("gap_test connection in monitor");

    let eth_gaps = cs
        .symbols
        .get("ETHUSDT")
        .map(|s| s.gaps_today)
        .unwrap_or(0);
    assert!(eth_gaps >= 1, "ETHUSDT.gaps_today should be >= 1 (got {})", eth_gaps);
    assert!(
        cs.reconnects_today >= 1,
        "reconnects_today should be >= 1 (got {})",
        cs.reconnects_today
    );
}

// ── Test 6: partial snapshot failure — other symbols must not be affected ─────

/// When one symbol's REST snapshot returns HTTP 500, the connection must NOT
/// tear down the entire WS session for the other symbols.  BTCUSDT's snapshot
/// succeeds; its messages must be processed and written before ETHUSDT's first
/// event triggers a SnapshotRequired reconnect.
///
/// Bug A: the old code used `break` + `snapshots_ok = false` in the snapshot
/// for-loop, which caused `continue` on the outer loop, disconnecting every
/// symbol even if only one failed.
#[tokio::test]
async fn test_e2e_multi_symbol_partial_snapshot_failure() {
    let server = MockBinanceServer::new().await;

    // ETH: first request → 500, subsequent → ok (consumed one-by-one).
    server.push_http_error("ETHUSDT", 500);
    server.push_snapshot(
        "ETHUSDT",
        100,
        vec![("3000.00", "5.00")],
        vec![("3001.00", "4.00")],
    );
    // BTC: always ok.
    server.push_snapshot(
        "BTCUSDT",
        200,
        vec![("50000.00", "1.00")],
        vec![("50001.00", "1.00")],
    );

    // Round 1: BTC sync + post-sync arrive BEFORE ETH message.
    // ETH message triggers SnapshotRequired (book still unsynced) → reconnect.
    server.push_ws_round(vec![
        ws_msg("btcusdt", 1_700_000_001_000, 200, 201, vec![("50000.00", "1.00")], vec![]),
        ws_msg("btcusdt", 1_700_000_002_000, 202, 202, vec![("50000.00", "1.20")], vec![]),
        ws_msg("ethusdt", 1_700_000_003_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
    ]);
    // Round 2: both symbols synced after reconnect.
    server.push_ws_round(vec![
        ws_msg("ethusdt", 1_700_000_010_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_011_000, 102, 102, vec![("3000.00", "6.00")], vec![]),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(128);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(128);
    let raw_w = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60));
    let snap_w = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    // ETH listed first so its snapshot attempt fires first and returns 500.
    let conn = make_conn("partial_snap_test", vec!["ETHUSDT", "BTCUSDT"], &server);
    let task = tokio::spawn(connection_task(
        conn,
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
    ));

    // BTC round-1 msgs (~50 ms) + ETH triggers reconnect + backoff (~1 s) + round 2 + slack.
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    let raws = raw_parquets(dir.path());

    // BTC must have written rows from round 1 (before ETH triggered reconnect).
    let btc_rows: usize = raws
        .iter()
        .filter(|p| p.to_str().unwrap_or("").to_uppercase().contains("BTCUSDT"))
        .map(|p| count_parquet_rows(p))
        .sum();
    assert!(
        btc_rows >= 2,
        "BTCUSDT should have >= 2 raw rows from round 1 (got {}); \
         if 0, snapshot failure for ETHUSDT incorrectly killed the entire connection",
        btc_rows
    );
}

// ── Test 6: WS events buffered during slow snapshot fetch are not dropped ─────

/// Variant A correctness: the forwarder task buffers WS messages while REST
/// snapshot is being fetched.  A 300 ms snapshot delay simulates a slow network.
/// All 3 events sent at WS connect time must still appear in the raw parquet.
///
/// This validates that connection_task does not silently discard events that
/// arrive before the snapshot HTTP response returns.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_ws_buffered_during_slow_snapshot() {
    let server = MockBinanceServer::new().await;

    // Snapshot takes 300 ms to respond — WS events arrive before it returns.
    server.set_snapshot_delay_ms("ETHUSDT", 300);
    server.push_snapshot(
        "ETHUSDT",
        100,
        vec![("3000.00", "5.00")],
        vec![("3001.00", "4.00")],
    );

    // Three events queued immediately on WS connect (before snapshot returns).
    server.push_ws_round(vec![
        ws_msg("ethusdt", 1_700_000_001_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_002_000, 102, 102, vec![("3000.00", "6.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_003_000, 103, 103, vec![], vec![("3001.00", "2.00")]),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(128);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(128);
    let raw_w = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60));
    let snap_w = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    let conn = make_conn("slow_snap_test", vec!["ETHUSDT"], &server);
    let task = tokio::spawn(connection_task(
        conn,
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
    ));

    // Snapshot delay (300 ms) + event processing + writer flush + slack.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    let raws = raw_parquets(dir.path());
    assert!(!raws.is_empty(), "raw parquet must exist");
    let rows: usize = raws.iter().map(|p| count_parquet_rows(p)).sum();
    assert_eq!(
        rows, 3,
        "all 3 events must be processed despite slow snapshot fetch (got {rows})"
    );
}

// ── Test 5: snap 1s parquet is created ───────────────────────────────────────

/// Verifies that the 1s snapshot parquet file is written with the correct schema.
///
/// Mechanism: connection_task does an on-disconnect flush when the WS closes,
/// preserving any partially-accumulated 1s window before clearing state.
/// Round 2's events are flushed when the WS closes after sending its messages.
/// After abort the snap_writer closes and writes the parquet.
///
/// Marked multi_thread so the axum mock server can run concurrently.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_snap_parquet_created() {
    let server = MockBinanceServer::new().await;

    server.push_snapshot(
        "ETHUSDT",
        100,
        vec![("3000.00", "5.00")],
        vec![("3001.00", "4.00")],
    );

    // Round 1: sets last_1s_flush entry at t≈0.
    server.push_ws_round(vec![
        ws_msg("ethusdt", 1_700_000_001_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_002_000, 102, 102, vec![("3000.00", "6.00")], vec![]),
    ]);
    // Round 2: arrives after ~1 s backoff → first event triggers flush.
    server.push_ws_round(vec![
        ws_msg("ethusdt", 1_700_000_010_000, 100, 101, vec![("3000.00", "5.00")], vec![]),
        ws_msg("ethusdt", 1_700_000_011_000, 102, 102, vec![("3000.00", "6.50")], vec![]),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(128);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(128);
    let raw_w = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60));
    let snap_w = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    let conn = make_conn("snap_test", vec!["ETHUSDT"], &server);
    let task = tokio::spawn(connection_task(
        conn,
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
    ));

    // Round 1 (~50 ms) + backoff (~1 000–1 250 ms) + round 2 (~50 ms) + slack.
    // On-disconnect flush fires immediately when WS closes after round 2 messages.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    let snaps = snap_parquets(dir.path());
    assert!(
        !snaps.is_empty(),
        "snap 1s parquet should exist under 1s/; data_dir = {}",
        dir.path().display()
    );

    let snap_file = &snaps[0];
    let file = std::fs::File::open(snap_file).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();

    // Schema must have exactly 60 fields (see schema.rs → snap_1s_schema).
    assert_eq!(
        schema.fields().len(),
        60,
        "snap schema should have 60 columns (got {})",
        schema.fields().len()
    );

    // Spot-check a few field names.
    schema.field_with_name("ts_us").expect("ts_us field");
    schema.field_with_name("n_events").expect("n_events field");
    schema.field_with_name("ofi_l1").expect("ofi_l1 field");
    schema.field_with_name("bid_px_0").expect("bid_px_0 field");

    // At least one row written (the on-disconnect flush after round 2).
    let rows: usize = builder
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert!(rows > 0, "snap parquet should have at least 1 row");
}
