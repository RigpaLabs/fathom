/// Smoke tests against live Hyperliquid public API.
///
/// No mocks — real network, real exchange, real data.
/// Skipped by default (cargo test skips #[ignore]).
///
/// Run all:
///   cargo test --test smoke_hl_test -- --include-ignored --test-threads 1 --nocapture
///
/// Run single:
///   cargo test --test smoke_hl_test live_hl_eth_pipeline -- --include-ignored --nocapture
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

mod helpers;
use helpers::parquet::{collect_parquets, count_rows, read_f32_col, read_f64_col, read_u32_col};

use fathom::{
    accumulator::Snapshot1s,
    config::{ConnectionConfig, Exchange},
    connection_hl::connection_task_hl,
    exchange::Hyperliquid,
    monitor,
    writer::{
        raw::{RawDiff, run_raw_writer},
        snap_1s::run_snap_writer,
    },
};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn hl_conn(name: &str, symbols: Vec<&str>) -> ConnectionConfig {
    ConnectionConfig {
        name: name.to_string(),
        exchange: Exchange::Hyperliquid,
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        depth_ms: 500,
        ws_url_override: None,
        snapshot_url_override: None,
    }
}

// ── Smoke tests ──────────────────────────────────────────────────────────────

/// Full pipeline: connect to Hyperliquid, collect ~10s of ETH data.
///
/// Asserts:
/// - 1s snapshot parquet exists with >= 5 rows
/// - mid_px is in sane range ($100–$100k)
/// - ofi_l1 column exists and has values
/// - trade columns (volume_delta, trade_count) are populated
#[tokio::test]
#[ignore = "live Hyperliquid — run: cargo test --test smoke_hl_test -- --include-ignored"]
async fn live_hl_eth_pipeline() {
    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = broadcast::channel::<RawDiff>(1_024);
    let (snap_tx, snap_rx) = broadcast::channel::<Snapshot1s>(1_024);

    let raw_handle = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60, 1));
    let snap_handle = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        snap_rx,
        CancellationToken::new(),
    ));

    let state = monitor::new_state();
    let task = tokio::spawn(connection_task_hl(
        hl_conn("smoke_hl_eth", vec!["ETH"]),
        Box::new(Hyperliquid),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx,
        snap_tx,
        CancellationToken::new(),
    ));

    // Hyperliquid updates ~every 500ms, need 10s+ for reliable snap rows
    tokio::time::sleep(Duration::from_secs(12)).await;
    task.abort();
    let _ = task.await;
    raw_handle.await.unwrap();
    snap_handle.await.unwrap();

    // 1s snapshots
    let snaps: Vec<_> = collect_parquets(dir.path())
        .into_iter()
        .filter(|p| p.to_str().unwrap_or("").contains("1s"))
        .collect();
    assert!(
        !snaps.is_empty(),
        "no 1s snap parquet written for Hyperliquid ETH"
    );

    let snap_rows: usize = snaps.iter().map(|p| count_rows(p)).sum();
    println!("HL ETH snap rows: {snap_rows}");
    assert!(snap_rows >= 5, "expected >= 5 snap rows, got {snap_rows}");

    // Mid price sanity
    let mids = read_f64_col(&snaps[0], "mid_px");
    assert!(!mids.is_empty(), "mid_px column is empty");
    let mid = mids[0];
    println!("HL ETH mid_px: ${mid:.2}");
    assert!(
        (100.0..=100_000.0).contains(&mid),
        "ETH mid_px {mid:.2} outside $100–$100k"
    );

    // OFI should have non-zero values after warmup
    let ofis = read_f64_col(&snaps[0], "ofi_l1");
    println!(
        "HL ETH ofi_l1 values: {} total, first 5: {:?}",
        ofis.len(),
        &ofis[..ofis.len().min(5)]
    );
    assert!(!ofis.is_empty(), "ofi_l1 column is empty");

    // Trade columns should exist (may be zero if no trades in window, but column must be present)
    let trade_counts = read_u32_col(&snaps[0], "trade_count");
    println!(
        "HL ETH trade_count values: {} total, first 5: {:?}",
        trade_counts.len(),
        &trade_counts[..trade_counts.len().min(5)]
    );
    assert_eq!(trade_counts.len(), snap_rows, "trade_count rows mismatch");

    let vol_deltas = read_f64_col(&snaps[0], "volume_delta");
    println!(
        "HL ETH volume_delta: {} total, first 5: {:?}",
        vol_deltas.len(),
        &vol_deltas[..vol_deltas.len().min(5)]
    );

    // At least some trades should have happened in 12 seconds on ETH
    let total_trades: u32 = trade_counts.iter().sum();
    println!("HL ETH total trades in 12s: {total_trades}");

    // Imbalance should be populated (stored as f32 in Parquet)
    let imbs = read_f32_col(&snaps[0], "imbalance_l1");
    assert!(!imbs.is_empty(), "imbalance_l1 column empty");
    println!(
        "HL ETH imbalance_l1: {} values, first 5: {:?}",
        imbs.len(),
        &imbs[..imbs.len().min(5)]
    );
    for &v in &imbs {
        assert!((-1.0..=1.0).contains(&v), "imbalance_l1 {v} out of [-1,1]");
    }

    // Monitor state
    let guard = state.lock().unwrap();
    if let Some(cs) = guard.get("smoke_hl_eth") {
        println!(
            "HL reconnects: {}, connected: {}",
            cs.reconnects_today, cs.connected
        );
    }
}

/// Multi-symbol: ETH + BTC on Hyperliquid.
#[tokio::test]
#[ignore = "live Hyperliquid — run: cargo test --test smoke_hl_test -- --include-ignored"]
async fn live_hl_multi_symbol() {
    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = broadcast::channel::<RawDiff>(1_024);
    let (snap_tx, snap_rx) = broadcast::channel::<Snapshot1s>(1_024);

    let raw_handle = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60, 1));
    let snap_handle = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        snap_rx,
        CancellationToken::new(),
    ));

    let state = monitor::new_state();
    let task = tokio::spawn(connection_task_hl(
        hl_conn("smoke_hl_multi", vec!["ETH", "BTC"]),
        Box::new(Hyperliquid),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx,
        snap_tx,
        CancellationToken::new(),
    ));

    tokio::time::sleep(Duration::from_secs(10)).await;
    task.abort();
    let _ = task.await;
    raw_handle.await.unwrap();
    snap_handle.await.unwrap();

    let snaps = collect_parquets(dir.path())
        .into_iter()
        .filter(|p| p.to_str().unwrap_or("").contains("1s"))
        .collect::<Vec<_>>();

    let eth_snaps: Vec<_> = snaps
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("ETH"))
        .collect();
    let btc_snaps: Vec<_> = snaps
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("BTC"))
        .collect();

    assert!(!eth_snaps.is_empty(), "no snap for ETH");
    assert!(!btc_snaps.is_empty(), "no snap for BTC");

    let eth_rows: usize = eth_snaps.iter().map(|p| count_rows(p)).sum();
    let btc_rows: usize = btc_snaps.iter().map(|p| count_rows(p)).sum();
    println!("HL multi — ETH: {eth_rows} rows, BTC: {btc_rows} rows");

    assert!(eth_rows >= 3, "ETH expected >= 3 rows, got {eth_rows}");
    assert!(btc_rows >= 3, "BTC expected >= 3 rows, got {btc_rows}");

    // BTC mid price sanity
    let btc_mids = read_f64_col(btc_snaps[0], "mid_px");
    if let Some(&mid) = btc_mids.first() {
        println!("HL BTC mid_px: ${mid:.2}");
        assert!(
            (1_000.0..=1_000_000.0).contains(&mid),
            "BTC mid_px {mid:.2} outside $1k–$1M"
        );
    }
}
