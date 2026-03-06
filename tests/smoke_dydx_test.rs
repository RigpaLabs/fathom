/// Smoke tests against live dYdX v4 Indexer API.
///
/// No mocks — real network, real exchange, real data.
/// Skipped by default (cargo test skips #[ignore]).
///
/// Run all:
///   cargo test --test smoke_dydx_test -- --include-ignored --test-threads 1 --nocapture
///
/// Run single:
///   cargo test --test smoke_dydx_test live_dydx_eth_pipeline -- --include-ignored --nocapture
use std::{path::Path, time::Duration};

use arrow::array::{Array, Float64Array, UInt32Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use tokio::sync::mpsc;

use fathom::{
    accumulator::Snapshot1s,
    config::{ConnectionConfig, Exchange},
    connection_dydx::connection_task_dydx,
    monitor,
    writer::{
        raw::{RawDiff, run_raw_writer},
        snap_1s::run_snap_writer,
    },
};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn collect_parquets(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(collect_parquets(&path));
            } else if path.extension().map_or(false, |e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out
}

fn count_rows(path: &Path) -> usize {
    let file = std::fs::File::open(path).expect("parquet file");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

fn read_f64_col(path: &Path, col: &str) -> Vec<f64> {
    let file = std::fs::File::open(path).expect("parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        if let Some(arr) = batch.column_by_name(col) {
            if let Some(fa) = arr.as_any().downcast_ref::<Float64Array>() {
                for i in 0..fa.len() {
                    if !fa.is_null(i) {
                        values.push(fa.value(i));
                    }
                }
            }
        }
    }
    values
}

fn read_u32_col(path: &Path, col: &str) -> Vec<u32> {
    let file = std::fs::File::open(path).expect("parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        if let Some(arr) = batch.column_by_name(col) {
            if let Some(ua) = arr.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..ua.len() {
                    if !ua.is_null(i) {
                        values.push(ua.value(i));
                    }
                }
            }
        }
    }
    values
}

fn dydx_conn(name: &str, symbols: Vec<&str>) -> ConnectionConfig {
    ConnectionConfig {
        name: name.to_string(),
        exchange: Exchange::Dydx,
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        depth_ms: 250,
        ws_url_override: None,
        snapshot_url_override: None,
    }
}

// ── Smoke tests ──────────────────────────────────────────────────────────────

/// Full pipeline: connect to dYdX v4, collect ~12s of ETH-USD data.
///
/// Asserts:
/// - 1s snapshot parquet exists with >= 5 rows
/// - mid_px is in sane range ($100–$100k)
/// - ofi_l1 column has values
/// - Trade columns present
#[tokio::test]
#[ignore = "live dYdX v4 — run: cargo test --test smoke_dydx_test -- --include-ignored"]
async fn live_dydx_eth_pipeline() {
    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(1_024);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(1_024);

    let raw_handle = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60, 1));
    let snap_handle = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    let task = tokio::spawn(connection_task_dydx(
        dydx_conn("smoke_dydx_eth", vec!["ETH-USD"]),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx,
        snap_tx,
    ));

    // dYdX batched updates ~250ms, give 12s for reliable data
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
    assert!(!snaps.is_empty(), "no 1s snap parquet written for dYdX ETH-USD");

    let snap_rows: usize = snaps.iter().map(|p| count_rows(p)).sum();
    println!("dYdX ETH-USD snap rows: {snap_rows}");
    assert!(snap_rows >= 5, "expected >= 5 snap rows, got {snap_rows}");

    // Mid price sanity
    let mids = read_f64_col(&snaps[0], "mid_px");
    assert!(!mids.is_empty(), "mid_px column is empty");
    let mid = mids[0];
    println!("dYdX ETH-USD mid_px: ${mid:.2}");
    assert!(
        (100.0..=100_000.0).contains(&mid),
        "ETH-USD mid_px {mid:.2} outside $100–$100k"
    );

    // OFI values
    let ofis = read_f64_col(&snaps[0], "ofi_l1");
    println!(
        "dYdX ETH-USD ofi_l1: {} values, first 5: {:?}",
        ofis.len(),
        &ofis[..ofis.len().min(5)]
    );
    assert!(!ofis.is_empty(), "ofi_l1 column is empty");

    // Trade columns
    let trade_counts = read_u32_col(&snaps[0], "trade_count");
    println!(
        "dYdX ETH-USD trade_count: {} values, first 5: {:?}",
        trade_counts.len(),
        &trade_counts[..trade_counts.len().min(5)]
    );
    assert_eq!(trade_counts.len(), snap_rows, "trade_count rows mismatch");

    let vol_deltas = read_f64_col(&snaps[0], "volume_delta");
    println!(
        "dYdX ETH-USD volume_delta: {} values, first 5: {:?}",
        vol_deltas.len(),
        &vol_deltas[..vol_deltas.len().min(5)]
    );

    let total_trades: u32 = trade_counts.iter().sum();
    println!("dYdX ETH-USD total trades in 12s: {total_trades}");

    // Raw diffs should also be written
    let raws: Vec<_> = collect_parquets(dir.path())
        .into_iter()
        .filter(|p| p.to_str().unwrap_or("").contains("raw"))
        .collect();
    if !raws.is_empty() {
        let raw_rows: usize = raws.iter().map(|p| count_rows(p)).sum();
        println!("dYdX ETH-USD raw rows: {raw_rows}");
    }

    // Monitor
    let guard = state.lock().unwrap();
    if let Some(cs) = guard.get("smoke_dydx_eth") {
        println!(
            "dYdX reconnects: {}, connected: {}",
            cs.reconnects_today, cs.connected
        );
        assert_eq!(cs.reconnects_today, 0, "unexpected reconnects in 12s run");
    }
}

/// Multi-symbol: ETH-USD + BTC-USD on dYdX.
#[tokio::test]
#[ignore = "live dYdX v4 — run: cargo test --test smoke_dydx_test -- --include-ignored"]
async fn live_dydx_multi_symbol() {
    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = mpsc::channel::<RawDiff>(1_024);
    let (snap_tx, snap_rx) = mpsc::channel::<Snapshot1s>(1_024);

    let raw_handle = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), raw_rx, 60, 1));
    let snap_handle = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), snap_rx));

    let state = monitor::new_state();
    let task = tokio::spawn(connection_task_dydx(
        dydx_conn("smoke_dydx_multi", vec!["ETH-USD", "BTC-USD"]),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx,
        snap_tx,
    ));

    tokio::time::sleep(Duration::from_secs(12)).await;
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
        .filter(|p| p.to_str().unwrap_or("").contains("ETH-USD"))
        .collect();
    let btc_snaps: Vec<_> = snaps
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("BTC-USD"))
        .collect();

    assert!(!eth_snaps.is_empty(), "no snap for ETH-USD");
    assert!(!btc_snaps.is_empty(), "no snap for BTC-USD");

    let eth_rows: usize = eth_snaps.iter().map(|p| count_rows(p)).sum();
    let btc_rows: usize = btc_snaps.iter().map(|p| count_rows(p)).sum();
    println!("dYdX multi — ETH-USD: {eth_rows} rows, BTC-USD: {btc_rows} rows");

    assert!(eth_rows >= 3, "ETH-USD expected >= 3 rows, got {eth_rows}");
    assert!(btc_rows >= 3, "BTC-USD expected >= 3 rows, got {btc_rows}");

    // BTC-USD mid price sanity
    let btc_mids = read_f64_col(btc_snaps[0], "mid_px");
    if let Some(&mid) = btc_mids.first() {
        println!("dYdX BTC-USD mid_px: ${mid:.2}");
        assert!(
            (1_000.0..=1_000_000.0).contains(&mid),
            "BTC-USD mid_px {mid:.2} outside $1k–$1M"
        );
    }
}
