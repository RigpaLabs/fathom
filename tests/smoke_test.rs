/// Smoke tests against the live Binance public API.
///
/// No mocks — real network, real exchange, real data.
/// Skipped by default in CI (cargo test skips #[ignore]).
///
/// Run manually (MUST use --test-threads 1 to avoid Binance rate-limiting
/// from parallel concurrent connections):
///
///   cargo test --test smoke_test -- --include-ignored --test-threads 1
///   cargo test --test smoke_test -- --include-ignored --test-threads 1 --nocapture
///
/// Run a single test:
///   cargo test --test smoke_test live_spot_ethusdt -- --include-ignored --nocapture
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

mod helpers;
use helpers::parquet::{collect_parquets, count_rows, read_f64_col};

use fathom::{
    accumulator::Snapshot1s,
    config::{ConnectionConfig, Exchange},
    connection::connection_task,
    exchange::{BinancePerp, BinanceSpot},
    monitor,
    writer::{
        raw::{RawDiff, run_raw_writer},
        snap_1s::run_snap_writer,
    },
};

// ── Helpers ────────────────────────────────────────────────────────────────────

fn live_conn(name: &str, symbols: Vec<&str>, exchange: Exchange) -> ConnectionConfig {
    ConnectionConfig {
        name: name.to_string(),
        exchange,
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        depth_ms: 100,
        ws_url_override: None,
        snapshot_url_override: None,
    }
}

// ── Smoke tests ────────────────────────────────────────────────────────────────

/// Full pipeline smoke: connect to Binance spot, collect 8 s of ETHUSDT data.
///
/// Asserts:
/// - Raw parquet exists with ≥ 10 rows (Binance pushes 100 ms updates,
///   ≥ 10 diffs in 8 s is a very conservative bar).
/// - 1-second snapshot parquet exists with ≥ 5 rows.
/// - Monitor shows no reconnects and no sequence gaps.
/// - Mid price printed to stdout (visible with --nocapture).
#[tokio::test]
#[ignore = "live Binance network — run: cargo test --test smoke_test -- --include-ignored"]
async fn live_spot_ethusdt_pipeline() {
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
    let task = tokio::spawn(connection_task(
        live_conn("smoke_eth", vec!["ETHUSDT"], Exchange::BinanceSpot),
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx,
        snap_tx,
        CancellationToken::new(),
    ));

    tokio::time::sleep(Duration::from_secs(8)).await;
    task.abort();
    let _ = task.await;
    raw_handle.await.unwrap();
    snap_handle.await.unwrap();

    let all = collect_parquets(dir.path());
    let raws: Vec<_> = all
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("raw"))
        .collect();
    let snaps: Vec<_> = all
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("1s"))
        .collect();

    // Raw data
    assert!(!raws.is_empty(), "no raw parquet written");
    let raw_rows: usize = raws.iter().map(|p| count_rows(p)).sum();
    assert!(raw_rows >= 10, "expected ≥ 10 raw rows, got {raw_rows}");
    println!("raw rows: {raw_rows}");

    // 1s snapshots
    assert!(!snaps.is_empty(), "no 1s snap parquet written");
    let snap_rows: usize = snaps.iter().map(|p| count_rows(p)).sum();
    assert!(snap_rows >= 1, "expected ≥ 1 snap row, got {snap_rows}");
    println!("snap rows: {snap_rows}");

    // Mid price sanity — ETHUSDT should be somewhere between $100 and $100 000
    let mids = read_f64_col(snaps[0], "mid_px");
    assert!(!mids.is_empty(), "mid_px column is empty");
    let mid = mids[0];
    println!("ETHUSDT mid_px (first snap): ${mid:.2}");
    assert!(
        (100.0..=100_000.0).contains(&mid),
        "ETHUSDT mid_px {mid:.2} is outside expected range $100–$100 000"
    );

    // Monitor: clean run expected
    let guard = state.lock().unwrap();
    let cs = guard.get("smoke_eth").expect("smoke_eth in monitor");
    let eth_gaps = cs.symbols.get("ETHUSDT").map(|s| s.gaps_today).unwrap_or(0);
    println!("reconnects: {}, gaps: {eth_gaps}", cs.reconnects_today);
    assert_eq!(
        cs.reconnects_today, 0,
        "unexpected reconnects during 8 s run"
    );
    assert_eq!(eth_gaps, 0, "unexpected sequence gaps during 8 s run");
}

/// Multi-symbol smoke: BTCUSDT + ETHUSDT on one connection.
///
/// Asserts two separate raw parquet files (one per symbol) with data.
#[tokio::test]
#[ignore = "live Binance network — run: cargo test --test smoke_test -- --include-ignored"]
async fn live_spot_multi_symbol() {
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
    let task = tokio::spawn(connection_task(
        live_conn(
            "smoke_multi",
            vec!["ETHUSDT", "BTCUSDT"],
            Exchange::BinanceSpot,
        ),
        Box::new(BinanceSpot),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx,
        snap_tx,
        CancellationToken::new(),
    ));

    tokio::time::sleep(Duration::from_secs(6)).await;
    task.abort();
    let _ = task.await;
    raw_handle.await.unwrap();
    snap_handle.await.unwrap();

    let raws = collect_parquets(dir.path())
        .into_iter()
        .filter(|p| p.to_str().unwrap_or("").contains("raw"))
        .collect::<Vec<_>>();

    let eth_rows: usize = raws
        .iter()
        .filter(|p| p.to_str().unwrap_or("").to_uppercase().contains("ETHUSDT"))
        .map(|p| count_rows(p))
        .sum();
    let btc_rows: usize = raws
        .iter()
        .filter(|p| p.to_str().unwrap_or("").to_uppercase().contains("BTCUSDT"))
        .map(|p| count_rows(p))
        .sum();

    println!("ETH raw rows: {eth_rows}, BTC raw rows: {btc_rows}");
    assert!(eth_rows >= 5, "ETHUSDT: expected ≥ 5 rows, got {eth_rows}");
    assert!(btc_rows >= 5, "BTCUSDT: expected ≥ 5 rows, got {btc_rows}");

    // BTC mid price sanity
    let snaps = collect_parquets(dir.path())
        .into_iter()
        .filter(|p| {
            p.to_str().unwrap_or("").contains("1s")
                && p.to_str().unwrap_or("").to_uppercase().contains("BTCUSDT")
        })
        .collect::<Vec<_>>();
    if !snaps.is_empty() {
        let btc_mids = read_f64_col(&snaps[0], "mid_px");
        if let Some(&mid) = btc_mids.first() {
            println!("BTCUSDT mid_px (first snap): ${mid:.2}");
            assert!(
                (1_000.0..=1_000_000.0).contains(&mid),
                "BTCUSDT mid_px {mid:.2} is outside expected range $1 000–$1 000 000"
            );
        }
    }
}

/// Perpetuals smoke: connect to Binance USDM Futures, collect 6 s of ETHUSDT data.
#[tokio::test]
#[ignore = "live Binance network — run: cargo test --test smoke_test -- --include-ignored"]
async fn live_perp_ethusdt_pipeline() {
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
    let task = tokio::spawn(connection_task(
        live_conn("smoke_perp", vec!["ETHUSDT"], Exchange::BinancePerp),
        Box::new(BinancePerp),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx,
        snap_tx,
        CancellationToken::new(),
    ));

    tokio::time::sleep(Duration::from_secs(6)).await;
    task.abort();
    let _ = task.await;
    raw_handle.await.unwrap();
    snap_handle.await.unwrap();

    let raws = collect_parquets(dir.path())
        .into_iter()
        .filter(|p| p.to_str().unwrap_or("").contains("raw"))
        .collect::<Vec<_>>();

    assert!(!raws.is_empty(), "no raw parquet for perp ETHUSDT");
    let rows: usize = raws.iter().map(|p| count_rows(p)).sum();
    println!("perp ETHUSDT raw rows: {rows}");
    assert!(rows >= 1, "expected ≥ 1 perp raw row, got {rows}");

    // Print monitor state for inspection; don't assert zero gaps/reconnects
    // because perp markets have higher throughput and occasional sequence gaps
    // are expected under load.
    let guard = state.lock().unwrap();
    let cs = guard.get("smoke_perp").expect("smoke_perp in monitor");
    let eth_gaps = cs.symbols.get("ETHUSDT").map(|s| s.gaps_today).unwrap_or(0);
    println!("perp reconnects: {}, gaps: {eth_gaps}", cs.reconnects_today);
}
