/// Smoke tests against live Bybit v5 public WS (spot + linear).
///
/// No mocks — real network, real exchange, real data.
/// Skipped by default (cargo test skips #[ignore]).
///
/// Run all:
///   cargo test --test smoke_bybit_test -- --include-ignored --test-threads 1 --nocapture
///
/// Run single:
///   cargo test --test smoke_bybit_test live_bybit_pipeline -- --include-ignored --nocapture
///
/// `live_bybit_verify_gap_assumptions_and_symbols` is the one that matters
/// for `specs/bybit-collection.md`'s two flagged-as-unconfirmed assumptions
/// (documented there as "verify against a live capture before implementing"):
/// does `u` increment by exactly +1 per delta message, and do all 6
/// reference symbols exist on both `spot` and `linear` under the same
/// string. It talks to the raw WS directly (bypassing `connection_task_bybit`
/// entirely) so a violation is *observed*, not silently absorbed by the
/// gap-detector's own reconnect logic.
use std::{collections::HashMap, time::Duration};

use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

mod helpers;
use helpers::parquet::{collect_parquets, count_rows, read_f64_col};

use fathom::{
    accumulator::Snapshot1s,
    config::{ConnectionConfig, Exchange},
    connection::connection_task_bybit,
    exchange::{BybitPerp, BybitSpot},
    monitor,
    writer::{
        deriv::DerivEvent,
        raw::{RawDiff, run_raw_writer},
        snap_1s::run_snap_writer,
        trades::{RawTrade, run_trades_writer},
    },
};

const SYMBOLS: [&str; 6] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT",
];

fn bybit_conn(name: &str, exchange: Exchange, symbols: &[&str]) -> ConnectionConfig {
    ConnectionConfig {
        name: name.to_string(),
        exchange,
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        depth_ms: 0, // ignored by the Bybit adapter
        ws_url_override: None,
        market_ws_url_override: None,
        snapshot_url_override: None,
    }
}

// ── Smoke test 1: full pipeline, spot + linear ──────────────────────────────

/// Connects both `bybit_spot` and `bybit_perp` categories for all 6 reference
/// symbols, collects ~15s of live data, and checks basic liveness: 1s snap
/// parquet exists with rows, raw diff parquet exists (proves at least one
/// snapshot + delta landed), trades parquet exists, and (linear only) deriv
/// funding/OI parquet exists. No panics is itself part of what this proves.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live Bybit — run: cargo test --test smoke_bybit_test -- --include-ignored"]
async fn live_bybit_pipeline() {
    let dir = TempDir::new().unwrap();
    let (raw_tx, raw_rx) = broadcast::channel::<RawDiff>(4_096);
    let (snap_tx, snap_rx) = broadcast::channel::<Snapshot1s>(4_096);
    let (trade_tx, trade_rx) = broadcast::channel::<RawTrade>(4_096);
    let (deriv_tx, deriv_rx) = broadcast::channel::<DerivEvent>(4_096);

    let raw_handle = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        raw_rx,
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let snap_handle = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        snap_rx,
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));
    let trades_handle = tokio::spawn(run_trades_writer(
        dir.path().to_path_buf(),
        trade_rx,
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let deriv_handle = tokio::spawn(fathom::writer::deriv::run_deriv_writer(
        dir.path().to_path_buf(),
        deriv_rx,
        60,
        fathom::metrics::new_metrics().metrics,
    ));

    let state = monitor::new_state();
    let spot_task = tokio::spawn(connection_task_bybit(
        bybit_conn("smoke_bybit_spot", Exchange::BybitSpot, &SYMBOLS),
        Box::new(BybitSpot),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx.clone(),
        snap_tx.clone(),
        trade_tx.clone(),
        deriv_tx.clone(),
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));
    let perp_task = tokio::spawn(connection_task_bybit(
        bybit_conn("smoke_bybit_perp", Exchange::BybitPerp, &SYMBOLS),
        Box::new(BybitPerp),
        dir.path().to_path_buf(),
        state.clone(),
        raw_tx,
        snap_tx,
        trade_tx,
        deriv_tx,
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    tokio::time::sleep(Duration::from_secs(15)).await;
    spot_task.abort();
    perp_task.abort();
    let _ = spot_task.await;
    let _ = perp_task.await;
    raw_handle.await.unwrap();
    snap_handle.await.unwrap();
    trades_handle.await.unwrap();
    deriv_handle.await.unwrap();

    let all = collect_parquets(dir.path());
    println!("bybit smoke: {} parquet files total", all.len());

    let raws: Vec<_> = all
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("/raw/"))
        .collect();
    assert!(!raws.is_empty(), "no raw diff parquet written for Bybit");
    let raw_rows: usize = raws.iter().map(|p| count_rows(p)).sum();
    println!("bybit raw rows (spot+perp, both categories): {raw_rows}");
    assert!(raw_rows > 0, "raw rows should be > 0");

    let snaps: Vec<_> = all
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("/1s/"))
        .collect();
    assert!(!snaps.is_empty(), "no 1s snap parquet written for Bybit");
    let snap_rows: usize = snaps.iter().map(|p| count_rows(p)).sum();
    println!("bybit snap rows: {snap_rows}");
    assert!(snap_rows > 0, "snap rows should be > 0");

    if let Some(btc_spot) = snaps
        .iter()
        .find(|p| p.to_str().unwrap_or("").contains("bybit_spot/BTCUSDT"))
    {
        let mids = read_f64_col(btc_spot, "mid_px");
        if let Some(&mid) = mids.first() {
            println!("bybit_spot BTCUSDT mid_px: ${mid:.2}");
            assert!(
                (1_000.0..=1_000_000.0).contains(&mid),
                "BTCUSDT mid_px {mid:.2} outside $1k-$1M"
            );
        }
    }

    let trades: Vec<_> = all
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("/trades/"))
        .collect();
    println!("bybit trades files: {} ", trades.len());
    let trade_rows: usize = trades.iter().map(|p| count_rows(p)).sum();
    println!("bybit trade rows in 15s: {trade_rows}");
    // Not hard-asserted > 0: low-liquidity symbols (DOGEUSDT etc.) could
    // plausibly have zero trades in a 15s window, but at least one of the 12
    // spot+perp streams should show some trading activity.
    assert!(
        trade_rows > 0,
        "expected at least some trades across 6 symbols x 2 categories in 15s"
    );

    let deriv: Vec<_> = all
        .iter()
        .filter(|p| p.to_str().unwrap_or("").contains("/deriv/"))
        .collect();
    println!("bybit deriv files: {deriv:?}");
    assert!(
        deriv
            .iter()
            .any(|p| p.to_str().unwrap_or("").contains("funding_")),
        "expected at least one funding parquet from bybit_perp's tickers channel"
    );

    let guard = state.lock().unwrap();
    for name in ["smoke_bybit_spot", "smoke_bybit_perp"] {
        if let Some(cs) = guard.get(name) {
            println!(
                "{name}: reconnects={}, connected={}",
                cs.reconnects_today, cs.connected
            );
            for (sym, ss) in &cs.symbols {
                println!("  {sym}: gaps_today={}", ss.gaps_today);
            }
        }
    }
}

// ── Smoke test 2: raw-WS verification of the two flagged spec assumptions ──

/// Raw per-message capture of `orderbook.1000.{symbol}` on both categories,
/// bypassing `connection_task_bybit` (whose own gap-detector would just
/// reconnect on a violation rather than surface it). Prints, per symbol:
/// how many consecutive delta pairs had `u_new == u_prev + 1` vs not, and
/// whether the symbol was seen at all (existence proof, not just an
/// assumption from the spec doc).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live Bybit — run: cargo test --test smoke_bybit_test -- --include-ignored"]
async fn live_bybit_verify_gap_assumptions_and_symbols() {
    for (label, url) in [
        ("spot", "wss://stream.bybit.com/v5/public/spot"),
        ("linear", "wss://stream.bybit.com/v5/public/linear"),
    ] {
        println!("\n=== category: {label} ({url}) ===");
        let (mut ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .unwrap_or_else(|e| panic!("connect to {url} failed: {e}"));

        let topics: Vec<String> = SYMBOLS
            .iter()
            .map(|s| format!("orderbook.1000.{s}"))
            .collect();
        let sub = serde_json::json!({"op": "subscribe", "args": topics}).to_string();
        ws.send(Message::Text(sub.into())).await.unwrap();

        // last-seen `u` per symbol, plus contiguous/gap counters.
        let mut last_u: HashMap<String, i64> = HashMap::new();
        let mut contiguous = 0u32;
        let mut gaps = 0u32;
        let mut snapshots_seen: HashMap<String, u32> = HashMap::new();
        let mut deltas_seen: HashMap<String, u32> = HashMap::new();
        let mut gap_examples: Vec<(String, i64, i64)> = Vec::new();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < deadline {
            let msg = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
            let Ok(Some(Ok(Message::Text(text)))) = msg else {
                continue;
            };
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            let Some(topic) = v.get("topic").and_then(|t| t.as_str()) else {
                continue; // admin/ack frame
            };
            let Some(symbol) = topic.rsplit('.').next() else {
                continue;
            };
            let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let Some(u) = v
                .get("data")
                .and_then(|d| d.get("u"))
                .and_then(serde_json::Value::as_i64)
            else {
                continue;
            };

            match msg_type {
                "snapshot" => {
                    *snapshots_seen.entry(symbol.to_string()).or_insert(0) += 1;
                    last_u.insert(symbol.to_string(), u);
                }
                "delta" => {
                    *deltas_seen.entry(symbol.to_string()).or_insert(0) += 1;
                    if let Some(&prev) = last_u.get(symbol) {
                        if u == prev + 1 {
                            contiguous += 1;
                        } else {
                            gaps += 1;
                            if gap_examples.len() < 5 {
                                gap_examples.push((symbol.to_string(), prev, u));
                            }
                        }
                    }
                    last_u.insert(symbol.to_string(), u);
                }
                _ => {}
            }
        }

        println!("--- {label}: symbol existence (snapshot received) ---");
        for s in SYMBOLS {
            let n = snapshots_seen.get(s).copied().unwrap_or(0);
            println!(
                "  {s}: {n} snapshot(s), {} delta(s)",
                deltas_seen.get(s).copied().unwrap_or(0)
            );
        }
        let missing: Vec<&str> = SYMBOLS
            .iter()
            .filter(|s| !snapshots_seen.contains_key(**s))
            .copied()
            .collect();
        if !missing.is_empty() {
            println!("  ⚠ symbols with NO snapshot observed on {label}: {missing:?}");
        }

        println!(
            "--- {label}: u-sequence — {contiguous} contiguous (u_new == u_prev+1), {gaps} non-contiguous, out of {} delta observations ---",
            contiguous + gaps
        );
        if gaps > 0 {
            println!("  ⚠ non-contiguous examples (symbol, prev_u, new_u): {gap_examples:?}");
        } else if contiguous > 0 {
            println!("  ✓ every observed delta this run had u_new == u_prev + 1");
        }

        let _ = ws.close(None).await;
    }
}
