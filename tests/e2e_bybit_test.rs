/// E2E tests for the Bybit v5 collector using `MockBybitServer` (axum WS
/// mock — see `tests/helpers/bybit.rs` for why the harness shape differs
/// from `MockBinanceServer`: Bybit is WS-native, no REST snapshot route).
///
/// Scenario 1 exercises the full per-message dispatch `connection_task_bybit`
/// implements: orderbook snapshot → deltas applied → publicTrade batch →
/// tickers snapshot+partial-delta (merge-state) → allLiquidation batch → a
/// deliberate `u` gap that must trigger reconnect + resync, all asserted via
/// the resulting Parquet output (raw diff / trades / deriv funding+oi+liq /
/// 1s snap) plus monitor gap/reconnect counters.
///
/// Scenario 2 exercises spot's mandated multi-batch subscribe (6 symbols ×
/// 2 topics = 12 args, over the documented 10-arg cap) actually happening on
/// the wire, plus a basic snapshot+delta sync.
///
/// Scenario 3 exercises `bybit.rs`'s `!ack.success` → `break 'inner` wiring
/// end-to-end (not just `BybitAck`'s own parsing unit tests): a rejected
/// subscribe ack on the first connection must tear the connection down and
/// reconnect+resubscribe, and the second (accepted) connection must recover
/// normal snapshot/delta flow.
///
/// Scenario 4 exercises the `should_drop_ticker_delta`/`ticker_seen` gate
/// through real `connection_task_bybit` dispatch (not just the pure fn's
/// unit tests): a `tickers` delta arriving before the symbol's first
/// `tickers` snapshot must be dropped, contributing zero deriv rows.
mod helpers;

use std::path::{Path, PathBuf};
use std::time::Duration;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use fathom::{
    accumulator::Snapshot1s,
    config::{ConnectionConfig, Exchange},
    connection::connection_task_bybit,
    exchange::{BybitPerp, BybitSpot},
    monitor,
    writer::{
        deriv::{DerivEvent, run_deriv_writer},
        raw::{RawDiff, run_raw_writer},
        snap_1s::run_snap_writer,
        trades::{RawTrade, run_trades_writer},
    },
};
use helpers::bybit::MockBybitServer;

// ── Message builders (Bybit wire shapes — specs/bybit-collection.md) ────────

fn bybit_ob_msg(
    symbol: &str,
    msg_type: &str,
    ts_ms: i64,
    u: i64,
    bids: Vec<(&str, &str)>,
    asks: Vec<(&str, &str)>,
) -> String {
    let bids_json: Vec<[&str; 2]> = bids.iter().map(|(p, q)| [*p, *q]).collect();
    let asks_json: Vec<[&str; 2]> = asks.iter().map(|(p, q)| [*p, *q]).collect();
    serde_json::json!({
        "topic": format!("orderbook.1000.{symbol}"),
        "type": msg_type,
        "ts": ts_ms,
        "cts": ts_ms + 5,
        "data": {
            "s": symbol,
            "b": bids_json,
            "a": asks_json,
            "u": u,
            "seq": 1_000_000 + u,
        }
    })
    .to_string()
}

fn bybit_trade_msg(
    symbol: &str,
    ts_ms: i64,
    items: Vec<(&str, &str, &str, &str)>, // (side, qty, price, trade_id)
) -> String {
    let data: Vec<_> = items
        .iter()
        .map(|(side, qty, price, id)| {
            // Real Bybit linear publicTrade `i` is a hyphenated UUID (not i64);
            // the numeric id lives in `seq`. Mirror that here so the e2e path
            // exercises build_raw_trade's seq-fallback — a numeric `i` (the old
            // mock) silently skipped the whole class (prod incident 2026-07-07).
            let seq: i64 = id.parse().expect("trade_id param must be numeric (→ seq)");
            serde_json::json!({
                "T": ts_ms, "s": symbol, "S": side, "v": qty, "p": price,
                "L": "PlusTick", "i": "00448946-2357-5a2c-ba29-44e187a93f43", "seq": seq
            })
        })
        .collect();
    serde_json::json!({"topic": format!("publicTrade.{symbol}"), "type": "snapshot", "ts": ts_ms, "data": data}).to_string()
}

fn bybit_ticker_msg(symbol: &str, msg_type: &str, ts_ms: i64, fields: serde_json::Value) -> String {
    serde_json::json!({"topic": format!("tickers.{symbol}"), "type": msg_type, "ts": ts_ms, "data": fields}).to_string()
}

fn bybit_liq_msg(symbol: &str, ts_ms: i64, items: Vec<(&str, &str, &str)>) -> String {
    // (side, qty, price)
    let data: Vec<_> = items
        .iter()
        .map(|(side, qty, price)| {
            serde_json::json!({"T": ts_ms, "s": symbol, "S": side, "v": qty, "p": price})
        })
        .collect();
    serde_json::json!({"topic": format!("allLiquidation.{symbol}"), "ts": ts_ms, "data": data})
        .to_string()
}

// ── Parquet helpers ──────────────────────────────────────────────────────────

fn find_parquets(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect(dir, &mut out);
    out
}
fn collect(dir: &Path, acc: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect(&path, acc);
            } else if path.extension().map_or(false, |e| e == "parquet") {
                acc.push(path);
            }
        }
    }
}
fn count_rows(path: &Path) -> usize {
    let file = std::fs::File::open(path).expect("parquet file should exist");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}
fn by_dir<'a>(paths: &'a [PathBuf], component: &str) -> Vec<&'a PathBuf> {
    paths
        .iter()
        .filter(|p| p.components().any(|c| c.as_os_str() == component))
        .collect()
}
fn by_stem_prefix<'a>(paths: &'a [&'a PathBuf], prefix: &str) -> Option<&'a PathBuf> {
    paths
        .iter()
        .copied()
        .find(|p| p.file_name().unwrap().to_str().unwrap().starts_with(prefix))
}

fn perp_conn(name: &str, symbols: Vec<&str>, server: &MockBybitServer) -> ConnectionConfig {
    ConnectionConfig {
        name: name.to_string(),
        exchange: Exchange::BybitPerp,
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        depth_ms: 0, // ignored by the Bybit adapter (spec: depth-1000 hardcoded)
        ws_url_override: Some(server.ws_url()),
        snapshot_url_override: None, // Bybit is WS-native, no REST snapshot
    }
}

fn spot_conn(name: &str, symbols: Vec<&str>, server: &MockBybitServer) -> ConnectionConfig {
    ConnectionConfig {
        name: name.to_string(),
        exchange: Exchange::BybitSpot,
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        depth_ms: 0,
        ws_url_override: Some(server.ws_url()),
        snapshot_url_override: None,
    }
}

// ── Scenario 1: full linear dispatch + gap-triggered reconnect ─────────────

/// Round 1: snapshot (u=100) → 2 contiguous deltas (u=101,102) → publicTrade
/// batch (2 trades) → tickers snapshot (seeds mark/funding/OI) → tickers
/// delta (funding-rate only — must emit a MarkFunding row but NOT a new
/// OpenInterest row) → allLiquidation batch (2 liqs) → a deliberate gap
/// delta (u=105, expected 103) that must trigger reconnect + resync.
///
/// Round 2 (after reconnect): fresh snapshot (u=200) + one contiguous delta
/// (u=201) — proves the book actually resyncs from scratch rather than
/// hanging or replaying stale state.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_bybit_perp_full_scenario() {
    let server = MockBybitServer::new().await;
    let symbol = "BTCUSDT";

    server.push_ws_round(vec![
        bybit_ob_msg(
            symbol,
            "snapshot",
            1_700_000_000_000,
            100,
            vec![("43000.0", "1.0")],
            vec![("43001.0", "1.0")],
        ),
        bybit_ob_msg(
            symbol,
            "delta",
            1_700_000_001_000,
            101,
            vec![("43000.0", "1.5")],
            vec![],
        ),
        bybit_ob_msg(
            symbol,
            "delta",
            1_700_000_002_000,
            102,
            vec![],
            vec![("43002.0", "0.5")],
        ),
        bybit_trade_msg(
            symbol,
            1_700_000_002_500,
            vec![
                ("Buy", "0.01", "43000.5", "1001"),
                ("Sell", "0.02", "43001.0", "1002"),
            ],
        ),
        bybit_ticker_msg(
            symbol,
            "snapshot",
            1_700_000_003_000,
            serde_json::json!({
                "markPrice": "43001.0",
                "indexPrice": "43000.8",
                "fundingRate": "0.0001",
                "nextFundingTime": "1700006400000",
                "openInterest": "500.0",
                "openInterestValue": "21500000.0"
            }),
        ),
        bybit_ticker_msg(
            symbol,
            "delta",
            1_700_000_003_500,
            serde_json::json!({ "fundingRate": "0.00015" }),
        ),
        bybit_liq_msg(
            symbol,
            1_700_000_004_000,
            vec![("Sell", "0.05", "42990.0"), ("Buy", "0.03", "43010.0")],
        ),
        // Deliberate gap: expected 103, got 105 — must reconnect.
        bybit_ob_msg(
            symbol,
            "delta",
            1_700_000_005_000,
            105,
            vec![("43000.0", "2.0")],
            vec![],
        ),
    ]);
    server.push_ws_round(vec![
        bybit_ob_msg(
            symbol,
            "snapshot",
            1_700_000_010_000,
            200,
            vec![("43000.0", "1.0")],
            vec![("43001.0", "1.0")],
        ),
        bybit_ob_msg(
            symbol,
            "delta",
            1_700_000_011_000,
            201,
            vec![("43000.0", "1.2")],
            vec![],
        ),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, _) = broadcast::channel::<RawDiff>(256);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(256);
    let (trade_tx, _) = broadcast::channel::<RawTrade>(256);
    let (deriv_tx, _) = broadcast::channel::<DerivEvent>(256);

    let raw_w = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        raw_tx.subscribe(),
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let snap_w = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        snap_tx.subscribe(),
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));
    let trades_w = tokio::spawn(run_trades_writer(
        dir.path().to_path_buf(),
        trade_tx.subscribe(),
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let deriv_w = tokio::spawn(run_deriv_writer(
        dir.path().to_path_buf(),
        deriv_tx.subscribe(),
        60,
        fathom::metrics::new_metrics().metrics,
    ));

    let state = monitor::new_state();
    let conn = perp_conn("bybit_perp_e2e", vec![symbol], &server);
    let task = tokio::spawn(connection_task_bybit(
        conn,
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

    // Round 1 processing (~instant) + gap → backoff (~1-1.25s) + round 2 + slack.
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();
    trades_w.await.unwrap();
    deriv_w.await.unwrap();

    assert!(
        server.connected_count() >= 2,
        "gap must trigger a reconnect (got {} connections)",
        server.connected_count()
    );

    // Linear's 4 topics/symbol fit under the arg cap in one subscribe frame,
    // resent on every (re)connect attempt. At least 2 connections happen
    // within the test window (initial + post-gap-reconnect); the reconnect
    // loop may attempt a further reconnect once round 2's queue is drained
    // (no more scripted messages left, socket closes, backoff, retry) —
    // that's expected connection_task_bybit behavior, not asserted exactly
    // to avoid timing-dependent flakiness (same latitude the existing
    // Binance e2e tests give `connected_count() >= 2`).
    let batches = server.subscribed_batches();
    assert!(
        batches.len() >= 2,
        "expected >= 2 subscribe frames (initial + post-gap-reconnect): {batches:?}"
    );
    for b in &batches {
        assert_eq!(
            b.len(),
            4,
            "orderbook+publicTrade+tickers+allLiquidation for 1 symbol"
        );
    }

    // ── Orderbook: snapshot + deltas applied, gap delta itself not persisted ──
    let all = find_parquets(dir.path());
    let raws = by_dir(&all, "raw");
    assert_eq!(raws.len(), 1, "one raw parquet for {symbol}: {raws:?}");
    let raw_rows = count_rows(raws[0]);
    assert_eq!(
        raw_rows, 5,
        "round1 snapshot+2 deltas (3) + round2 snapshot+delta (2) = 5; gap delta itself must not be written"
    );

    // ── Trades ──
    let tapes = by_dir(&all, "trades");
    assert_eq!(tapes.len(), 1, "one trades parquet: {tapes:?}");
    assert_eq!(count_rows(tapes[0]), 2, "both publicTrade items persisted");
    {
        use arrow_array::{BooleanArray, Float64Array};
        let file = std::fs::File::open(tapes[0]).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let px = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(px.value(0), 43000.5);
        assert_eq!(px.value(1), 43001.0);
        let bm = batch
            .column_by_name("is_buyer_maker")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!bm.value(0), "taker Buy → buyer is NOT the maker");
        assert!(bm.value(1), "taker Sell → buyer IS the maker");
    }

    // ── Deriv: funding (2 rows: snapshot + funding-rate-only delta), OI (1 row: snapshot only), liq (2 rows) ──
    let derivs = by_dir(&all, "deriv");
    let funding = by_stem_prefix(&derivs, "funding_").expect("funding_HHMM_HHMM.parquet");
    assert_eq!(
        count_rows(funding),
        2,
        "MarkFunding: snapshot + the funding-rate-only delta (mark/funding group changed both times)"
    );
    let oi = by_stem_prefix(&derivs, "oi_").expect("oi_HHMM_HHMM.parquet");
    assert_eq!(
        count_rows(oi),
        1,
        "OpenInterest: only the snapshot touched OI fields — the funding-only delta must not emit a duplicate OI row"
    );
    let liq = by_stem_prefix(&derivs, "liq_").expect("liq_HHMM_HHMM.parquet");
    assert_eq!(count_rows(liq), 2, "both allLiquidation items persisted");
    {
        use arrow_array::{Float64Array, StringArray};
        let file = std::fs::File::open(liq).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let side = batch
            .column_by_name("side")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(side.value(0), "Sell");
        assert_eq!(side.value(1), "Buy");
        let price = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(price.value(0), 42990.0);
        assert_eq!(price.value(1), 43010.0);
    }

    {
        use arrow_array::Float64Array;
        let file = std::fs::File::open(funding).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let rate = batch
            .column_by_name("funding_rate")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(rate.value(0), 0.0001, "snapshot funding rate");
        assert_eq!(rate.value(1), 0.00015, "delta-updated funding rate");
        let mark = batch
            .column_by_name("mark_px")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            mark.value(1),
            43001.0,
            "delta only carried fundingRate — mark_px must be carried over from the merged state, not lost"
        );
    }

    // ── 1s snap: on-reconnect flush must have produced at least one row ──
    let snaps = by_dir(&all, "1s");
    assert!(!snaps.is_empty(), "1s snap parquet should exist");

    // ── Monitor: gap + reconnect recorded ──
    let guard = state.lock().unwrap();
    let cs = guard.get("bybit_perp_e2e").expect("connection in monitor");
    let gaps = cs.symbols.get(symbol).map(|s| s.gaps_today).unwrap_or(0);
    assert!(gaps >= 1, "gaps_today should be >= 1 (got {gaps})");
    assert!(
        cs.reconnects_today >= 1,
        "reconnects_today should be >= 1 (got {})",
        cs.reconnects_today
    );
}

// ── Scenario 2: spot subscribe batching (10-arg cap) + basic sync ──────────

/// 6 symbols × 2 topics (orderbook + publicTrade, no ticker/liq on spot) = 12
/// args, over the documented 10-arg-per-`subscribe`-request cap — the spot
/// connection must split this into 2 batches on the same socket (spec's
/// "Channels / topics" section). Also exercises basic snapshot+delta sync
/// for one of the six symbols.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_bybit_spot_subscribe_batching_and_sync() {
    let server = MockBybitServer::new().await;
    let symbols = [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT",
    ];

    server.push_ws_round(vec![
        bybit_ob_msg(
            "BTCUSDT",
            "snapshot",
            1_700_000_000_000,
            100,
            vec![("43000.0", "1.0")],
            vec![("43001.0", "1.0")],
        ),
        bybit_ob_msg(
            "BTCUSDT",
            "delta",
            1_700_000_001_000,
            101,
            vec![("43000.0", "1.1")],
            vec![],
        ),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, _) = broadcast::channel::<RawDiff>(256);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(256);
    let (trade_tx, _) = broadcast::channel::<RawTrade>(256);
    let (deriv_tx, _) = broadcast::channel::<DerivEvent>(256);

    let raw_w = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        raw_tx.subscribe(),
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let snap_w = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        snap_tx.subscribe(),
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    let state = monitor::new_state();
    let conn = spot_conn("bybit_spot_e2e", symbols.to_vec(), &server);
    let task = tokio::spawn(connection_task_bybit(
        conn,
        Box::new(BybitSpot),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
        trade_tx,
        deriv_tx,
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    tokio::time::sleep(Duration::from_millis(800)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    let batches = server.subscribed_batches();
    assert_eq!(
        batches.len(),
        2,
        "12 topics / 10-arg cap = 2 subscribe frames on the wire: {batches:?}"
    );
    assert_eq!(batches[0].len(), 10, "first subscribe frame hits the cap");
    assert_eq!(batches[1].len(), 2, "second frame carries the remainder");
    let total_args: usize = batches.iter().map(Vec::len).sum();
    assert_eq!(
        total_args, 12,
        "6 symbols x 2 topics = 12 args total, none lost across frames"
    );
    let all_topics: Vec<&String> = batches.iter().flatten().collect();
    assert!(
        all_topics
            .iter()
            .all(|t| t.starts_with("orderbook.1000.") || t.starts_with("publicTrade.")),
        "spot must not subscribe tickers/allLiquidation: {all_topics:?}"
    );
    assert!(
        !all_topics
            .iter()
            .any(|t| t.starts_with("tickers.") || t.starts_with("allLiquidation.")),
        "spot has no ticker/liq topics: {all_topics:?}"
    );

    let all = find_parquets(dir.path());
    let raws = by_dir(&all, "raw");
    let btc = raws
        .iter()
        .find(|p| p.to_string_lossy().contains("BTCUSDT"))
        .expect("BTCUSDT raw parquet");
    assert_eq!(count_rows(btc), 2, "snapshot + 1 delta for BTCUSDT");
}

// ── Scenario 3: subscribe-ack rejection → reconnect + resubscribe ─────────

/// First connection's subscribe batch is rejected (`{"success":false}` —
/// `MockBybitServer::push_subscribe_ack_rejection`). `bybit.rs`'s
/// `!ack.success` branch must treat this exactly like a client-detected gap:
/// tear the connection down and reconnect *before* consuming anything else
/// on that socket.
///
/// Round 1 carries "trap" orderbook data behind the rejected ack — data that
/// would only ever reach `connection_task_bybit`'s dispatch if the client
/// stayed on that connection past the ack (i.e. if the `!ack.success` branch
/// were missing/broken and reconnection instead waited for the natural
/// per-connection socket close every `MockBybitServer` connection eventually
/// does). A working implementation disconnects fast enough that the mock's
/// send of that trap data fails (socket already torn down client-side)
/// before it can ever apply to the book; round 2 (ack accepted — the mock's
/// default behavior) then resubscribes and delivers the real snapshot+delta.
/// Asserting the final raw-parquet row count is exactly round 2's 2 rows
/// (not round 1's trap rows too) is what actually proves the rejection was
/// caught immediately, not just that *some* reconnect eventually happened
/// (that part would be true even with the branch deleted, since this mock
/// closes every connection after its round regardless of ack outcome).
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_bybit_subscribe_ack_rejection_reconnects_and_resubscribes() {
    let server = MockBybitServer::new().await;
    let symbol = "BTCUSDT";

    server.push_subscribe_ack_rejection();
    server.push_ws_round(vec![
        // Trap: must never be applied — proves the client bailed out on the
        // rejected ack instead of riding the connection to its natural close.
        bybit_ob_msg(
            symbol,
            "snapshot",
            1_700_000_000_000,
            900,
            vec![("40000.0", "9.9")],
            vec![("40001.0", "9.9")],
        ),
        bybit_ob_msg(
            symbol,
            "delta",
            1_700_000_001_000,
            901,
            vec![("40000.0", "8.8")],
            vec![],
        ),
    ]);

    server.push_ws_round(vec![
        bybit_ob_msg(
            symbol,
            "snapshot",
            1_700_000_010_000,
            100,
            vec![("43000.0", "1.0")],
            vec![("43001.0", "1.0")],
        ),
        bybit_ob_msg(
            symbol,
            "delta",
            1_700_000_011_000,
            101,
            vec![("43000.0", "1.1")],
            vec![],
        ),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, _) = broadcast::channel::<RawDiff>(256);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(256);
    let (trade_tx, _) = broadcast::channel::<RawTrade>(256);
    let (deriv_tx, _) = broadcast::channel::<DerivEvent>(256);

    let raw_w = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        raw_tx.subscribe(),
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let snap_w = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        snap_tx.subscribe(),
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    let state = monitor::new_state();
    let conn = perp_conn("bybit_perp_ack_reject_e2e", vec![symbol], &server);
    let task = tokio::spawn(connection_task_bybit(
        conn,
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

    // Rejected-ack reconnect backoff (~1s, BACKOFF_START_MS) + round 2 + slack.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    task.abort();
    let _ = task.await;
    raw_w.await.unwrap();
    snap_w.await.unwrap();

    assert!(
        server.connected_count() >= 2,
        "ack rejection must trigger a reconnect (got {} connections)",
        server.connected_count()
    );

    let batches = server.subscribed_batches();
    assert!(
        batches.len() >= 2,
        "expected >= 2 subscribe frames (rejected + post-reconnect resubscribe): {batches:?}"
    );

    let all = find_parquets(dir.path());
    let raws = by_dir(&all, "raw");
    assert_eq!(raws.len(), 1, "one raw parquet for {symbol}: {raws:?}");
    assert_eq!(
        count_rows(raws[0]),
        2,
        "only round 2's snapshot + delta persisted; round 1's trap snapshot+delta \
         (behind the rejected ack) must never have reached the book — a count of 4 \
         here would mean the client rode the rejected connection to its natural \
         close instead of bailing on `!ack.success`"
    );

    let guard = state.lock().unwrap();
    let cs = guard
        .get("bybit_perp_ack_reject_e2e")
        .expect("connection in monitor");
    assert!(
        cs.reconnects_today >= 1,
        "reconnects_today should be >= 1 after ack rejection (got {})",
        cs.reconnects_today
    );
}

// ── Scenario 4: tickers delta before any snapshot is gated ────────────────

/// A `tickers` `delta` carrying a full valid field set (mark price + funding
/// rate + open interest — everything `build_deriv_events` needs to build
/// both a `MarkFunding` and an `OpenInterest` row) arrives before any
/// `tickers` `snapshot` for the symbol. `should_drop_ticker_delta`'s
/// `ticker_seen` gate must drop it silently rather than merging it onto
/// `BybitTickerState::default()` — merging would still register both groups
/// as "changed" (None → Some) and, since every required field is present,
/// would emit both rows right there. The snapshot that follows seeds the
/// real state and emits its own rows; a further funding-rate-only delta
/// emits one more `MarkFunding` row. Exact final row counts (funding=2,
/// oi=1) prove the premature delta contributed nothing — a broken gate
/// would leave funding=3, oi=2.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_bybit_ticker_delta_gated_before_snapshot() {
    let server = MockBybitServer::new().await;
    let symbol = "ETHUSDT";

    server.push_ws_round(vec![
        // Premature delta: full field set, arrives before any snapshot —
        // must be dropped by ticker_seen gating, not merged.
        bybit_ticker_msg(
            symbol,
            "delta",
            1_700_000_000_000,
            serde_json::json!({
                "markPrice": "43001.0",
                "fundingRate": "0.0001",
                "openInterest": "500.0"
            }),
        ),
        bybit_ticker_msg(
            symbol,
            "snapshot",
            1_700_000_001_000,
            serde_json::json!({
                "markPrice": "43005.0",
                "indexPrice": "43004.5",
                "fundingRate": "0.00012",
                "nextFundingTime": "1700006400000",
                "openInterest": "600.0",
                "openInterestValue": "25800000.0"
            }),
        ),
        bybit_ticker_msg(
            symbol,
            "delta",
            1_700_000_002_000,
            serde_json::json!({ "fundingRate": "0.0002" }),
        ),
    ]);

    let dir = TempDir::new().unwrap();
    let (raw_tx, _) = broadcast::channel::<RawDiff>(256);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(256);
    let (trade_tx, _) = broadcast::channel::<RawTrade>(256);
    let (deriv_tx, _) = broadcast::channel::<DerivEvent>(256);

    let deriv_w = tokio::spawn(run_deriv_writer(
        dir.path().to_path_buf(),
        deriv_tx.subscribe(),
        60,
        fathom::metrics::new_metrics().metrics,
    ));

    let state = monitor::new_state();
    let conn = perp_conn("bybit_perp_ticker_gate_e2e", vec![symbol], &server);
    let task = tokio::spawn(connection_task_bybit(
        conn,
        Box::new(BybitPerp),
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
        trade_tx,
        deriv_tx,
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    tokio::time::sleep(Duration::from_millis(800)).await;
    task.abort();
    let _ = task.await;
    deriv_w.await.unwrap();

    let all = find_parquets(dir.path());
    let derivs = by_dir(&all, "deriv");
    let funding = by_stem_prefix(&derivs, "funding_").expect("funding_HHMM_HHMM.parquet");
    assert_eq!(
        count_rows(funding),
        2,
        "snapshot + funding-rate delta only; the premature pre-snapshot delta must not have emitted a row"
    );
    let oi = by_stem_prefix(&derivs, "oi_").expect("oi_HHMM_HHMM.parquet");
    assert_eq!(
        count_rows(oi),
        1,
        "snapshot only; the premature pre-snapshot delta must not have emitted an OI row"
    );
}
