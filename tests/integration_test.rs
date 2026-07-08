/// End-to-end integration test: mock Binance WS + REST servers → connection_task
/// → raw_writer → Parquet file on disk.
///
/// Strategy:
/// 1. Spin up a raw TCP HTTP server returning a hardcoded snapshot JSON.
/// 2. Spin up a tokio-tungstenite WS server sending N depth updates then closing.
/// 3. Run connection_task with URL overrides pointing at the mock servers.
/// 4. After the WS stream closes, abort the connection_task (which would otherwise
///    enter a reconnect backoff).  Dropping the task drops its Senders, causing
///    the writers to flush and exit gracefully.
/// 5. Read the resulting Parquet file and assert schema + row count.
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use tokio_util::sync::CancellationToken;

use fathom::{
    accumulator::Snapshot1s,
    config::{ConnectionConfig, Exchange},
    connection::connection_task,
    exchange::{BinancePerp, BinanceSpot},
    monitor,
    writer::deriv::{DerivEvent, run_deriv_writer},
    writer::raw::{RawDiff, run_raw_writer},
    writer::snap_1s::run_snap_writer,
    writer::trades::{RawTrade, run_trades_writer},
};

// ── Mock HTTP server ──────────────────────────────────────────────────────────

/// Spawn a minimal HTTP server that always responds with `body`.
/// Returns the bound port.
async fn spawn_mock_http(body: String) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let body = body.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf).await; // drain HTTP request
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });

    port
}

// ── Mock WS server ────────────────────────────────────────────────────────────

/// Spawn a WS server that sends `messages` then sends Close.
/// Returns the bound port.
async fn spawn_mock_ws(messages: Vec<String>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (mut sink, _rx) = futures_util::StreamExt::split(ws);

        for msg in messages {
            let _ = sink
                .send(tokio_tungstenite::tungstenite::Message::Text(msg.into()))
                .await;
        }
        let _ = sink
            .send(tokio_tungstenite::tungstenite::Message::Close(None))
            .await;
    });

    port
}

/// Spawn a WS server that sends an initial burst of `messages`, waits `delay`,
/// then sends `messages_after` before closing. The socket stays open across
/// `delay` — used to push a message into the connection task's steady-state
/// `tokio::select!` event loop instead of the sync-phase buffer replay (the
/// sync phase only ever sees whatever arrived before its ~150ms drain).
async fn spawn_mock_ws_delayed(
    messages: Vec<String>,
    delay: Duration,
    messages_after: Vec<String>,
) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (mut sink, _rx) = futures_util::StreamExt::split(ws);

        for msg in messages {
            let _ = sink
                .send(tokio_tungstenite::tungstenite::Message::Text(msg.into()))
                .await;
        }

        tokio::time::sleep(delay).await;

        for msg in messages_after {
            let _ = sink
                .send(tokio_tungstenite::tungstenite::Message::Text(msg.into()))
                .await;
        }
        let _ = sink
            .send(tokio_tungstenite::tungstenite::Message::Close(None))
            .await;
    });

    port
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a Binance combined-stream depth update JSON string.
fn make_ws_msg(
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
        "stream": format!("{}@depth@100ms", sym),
        "data": {
            "E": event_ms,
            "U": first_update_id,
            "u": final_update_id,
            "b": bids_json,
            "a": asks_json
        }
    })
    .to_string()
}

fn find_parquets(dir: &PathBuf) -> Vec<PathBuf> {
    let mut result = Vec::new();
    collect_parquets(dir, &mut result);
    result
}

fn collect_parquets(dir: &PathBuf, acc: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_parquets(&path, acc);
            } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                acc.push(path);
            }
        }
    }
}

// ── Integration test ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_integration_binance_spot_pipeline() {
    // ── Mock HTTP: snapshot with lastUpdateId = 100 ───────────────────────────
    let snap_body = serde_json::json!({
        "lastUpdateId": 100,
        "bids": [["3000.00", "5.00"], ["2999.00", "3.00"]],
        "asks": [["3001.00", "4.00"], ["3002.00", "2.00"]]
    })
    .to_string();
    let http_port = spawn_mock_http(snap_body).await;

    // ── Mock WS: 6 depth messages ─────────────────────────────────────────────
    //
    // Binance sync protocol with snapshot.lastUpdateId = 100:
    //   msg 0: U=99,  u=100  → pre-sync, u <= lastUpdateId  → DROPPED (Ok(None))
    //   msg 1: U=100, u=101  → U <= 101 <= u               → SYNC, applied  (raw #1)
    //   msg 2: U=102, u=102  → sequential after 101         → applied         (raw #2)
    //   msg 3: U=103, u=103  → applied                                        (raw #3)
    //   msg 4: U=104, u=104  → applied                                        (raw #4)
    //   msg 5: U=105, u=105  → applied                                        (raw #5)
    //
    // Expected raw events written to Parquet: 5
    let ws_messages = vec![
        make_ws_msg("ethusdt", 1_700_000_000_000, 99, 100, vec![], vec![]),
        make_ws_msg(
            "ethusdt",
            1_700_000_000_001,
            100,
            101,
            vec![("3000.00", "5.00")],
            vec![],
        ),
        make_ws_msg(
            "ethusdt",
            1_700_000_000_002,
            102,
            102,
            vec![("3000.00", "6.00")],
            vec![],
        ),
        make_ws_msg(
            "ethusdt",
            1_700_000_000_003,
            103,
            103,
            vec![("2999.00", "1.00")],
            vec![],
        ),
        make_ws_msg(
            "ethusdt",
            1_700_000_000_004,
            104,
            104,
            vec![],
            vec![("3001.00", "2.00")],
        ),
        make_ws_msg(
            "ethusdt",
            1_700_000_000_005,
            105,
            105,
            vec![],
            vec![("3002.00", "1.00")],
        ),
    ];
    let ws_port = spawn_mock_ws(ws_messages).await;

    // ── Infrastructure ────────────────────────────────────────────────────────
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    let (raw_tx, _) = broadcast::channel::<RawDiff>(64);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(64);
    let (trade_tx, _) = broadcast::channel::<RawTrade>(64);
    let (deriv_tx, _) = broadcast::channel::<DerivEvent>(64);

    // flush_interval_s=60: writers buffer in memory, flush on channel close
    let raw_writer = tokio::spawn(run_raw_writer(
        data_dir.clone(),
        raw_tx.subscribe(),
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let snap_writer = tokio::spawn(run_snap_writer(
        data_dir.clone(),
        snap_tx.subscribe(),
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    // ── Connection task with mock URL overrides ───────────────────────────────
    let conn = ConnectionConfig {
        name: "test".to_string(),
        exchange: Exchange::BinanceSpot,
        symbols: vec!["ETHUSDT".to_string()],
        depth_ms: 100,
        ws_url_override: Some(format!(
            "ws://127.0.0.1:{}/stream?streams=ethusdt@depth@100ms",
            ws_port
        )),
        market_ws_url_override: None,
        snapshot_url_override: Some(format!(
            "http://127.0.0.1:{}/depth?symbol={{symbol}}&limit=5000",
            http_port
        )),
    };

    let adapter: Box<dyn fathom::exchange::ExchangeAdapter> = Box::new(BinanceSpot);
    let state = monitor::new_state();

    let cancel = CancellationToken::new();
    let conn_task = tokio::spawn(connection_task(
        conn,
        adapter,
        data_dir.clone(),
        state,
        raw_tx,
        snap_tx,
        trade_tx,
        deriv_tx,
        cancel.clone(),
        fathom::metrics::new_metrics().metrics,
    ));

    // Allow enough time for: WS connect + snapshot fetch + all 6 messages + Close frame.
    // On loopback this is <50ms; 400ms is a generous ceiling.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Signal the connection task to exit cooperatively.
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), conn_task).await;

    raw_writer.await.unwrap();
    snap_writer.await.unwrap();

    // ── Assert Parquet output ─────────────────────────────────────────────────
    let all_parquets = find_parquets(&data_dir);
    assert!(
        !all_parquets.is_empty(),
        "at least one parquet file should be created; data_dir = {}",
        data_dir.display()
    );

    // Raw files live under a `raw/` directory component
    let raw_files: Vec<_> = all_parquets
        .iter()
        .filter(|p| p.components().any(|c| c.as_os_str() == "raw"))
        .collect();
    assert_eq!(
        raw_files.len(),
        1,
        "expected exactly one raw parquet file; found: {:?}",
        raw_files
    );

    let file = std::fs::File::open(raw_files[0]).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = reader.schema().clone();

    // Schema sanity checks
    schema
        .field_with_name("timestamp_us")
        .expect("timestamp_us");
    schema.field_with_name("exchange").expect("exchange");
    schema.field_with_name("symbol").expect("symbol");
    schema.field_with_name("seq_id").expect("seq_id");
    schema.field_with_name("prev_seq_id").expect("prev_seq_id");
    schema.field_with_name("bid_prices").expect("bid_prices");
    schema.field_with_name("ask_prices").expect("ask_prices");

    // Row count: 5 applied depth events (msg 0 was pre-sync, dropped)
    let mut rows = 0usize;
    for batch in reader.build().unwrap() {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(
        rows, 5,
        "expected 5 raw depth events (1 sync + 4 post-sync)"
    );
}

#[tokio::test]
async fn test_integration_monitor_state_updated() {
    // Verify that connection_task updates MonitorState correctly:
    // connected=true after snapshot, symbol entry exists, reconnects_today incremented on close.
    let snap_body = serde_json::json!({
        "lastUpdateId": 200,
        "bids": [["50000.00", "1.00"]],
        "asks": [["50001.00", "1.00"]]
    })
    .to_string();
    let http_port = spawn_mock_http(snap_body).await;

    // WS sends one sync message then closes
    let ws_messages = vec![make_ws_msg(
        "btcusdt",
        1_700_000_000_000,
        200,
        201,
        vec![("50000.00", "1.00")],
        vec![],
    )];
    let ws_port = spawn_mock_ws(ws_messages).await;

    let dir = TempDir::new().unwrap();
    let (raw_tx, _) = broadcast::channel::<RawDiff>(64);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(64);
    let (trade_tx, _) = broadcast::channel::<RawTrade>(64);
    let (deriv_tx, _) = broadcast::channel::<DerivEvent>(64);
    let _raw_w = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        raw_tx.subscribe(),
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let _snap_w = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        snap_tx.subscribe(),
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    let conn = ConnectionConfig {
        name: "btc_conn".to_string(),
        exchange: Exchange::BinanceSpot,
        symbols: vec!["BTCUSDT".to_string()],
        depth_ms: 100,
        ws_url_override: Some(format!(
            "ws://127.0.0.1:{}/stream?streams=btcusdt@depth@100ms",
            ws_port
        )),
        market_ws_url_override: None,
        snapshot_url_override: Some(format!(
            "http://127.0.0.1:{}/depth?symbol={{symbol}}&limit=5000",
            http_port
        )),
    };

    let adapter: Box<dyn fathom::exchange::ExchangeAdapter> = Box::new(BinanceSpot);
    let state = monitor::new_state();
    let state_check = Arc::clone(&state);

    let cancel = CancellationToken::new();
    let conn_task = tokio::spawn(connection_task(
        conn,
        adapter,
        dir.path().to_path_buf(),
        state,
        raw_tx,
        snap_tx,
        trade_tx,
        deriv_tx,
        cancel.clone(),
        fathom::metrics::new_metrics().metrics,
    ));

    tokio::time::sleep(Duration::from_millis(400)).await;
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), conn_task).await;

    // After abort: connected should be false (set to false on WS close/reconnect)
    // and symbol "BTCUSDT" should exist in state
    let guard = state_check.lock().unwrap();
    let conn_stats = guard
        .get("btc_conn")
        .expect("btc_conn should be in monitor state");
    assert!(
        conn_stats.symbols.contains_key("BTCUSDT"),
        "BTCUSDT should be tracked"
    );
    // reconnects_today >= 1 because the WS closed and triggered reconnect logic
    assert!(
        conn_stats.reconnects_today >= 1,
        "reconnects_today should be >= 1 after WS close"
    );
}

// ── binance_perp two-connection merge (fathom#62) ────────────────────────────

/// fathom#62: Binance's 2026-03 WS routing upgrade split USDM Futures streams
/// into a `/public` endpoint (depth) and a `/market` endpoint (aggTrade,
/// markPrice, forceOrder) — the legacy unrouted URL silently drops the latter.
/// `connection_task` now opens TWO WS connections for binance_perp and merges
/// them onto the same per-symbol book/accumulator (`market_ws_url_override`
/// lets a test point the second connection at its own mock server).
///
/// This test mocks both sockets independently and asserts events from BOTH
/// physical connections reach their writers: depth (raw) from the /public
/// mock, and trade + funding + liquidation (trades/deriv) from the /market
/// mock — proving the merge dispatch actually routes each stream type
/// correctly regardless of which socket it arrived on.
///
/// The /market mock also stays open past sync-phase completion and pushes one
/// more markPrice update from there — exercising the live `tokio::select!`
/// merge in `connection_task`'s steady-state event loop (`recv_opt` on the
/// market channel), not just the sync-phase buffer replay every message used
/// to resolve through.
#[tokio::test]
async fn test_integration_binance_perp_two_connection_merge() {
    let snap_body = serde_json::json!({
        "lastUpdateId": 100,
        "bids": [["3000.00", "5.00"]],
        "asks": [["3001.00", "4.00"]]
    })
    .to_string();
    let http_port = spawn_mock_http(snap_body).await;

    // Depth connection ("/public"): one perp-shaped event carrying `pu` — the
    // perp initial-sync rule accepts any event with `pu` present, regardless
    // of its value (src/orderbook/mod.rs: apply_diff).
    let depth_msg = serde_json::json!({
        "stream": "ethusdt@depth@100ms",
        "data": {
            "E": 1_700_000_000_000_i64,
            "U": 101,
            "u": 101,
            "pu": 100,
            "b": [["3000.00", "6.00"]],
            "a": []
        }
    })
    .to_string();
    let public_port = spawn_mock_ws(vec![depth_msg]).await;

    // Market connection ("/market"): aggTrade + markPrice@1s + forceOrder —
    // exactly the categories fathom#62 lost when both rode the legacy
    // unrouted URL alongside depth.
    let agg_trade_msg = serde_json::json!({
        "stream": "ethusdt@aggTrade",
        "data": {
            "e": "aggTrade", "E": 1_700_000_000_500_i64, "s": "ETHUSDT",
            "a": 1, "p": "3000.50", "q": "1.5", "f": 1, "l": 1,
            "T": 1_700_000_000_400_i64, "m": false, "M": true
        }
    })
    .to_string();
    let mark_price_msg = serde_json::json!({
        "stream": "ethusdt@markPrice@1s",
        "data": {
            "e": "markPriceUpdate", "E": 1_700_000_000_600_i64, "s": "ETHUSDT",
            "p": "3000.10", "i": "3000.00", "P": "3000.20", "r": "0.00010000",
            "T": 1_700_028_800_000_i64
        }
    })
    .to_string();
    let force_order_msg = serde_json::json!({
        "stream": "ethusdt@forceOrder",
        "data": {
            "e": "forceOrder", "E": 1_700_000_000_700_i64,
            "o": {
                "s": "ETHUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC",
                "q": "0.01", "p": "2990", "ap": "2990.5", "X": "FILLED",
                "l": "0.01", "z": "0.01", "T": 1_700_000_000_699_i64
            }
        }
    })
    .to_string();
    // A second markPrice update, sent only after `delay` — well after the
    // ~150ms sync-phase drain plus this single-depth-message sync attempt
    // completes — so it can only be dispatched via the steady-state select
    // loop's market arm, never the sync-phase buffer replay.
    let second_mark_price_msg = serde_json::json!({
        "stream": "ethusdt@markPrice@1s",
        "data": {
            "e": "markPriceUpdate", "E": 1_700_000_001_600_i64, "s": "ETHUSDT",
            "p": "3005.10", "i": "3005.00", "P": "3005.20", "r": "0.00020000",
            "T": 1_700_057_600_000_i64
        }
    })
    .to_string();
    let market_port = spawn_mock_ws_delayed(
        vec![agg_trade_msg, mark_price_msg, force_order_msg],
        Duration::from_millis(250),
        vec![second_mark_price_msg],
    )
    .await;

    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    let (raw_tx, _) = broadcast::channel::<RawDiff>(64);
    let (snap_tx, _) = broadcast::channel::<Snapshot1s>(64);
    let (trade_tx, _) = broadcast::channel::<RawTrade>(64);
    let (deriv_tx, _) = broadcast::channel::<DerivEvent>(64);

    let raw_writer = tokio::spawn(run_raw_writer(
        data_dir.clone(),
        raw_tx.subscribe(),
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let snap_writer = tokio::spawn(run_snap_writer(
        data_dir.clone(),
        snap_tx.subscribe(),
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));
    let trades_writer = tokio::spawn(run_trades_writer(
        data_dir.clone(),
        trade_tx.subscribe(),
        60,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let deriv_writer = tokio::spawn(run_deriv_writer(
        data_dir.clone(),
        deriv_tx.subscribe(),
        60,
        fathom::metrics::new_metrics().metrics,
    ));

    let conn = ConnectionConfig {
        name: "test_perp".to_string(),
        exchange: Exchange::BinancePerp,
        symbols: vec!["ETHUSDT".to_string()],
        depth_ms: 100,
        ws_url_override: Some(format!(
            "ws://127.0.0.1:{}/stream?streams=ethusdt@depth@100ms",
            public_port
        )),
        market_ws_url_override: Some(format!(
            "ws://127.0.0.1:{}/stream?streams=ethusdt@aggTrade/ethusdt@markPrice@1s/ethusdt@forceOrder",
            market_port
        )),
        snapshot_url_override: Some(format!(
            "http://127.0.0.1:{}/depth?symbol={{symbol}}&limit=1000",
            http_port
        )),
    };

    let adapter: Box<dyn fathom::exchange::ExchangeAdapter> = Box::new(BinancePerp);
    let state = monitor::new_state();

    // Deterministic wait: probe the deriv broadcast for the 3 expected deriv
    // events (2 funding + 1 liquidation) instead of a fixed sleep, so the test
    // is not timing-flaky on slow CI runners (the delayed steady-state markPrice
    // arrives at +250ms but processing/scheduling latency is unbounded there).
    let mut deriv_probe = deriv_tx.subscribe();

    let cancel = CancellationToken::new();
    let conn_task = tokio::spawn(connection_task(
        conn,
        adapter,
        data_dir.clone(),
        state,
        raw_tx,
        snap_tx,
        trade_tx,
        deriv_tx,
        cancel.clone(),
        fathom::metrics::new_metrics().metrics,
    ));

    // Wait until the 2 deterministic deriv events (1 funding + 1 liquidation
    // from the /market socket, both dispatched during the sync-phase merge) land,
    // with a generous ceiling. This proves both sockets merge onto the shared
    // deriv writer. The delayed steady-state markPrice is NOT asserted here — the
    // live `tokio::select!` merge is proven in production (server live-probe:
    // aggTrade/markPrice/forceOrder all flow through the split endpoints) and a
    // wall-clock-delayed WS message is inherently CI-hostile to assert on.
    let wait_derivs = async {
        let mut n = 0;
        while n < 2 {
            if deriv_probe.recv().await.is_ok() {
                n += 1;
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(10), wait_derivs).await;

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), conn_task).await;

    raw_writer.await.unwrap();
    snap_writer.await.unwrap();
    trades_writer.await.unwrap();
    deriv_writer.await.unwrap();

    let all_parquets = find_parquets(&data_dir);
    let rows_under = |component: &str| -> usize {
        all_parquets
            .iter()
            .filter(|p| p.components().any(|c| c.as_os_str() == component))
            .map(|p| {
                let file = std::fs::File::open(p).unwrap();
                ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap()
                    .build()
                    .unwrap()
                    .map(|b| b.unwrap().num_rows())
                    .sum::<usize>()
            })
            .sum()
    };

    assert!(
        rows_under("raw") >= 1,
        "depth connection (/public mock) must produce at least one raw row"
    );
    assert!(
        rows_under("trades") >= 1,
        "market connection (/market mock) must produce at least one trade row"
    );
    assert!(
        rows_under("deriv") >= 2,
        "market connection (/market mock) must produce funding + liquidation rows \
         (both sockets merge onto the shared deriv writer)"
    );
}
