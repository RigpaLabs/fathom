use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    accumulator::{Snapshot1s, WindowAccumulator},
    config::ConnectionConfig,
    error::AppError,
    exchange::ExchangeAdapter,
    monitor::MonitorState,
    orderbook::{DepthDiff, OrderBook, SnapshotMsg},
    writer::raw::RawDiff,
};

const BACKOFF_START_MS: u64 = 1_000;
const BACKOFF_MAX_MS: u64 = 60_000;
pub const HEARTBEAT_TIMEOUT_S: u64 = 30;

// ── Binance WS message types ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct WsCombined {
    stream: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct DepthUpdate {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub final_update_id: i64,
    /// Binance USDM Futures only: previous final update ID.
    /// Absent from spot events (defaults to None).
    #[serde(rename = "pu", default)]
    pub prev_final_update_id: Option<i64>,
    #[serde(rename = "b")]
    pub bids: Vec<[serde_json::Value; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[serde_json::Value; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct SnapshotRest {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,
    pub bids: Vec<[serde_json::Value; 2]>,
    pub asks: Vec<[serde_json::Value; 2]>,
}

pub fn parse_level(v: &[serde_json::Value; 2]) -> Option<(f64, f64)> {
    let px = v[0].as_str()?.parse::<f64>().ok()?;
    let qty = v[1].as_str()?.parse::<f64>().ok()?;
    Some((px, qty))
}

// ── Connection task ───────────────────────────────────────────────────────────

pub async fn connection_task(
    conn: ConnectionConfig,
    adapter: Box<dyn ExchangeAdapter>,
    _data_dir: PathBuf,
    monitor: MonitorState,
    raw_tx: mpsc::Sender<RawDiff>,
    snap_tx: mpsc::Sender<Snapshot1s>,
) {
    let name = conn.name.clone();
    let symbols: Vec<String> = conn.symbols.iter().map(|s| s.to_uppercase()).collect();

    let mut books: HashMap<String, OrderBook> = symbols
        .iter()
        .map(|s| (s.clone(), OrderBook::new()))
        .collect();
    let mut accumulators: HashMap<String, WindowAccumulator> = HashMap::new();
    let mut last_1s_flush: HashMap<String, Instant> = HashMap::new();

    {
        let mut state = monitor.lock().unwrap();
        let cs = state.entry(name.clone()).or_default();
        cs.connected = false;
        for sym in &symbols {
            cs.symbols.entry(sym.clone()).or_default();
        }
    }

    let mut backoff_ms = BACKOFF_START_MS;
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client");

    loop {
        info!(conn = %name, "connecting...");

        // Build WS URL — use override if provided (for tests)
        let ws_url = conn
            .ws_url_override
            .clone()
            .unwrap_or_else(|| adapter.ws_url(&symbols, conn.depth_ms));

        let ws = match connect_async(&ws_url).await {
            Ok((ws, _)) => {
                info!(conn = %name, url = %ws_url, "WS connected");
                backoff_ms = BACKOFF_START_MS;
                ws
            }
            Err(e) => {
                warn!(conn = %name, error = %e, "WS connect failed");
                sleep_backoff(&mut backoff_ms).await;
                continue;
            }
        };

        // ── Variant A: forwarder buffers WS events while REST snapshots are fetched ──
        // Spawning the forwarder immediately means:
        //  - Ping frames are answered even during the snapshot HTTP round-trips.
        //  - Events queued in the OS TCP buffer before snapshot fetch completes
        //    are forwarded to fwd_rx and processed in order.
        let (ws_sink, ws_stream) = ws.split();
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<String>(4_096);
        let fwd_name = name.clone();
        let forwarder = tokio::spawn(async move {
            let hb_dur = Duration::from_secs(HEARTBEAT_TIMEOUT_S);
            let mut sink = ws_sink;
            let mut stream = ws_stream;
            loop {
                match tokio::time::timeout(hb_dur, stream.next()).await {
                    Err(_) => { warn!(conn = %fwd_name, "heartbeat timeout"); break; }
                    Ok(None) => { info!(conn = %fwd_name, "WS stream closed"); break; }
                    Ok(Some(Err(e))) => { warn!(conn = %fwd_name, error = %e, "WS error"); break; }
                    Ok(Some(Ok(msg))) => match msg {
                        Message::Text(t) => {
                            if fwd_tx.send(t.to_string()).await.is_err() { break; }
                        }
                        Message::Binary(b) => {
                            if let Ok(s) = String::from_utf8(b.into())
                                && fwd_tx.send(s).await.is_err() { break; }
                        }
                        Message::Ping(p) => { let _ = sink.send(Message::Pong(p)).await; }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
            }
            // Dropping fwd_tx signals the main loop that the stream is done.
        });

        // Fetch REST snapshots per symbol.
        // A failure for one symbol only skips that symbol — other symbols keep
        // their WS stream.  The skipped symbol's book stays unsynced and will
        // return SnapshotRequired on the first incoming diff, triggering a full
        // reconnect (which re-fetches all snapshots).
        for sym in &symbols {
            // Build snapshot URL — use override template if provided
            let snap_url = conn
                .snapshot_url_override
                .as_ref()
                .map(|t| t.replace("{symbol}", sym))
                .unwrap_or_else(|| adapter.snapshot_url(sym));

            match http_client.get(&snap_url).send().await {
                Err(e) => {
                    warn!(conn = %name, symbol = %sym, error = %e, "snapshot fetch failed — skipping symbol");
                    continue;
                }
                Ok(resp) => match resp.json::<SnapshotRest>().await {
                    Err(e) => {
                        warn!(conn = %name, symbol = %sym, error = %e, "snapshot parse failed — skipping symbol");
                        continue;
                    }
                    Ok(snap) => {
                        info!(conn = %name, symbol = %sym, last_update_id = snap.last_update_id, "snapshot ok");
                        let bids: Vec<(f64, f64)> = snap.bids.iter().filter_map(parse_level).collect();
                        let asks: Vec<(f64, f64)> = snap.asks.iter().filter_map(parse_level).collect();
                        books.entry(sym.clone()).or_default().apply_snapshot(SnapshotMsg {
                            symbol: sym.clone(),
                            last_update_id: snap.last_update_id,
                            bids,
                            asks,
                        });
                    }
                },
            }
        }

        {
            let mut state = monitor.lock().unwrap();
            if let Some(cs) = state.get_mut(&name) {
                cs.connected = true;
            }
        }

        loop {
            let text = match fwd_rx.recv().await {
                None => break, // forwarder closed (stream ended, timeout, or error)
                Some(t) => t,
            };

            let combined: WsCombined = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let sym_lower = combined.stream.split('@').next().unwrap_or("").to_string();
            let symbol = sym_lower.to_uppercase();
            if !symbols.contains(&symbol) { continue; }

            let depth: DepthUpdate = match serde_json::from_value(combined.data) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let bids: Vec<(f64, f64)> = depth.bids.iter().filter_map(parse_level).collect();
            let asks: Vec<(f64, f64)> = depth.asks.iter().filter_map(parse_level).collect();
            let timestamp_us = depth.event_time_ms * 1_000;

            let diff = DepthDiff {
                exchange: adapter.name().to_string(),
                symbol: symbol.clone(),
                timestamp_us,
                seq_id: depth.final_update_id,
                prev_seq_id: depth.first_update_id,
                prev_final_update_id: depth.prev_final_update_id,
                bids: bids.clone(),
                asks: asks.clone(),
            };

            let book = books.entry(symbol.clone()).or_default();

            match book.apply_diff(&diff) {
                Err(AppError::SnapshotRequired(_)) | Err(AppError::OrderBookGap { .. }) => {
                    warn!(conn = %name, symbol = %symbol, "gap — reconnecting");
                    {
                        let mut state = monitor.lock().unwrap();
                        if let Some(cs) = state.get_mut(&name)
                            && let Some(ss) = cs.symbols.get_mut(&symbol) {
                            ss.gaps_today += 1;
                        }
                    }
                    break;
                }
                Err(e) => { warn!(error = %e, "book error"); continue; }
                Ok(None) => continue,
                Ok(Some(applied)) => {
                    {
                        let mut state = monitor.lock().unwrap();
                        if let Some(cs) = state.get_mut(&name)
                            && let Some(ss) = cs.symbols.get_mut(&symbol) {
                            ss.last_event_at = Some(Instant::now());
                        }
                    }

                    let _ = raw_tx.try_send(RawDiff {
                        timestamp_us,
                        exchange: adapter.name().to_string(),
                        symbol: symbol.clone(),
                        seq_id: diff.seq_id,
                        prev_seq_id: diff.prev_seq_id,
                        bids,
                        asks,
                    });

                    let now = Instant::now();
                    let acc = accumulators.entry(symbol.clone()).or_insert_with(|| {
                        WindowAccumulator::new(adapter.name(), &symbol, timestamp_us)
                    });
                    acc.on_diff(book, &applied);

                    let last_flush = last_1s_flush.entry(symbol.clone()).or_insert(now);
                    if now.duration_since(*last_flush) >= Duration::from_secs(1) {
                        let snap = acc.flush(book, timestamp_us);
                        let _ = snap_tx.try_send(snap);
                        *last_flush = now;
                    }
                }
            }
        }

        forwarder.abort();
        let _ = forwarder.await;

        {
            let mut state = monitor.lock().unwrap();
            if let Some(cs) = state.get_mut(&name) {
                cs.connected = false;
                cs.reconnects_today += 1;
            }
        }

        for book in books.values_mut() { *book = OrderBook::new(); }
        // Clear per-symbol accumulators so stale OFI/open_px/intra_sigma from the
        // previous session don't bleed into the first 1s snapshot after reconnect.
        // last_1s_flush is intentionally kept so the flush timer continues correctly.
        accumulators.clear();
        sleep_backoff(&mut backoff_ms).await;
    }
}

pub async fn sleep_backoff(backoff_ms: &mut u64) {
    let jitter = rand::thread_rng().gen_range(0..*backoff_ms / 4 + 1);
    let sleep_ms = (*backoff_ms + jitter).min(BACKOFF_MAX_MS);
    info!("reconnect backoff {}ms", sleep_ms);
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    *backoff_ms = (*backoff_ms * 2).min(BACKOFF_MAX_MS);
}
