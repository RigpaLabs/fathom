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

        let (mut ws_sink, mut ws_stream) = ws.split();

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

        let hb_dur = Duration::from_secs(HEARTBEAT_TIMEOUT_S);

        loop {
            let msg = match tokio::time::timeout(hb_dur, ws_stream.next()).await {
                Err(_) => { warn!(conn = %name, "heartbeat timeout"); break; }
                Ok(None) => { info!(conn = %name, "WS stream closed"); break; }
                Ok(Some(Err(e))) => { warn!(conn = %name, error = %e, "WS error"); break; }
                Ok(Some(Ok(m))) => m,
            };

            let text: String = match msg {
                Message::Text(t) => t.to_string(),
                Message::Binary(b) => match String::from_utf8(b.into()) {
                    Ok(s) => s,
                    Err(_) => continue,
                },
                Message::Ping(p) => { let _ = ws_sink.send(Message::Pong(p)).await; continue; }
                Message::Close(_) => break,
                _ => continue,
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
                bids: bids.clone(),
                asks: asks.clone(),
            };

            let book = books.entry(symbol.clone()).or_default();

            match book.apply_diff(&diff) {
                Err(AppError::SnapshotRequired(_)) | Err(AppError::OrderBookGap { .. }) => {
                    warn!(conn = %name, symbol = %symbol, "gap — reconnecting");
                    {
                        let mut state = monitor.lock().unwrap();
                        if let Some(cs) = state.get_mut(&name) {
                            if let Some(ss) = cs.symbols.get_mut(&symbol) {
                                ss.gaps_today += 1;
                            }
                        }
                    }
                    break;
                }
                Err(e) => { warn!(error = %e, "book error"); continue; }
                Ok(None) => continue,
                Ok(Some(applied)) => {
                    {
                        let mut state = monitor.lock().unwrap();
                        if let Some(cs) = state.get_mut(&name) {
                            if let Some(ss) = cs.symbols.get_mut(&symbol) {
                                ss.last_event_at = Some(Instant::now());
                            }
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
