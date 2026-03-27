use std::{collections::HashMap, path::PathBuf, time::Duration};

use chrono::Utc;

use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Snapshot1s, WindowAccumulator},
    config::ConnectionConfig,
    error::AppError,
    exchange::ExchangeAdapter,
    metrics::Metrics,
    monitor::MonitorState,
    orderbook::{DepthDiff, OrderBook, SnapshotMsg},
    writer::raw::RawDiff,
};

use super::runtime::{self, BACKOFF_START_MS, DEFAULT_HEARTBEAT_TIMEOUT_S, RATE_LIMIT_BACKOFF_S};

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

#[derive(Debug, Deserialize)]
struct BinanceError {
    code: i64,
    msg: String,
}

pub fn parse_level(v: &[serde_json::Value; 2]) -> Option<(f64, f64)> {
    let px = v[0].as_str()?.parse::<f64>().ok()?;
    let qty = v[1].as_str()?.parse::<f64>().ok()?;
    Some((px, qty))
}

#[allow(clippy::type_complexity)]
fn parse_depth_levels(depth: &DepthUpdate) -> (Vec<(f64, f64)>, Vec<(f64, f64)>, usize) {
    let mut errs = 0usize;
    let bids: Vec<(f64, f64)> = depth
        .bids
        .iter()
        .filter_map(|v| match parse_level(v) {
            Some(l) => Some(l),
            None => {
                errs += 1;
                None
            }
        })
        .collect();
    let asks: Vec<(f64, f64)> = depth
        .asks
        .iter()
        .filter_map(|v| match parse_level(v) {
            Some(l) => Some(l),
            None => {
                errs += 1;
                None
            }
        })
        .collect();
    (bids, asks, errs)
}

// ── Connection task ───────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn connection_task(
    conn: ConnectionConfig,
    adapter: Box<dyn ExchangeAdapter>,
    _data_dir: PathBuf,
    monitor: MonitorState,
    raw_tx: broadcast::Sender<RawDiff>,
    snap_tx: broadcast::Sender<Snapshot1s>,
    cancel: CancellationToken,
    metrics: std::sync::Arc<Metrics>,
) {
    let name = conn.name.clone();
    let exchange_name = adapter.name().to_string();
    let symbols: Vec<String> = conn.symbols.iter().map(|s| s.to_uppercase()).collect();
    let symbols_set: std::collections::HashSet<String> = symbols.iter().cloned().collect();

    let mut books: HashMap<String, OrderBook> = symbols
        .iter()
        .map(|s| (s.clone(), OrderBook::new()))
        .collect();
    let mut accumulators: HashMap<String, WindowAccumulator> = HashMap::new();

    runtime::init_monitor(&monitor, &name, &symbols);

    let mut backoff_ms = BACKOFF_START_MS;
    #[allow(clippy::expect_used)] // infallible: no custom TLS config
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client");

    loop {
        if cancel.is_cancelled() {
            info!(conn = %name, "shutdown signal received, exiting connection loop");
            break;
        }
        info!(conn = %name, "connecting...");

        let ws_url = conn
            .ws_url_override
            .clone()
            .unwrap_or_else(|| adapter.ws_url(&symbols, conn.depth_ms));

        let ws = match runtime::connect_ws(&ws_url, &name, &mut backoff_ms).await {
            Some(ws) => ws,
            None => continue,
        };

        let (ws_sink, ws_stream) = ws.split();
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<String>(crate::CHANNEL_BUFFER);
        let forwarder = runtime::spawn_forwarder(
            name.clone(),
            ws_sink,
            ws_stream,
            DEFAULT_HEARTBEAT_TIMEOUT_S,
            fwd_tx,
        );

        // Fetch REST snapshots for all symbols in parallel (bounded concurrency: max 8).
        let mut rate_limited = false;

        use futures_util::stream;
        let snap_futs: Vec<_> = symbols
            .iter()
            .map(|sym| {
                let snap_url = conn
                    .snapshot_url_override
                    .as_ref()
                    .map(|t| t.replace("{symbol}", sym))
                    .unwrap_or_else(|| adapter.snapshot_url(sym));
                let client = &http_client;
                let sym = sym.clone();
                async move {
                    match client.get(&snap_url).send().await {
                        Ok(resp) => {
                            let status = resp.status();
                            match resp.text().await {
                                Ok(body) => (sym, Ok((status, body))),
                                Err(e) => (sym, Err(e)),
                            }
                        }
                        Err(e) => (sym, Err(e)),
                    }
                }
            })
            .collect();
        let snap_results: Vec<_> = tokio::select! {
            results = stream::iter(snap_futs).buffer_unordered(8).collect() => results,
            _ = cancel.cancelled() => {
                warn!(conn = %name, "snapshot fetch cancelled during shutdown");
                forwarder.abort();
                let _ = forwarder.await;
                break;
            }
        };

        for (sym, result) in snap_results {
            match result {
                Err(e) => {
                    warn!(conn = %name, symbol = %sym, error = %e, "snapshot fetch failed — skipping symbol");
                }
                Ok((status, body)) => {
                    if !status.is_success() {
                        if let Ok(err) = serde_json::from_str::<BinanceError>(&body)
                            && (err.code == -1003 || err.code == -1015)
                        {
                            warn!(conn = %name, code = err.code, msg = %err.msg, "Binance rate limit — backing off");
                            rate_limited = true;
                            break;
                        }
                        warn!(conn = %name, symbol = %sym, status = %status, "snapshot HTTP error — skipping symbol");
                        continue;
                    }
                    match serde_json::from_str::<SnapshotRest>(&body) {
                        Err(_) => {
                            if let Ok(err) = serde_json::from_str::<BinanceError>(&body)
                                && (err.code == -1003 || err.code == -1015)
                            {
                                warn!(conn = %name, code = err.code, msg = %err.msg, "Binance rate limit — backing off");
                                rate_limited = true;
                                break;
                            }
                            warn!(conn = %name, symbol = %sym, "snapshot parse failed — skipping symbol");
                        }
                        Ok(snap) => {
                            info!(conn = %name, symbol = %sym, last_update_id = snap.last_update_id, "snapshot ok");
                            let bids: Vec<(f64, f64)> =
                                snap.bids.iter().filter_map(parse_level).collect();
                            let asks: Vec<(f64, f64)> =
                                snap.asks.iter().filter_map(parse_level).collect();
                            books
                                .entry(sym.clone())
                                .or_default()
                                .apply_snapshot(SnapshotMsg {
                                    symbol: sym.clone(),
                                    last_update_id: snap.last_update_id,
                                    bids,
                                    asks,
                                });
                        }
                    }
                }
            }
        }

        if rate_limited {
            warn!(conn = %name, backoff_s = RATE_LIMIT_BACKOFF_S, "rate limited — extended backoff");
            forwarder.abort();
            let _ = forwarder.await;
            runtime::mark_disconnected(&monitor, &name);
            for book in books.values_mut() {
                *book = OrderBook::new();
            }
            accumulators.clear();
            tokio::time::sleep(Duration::from_secs(RATE_LIMIT_BACKOFF_S)).await;
            continue;
        }

        runtime::mark_connected(&monitor, &name);

        // ── Sync phase: drain buffered WS events → replay → re-snapshot unsynced ──
        tokio::time::sleep(Duration::from_millis(150)).await;

        let is_perp = exchange_name == "binance_perp";
        let mut sync_gap_detected = false;
        'sync: for attempt in 0..3u32 {
            let mut buf: Vec<String> = Vec::new();
            while let Ok(msg) = fwd_rx.try_recv() {
                buf.push(msg);
            }
            debug!(conn = %name, attempt, buf_size = buf.len(), "sync phase drain");
            if buf.is_empty() && attempt > 0 {
                break 'sync;
            }

            for text in &buf {
                let combined: WsCombined = match serde_json::from_str(text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let sym_lower = combined.stream.split('@').next().unwrap_or("").to_string();
                let symbol = sym_lower.to_uppercase();
                if !symbols_set.contains(&symbol) {
                    continue;
                }

                let depth: DepthUpdate = match serde_json::from_value(combined.data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let (bids, asks, parse_errs) = parse_depth_levels(&depth);
                if parse_errs > 0 {
                    warn!(conn = %name, symbol = %symbol, errors = parse_errs, "parse errors in depth levels (sync)");
                }
                let timestamp_us = depth.event_time_ms * 1_000;

                let diff = DepthDiff {
                    exchange: exchange_name.clone(),
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
                    Err(AppError::SnapshotRequired(_)) => {
                        continue;
                    }
                    Err(AppError::OrderBookGap { .. }) => {
                        warn!(conn = %name, symbol = %symbol, "gap detected during sync replay — will reconnect");
                        runtime::record_gap(&monitor, &name, &symbol);
                        sync_gap_detected = true;
                        break 'sync;
                    }
                    Err(_) => continue,
                    Ok(None) => continue,
                    Ok(Some(applied)) => {
                        runtime::record_event(&monitor, &name, &symbol);

                        if raw_tx
                            .send(RawDiff {
                                timestamp_us,
                                exchange: exchange_name.clone(),
                                symbol: symbol.clone(),
                                seq_id: diff.seq_id,
                                prev_seq_id: diff.prev_seq_id,
                                bids,
                                asks,
                            })
                            .is_err()
                        {
                            warn!(conn = %name, symbol = %symbol, "raw: no receivers (sync phase)");
                        }

                        let acc = accumulators.entry(symbol.clone()).or_insert_with(|| {
                            WindowAccumulator::new(adapter.name(), &symbol, timestamp_us)
                        });
                        acc.on_diff(book, &applied);
                    }
                }
            }

            let unsynced: Vec<String> = books
                .iter()
                .filter(|(_, b)| !b.synced)
                .map(|(s, _)| s.clone())
                .collect();
            if unsynced.is_empty() {
                info!(conn = %name, attempt, "all symbols synced via buffer replay");
                break 'sync;
            }

            if attempt < 2 && !is_perp {
                info!(conn = %name, attempt, unsynced = ?unsynced, "re-snapshot for unsynced symbols");
                let re_futs: Vec<_> = unsynced
                    .iter()
                    .map(|sym| {
                        let snap_url = conn
                            .snapshot_url_override
                            .as_ref()
                            .map(|t| t.replace("{symbol}", sym))
                            .unwrap_or_else(|| adapter.snapshot_url(sym));
                        let client = &http_client;
                        let sym = sym.clone();
                        async move {
                            match client.get(&snap_url).send().await {
                                Ok(resp) => {
                                    let status = resp.status();
                                    match resp.text().await {
                                        Ok(body) => (sym, Ok((status, body))),
                                        Err(e) => (sym, Err(e)),
                                    }
                                }
                                Err(e) => (sym, Err(e)),
                            }
                        }
                    })
                    .collect();
                let re_snap_results: Vec<_> =
                    stream::iter(re_futs).buffer_unordered(8).collect().await;

                for (sym, result) in re_snap_results {
                    match result {
                        Err(e) => {
                            warn!(conn = %name, symbol = %sym, error = %e, "re-snapshot fetch failed");
                        }
                        Ok((status, body)) => {
                            if !status.is_success() {
                                if let Ok(err) = serde_json::from_str::<BinanceError>(&body)
                                    && (err.code == -1003 || err.code == -1015)
                                {
                                    warn!(conn = %name, code = err.code, msg = %err.msg, "rate limit during re-snapshot — aborting sync phase");
                                    break 'sync;
                                }
                                warn!(conn = %name, symbol = %sym, status = %status, "re-snapshot HTTP error");
                                continue;
                            }
                            match serde_json::from_str::<SnapshotRest>(&body) {
                                Err(_) => {
                                    if let Ok(err) = serde_json::from_str::<BinanceError>(&body)
                                        && (err.code == -1003 || err.code == -1015)
                                    {
                                        warn!(conn = %name, code = err.code, msg = %err.msg, "rate limit during re-snapshot — aborting sync phase");
                                        break 'sync;
                                    }
                                    warn!(conn = %name, symbol = %sym, "re-snapshot parse failed");
                                }
                                Ok(snap) => {
                                    info!(conn = %name, symbol = %sym, last_update_id = snap.last_update_id, "re-snapshot ok");
                                    let bids: Vec<(f64, f64)> =
                                        snap.bids.iter().filter_map(parse_level).collect();
                                    let asks: Vec<(f64, f64)> =
                                        snap.asks.iter().filter_map(parse_level).collect();
                                    books.entry(sym.clone()).or_default().apply_snapshot(
                                        SnapshotMsg {
                                            symbol: sym.clone(),
                                            last_update_id: snap.last_update_id,
                                            bids,
                                            asks,
                                        },
                                    );
                                }
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        {
            let synced_count = books.values().filter(|b| b.synced).count();
            info!(conn = %name, synced = synced_count, total = symbols.len(), "sync phase complete");
        }

        if sync_gap_detected {
            forwarder.abort();
            let _ = forwarder.await;
            runtime::mark_disconnected(&monitor, &name);
            for book in books.values_mut() {
                *book = OrderBook::new();
            }
            accumulators.clear();
            runtime::sleep_backoff(&mut backoff_ms).await;
            continue;
        }

        let mut snap_ticker = runtime::snap_ticker();
        snap_ticker.tick().await;

        let mut stats = runtime::StatsTracker::new();
        stats.skip_first_tick().await;

        'inner: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!(conn = %name, "shutdown signal — exiting event loop");
                    break 'inner;
                }

                msg = fwd_rx.recv() => {
                    let text = match msg {
                        None => break 'inner,
                        Some(t) => t,
                    };

                    let combined: WsCombined = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let sym_lower = combined.stream.split('@').next().unwrap_or("").to_string();
                    let symbol = sym_lower.to_uppercase();
                    if !symbols_set.contains(&symbol) { continue; }

                    let depth: DepthUpdate = match serde_json::from_value(combined.data) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let (bids, asks, parse_errs) = parse_depth_levels(&depth);
                    if parse_errs > 0 {
                        warn!(conn = %name, symbol = %symbol, errors = parse_errs, "parse errors in depth levels");
                    }
                    let timestamp_us = depth.event_time_ms * 1_000;

                    let diff = DepthDiff {
                        exchange: exchange_name.clone(),
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
                            runtime::record_gap(&monitor, &name, &symbol);
                            break 'inner;
                        }
                        Err(e) => { warn!(error = %e, "book error"); continue; }
                        Ok(None) => continue,
                        Ok(Some(applied)) => {
                            runtime::record_event(&monitor, &name, &symbol);

                            if raw_tx.send(RawDiff {
                                timestamp_us,
                                exchange: exchange_name.clone(),
                                symbol: symbol.clone(),
                                seq_id: diff.seq_id,
                                prev_seq_id: diff.prev_seq_id,
                                bids,
                                asks,
                            }).is_err() {
                                warn!(conn = %name, symbol = %symbol, "raw: no receivers");
                            }

                            let acc = accumulators.entry(symbol.clone()).or_insert_with(|| {
                                WindowAccumulator::new(adapter.name(), &symbol, timestamp_us)
                            });
                            acc.on_diff(book, &applied);
                            stats.inc();
                            runtime::inc_event_metrics(&metrics, &name, &symbol);
                        }
                    }
                }

                _ = snap_ticker.tick() => {
                    let ts_us = Utc::now().timestamp_micros();
                    for sym in &symbols {
                        if let Some(acc) = accumulators.get_mut(sym)
                            && let Some(book) = books.get(sym)
                        {
                            let snap = acc.flush(book, ts_us);
                            if snap_tx.send(snap).is_err() {
                                warn!(conn = %name, symbol = %sym, "snap: no receivers");
                            }
                        }
                    }
                }

                _ = stats.ticker.tick() => {
                    stats.log(&name, symbols.len());
                }
            }
        }

        forwarder.abort();
        let _ = forwarder.await;

        runtime::mark_disconnected(&monitor, &name);

        // Flush partial accumulators before resetting state.
        {
            let ts_us = Utc::now().timestamp_micros();
            for sym in &symbols {
                if let Some(acc) = accumulators.get_mut(sym)
                    && let Some(book) = books.get(sym)
                {
                    let snap = acc.flush(book, ts_us);
                    if snap_tx.send(snap).is_err() {
                        warn!(conn = %name, symbol = %sym, "snap: no receivers (disconnect flush)");
                    }
                }
            }
        }

        for book in books.values_mut() {
            *book = OrderBook::new();
        }
        accumulators.clear();
        runtime::sleep_backoff(&mut backoff_ms).await;
    }
}
