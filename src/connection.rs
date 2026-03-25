use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use chrono::Utc;

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Snapshot1s, WindowAccumulator},
    config::ConnectionConfig,
    error::AppError,
    exchange::ExchangeAdapter,
    monitor::{MonitorState, lock_state},
    orderbook::{DepthDiff, OrderBook, SnapshotMsg},
    writer::raw::RawDiff,
};

const BACKOFF_START_MS: u64 = 1_000;
const BACKOFF_MAX_MS: u64 = 60_000;
const RATE_LIMIT_BACKOFF_S: u64 = 300;
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

/// Parse bid/ask levels from a DepthUpdate, counting parse failures instead of silently dropping.
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

pub async fn connection_task(
    conn: ConnectionConfig,
    adapter: Box<dyn ExchangeAdapter>,
    _data_dir: PathBuf,
    monitor: MonitorState,
    raw_tx: broadcast::Sender<RawDiff>,
    snap_tx: broadcast::Sender<Snapshot1s>,
    cancel: CancellationToken,
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

    {
        let mut state = lock_state(&monitor);
        let cs = state.entry(name.clone()).or_default();
        cs.connected = false;
        for sym in &symbols {
            cs.symbols.entry(sym.clone()).or_default();
        }
    }

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
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<String>(crate::CHANNEL_BUFFER);
        let fwd_name = name.clone();
        let forwarder = tokio::spawn(async move {
            let hb_dur = Duration::from_secs(HEARTBEAT_TIMEOUT_S);
            let mut sink = ws_sink;
            let mut stream = ws_stream;
            loop {
                match tokio::time::timeout(hb_dur, stream.next()).await {
                    Err(_) => {
                        warn!(conn = %fwd_name, "heartbeat timeout");
                        break;
                    }
                    Ok(None) => {
                        info!(conn = %fwd_name, "WS stream closed");
                        break;
                    }
                    Ok(Some(Err(e))) => {
                        warn!(conn = %fwd_name, error = %e, "WS error");
                        break;
                    }
                    Ok(Some(Ok(msg))) => match msg {
                        Message::Text(t) => {
                            if fwd_tx.send(t.to_string()).await.is_err() {
                                break;
                            }
                        }
                        Message::Binary(b) => {
                            if let Ok(s) = String::from_utf8(b.into())
                                && fwd_tx.send(s).await.is_err()
                            {
                                break;
                            }
                        }
                        Message::Ping(p) => {
                            let _ = sink.send(Message::Pong(p)).await;
                        }
                        Message::Close(_) => break,
                        _ => {}
                    },
                }
            }
            // Dropping fwd_tx signals the main loop that the stream is done.
        });

        // Fetch REST snapshots for all symbols in parallel.
        // If a symbol's fetch fails, its book has no snapshot — apply_diff
        // will return SnapshotRequired on the first diff, triggering reconnect.
        // Spot symbols that don't bridge sync via re-snapshot in the sync phase.
        // Perp symbols accept any event with pu field (pu chain is self-consistent).
        let mut rate_limited = false;

        // Bounded concurrency: at most 8 parallel snapshot fetches to avoid
        // fanning out unbounded during reconnect storms.
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
                            // HTTP 200 but unexpected body — check for rate limit error
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

        // Binance IP ban detected — skip the event loop entirely and apply
        // extended backoff to avoid a reconnect storm that extends the ban.
        if rate_limited {
            warn!(conn = %name, backoff_s = RATE_LIMIT_BACKOFF_S, "rate limited — extended backoff");
            forwarder.abort();
            let _ = forwarder.await;
            {
                let mut state = lock_state(&monitor);
                if let Some(cs) = state.get_mut(&name) {
                    cs.connected = false;
                    cs.reconnects_today += 1;
                }
            }
            for book in books.values_mut() {
                *book = OrderBook::new();
            }
            accumulators.clear();
            tokio::time::sleep(Duration::from_secs(RATE_LIMIT_BACKOFF_S)).await;
            continue;
        }

        {
            let mut state = lock_state(&monitor);
            if let Some(cs) = state.get_mut(&name) {
                cs.connected = true;
            }
        }

        // ── Sync phase: drain buffered WS events → replay → re-snapshot unsynced ──
        // Let WS events buffer before first drain. Binance sends first event
        // ~100ms after connect; without delay the buffer is empty.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let is_perp = exchange_name == "binance_perp";
        let mut sync_gap_detected = false;
        'sync: for attempt in 0..3u32 {
            // Drain buffered events
            let mut buf: Vec<String> = Vec::new();
            while let Ok(msg) = fwd_rx.try_recv() {
                buf.push(msg);
            }
            debug!(conn = %name, attempt, buf_size = buf.len(), "sync phase drain");
            if buf.is_empty() && attempt > 0 {
                break 'sync;
            }

            // Replay buffer
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
                        // During sync phase, silently skip — will re-snapshot below
                        continue;
                    }
                    Err(AppError::OrderBookGap { .. }) => {
                        // Genuine gap on already-synced book — need full reconnect
                        warn!(conn = %name, symbol = %symbol, "gap detected during sync replay — will reconnect");
                        {
                            let mut state = lock_state(&monitor);
                            if let Some(cs) = state.get_mut(&name)
                                && let Some(ss) = cs.symbols.get_mut(&symbol)
                            {
                                ss.gaps_today += 1;
                            }
                        }
                        sync_gap_detected = true;
                        break 'sync;
                    }
                    Err(_) => continue,
                    Ok(None) => continue,
                    Ok(Some(applied)) => {
                        {
                            let mut state = lock_state(&monitor);
                            if let Some(cs) = state.get_mut(&name)
                                && let Some(ss) = cs.symbols.get_mut(&symbol)
                            {
                                ss.last_event_at = Some(Instant::now());
                            }
                        }

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

            // Check unsynced
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
                // Re-fetch snapshots only for unsynced symbols (bounded concurrency)
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

                // Small delay to let more events buffer
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        {
            let synced_count = books.values().filter(|b| b.synced).count();
            info!(conn = %name, synced = synced_count, total = symbols.len(), "sync phase complete");
        }

        // Gap detected during sync replay — skip to reconnect
        if sync_gap_detected {
            forwarder.abort();
            let _ = forwarder.await;
            {
                let mut state = lock_state(&monitor);
                if let Some(cs) = state.get_mut(&name) {
                    cs.connected = false;
                    cs.reconnects_today += 1;
                }
            }
            for book in books.values_mut() {
                *book = OrderBook::new();
            }
            accumulators.clear();
            sleep_backoff(&mut backoff_ms).await;
            continue;
        }

        // 1s ticker for uniform snapshot sampling, independent of WS event rate.
        // Consume the immediate first tick so the first flush fires ~1s after connect.
        let mut snap_ticker = tokio::time::interval(Duration::from_secs(1));
        snap_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        snap_ticker.tick().await;

        // Periodic stats: event counter + 60s summary
        let mut stats_ticker = tokio::time::interval(Duration::from_secs(60));
        stats_ticker.tick().await; // consume immediate first tick
        let mut event_count: u64 = 0;
        let stats_start = Instant::now();

        'inner: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!(conn = %name, "shutdown signal — exiting event loop");
                    break 'inner;
                }

                msg = fwd_rx.recv() => {
                    let text = match msg {
                        None => break 'inner, // forwarder closed (stream ended, timeout, or error)
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
                            {
                                let mut state = lock_state(&monitor);
                                if let Some(cs) = state.get_mut(&name)
                                    && let Some(ss) = cs.symbols.get_mut(&symbol) {
                                    ss.gaps_today += 1;
                                }
                            }
                            break 'inner;
                        }
                        Err(e) => { warn!(error = %e, "book error"); continue; }
                        Ok(None) => continue,
                        Ok(Some(applied)) => {
                            {
                                let mut state = lock_state(&monitor);
                                if let Some(cs) = state.get_mut(&name)
                                    && let Some(ss) = cs.symbols.get_mut(&symbol) {
                                    ss.last_event_at = Some(Instant::now());
                                }
                            }

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
                            event_count += 1;
                        }
                    }
                }

                _ = snap_ticker.tick() => {
                    // Flush every symbol that has an accumulator — uniform 1Hz sampling
                    // regardless of WS event rate (quiet symbols still emit rows).
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

                _ = stats_ticker.tick() => {
                    let elapsed = stats_start.elapsed().as_secs();
                    let rate = if elapsed > 0 { event_count / elapsed } else { 0 };
                    info!(
                        conn = %name,
                        events = event_count,
                        uptime_s = elapsed,
                        events_per_sec = rate,
                        symbols = symbols.len(),
                        "periodic stats"
                    );
                }
            }
        }

        forwarder.abort();
        let _ = forwarder.await;

        {
            let mut state = lock_state(&monitor);
            if let Some(cs) = state.get_mut(&name) {
                cs.connected = false;
                cs.reconnects_today += 1;
            }
        }

        // Flush any partially-accumulated 1s window before resetting state.
        // Prevents data loss when WS disconnects mid-second (the partial window
        // would otherwise be silently dropped by accumulators.clear() below).
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
        // Clear per-symbol accumulators so stale OFI/open_px/intra_sigma from the
        // previous session don't bleed into the first 1s snapshot after reconnect.
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
