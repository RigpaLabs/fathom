use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Levels, Snapshot1s, WindowAccumulator},
    config::ConnectionConfig,
    connection::sleep_backoff,
    exchange::ExchangeAdapter,
    metrics::{ConnLabel, Metrics, SymbolLabel},
    monitor::{MonitorState, lock_state},
    orderbook::DiffApplied,
    writer::raw::RawDiff,
};

const BACKOFF_START_MS: u64 = 1_000;
pub const HEARTBEAT_TIMEOUT_S: u64 = 30;

/// Extract top-10 bid/ask levels from full-depth storage.
fn top10(full: &(Levels, Levels)) -> (Levels, Levels) {
    let b10: Levels = full.0.iter().take(10).copied().collect();
    let a10: Levels = full.1.iter().take(10).copied().collect();
    (b10, a10)
}

// ── Hyperliquid WS message types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct HlWsMsg {
    channel: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct HlL2Book {
    coin: String,
    time: i64,
    levels: Vec<Vec<HlLevel>>,
}

#[derive(Debug, Deserialize)]
struct HlLevel {
    px: String,
    sz: String,
    #[allow(dead_code)]
    n: u32,
}

#[derive(Debug, Deserialize)]
struct HlTrade {
    coin: String,
    side: String,
    #[allow(dead_code)]
    px: String,
    sz: String,
    time: i64,
}

/// Previous snapshot state for OFI computation between consecutive full snapshots.
struct PrevSnapshot {
    best_bid_px: f64,
    best_bid_qty: f64,
    best_ask_px: f64,
    best_ask_qty: f64,
}

// ── Connection task ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn connection_task_hl(
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
    // Hyperliquid uses bare coin names (ETH, BTC) — keep as configured
    let symbols: Vec<String> = conn.symbols.clone();

    let mut accumulators: HashMap<String, WindowAccumulator> = HashMap::new();
    let mut prev_snapshots: HashMap<String, PrevSnapshot> = HashMap::new();
    let mut last_levels: HashMap<String, (Levels, Levels)> = HashMap::new();

    {
        let mut state = lock_state(&monitor);
        let cs = state.entry(name.clone()).or_default();
        cs.connected = false;
        for sym in &symbols {
            cs.symbols.entry(sym.clone()).or_default();
        }
    }

    let mut backoff_ms = BACKOFF_START_MS;

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

        let (mut ws_sink, ws_stream) = ws.split();

        // Subscribe to l2Book and trades for each symbol before starting forwarder
        let mut sub_ok = true;
        for sym in &symbols {
            let l2_sub = serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": sym, "nSigFigs": 5}
            });
            if let Err(e) = ws_sink.send(Message::Text(l2_sub.to_string().into())).await {
                warn!(conn = %name, symbol = %sym, error = %e, "l2Book subscribe failed");
                sub_ok = false;
                break;
            }
            let trade_sub = serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": sym}
            });
            if let Err(e) = ws_sink
                .send(Message::Text(trade_sub.to_string().into()))
                .await
            {
                warn!(conn = %name, symbol = %sym, error = %e, "trades subscribe failed");
                sub_ok = false;
                break;
            }
        }
        if !sub_ok {
            sleep_backoff(&mut backoff_ms).await;
            continue;
        }
        info!(conn = %name, symbols = ?symbols, "subscriptions sent");

        // Forwarder task: reads WS frames, answers pings, forwards text to channel
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
        });

        {
            let mut state = lock_state(&monitor);
            if let Some(cs) = state.get_mut(&name) {
                cs.connected = true;
            }
        }

        // 1s ticker for uniform snapshot sampling
        let mut snap_ticker = tokio::time::interval(Duration::from_secs(1));
        snap_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        snap_ticker.tick().await;

        // Periodic stats
        let mut stats_ticker = tokio::time::interval(Duration::from_secs(60));
        stats_ticker.tick().await;
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
                        None => break 'inner,
                        Some(t) => t,
                    };

                    let hl_msg: HlWsMsg = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue, // subscription ack or other non-data message
                    };

                    match hl_msg.channel.as_str() {
                        "l2Book" => {
                            let book: HlL2Book = match serde_json::from_value(hl_msg.data) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };

                            let symbol = book.coin.clone();
                            if !symbols.contains(&symbol) { continue; }
                            if book.levels.len() < 2 { continue; }

                            // Parse all levels for accurate churn/OFI
                            let all_bids: Vec<(f64, f64)> = book.levels[0]
                                .iter()
                                .filter_map(|l| {
                                    let px = l.px.parse::<f64>().ok()?;
                                    let sz = l.sz.parse::<f64>().ok()?;
                                    Some((px, sz))
                                })
                                .collect();

                            let all_asks: Vec<(f64, f64)> = book.levels[1]
                                .iter()
                                .filter_map(|l| {
                                    let px = l.px.parse::<f64>().ok()?;
                                    let sz = l.sz.parse::<f64>().ok()?;
                                    Some((px, sz))
                                })
                                .collect();

                            // Top 10 for raw diff and snapshot output
                            let bids: Vec<(f64, f64)> = all_bids.iter().take(10).copied().collect();
                            let asks: Vec<(f64, f64)> = all_asks.iter().take(10).copied().collect();

                            let timestamp_us = book.time * 1_000;

                            // Write raw snapshot as diff
                            if raw_tx
                                .send(RawDiff {
                                    timestamp_us,
                                    exchange: exchange_name.clone(),
                                    symbol: symbol.clone(),
                                    seq_id: book.time,
                                    prev_seq_id: 0,
                                    bids: bids.clone(),
                                    asks: asks.clone(),
                                })
                                .is_err()
                            {
                                warn!(conn = %name, symbol = %symbol, "raw: no receivers");
                            }

                            // OFI: compare current best bid/ask with previous snapshot
                            let curr_best_bid_px = all_bids.first().map(|(p, _)| *p).unwrap_or(f64::NEG_INFINITY);
                            let curr_best_bid_qty = all_bids.first().map(|(_, q)| *q).unwrap_or(0.0);
                            let curr_best_ask_px = all_asks.first().map(|(p, _)| *p).unwrap_or(f64::INFINITY);
                            let curr_best_ask_qty = all_asks.first().map(|(_, q)| *q).unwrap_or(0.0);

                            let ofi_l1_delta = if let Some(prev) = prev_snapshots.get(&symbol) {
                                let ofi_bid = if curr_best_bid_px >= prev.best_bid_px {
                                    curr_best_bid_qty
                                } else {
                                    -prev.best_bid_qty
                                };
                                let ofi_ask = if curr_best_ask_px <= prev.best_ask_px {
                                    curr_best_ask_qty
                                } else {
                                    -prev.best_ask_qty
                                };
                                ofi_bid - ofi_ask
                            } else {
                                0.0
                            };

                            // Churn: sum of |qty change| at each price level vs previous snapshot (full depth)
                            let (churn_bid, churn_ask) = if let Some((prev_bids, prev_asks)) = last_levels.get(&symbol) {
                                (compute_churn(prev_bids, &all_bids), compute_churn(prev_asks, &all_asks))
                            } else {
                                (0.0, 0.0)
                            };

                            prev_snapshots.insert(symbol.clone(), PrevSnapshot {
                                best_bid_px: curr_best_bid_px,
                                best_bid_qty: curr_best_bid_qty,
                                best_ask_px: curr_best_ask_px,
                                best_ask_qty: curr_best_ask_qty,
                            });

                            // Store full levels for churn, top-10 for snapshot output
                            last_levels.insert(symbol.clone(), (all_bids, all_asks));

                            let acc = accumulators.entry(symbol.clone()).or_insert_with(|| {
                                WindowAccumulator::new(adapter.name(), &symbol, timestamp_us)
                            });

                            let applied = DiffApplied {
                                ofi_l1_delta,
                                bid_abs_change: churn_bid,
                                ask_abs_change: churn_ask,
                            };
                            acc.on_diff_from_levels(
                                bids.first().map(|(p, _)| *p),
                                asks.first().map(|(p, _)| *p),
                                &applied,
                            );
                            event_count += 1;
                            metrics
                                .events_total
                                .get_or_create(&ConnLabel { conn: name.clone() })
                                .inc();
                            metrics
                                .events_by_symbol
                                .get_or_create(&SymbolLabel { conn: name.clone(), symbol: symbol.clone() })
                                .inc();

                            {
                                let mut state = lock_state(&monitor);
                                if let Some(cs) = state.get_mut(&name)
                                    && let Some(ss) = cs.symbols.get_mut(&symbol) {
                                    ss.last_event_at = Some(Instant::now());
                                }
                            }
                        }
                        "trades" => {
                            let trades: Vec<HlTrade> = match serde_json::from_value(hl_msg.data) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            for trade in &trades {
                                if !symbols.contains(&trade.coin) { continue; }
                                let is_buy = match trade.side.as_str() {
                                    "B" => true,
                                    "A" => false,
                                    _ => continue, // skip trades with unknown side
                                };
                                let size = match trade.sz.parse::<f64>() {
                                    Ok(s) => s,
                                    Err(_) => continue,
                                };
                                let ts_us = trade.time * 1_000;
                                let acc = accumulators.entry(trade.coin.clone()).or_insert_with(|| {
                                    WindowAccumulator::new(adapter.name(), &trade.coin, ts_us)
                                });
                                acc.accumulate_trade(size, is_buy);
                            }
                        }
                        _ => {}
                    }
                }

                _ = snap_ticker.tick() => {
                    let ts_us = Utc::now().timestamp_micros();
                    for sym in &symbols {
                        if let Some(acc) = accumulators.get_mut(sym) {
                            let levels = last_levels.get(sym).map(top10);
                            let snap = acc.flush_with_levels(levels.as_ref().map(|(b, a)| (b, a)), ts_us);
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

        // Flush partial accumulators before resetting
        {
            let ts_us = Utc::now().timestamp_micros();
            for sym in &symbols {
                if let Some(acc) = accumulators.get_mut(sym) {
                    let levels = last_levels.get(sym).map(|(b, a)| {
                        let b10: Levels = b.iter().take(10).copied().collect();
                        let a10: Levels = a.iter().take(10).copied().collect();
                        (b10, a10)
                    });
                    let snap = acc.flush_with_levels(levels.as_ref().map(|(b, a)| (b, a)), ts_us);
                    if snap_tx.send(snap).is_err() {
                        warn!(conn = %name, symbol = %sym, "snap: no receivers (disconnect flush)");
                    }
                }
            }
        }

        accumulators.clear();
        prev_snapshots.clear();
        last_levels.clear();
        sleep_backoff(&mut backoff_ms).await;
    }
}

/// Compute churn between two sets of levels: sum of |qty change| at each price.
fn compute_churn(prev: &[(f64, f64)], curr: &[(f64, f64)]) -> f64 {
    let prev_map: HashMap<u64, f64> = prev.iter().map(|(p, q)| (p.to_bits(), *q)).collect();
    let curr_map: HashMap<u64, f64> = curr.iter().map(|(p, q)| (p.to_bits(), *q)).collect();

    let mut churn = 0.0_f64;

    for (px_bits, prev_qty) in &prev_map {
        let curr_qty = curr_map.get(px_bits).copied().unwrap_or(0.0);
        churn += (curr_qty - prev_qty).abs();
    }
    for (px_bits, curr_qty) in &curr_map {
        if !prev_map.contains_key(px_bits) {
            churn += curr_qty.abs();
        }
    }

    churn
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_churn_identical() {
        let levels = vec![(100.0, 5.0), (99.0, 3.0)];
        assert!((compute_churn(&levels, &levels)).abs() < 1e-10);
    }

    #[test]
    fn test_compute_churn_qty_change() {
        let prev = vec![(100.0, 5.0), (99.0, 3.0)];
        let curr = vec![(100.0, 7.0), (99.0, 1.0)];
        // |7-5| + |1-3| = 2 + 2 = 4
        assert!((compute_churn(&prev, &curr) - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_compute_churn_new_level() {
        let prev = vec![(100.0, 5.0)];
        let curr = vec![(100.0, 5.0), (99.0, 3.0)];
        // 99.0 is new: |3.0| = 3.0, 100.0 unchanged: 0
        assert!((compute_churn(&prev, &curr) - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_compute_churn_removed_level() {
        let prev = vec![(100.0, 5.0), (99.0, 3.0)];
        let curr = vec![(100.0, 5.0)];
        // 99.0 removed: |0-3| = 3.0, 100.0 unchanged: 0
        assert!((compute_churn(&prev, &curr) - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_compute_churn_empty() {
        assert!((compute_churn(&[], &[])).abs() < 1e-10);
        assert!((compute_churn(&[], &[(100.0, 5.0)]) - 5.0).abs() < 1e-10);
        assert!((compute_churn(&[(100.0, 5.0)], &[]) - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_hl_trade_deser() {
        let json = serde_json::json!({
            "coin": "ETH",
            "side": "B",
            "px": "2500.0",
            "sz": "1.5",
            "time": 1709654400000_i64
        });
        let trade: HlTrade = serde_json::from_value(json).unwrap();
        assert_eq!(trade.coin, "ETH");
        assert_eq!(trade.side, "B");
        assert_eq!(trade.sz, "1.5");
        assert_eq!(trade.time, 1709654400000);
    }

    #[test]
    fn test_hl_l2book_deser() {
        let json = serde_json::json!({
            "coin": "ETH",
            "time": 1709654400000_i64,
            "levels": [
                [{"px": "2500.0", "sz": "1.0", "n": 1}],
                [{"px": "2501.0", "sz": "2.0", "n": 1}]
            ]
        });
        let book: HlL2Book = serde_json::from_value(json).unwrap();
        assert_eq!(book.coin, "ETH");
        assert_eq!(book.levels.len(), 2);
        assert_eq!(book.levels[0][0].px, "2500.0");
    }
}
