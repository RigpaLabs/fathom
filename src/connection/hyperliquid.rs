use std::{collections::HashMap, path::PathBuf};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Levels, Snapshot1s, WindowAccumulator},
    config::ConnectionConfig,
    exchange::ExchangeAdapter,
    metrics::Metrics,
    monitor::MonitorState,
    orderbook::DiffApplied,
    writer::raw::RawDiff,
};

use super::runtime::{self, BACKOFF_START_MS, DEFAULT_HEARTBEAT_TIMEOUT_S};

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
    let symbols: Vec<String> = conn.symbols.clone();

    let mut accumulators: HashMap<String, WindowAccumulator> = HashMap::new();
    let mut prev_snapshots: HashMap<String, PrevSnapshot> = HashMap::new();
    let mut last_levels: HashMap<String, (Levels, Levels)> = HashMap::new();

    runtime::init_monitor(&monitor, &name, &symbols);

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

        let ws = match runtime::connect_ws(&ws_url, &name, &mut backoff_ms).await {
            Some(ws) => ws,
            None => continue,
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
            runtime::sleep_backoff(&mut backoff_ms).await;
            continue;
        }
        info!(conn = %name, symbols = ?symbols, "subscriptions sent");

        let (fwd_tx, mut fwd_rx) = mpsc::channel::<String>(crate::CHANNEL_BUFFER);
        let forwarder = runtime::spawn_forwarder(
            name.clone(),
            ws_sink,
            ws_stream,
            DEFAULT_HEARTBEAT_TIMEOUT_S,
            fwd_tx,
        );

        runtime::mark_connected(&monitor, &name);

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

                    let hl_msg: HlWsMsg = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
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

                            let bids: Vec<(f64, f64)> = all_bids.iter().take(10).copied().collect();
                            let asks: Vec<(f64, f64)> = all_asks.iter().take(10).copied().collect();

                            let timestamp_us = book.time * 1_000;

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
                            stats.inc();
                            runtime::inc_event_metrics(&metrics, &name, &symbol);

                            runtime::record_event(&monitor, &name, &symbol);
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
                                    _ => continue,
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

                _ = stats.ticker.tick() => {
                    stats.log(&name, symbols.len());
                }
            }
        }

        forwarder.abort();
        let _ = forwarder.await;

        runtime::mark_disconnected(&monitor, &name);

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
        runtime::sleep_backoff(&mut backoff_ms).await;
    }
}

/// Compute churn between two sets of levels: sum of |qty change| at each price.
pub(crate) fn compute_churn(prev: &[(f64, f64)], curr: &[(f64, f64)]) -> f64 {
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
        assert!((compute_churn(&prev, &curr) - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_compute_churn_new_level() {
        let prev = vec![(100.0, 5.0)];
        let curr = vec![(100.0, 5.0), (99.0, 3.0)];
        assert!((compute_churn(&prev, &curr) - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_compute_churn_removed_level() {
        let prev = vec![(100.0, 5.0), (99.0, 3.0)];
        let curr = vec![(100.0, 5.0)];
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
