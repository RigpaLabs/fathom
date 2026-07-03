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
    writer::{
        deriv::{DerivEvent, MarkFunding, OpenInterest},
        raw::RawDiff,
        trades::RawTrade,
    },
};

use super::runtime::{self, BACKOFF_START_MS, DEFAULT_HEARTBEAT_TIMEOUT_S};

/// Extract top-10 bid/ask levels from full-depth storage.
fn top10(full: &(Levels, Levels)) -> (Levels, Levels) {
    let b10: Levels = full.0.iter().take(10).copied().collect();
    let a10: Levels = full.1.iter().take(10).copied().collect();
    (b10, a10)
}

/// Parse one side of an l2Book snapshot into (price, size) levels.
/// Levels with unparseable numbers are skipped.
fn parse_levels(levels: &[HlLevel]) -> Levels {
    levels
        .iter()
        .filter_map(|l| {
            let px = l.px.parse::<f64>().ok()?;
            let sz = l.sz.parse::<f64>().ok()?;
            Some((px, sz))
        })
        .collect()
}

/// Build the RawDiff persisted by the raw writer from a parsed l2Book snapshot.
/// Carries the full snapshot depth — the 1s path applies its own top-10 cut.
fn build_raw_diff(
    exchange: &str,
    symbol: &str,
    time_ms: i64,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
) -> RawDiff {
    RawDiff {
        timestamp_us: time_ms * 1_000,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        seq_id: time_ms,
        prev_seq_id: 0,
        bids: bids.to_vec(),
        asks: asks.to_vec(),
    }
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
    px: String,
    sz: String,
    time: i64,
    /// Optional so one tid-less trade can't fail the whole batch decode —
    /// the accumulator path must survive; only tape persistence needs tid.
    #[serde(default)]
    tid: Option<i64>,
}

/// Taker side for the 1s accumulator: `"B"` = taker bought, `"A"` = taker sold.
/// Must stay the exact inverse of `build_raw_trade`'s `is_buyer_maker` — the
/// consistency test below pins the two mappings together.
fn hl_side_is_buy(side: &str) -> Option<bool> {
    match side {
        "B" => Some(true),
        "A" => Some(false),
        _ => None,
    }
}

/// Build a RawTrade from a Hyperliquid trade message.
///
/// HL `side` is the aggressing (taker) side: `"B"` = taker bought → the buyer
/// is the taker → `is_buyer_maker = false`; `"A"` = taker sold → the buyer is
/// the resting maker → `is_buyer_maker = true`. This matches Binance aggTrade
/// `m` semantics (m=true ⇔ taker sold), so the flag is comparable across
/// exchanges. Returns `None` on unknown side or unparseable px/sz.
fn build_raw_trade(exchange: &str, trade: &HlTrade) -> Option<RawTrade> {
    let is_buyer_maker = match trade.side.as_str() {
        "B" => false,
        "A" => true,
        _ => return None,
    };
    let price = trade.px.parse::<f64>().ok()?;
    let qty = trade.sz.parse::<f64>().ok()?;
    Some(RawTrade {
        timestamp_us: trade.time * 1_000,
        exchange: exchange.to_string(),
        symbol: trade.coin.clone(),
        trade_id: trade.tid?,
        price,
        qty,
        is_buyer_maker,
    })
}

// ── activeAssetCtx → derivatives feeds ──────────────────────────────────────

/// `activeAssetCtx` message data: one channel carries funding, oracle px,
/// mark px and open interest for a coin (specs/derivatives-feeds.md).
#[derive(Debug, Deserialize)]
struct HlActiveAssetCtx {
    coin: String,
    ctx: HlAssetCtx,
}

/// The perp asset context. HL sends numerics as strings; kept as
/// `serde_json::Value` so a plain number is accepted too (see `json_f64`).
/// Extra fields (prevDayPx, dayNtlVlm, premium, midPx, impactPxs, …) ignored.
#[derive(Debug, Deserialize)]
struct HlAssetCtx {
    funding: serde_json::Value,
    #[serde(rename = "openInterest")]
    open_interest: serde_json::Value,
    #[serde(rename = "oraclePx")]
    oracle_px: serde_json::Value,
    #[serde(rename = "markPx")]
    mark_px: serde_json::Value,
}

/// Parse a JSON value that is either a numeric string ("3000.05", HL's usual
/// encoding) or a plain number.
fn json_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok())
}

/// Map one activeAssetCtx message to its two derivative rows.
///
/// funding → funding_rate, markPx → mark_px, oraclePx → index_px,
/// openInterest → oi_base. `ts_us` is the receipt time — the message carries
/// no exchange timestamp. `next_funding_ts` is None (HL funds hourly, the feed
/// has no discrete next-funding time). Returns `None` if any numeric fails.
fn asset_ctx_to_events(
    exchange: &str,
    msg: &HlActiveAssetCtx,
    ts_us: i64,
) -> Option<(MarkFunding, OpenInterest)> {
    let mark_funding = MarkFunding {
        timestamp_us: ts_us,
        exchange: exchange.to_string(),
        symbol: msg.coin.clone(),
        mark_px: json_f64(&msg.ctx.mark_px)?,
        index_px: Some(json_f64(&msg.ctx.oracle_px)?),
        funding_rate: json_f64(&msg.ctx.funding)?,
        next_funding_ts: None,
    };
    let open_interest = OpenInterest {
        timestamp_us: ts_us,
        exchange: exchange.to_string(),
        symbol: msg.coin.clone(),
        oi_base: json_f64(&msg.ctx.open_interest)?,
        oi_quote: None,
    };
    Some((mark_funding, open_interest))
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
    trade_tx: broadcast::Sender<RawTrade>,
    deriv_tx: broadcast::Sender<DerivEvent>,
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
            // Derivatives context: funding + oracle/mark px + OI in one channel.
            let ctx_sub = serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": "activeAssetCtx", "coin": sym}
            });
            if let Err(e) = ws_sink
                .send(Message::Text(ctx_sub.to_string().into()))
                .await
            {
                warn!(conn = %name, symbol = %sym, error = %e, "activeAssetCtx subscribe failed");
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

                            let all_bids = parse_levels(&book.levels[0]);
                            let all_asks = parse_levels(&book.levels[1]);

                            let timestamp_us = book.time * 1_000;

                            if raw_tx
                                .send(build_raw_diff(
                                    &exchange_name,
                                    &symbol,
                                    book.time,
                                    &all_bids,
                                    &all_asks,
                                ))
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

                            let best_bid_px = all_bids.first().map(|(p, _)| *p);
                            let best_ask_px = all_asks.first().map(|(p, _)| *p);
                            last_levels.insert(symbol.clone(), (all_bids, all_asks));

                            let acc = accumulators.entry(symbol.clone()).or_insert_with(|| {
                                WindowAccumulator::new(adapter.name(), &symbol, timestamp_us)
                            });

                            let applied = DiffApplied {
                                ofi_l1_delta,
                                bid_abs_change: churn_bid,
                                ask_abs_change: churn_ask,
                            };
                            acc.on_diff_from_levels(best_bid_px, best_ask_px, &applied);
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
                                let is_buy = match hl_side_is_buy(&trade.side) {
                                    Some(b) => b,
                                    None => continue,
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

                                // Persist the tape (px included). Side mapping documented
                                // on build_raw_trade.
                                match build_raw_trade(&exchange_name, trade) {
                                    Some(raw) => {
                                        if trade_tx.send(raw).is_err() {
                                            warn!(conn = %name, symbol = %trade.coin, "trade: no receivers");
                                        }
                                    }
                                    None => {
                                        warn!(conn = %name, symbol = %trade.coin, "trade px parse failed — not persisted");
                                    }
                                }
                            }
                        }
                        "activeAssetCtx" => {
                            let ctx: HlActiveAssetCtx = match serde_json::from_value(hl_msg.data) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            if !symbols.contains(&ctx.coin) { continue; }
                            // Receipt time — the message has no exchange timestamp.
                            let ts_us = Utc::now().timestamp_micros();
                            let Some((mf, oi)) = asset_ctx_to_events(&exchange_name, &ctx, ts_us) else {
                                warn!(conn = %name, symbol = %ctx.coin, "activeAssetCtx numeric parse failed");
                                continue;
                            };
                            // Deriv events do NOT feed record_event — depth
                            // liveness stays meaningful (same decision as trades).
                            if deriv_tx.send(DerivEvent::MarkFunding(mf)).is_err()
                                || deriv_tx.send(DerivEvent::OpenInterest(oi)).is_err()
                            {
                                warn!(conn = %name, symbol = %ctx.coin, "deriv: no receivers");
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

    /// Real-shaped activeAssetCtx message data (perps variant). HL sends
    /// numerics as strings; extra fields must be ignored.
    fn active_asset_ctx_json() -> serde_json::Value {
        serde_json::json!({
            "coin": "ETH",
            "ctx": {
                "funding": "0.0000125",
                "openInterest": "1234567.89",
                "prevDayPx": "2900.0",
                "dayNtlVlm": "111222333.0",
                "premium": "0.00001",
                "oraclePx": "3000.05",
                "markPx": "3000.12",
                "midPx": "3000.10",
                "impactPxs": ["3000.08", "3000.14"],
                "dayBaseVlm": "37000.5"
            }
        })
    }

    /// One activeAssetCtx message carries funding + mark + oracle + OI →
    /// emits both a MarkFunding and an OpenInterest row.
    #[test]
    fn test_active_asset_ctx_to_mark_funding_and_open_interest() {
        let msg: HlActiveAssetCtx = serde_json::from_value(active_asset_ctx_json()).unwrap();
        assert_eq!(msg.coin, "ETH");

        let ts_us = 1_709_654_400_000_000_i64;
        let (mf, oi) = asset_ctx_to_events("hyperliquid", &msg, ts_us).unwrap();

        assert_eq!(
            mf.timestamp_us, ts_us,
            "receipt time — HL sends no event ts"
        );
        assert_eq!(mf.exchange, "hyperliquid");
        assert_eq!(mf.symbol, "ETH");
        assert_eq!(mf.mark_px, 3000.12, "markPx → mark_px");
        assert_eq!(mf.index_px, Some(3000.05), "oraclePx → index_px");
        assert_eq!(mf.funding_rate, 0.0000125, "funding → funding_rate");
        assert_eq!(
            mf.next_funding_ts, None,
            "HL funds hourly — no discrete next-funding ts in the feed"
        );

        assert_eq!(oi.timestamp_us, ts_us);
        assert_eq!(oi.exchange, "hyperliquid");
        assert_eq!(oi.symbol, "ETH");
        assert_eq!(oi.oi_base, 1_234_567.89, "openInterest → oi_base");
        assert_eq!(oi.oi_quote, None);
    }

    /// Defensive: accept plain JSON numbers too, not only HL's usual strings.
    #[test]
    fn test_active_asset_ctx_numeric_fields_as_numbers() {
        let json = serde_json::json!({
            "coin": "BTC",
            "ctx": {
                "funding": 0.0000125,
                "openInterest": 100.5,
                "oraclePx": 60000.0,
                "markPx": 60001.5
            }
        });
        let msg: HlActiveAssetCtx = serde_json::from_value(json).unwrap();
        let (mf, oi) = asset_ctx_to_events("hyperliquid", &msg, 1).unwrap();
        assert_eq!(mf.mark_px, 60001.5);
        assert_eq!(mf.index_px, Some(60000.0));
        assert_eq!(oi.oi_base, 100.5);
    }

    #[test]
    fn test_active_asset_ctx_unparseable_returns_none() {
        let json = serde_json::json!({
            "coin": "ETH",
            "ctx": {
                "funding": "oops",
                "openInterest": "100.0",
                "oraclePx": "3000.0",
                "markPx": "3000.0"
            }
        });
        let msg: HlActiveAssetCtx = serde_json::from_value(json).unwrap();
        assert!(asset_ctx_to_events("hyperliquid", &msg, 1).is_none());
    }

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
            "time": 1709654400000_i64,
            "tid": 118906668,
            "hash": "0xabc",
            "users": ["0x1", "0x2"]
        });
        let trade: HlTrade = serde_json::from_value(json).unwrap();
        assert_eq!(trade.coin, "ETH");
        assert_eq!(trade.side, "B");
        assert_eq!(trade.px, "2500.0");
        assert_eq!(trade.sz, "1.5");
        assert_eq!(trade.time, 1709654400000);
        assert_eq!(trade.tid, Some(118906668));
    }

    /// A tid-less trade must still deserialize (tid is Option so one such
    /// trade can't fail the whole batch decode and starve the accumulator);
    /// only tape persistence is skipped.
    #[test]
    fn test_hl_trade_missing_tid_decodes_but_skips_tape() {
        let json = serde_json::json!({
            "coin": "ETH", "side": "B", "px": "2500.0", "sz": "1.5",
            "time": 1709654400000_i64
        });
        let trade: HlTrade = serde_json::from_value(json).unwrap();
        assert_eq!(trade.tid, None);
        assert!(build_raw_trade("hyperliquid", &trade).is_none());
        // accumulator path doesn't touch tid — side mapping still resolves
        assert_eq!(hl_side_is_buy(&trade.side), Some(true));
    }

    /// Pins the accumulator's side mapping AND its consistency with the tape:
    /// is_buy must equal !is_buyer_maker for every valid side.
    #[test]
    fn test_hl_accumulator_side_mapping_consistent_with_tape() {
        for side in ["B", "A"] {
            let is_buy = hl_side_is_buy(side).unwrap();
            let raw = build_raw_trade("hyperliquid", &hl_trade(side)).unwrap();
            assert_eq!(
                is_buy, !raw.is_buyer_maker,
                "accumulator and tape side mappings drifted for side {side}"
            );
        }
        assert_eq!(
            hl_side_is_buy("B"),
            Some(true),
            "B = taker bought → buy_vol"
        );
        assert_eq!(
            hl_side_is_buy("A"),
            Some(false),
            "A = taker sold → sell_vol"
        );
        assert_eq!(hl_side_is_buy("X"), None);
    }

    fn hl_trade(side: &str) -> HlTrade {
        HlTrade {
            coin: "ETH".into(),
            side: side.into(),
            px: "2500.5".into(),
            sz: "1.5".into(),
            time: 1_709_654_400_000,
            tid: Some(42),
        }
    }

    /// HL side "B" = taker bought → buyer is taker → is_buyer_maker=false
    /// (same semantics as Binance m=false).
    #[test]
    fn test_hl_trade_to_raw_trade_taker_buy() {
        let raw = build_raw_trade("hyperliquid", &hl_trade("B")).unwrap();
        assert_eq!(raw.timestamp_us, 1_709_654_400_000_000);
        assert_eq!(raw.exchange, "hyperliquid");
        assert_eq!(raw.symbol, "ETH");
        assert_eq!(raw.trade_id, 42);
        assert_eq!(raw.price, 2500.5);
        assert_eq!(raw.qty, 1.5);
        assert!(!raw.is_buyer_maker, "taker buy → buyer is NOT the maker");
    }

    /// HL side "A" = taker sold → buyer is maker → is_buyer_maker=true
    /// (same semantics as Binance m=true).
    #[test]
    fn test_hl_trade_to_raw_trade_taker_sell() {
        let raw = build_raw_trade("hyperliquid", &hl_trade("A")).unwrap();
        assert!(raw.is_buyer_maker, "taker sell → buyer IS the maker");
    }

    #[test]
    fn test_hl_trade_to_raw_trade_unknown_side_or_bad_number() {
        assert!(build_raw_trade("hyperliquid", &hl_trade("X")).is_none());
        let mut bad_px = hl_trade("B");
        bad_px.px = "oops".into();
        assert!(build_raw_trade("hyperliquid", &bad_px).is_none());
    }

    /// Build an l2Book message with `depth` levels per side.
    fn l2book_with_depth(depth: usize) -> HlL2Book {
        let bids: Vec<serde_json::Value> = (0..depth)
            .map(|i| serde_json::json!({"px": format!("{}", 2500 - i as i64), "sz": "1.0", "n": 1}))
            .collect();
        let asks: Vec<serde_json::Value> = (0..depth)
            .map(|i| serde_json::json!({"px": format!("{}", 2501 + i as i64), "sz": "2.0", "n": 1}))
            .collect();
        let json = serde_json::json!({
            "coin": "ETH",
            "time": 1709654400000_i64,
            "levels": [bids, asks]
        });
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn test_raw_diff_carries_full_snapshot_depth() {
        let book = l2book_with_depth(25);
        let all_bids = parse_levels(&book.levels[0]);
        let all_asks = parse_levels(&book.levels[1]);
        assert_eq!(all_bids.len(), 25);
        assert_eq!(all_asks.len(), 25);

        let diff = build_raw_diff("hyperliquid", &book.coin, book.time, &all_bids, &all_asks);

        assert_eq!(diff.bids.len(), 25, "raw path must persist full depth");
        assert_eq!(diff.asks.len(), 25, "raw path must persist full depth");
        assert_eq!(diff.bids[0], (2500.0, 1.0));
        assert_eq!(diff.bids[24], (2476.0, 1.0));
        assert_eq!(diff.asks[24], (2525.0, 2.0));
        assert_eq!(diff.timestamp_us, 1709654400000_i64 * 1_000);
        assert_eq!(diff.seq_id, 1709654400000_i64);
        assert_eq!(diff.exchange, "hyperliquid");
        assert_eq!(diff.symbol, "ETH");
    }

    #[test]
    fn test_1s_path_stays_top10() {
        let book = l2book_with_depth(25);
        let full = (parse_levels(&book.levels[0]), parse_levels(&book.levels[1]));
        let (b10, a10) = top10(&full);
        assert_eq!(b10.len(), 10, "1s snapshot path must stay top-10");
        assert_eq!(a10.len(), 10, "1s snapshot path must stay top-10");
        assert_eq!(b10[9], (2491.0, 1.0));
        assert_eq!(a10[9], (2510.0, 2.0));
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
