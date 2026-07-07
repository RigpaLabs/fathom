//! Bybit v5 connection task (`specs/bybit-collection.md`).
//!
//! One task instance handles one category (`bybit_spot` = `spot`, `bybit_perp`
//! = `linear`) over a single WS connection covering all its configured
//! symbols — same "one connection, N symbols, one tokio task" shape as every
//! other connection task in this file.
//!
//! Architecturally closest to Hyperliquid's single-WS-loop pattern (no REST
//! snapshot call, no buffer-replay sync phase — Bybit pushes the initial book
//! as a `type: "snapshot"` WS message right after subscribing), but with real
//! gap detection unlike HL: Bybit deltas carry a monotonically increasing `u`
//! (update id) that the client must itself verify is contiguous. See
//! [`check_orderbook_gap`] for the pure comparison function and the spec's
//! "Gap detection" section for the documented semantics.

use std::{collections::HashMap, path::PathBuf, time::Duration};

use chrono::Utc;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Snapshot1s, WindowAccumulator},
    config::ConnectionConfig,
    exchange::ExchangeAdapter,
    metrics::Metrics,
    monitor::MonitorState,
    orderbook::{DepthDiff, OrderBook, SnapshotMsg},
    writer::{
        deriv::{DerivEvent, Liquidation},
        raw::RawDiff,
        trades::RawTrade,
    },
};

use super::binance::parse_level;
use super::bybit_ticker;
use super::runtime::{self, BACKOFF_START_MS, DEFAULT_HEARTBEAT_TIMEOUT_S};

/// Bybit's documented heartbeat interval (spec's "Channels / topics" section:
/// "send `{"op": "ping"}` about every 20s").
const PING_INTERVAL_S: u64 = 20;

/// Spot's documented per-`subscribe`-request arg cap (spec: "spot allows at
/// most 10 topics per subscribe request"). Linear's cap is far higher — 6
/// symbols × 4 topics = 24 args fits comfortably in one message, so linear
/// uses `topics.len()` as its effective batch size (single message).
const SPOT_SUBSCRIBE_ARG_CAP: usize = 10;

/// Orderbook topic prefix — depth level hardcoded per spec's recommendation
/// (deepest available, matching the "maximum data" philosophy); `depth_ms`
/// config is ignored by this adapter, same documented precedent as
/// Hyperliquid.
const ORDERBOOK_TOPIC_PREFIX: &str = "orderbook.";

// ── Bybit WS message types ──────────────────────────────────────────────────

/// Generic envelope shared by all four topics: `topic` selects the dispatch
/// branch, `data` is parsed into the topic-specific shape downstream (object
/// for orderbook/tickers, array for publicTrade/allLiquidation — same
/// two-phase parse Binance's `WsCombined`/`combined.data` already uses).
/// `type` is absent from `allLiquidation` messages (defaults to empty string,
/// unused by that branch) and is a constant/meaningless `"snapshot"` on
/// `publicTrade` (also unused). `cts` (matching-engine timestamp) is present
/// only on `orderbook` messages; other topics fall back to `ts`.
#[derive(Debug, Deserialize)]
struct BybitEnvelope {
    topic: String,
    #[serde(rename = "type", default)]
    msg_type: String,
    #[serde(default)]
    ts: i64,
    #[serde(default)]
    cts: Option<i64>,
    data: serde_json::Value,
}

/// `orderbook.1000.{symbol}` `data` object (spec's "Message schemas" section).
#[derive(Debug, Deserialize)]
struct BybitOrderBookData {
    /// Bids: [price, size] strings, descending.
    b: Vec<[serde_json::Value; 2]>,
    /// Asks: [price, size] strings, ascending.
    a: Vec<[serde_json::Value; 2]>,
    /// Per-symbol monotonically increasing update id.
    u: i64,
    // `seq` (cross-sequence, comparing different depth levels of the same
    // symbol) is not used — fathom only ever subscribes to one depth level
    // per symbol (spec: "note it for completeness, don't build logic around
    // it").
}

#[allow(clippy::type_complexity)]
fn parse_ob_levels(data: &BybitOrderBookData) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
    let bids = data.b.iter().filter_map(parse_level).collect();
    let asks = data.a.iter().filter_map(parse_level).collect();
    (bids, asks)
}

/// One `publicTrade.{symbol}` `data` array element.
#[derive(Debug, Deserialize)]
struct BybitTradeItem {
    /// Execution time, ms.
    #[serde(rename = "T")]
    time_ms: i64,
    /// Taker side: `"Buy"` or `"Sell"`.
    #[serde(rename = "S")]
    side: String,
    /// Quantity, string.
    v: String,
    /// Price, string.
    p: String,
    /// Trade id, string (Bybit encodes as a numeric string large enough to
    /// need i64, not u32).
    i: String,
}

/// Taker side for Bybit's `publicTrade`/`allLiquidation` `S` field: `"Buy"` =
/// taker bought, `"Sell"` = taker sold. Bybit gives the side directly — no
/// maker/taker inversion needed, unlike Binance's `m` flag (spec: "S maps
/// directly to fathom's existing is_buy-style taker-side attribution").
fn bybit_side_is_buy(side: &str) -> Option<bool> {
    match side {
        "Buy" => Some(true),
        "Sell" => Some(false),
        _ => None,
    }
}

/// Build a RawTrade from one publicTrade element. Returns `None` if the side
/// is unrecognized or any numeric field fails to parse.
fn build_raw_trade(exchange: &str, symbol: &str, item: &BybitTradeItem) -> Option<RawTrade> {
    let is_buy = bybit_side_is_buy(&item.side)?;
    let price = item.p.parse::<f64>().ok()?;
    let qty = item.v.parse::<f64>().ok()?;
    let trade_id = item.i.parse::<i64>().ok()?;
    Some(RawTrade {
        timestamp_us: item.time_ms * 1_000,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        trade_id,
        price,
        qty,
        // is_buyer_maker = true means the taker sold (RawTrade's documented
        // semantics) — the exact inverse of is_buy, same convention pinned by
        // hyperliquid.rs's build_raw_trade/hl_side_is_buy consistency test.
        is_buyer_maker: !is_buy,
    })
}

/// One `allLiquidation.{symbol}` `data` array element.
#[derive(Debug, Deserialize)]
struct BybitLiquidationItem {
    #[serde(rename = "T")]
    time_ms: i64,
    #[serde(rename = "S")]
    side: String,
    v: String,
    p: String,
}

/// Build a Liquidation from one allLiquidation element. Returns `None` on
/// unparseable numbers. `side` is persisted as sent by the venue (`"Buy"` /
/// `"Sell"`) — same "no forced case normalization" treatment as Binance's
/// `force_order_to_liquidation`, which also persists the venue's own casing.
fn build_liquidation(
    exchange: &str,
    symbol: &str,
    item: &BybitLiquidationItem,
) -> Option<Liquidation> {
    let price = item.p.parse::<f64>().ok()?;
    let qty = item.v.parse::<f64>().ok()?;
    Some(Liquidation {
        timestamp_us: item.time_ms * 1_000,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        side: item.side.clone(),
        price,
        qty,
    })
}

fn build_raw_diff(
    exchange: &str,
    symbol: &str,
    timestamp_us: i64,
    seq_id: i64,
    prev_seq_id: i64,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
) -> RawDiff {
    RawDiff {
        timestamp_us,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        seq_id,
        prev_seq_id,
        bids,
        asks,
    }
}

// ── Gap detection (pure, unit-testable — see spec's "Gap detection") ───────

/// Result of comparing a Bybit orderbook delta's `u` against the last-applied
/// `u` for that symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GapCheck {
    /// Contiguous — safe to apply.
    Ok,
    /// A genuine client-detected gap: the caller must reconnect + resubscribe
    /// (spec: Bybit does not promise an unsolicited snapshot for a loss the
    /// client detects on its own).
    Gap { expected: i64, got: i64 },
}

/// Compare the last-applied Bybit orderbook `u` to a new delta's `u`.
///
/// Contiguous exactly when `new_u == prev_u + 1`. Bybit's documented
/// restart-sequence marker (`u == 1` mid-stream, which per the docs is always
/// paired with `type: "snapshot"` on the wire) is also treated as `Ok` here —
/// defense in depth per the spec ("checking `type` is the primary signal,
/// `u`-sequence checking is the secondary check"): the caller dispatches
/// `type: "snapshot"` messages to `apply_snapshot` before this function is
/// ever consulted for a `"delta"`, so in practice this function should never
/// see `new_u == 1`; the branch exists so that *if* it somehow did (the wire
/// contradicting the documented pairing), it could never be misread as a
/// "genuine gap" requiring an unwarranted reconnect.
///
/// Any other discontinuity is a gap — including a `new_u` smaller than
/// `prev_u` by more than the `u == 1` marker. Bybit's docs do not describe
/// `u` as a fixed-width counter that wraps; a backward jump this large is
/// treated the same as a forward jump: not tolerated, reconnect required.
/// (Flagged as a spec assumption needing live verification — see PR body.)
pub(crate) fn check_orderbook_gap(prev_u: i64, new_u: i64) -> GapCheck {
    if new_u == 1 || new_u == prev_u + 1 {
        GapCheck::Ok
    } else {
        GapCheck::Gap {
            expected: prev_u + 1,
            got: new_u,
        }
    }
}

// ── Subscribe message construction ──────────────────────────────────────────

/// Build the full topic list for one category. `orderbook`/`publicTrade` for
/// every symbol; `linear` additionally gets `tickers`/`allLiquidation`.
fn build_topics(symbols: &[String], linear: bool) -> Vec<String> {
    let mut topics: Vec<String> = Vec::with_capacity(symbols.len() * if linear { 4 } else { 2 });
    for s in symbols {
        topics.push(format!("orderbook.1000.{s}"));
        topics.push(format!("publicTrade.{s}"));
    }
    if linear {
        for s in symbols {
            topics.push(format!("tickers.{s}"));
            topics.push(format!("allLiquidation.{s}"));
        }
    }
    topics
}

/// Split `topics` into `subscribe` message batches of at most `max_per_batch`
/// args each.
fn subscribe_batches(topics: &[String], max_per_batch: usize) -> Vec<Vec<String>> {
    if max_per_batch == 0 {
        return vec![topics.to_vec()];
    }
    topics
        .chunks(max_per_batch)
        .map(<[String]>::to_vec)
        .collect()
}

// ── Connection task ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn connection_task_bybit(
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
    // Linear (bybit_perp) gets tickers + allLiquidation on top of orderbook +
    // publicTrade; spot (bybit_spot) gets only the latter two (spec's
    // "Channels / topics" table).
    let linear = exchange_name == "bybit_perp";
    let symbols: Vec<String> = conn.symbols.clone();

    let mut books: HashMap<String, OrderBook> = symbols
        .iter()
        .map(|s| (s.clone(), OrderBook::new()))
        .collect();
    let mut accumulators: HashMap<String, WindowAccumulator> = HashMap::new();
    let mut ticker_states: HashMap<String, bybit_ticker::BybitTickerState> = HashMap::new();

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

        let (ws_sink, ws_stream) = ws.split();

        // Bidirectional forwarder: Bybit needs periodic client-initiated
        // `{"op":"ping"}` frames (spec: ~20s), not just server-Ping/Pong
        // auto-answering — mirrors dydx.rs's bidi-forwarder shape.
        let (send_tx, send_rx) = mpsc::channel::<String>(64);
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<String>(crate::CHANNEL_BUFFER);
        let forwarder = runtime::spawn_bidi_forwarder(
            name.clone(),
            ws_sink,
            ws_stream,
            DEFAULT_HEARTBEAT_TIMEOUT_S,
            fwd_tx,
            send_rx,
        );

        // Subscribe: batched per spot's 10-arg cap; one message for linear.
        // Re-issued on every reconnect (this whole block re-runs inside the
        // outer loop), not only on initial connect.
        let topics = build_topics(&symbols, linear);
        let batch_cap = if linear {
            topics.len().max(1)
        } else {
            SPOT_SUBSCRIBE_ARG_CAP
        };
        let mut sub_ok = true;
        for batch in subscribe_batches(&topics, batch_cap) {
            let sub_msg = serde_json::json!({"op": "subscribe", "args": batch}).to_string();
            if send_tx.send(sub_msg).await.is_err() {
                warn!(conn = %name, "subscribe send failed");
                sub_ok = false;
                break;
            }
        }
        if !sub_ok {
            forwarder.abort();
            let _ = forwarder.await;
            runtime::sleep_backoff(&mut backoff_ms).await;
            continue;
        }
        info!(conn = %name, symbols = ?symbols, linear, "subscriptions sent");

        runtime::mark_connected(&monitor, &name);

        let mut snap_ticker = runtime::snap_ticker();
        snap_ticker.tick().await;

        let mut ping_ticker = tokio::time::interval(Duration::from_secs(PING_INTERVAL_S));
        ping_ticker.tick().await; // skip immediate first tick — just subscribed

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

                    let envelope: BybitEnvelope = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let Some(symbol) = envelope.topic.rsplit('.').next() else { continue };
                    if !symbols.iter().any(|s| s == symbol) { continue; }

                    if envelope.topic.starts_with(ORDERBOOK_TOPIC_PREFIX) {
                        let data: BybitOrderBookData = match serde_json::from_value(envelope.data.clone()) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let (bids, asks) = parse_ob_levels(&data);
                        let timestamp_us = envelope.cts.unwrap_or(envelope.ts) * 1_000;

                        let book = books.entry(symbol.to_string()).or_default();

                        match envelope.msg_type.as_str() {
                            "snapshot" => {
                                book.apply_snapshot(SnapshotMsg {
                                    symbol: symbol.to_string(),
                                    last_update_id: data.u,
                                    bids: bids.clone(),
                                    asks: asks.clone(),
                                });
                                runtime::record_event(&monitor, &name, symbol);
                                if raw_tx
                                    .send(build_raw_diff(&exchange_name, symbol, timestamp_us, data.u, 0, bids, asks))
                                    .is_err()
                                {
                                    warn!(conn = %name, symbol = %symbol, "raw: no receivers");
                                }
                            }
                            "delta" => {
                                if !book.has_snapshot {
                                    // No base state yet (pre-first-snapshot or
                                    // post-reconnect before a fresh snapshot
                                    // arrives) — drop, do not reconnect.
                                    continue;
                                }
                                let prev_u = book.last_update_id;
                                match check_orderbook_gap(prev_u, data.u) {
                                    GapCheck::Ok => {
                                        let diff = DepthDiff {
                                            exchange: exchange_name.clone(),
                                            symbol: symbol.to_string(),
                                            timestamp_us,
                                            seq_id: data.u,
                                            // Bybit deltas carry only `u` (no
                                            // separate first/final id like
                                            // Binance's U/u pair) — using `u`
                                            // for both reduces the shared
                                            // OrderBook's generic spot-rule
                                            // gap check to the same `u ==
                                            // last_update_id + 1` condition
                                            // already verified above, so the
                                            // two checks stay consistent.
                                            prev_seq_id: data.u,
                                            prev_final_update_id: None,
                                            bids: bids.clone(),
                                            asks: asks.clone(),
                                        };
                                        match book.apply_diff(&diff) {
                                            Ok(Some(applied)) => {
                                                runtime::record_event(&monitor, &name, symbol);
                                                if raw_tx
                                                    .send(build_raw_diff(&exchange_name, symbol, timestamp_us, data.u, prev_u, bids, asks))
                                                    .is_err()
                                                {
                                                    warn!(conn = %name, symbol = %symbol, "raw: no receivers");
                                                }
                                                let acc = accumulators.entry(symbol.to_string()).or_insert_with(|| {
                                                    WindowAccumulator::new(&exchange_name, symbol, timestamp_us)
                                                });
                                                acc.on_diff(book, &applied);
                                                stats.inc();
                                                runtime::inc_event_metrics(&metrics, &name, symbol);
                                            }
                                            Ok(None) => {
                                                // Stale/duplicate — dropped, not an error.
                                            }
                                            Err(e) => {
                                                warn!(conn = %name, symbol = %symbol, error = %e, "unexpected book error — reconnecting");
                                                runtime::record_gap(&monitor, &name, symbol);
                                                break 'inner;
                                            }
                                        }
                                    }
                                    GapCheck::Gap { expected, got } => {
                                        warn!(conn = %name, symbol = %symbol, expected, got, "orderbook gap detected — reconnecting");
                                        runtime::record_gap(&monitor, &name, symbol);
                                        break 'inner;
                                    }
                                }
                            }
                            _ => {}
                        }
                        continue;
                    }

                    if envelope.topic.starts_with("publicTrade.") {
                        let items: Vec<BybitTradeItem> = match serde_json::from_value(envelope.data.clone()) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        for item in &items {
                            match build_raw_trade(&exchange_name, symbol, item) {
                                Some(raw) => {
                                    let is_buy = !raw.is_buyer_maker;
                                    let qty = raw.qty;
                                    let ts_us = raw.timestamp_us;
                                    let acc = accumulators.entry(symbol.to_string()).or_insert_with(|| {
                                        WindowAccumulator::new(&exchange_name, symbol, ts_us)
                                    });
                                    acc.accumulate_trade(qty, is_buy);
                                    if trade_tx.send(raw).is_err() {
                                        warn!(conn = %name, symbol = %symbol, "trade: no receivers");
                                    }
                                }
                                None => {
                                    warn!(conn = %name, symbol = %symbol, "publicTrade parse failed — not persisted");
                                }
                            }
                        }
                        continue;
                    }

                    if linear && envelope.topic.starts_with("tickers.") {
                        let fields: bybit_ticker::BybitTickerFields =
                            match serde_json::from_value(envelope.data.clone()) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                        let ticker_msg = bybit_ticker::BybitTickerMsg {
                            msg_type: envelope.msg_type.clone(),
                            data: fields,
                        };
                        let state = ticker_states.entry(symbol.to_string()).or_default();
                        let change = bybit_ticker::merge_ticker_message(state, &ticker_msg);
                        if !change.is_empty() {
                            let ts_us = envelope.ts * 1_000;
                            let (mf, oi) = bybit_ticker::build_deriv_events(
                                &exchange_name,
                                symbol,
                                ts_us,
                                state,
                                change,
                            );
                            // Deriv events do NOT feed record_event — depth
                            // liveness stays meaningful (same decision as
                            // Binance/HL's trade and mark-price handling).
                            if let Some(mf) = mf
                                && deriv_tx.send(DerivEvent::MarkFunding(mf)).is_err()
                            {
                                warn!(conn = %name, symbol = %symbol, "deriv: no receivers (mark/funding)");
                            }
                            if let Some(oi) = oi
                                && deriv_tx.send(DerivEvent::OpenInterest(oi)).is_err()
                            {
                                warn!(conn = %name, symbol = %symbol, "deriv: no receivers (OI)");
                            }
                        }
                        continue;
                    }

                    if linear && envelope.topic.starts_with("allLiquidation.") {
                        let items: Vec<BybitLiquidationItem> = match serde_json::from_value(envelope.data.clone()) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        for item in &items {
                            match build_liquidation(&exchange_name, symbol, item) {
                                Some(liq) => {
                                    if deriv_tx.send(DerivEvent::Liquidation(liq)).is_err() {
                                        warn!(conn = %name, symbol = %symbol, "deriv: no receivers (liq)");
                                    }
                                }
                                None => {
                                    warn!(conn = %name, symbol = %symbol, "allLiquidation parse failed");
                                }
                            }
                        }
                        continue;
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

                _ = ping_ticker.tick() => {
                    let ping = serde_json::json!({"op": "ping"}).to_string();
                    if send_tx.send(ping).await.is_err() {
                        warn!(conn = %name, "ping send failed — exiting event loop");
                        break 'inner;
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

        // Reset ALL per-symbol state — order books AND ticker-merge state —
        // so a stale pre-reconnect book/ticker can never be merged against a
        // fresh post-reconnect stream (spec's reconnect requirements).
        for book in books.values_mut() {
            *book = OrderBook::new();
        }
        accumulators.clear();
        ticker_states.clear();
        runtime::sleep_backoff(&mut backoff_ms).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // ── Gap detection ────────────────────────────────────────────────────

    #[test]
    fn test_gap_check_contiguous() {
        assert_eq!(check_orderbook_gap(100, 101), GapCheck::Ok);
    }

    #[test]
    fn test_gap_check_genuine_forward_gap() {
        assert_eq!(
            check_orderbook_gap(100, 105),
            GapCheck::Gap {
                expected: 101,
                got: 105
            }
        );
    }

    /// Bybit's documented restart marker: `u == 1` mid-stream is not a gap
    /// (defense-in-depth secondary check — `type == "snapshot"` is the
    /// primary signal, handled by the caller's dispatch before this function
    /// is ever consulted for a "delta").
    #[test]
    fn test_gap_check_u_equals_one_restart_marker() {
        assert_eq!(check_orderbook_gap(999_999_999, 1), GapCheck::Ok);
    }

    /// No documented wraparound for Bybit's `u` (unlike a fixed-width
    /// counter) — a backward jump other than the literal `u == 1` marker is
    /// still treated as a gap, not silently tolerated.
    #[test]
    fn test_gap_check_backward_jump_not_one_is_still_a_gap() {
        assert_eq!(
            check_orderbook_gap(100, 50),
            GapCheck::Gap {
                expected: 101,
                got: 50
            }
        );
    }

    /// An exact repeat (server resent the same `u`) is also a gap under
    /// Bybit's stricter model — no Binance-style "stale, drop silently"
    /// tolerance, since Bybit deltas carry no `pu` field to distinguish
    /// "already applied" from "out of order".
    #[test]
    fn test_gap_check_exact_repeat_is_a_gap() {
        assert_eq!(
            check_orderbook_gap(100, 100),
            GapCheck::Gap {
                expected: 101,
                got: 100
            }
        );
    }

    // ── Subscribe batching ───────────────────────────────────────────────

    fn syms(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("SYM{i}")).collect()
    }

    #[test]
    fn test_build_topics_spot_two_per_symbol() {
        let topics = build_topics(&syms(6), false);
        assert_eq!(topics.len(), 12, "spot: orderbook + publicTrade only");
        assert!(topics.contains(&"orderbook.1000.SYM0".to_string()));
        assert!(topics.contains(&"publicTrade.SYM0".to_string()));
        assert!(!topics.iter().any(|t| t.starts_with("tickers.")));
        assert!(!topics.iter().any(|t| t.starts_with("allLiquidation.")));
    }

    #[test]
    fn test_build_topics_linear_four_per_symbol() {
        let topics = build_topics(&syms(6), true);
        assert_eq!(topics.len(), 24, "linear: all four topics");
        assert!(topics.contains(&"tickers.SYM0".to_string()));
        assert!(topics.contains(&"allLiquidation.SYM0".to_string()));
    }

    #[test]
    fn test_subscribe_batches_spot_splits_over_cap() {
        let topics = build_topics(&syms(6), false); // 12 topics
        let batches = subscribe_batches(&topics, SPOT_SUBSCRIBE_ARG_CAP);
        assert_eq!(batches.len(), 2, "12 topics / 10-arg cap = 2 batches");
        assert_eq!(batches[0].len(), 10);
        assert_eq!(batches[1].len(), 2);
        let total: usize = batches.iter().map(Vec::len).sum();
        assert_eq!(total, 12, "no topic lost across batches");
    }

    #[test]
    fn test_subscribe_batches_linear_single_message() {
        let topics = build_topics(&syms(6), true); // 24 topics
        let batches = subscribe_batches(&topics, topics.len().max(1));
        assert_eq!(batches.len(), 1, "linear fits one subscribe message");
        assert_eq!(batches[0].len(), 24);
    }

    // ── Orderbook data parsing ───────────────────────────────────────────

    fn ob_data_json() -> serde_json::Value {
        serde_json::json!({
            "s": "BTCUSDT",
            "b": [["43000.5", "1.234"], ["42999.0", "0.5"]],
            "a": [["43001.0", "0.567"]],
            "u": 123456,
            "seq": 9876543210_i64
        })
    }

    #[test]
    fn test_orderbook_data_parse_and_levels() {
        let data: BybitOrderBookData = serde_json::from_value(ob_data_json()).unwrap();
        assert_eq!(data.u, 123456);
        let (bids, asks) = parse_ob_levels(&data);
        assert_eq!(bids, vec![(43000.5, 1.234), (42999.0, 0.5)]);
        assert_eq!(asks, vec![(43001.0, 0.567)]);
    }

    // ── Trade parsing ────────────────────────────────────────────────────

    fn trade_item(side: &str) -> BybitTradeItem {
        BybitTradeItem {
            time_ms: 1_700_000_000_000,
            side: side.to_string(),
            v: "0.012".to_string(),
            p: "43000.5".to_string(),
            i: "2100000000012345".to_string(),
        }
    }

    #[test]
    fn test_build_raw_trade_buy() {
        let raw = build_raw_trade("bybit_spot", "BTCUSDT", &trade_item("Buy")).unwrap();
        assert_eq!(raw.timestamp_us, 1_700_000_000_000_000);
        assert_eq!(raw.exchange, "bybit_spot");
        assert_eq!(raw.symbol, "BTCUSDT");
        assert_eq!(raw.trade_id, 2_100_000_000_012_345);
        assert_eq!(raw.price, 43000.5);
        assert_eq!(raw.qty, 0.012);
        assert!(!raw.is_buyer_maker, "taker buy → buyer is NOT the maker");
    }

    #[test]
    fn test_build_raw_trade_sell() {
        let raw = build_raw_trade("bybit_spot", "BTCUSDT", &trade_item("Sell")).unwrap();
        assert!(raw.is_buyer_maker, "taker sell → buyer IS the maker");
    }

    #[test]
    fn test_build_raw_trade_unknown_side_or_bad_number() {
        assert!(build_raw_trade("bybit_spot", "BTCUSDT", &trade_item("Unknown")).is_none());
        let mut bad = trade_item("Buy");
        bad.p = "oops".to_string();
        assert!(build_raw_trade("bybit_spot", "BTCUSDT", &bad).is_none());
    }

    /// Pins accumulator/tape side-mapping consistency, mirroring
    /// hyperliquid.rs's equivalent test.
    #[test]
    fn test_bybit_side_mapping_consistent_with_tape() {
        for side in ["Buy", "Sell"] {
            let is_buy = bybit_side_is_buy(side).unwrap();
            let raw = build_raw_trade("bybit_perp", "ETHUSDT", &trade_item(side)).unwrap();
            assert_eq!(
                is_buy, !raw.is_buyer_maker,
                "accumulator and tape side mappings drifted for side {side}"
            );
        }
    }

    // ── Liquidation parsing ──────────────────────────────────────────────

    #[test]
    fn test_build_liquidation() {
        let item = BybitLiquidationItem {
            time_ms: 1_700_000_001_000,
            side: "Sell".to_string(),
            v: "1.5".to_string(),
            p: "42998.0".to_string(),
        };
        let liq = build_liquidation("bybit_perp", "BTCUSDT", &item).unwrap();
        assert_eq!(liq.timestamp_us, 1_700_000_001_000_000);
        assert_eq!(liq.exchange, "bybit_perp");
        assert_eq!(liq.symbol, "BTCUSDT");
        assert_eq!(liq.side, "Sell");
        assert_eq!(liq.price, 42998.0);
        assert_eq!(liq.qty, 1.5);
    }

    #[test]
    fn test_build_liquidation_bad_number_returns_none() {
        let item = BybitLiquidationItem {
            time_ms: 1_700_000_001_000,
            side: "Buy".to_string(),
            v: "oops".to_string(),
            p: "42998.0".to_string(),
        };
        assert!(build_liquidation("bybit_perp", "BTCUSDT", &item).is_none());
    }

    // ── Envelope parsing (topic dispatch, real-shaped messages) ──────────

    #[test]
    fn test_envelope_parses_orderbook_snapshot() {
        let text = serde_json::json!({
            "topic": "orderbook.1000.BTCUSDT",
            "type": "snapshot",
            "ts": 1_700_000_000_000_i64,
            "cts": 1_700_000_000_050_i64,
            "data": ob_data_json()
        })
        .to_string();
        let envelope: BybitEnvelope = serde_json::from_str(&text).unwrap();
        assert_eq!(envelope.topic, "orderbook.1000.BTCUSDT");
        assert_eq!(envelope.msg_type, "snapshot");
        assert_eq!(envelope.cts, Some(1_700_000_000_050));
        assert_eq!(envelope.topic.rsplit('.').next(), Some("BTCUSDT"));
    }

    #[test]
    fn test_envelope_parses_all_liquidation_without_type_field() {
        let text = serde_json::json!({
            "topic": "allLiquidation.BTCUSDT",
            "ts": 1_700_000_000_000_i64,
            "data": [{"T": 1_700_000_000_000_i64, "s": "BTCUSDT", "S": "Buy", "v": "1.5", "p": "42998.0"}]
        })
        .to_string();
        let envelope: BybitEnvelope = serde_json::from_str(&text).unwrap();
        assert_eq!(envelope.msg_type, "", "type field absent — defaults empty");
        assert_eq!(envelope.cts, None, "allLiquidation has no cts field");
    }

    #[test]
    fn test_envelope_ignores_non_topic_admin_frames() {
        // Subscribe ack / pong responses have no `topic` field — must not parse.
        let ack = r#"{"success":true,"ret_msg":"","conn_id":"abc","req_id":"","op":"subscribe"}"#;
        assert!(serde_json::from_str::<BybitEnvelope>(ack).is_err());
        let pong = r#"{"success":true,"ret_msg":"pong","conn_id":"abc","op":"ping"}"#;
        assert!(serde_json::from_str::<BybitEnvelope>(pong).is_err());
    }
}
