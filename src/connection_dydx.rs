use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use ordered_float::OrderedFloat;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Snapshot1s, WindowAccumulator},
    config::ConnectionConfig,
    connection::sleep_backoff,
    exchange::dydx::{EXCHANGE_NAME, WS_URL},
    metrics::{ConnLabel, Metrics},
    monitor::{MonitorState, lock_state},
    orderbook::DiffApplied,
    writer::raw::RawDiff,
};

const BACKOFF_START_MS: u64 = 1_000;
/// dYdX pings every 30s; respond within 10s. Use 45s as safe heartbeat timeout.
const HEARTBEAT_TIMEOUT_S: u64 = 45;

// ── Local order book for dYdX ───────────────────────────────────────────────

struct DydxBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    prev_best_bid: (f64, f64),
    prev_best_ask: (f64, f64),
    bid_last: HashMap<OrderedFloat<f64>, f64>,
    ask_last: HashMap<OrderedFloat<f64>, f64>,
}

impl DydxBook {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            prev_best_bid: (f64::NEG_INFINITY, 0.0),
            prev_best_ask: (f64::INFINITY, 0.0),
            bid_last: HashMap::new(),
            ask_last: HashMap::new(),
        }
    }

    fn apply_snapshot(&mut self, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) {
        self.bids.clear();
        self.asks.clear();
        self.bid_last.clear();
        self.ask_last.clear();
        for (px, qty) in bids {
            if qty > 0.0 {
                let key = OrderedFloat(px);
                self.bids.insert(key, qty);
                self.bid_last.insert(key, qty);
            }
        }
        for (px, qty) in asks {
            if qty > 0.0 {
                let key = OrderedFloat(px);
                self.asks.insert(key, qty);
                self.ask_last.insert(key, qty);
            }
        }
        self.prev_best_bid = self.best_bid();
        self.prev_best_ask = self.best_ask();
    }

    fn apply_diffs(&mut self, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> DiffApplied {
        let prev_best_bid_px = self.prev_best_bid.0;
        let prev_best_bid_qty = self.prev_best_bid.1;
        let prev_best_ask_px = self.prev_best_ask.0;
        let prev_best_ask_qty = self.prev_best_ask.1;

        let mut bid_abs_change = 0.0_f64;
        let mut ask_abs_change = 0.0_f64;

        for (px, qty) in bids {
            let key = OrderedFloat(px);
            let prev = self.bid_last.get(&key).copied().unwrap_or(0.0);
            bid_abs_change += (qty - prev).abs();
            if qty == 0.0 {
                self.bids.remove(&key);
                self.bid_last.remove(&key);
            } else {
                self.bids.insert(key, qty);
                self.bid_last.insert(key, qty);
            }
        }

        for (px, qty) in asks {
            let key = OrderedFloat(px);
            let prev = self.ask_last.get(&key).copied().unwrap_or(0.0);
            ask_abs_change += (qty - prev).abs();
            if qty == 0.0 {
                self.asks.remove(&key);
                self.ask_last.remove(&key);
            } else {
                self.asks.insert(key, qty);
                self.ask_last.insert(key, qty);
            }
        }

        // dYdX v4 diff stream does not always send explicit qty=0 for consumed
        // levels (matched orders). Stale levels accumulate and cross the book.
        // Uncross by removing bids >= best_ask and asks <= best_bid.
        self.uncross();

        let (new_best_bid_px, new_best_bid_qty) = self.best_bid();
        let (new_best_ask_px, new_best_ask_qty) = self.best_ask();

        let ofi_bid = if new_best_bid_px >= prev_best_bid_px {
            new_best_bid_qty
        } else {
            -prev_best_bid_qty
        };
        let ofi_ask = if new_best_ask_px <= prev_best_ask_px {
            new_best_ask_qty
        } else {
            -prev_best_ask_qty
        };

        self.prev_best_bid = (new_best_bid_px, new_best_bid_qty);
        self.prev_best_ask = (new_best_ask_px, new_best_ask_qty);

        DiffApplied {
            ofi_l1_delta: ofi_bid - ofi_ask,
            bid_abs_change,
            ask_abs_change,
        }
    }

    /// Remove crossed levels: bids at or above best ask, asks at or below best bid.
    /// dYdX's on-chain CLOB doesn't always send explicit removals for matched orders,
    /// so stale levels accumulate on both sides. This sanitizes the book after each diff.
    fn uncross(&mut self) {
        loop {
            let best_bid = self.bids.keys().next_back().copied();
            let best_ask = self.asks.keys().next().copied();
            match (best_bid, best_ask) {
                (Some(bid), Some(ask)) if bid >= ask => {
                    // Remove the side with less liquidity at the crossing level
                    let bid_qty = self.bids.get(&bid).copied().unwrap_or(0.0);
                    let ask_qty = self.asks.get(&ask).copied().unwrap_or(0.0);
                    if bid_qty <= ask_qty {
                        self.bids.remove(&bid);
                        self.bid_last.remove(&bid);
                    } else {
                        self.asks.remove(&ask);
                        self.ask_last.remove(&ask);
                    }
                }
                _ => break,
            }
        }
    }

    fn best_bid(&self) -> (f64, f64) {
        self.bids
            .iter()
            .next_back()
            .map(|(k, v)| (k.0, *v))
            .unwrap_or((f64::NEG_INFINITY, 0.0))
    }

    fn best_ask(&self) -> (f64, f64) {
        self.asks
            .iter()
            .next()
            .map(|(k, v)| (k.0, *v))
            .unwrap_or((f64::INFINITY, 0.0))
    }

    /// Top N levels: bids descending, asks ascending.
    #[allow(clippy::type_complexity)]
    fn top_n(&self, n: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        let bids: Vec<(f64, f64)> = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(k, v)| (k.0, *v))
            .collect();
        let asks: Vec<(f64, f64)> = self.asks.iter().take(n).map(|(k, v)| (k.0, *v)).collect();
        (bids, asks)
    }
}

// ── JSON parsing helpers ────────────────────────────────────────────────────

fn parse_snapshot_level(v: &serde_json::Value) -> Option<(f64, f64)> {
    let px = v["price"].as_str()?.parse().ok()?;
    let sz = v["size"].as_str()?.parse().ok()?;
    Some((px, sz))
}

fn parse_diff_level(v: &serde_json::Value) -> Option<(f64, f64)> {
    let arr = v.as_array()?;
    if arr.len() < 2 {
        return None;
    }
    let px = arr[0].as_str()?.parse().ok()?;
    let sz = arr[1].as_str()?.parse().ok()?;
    Some((px, sz))
}

// ── Connection task ─────────────────────────────────────────────────────────

pub async fn connection_task_dydx(
    conn: ConnectionConfig,
    _data_dir: PathBuf,
    monitor: MonitorState,
    raw_tx: broadcast::Sender<RawDiff>,
    snap_tx: broadcast::Sender<Snapshot1s>,
    cancel: CancellationToken,
    metrics: std::sync::Arc<Metrics>,
) {
    let name = conn.name.clone();
    let exchange_name = EXCHANGE_NAME.to_string();
    let symbols: Vec<String> = conn.symbols.clone();

    let mut books: HashMap<String, DydxBook> = HashMap::new();
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

    loop {
        if cancel.is_cancelled() {
            info!(conn = %name, "shutdown signal received, exiting connection loop");
            break;
        }
        info!(conn = %name, "connecting...");

        let ws_url = conn
            .ws_url_override
            .clone()
            .unwrap_or_else(|| WS_URL.to_string());

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

        let (ws_sink, ws_stream) = ws.split();

        // Bidirectional forwarder: send_tx for outgoing, fwd_tx for incoming
        let (send_tx, mut send_rx) = mpsc::channel::<String>(64);
        let (fwd_tx, mut fwd_rx) = mpsc::channel::<String>(crate::CHANNEL_BUFFER);

        let fwd_name = name.clone();
        let forwarder = tokio::spawn(async move {
            let hb_dur = Duration::from_secs(HEARTBEAT_TIMEOUT_S);
            let mut sink = ws_sink;
            let mut stream = ws_stream;
            loop {
                tokio::select! {
                    msg = tokio::time::timeout(hb_dur, stream.next()) => {
                        match msg {
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
                    outgoing = send_rx.recv() => {
                        match outgoing {
                            None => break,
                            Some(text) => {
                                if sink.send(Message::Text(text.into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        // Wait for the "connected" message
        let connected = match tokio::time::timeout(Duration::from_secs(10), fwd_rx.recv()).await {
            Ok(Some(text)) => {
                let v: serde_json::Value =
                    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
                v["type"].as_str() == Some("connected")
            }
            _ => false,
        };

        if !connected {
            warn!(conn = %name, "did not receive 'connected' message");
            forwarder.abort();
            let _ = forwarder.await;
            sleep_backoff(&mut backoff_ms).await;
            continue;
        }
        info!(conn = %name, "received 'connected' from dYdX");

        // Subscribe to orderbook + trades for each symbol
        let mut subscribe_ok = true;
        for sym in &symbols {
            let sub_book = serde_json::json!({
                "type": "subscribe",
                "channel": "v4_orderbook",
                "id": sym,
                "batched": true
            });
            if send_tx.send(sub_book.to_string()).await.is_err() {
                subscribe_ok = false;
                break;
            }

            let sub_trades = serde_json::json!({
                "type": "subscribe",
                "channel": "v4_trades",
                "id": sym,
                "batched": true
            });
            if send_tx.send(sub_trades.to_string()).await.is_err() {
                subscribe_ok = false;
                break;
            }
        }

        if !subscribe_ok {
            warn!(conn = %name, "failed to send subscribe messages");
            forwarder.abort();
            let _ = forwarder.await;
            sleep_backoff(&mut backoff_ms).await;
            continue;
        }

        info!(conn = %name, symbols = ?symbols, "subscribed to dYdX channels");

        let mut snapshotted: HashMap<String, bool> = HashMap::new();
        let mut last_msg_id: HashMap<String, i64> = HashMap::new();
        for sym in &symbols {
            snapshotted.insert(sym.clone(), false);
        }

        {
            let mut state = lock_state(&monitor);
            if let Some(cs) = state.get_mut(&name) {
                cs.connected = true;
            }
        }

        let mut snap_ticker = tokio::time::interval(Duration::from_secs(1));
        snap_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        snap_ticker.tick().await;

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

                    let v: serde_json::Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let msg_type = match v["type"].as_str() {
                        Some(t) => t,
                        None => continue,
                    };

                    let channel = v["channel"].as_str().unwrap_or("");
                    let symbol = v["id"].as_str().unwrap_or("").to_string();

                    if !symbol.is_empty() && !symbols.contains(&symbol) {
                        continue;
                    }

                    match (msg_type, channel) {
                        ("subscribed", "v4_orderbook") => {
                            let contents = &v["contents"];
                            let bids: Vec<(f64, f64)> = contents["bids"]
                                .as_array()
                                .map(|arr| arr.iter().filter_map(parse_snapshot_level).collect())
                                .unwrap_or_default();
                            let asks: Vec<(f64, f64)> = contents["asks"]
                                .as_array()
                                .map(|arr| arr.iter().filter_map(parse_snapshot_level).collect())
                                .unwrap_or_default();

                            info!(
                                conn = %name,
                                symbol = %symbol,
                                bids = bids.len(),
                                asks = asks.len(),
                                "orderbook snapshot"
                            );

                            let book = books.entry(symbol.clone()).or_insert_with(DydxBook::new);
                            book.apply_snapshot(bids, asks);
                            snapshotted.insert(symbol, true);
                        }

                        ("subscribed", "v4_trades") => {
                            info!(conn = %name, symbol = %symbol, "trades subscribed");
                        }

                        ("channel_batch_data", "v4_orderbook") => {
                            if !snapshotted.get(&symbol).copied().unwrap_or(false) {
                                continue;
                            }

                            let contents = match v["contents"].as_array() {
                                Some(arr) => arr,
                                None => continue,
                            };

                            let mut all_bids: Vec<(f64, f64)> = Vec::new();
                            let mut all_asks: Vec<(f64, f64)> = Vec::new();
                            for item in contents {
                                if let Some(bids) = item["bids"].as_array() {
                                    for level in bids {
                                        if let Some(parsed) = parse_diff_level(level) {
                                            all_bids.push(parsed);
                                        }
                                    }
                                }
                                if let Some(asks) = item["asks"].as_array() {
                                    for level in asks {
                                        if let Some(parsed) = parse_diff_level(level) {
                                            all_asks.push(parsed);
                                        }
                                    }
                                }
                            }

                            let ts_us = Utc::now().timestamp_micros();
                            let msg_id = v["message_id"].as_i64().unwrap_or(0);
                            let prev_msg_id = last_msg_id.get(&symbol).copied().unwrap_or(0);
                            last_msg_id.insert(symbol.clone(), msg_id);

                            if raw_tx
                                .send(RawDiff {
                                    timestamp_us: ts_us,
                                    exchange: exchange_name.clone(),
                                    symbol: symbol.clone(),
                                    seq_id: msg_id,
                                    prev_seq_id: prev_msg_id,
                                    bids: all_bids.clone(),
                                    asks: all_asks.clone(),
                                })
                                .is_err()
                            {
                                warn!(conn = %name, symbol = %symbol, "raw: no receivers");
                            }

                            let book = books.entry(symbol.clone()).or_insert_with(DydxBook::new);
                            let applied = book.apply_diffs(all_bids, all_asks);

                            let best_bid = book.best_bid();
                            let best_ask = book.best_ask();

                            {
                                let mut state = lock_state(&monitor);
                                if let Some(cs) = state.get_mut(&name)
                                    && let Some(ss) = cs.symbols.get_mut(&symbol)
                                {
                                    ss.last_event_at = Some(Instant::now());
                                }
                            }

                            let acc = accumulators
                                .entry(symbol.clone())
                                .or_insert_with(|| WindowAccumulator::new(EXCHANGE_NAME, &symbol, ts_us));

                            let bid_px = if best_bid.0 == f64::NEG_INFINITY { None } else { Some(best_bid.0) };
                            let ask_px = if best_ask.0 == f64::INFINITY { None } else { Some(best_ask.0) };
                            acc.on_diff_from_levels(bid_px, ask_px, &applied);
                            event_count += 1;
                            metrics
                                .events_total
                                .get_or_create(&ConnLabel { conn: name.clone() })
                                .inc();
                        }

                        ("channel_batch_data", "v4_trades") => {
                            let contents = match v["contents"].as_array() {
                                Some(arr) => arr,
                                None => continue,
                            };

                            let ts_us = Utc::now().timestamp_micros();
                            let acc = accumulators
                                .entry(symbol.clone())
                                .or_insert_with(|| WindowAccumulator::new(EXCHANGE_NAME, &symbol, ts_us));

                            for item in contents {
                                if let Some(trades) = item["trades"].as_array() {
                                    for trade in trades {
                                        let is_buy = match trade["side"].as_str() {
                                            Some("BUY") => true,
                                            Some("SELL") => false,
                                            _ => continue, // skip trades with unknown side
                                        };
                                        let size: f64 = match trade["size"]
                                            .as_str()
                                            .and_then(|s| s.parse().ok())
                                        {
                                            Some(s) => s,
                                            None => continue,
                                        };
                                        acc.accumulate_trade(size, is_buy);
                                    }
                                }
                            }
                        }

                        _ => {}
                    }
                }

                _ = snap_ticker.tick() => {
                    let ts_us = Utc::now().timestamp_micros();
                    for sym in &symbols {
                        if let Some(book) = books.get(sym)
                            && let Some(acc) = accumulators.get_mut(sym)
                        {
                            let (bids, asks) = book.top_n(10);
                            let snap = acc.flush_with_levels(Some((&bids, &asks)), ts_us);
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

        // Flush partial accumulators before reset
        {
            let ts_us = Utc::now().timestamp_micros();
            for sym in &symbols {
                if let Some(book) = books.get(sym)
                    && let Some(acc) = accumulators.get_mut(sym)
                {
                    let (bids, asks) = book.top_n(10);
                    let snap = acc.flush_with_levels(Some((&bids, &asks)), ts_us);
                    if snap_tx.send(snap).is_err() {
                        warn!(conn = %name, symbol = %sym, "snap: no receivers (disconnect flush)");
                    }
                }
            }
        }

        books.clear();
        accumulators.clear();
        sleep_backoff(&mut backoff_ms).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snapshot_level() {
        let v = serde_json::json!({"price": "2099.9", "size": "0.532"});
        let (px, sz) = parse_snapshot_level(&v).unwrap();
        assert!((px - 2099.9).abs() < 1e-10);
        assert!((sz - 0.532).abs() < 1e-10);
    }

    #[test]
    fn test_parse_diff_level() {
        let v = serde_json::json!(["2099.5", "3.1"]);
        let (px, sz) = parse_diff_level(&v).unwrap();
        assert!((px - 2099.5).abs() < 1e-10);
        assert!((sz - 3.1).abs() < 1e-10);
    }

    #[test]
    fn test_parse_diff_level_remove() {
        let v = serde_json::json!(["2099.5", "0"]);
        let (px, sz) = parse_diff_level(&v).unwrap();
        assert!((px - 2099.5).abs() < 1e-10);
        assert!((sz - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_dydx_book_snapshot_and_top_n() {
        let mut book = DydxBook::new();
        book.apply_snapshot(
            vec![(100.0, 1.0), (99.0, 2.0), (98.0, 3.0)],
            vec![(101.0, 1.5), (102.0, 2.5)],
        );

        let (bids, asks) = book.top_n(2);
        assert_eq!(bids.len(), 2);
        assert!((bids[0].0 - 100.0).abs() < 1e-10); // best bid first
        assert!((bids[1].0 - 99.0).abs() < 1e-10);
        assert_eq!(asks.len(), 2);
        assert!((asks[0].0 - 101.0).abs() < 1e-10); // best ask first
    }

    #[test]
    fn test_dydx_book_apply_diffs_remove() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 1.0), (99.0, 2.0)], vec![(101.0, 1.5)]);

        // Remove bid at 100.0
        book.apply_diffs(vec![(100.0, 0.0)], vec![]);
        let (bids, _) = book.top_n(5);
        assert_eq!(bids.len(), 1);
        assert!((bids[0].0 - 99.0).abs() < 1e-10);
    }

    #[test]
    fn test_dydx_book_remove_cleans_bid_last() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 5.0), (99.0, 2.0)], vec![(101.0, 3.0)]);
        assert!(book.bid_last.contains_key(&OrderedFloat(100.0)));

        // Remove bid at 100.0 (qty=0)
        book.apply_diffs(vec![(100.0, 0.0)], vec![]);
        assert!(
            !book.bid_last.contains_key(&OrderedFloat(100.0)),
            "removed bid must not leave tombstone in bid_last"
        );
    }

    #[test]
    fn test_dydx_book_remove_cleans_ask_last() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 5.0)], vec![(101.0, 3.0), (102.0, 2.0)]);
        assert!(book.ask_last.contains_key(&OrderedFloat(101.0)));

        book.apply_diffs(vec![], vec![(101.0, 0.0)]);
        assert!(
            !book.ask_last.contains_key(&OrderedFloat(101.0)),
            "removed ask must not leave tombstone in ask_last"
        );
    }

    #[test]
    fn test_dydx_book_ofi_negative() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 5.0)], vec![(101.0, 4.0)]);

        // Increase ask qty: ask price unchanged (<=), ofi_ask = new_qty = 8
        // Bid unchanged: ofi_bid = 5, OFI = 5 - 8 = -3 (ask pressure)
        let applied = book.apply_diffs(vec![], vec![(101.0, 8.0)]);
        assert!(
            (applied.ofi_l1_delta - (-3.0)).abs() < 1e-10,
            "OFI should be negative"
        );
    }

    #[test]
    fn test_dydx_book_ofi_both_sides() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 5.0)], vec![(101.0, 4.0)]);

        // Both sides change: bid 5→10 (same price), ask 4→2 (same price)
        // ofi_bid = 10 (price held), ofi_ask = 2 (price held), OFI = 10 - 2 = 8
        let applied = book.apply_diffs(vec![(100.0, 10.0)], vec![(101.0, 2.0)]);
        assert!((applied.ofi_l1_delta - 8.0).abs() < 1e-10);
    }

    #[test]
    fn test_dydx_book_churn() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 5.0)], vec![(101.0, 4.0)]);

        // bid: |8 - 5| = 3, ask: |6 - 4| = 2
        let applied = book.apply_diffs(vec![(100.0, 8.0)], vec![(101.0, 6.0)]);
        assert!((applied.bid_abs_change - 3.0).abs() < 1e-10);
        assert!((applied.ask_abs_change - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_dydx_book_empty_diff() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 5.0)], vec![(101.0, 4.0)]);

        let applied = book.apply_diffs(vec![], vec![]);
        // No changes, but OFI still computed (price held): ofi_bid = 5, ofi_ask = 4
        assert!((applied.ofi_l1_delta - 1.0).abs() < 1e-10);
        assert_eq!(applied.bid_abs_change, 0.0);
        assert_eq!(applied.ask_abs_change, 0.0);
    }

    #[test]
    fn test_dydx_book_uncross_stale_ask() {
        let mut book = DydxBook::new();
        book.apply_snapshot(
            vec![(100.0, 1.0), (99.0, 2.0)],
            vec![(101.0, 1.5), (102.0, 2.5)],
        );

        // Simulate stale ask: bid moves up past ask without ask being removed
        book.bids.insert(OrderedFloat(101.5), 3.0);
        book.bid_last.insert(OrderedFloat(101.5), 3.0);

        // Manually uncross
        book.uncross();

        let (best_bid_px, _) = book.best_bid();
        let (best_ask_px, _) = book.best_ask();
        assert!(
            best_bid_px < best_ask_px,
            "book should not be crossed after uncross: bid={best_bid_px} ask={best_ask_px}"
        );
    }

    #[test]
    fn test_dydx_book_uncross_multiple_levels() {
        let mut book = DydxBook::new();
        book.apply_snapshot(
            vec![(100.0, 1.0)],
            vec![(101.0, 0.5), (102.0, 2.0), (103.0, 3.0)],
        );

        // Add bids that cross multiple ask levels
        book.bids.insert(OrderedFloat(102.5), 10.0);
        book.bid_last.insert(OrderedFloat(102.5), 10.0);

        book.uncross();

        let (best_bid_px, _) = book.best_bid();
        let (best_ask_px, _) = book.best_ask();
        assert!(
            best_bid_px < best_ask_px,
            "book should not be crossed: bid={best_bid_px} ask={best_ask_px}"
        );
    }

    #[test]
    fn test_dydx_book_uncross_via_apply_diffs() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 1.0)], vec![(101.0, 1.5)]);

        // Apply diff that creates a crossing bid (simulating stale ask at 101.0)
        let _applied = book.apply_diffs(vec![(101.5, 3.0)], vec![]);

        let (best_bid_px, _) = book.best_bid();
        let (best_ask_px, _) = book.best_ask();
        assert!(
            best_bid_px < best_ask_px,
            "apply_diffs should auto-uncross: bid={best_bid_px} ask={best_ask_px}"
        );
    }

    #[test]
    fn test_dydx_book_uncross_no_op_when_not_crossed() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 1.0), (99.0, 2.0)], vec![(101.0, 1.5)]);

        let bid_count_before = book.bids.len();
        let ask_count_before = book.asks.len();
        book.uncross();
        assert_eq!(book.bids.len(), bid_count_before);
        assert_eq!(book.asks.len(), ask_count_before);
    }

    #[test]
    fn test_dydx_book_ofi() {
        let mut book = DydxBook::new();
        book.apply_snapshot(vec![(100.0, 5.0)], vec![(101.0, 4.0)]);

        // Increase best bid qty: ofi_bid = new_qty = 8.0 (price unchanged >= prev)
        // Ask unchanged: ofi_ask = new_qty = 4.0 (price unchanged <= prev)
        // OFI = 8.0 - 4.0 = 4.0
        let applied = book.apply_diffs(vec![(100.0, 8.0)], vec![]);
        assert!((applied.ofi_l1_delta - 4.0).abs() < 1e-10);
    }
}
