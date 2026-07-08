use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::Duration,
};

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
    writer::{
        deriv::{DerivEvent, Liquidation, MarkFunding, OpenInterest},
        raw::RawDiff,
        trades::RawTrade,
    },
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

/// Parse a Binance combined-stream WebSocket message into its component parts.
///
/// Returns `(stream_name, depth_update, bids, asks)` or `None` if parsing fails.
/// Exposed for benchmarks.
#[doc(hidden)]
#[allow(clippy::type_complexity)]
pub fn parse_combined_message(
    text: &str,
) -> Option<(String, DepthUpdate, Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let combined: WsCombined = serde_json::from_str(text).ok()?;
    let depth: DepthUpdate = serde_json::from_value(combined.data).ok()?;
    let (bids, asks, _errs) = parse_depth_levels(&depth);
    Some((combined.stream, depth, bids, asks))
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

// ── aggTrade parsing ──────────────────────────────────────────────────────────

/// Binance aggTrade event payload (combined stream `{sym}@aggTrade`).
#[derive(Debug, Deserialize)]
pub struct AggTradeUpdate {
    #[serde(rename = "a")]
    pub agg_trade_id: i64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub qty: String,
    #[serde(rename = "T")]
    pub trade_time_ms: i64,
    /// `m` — true when the buyer was the maker (the taker sold).
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

/// Build a RawTrade from a parsed aggTrade event.
/// Returns `None` if price/qty strings don't parse.
pub fn agg_trade_to_raw(exchange: &str, symbol: &str, ev: &AggTradeUpdate) -> Option<RawTrade> {
    let price = ev.price.parse::<f64>().ok()?;
    let qty = ev.qty.parse::<f64>().ok()?;
    Some(RawTrade {
        timestamp_us: ev.trade_time_ms * 1_000,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        trade_id: ev.agg_trade_id,
        price,
        qty,
        is_buyer_maker: ev.is_buyer_maker,
    })
}

/// Handle one aggTrade event: feed the 1s accumulator (taker side) and publish
/// the trade to the tape channel. `m` (is_buyer_maker) = true means the taker sold.
fn handle_agg_trade(
    conn_name: &str,
    exchange_name: &str,
    symbol: &str,
    data: serde_json::Value,
    accumulators: &mut HashMap<String, WindowAccumulator>,
    trade_tx: &broadcast::Sender<RawTrade>,
) {
    let ev: AggTradeUpdate = match serde_json::from_value(data) {
        Ok(v) => v,
        Err(_) => return,
    };
    let Some(trade) = agg_trade_to_raw(exchange_name, symbol, &ev) else {
        warn!(conn = %conn_name, symbol = %symbol, "aggTrade price/qty parse failed");
        return;
    };
    let acc = accumulators
        .entry(symbol.to_string())
        .or_insert_with(|| WindowAccumulator::new(exchange_name, symbol, trade.timestamp_us));
    acc.accumulate_trade(trade.qty, !trade.is_buyer_maker);

    if trade_tx.send(trade).is_err() {
        warn!(conn = %conn_name, symbol = %symbol, "trade: no receivers");
    }
}

// ── Derivatives: markPrice / forceOrder / openInterest (binance_perp only) ────
//
// These handlers publish to the deriv channel and deliberately do NOT call
// `runtime::record_event` — depth-liveness stays meaningful (same decision as
// trades): a connection receiving only mark prices while depth is stalled must
// still look stale to the monitor.

/// Binance markPrice event payload (combined stream `{sym}@markPrice@1s`).
#[derive(Debug, Deserialize)]
pub struct MarkPriceUpdate {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    /// Mark price.
    #[serde(rename = "p")]
    pub mark_px: String,
    /// Index price.
    #[serde(rename = "i")]
    pub index_px: String,
    /// Funding rate.
    #[serde(rename = "r")]
    pub funding_rate: String,
    /// Next funding time (ms).
    #[serde(rename = "T")]
    pub next_funding_time_ms: i64,
}

/// Build a MarkFunding from a parsed markPrice event (ms → µs).
/// Returns `None` if any numeric string doesn't parse.
pub fn mark_price_to_funding(
    exchange: &str,
    symbol: &str,
    ev: &MarkPriceUpdate,
) -> Option<MarkFunding> {
    Some(MarkFunding {
        timestamp_us: ev.event_time_ms * 1_000,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        mark_px: ev.mark_px.parse::<f64>().ok()?,
        index_px: Some(ev.index_px.parse::<f64>().ok()?),
        funding_rate: ev.funding_rate.parse::<f64>().ok()?,
        next_funding_ts: Some(ev.next_funding_time_ms * 1_000),
    })
}

/// Binance forceOrder (liquidation) event payload — the order object `o`.
#[derive(Debug, Deserialize)]
pub struct ForceOrderUpdate {
    #[serde(rename = "o")]
    pub order: ForceOrder,
}

#[derive(Debug, Deserialize)]
pub struct ForceOrder {
    /// Side of the forced order (`BUY` / `SELL`).
    #[serde(rename = "S")]
    pub side: String,
    /// Original quantity.
    #[serde(rename = "q")]
    pub qty: String,
    /// Average fill price.
    #[serde(rename = "ap")]
    pub avg_price: String,
    /// Trade time (ms).
    #[serde(rename = "T")]
    pub trade_time_ms: i64,
}

/// Build a Liquidation from a parsed forceOrder event. Price is the average
/// fill price (`ap`), not the order price. Returns `None` on unparseable numbers.
pub fn force_order_to_liquidation(
    exchange: &str,
    symbol: &str,
    ev: &ForceOrderUpdate,
) -> Option<Liquidation> {
    Some(Liquidation {
        timestamp_us: ev.order.trade_time_ms * 1_000,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        side: ev.order.side.clone(),
        price: ev.order.avg_price.parse::<f64>().ok()?,
        qty: ev.order.qty.parse::<f64>().ok()?,
    })
}

/// GET /fapi/v1/openInterest response.
#[derive(Debug, Deserialize)]
pub struct OpenInterestRest {
    #[serde(rename = "openInterest")]
    pub open_interest: String,
    pub symbol: String,
    #[serde(rename = "time")]
    pub time_ms: i64,
}

/// Build an OpenInterest from the REST response (base units only — the
/// endpoint has no quote-denominated figure).
pub fn oi_rest_to_open_interest(exchange: &str, resp: &OpenInterestRest) -> Option<OpenInterest> {
    Some(OpenInterest {
        timestamp_us: resp.time_ms * 1_000,
        exchange: exchange.to_string(),
        symbol: resp.symbol.clone(),
        oi_base: resp.open_interest.parse::<f64>().ok()?,
        oi_quote: None,
    })
}

/// Handle one markPrice event: parse and publish MarkFunding to the deriv channel.
fn handle_mark_price(
    conn_name: &str,
    exchange_name: &str,
    symbol: &str,
    data: serde_json::Value,
    deriv_tx: &broadcast::Sender<DerivEvent>,
) {
    let ev: MarkPriceUpdate = match serde_json::from_value(data) {
        Ok(v) => v,
        Err(_) => return,
    };
    let Some(mf) = mark_price_to_funding(exchange_name, symbol, &ev) else {
        warn!(conn = %conn_name, symbol = %symbol, "markPrice numeric parse failed");
        return;
    };
    if deriv_tx.send(DerivEvent::MarkFunding(mf)).is_err() {
        warn!(conn = %conn_name, symbol = %symbol, "deriv: no receivers");
    }
}

/// Handle one forceOrder event: parse and publish Liquidation to the deriv channel.
fn handle_force_order(
    conn_name: &str,
    exchange_name: &str,
    symbol: &str,
    data: serde_json::Value,
    deriv_tx: &broadcast::Sender<DerivEvent>,
) {
    let ev: ForceOrderUpdate = match serde_json::from_value(data) {
        Ok(v) => v,
        Err(_) => return,
    };
    let Some(liq) = force_order_to_liquidation(exchange_name, symbol, &ev) else {
        warn!(conn = %conn_name, symbol = %symbol, "forceOrder numeric parse failed");
        return;
    };
    if deriv_tx.send(DerivEvent::Liquidation(liq)).is_err() {
        warn!(conn = %conn_name, symbol = %symbol, "deriv: no receivers");
    }
}

/// Dispatch one parsed combined-stream event by its stream-name suffix.
///
/// Handles aggTrade/markPrice/forceOrder in place and returns `None`. For a
/// depth update (or any unrecognized suffix) returns the event's `data`
/// unchanged so the caller can parse it as a `DepthUpdate` — this lets both
/// the sync-phase buffer replay and the steady-state event loop dispatch
/// messages identically, regardless of which physical WS connection they
/// arrived on (binance_perp merges two: see `connection_task`).
#[allow(clippy::too_many_arguments)]
fn dispatch_non_depth(
    conn_name: &str,
    exchange_name: &str,
    stream: &str,
    symbol: &str,
    data: serde_json::Value,
    accumulators: &mut HashMap<String, WindowAccumulator>,
    trade_tx: &broadcast::Sender<RawTrade>,
    deriv_tx: &broadcast::Sender<DerivEvent>,
) -> Option<serde_json::Value> {
    if stream.ends_with("@aggTrade") {
        handle_agg_trade(
            conn_name,
            exchange_name,
            symbol,
            data,
            accumulators,
            trade_tx,
        );
        return None;
    }
    if stream.ends_with("@markPrice@1s") {
        handle_mark_price(conn_name, exchange_name, symbol, data, deriv_tx);
        return None;
    }
    if stream.ends_with("@forceOrder") {
        handle_force_order(conn_name, exchange_name, symbol, data, deriv_tx);
        return None;
    }
    Some(data)
}

/// Poll interval for the open-interest REST endpoint (per symbol batch).
const OI_POLL_INTERVAL_S: u64 = 60;

/// Background open-interest REST poll (binance_perp only — spot and HL return
/// no URLs). Fully independent from the WS path: any error is warn + continue,
/// never triggers reconnects (specs/derivatives-feeds.md acceptance).
async fn poll_open_interest(
    conn_name: String,
    exchange_name: String,
    urls: Vec<(String, String)>, // (symbol, url)
    http_client: reqwest::Client,
    deriv_tx: broadcast::Sender<DerivEvent>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(OI_POLL_INTERVAL_S));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(conn = %conn_name, "OI poll: shutdown");
                return;
            }
            _ = interval.tick() => {}
        }
        for (symbol, url) in &urls {
            let body = match http_client.get(url).send().await {
                Ok(resp) => match resp.error_for_status() {
                    Ok(resp) => match resp.text().await {
                        Ok(b) => b,
                        Err(e) => {
                            warn!(conn = %conn_name, symbol = %symbol, error = %e, "OI poll body read failed");
                            continue;
                        }
                    },
                    Err(e) => {
                        warn!(conn = %conn_name, symbol = %symbol, error = %e, "OI poll HTTP error");
                        continue;
                    }
                },
                Err(e) => {
                    warn!(conn = %conn_name, symbol = %symbol, error = %e, "OI poll request failed");
                    continue;
                }
            };
            let resp: OpenInterestRest = match serde_json::from_str(&body) {
                Ok(r) => r,
                Err(e) => {
                    warn!(conn = %conn_name, symbol = %symbol, error = %e, "OI poll parse failed");
                    continue;
                }
            };
            let Some(oi) = oi_rest_to_open_interest(&exchange_name, &resp) else {
                warn!(conn = %conn_name, symbol = %symbol, "OI numeric parse failed");
                continue;
            };
            if deriv_tx.send(DerivEvent::OpenInterest(oi)).is_err() {
                warn!(conn = %conn_name, symbol = %symbol, "deriv: no receivers (OI)");
            }
        }
    }
}

// ── Two-connection merge (binance_perp only) ─────────────────────────────────
//
// binance_perp opens a second WS (`ExchangeAdapter::market_ws_url`) alongside
// the depth connection (fathom#62 — see module doc comment in
// `exchange/binance_perp.rs`). Both feed the SAME per-symbol book +
// accumulator: `handle_event_message` is the shared per-message handler so the
// dispatch logic doesn't care which physical socket a frame arrived on. The
// two connections are treated as one unit — either one closing tears down and
// reconnects both (`recv_opt` surfaces the market connection's closure the
// same way `fwd_rx.recv() -> None` already does for the depth connection).

/// `rx.recv().await`, or pend forever when there is no second connection
/// (every venue except binance_perp) — lets a single `tokio::select!` treat
/// the optional market channel uniformly with the always-present depth one.
async fn recv_opt(rx: &mut Option<mpsc::Receiver<String>>) -> Option<String> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

/// Abort and join an optional forwarder task (no-op when `None`).
async fn abort_forwarder(handle: Option<tokio::task::JoinHandle<()>>) {
    if let Some(h) = handle {
        h.abort();
        let _ = h.await;
    }
}

/// Parse and dispatch one WS text frame in the steady-state event loop.
/// Shared by the depth (`fwd_rx`) and market (`market_fwd_rx`) select arms —
/// see the module comment above. Returns `true` when a sequence gap was
/// detected and the caller should tear down and reconnect.
#[allow(clippy::too_many_arguments)]
fn handle_event_message(
    name: &str,
    exchange_name: &str,
    adapter_name: &str,
    symbols_set: &HashSet<String>,
    text: &str,
    books: &mut HashMap<String, OrderBook>,
    accumulators: &mut HashMap<String, WindowAccumulator>,
    monitor: &MonitorState,
    raw_tx: &broadcast::Sender<RawDiff>,
    trade_tx: &broadcast::Sender<RawTrade>,
    deriv_tx: &broadcast::Sender<DerivEvent>,
    metrics: &Metrics,
    stats: &mut runtime::StatsTracker,
) -> bool {
    let combined: WsCombined = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let sym_lower = combined.stream.split('@').next().unwrap_or("").to_string();
    let symbol = sym_lower.to_uppercase();
    if !symbols_set.contains(&symbol) {
        return false;
    }

    let data = match dispatch_non_depth(
        name,
        exchange_name,
        &combined.stream,
        &symbol,
        combined.data,
        accumulators,
        trade_tx,
        deriv_tx,
    ) {
        Some(d) => d,
        None => return false,
    };

    let depth: DepthUpdate = match serde_json::from_value(data) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let (bids, asks, parse_errs) = parse_depth_levels(&depth);
    if parse_errs > 0 {
        warn!(conn = %name, symbol = %symbol, errors = parse_errs, "parse errors in depth levels");
    }
    let timestamp_us = depth.event_time_ms * 1_000;

    let diff = DepthDiff {
        exchange: exchange_name.to_string(),
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
            runtime::record_gap(monitor, name, &symbol);
            true
        }
        Err(e) => {
            warn!(error = %e, "book error");
            false
        }
        Ok(None) => false,
        Ok(Some(applied)) => {
            runtime::record_event(monitor, name, &symbol);

            if raw_tx
                .send(RawDiff {
                    timestamp_us,
                    exchange: exchange_name.to_string(),
                    symbol: symbol.clone(),
                    seq_id: diff.seq_id,
                    prev_seq_id: diff.prev_seq_id,
                    bids,
                    asks,
                })
                .is_err()
            {
                warn!(conn = %name, symbol = %symbol, "raw: no receivers");
            }

            let acc = accumulators
                .entry(symbol.clone())
                .or_insert_with(|| WindowAccumulator::new(adapter_name, &symbol, timestamp_us));
            acc.on_diff(book, &applied);
            stats.inc();
            runtime::inc_event_metrics(metrics, name, &symbol);
            false
        }
    }
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
    trade_tx: broadcast::Sender<RawTrade>,
    deriv_tx: broadcast::Sender<DerivEvent>,
    cancel: CancellationToken,
    metrics: std::sync::Arc<Metrics>,
) {
    let name = conn.name.clone();
    let exchange_name = adapter.name().to_string();
    let symbols: Vec<String> = conn.symbols.iter().map(|s| s.to_uppercase()).collect();
    let symbols_set: HashSet<String> = symbols.iter().cloned().collect();

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

    // Open-interest REST poll (perp only): spawned once, outside the reconnect
    // loop — it must survive WS reconnects and never affect them. Exits on cancel.
    let oi_urls: Vec<(String, String)> = symbols
        .iter()
        .filter_map(|s| adapter.open_interest_url(s).map(|u| (s.clone(), u)))
        .collect();
    let _oi_poll = (!oi_urls.is_empty()).then(|| {
        tokio::spawn(poll_open_interest(
            name.clone(),
            exchange_name.clone(),
            oi_urls,
            http_client.clone(),
            deriv_tx.clone(),
            cancel.clone(),
        ))
    });

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

        // Second (market) connection — binance_perp only, see module comment
        // above `recv_opt`. Connected before either forwarder is spawned so a
        // failure here drops the already-open depth socket and retries both
        // together rather than running depth-only.
        let market_url = conn
            .market_ws_url_override
            .clone()
            .or_else(|| adapter.market_ws_url(&symbols));
        let market_ws = match &market_url {
            Some(url) => match runtime::connect_ws(url, &name, &mut backoff_ms).await {
                Some(ws) => Some(ws),
                None => continue,
            },
            None => None,
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

        let (mut market_fwd_rx, market_forwarder): (
            Option<mpsc::Receiver<String>>,
            Option<tokio::task::JoinHandle<()>>,
        ) = match market_ws {
            Some(ws) => {
                let (sink, stream) = ws.split();
                let (tx, rx) = mpsc::channel::<String>(crate::CHANNEL_BUFFER);
                let handle = runtime::spawn_forwarder(
                    name.clone(),
                    sink,
                    stream,
                    DEFAULT_HEARTBEAT_TIMEOUT_S,
                    tx,
                );
                (Some(rx), Some(handle))
            }
            None => (None, None),
        };

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
                abort_forwarder(market_forwarder).await;
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
            abort_forwarder(market_forwarder).await;
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
            if let Some(rx) = market_fwd_rx.as_mut() {
                while let Ok(msg) = rx.try_recv() {
                    buf.push(msg);
                }
            }
            debug!(conn = %name, attempt, buf_size = buf.len(), "sync phase drain");
            if buf.is_empty() && attempt > 0 {
                break 'sync;
            }

            // Once a depth gap is found in this attempt, `gap_this_attempt` stops
            // further depth application (the book is getting reset + reconnected
            // regardless) but the loop keeps draining `buf` — market events
            // (aggTrade/markPrice/forceOrder) are dispatched unconditionally
            // above and must never be discarded just because a later/earlier
            // depth entry in the same buffer triggered a gap (fathom#65 review).
            let mut gap_this_attempt = false;
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

                let Some(data) = dispatch_non_depth(
                    &name,
                    &exchange_name,
                    &combined.stream,
                    &symbol,
                    combined.data,
                    &mut accumulators,
                    &trade_tx,
                    &deriv_tx,
                ) else {
                    // Market event — already dispatched above. Never depends on
                    // the depth-gap state below.
                    continue;
                };

                if gap_this_attempt {
                    // Depth sync already failed this attempt — skip further
                    // book mutation, but keep looping so trailing market
                    // events in `buf` still reach dispatch_non_depth above.
                    continue;
                }

                let depth: DepthUpdate = match serde_json::from_value(data) {
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
                        gap_this_attempt = true;
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

            if gap_this_attempt {
                break 'sync;
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
            abort_forwarder(market_forwarder).await;
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
                    let gap = handle_event_message(
                        &name, &exchange_name, adapter.name(), &symbols_set, &text,
                        &mut books, &mut accumulators, &monitor,
                        &raw_tx, &trade_tx, &deriv_tx, &metrics, &mut stats,
                    );
                    if gap { break 'inner; }
                }

                // Market connection (binance_perp only) — aggTrade/markPrice/forceOrder.
                // See module comment above `recv_opt`: its closure is treated the same
                // as the depth connection's, tearing down and reconnecting both.
                msg = recv_opt(&mut market_fwd_rx) => {
                    let text = match msg {
                        None => break 'inner,
                        Some(t) => t,
                    };
                    let gap = handle_event_message(
                        &name, &exchange_name, adapter.name(), &symbols_set, &text,
                        &mut books, &mut accumulators, &monitor,
                        &raw_tx, &trade_tx, &deriv_tx, &metrics, &mut stats,
                    );
                    if gap { break 'inner; }
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
        abort_forwarder(market_forwarder).await;

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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::accumulator::WindowAccumulator;

    /// Real-shaped Binance combined-stream aggTrade message.
    fn agg_trade_json() -> String {
        serde_json::json!({
            "stream": "ethusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1_700_000_001_000_i64,
                "s": "ETHUSDT",
                "a": 26129,
                "p": "3000.50",
                "q": "4.70443515",
                "f": 27781,
                "l": 27781,
                "T": 1_700_000_000_999_i64,
                "m": true,
                "M": true
            }
        })
        .to_string()
    }

    #[test]
    fn test_agg_trade_parse_to_raw_trade() {
        let combined: WsCombined = serde_json::from_str(&agg_trade_json()).unwrap();
        assert!(combined.stream.ends_with("@aggTrade"));

        let ev: AggTradeUpdate = serde_json::from_value(combined.data).unwrap();
        let trade = agg_trade_to_raw("binance_spot", "ETHUSDT", &ev).unwrap();

        assert_eq!(
            trade.timestamp_us, 1_700_000_000_999_000,
            "T (trade time) in µs"
        );
        assert_eq!(trade.exchange, "binance_spot");
        assert_eq!(trade.symbol, "ETHUSDT");
        assert_eq!(trade.trade_id, 26129);
        assert_eq!(trade.price, 3000.50);
        assert_eq!(trade.qty, 4.70443515);
        assert!(trade.is_buyer_maker);
    }

    #[test]
    fn test_agg_trade_unparseable_price_returns_none() {
        let ev = AggTradeUpdate {
            agg_trade_id: 1,
            price: "not_a_number".into(),
            qty: "1.0".into(),
            trade_time_ms: 1_700_000_000_000,
            is_buyer_maker: false,
        };
        assert!(agg_trade_to_raw("binance_spot", "ETHUSDT", &ev).is_none());
    }

    /// Real-shaped Binance combined-stream markPrice@1s message.
    fn mark_price_json() -> String {
        serde_json::json!({
            "stream": "ethusdt@markPrice@1s",
            "data": {
                "e": "markPriceUpdate",
                "E": 1_700_000_001_000_i64,
                "s": "ETHUSDT",
                "p": "3000.12345678",
                "i": "3000.05000000",
                "P": "3001.00000000",
                "r": "0.00010000",
                "T": 1_700_028_800_000_i64
            }
        })
        .to_string()
    }

    #[test]
    fn test_mark_price_parse_to_mark_funding() {
        let combined: WsCombined = serde_json::from_str(&mark_price_json()).unwrap();
        assert!(combined.stream.ends_with("@markPrice@1s"));

        let ev: MarkPriceUpdate = serde_json::from_value(combined.data).unwrap();
        let mf = mark_price_to_funding("binance_perp", "ETHUSDT", &ev).unwrap();

        assert_eq!(
            mf.timestamp_us, 1_700_000_001_000_000,
            "E (event time) in µs"
        );
        assert_eq!(mf.exchange, "binance_perp");
        assert_eq!(mf.symbol, "ETHUSDT");
        assert_eq!(mf.mark_px, 3000.12345678);
        assert_eq!(mf.index_px, Some(3000.05));
        assert_eq!(mf.funding_rate, 0.0001, "r string → f64");
        assert_eq!(
            mf.next_funding_ts,
            Some(1_700_028_800_000_000),
            "T (next funding time) in µs"
        );
    }

    #[test]
    fn test_mark_price_unparseable_rate_returns_none() {
        let ev = MarkPriceUpdate {
            event_time_ms: 1_700_000_000_000,
            mark_px: "3000.0".into(),
            index_px: "3000.0".into(),
            funding_rate: "not_a_number".into(),
            next_funding_time_ms: 1_700_028_800_000,
        };
        assert!(mark_price_to_funding("binance_perp", "ETHUSDT", &ev).is_none());
    }

    /// Real-shaped Binance combined-stream forceOrder message.
    fn force_order_json() -> String {
        serde_json::json!({
            "stream": "ethusdt@forceOrder",
            "data": {
                "e": "forceOrder",
                "E": 1_700_000_001_100_i64,
                "o": {
                    "s": "ETHUSDT",
                    "S": "SELL",
                    "o": "LIMIT",
                    "f": "IOC",
                    "q": "0.014",
                    "p": "9910",
                    "ap": "9910.5",
                    "X": "FILLED",
                    "l": "0.014",
                    "z": "0.014",
                    "T": 1_700_000_001_099_i64
                }
            }
        })
        .to_string()
    }

    #[test]
    fn test_force_order_parse_to_liquidation() {
        let combined: WsCombined = serde_json::from_str(&force_order_json()).unwrap();
        assert!(combined.stream.ends_with("@forceOrder"));

        let ev: ForceOrderUpdate = serde_json::from_value(combined.data).unwrap();
        let liq = force_order_to_liquidation("binance_perp", "ETHUSDT", &ev).unwrap();

        assert_eq!(
            liq.timestamp_us, 1_700_000_001_099_000,
            "o.T (trade time) in µs"
        );
        assert_eq!(liq.exchange, "binance_perp");
        assert_eq!(liq.symbol, "ETHUSDT");
        assert_eq!(liq.side, "SELL");
        assert_eq!(liq.price, 9910.5, "avg price o.ap, not order price o.p");
        assert_eq!(liq.qty, 0.014);
    }

    #[test]
    fn test_force_order_unparseable_price_returns_none() {
        let json = serde_json::json!({
            "o": {"S": "BUY", "q": "1.0", "ap": "oops", "T": 1_700_000_000_000_i64}
        });
        let ev: ForceOrderUpdate = serde_json::from_value(json).unwrap();
        assert!(force_order_to_liquidation("binance_perp", "ETHUSDT", &ev).is_none());
    }

    /// GET /fapi/v1/openInterest response shape.
    #[test]
    fn test_open_interest_rest_parse() {
        let body = r#"{"openInterest":"10659.509","symbol":"BTCUSDT","time":1583127900000}"#;
        let resp: OpenInterestRest = serde_json::from_str(body).unwrap();
        let oi = oi_rest_to_open_interest("binance_perp", &resp).unwrap();

        assert_eq!(oi.timestamp_us, 1_583_127_900_000_000, "time (ms) in µs");
        assert_eq!(oi.exchange, "binance_perp");
        assert_eq!(oi.symbol, "BTCUSDT");
        assert_eq!(oi.oi_base, 10_659.509);
        assert_eq!(oi.oi_quote, None, "endpoint reports base units only");
    }

    #[test]
    fn test_open_interest_rest_unparseable_returns_none() {
        let resp = OpenInterestRest {
            open_interest: "oops".into(),
            symbol: "BTCUSDT".into(),
            time_ms: 1_583_127_900_000,
        };
        assert!(oi_rest_to_open_interest("binance_perp", &resp).is_none());
    }

    /// m=true → buyer is maker → the taker SOLD → sell_vol, negative delta.
    #[test]
    fn test_agg_trade_buyer_maker_accumulates_as_sell() {
        let mut acc = WindowAccumulator::new("binance_spot", "ETHUSDT", 0);
        let ev = AggTradeUpdate {
            agg_trade_id: 1,
            price: "3000.0".into(),
            qty: "2.0".into(),
            trade_time_ms: 1_700_000_000_000,
            is_buyer_maker: true,
        };
        let trade = agg_trade_to_raw("binance_spot", "ETHUSDT", &ev).unwrap();
        acc.accumulate_trade(trade.qty, !trade.is_buyer_maker);

        let snap = acc.flush_with_levels(None, 1_000_000);
        assert_eq!(snap.sell_vol, 2.0);
        assert_eq!(snap.buy_vol, 0.0);
        assert_eq!(snap.volume_delta, -2.0);
        assert_eq!(snap.trade_count, 1);
    }

    /// m=false → seller is maker → the taker BOUGHT → buy_vol, positive delta.
    #[test]
    fn test_agg_trade_seller_maker_accumulates_as_buy() {
        let mut acc = WindowAccumulator::new("binance_spot", "ETHUSDT", 0);
        let ev = AggTradeUpdate {
            agg_trade_id: 2,
            price: "3000.0".into(),
            qty: "1.5".into(),
            trade_time_ms: 1_700_000_000_000,
            is_buyer_maker: false,
        };
        let trade = agg_trade_to_raw("binance_spot", "ETHUSDT", &ev).unwrap();
        acc.accumulate_trade(trade.qty, !trade.is_buyer_maker);

        let snap = acc.flush_with_levels(None, 1_000_000);
        assert_eq!(snap.buy_vol, 1.5);
        assert_eq!(snap.sell_vol, 0.0);
        assert_eq!(snap.volume_delta, 1.5);
        assert_eq!(snap.trade_count, 1);
    }
}
