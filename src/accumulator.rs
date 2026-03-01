use crate::orderbook::{DiffApplied, OrderBook};

/// One-second snapshot row emitted by WindowAccumulator::flush()
#[derive(Debug, Clone)]
pub struct Snapshot1s {
    pub ts_us: i64,
    pub exchange: String,
    pub symbol: String,
    /// Top 10 bid levels: (price, size) descending
    pub bids: Vec<(f64, f64)>,
    /// Top 10 ask levels: (price, size) ascending
    pub asks: Vec<(f64, f64)>,
    pub mid_px: Option<f64>,
    pub microprice: Option<f64>,
    pub spread_bps: Option<f32>,
    pub imbalance_l1: Option<f32>,
    pub imbalance_l5: Option<f32>,
    pub imbalance_l10: Option<f32>,
    pub bid_depth_l5: f64,
    pub bid_depth_l10: f64,
    pub ask_depth_l5: f64,
    pub ask_depth_l10: f64,
    pub ofi_l1: f64,
    pub churn_bid: f64,
    pub churn_ask: f64,
    pub intra_sigma: f32,
    pub open_px: Option<f64>,
    pub close_px: Option<f64>,
    pub n_events: u32,
}

/// Accumulates per-second stats for one symbol.
pub struct WindowAccumulator {
    exchange: String,
    symbol: String,
    // ── Per-window counters (reset on flush) ──────────────────────────────
    n_events: u32,
    ofi_sum: f64,
    churn_bid: f64,
    churn_ask: f64,
    /// First mid price seen this window
    open_px: Option<f64>,
    /// Running sum of mid prices (for mean)
    mid_sum: f64,
    /// Running sum of mid² (for variance)
    mid_sq_sum: f64,
    window_start_us: i64,
}

impl WindowAccumulator {
    pub fn new(exchange: &str, symbol: &str, now_us: i64) -> Self {
        Self {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            n_events: 0,
            ofi_sum: 0.0,
            churn_bid: 0.0,
            churn_ask: 0.0,
            open_px: None,
            mid_sum: 0.0,
            mid_sq_sum: 0.0,
            window_start_us: now_us,
        }
    }

    /// Feed one diff event into the accumulator.
    pub fn on_diff(&mut self, book: &OrderBook, applied: &DiffApplied) {
        self.n_events += 1;
        self.ofi_sum += applied.ofi_l1_delta;
        self.churn_bid += applied.bid_abs_change;
        self.churn_ask += applied.ask_abs_change;

        if let Some(mid) = book.mid_price() {
            if self.open_px.is_none() {
                self.open_px = Some(mid);
            }
            self.mid_sum += mid;
            self.mid_sq_sum += mid * mid;
        }
    }

    /// Flush accumulated state into a Snapshot1s and reset per-window counters.
    pub fn flush(&mut self, book: &OrderBook, now_us: i64) -> Snapshot1s {
        let n = self.n_events;
        let n_f64 = n as f64;

        // intra_sigma = sqrt(E[mid²] - E[mid]²)
        let intra_sigma = if n > 1 {
            let mean = self.mid_sum / n_f64;
            let variance = (self.mid_sq_sum / n_f64 - mean * mean).max(0.0);
            variance.sqrt() as f32
        } else {
            0.0_f32
        };

        let close_px = book.mid_price();
        let bids = book.bids_top_n(10);
        let asks = book.asks_top_n(10);
        let (bid_depth_l5, ask_depth_l5) = book.depth(5);
        let (bid_depth_l10, ask_depth_l10) = book.depth(10);

        let snap = Snapshot1s {
            ts_us: now_us,
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            bids,
            asks,
            mid_px: book.mid_price(),
            microprice: book.microprice(),
            spread_bps: book.spread_bps(),
            imbalance_l1: book.imbalance(1),
            imbalance_l5: book.imbalance(5),
            imbalance_l10: book.imbalance(10),
            bid_depth_l5,
            bid_depth_l10,
            ask_depth_l5,
            ask_depth_l10,
            ofi_l1: self.ofi_sum,
            churn_bid: self.churn_bid,
            churn_ask: self.churn_ask,
            intra_sigma,
            open_px: self.open_px,
            close_px,
            n_events: n,
        };

        // Reset per-window state (carry-over intentionally left)
        self.n_events = 0;
        self.ofi_sum = 0.0;
        self.churn_bid = 0.0;
        self.churn_ask = 0.0;
        self.open_px = None;
        self.mid_sum = 0.0;
        self.mid_sq_sum = 0.0;
        self.window_start_us = now_us;

        snap
    }
}
