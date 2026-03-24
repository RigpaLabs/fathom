use crate::orderbook::{DiffApplied, OrderBook};

// Re-export shared types from fathom-types crate.
pub use fathom_types::{Levels, Snapshot1s};

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
    /// Running sum of mid prices (for mean). Event-weighted, not time-weighted:
    /// each diff event contributes one sample regardless of inter-event timing.
    mid_sum: f64,
    /// Running sum of mid² (for population variance via E[x²] - E[x]²)
    mid_sq_sum: f64,
    window_start_us: i64,
    // ── Trade accumulators ───────────────────────────────────────────────
    volume_delta: f64,
    buy_vol: f64,
    sell_vol: f64,
    trade_count: u32,
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
            volume_delta: 0.0,
            buy_vol: 0.0,
            sell_vol: 0.0,
            trade_count: 0,
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

    /// Feed one trade event into the accumulator.
    pub fn accumulate_trade(&mut self, size: f64, is_buy: bool) {
        self.trade_count += 1;
        if is_buy {
            self.buy_vol += size;
            self.volume_delta += size;
        } else {
            self.sell_vol += size;
            self.volume_delta -= size;
        }
    }

    /// Feed one snapshot-derived diff into the accumulator (HL/dYdX path).
    /// Uses raw best bid/ask prices instead of an OrderBook reference.
    pub fn on_diff_from_levels(
        &mut self,
        best_bid_px: Option<f64>,
        best_ask_px: Option<f64>,
        applied: &DiffApplied,
    ) {
        self.n_events += 1;
        self.ofi_sum += applied.ofi_l1_delta;
        self.churn_bid += applied.bid_abs_change;
        self.churn_ask += applied.ask_abs_change;

        if let (Some(bid), Some(ask)) = (best_bid_px, best_ask_px) {
            let mid = (bid + ask) / 2.0;
            if self.open_px.is_none() {
                self.open_px = Some(mid);
            }
            self.mid_sum += mid;
            self.mid_sq_sum += mid * mid;
        }
    }

    /// Flush accumulated state into a Snapshot1s using raw level data (HL/dYdX path).
    /// `levels` is `Some((bids, asks))` where bids are descending and asks ascending.
    pub fn flush_with_levels(
        &mut self,
        levels: Option<(&Levels, &Levels)>,
        now_us: i64,
    ) -> Snapshot1s {
        let n = self.n_events;
        let n_f64 = n as f64;

        // intra_sigma: absolute mid-price dispersion (USD), NOT returns-based volatility.
        // See flush() for full doc comment.
        let intra_sigma = if n > 1 {
            let mean = self.mid_sum / n_f64;
            let variance = (self.mid_sq_sum / n_f64 - mean * mean).max(0.0);
            variance.sqrt() as f32
        } else {
            0.0_f32
        };

        let (
            bids,
            asks,
            mid_px,
            microprice,
            spread_bps,
            imb_l1,
            imb_l5,
            imb_l10,
            bid_d5,
            bid_d10,
            ask_d5,
            ask_d10,
        ) = if let Some((b, a)) = levels {
            let mid = match (b.first(), a.first()) {
                (Some((bp, _)), Some((ap, _))) => Some((bp + ap) / 2.0),
                _ => None,
            };
            let micro = match (b.first(), a.first()) {
                (Some((bp, bq)), Some((ap, aq))) => {
                    let total = bq + aq;
                    if total > 0.0 {
                        Some((bp * aq + ap * bq) / total)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            let spread = match (b.first(), a.first()) {
                (Some((bp, _)), Some((ap, _))) => {
                    let m = (bp + ap) / 2.0;
                    if m > 0.0 {
                        Some(((ap - bp) / m * 10_000.0) as f32)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            let imbalance = |n: usize| -> Option<f32> {
                let bd: f64 = b.iter().take(n).map(|(_, q)| q).sum();
                let ad: f64 = a.iter().take(n).map(|(_, q)| q).sum();
                let total = bd + ad;
                if total > 0.0 {
                    Some(((bd - ad) / total) as f32)
                } else {
                    None
                }
            };
            let depth = |n: usize| -> (f64, f64) {
                let bd: f64 = b.iter().take(n).map(|(_, q)| q).sum();
                let ad: f64 = a.iter().take(n).map(|(_, q)| q).sum();
                (bd, ad)
            };
            let (bd5, ad5) = depth(5);
            let (bd10, ad10) = depth(10);
            (
                b.clone(),
                a.clone(),
                mid,
                micro,
                spread,
                imbalance(1),
                imbalance(5),
                imbalance(10),
                bd5,
                bd10,
                ad5,
                ad10,
            )
        } else {
            (
                vec![],
                vec![],
                None,
                None,
                None,
                None,
                None,
                None,
                0.0,
                0.0,
                0.0,
                0.0,
            )
        };

        let close_px = mid_px;

        let snap = Snapshot1s {
            ts_us: now_us,
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            bids,
            asks,
            mid_px,
            microprice,
            spread_bps,
            imbalance_l1: imb_l1,
            imbalance_l5: imb_l5,
            imbalance_l10: imb_l10,
            bid_depth_l5: bid_d5,
            bid_depth_l10: bid_d10,
            ask_depth_l5: ask_d5,
            ask_depth_l10: ask_d10,
            ofi_l1: self.ofi_sum,
            churn_bid: self.churn_bid,
            churn_ask: self.churn_ask,
            intra_sigma,
            open_px: self.open_px,
            close_px,
            n_events: n,
            volume_delta: self.volume_delta,
            buy_vol: self.buy_vol,
            sell_vol: self.sell_vol,
            trade_count: self.trade_count,
        };

        self.n_events = 0;
        self.ofi_sum = 0.0;
        self.churn_bid = 0.0;
        self.churn_ask = 0.0;
        self.open_px = None;
        self.mid_sum = 0.0;
        self.mid_sq_sum = 0.0;
        self.window_start_us = now_us;
        self.volume_delta = 0.0;
        self.buy_vol = 0.0;
        self.sell_vol = 0.0;
        self.trade_count = 0;

        snap
    }

    /// Flush accumulated state into a Snapshot1s and reset per-window counters.
    pub fn flush(&mut self, book: &OrderBook, now_us: i64) -> Snapshot1s {
        let n = self.n_events;
        let n_f64 = n as f64;

        // intra_sigma: absolute mid-price dispersion (USD), NOT returns-based volatility.
        // Formula: sqrt(E[mid²] - E[mid]²) — population stddev of mid prices within the window.
        // Event-weighted: each diff event = one sample. At 1s granularity this is adequate.
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
            volume_delta: self.volume_delta,
            buy_vol: self.buy_vol,
            sell_vol: self.sell_vol,
            trade_count: self.trade_count,
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
        self.volume_delta = 0.0;
        self.buy_vol = 0.0;
        self.sell_vol = 0.0;
        self.trade_count = 0;

        snap
    }
}
