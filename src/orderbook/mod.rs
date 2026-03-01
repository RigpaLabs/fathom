pub mod manager;

use std::collections::{BTreeMap, HashMap};

use ordered_float::OrderedFloat;

use crate::error::{AppError, Result};

/// A single price-level diff from a Binance depthUpdate event.
#[derive(Debug, Clone)]
pub struct DepthDiff {
    pub exchange: String,
    pub symbol: String,
    pub timestamp_us: i64,
    pub seq_id: i64,      // u (final update id)
    pub prev_seq_id: i64, // U (first update id) — used for sync check on first event
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// REST snapshot from /api/v3/depth
#[derive(Debug)]
pub struct SnapshotMsg {
    pub symbol: String,
    pub last_update_id: i64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// Result of applying a diff to the order book.
#[derive(Debug, Default, Clone)]
pub struct DiffApplied {
    /// Signed OFI contribution at best bid (positive = bid pressure, negative = ask pressure)
    pub ofi_l1_delta: f64,
    /// Σ|Δqty| on bid side (for churn)
    pub bid_abs_change: f64,
    /// Σ|Δqty| on ask side (for churn)
    pub ask_abs_change: f64,
}

/// Level-2 order book with Binance sync protocol.
pub struct OrderBook {
    /// Price → quantity, descending iteration (best bid first)
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    /// Price → quantity, ascending iteration (best ask first)
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    /// Last known qty per bid price level — carries over across 1s windows for churn
    bid_last: HashMap<OrderedFloat<f64>, f64>,
    /// Last known qty per ask price level — carries over across 1s windows
    ask_last: HashMap<OrderedFloat<f64>, f64>,
    /// The `u` from the last applied event (or snapshot's lastUpdateId)
    pub last_update_id: i64,
    /// True once we've found the sync event and applied it
    pub synced: bool,
    /// Best bid qty at last OFI calculation point
    prev_best_bid_qty: f64,
    /// Best ask qty at last OFI calculation point
    prev_best_ask_qty: f64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            bid_last: HashMap::new(),
            ask_last: HashMap::new(),
            last_update_id: 0,
            synced: false,
            prev_best_bid_qty: 0.0,
            prev_best_ask_qty: 0.0,
        }
    }

    pub fn apply_snapshot(&mut self, snap: SnapshotMsg) {
        self.bids.clear();
        self.asks.clear();
        self.bid_last.clear();
        self.ask_last.clear();
        for (px, qty) in snap.bids {
            if qty > 0.0 {
                let key = OrderedFloat(px);
                self.bids.insert(key, qty);
                // Seed bid_last so first diff computes |qty_new - qty_snap|, not |qty_new - 0|
                self.bid_last.insert(key, qty);
            }
        }
        for (px, qty) in snap.asks {
            if qty > 0.0 {
                let key = OrderedFloat(px);
                self.asks.insert(key, qty);
                self.ask_last.insert(key, qty);
            }
        }
        self.last_update_id = snap.last_update_id;
        self.synced = false;
        self.prev_best_bid_qty = self.best_bid_qty();
        self.prev_best_ask_qty = self.best_ask_qty();
    }

    /// Apply a diff event. Returns `Err(SnapshotRequired)` on gap.
    /// Returns `Ok(None)` if the event is still pre-sync and was dropped.
    pub fn apply_diff(&mut self, diff: &DepthDiff) -> Result<Option<DiffApplied>> {
        let u = diff.seq_id;      // final_update_id
        let big_u = diff.prev_seq_id; // first_update_id

        if !self.synced {
            // Drop events where u <= last_update_id (already in snapshot)
            if u <= self.last_update_id {
                return Ok(None);
            }
            // Sync: first event where U <= lastUpdateId + 1 <= u
            if big_u > self.last_update_id + 1 {
                // Gap between snapshot and stream — need re-snapshot
                return Err(AppError::SnapshotRequired(diff.symbol.clone()));
            }
            self.synced = true;
        } else {
            // Ongoing: detect gap
            if big_u != self.last_update_id + 1 {
                return Err(AppError::OrderBookGap {
                    expected: self.last_update_id + 1,
                    got: big_u,
                });
            }
        }

        let result = self.apply_levels(diff);
        self.last_update_id = u;
        Ok(Some(result))
    }

    fn apply_levels(&mut self, diff: &DepthDiff) -> DiffApplied {
        let prev_best_bid = self.best_bid_px();
        let prev_best_bid_qty = self.prev_best_bid_qty;
        let prev_best_ask = self.best_ask_px();
        let prev_best_ask_qty = self.prev_best_ask_qty;

        let mut bid_abs_change = 0.0_f64;
        let mut ask_abs_change = 0.0_f64;

        for &(px, qty) in &diff.bids {
            let key = OrderedFloat(px);
            let prev = self.bid_last.get(&key).copied().unwrap_or(0.0);
            bid_abs_change += (qty - prev).abs();
            self.bid_last.insert(key, qty);
            if qty == 0.0 {
                self.bids.remove(&key);
            } else {
                self.bids.insert(key, qty);
            }
        }

        for &(px, qty) in &diff.asks {
            let key = OrderedFloat(px);
            let prev = self.ask_last.get(&key).copied().unwrap_or(0.0);
            ask_abs_change += (qty - prev).abs();
            self.ask_last.insert(key, qty);
            if qty == 0.0 {
                self.asks.remove(&key);
            } else {
                self.asks.insert(key, qty);
            }
        }

        // OFI at L1: change in best bid - change in best ask
        let new_best_bid = self.best_bid_px();
        let new_best_bid_qty = self.best_bid_qty();
        let new_best_ask = self.best_ask_px();
        let new_best_ask_qty = self.best_ask_qty();

        let ofi_bid = if new_best_bid >= prev_best_bid {
            new_best_bid_qty
        } else {
            -prev_best_bid_qty
        };
        let ofi_ask = if new_best_ask <= prev_best_ask {
            new_best_ask_qty
        } else {
            -prev_best_ask_qty
        };
        let ofi_l1_delta = ofi_bid - ofi_ask;

        self.prev_best_bid_qty = new_best_bid_qty;
        self.prev_best_ask_qty = new_best_ask_qty;

        DiffApplied {
            ofi_l1_delta,
            bid_abs_change,
            ask_abs_change,
        }
    }

    // ── Accessors ──────────────────────────────────────────────────────────

    fn best_bid_px(&self) -> f64 {
        self.bids
            .keys()
            .next_back()
            .map(|k| k.0)
            .unwrap_or(f64::NEG_INFINITY)
    }

    fn best_ask_px(&self) -> f64 {
        self.asks
            .keys()
            .next()
            .map(|k| k.0)
            .unwrap_or(f64::INFINITY)
    }

    fn best_bid_qty(&self) -> f64 {
        self.bids.values().next_back().copied().unwrap_or(0.0)
    }

    fn best_ask_qty(&self) -> f64 {
        self.asks.values().next().copied().unwrap_or(0.0)
    }

    pub fn mid_price(&self) -> Option<f64> {
        let bid = self.bids.keys().next_back()?;
        let ask = self.asks.keys().next()?;
        Some((bid.0 + ask.0) / 2.0)
    }

    pub fn microprice(&self) -> Option<f64> {
        let best_bid_px = self.bids.keys().next_back()?.0;
        let best_bid_qty = *self.bids.values().next_back()?;
        let best_ask_px = self.asks.keys().next()?.0;
        let best_ask_qty = *self.asks.values().next()?;
        let total = best_bid_qty + best_ask_qty;
        if total == 0.0 {
            return None;
        }
        Some((best_bid_px * best_ask_qty + best_ask_px * best_bid_qty) / total)
    }

    pub fn spread_bps(&self) -> Option<f32> {
        let bid = self.bids.keys().next_back()?.0;
        let ask = self.asks.keys().next()?.0;
        let mid = (bid + ask) / 2.0;
        if mid == 0.0 {
            return None;
        }
        Some(((ask - bid) / mid * 10_000.0) as f32)
    }

    /// Returns (bid_px_i, bid_sz_i) for top N levels (bids descending)
    pub fn bids_top_n(&self, n: usize) -> Vec<(f64, f64)> {
        self.bids
            .iter()
            .rev()
            .take(n)
            .map(|(k, v)| (k.0, *v))
            .collect()
    }

    /// Returns (ask_px_i, ask_sz_i) for top N levels (asks ascending)
    pub fn asks_top_n(&self, n: usize) -> Vec<(f64, f64)> {
        self.asks
            .iter()
            .take(n)
            .map(|(k, v)| (k.0, *v))
            .collect()
    }

    /// Imbalance at top N levels: (bid_depth - ask_depth) / (bid_depth + ask_depth)
    pub fn imbalance(&self, levels: usize) -> Option<f32> {
        let bid_depth: f64 = self.bids.values().rev().take(levels).sum();
        let ask_depth: f64 = self.asks.values().take(levels).sum();
        let total = bid_depth + ask_depth;
        if total == 0.0 {
            return None;
        }
        Some(((bid_depth - ask_depth) / total) as f32)
    }

    pub fn depth(&self, levels: usize) -> (f64, f64) {
        let bid: f64 = self.bids.values().rev().take(levels).sum();
        let ask: f64 = self.asks.values().take(levels).sum();
        (bid, ask)
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}
