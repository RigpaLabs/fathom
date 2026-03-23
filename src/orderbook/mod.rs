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
    pub prev_seq_id: i64, // U (first update id) — used for initial sync and spot ongoing gap check
    /// Binance USDM Futures only: `pu` field (prev final update id).
    /// When present, the ongoing gap check uses `pu == last_update_id` instead of
    /// the spot rule `U == last_update_id + 1`.
    pub prev_final_update_id: Option<i64>,
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
    /// Returns `Ok(None)` if the event is pre-sync or stale (perp out-of-order).
    pub fn apply_diff(&mut self, diff: &DepthDiff) -> Result<Option<DiffApplied>> {
        let u = diff.seq_id; // final_update_id
        let big_u = diff.prev_seq_id; // first_update_id

        if !self.synced {
            // Drop events where u <= last_update_id (already in snapshot)
            if u <= self.last_update_id {
                return Ok(None);
            }
            // Perp: use pu for initial sync — mirror the ongoing stale-drop logic
            if let Some(pu) = diff.prev_final_update_id {
                if pu > self.last_update_id {
                    return Err(AppError::SnapshotRequired(diff.symbol.clone()));
                }
                if pu < self.last_update_id {
                    return Ok(None); // stale, drop
                }
                // pu == last_update_id: valid sync
            } else {
                // Spot: U <= lastUpdateId + 1 <= u
                if big_u > self.last_update_id + 1 {
                    if self.last_update_id == 0 {
                        // Snapshot was never fetched (fetch failed or skipped) —
                        // need a real reconnect to get a snapshot.
                        return Err(AppError::SnapshotRequired(diff.symbol.clone()));
                    }
                    // Snapshot fetched but WS events moved ahead during fetch.
                    // With parallel snapshot fetch the gap is typically ~3 IDs.
                    // Drop and wait — the bridging event is likely still in the
                    // forwarder buffer from before the snapshot was taken.
                    return Ok(None);
                }
            }
            self.synced = true;
        } else {
            // Ongoing: detect gap.
            // Perp (USDM Futures) events carry `pu` (prev_final_update_id); use it
            // when available.  Spot falls back to `U == prev_u + 1`.
            match diff.prev_final_update_id {
                Some(pu) => {
                    if pu > self.last_update_id {
                        // Genuine gap: we missed events
                        return Err(AppError::OrderBookGap {
                            expected: self.last_update_id,
                            got: pu,
                        });
                    }
                    if pu < self.last_update_id {
                        // Stale/out-of-order event — already applied a later one
                        return Ok(None);
                    }
                    // pu == last_update_id: normal sequence, fall through
                }
                None => {
                    if big_u != self.last_update_id + 1 {
                        return Err(AppError::OrderBookGap {
                            expected: self.last_update_id + 1,
                            got: big_u,
                        });
                    }
                }
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
            if qty == 0.0 {
                self.bid_last.remove(&key);
                self.bids.remove(&key);
            } else {
                self.bid_last.insert(key, qty);
                self.bids.insert(key, qty);
            }
        }

        for &(px, qty) in &diff.asks {
            let key = OrderedFloat(px);
            let prev = self.ask_last.get(&key).copied().unwrap_or(0.0);
            ask_abs_change += (qty - prev).abs();
            if qty == 0.0 {
                self.ask_last.remove(&key);
                self.asks.remove(&key);
            } else {
                self.ask_last.insert(key, qty);
                self.asks.insert(key, qty);
            }
        }

        // OFI at L1 — "level OFI" variant (Cont, Kukanov, Stoikov 2014):
        // When best price improves or holds: ofi_side = new_qty (full qty, not delta).
        // When best price worsens: ofi_side = -prev_qty.
        // This is consistent across Binance, HL, and dYdX paths.
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
        self.asks.iter().take(n).map(|(k, v)| (k.0, *v)).collect()
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

#[cfg(test)]
impl OrderBook {
    fn bid_last_contains(&self, price: f64) -> bool {
        self.bid_last.contains_key(&OrderedFloat(price))
    }

    fn ask_last_contains(&self, price: f64) -> bool {
        self.ask_last.contains_key(&OrderedFloat(price))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn snap(bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> SnapshotMsg {
        SnapshotMsg {
            symbol: "ETHUSDT".to_string(),
            last_update_id: 100,
            bids,
            asks,
        }
    }

    fn diff(seq: i64, prev_seq: i64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> DepthDiff {
        DepthDiff {
            exchange: "test".to_string(),
            symbol: "ETHUSDT".to_string(),
            timestamp_us: seq * 1_000,
            seq_id: seq,
            prev_seq_id: prev_seq,
            prev_final_update_id: None,
            bids,
            asks,
        }
    }

    /// Binance USDM Futures perp events carry `pu` (prev_final_update_id).
    /// An event where U != prev_u + 1 but pu == prev_u must NOT trigger a gap.
    /// This exercises the perp-specific gap check branch.
    #[test]
    fn test_perp_pu_no_false_gap() {
        let mut book = OrderBook::new();
        // Snapshot: lastUpdateId = 105
        book.apply_snapshot(snap(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]));
        // Override last_update_id (snap() hardcodes 100; set it to 105 via a sync diff)
        // Sync event: U=100 <= 101 <= u=110  (spans multiple IDs — valid sync)
        let sync = DepthDiff {
            exchange: "test".into(),
            symbol: "ETHUSDT".into(),
            timestamp_us: 1_000,
            seq_id: 110,
            prev_seq_id: 100,
            prev_final_update_id: None,
            bids: vec![],
            asks: vec![],
        };
        book.apply_diff(&sync).expect("sync event should apply");
        assert_eq!(book.last_update_id, 110);

        // Perp next event: U=106 (not 111!), u=115, pu=110
        // Spot rule: U(106) != 110+1=111 → would be a false gap
        // Perp rule: pu(110) == last_update_id(110) → no gap
        let perp_event = DepthDiff {
            exchange: "test".into(),
            symbol: "ETHUSDT".into(),
            timestamp_us: 2_000,
            seq_id: 115,
            prev_seq_id: 106,
            prev_final_update_id: Some(110),
            bids: vec![],
            asks: vec![],
        };
        assert!(
            book.apply_diff(&perp_event).is_ok(),
            "perp event with pu matching last_update_id must not trigger a gap"
        );
        assert_eq!(book.last_update_id, 115);
    }

    #[test]
    fn test_removed_bid_level_not_retained_in_bid_last() {
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]));
        assert!(
            book.bid_last_contains(3000.0),
            "snapshot should seed bid_last"
        );

        book.apply_diff(&diff(101, 100, vec![(3000.0, 0.0)], vec![]))
            .unwrap();

        assert!(
            !book.bid_last_contains(3000.0),
            "removed bid level must not leave a tombstone in bid_last"
        );
    }

    #[test]
    fn test_removed_ask_level_not_retained_in_ask_last() {
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]));
        assert!(
            book.ask_last_contains(3001.0),
            "snapshot should seed ask_last"
        );

        book.apply_diff(&diff(101, 100, vec![], vec![(3001.0, 0.0)]))
            .unwrap();

        assert!(
            !book.ask_last_contains(3001.0),
            "removed ask level must not leave a tombstone in ask_last"
        );
    }

    #[test]
    fn test_perp_stale_event_dropped_not_gap() {
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]));

        // Sync event: U=100 <= 101 <= u=110
        let sync = DepthDiff {
            exchange: "test".into(),
            symbol: "ETHUSDT".into(),
            timestamp_us: 1_000,
            seq_id: 110,
            prev_seq_id: 100,
            prev_final_update_id: None,
            bids: vec![],
            asks: vec![],
        };
        book.apply_diff(&sync).unwrap();
        assert_eq!(book.last_update_id, 110);

        // Normal perp event: pu=110 matches last_update_id
        let event1 = DepthDiff {
            exchange: "test".into(),
            symbol: "ETHUSDT".into(),
            timestamp_us: 2_000,
            seq_id: 115,
            prev_seq_id: 111,
            prev_final_update_id: Some(110),
            bids: vec![],
            asks: vec![],
        };
        book.apply_diff(&event1).unwrap();
        assert_eq!(book.last_update_id, 115);

        // Stale event: pu=110 < last_update_id=115 → should be dropped, NOT gap
        let stale = DepthDiff {
            exchange: "test".into(),
            symbol: "ETHUSDT".into(),
            timestamp_us: 3_000,
            seq_id: 113,
            prev_seq_id: 111,
            prev_final_update_id: Some(110),
            bids: vec![(3000.0, 6.0)],
            asks: vec![],
        };
        let result = book.apply_diff(&stale);
        assert!(
            result.is_ok(),
            "stale perp event must not trigger gap error"
        );
        assert!(
            result.unwrap().is_none(),
            "stale perp event must return None (dropped)"
        );
        assert_eq!(book.last_update_id, 115, "last_update_id must not regress");
    }

    #[test]
    fn test_perp_genuine_gap_still_detected() {
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]));

        let sync = DepthDiff {
            exchange: "test".into(),
            symbol: "ETHUSDT".into(),
            timestamp_us: 1_000,
            seq_id: 110,
            prev_seq_id: 100,
            prev_final_update_id: None,
            bids: vec![],
            asks: vec![],
        };
        book.apply_diff(&sync).unwrap();

        // Genuine gap: pu=120 > last_update_id=110 → missed events
        let gap_event = DepthDiff {
            exchange: "test".into(),
            symbol: "ETHUSDT".into(),
            timestamp_us: 2_000,
            seq_id: 125,
            prev_seq_id: 121,
            prev_final_update_id: Some(120),
            bids: vec![],
            asks: vec![],
        };
        let result = book.apply_diff(&gap_event);
        assert!(result.is_err(), "genuine perp gap must still trigger error");
    }

    /// Spot initial sync: when snapshot was fetched (last_update_id > 0) but
    /// WS events raced ahead, drop the event instead of reconnecting.
    #[test]
    fn test_spot_initial_sync_drops_ahead_events() {
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]));
        assert_eq!(book.last_update_id, 100);

        // Event with U=105 > lastUpdateId+1=101 — ahead of snapshot
        let ahead = diff(110, 105, vec![], vec![]);
        let result = book.apply_diff(&ahead);
        assert!(
            result.is_ok(),
            "ahead event must not trigger SnapshotRequired"
        );
        assert!(
            result.unwrap().is_none(),
            "ahead event must return None (dropped)"
        );
        assert!(!book.synced, "book must remain unsynced after dropping");

        // Bridge event arrives: U=100 <= 101 <= u=103
        let bridge = diff(103, 100, vec![(3000.0, 6.0)], vec![]);
        let result = book.apply_diff(&bridge);
        assert!(
            result.unwrap().is_some(),
            "bridge event must sync and apply"
        );
        assert!(book.synced, "book must be synced after bridge event");
    }

    /// Spot initial sync: when snapshot was NEVER fetched (last_update_id=0),
    /// SnapshotRequired must still fire to trigger a reconnect.
    #[test]
    fn test_spot_initial_sync_snapshot_required_when_never_fetched() {
        let mut book = OrderBook::new();
        assert_eq!(book.last_update_id, 0);

        // Any event with U > 1 should trigger SnapshotRequired
        let event = diff(110, 105, vec![], vec![]);
        let result = book.apply_diff(&event);
        assert!(
            result.is_err(),
            "must return SnapshotRequired when snapshot was never fetched"
        );
    }
}
