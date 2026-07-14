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

/// Levels kept per side after a prune (nearest the mid). We only ever read the
/// top ~10 (snapshot columns, OFI, microprice), so 1000 is ~100× headroom —
/// pruning never touches live near-mid state.
const BOOK_LEVEL_CAP: usize = 1000;
/// Prune only once a side exceeds this. The gap between it and `BOOK_LEVEL_CAP`
/// is hysteresis: it amortizes the O(n) prune walk over ~200 events instead of
/// firing on every near-mid insert while the book sits full.
const BOOK_PRUNE_TRIGGER: usize = 1200;

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
    /// True once a REST snapshot has been applied — guards against processing
    /// diffs for symbols whose snapshot fetch failed.
    pub has_snapshot: bool,
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
            has_snapshot: false,
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
        self.prune_far_levels();
        self.last_update_id = snap.last_update_id;
        self.has_snapshot = true;
        self.synced = false;
        self.prev_best_bid_qty = self.best_bid_qty();
        self.prev_best_ask_qty = self.best_ask_qty();
    }

    /// Keep only the `BOOK_LEVEL_CAP` levels nearest the mid on each side,
    /// dropping far-from-mid levels. Binance/Bybit only send an explicit 0-qty
    /// diff for levels inside their active window, so a level that drifts out of
    /// range is never removed — a long-lived book (Binance perp resyncs ~once per
    /// 6 days) accumulates stale deep levels unboundedly (observed ~800 MB anon
    /// RSS over 6 days). Near-mid metrics stay exact — we emit top-10 only and the
    /// cap is ~100× deeper than any level read (best/OFI/microprice/imbalance/
    /// depth L5/L10), and full depth lives in the raw-diff stream, not this book.
    /// The one caveat: a pruned level that later re-enters loses its remembered
    /// qty, so full-book churn (`bid_abs_change`) is then measured from a zero
    /// baseline — deep-book noise dwarfed by near-mid churn, accepted as such.
    /// `bid_last`/`ask_last` share the book's exact key set, so the same dropped
    /// keys are removed from them to keep the mirror in sync.
    fn prune_far_levels(&mut self) {
        if self.bids.len() > BOOK_PRUNE_TRIGGER {
            // Best bid = highest price; keep the highest CAP, drop the lowest.
            if let Some(&cutoff) = self.bids.keys().nth_back(BOOK_LEVEL_CAP - 1) {
                let kept = self.bids.split_off(&cutoff); // keys >= cutoff (near mid)
                let dropped = std::mem::replace(&mut self.bids, kept);
                for k in dropped.keys() {
                    self.bid_last.remove(k);
                }
            }
        }
        if self.asks.len() > BOOK_PRUNE_TRIGGER {
            // Best ask = lowest price; keep the lowest CAP, drop the highest.
            if let Some(&cutoff) = self.asks.keys().nth(BOOK_LEVEL_CAP) {
                let dropped = self.asks.split_off(&cutoff); // keys >= cutoff (far)
                for k in dropped.keys() {
                    self.ask_last.remove(k);
                }
            }
        }
    }

    /// Apply a diff event. Returns `Err(SnapshotRequired)` on gap.
    /// Returns `Ok(None)` if the event is pre-sync or stale (perp out-of-order).
    pub fn apply_diff(&mut self, diff: &DepthDiff) -> Result<Option<DiffApplied>> {
        let u = diff.seq_id; // final_update_id
        let big_u = diff.prev_seq_id; // first_update_id

        if !self.synced {
            if !self.has_snapshot {
                return Err(AppError::SnapshotRequired(diff.symbol.clone()));
            }
            // Drop events where u <= last_update_id (already in snapshot)
            if u <= self.last_update_id {
                return Ok(None);
            }
            // Perp: accept any event with pu during initial sync.
            // pu chain guarantees consistency — snapshot may have aged but
            // the event stream is valid from this point forward.
            if let Some(_pu) = diff.prev_final_update_id {
                // Always accept — fall through to sync
            } else {
                // Spot: U <= lastUpdateId+1 <= u required for bridging
                if big_u > self.last_update_id + 1 {
                    return Err(AppError::SnapshotRequired(diff.symbol.clone()));
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
            // Exact float comparison: Binance sends literal "0.00000000" for removed
            // levels, which parses to exact 0.0. No epsilon needed here.
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

        self.prune_far_levels();

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

    fn bids_len(&self) -> usize {
        self.bids.len()
    }

    fn asks_len(&self) -> usize {
        self.asks.len()
    }

    fn bid_last_len(&self) -> usize {
        self.bid_last.len()
    }

    fn ask_last_len(&self) -> usize {
        self.ask_last.len()
    }

    fn bids_contains(&self, price: f64) -> bool {
        self.bids.contains_key(&OrderedFloat(price))
    }

    fn asks_contains(&self, price: f64) -> bool {
        self.asks.contains_key(&OrderedFloat(price))
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

    #[test]
    fn test_snapshot_caps_levels_and_keeps_near_mid() {
        // Snapshot far wider than the cap on both sides.
        let bids: Vec<(f64, f64)> = (1..=1300).map(|i| (i as f64, 1.0)).collect();
        let asks: Vec<(f64, f64)> = (2000..=3300).map(|i| (i as f64, 1.0)).collect();
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(bids, asks));

        // Exact retained window, not just a count — catches off-by-one in the
        // split_off boundary. bids 1..=1300 keep the highest 1000 → 301..=1300;
        // asks 2000..=3300 keep the lowest 1000 → 2000..=2999.
        assert_eq!(book.bids_len(), BOOK_LEVEL_CAP, "bids pruned to exact cap");
        assert_eq!(book.asks_len(), BOOK_LEVEL_CAP, "asks pruned to exact cap");
        assert!(book.bids_contains(301.0), "lowest kept bid retained");
        assert!(book.bids_contains(1300.0), "best bid retained");
        assert!(!book.bids_contains(300.0), "far bid dropped at boundary");
        assert!(!book.bids_contains(1.0), "farthest bid dropped");
        assert!(book.asks_contains(2000.0), "best ask retained");
        assert!(book.asks_contains(2999.0), "highest kept ask retained");
        assert!(!book.asks_contains(3000.0), "far ask dropped at boundary");
        assert!(!book.asks_contains(3300.0), "farthest ask dropped");
        // Best levels (nearest the mid) survive: highest bid, lowest ask.
        assert_eq!(book.best_bid_px(), 1300.0, "best bid preserved after prune");
        assert_eq!(book.best_ask_px(), 2000.0, "best ask preserved after prune");
        // _last maps mirror the book exactly — no independent leak.
        assert_eq!(book.bid_last_len(), book.bids_len());
        assert_eq!(book.ask_last_len(), book.asks_len());
    }

    #[test]
    fn test_churn_baseline_lost_for_pruned_level_reentry() {
        // Documents an accepted limitation (not a bug): a level pruned as
        // far-from-mid loses its remembered qty, so if it later re-enters, churn
        // (bid_abs_change) is measured against a zero baseline instead of the
        // pre-prune qty. This only affects levels >1000 ranks deep — deep-book
        // noise dwarfed by near-mid churn. Near-mid metrics stay exact.
        let bids: Vec<(f64, f64)> = (100..=1400).map(|i| (i as f64, 5.0)).collect();
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(bids, vec![]));
        // 200.0 was pruned (kept range is 401..=1400).
        assert!(!book.bids_contains(200.0), "level 200 was pruned");

        // Re-add 200.0 at qty 7 via a sync diff. Correct churn would be |7-5|=2,
        // but the baseline is gone → reported as |7-0|=7.
        let applied = book
            .apply_diff(&diff(101, 100, vec![(200.0, 7.0)], vec![]))
            .unwrap()
            .unwrap();
        assert_eq!(
            applied.bid_abs_change, 7.0,
            "pruned-level re-entry uses zero churn baseline (accepted limitation)"
        );
    }

    #[test]
    fn test_apply_diff_caps_far_levels() {
        // Reproduces the prod leak: a synced book fed levels far from mid that
        // never receive an explicit 0-removal must not grow past the cap.
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(vec![(1000.0, 5.0)], vec![(5000.0, 5.0)]));
        let far_bids: Vec<(f64, f64)> = (1..=1300).map(|i| (i as f64, 1.0)).collect();
        // Sync diff also carries levels → applies + prunes in one shot.
        book.apply_diff(&diff(101, 100, far_bids, vec![])).unwrap();

        assert!(book.bids_len() <= BOOK_LEVEL_CAP, "far bids pruned");
        assert_eq!(
            book.bid_last_len(),
            book.bids_len(),
            "bid_last mirrors bids after prune"
        );
        assert_eq!(book.best_bid_px(), 1300.0, "near-mid best preserved");
    }

    #[test]
    fn test_prune_leaves_small_book_untouched() {
        // Below the trigger: nothing is pruned, exact levels retained.
        let bids: Vec<(f64, f64)> = (1..=50).map(|i| (i as f64, 1.0)).collect();
        let asks: Vec<(f64, f64)> = (100..=150).map(|i| (i as f64, 1.0)).collect();
        let mut book = OrderBook::new();
        book.apply_snapshot(snap(bids, asks));
        assert_eq!(book.bids_len(), 50);
        assert_eq!(book.asks_len(), 51);
    }
}
