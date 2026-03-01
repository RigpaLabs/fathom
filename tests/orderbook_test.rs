use fathom::orderbook::{DepthDiff, OrderBook, SnapshotMsg};

fn make_diff(symbol: &str, first_id: i64, final_id: i64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> DepthDiff {
    DepthDiff {
        exchange: "binance_spot".to_string(),
        symbol: symbol.to_string(),
        timestamp_us: 1_000_000,
        seq_id: final_id,
        prev_seq_id: first_id,
        bids,
        asks,
    }
}

fn snapshot(last_id: i64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> SnapshotMsg {
    SnapshotMsg {
        symbol: "ETHUSDT".to_string(),
        last_update_id: last_id,
        bids,
        asks,
    }
}

/// Convenience: snapshot + sync diff, returns a synced book.
fn synced_book(bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> OrderBook {
    let mut book = OrderBook::new();
    book.apply_snapshot(snapshot(100, bids, asks));
    // Sync diff: U=100, u=101
    let _ = book.apply_diff(&make_diff("ETHUSDT", 100, 101, vec![], vec![]));
    book
}

// ── Sync protocol ─────────────────────────────────────────────────────────────

#[test]
fn test_apply_snapshot_and_diff() {
    let mut book = OrderBook::new();

    book.apply_snapshot(snapshot(
        100,
        vec![(3000.0, 1.0), (2999.0, 2.0)],
        vec![(3001.0, 1.5), (3002.0, 0.5)],
    ));

    assert_eq!(book.last_update_id, 100);
    assert!(!book.synced);

    // Pre-sync diff: should be dropped (u <= last_update_id)
    let dropped = book.apply_diff(&make_diff("ETHUSDT", 90, 100, vec![], vec![])).unwrap();
    assert!(dropped.is_none());

    // Sync event: U <= 101 <= u
    let applied = book.apply_diff(&make_diff("ETHUSDT", 100, 102, vec![(3000.0, 1.5)], vec![])).unwrap();
    assert!(applied.is_some());
    assert!(book.synced);
    assert_eq!(book.last_update_id, 102);
}

#[test]
fn test_multiple_pre_sync_drops() {
    let mut book = OrderBook::new();
    book.apply_snapshot(snapshot(100, vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]));

    // All these are pre-snapshot — drop them
    for u in [50, 80, 99, 100] {
        let r = book.apply_diff(&make_diff("ETHUSDT", u - 1, u, vec![], vec![])).unwrap();
        assert!(r.is_none(), "u={u} should be dropped");
    }
}

// ── Zero-qty removal ──────────────────────────────────────────────────────────

#[test]
fn test_remove_zero_qty_level() {
    let mut book = OrderBook::new();
    book.apply_snapshot(snapshot(
        100,
        vec![(3000.0, 1.0), (2999.0, 2.0)],
        vec![(3001.0, 1.5)],
    ));

    let _ = book.apply_diff(&make_diff("ETHUSDT", 100, 101, vec![], vec![]));
    let _ = book.apply_diff(&make_diff("ETHUSDT", 102, 103, vec![(3000.0, 0.0)], vec![]));

    let bids = book.bids_top_n(5);
    assert!(!bids.iter().any(|(px, _)| *px == 3000.0), "zero-qty level should be removed");
    assert!(bids.iter().any(|(px, _)| *px == 2999.0));
}

#[test]
fn test_remove_zero_qty_nonexistent_level_is_noop() {
    let mut book = synced_book(vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]);
    // Remove a price that doesn't exist — should not panic or change anything
    let _ = book.apply_diff(&make_diff("ETHUSDT", 102, 103, vec![(2500.0, 0.0)], vec![]));
    assert_eq!(book.bids_top_n(5).len(), 1);
}

// ── Gap detection ─────────────────────────────────────────────────────────────

#[test]
fn test_gap_detection() {
    let mut book = OrderBook::new();
    book.apply_snapshot(snapshot(100, vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]));

    let _ = book.apply_diff(&make_diff("ETHUSDT", 100, 101, vec![], vec![]));

    // Gap: expected U=102, got U=103
    let result = book.apply_diff(&make_diff("ETHUSDT", 103, 104, vec![], vec![]));
    assert!(matches!(result, Err(fathom::error::AppError::OrderBookGap { expected: 102, got: 103 })));
}

#[test]
fn test_pre_snapshot_gap_requires_resnapshot() {
    let mut book = OrderBook::new();
    book.apply_snapshot(snapshot(100, vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]));

    let result = book.apply_diff(&make_diff("ETHUSDT", 103, 105, vec![], vec![]));
    assert!(matches!(result, Err(fathom::error::AppError::SnapshotRequired(_))));
}

// ── Empty book edge cases ─────────────────────────────────────────────────────

#[test]
fn test_empty_book_mid_price_is_none() {
    let book = OrderBook::new();
    assert!(book.mid_price().is_none());
}

#[test]
fn test_empty_book_microprice_is_none() {
    let book = OrderBook::new();
    assert!(book.microprice().is_none());
}

#[test]
fn test_empty_book_spread_bps_is_none() {
    let book = OrderBook::new();
    assert!(book.spread_bps().is_none());
}

#[test]
fn test_empty_book_imbalance_is_none() {
    let book = OrderBook::new();
    assert!(book.imbalance(1).is_none());
}

#[test]
fn test_empty_snapshot_levels() {
    let mut book = OrderBook::new();
    book.apply_snapshot(snapshot(100, vec![], vec![]));
    assert!(book.mid_price().is_none());
    assert_eq!(book.bids_top_n(10).len(), 0);
    assert_eq!(book.asks_top_n(10).len(), 0);
}

// ── Mid price ─────────────────────────────────────────────────────────────────

#[test]
fn test_mid_price() {
    let mut book = OrderBook::new();
    book.apply_snapshot(snapshot(100, vec![(3000.0, 1.0)], vec![(3002.0, 1.0)]));
    assert_eq!(book.mid_price(), Some(3001.0));
}

#[test]
fn test_mid_price_uses_best_levels_only() {
    let book = synced_book(
        vec![(3000.0, 1.0), (2990.0, 5.0)],
        vec![(3001.0, 1.0), (3010.0, 5.0)],
    );
    // Mid should be (3000 + 3001) / 2, not affected by deeper levels
    assert_eq!(book.mid_price(), Some(3000.5));
}

// ── Microprice ────────────────────────────────────────────────────────────────

#[test]
fn test_microprice_equal_sizes() {
    let book = synced_book(vec![(3000.0, 1.0)], vec![(3002.0, 1.0)]);
    // Equal bid/ask sizes → microprice == mid
    let mp = book.microprice().unwrap();
    assert!((mp - 3001.0).abs() < 1e-9, "microprice={mp}");
}

#[test]
fn test_microprice_large_bid() {
    // Big bid → microprice closer to ask
    let book = synced_book(vec![(3000.0, 9.0)], vec![(3002.0, 1.0)]);
    let mp = book.microprice().unwrap();
    assert!(mp > 3001.0, "large bid → microprice should be above mid, got {mp}");
    assert!(mp < 3002.0, "microprice should not exceed ask");
}

#[test]
fn test_microprice_large_ask() {
    // Big ask → microprice closer to bid
    let book = synced_book(vec![(3000.0, 1.0)], vec![(3002.0, 9.0)]);
    let mp = book.microprice().unwrap();
    assert!(mp < 3001.0, "large ask → microprice should be below mid, got {mp}");
    assert!(mp > 3000.0);
}

// ── Spread ────────────────────────────────────────────────────────────────────

#[test]
fn test_spread_bps() {
    // bid=3000, ask=3001, mid=3000.5 → spread_bps = 1/3000.5 * 10000 ≈ 3.33
    let book = synced_book(vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]);
    let spread = book.spread_bps().unwrap();
    let expected = (1.0 / 3000.5 * 10_000.0) as f32;
    assert!((spread - expected).abs() < 0.01, "spread_bps={spread}, expected≈{expected}");
}

#[test]
fn test_spread_bps_wider() {
    // Wider spread → larger bps
    let book1 = synced_book(vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]);
    let book2 = synced_book(vec![(3000.0, 1.0)], vec![(3010.0, 1.0)]);
    assert!(book2.spread_bps().unwrap() > book1.spread_bps().unwrap());
}

// ── Depth ─────────────────────────────────────────────────────────────────────

#[test]
fn test_depth_single_level() {
    let book = synced_book(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]);
    let (bid, ask) = book.depth(1);
    assert_eq!(bid, 5.0);
    assert_eq!(ask, 4.0);
}

#[test]
fn test_depth_sum_multiple_levels() {
    let book = synced_book(
        vec![(3000.0, 1.0), (2999.0, 2.0), (2998.0, 3.0)],
        vec![(3001.0, 1.0), (3002.0, 2.0)],
    );
    let (bid5, _) = book.depth(5);
    assert_eq!(bid5, 6.0, "sum of all 3 bid levels");
}

#[test]
fn test_depth_fewer_levels_than_requested() {
    // Only 2 levels but asking for 10 — should sum what's there
    let book = synced_book(vec![(3000.0, 5.0), (2999.0, 3.0)], vec![(3001.0, 4.0)]);
    let (bid, _) = book.depth(10);
    assert_eq!(bid, 8.0, "sum of available levels: 5+3");
}

// ── Imbalance ─────────────────────────────────────────────────────────────────

#[test]
fn test_imbalance_equal() {
    let book = synced_book(vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]);
    let imb = book.imbalance(1).unwrap();
    assert!((imb - 0.0).abs() < 1e-6);
}

#[test]
fn test_imbalance_bid_heavy() {
    let book = synced_book(vec![(3000.0, 9.0)], vec![(3001.0, 1.0)]);
    let imb = book.imbalance(1).unwrap();
    assert!(imb > 0.0, "bid-heavy should have positive imbalance");
    assert!((imb - 0.8_f32).abs() < 0.001);
}

#[test]
fn test_imbalance_ask_heavy() {
    let book = synced_book(vec![(3000.0, 1.0)], vec![(3001.0, 9.0)]);
    let imb = book.imbalance(1).unwrap();
    assert!(imb < 0.0, "ask-heavy should have negative imbalance");
}

// ── bids_top_n / asks_top_n ordering ─────────────────────────────────────────

#[test]
fn test_bids_top_n_descending_order() {
    let book = synced_book(
        vec![(2990.0, 1.0), (3000.0, 2.0), (2995.0, 3.0)],
        vec![(3001.0, 1.0)],
    );
    let bids = book.bids_top_n(3);
    assert_eq!(bids.len(), 3);
    assert!(bids[0].0 > bids[1].0, "bids should be descending");
    assert!(bids[1].0 > bids[2].0);
    assert_eq!(bids[0].0, 3000.0);
}

#[test]
fn test_asks_top_n_ascending_order() {
    let book = synced_book(
        vec![(3000.0, 1.0)],
        vec![(3010.0, 1.0), (3001.0, 2.0), (3005.0, 3.0)],
    );
    let asks = book.asks_top_n(3);
    assert_eq!(asks.len(), 3);
    assert!(asks[0].0 < asks[1].0, "asks should be ascending");
    assert_eq!(asks[0].0, 3001.0);
}

#[test]
fn test_top_n_fewer_than_n_levels() {
    let book = synced_book(vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]);
    let bids = book.bids_top_n(10);
    assert_eq!(bids.len(), 1, "only 1 level available");
}

// ── OFI via DiffApplied ───────────────────────────────────────────────────────

#[test]
fn test_ofi_bid_pressure_increases_qty() {
    let mut book = synced_book(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]);
    // Best bid increases qty → positive OFI
    let applied = book
        .apply_diff(&make_diff("ETHUSDT", 102, 103, vec![(3000.0, 8.0)], vec![]))
        .unwrap()
        .unwrap();
    assert!(applied.ofi_l1_delta > 0.0, "bid qty increase → positive OFI delta");
}

#[test]
fn test_bid_abs_change_matches_delta() {
    let mut book = synced_book(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]);
    let applied = book
        .apply_diff(&make_diff("ETHUSDT", 102, 103, vec![(3000.0, 8.0)], vec![]))
        .unwrap()
        .unwrap();
    // |8 - 5| = 3
    assert!((applied.bid_abs_change - 3.0).abs() < 1e-9);
}

#[test]
fn test_churn_counts_removals() {
    let mut book = synced_book(vec![(3000.0, 5.0)], vec![(3001.0, 4.0)]);
    let applied = book
        .apply_diff(&make_diff("ETHUSDT", 102, 103, vec![(3000.0, 0.0)], vec![]))
        .unwrap()
        .unwrap();
    // |0 - 5| = 5
    assert!((applied.bid_abs_change - 5.0).abs() < 1e-9);
}

// ── Snapshot re-apply resets state ───────────────────────────────────────────

#[test]
fn test_re_snapshot_resets_sync() {
    let mut book = synced_book(vec![(3000.0, 1.0)], vec![(3001.0, 1.0)]);
    assert!(book.synced);

    // Re-snapshot after gap
    book.apply_snapshot(snapshot(200, vec![(3001.0, 2.0)], vec![(3002.0, 1.0)]));
    assert!(!book.synced, "snapshot should reset synced flag");
    assert_eq!(book.last_update_id, 200);
}
