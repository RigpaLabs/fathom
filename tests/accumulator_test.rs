use fathom::{
    accumulator::WindowAccumulator,
    orderbook::{DepthDiff, DiffApplied, OrderBook, SnapshotMsg},
};

fn make_book_synced() -> OrderBook {
    let mut book = OrderBook::new();
    book.apply_snapshot(SnapshotMsg {
        symbol: "ETHUSDT".to_string(),
        last_update_id: 100,
        bids: vec![(3000.0, 5.0), (2999.0, 3.0)],
        asks: vec![(3001.0, 4.0), (3002.0, 2.0)],
    });
    // Sync
    let diff = DepthDiff {
        exchange: "test".to_string(),
        symbol: "ETHUSDT".to_string(),
        timestamp_us: 1_000_000,
        seq_id: 101,
        prev_seq_id: 100,
        prev_final_update_id: None,
        bids: vec![],
        asks: vec![],
    };
    let _ = book.apply_diff(&diff);
    book
}

#[test]
fn test_flush_n_events() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    // Apply a diff that changes bid qty at best price
    let diff = DepthDiff {
        exchange: "test".to_string(),
        symbol: "ETHUSDT".to_string(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![(3000.0, 6.0)],
        asks: vec![],
    };
    let applied = book.apply_diff(&diff).unwrap().unwrap();
    acc.on_diff(&book, &applied);

    let snap = acc.flush(&book, 2_000_000);
    assert_eq!(snap.n_events, 1);
    assert!(snap.mid_px.is_some());
    assert!(
        snap.churn_bid > 0.0,
        "churn_bid should reflect qty change: |6-5|=1"
    );
}

#[test]
fn test_bid_last_carry_over() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    // First window: change bid qty 5 → 7
    let diff1 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![(3000.0, 7.0)],
        asks: vec![],
    };
    let applied1 = book.apply_diff(&diff1).unwrap().unwrap();
    acc.on_diff(&book, &applied1);
    // churn_bid = |7 - 5| = 2
    assert!((acc.flush(&book, 2_000_000).churn_bid - 2.0).abs() < 1e-10);

    // Second window: change same level 7 → 9 (should use carry-over bid_last = 7)
    let diff2 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 2_000_000,
        seq_id: 103,
        prev_seq_id: 103,
        prev_final_update_id: None,
        bids: vec![(3000.0, 9.0)],
        asks: vec![],
    };
    let applied2 = book.apply_diff(&diff2).unwrap().unwrap();
    acc.on_diff(&book, &applied2);
    let snap2 = acc.flush(&book, 3_000_000);
    // churn_bid = |9 - 7| = 2, NOT |9 - 0| = 9
    assert!(
        (snap2.churn_bid - 2.0).abs() < 1e-10,
        "carry-over bid_last not working: got {}",
        snap2.churn_bid
    );
}

#[test]
fn test_intra_sigma_zero_single_event() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    let diff = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![],
        asks: vec![],
    };
    let applied = book.apply_diff(&diff).unwrap().unwrap();
    acc.on_diff(&book, &applied);

    let snap = acc.flush(&book, 2_000_000);
    assert_eq!(snap.intra_sigma, 0.0, "single event → sigma = 0");
}

#[test]
fn test_flush_resets_counters() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    let diff = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![(3000.0, 8.0)],
        asks: vec![],
    };
    let applied = book.apply_diff(&diff).unwrap().unwrap();
    acc.on_diff(&book, &applied);
    let _ = acc.flush(&book, 2_000_000);

    // After flush, no new events → n_events == 0
    let snap2 = acc.flush(&book, 3_000_000);
    assert_eq!(snap2.n_events, 0);
    assert_eq!(snap2.churn_bid, 0.0);
    assert_eq!(snap2.ofi_l1, 0.0);
}

#[test]
fn test_flush_empty_no_events() {
    let book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);
    // Flush without any on_diff calls
    let snap = acc.flush(&book, 2_000_000);
    assert_eq!(snap.n_events, 0);
    assert_eq!(snap.ofi_l1, 0.0);
    assert_eq!(snap.churn_bid, 0.0);
    assert_eq!(snap.churn_ask, 0.0);
    assert_eq!(snap.intra_sigma, 0.0);
    assert!(snap.open_px.is_none(), "no events → no open_px");
    // close_px comes from book.mid_price(), which exists
    assert!(snap.close_px.is_some());
}

#[test]
fn test_intra_sigma_nonzero_with_price_changes() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    // Event 1: mid = (3000 + 3001) / 2 = 3000.5
    let diff1 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![(3000.0, 6.0)],
        asks: vec![],
    };
    let applied1 = book.apply_diff(&diff1).unwrap().unwrap();
    acc.on_diff(&book, &applied1);

    // Event 2: change best bid → mid shifts
    let diff2 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_500_000,
        seq_id: 103,
        prev_seq_id: 103,
        prev_final_update_id: None,
        bids: vec![(3010.0, 2.0)],
        asks: vec![],
    };
    let applied2 = book.apply_diff(&diff2).unwrap().unwrap();
    acc.on_diff(&book, &applied2);

    let snap = acc.flush(&book, 2_000_000);
    assert!(
        snap.intra_sigma > 0.0,
        "sigma should be > 0 when mid prices differ, got {}",
        snap.intra_sigma
    );
    assert_eq!(snap.n_events, 2);
}

#[test]
fn test_open_close_px() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    // First event: mid = (3000 + 3001) / 2 = 3000.5 → becomes open_px
    let diff1 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![(3000.0, 6.0)],
        asks: vec![],
    };
    let applied1 = book.apply_diff(&diff1).unwrap().unwrap();
    acc.on_diff(&book, &applied1);

    // Second event: change best bid, mid shifts to (3005 + 3001) / 2 = 3003.0
    let diff2 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_500_000,
        seq_id: 103,
        prev_seq_id: 103,
        prev_final_update_id: None,
        bids: vec![(3005.0, 2.0)],
        asks: vec![],
    };
    let applied2 = book.apply_diff(&diff2).unwrap().unwrap();
    acc.on_diff(&book, &applied2);

    let snap = acc.flush(&book, 2_000_000);
    assert_eq!(snap.open_px, Some(3000.5), "open_px is first mid seen");
    assert_eq!(snap.close_px, book.mid_price(), "close_px is current mid");
}

#[test]
fn test_churn_across_three_windows() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    // Window 1: bid 3000 changes 5 → 7, churn = |7-5| = 2
    let diff1 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![(3000.0, 7.0)],
        asks: vec![],
    };
    let applied1 = book.apply_diff(&diff1).unwrap().unwrap();
    acc.on_diff(&book, &applied1);
    let snap1 = acc.flush(&book, 2_000_000);
    assert!((snap1.churn_bid - 2.0).abs() < 1e-10);

    // Window 2: bid 3000 changes 7 → 10, churn = |10-7| = 3
    let diff2 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 2_000_000,
        seq_id: 103,
        prev_seq_id: 103,
        prev_final_update_id: None,
        bids: vec![(3000.0, 10.0)],
        asks: vec![],
    };
    let applied2 = book.apply_diff(&diff2).unwrap().unwrap();
    acc.on_diff(&book, &applied2);
    let snap2 = acc.flush(&book, 3_000_000);
    assert!(
        (snap2.churn_bid - 3.0).abs() < 1e-10,
        "window 2 churn should be 3, got {}",
        snap2.churn_bid
    );

    // Window 3: bid 3000 changes 10 → 4, churn = |4-10| = 6
    let diff3 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 3_000_000,
        seq_id: 104,
        prev_seq_id: 104,
        prev_final_update_id: None,
        bids: vec![(3000.0, 4.0)],
        asks: vec![],
    };
    let applied3 = book.apply_diff(&diff3).unwrap().unwrap();
    acc.on_diff(&book, &applied3);
    let snap3 = acc.flush(&book, 4_000_000);
    assert!(
        (snap3.churn_bid - 6.0).abs() < 1e-10,
        "window 3 churn should be 6, got {}",
        snap3.churn_bid
    );
}

#[test]
fn test_ofi_accumulation_across_diffs() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    // Two diffs in same window — OFI should accumulate
    let diff1 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![(3000.0, 8.0)], // bid increase → positive OFI
        asks: vec![],
    };
    let applied1 = book.apply_diff(&diff1).unwrap().unwrap();
    acc.on_diff(&book, &applied1);

    let diff2 = DepthDiff {
        exchange: "test".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_500_000,
        seq_id: 103,
        prev_seq_id: 103,
        prev_final_update_id: None,
        bids: vec![(3000.0, 10.0)], // more bid increase
        asks: vec![],
    };
    let applied2 = book.apply_diff(&diff2).unwrap().unwrap();
    acc.on_diff(&book, &applied2);

    let snap = acc.flush(&book, 2_000_000);
    // Both diffs had positive bid pressure → OFI should be positive sum
    assert!(
        snap.ofi_l1 > 0.0,
        "accumulated OFI should be positive, got {}",
        snap.ofi_l1
    );
    assert_eq!(snap.n_events, 2);
}

#[test]
fn test_snapshot_fields_populated() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test_exchange", "ETHUSDT", 1_000_000);

    let diff = DepthDiff {
        exchange: "test_exchange".into(),
        symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000,
        seq_id: 102,
        prev_seq_id: 102,
        prev_final_update_id: None,
        bids: vec![(3000.0, 6.0)],
        asks: vec![],
    };
    let applied = book.apply_diff(&diff).unwrap().unwrap();
    acc.on_diff(&book, &applied);

    let snap = acc.flush(&book, 2_000_000);
    assert_eq!(snap.ts_us, 2_000_000);
    assert_eq!(snap.exchange, "test_exchange");
    assert_eq!(snap.symbol, "ETHUSDT");
    assert!(!snap.bids.is_empty());
    assert!(!snap.asks.is_empty());
    assert!(snap.mid_px.is_some());
    assert!(snap.microprice.is_some());
    assert!(snap.spread_bps.is_some());
    assert!(snap.imbalance_l1.is_some());
}

// ── accumulate_trade tests (HL/dYdX path) ────────────────────────────────────

#[test]
fn test_accumulate_trade_buy() {
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);
    acc.accumulate_trade(1.5, true);
    let snap = acc.flush_with_levels(None, 2_000_000);
    assert_eq!(snap.trade_count, 1);
    assert!((snap.buy_vol - 1.5).abs() < 1e-10);
    assert_eq!(snap.sell_vol, 0.0);
    assert!((snap.volume_delta - 1.5).abs() < 1e-10);
}

#[test]
fn test_accumulate_trade_sell() {
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);
    acc.accumulate_trade(2.0, false);
    let snap = acc.flush_with_levels(None, 2_000_000);
    assert_eq!(snap.trade_count, 1);
    assert_eq!(snap.buy_vol, 0.0);
    assert!((snap.sell_vol - 2.0).abs() < 1e-10);
    assert!((snap.volume_delta - (-2.0)).abs() < 1e-10);
}

#[test]
fn test_accumulate_trade_mixed_buy_sell() {
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);
    acc.accumulate_trade(3.0, true);
    acc.accumulate_trade(1.0, false);
    acc.accumulate_trade(2.0, true);
    let snap = acc.flush_with_levels(None, 2_000_000);
    assert_eq!(snap.trade_count, 3);
    assert!((snap.buy_vol - 5.0).abs() < 1e-10);
    assert!((snap.sell_vol - 1.0).abs() < 1e-10);
    // volume_delta = +3 - 1 + 2 = 4
    assert!((snap.volume_delta - 4.0).abs() < 1e-10);
}

#[test]
fn test_accumulate_trade_resets_after_flush() {
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);
    acc.accumulate_trade(5.0, true);
    let _ = acc.flush_with_levels(None, 2_000_000);

    let snap2 = acc.flush_with_levels(None, 3_000_000);
    assert_eq!(snap2.trade_count, 0);
    assert_eq!(snap2.buy_vol, 0.0);
    assert_eq!(snap2.sell_vol, 0.0);
    assert_eq!(snap2.volume_delta, 0.0);
}

// ── on_diff_from_levels tests (HL/dYdX path) ─────────────────────────────────

#[test]
fn test_on_diff_from_levels_basic() {
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);
    let applied = DiffApplied {
        ofi_l1_delta: 3.0,
        bid_abs_change: 1.0,
        ask_abs_change: 0.5,
    };
    acc.on_diff_from_levels(Some(3000.0), Some(3001.0), &applied);

    let snap = acc.flush_with_levels(None, 2_000_000);
    assert_eq!(snap.n_events, 1);
    assert!((snap.ofi_l1 - 3.0).abs() < 1e-10);
    assert!((snap.churn_bid - 1.0).abs() < 1e-10);
    assert!((snap.churn_ask - 0.5).abs() < 1e-10);
    // open_px should be mid = (3000 + 3001) / 2 = 3000.5
    assert_eq!(snap.open_px, Some(3000.5));
}

#[test]
fn test_on_diff_from_levels_none_prices() {
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);
    let applied = DiffApplied {
        ofi_l1_delta: 1.0,
        bid_abs_change: 0.0,
        ask_abs_change: 0.0,
    };
    // One side missing — mid should NOT be recorded
    acc.on_diff_from_levels(Some(3000.0), None, &applied);

    let snap = acc.flush_with_levels(None, 2_000_000);
    assert_eq!(snap.n_events, 1);
    assert!(
        snap.open_px.is_none(),
        "mid not computable with one side None"
    );
    assert_eq!(snap.intra_sigma, 0.0);
}

#[test]
fn test_on_diff_from_levels_accumulates_ofi() {
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);
    let a1 = DiffApplied {
        ofi_l1_delta: 2.0,
        bid_abs_change: 0.0,
        ask_abs_change: 0.0,
    };
    let a2 = DiffApplied {
        ofi_l1_delta: -1.0,
        bid_abs_change: 0.0,
        ask_abs_change: 0.0,
    };
    acc.on_diff_from_levels(Some(100.0), Some(101.0), &a1);
    acc.on_diff_from_levels(Some(100.0), Some(101.0), &a2);

    let snap = acc.flush_with_levels(None, 2_000_000);
    assert_eq!(snap.n_events, 2);
    assert!(
        (snap.ofi_l1 - 1.0).abs() < 1e-10,
        "OFI should accumulate: 2 + (-1) = 1"
    );
}

// ── flush_with_levels tests (HL/dYdX path) ───────────────────────────────────

#[test]
fn test_flush_with_levels_computes_metrics() {
    let mut acc = WindowAccumulator::new("hl", "ETH", 1_000_000);
    // Feed two events with different mids for sigma > 0
    let a1 = DiffApplied {
        ofi_l1_delta: 1.0,
        bid_abs_change: 0.5,
        ask_abs_change: 0.3,
    };
    let a2 = DiffApplied {
        ofi_l1_delta: -0.5,
        bid_abs_change: 0.2,
        ask_abs_change: 0.1,
    };
    acc.on_diff_from_levels(Some(3000.0), Some(3002.0), &a1); // mid = 3001
    acc.on_diff_from_levels(Some(3004.0), Some(3006.0), &a2); // mid = 3005

    let bids = vec![(3004.0, 5.0), (3003.0, 3.0), (3002.0, 2.0)];
    let asks = vec![(3006.0, 4.0), (3007.0, 2.0)];
    let snap = acc.flush_with_levels(Some((&bids, &asks)), 2_000_000);

    assert_eq!(snap.exchange, "hl");
    assert_eq!(snap.symbol, "ETH");
    assert_eq!(snap.n_events, 2);
    // mid from levels: (3004 + 3006) / 2 = 3005
    assert!((snap.mid_px.unwrap() - 3005.0).abs() < 1e-10);
    // microprice: (3004 * 4 + 3006 * 5) / (5 + 4) = (12016 + 15030) / 9 = 3005.111...
    let expected_micro = (3004.0 * 4.0 + 3006.0 * 5.0) / 9.0;
    assert!((snap.microprice.unwrap() - expected_micro).abs() < 1e-6);
    // spread_bps: (3006 - 3004) / 3005 * 10000 = 6.655...
    let expected_spread = (3006.0 - 3004.0) / 3005.0 * 10_000.0;
    assert!((snap.spread_bps.unwrap() - expected_spread as f32).abs() < 0.01);
    // imbalance_l1: (5 - 4) / (5 + 4) = 0.111...
    assert!((snap.imbalance_l1.unwrap() - 1.0 / 9.0).abs() < 1e-6);
    // depths
    assert!((snap.bid_depth_l5 - 10.0).abs() < 1e-10); // 5 + 3 + 2
    assert!((snap.ask_depth_l5 - 6.0).abs() < 1e-10); // 4 + 2
    // sigma > 0 (two different mids: 3001 and 3005)
    assert!(snap.intra_sigma > 0.0);
    // OFI: 1.0 + (-0.5) = 0.5
    assert!((snap.ofi_l1 - 0.5).abs() < 1e-10);
    // churn: bid = 0.5 + 0.2 = 0.7, ask = 0.3 + 0.1 = 0.4
    assert!((snap.churn_bid - 0.7).abs() < 1e-10);
    assert!((snap.churn_ask - 0.4).abs() < 1e-10);
    // open_px = first mid = 3001
    assert_eq!(snap.open_px, Some(3001.0));
    // close_px = current mid from levels = 3005
    assert!((snap.close_px.unwrap() - 3005.0).abs() < 1e-10);
}

#[test]
fn test_flush_with_levels_none_returns_empty() {
    let mut acc = WindowAccumulator::new("hl", "ETH", 1_000_000);
    let snap = acc.flush_with_levels(None, 2_000_000);
    assert!(snap.mid_px.is_none());
    assert!(snap.microprice.is_none());
    assert!(snap.spread_bps.is_none());
    assert!(snap.imbalance_l1.is_none());
    assert!(snap.bids.is_empty());
    assert!(snap.asks.is_empty());
    assert_eq!(snap.bid_depth_l5, 0.0);
    assert_eq!(snap.ask_depth_l5, 0.0);
}

#[test]
fn test_flush_with_levels_resets_counters() {
    let mut acc = WindowAccumulator::new("hl", "ETH", 1_000_000);
    let applied = DiffApplied {
        ofi_l1_delta: 5.0,
        bid_abs_change: 2.0,
        ask_abs_change: 1.0,
    };
    acc.on_diff_from_levels(Some(100.0), Some(101.0), &applied);
    acc.accumulate_trade(3.0, true);
    let _ = acc.flush_with_levels(None, 2_000_000);

    let snap2 = acc.flush_with_levels(None, 3_000_000);
    assert_eq!(snap2.n_events, 0);
    assert_eq!(snap2.ofi_l1, 0.0);
    assert_eq!(snap2.churn_bid, 0.0);
    assert_eq!(snap2.churn_ask, 0.0);
    assert!(snap2.open_px.is_none());
    assert_eq!(snap2.trade_count, 0);
    assert_eq!(snap2.volume_delta, 0.0);
}

#[test]
fn test_flush_with_levels_sigma_exact_value() {
    let mut acc = WindowAccumulator::new("test", "ETH", 1_000_000);
    // Two events: mid = 100, mid = 200. Mean = 150, var = E[x²]-E[x]² = 25000 - 22500 = 2500
    let a = DiffApplied::default();
    acc.on_diff_from_levels(Some(90.0), Some(110.0), &a); // mid = 100
    acc.on_diff_from_levels(Some(190.0), Some(210.0), &a); // mid = 200

    let snap = acc.flush_with_levels(None, 2_000_000);
    // sigma = sqrt(2500) = 50
    assert!(
        (snap.intra_sigma - 50.0).abs() < 0.01,
        "sigma should be 50, got {}",
        snap.intra_sigma
    );
}
