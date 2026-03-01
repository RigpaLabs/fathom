use fathom::{
    accumulator::WindowAccumulator,
    orderbook::{DepthDiff, OrderBook, SnapshotMsg},
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
        bids: vec![(3000.0, 6.0)],
        asks: vec![],
    };
    let applied = book.apply_diff(&diff).unwrap().unwrap();
    acc.on_diff(&book, &applied);

    let snap = acc.flush(&book, 2_000_000);
    assert_eq!(snap.n_events, 1);
    assert!(snap.mid_px.is_some());
    assert!(snap.churn_bid > 0.0, "churn_bid should reflect qty change: |6-5|=1");
}

#[test]
fn test_bid_last_carry_over() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    // First window: change bid qty 5 → 7
    let diff1 = DepthDiff {
        exchange: "test".into(), symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000, seq_id: 102, prev_seq_id: 102,
        bids: vec![(3000.0, 7.0)], asks: vec![],
    };
    let applied1 = book.apply_diff(&diff1).unwrap().unwrap();
    acc.on_diff(&book, &applied1);
    // churn_bid = |7 - 5| = 2
    assert!((acc.flush(&book, 2_000_000).churn_bid - 2.0).abs() < 1e-10);

    // Second window: change same level 7 → 9 (should use carry-over bid_last = 7)
    let diff2 = DepthDiff {
        exchange: "test".into(), symbol: "ETHUSDT".into(),
        timestamp_us: 2_000_000, seq_id: 103, prev_seq_id: 103,
        bids: vec![(3000.0, 9.0)], asks: vec![],
    };
    let applied2 = book.apply_diff(&diff2).unwrap().unwrap();
    acc.on_diff(&book, &applied2);
    let snap2 = acc.flush(&book, 3_000_000);
    // churn_bid = |9 - 7| = 2, NOT |9 - 0| = 9
    assert!((snap2.churn_bid - 2.0).abs() < 1e-10, "carry-over bid_last not working: got {}", snap2.churn_bid);
}

#[test]
fn test_intra_sigma_zero_single_event() {
    let mut book = make_book_synced();
    let mut acc = WindowAccumulator::new("test", "ETHUSDT", 1_000_000);

    let diff = DepthDiff {
        exchange: "test".into(), symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000, seq_id: 102, prev_seq_id: 102,
        bids: vec![], asks: vec![],
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
        exchange: "test".into(), symbol: "ETHUSDT".into(),
        timestamp_us: 1_000_000, seq_id: 102, prev_seq_id: 102,
        bids: vec![(3000.0, 8.0)], asks: vec![],
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
