//! Criterion benchmarks for the fathom order book hot paths.
//!
//! Run with: `cargo bench`
//! HTML report: `target/criterion/report/index.html`

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use fathom::{
    accumulator::{Snapshot1s, WindowAccumulator},
    connection::parse_combined_message,
    orderbook::{DepthDiff, DiffApplied, OrderBook, SnapshotMsg},
    writer::snap_1s::write_snap_to_memory,
};

// ── Realistic Binance combined-stream message (20 bid + 20 ask levels) ───────

static BINANCE_DEPTH_MSG: &str = r#"{"stream":"ethusdt@depth@100ms","data":{"E":1700000000000,"U":1001,"u":1100,"b":[["3000.00","1.50"],["2999.50","2.00"],["2999.00","1.80"],["2998.50","3.10"],["2998.00","2.40"],["2997.50","1.90"],["2997.00","4.20"],["2996.50","2.70"],["2996.00","3.30"],["2995.50","1.60"],["2995.00","5.10"],["2994.50","2.30"],["2994.00","3.70"],["2993.50","4.80"],["2993.00","2.90"],["2992.50","1.50"],["2992.00","6.20"],["2991.50","3.40"],["2991.00","2.80"],["2990.50","4.10"]],"a":[["3000.50","1.40"],["3001.00","2.10"],["3001.50","1.70"],["3002.00","3.00"],["3002.50","2.50"],["3003.00","1.80"],["3003.50","4.30"],["3004.00","2.60"],["3004.50","3.20"],["3005.00","1.70"],["3005.50","5.00"],["3006.00","2.40"],["3006.50","3.60"],["3007.00","4.90"],["3007.50","3.00"],["3008.00","1.60"],["3008.50","6.10"],["3009.00","3.30"],["3009.50","2.90"],["3010.00","4.20"]]}}"#;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a synced 20-level OrderBook ready for `apply_diff` benchmarking.
fn make_synced_book_20_levels() -> OrderBook {
    let mut book = OrderBook::new();

    let bids: Vec<(f64, f64)> = (0..20)
        .map(|i| (3000.0 - i as f64 * 0.50, 1.0 + i as f64 * 0.15))
        .collect();
    let asks: Vec<(f64, f64)> = (0..20)
        .map(|i| (3000.50 + i as f64 * 0.50, 1.0 + i as f64 * 0.15))
        .collect();

    book.apply_snapshot(SnapshotMsg {
        symbol: "ETHUSDT".to_string(),
        last_update_id: 1000,
        bids,
        asks,
    });

    // Apply a sync event so the book is ready for ongoing diffs.
    // Spot rule: big_u (prev_seq_id=1000) <= lastUpdateId+1(1001) <= u(1001).
    let sync = DepthDiff {
        exchange: "binance_spot".to_string(),
        symbol: "ETHUSDT".to_string(),
        timestamp_us: 1_000_000,
        seq_id: 1001,
        prev_seq_id: 1000,
        prev_final_update_id: None,
        bids: vec![],
        asks: vec![],
    };
    // Sync failure here means the test helper is broken, not production code.
    book.apply_diff(&sync).expect("sync diff must succeed");
    book
}

/// Build 100 sequential spot diffs that won't trigger a gap on the synced book.
///
/// After sync the book has `last_update_id = 1001`.  Spot ongoing rule:
/// `prev_seq_id == last_update_id + 1`, so diff[i] must have
/// `prev_seq_id = 1002 + i` and `seq_id = 1002 + i`.
fn make_sequential_diffs(n: usize) -> Vec<DepthDiff> {
    (0..n)
        .map(|i| {
            let seq = 1002 + i as i64;
            // Each diff updates 5 bid and 5 ask levels — representative of
            // Binance 100ms depth updates in active markets.
            let bids: Vec<(f64, f64)> = (0..5)
                .map(|k| {
                    let px = 3000.0 - ((i + k) % 20) as f64 * 0.50;
                    let qty = 1.0 + (i % 5) as f64 * 0.1 + (k % 3) as f64 * 0.05;
                    (px, qty)
                })
                .collect();
            let asks: Vec<(f64, f64)> = (0..5)
                .map(|k| {
                    let px = 3000.50 + ((i + k) % 20) as f64 * 0.50;
                    let qty = 1.0 + (i % 4) as f64 * 0.1 + (k % 3) as f64 * 0.05;
                    (px, qty)
                })
                .collect();

            DepthDiff {
                exchange: "binance_spot".to_string(),
                symbol: "ETHUSDT".to_string(),
                timestamp_us: seq * 100_000,
                seq_id: seq,
                prev_seq_id: seq, // big_u == last_update_id + 1 for each step
                prev_final_update_id: None,
                bids,
                asks,
            }
        })
        .collect()
}

/// Build a Snapshot1s with realistic values for row `i`.
fn make_snap(i: usize) -> Snapshot1s {
    let base_px = 3000.0 + (i % 100) as f64 * 0.01;
    let bids: Vec<(f64, f64)> = (0..10)
        .map(|k| (base_px - k as f64 * 0.50, 1.5 + k as f64 * 0.1))
        .collect();
    let asks: Vec<(f64, f64)> = (0..10)
        .map(|k| (base_px + 0.50 + k as f64 * 0.50, 1.4 + k as f64 * 0.1))
        .collect();

    Snapshot1s {
        ts_us: 1_700_000_000_000_000 + i as i64 * 1_000_000,
        exchange: "binance_perp".to_string(),
        symbol: "ETHUSDT".to_string(),
        bids,
        asks,
        mid_px: Some(base_px + 0.25),
        microprice: Some(base_px + 0.24),
        spread_bps: Some(3.33),
        imbalance_l1: Some(0.1 + (i % 10) as f32 * 0.01),
        imbalance_l5: Some(0.05 + (i % 10) as f32 * 0.005),
        imbalance_l10: Some(0.02 + (i % 10) as f32 * 0.002),
        bid_depth_l5: 100.0 + (i % 50) as f64,
        bid_depth_l10: 200.0 + (i % 50) as f64,
        ask_depth_l5: 95.0 + (i % 50) as f64,
        ask_depth_l10: 190.0 + (i % 50) as f64,
        ofi_l1: -5.0 + (i % 20) as f64,
        churn_bid: 10.0 + (i % 10) as f64,
        churn_ask: 11.0 + (i % 10) as f64,
        intra_sigma: 0.01 + (i % 5) as f32 * 0.001,
        open_px: Some(base_px - 0.10),
        close_px: Some(base_px + 0.25),
        n_events: 15 + (i % 10) as u32,
        volume_delta: -2.0 + (i % 20) as f64 * 0.5,
        buy_vol: 50.0 + (i % 30) as f64,
        sell_vol: 52.0 + (i % 30) as f64,
        trade_count: 8 + (i % 5) as u32,
    }
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

/// Parse a full Binance combined-stream message: JSON string → WsCombined →
/// DepthUpdate → Vec<(f64, f64)> bid/ask levels.
///
/// This is the complete parse hot path executed for every WebSocket event.
fn bench_parse(c: &mut Criterion) {
    c.bench_function("parse/binance_depth_20_levels", |b| {
        b.iter(|| parse_combined_message(black_box(BINANCE_DEPTH_MSG)))
    });
}

/// Apply 100 sequential diff events to a synced 20-level BTreeMap order book.
///
/// Each diff updates 5 bid and 5 ask levels, matching typical Binance 100ms
/// depth stream activity on a liquid symbol.
fn bench_apply_diff(c: &mut Criterion) {
    let diffs = make_sequential_diffs(100);

    c.bench_function("orderbook/apply_diff_100_updates_20_levels", |b| {
        b.iter_batched(
            make_synced_book_20_levels,
            |mut book| {
                for diff in &diffs {
                    let _ = black_box(book.apply_diff(black_box(diff)));
                }
                book
            },
            BatchSize::SmallInput,
        )
    });
}

/// Flush a WindowAccumulator that has 1000 accumulated diff events.
///
/// The flush call computes intra-window statistics (variance, OFI sum, churn)
/// and extracts the top-10 book levels into a Snapshot1s.
fn bench_accumulator_flush(c: &mut Criterion) {
    c.bench_function("accumulator/flush_after_1000_events", |b| {
        b.iter_batched(
            || {
                let book = make_synced_book_20_levels();
                let mut acc = WindowAccumulator::new("binance_perp", "ETHUSDT", 0);
                // Simulate 1000 diff events arriving in the window.
                let applied = DiffApplied {
                    ofi_l1_delta: 0.5,
                    bid_abs_change: 1.2,
                    ask_abs_change: 0.9,
                };
                for _ in 0..1000 {
                    acc.on_diff(&book, &applied);
                }
                (acc, book)
            },
            |(mut acc, book)| acc.flush(black_box(&book), black_box(1_700_000_000_000_000)),
            BatchSize::SmallInput,
        )
    });
}

/// Encode 3600 Snapshot1s rows (1 hour of data for one symbol) into a
/// Snappy-compressed Parquet blob in memory.
///
/// This measures the Arrow array construction + Parquet encoding cost without
/// disk I/O variance. The returned `Vec<u8>` is dropped outside timing.
fn bench_writer_flush(c: &mut Criterion) {
    let snaps: Vec<Snapshot1s> = (0..3600).map(make_snap).collect();

    c.bench_function("writer/flush_3600_rows_to_memory", |b| {
        b.iter_with_large_drop(|| {
            write_snap_to_memory(black_box(&snaps)).expect("write_snap_to_memory must not fail")
        })
    });
}

criterion_group!(
    benches,
    bench_parse,
    bench_apply_diff,
    bench_accumulator_flush,
    bench_writer_flush,
);
criterion_main!(benches);
