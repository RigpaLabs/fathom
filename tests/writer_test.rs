/// Writer unit tests: verify Parquet files are created and contain correct data.
///
/// Strategy: spawn writer tasks, send N records, drop channel (graceful shutdown),
/// await task completion, read Parquet back and assert.
use std::path::PathBuf;

use chrono::Timelike;
use fathom::{
    accumulator::Snapshot1s,
    writer::{
        deriv::{DerivEvent, Liquidation, MarkFunding, OpenInterest, run_deriv_writer},
        raw::RawDiff,
        raw::{bucket_open, run_raw_writer},
        snap_1s::{run_snap_writer, run_snap_writer_with_flush_interval},
        trades::{RawTrade, run_trades_writer},
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

// ── Raw writer ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_raw_writer_creates_file() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    let (tx, rx) = broadcast::channel::<RawDiff>(64);

    // Spawn writer with very short flush interval (1s for test)
    let writer = tokio::spawn(run_raw_writer(
        data_dir.clone(),
        rx,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    // Send 5 events
    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..5u64 {
        tx.send(RawDiff {
            timestamp_us: now_us + i as i64 * 100_000,
            exchange: "binance_spot".to_string(),
            symbol: "ETHUSDT".to_string(),
            seq_id: 101 + i as i64,
            prev_seq_id: 100 + i as i64,
            bids: vec![(3000.0, 5.0), (2999.0, 3.0)],
            asks: vec![(3001.0, 4.0)],
        })
        .unwrap();
    }

    // Drop sender → triggers graceful shutdown in writer
    drop(tx);
    writer.await.unwrap();

    // Find the parquet file
    let parquet_files = find_parquets(&data_dir);
    assert!(
        !parquet_files.is_empty(),
        "raw writer should create at least one parquet file"
    );

    // Read back and verify
    let file = std::fs::File::open(&parquet_files[0]).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = reader.schema().clone();

    // Schema has correct columns
    schema
        .field_with_name("timestamp_us")
        .expect("timestamp_us");
    schema.field_with_name("exchange").expect("exchange");
    schema.field_with_name("symbol").expect("symbol");
    schema.field_with_name("bid_prices").expect("bid_prices");
    schema.field_with_name("ask_prices").expect("ask_prices");

    let mut rows = 0;
    for batch in reader.build().unwrap() {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 5, "should have written exactly 5 rows");
}

#[tokio::test]
async fn test_raw_writer_multiple_symbols() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawDiff>(64);
    let writer = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for sym in &["ETHUSDT", "BTCUSDT"] {
        for i in 0..3u64 {
            tx.send(RawDiff {
                timestamp_us: now_us + i as i64 * 100_000,
                exchange: "binance_spot".to_string(),
                symbol: sym.to_string(),
                seq_id: 101 + i as i64,
                prev_seq_id: 100 + i as i64,
                bids: vec![(3000.0, 1.0)],
                asks: vec![(3001.0, 1.0)],
            })
            .unwrap();
        }
    }

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert!(
        files.len() >= 2,
        "should create separate files for each symbol"
    );
}

#[tokio::test]
async fn test_raw_writer_empty_channel() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawDiff>(64);
    let writer = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    // Close immediately without sending anything
    drop(tx);
    writer.await.unwrap();

    // No files should be created
    let files = find_parquets(&dir.path().to_path_buf());
    assert!(files.is_empty(), "empty channel → no parquet files");
}

// ── Trades writer ─────────────────────────────────────────────────────────────

fn make_trade(exchange: &str, symbol: &str, ts_us: i64, id: i64, buyer_maker: bool) -> RawTrade {
    RawTrade {
        timestamp_us: ts_us,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        trade_id: id,
        price: 3000.5,
        qty: 1.25,
        is_buyer_maker: buyer_maker,
    }
}

#[tokio::test]
async fn test_trades_writer_creates_file_and_roundtrips_values() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawTrade>(64);
    let writer = tokio::spawn(run_trades_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..5i64 {
        tx.send(make_trade(
            "binance_spot",
            "ETHUSDT",
            now_us + i * 1_000,
            100 + i,
            i % 2 == 0,
        ))
        .unwrap();
    }

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(files.len(), 1, "one trades parquet file for ETHUSDT");
    assert!(
        files[0]
            .to_string_lossy()
            .contains("/trades/binance_spot/ETHUSDT/"),
        "path layout must be trades/{{exchange}}/{{symbol}}/{{date}}: {:?}",
        files[0]
    );

    let file = std::fs::File::open(&files[0]).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = reader.schema().clone();
    for col in [
        "timestamp_us",
        "exchange",
        "symbol",
        "trade_id",
        "price",
        "qty",
        "is_buyer_maker",
    ] {
        schema.field_with_name(col).expect(col);
    }

    use arrow_array::{BooleanArray, Float64Array, Int64Array};
    let mut rows = 0;
    for batch in reader.build().unwrap() {
        let batch = batch.unwrap();
        if rows == 0 {
            let ts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(ts.value(0), now_us);
            let id = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(id.value(0), 100);
            let px = batch
                .column(4)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(px.value(0), 3000.5);
            let qty = batch
                .column(5)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(qty.value(0), 1.25);
            let bm = batch
                .column(6)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert!(bm.value(0));
            assert!(!bm.value(1));
        }
        rows += batch.num_rows();
    }
    assert_eq!(rows, 5, "should have written exactly 5 rows");
}

#[tokio::test]
async fn test_trades_writer_rotation_file_naming() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawTrade>(64);
    let writer = tokio::spawn(run_trades_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    let now = chrono::Utc::now();
    tx.send(make_trade(
        "binance_perp",
        "BTCUSDT",
        now.timestamp_micros(),
        7,
        false,
    ))
    .unwrap();

    drop(tx);
    writer.await.unwrap();

    // With rotate_hours=1 the file opens at the current hour bucket and is
    // renamed trades_{open}_{end}.parquet on close.
    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(files.len(), 1);
    let filename = files[0].file_name().unwrap().to_str().unwrap();
    let expected_prefix = format!("trades_{:02}00_", now.hour());
    assert!(
        filename.starts_with(&expected_prefix),
        "expected file starting with {expected_prefix}, got {filename}"
    );
    assert!(
        !filename.ends_with("_open.parquet"),
        "graceful shutdown must rename the open file: {filename}"
    );
}

#[tokio::test]
async fn test_trades_writer_multiple_symbols() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawTrade>(64);
    let writer = tokio::spawn(run_trades_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for (i, sym) in ["ETHUSDT", "BTCUSDT"].iter().enumerate() {
        tx.send(make_trade("binance_spot", sym, now_us, i as i64, false))
            .unwrap();
    }

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(files.len(), 2, "separate file per symbol");
}

// ── Snap 1s writer ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_snap_writer_creates_file() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<Snapshot1s>(64);
    let writer = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        rx,
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..3u64 {
        tx.send(make_snap(
            "binance_spot",
            "ETHUSDT",
            now_us + i as i64 * 1_000_000,
        ))
        .unwrap();
    }

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert!(
        !files.is_empty(),
        "snap writer should create at least one parquet file"
    );

    let file = std::fs::File::open(&files[0]).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = reader.schema().clone();

    schema.field_with_name("ts_us").expect("ts_us");
    schema.field_with_name("n_events").expect("n_events");
    schema.field_with_name("bid_px_0").expect("bid_px_0");
    schema.field_with_name("ask_px_0").expect("ask_px_0");
    schema.field_with_name("ofi_l1").expect("ofi_l1");

    let mut rows = 0;
    for batch in reader.build().unwrap() {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 3, "should have 3 rows");
}

#[tokio::test]
async fn test_snap_writer_verifies_data_values() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<Snapshot1s>(64);
    let writer = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        rx,
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    let ts = 1_700_000_000_000_000_i64;
    tx.send(Snapshot1s {
        ts_us: ts,
        exchange: "binance_spot".to_string(),
        symbol: "BTCUSDT".to_string(),
        bids: vec![(50000.0, 2.0), (49999.0, 3.0)],
        asks: vec![(50001.0, 1.5)],
        mid_px: Some(50000.5),
        microprice: Some(50000.3),
        spread_bps: Some(0.4),
        imbalance_l1: Some(0.14),
        imbalance_l5: Some(0.1),
        imbalance_l10: Some(0.05),
        bid_depth_l5: 5.0,
        bid_depth_l10: 5.0,
        ask_depth_l5: 1.5,
        ask_depth_l10: 1.5,
        ofi_l1: 2.5,
        churn_bid: 10.0,
        churn_ask: 8.0,
        intra_sigma: 0.01,
        open_px: Some(50000.0),
        close_px: Some(50001.0),
        n_events: 42,
        volume_delta: 0.0,
        buy_vol: 0.0,
        sell_vol: 0.0,
        trade_count: 0,
    })
    .unwrap();

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    let file = std::fs::File::open(&files[0]).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    // n_events is 5th from last (4 trade columns were appended after it)
    use arrow_array::UInt32Array;
    let n_events = batch
        .column(batch.num_columns() - 5)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(n_events.value(0), 42);

    // ts_us is first column
    use arrow_array::Int64Array;
    let ts_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ts_col.value(0), ts);
}

#[tokio::test]
async fn test_snap_writer_multiple_symbols() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<Snapshot1s>(64);
    let writer = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        rx,
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for sym in &["ETHUSDT", "BTCUSDT", "BNBUSDT"] {
        tx.send(make_snap("binance_spot", sym, now_us)).unwrap();
    }

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert!(
        files.len() >= 3,
        "separate file per symbol: got {} files",
        files.len()
    );
}

/// Verify periodic disk flush creates multiple row groups.
///
/// Each `ArrowWriter::flush()` closes the current row group and starts a new one.
/// Without periodic flush, all rows end up in a single row group (closed at finish).
/// With flush_interval=10 and 25 rows, we expect 3 row groups: [10, 10, 5].
#[tokio::test]
async fn test_snap_writer_periodic_disk_flush() {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<Snapshot1s>(64);
    let flush_interval = 10;
    let total_rows = 25;
    let writer = tokio::spawn(run_snap_writer_with_flush_interval(
        dir.path().to_path_buf(),
        rx,
        flush_interval,
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..total_rows as u64 {
        tx.send(make_snap(
            "binance_spot",
            "ETHUSDT",
            now_us + i as i64 * 1_000_000,
        ))
        .unwrap();
    }

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(files.len(), 1);

    let file = std::fs::File::open(&files[0]).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();

    assert_eq!(
        count_parquet_rows(&files[0]),
        total_rows,
        "all rows written"
    );

    // With flush_interval=10 and 25 rows: flush at row 10, 20, then close flushes remaining 5
    let num_row_groups = metadata.num_row_groups();
    assert_eq!(
        num_row_groups, 3,
        "periodic flush should create 3 row groups (10+10+5), got {num_row_groups}"
    );
}

// ── bucket_open / rotation tests ─────────────────────────────────────────────

#[test]
fn test_bucket_open_1h() {
    // Every hour is its own bucket
    for h in 0..24 {
        assert_eq!(bucket_open(h, 1), h);
    }
}

#[test]
fn test_bucket_open_6h() {
    assert_eq!(bucket_open(0, 6), 0);
    assert_eq!(bucket_open(5, 6), 0);
    assert_eq!(bucket_open(6, 6), 6);
    assert_eq!(bucket_open(11, 6), 6);
    assert_eq!(bucket_open(12, 6), 12);
    assert_eq!(bucket_open(17, 6), 12);
    assert_eq!(bucket_open(18, 6), 18);
    assert_eq!(bucket_open(23, 6), 18);
}

#[test]
fn test_bucket_open_all_valid_intervals() {
    let valid: &[u32] = &[1, 2, 3, 4, 6, 8, 12, 24];
    for &interval in valid {
        for h in 0..24 {
            let bucket = bucket_open(h, interval);
            assert!(
                bucket <= h,
                "bucket {bucket} > hour {h} for interval {interval}"
            );
            assert_eq!(
                bucket % interval,
                0,
                "bucket {bucket} not aligned to interval {interval}"
            );
            // Next bucket boundary is > current hour (no missed rotation)
            assert!(
                bucket + interval > h,
                "hour {h} past bucket end for interval {interval}"
            );
        }
    }
}

#[test]
fn test_bucket_open_rotation_triggers_at_boundary() {
    // Simulate hour-by-hour: rotation happens when bucket changes
    for interval in [1, 2, 3, 4, 6, 8, 12, 24] {
        let mut rotations = 0;
        let mut prev_bucket = bucket_open(0, interval);
        for h in 1..24 {
            let cur = bucket_open(h, interval);
            if cur != prev_bucket {
                rotations += 1;
                prev_bucket = cur;
            }
        }
        let expected_rotations = (24 / interval) - 1; // first bucket doesn't rotate
        assert_eq!(
            rotations, expected_rotations,
            "interval={interval}: expected {expected_rotations} rotations, got {rotations}"
        );
    }
}

#[tokio::test]
async fn test_raw_writer_rotate_hours_1_creates_correct_bucket_file() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawDiff>(64);
    let writer = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    let now = chrono::Utc::now();
    let now_us = now.timestamp_micros();
    tx.send(RawDiff {
        timestamp_us: now_us,
        exchange: "binance_spot".to_string(),
        symbol: "ETHUSDT".to_string(),
        seq_id: 100,
        prev_seq_id: 99,
        bids: vec![(3000.0, 1.0)],
        asks: vec![(3001.0, 1.0)],
    })
    .unwrap();

    drop(tx);
    writer.await.unwrap();

    // With rotate_hours=1, the bucket open_hhmm should be the current hour
    let expected_prefix = format!("depth_{:02}00_", now.hour());
    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(files.len(), 1);
    let filename = files[0].file_name().unwrap().to_str().unwrap();
    assert!(
        filename.starts_with(&expected_prefix),
        "expected file starting with {expected_prefix}, got {filename}"
    );
}

#[tokio::test]
async fn test_raw_writer_rotate_hours_6_creates_correct_bucket_file() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawDiff>(64);
    let writer = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        6,
        fathom::metrics::new_metrics().metrics,
    ));

    let now = chrono::Utc::now();
    let now_us = now.timestamp_micros();
    tx.send(RawDiff {
        timestamp_us: now_us,
        exchange: "binance_spot".to_string(),
        symbol: "ETHUSDT".to_string(),
        seq_id: 100,
        prev_seq_id: 99,
        bids: vec![(3000.0, 1.0)],
        asks: vec![(3001.0, 1.0)],
    })
    .unwrap();

    drop(tx);
    writer.await.unwrap();

    // With rotate_hours=6, bucket aligns to 0/6/12/18
    let bucket = (now.hour() / 6) * 6;
    let expected_prefix = format!("depth_{:02}00_", bucket);
    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(files.len(), 1);
    let filename = files[0].file_name().unwrap().to_str().unwrap();
    assert!(
        filename.starts_with(&expected_prefix),
        "expected file starting with {expected_prefix}, got {filename}"
    );
}

// ── Blue-green data isolation test ────────────────────────────────────────────

#[tokio::test]
async fn test_two_writers_different_data_dirs_no_interference() {
    // Simulates blue-green deploy: two raw writers for the same symbol
    // writing to different data_dir subdirs simultaneously.
    let dir = TempDir::new().unwrap();
    let dir_old = dir.path().join("v1");
    let dir_new = dir.path().join("v2");

    let (tx_old, rx_old) = broadcast::channel::<RawDiff>(64);
    let (tx_new, rx_new) = broadcast::channel::<RawDiff>(64);

    let w_old = tokio::spawn(run_raw_writer(
        dir_old.clone(),
        rx_old,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    let w_new = tokio::spawn(run_raw_writer(
        dir_new.clone(),
        rx_new,
        1,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    let now_us = chrono::Utc::now().timestamp_micros();

    // Old writer: 3 events
    for i in 0..3u64 {
        tx_old
            .send(RawDiff {
                timestamp_us: now_us + i as i64 * 100_000,
                exchange: "binance_spot".to_string(),
                symbol: "ETHUSDT".to_string(),
                seq_id: 100 + i as i64,
                prev_seq_id: 99 + i as i64,
                bids: vec![(3000.0, 1.0)],
                asks: vec![(3001.0, 1.0)],
            })
            .unwrap();
    }

    // New writer: 5 events (different count proves isolation)
    for i in 0..5u64 {
        tx_new
            .send(RawDiff {
                timestamp_us: now_us + i as i64 * 100_000,
                exchange: "binance_spot".to_string(),
                symbol: "ETHUSDT".to_string(),
                seq_id: 200 + i as i64,
                prev_seq_id: 199 + i as i64,
                bids: vec![(3000.0, 2.0)],
                asks: vec![(3001.0, 2.0)],
            })
            .unwrap();
    }

    drop(tx_old);
    drop(tx_new);
    w_old.await.unwrap();
    w_new.await.unwrap();

    // Each dir has its own parquet — no cross-contamination
    let files_old = find_parquets(&dir_old);
    let files_new = find_parquets(&dir_new);
    assert_eq!(files_old.len(), 1, "old writer: 1 file");
    assert_eq!(files_new.len(), 1, "new writer: 1 file");

    // Verify row counts are independent
    let rows_old = count_parquet_rows(&files_old[0]);
    let rows_new = count_parquet_rows(&files_new[0]);
    assert_eq!(rows_old, 3, "old writer: 3 rows");
    assert_eq!(rows_new, 5, "new writer: 5 rows");
}

// ── Event-time rollover tests ─────────────────────────────────────────────────

/// Verify snap_writer partitions by event timestamp (ts_us), not wall clock.
/// Sends snapshots with timestamps from two different UTC days and asserts
/// they land in separate daily files.
#[tokio::test]
async fn test_snap_writer_event_time_rollover() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<Snapshot1s>(64);
    let writer = tokio::spawn(run_snap_writer_with_flush_interval(
        dir.path().to_path_buf(),
        rx,
        1, // flush every row
        CancellationToken::new(),
        fathom::metrics::new_metrics().metrics,
    ));

    // Day 1: 2025-01-15 12:00:00 UTC
    let day1_ts = 1736942400_000_000_i64; // 2025-01-15T12:00:00Z in µs
    tx.send(make_snap("binance_spot", "ETHUSDT", day1_ts))
        .unwrap();
    tx.send(make_snap("binance_spot", "ETHUSDT", day1_ts + 1_000_000))
        .unwrap();

    // Day 2: 2025-01-16 00:00:01 UTC (next day)
    let day2_ts = day1_ts + 12 * 3600 * 1_000_000 + 1_000_000; // +12h1s → crosses midnight
    tx.send(make_snap("binance_spot", "ETHUSDT", day2_ts))
        .unwrap();

    drop(tx);
    writer.await.unwrap();

    // Should have exactly 2 parquet files: one for 2025-01-15, one for 2025-01-16
    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(
        files.len(),
        2,
        "event-time rollover should create 2 daily files, got {}: {:?}",
        files.len(),
        files
    );

    // Verify file paths contain correct dates
    let file_names: Vec<String> = files
        .iter()
        .map(|f| f.to_string_lossy().to_string())
        .collect();
    assert!(
        file_names.iter().any(|f| f.contains("2025-01-15")),
        "expected a file for 2025-01-15"
    );
    assert!(
        file_names.iter().any(|f| f.contains("2025-01-16")),
        "expected a file for 2025-01-16"
    );

    // Verify row counts: 2 rows in day1, 1 row in day2
    let total_rows: usize = files.iter().map(|f| count_parquet_rows(f)).sum();
    assert_eq!(total_rows, 3, "all 3 rows should be present");
}

/// Verify that CancellationToken triggers graceful shutdown of snap writer,
/// flushing buffered data before exit.
#[tokio::test]
async fn test_snap_writer_cancellation_shutdown() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<Snapshot1s>(64);
    let cancel = CancellationToken::new();
    let writer = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        rx,
        cancel.clone(),
        fathom::metrics::new_metrics().metrics,
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..3u64 {
        tx.send(make_snap(
            "binance_spot",
            "ETHUSDT",
            now_us + i as i64 * 1_000_000,
        ))
        .unwrap();
    }

    // Small delay so writer processes the messages
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Cancel instead of dropping the sender
    cancel.cancel();
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert!(!files.is_empty(), "cancellation should flush data to disk");
    let total: usize = files.iter().map(|f| count_parquet_rows(f)).sum();
    assert_eq!(total, 3, "all 3 rows should survive cancellation");
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_snap(exchange: &str, symbol: &str, ts_us: i64) -> Snapshot1s {
    Snapshot1s {
        ts_us,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        bids: vec![(3000.0, 5.0), (2999.0, 3.0)],
        asks: vec![(3001.0, 4.0), (3002.0, 2.0)],
        mid_px: Some(3000.5),
        microprice: Some(3000.4),
        spread_bps: Some(1.67),
        imbalance_l1: Some(0.11),
        imbalance_l5: Some(0.1),
        imbalance_l10: Some(0.05),
        bid_depth_l5: 8.0,
        bid_depth_l10: 8.0,
        ask_depth_l5: 6.0,
        ask_depth_l10: 6.0,
        ofi_l1: 1.0,
        churn_bid: 5.0,
        churn_ask: 4.0,
        intra_sigma: 0.01,
        open_px: Some(3000.0),
        close_px: Some(3001.0),
        n_events: 7,
        volume_delta: 0.0,
        buy_vol: 0.0,
        sell_vol: 0.0,
        trade_count: 0,
    }
}

fn count_parquet_rows(path: &PathBuf) -> usize {
    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut rows = 0;
    for batch in reader.build().unwrap() {
        rows += batch.unwrap().num_rows();
    }
    rows
}

/// Recursively find all .parquet files under `dir`.
fn find_parquets(dir: &PathBuf) -> Vec<PathBuf> {
    let mut result = Vec::new();
    collect_parquets(dir, &mut result);
    result
}

fn collect_parquets(dir: &PathBuf, acc: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_parquets(&path, acc);
            } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                acc.push(path);
            }
        }
    }
}

// ── Deriv writer ──────────────────────────────────────────────────────────────

fn mk_funding(ts_us: i64) -> DerivEvent {
    DerivEvent::MarkFunding(MarkFunding {
        timestamp_us: ts_us,
        exchange: "binance_perp".to_string(),
        symbol: "ETHUSDT".to_string(),
        mark_px: 3000.12,
        index_px: Some(3000.05),
        funding_rate: 0.0001,
        next_funding_ts: Some(ts_us + 8 * 3600 * 1_000_000),
    })
}

#[tokio::test]
async fn test_deriv_writer_daily_file_per_feed_and_roundtrip() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<DerivEvent>(64);
    let writer = tokio::spawn(run_deriv_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    // Fixed event time → deterministic daily file name (2023-11-14 UTC).
    let ts_us = 1_700_000_000_000_000_i64;
    tx.send(mk_funding(ts_us)).unwrap();
    tx.send(mk_funding(ts_us + 1_000_000)).unwrap();
    tx.send(DerivEvent::OpenInterest(OpenInterest {
        timestamp_us: ts_us,
        exchange: "binance_perp".to_string(),
        symbol: "ETHUSDT".to_string(),
        oi_base: 10_659.509,
        oi_quote: None,
    }))
    .unwrap();
    tx.send(DerivEvent::Liquidation(Liquidation {
        timestamp_us: ts_us,
        exchange: "binance_perp".to_string(),
        symbol: "ETHUSDT".to_string(),
        side: "SELL".to_string(),
        price: 2998.4,
        qty: 0.014,
    }))
    .unwrap();

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(files.len(), 3, "one daily file per feed: {files:?}");
    let by_name = |name: &str| -> PathBuf {
        files
            .iter()
            .find(|f| f.file_name().unwrap().to_str().unwrap() == name)
            .unwrap_or_else(|| panic!("missing {name} in {files:?}"))
            .clone()
    };

    // Layout: deriv/{exchange}/{symbol}/{date}/{feed}.parquet, date from event ts.
    let funding = by_name("funding.parquet");
    assert!(
        funding
            .to_string_lossy()
            .contains("/deriv/binance_perp/ETHUSDT/2023-11-14/"),
        "layout must be deriv/{{exchange}}/{{symbol}}/{{date}}: {funding:?}"
    );
    assert_eq!(count_parquet_rows(&funding), 2);
    assert_eq!(count_parquet_rows(&by_name("oi.parquet")), 1);
    assert_eq!(count_parquet_rows(&by_name("liq.parquet")), 1);

    use arrow_array::{Array, Float64Array, Int64Array, StringArray};

    // funding.parquet values
    let file = std::fs::File::open(&funding).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = reader.schema().clone();
    for col in [
        "timestamp_us",
        "exchange",
        "symbol",
        "mark_px",
        "index_px",
        "funding_rate",
        "next_funding_ts",
    ] {
        schema.field_with_name(col).expect(col);
    }
    let batch = reader.build().unwrap().next().unwrap().unwrap();
    let ts = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ts.value(0), ts_us);
    let mark = batch
        .column_by_name("mark_px")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(mark.value(0), 3000.12);
    let index = batch
        .column_by_name("index_px")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(index.value(0), 3000.05);
    let rate = batch
        .column_by_name("funding_rate")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(rate.value(0), 0.0001);
    let nft = batch
        .column_by_name("next_funding_ts")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(nft.value(0), ts_us + 8 * 3600 * 1_000_000);

    // oi.parquet: oi_quote must be null (None)
    let file = std::fs::File::open(&by_name("oi.parquet")).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let batch = reader.build().unwrap().next().unwrap().unwrap();
    let oi_base = batch
        .column_by_name("oi_base")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(oi_base.value(0), 10_659.509);
    let oi_quote = batch
        .column_by_name("oi_quote")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(oi_quote.is_null(0), "oi_quote None must round-trip as null");

    // liq.parquet values
    let file = std::fs::File::open(&by_name("liq.parquet")).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let batch = reader.build().unwrap().next().unwrap().unwrap();
    let side = batch
        .column_by_name("side")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(side.value(0), "SELL");
    let price = batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(price.value(0), 2998.4);
    let qty = batch
        .column_by_name("qty")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(qty.value(0), 0.014);
}

#[tokio::test]
async fn test_deriv_writer_day_rollover_by_event_time() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<DerivEvent>(64);
    let writer = tokio::spawn(run_deriv_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        fathom::metrics::new_metrics().metrics,
    ));

    // 2023-11-14 22:13 then +27.8h → 2023-11-16: two daily funding files.
    tx.send(mk_funding(1_700_000_000_000_000)).unwrap();
    tx.send(mk_funding(1_700_100_000_000_000)).unwrap();

    drop(tx);
    writer.await.unwrap();

    let files = find_parquets(&dir.path().to_path_buf());
    assert_eq!(files.len(), 2, "day rollover → two files: {files:?}");
    let paths: Vec<String> = files.iter().map(|f| f.to_string_lossy().into()).collect();
    assert!(paths.iter().any(|p| p.contains("/2023-11-14/")));
    assert!(paths.iter().any(|p| p.contains("/2023-11-16/")));
    for f in &files {
        assert_eq!(count_parquet_rows(f), 1);
    }
}

#[tokio::test]
async fn test_deriv_writer_empty_channel() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<DerivEvent>(64);
    let writer = tokio::spawn(run_deriv_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        fathom::metrics::new_metrics().metrics,
    ));
    drop(tx);
    writer.await.unwrap();
    assert!(find_parquets(&dir.path().to_path_buf()).is_empty());
}

// ── Write-health metrics ──────────────────────────────────────────────────────
//
// Verify each writer bumps the write-health metrics (specs/observability.md):
// bytes-written + last-flush timestamp on a successful flush, and
// write_errors_total when a write path fails (injected by planting a regular
// file where the feed's output directory needs to be created).

use fathom::metrics::{Feed, FeedLabel, new_metrics};

fn feed_label(feed: Feed) -> FeedLabel {
    FeedLabel {
        feed: feed.as_str().to_string(),
    }
}

#[tokio::test]
async fn test_raw_writer_records_bytes_and_last_flush() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawDiff>(64);
    let handle = new_metrics();
    let writer = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        handle.metrics.clone(),
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..5u64 {
        tx.send(RawDiff {
            timestamp_us: now_us + i as i64 * 100_000,
            exchange: "binance_spot".to_string(),
            symbol: "ETHUSDT".to_string(),
            seq_id: 101 + i as i64,
            prev_seq_id: 100 + i as i64,
            bids: vec![(3000.0, 5.0)],
            asks: vec![(3001.0, 4.0)],
        })
        .unwrap();
    }
    drop(tx);
    writer.await.unwrap();

    let label = feed_label(Feed::Raw);
    assert!(
        handle
            .metrics
            .parquet_bytes_written_total
            .get_or_create(&label)
            .get()
            > 0,
        "raw feed should record bytes written"
    );
    assert!(
        handle
            .metrics
            .last_flush_timestamp
            .get_or_create(&label)
            .get()
            > 0,
        "raw feed should stamp last flush timestamp"
    );
}

#[tokio::test]
async fn test_raw_writer_records_write_error_on_failure() {
    let dir = TempDir::new().unwrap();
    // Plant a file where `raw/` must become a directory → create_dir_all fails.
    std::fs::write(dir.path().join("raw"), b"not a dir").unwrap();

    let (tx, rx) = broadcast::channel::<RawDiff>(64);
    let handle = new_metrics();
    let writer = tokio::spawn(run_raw_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        handle.metrics.clone(),
    ));

    tx.send(RawDiff {
        timestamp_us: chrono::Utc::now().timestamp_micros(),
        exchange: "binance_spot".to_string(),
        symbol: "ETHUSDT".to_string(),
        seq_id: 101,
        prev_seq_id: 100,
        bids: vec![(3000.0, 5.0)],
        asks: vec![(3001.0, 4.0)],
    })
    .unwrap();
    drop(tx);
    writer.await.unwrap();

    let label = feed_label(Feed::Raw);
    assert!(
        handle
            .metrics
            .write_errors_total
            .get_or_create(&label)
            .get()
            >= 1,
        "raw feed should count the open/create failure"
    );
    assert_eq!(
        handle
            .metrics
            .parquet_bytes_written_total
            .get_or_create(&label)
            .get(),
        0,
        "no bytes should be recorded when the write path fails"
    );
}

#[tokio::test]
async fn test_trades_writer_records_bytes_and_last_flush() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<RawTrade>(64);
    let handle = new_metrics();
    let writer = tokio::spawn(run_trades_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        handle.metrics.clone(),
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..5i64 {
        tx.send(make_trade(
            "binance_spot",
            "ETHUSDT",
            now_us + i * 1_000,
            100 + i,
            false,
        ))
        .unwrap();
    }
    drop(tx);
    writer.await.unwrap();

    let label = feed_label(Feed::Trades);
    assert!(
        handle
            .metrics
            .parquet_bytes_written_total
            .get_or_create(&label)
            .get()
            > 0,
        "trades feed should record bytes written"
    );
    assert!(
        handle
            .metrics
            .last_flush_timestamp
            .get_or_create(&label)
            .get()
            > 0,
        "trades feed should stamp last flush timestamp"
    );
}

#[tokio::test]
async fn test_trades_writer_records_write_error_on_failure() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("trades"), b"not a dir").unwrap();

    let (tx, rx) = broadcast::channel::<RawTrade>(64);
    let handle = new_metrics();
    let writer = tokio::spawn(run_trades_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        1,
        handle.metrics.clone(),
    ));

    tx.send(make_trade(
        "binance_spot",
        "ETHUSDT",
        chrono::Utc::now().timestamp_micros(),
        1,
        false,
    ))
    .unwrap();
    drop(tx);
    writer.await.unwrap();

    let label = feed_label(Feed::Trades);
    assert!(
        handle
            .metrics
            .write_errors_total
            .get_or_create(&label)
            .get()
            >= 1,
        "trades feed should count the open/create failure"
    );
}

#[tokio::test]
async fn test_snap_writer_records_bytes_and_last_flush() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<Snapshot1s>(64);
    let handle = new_metrics();
    let writer = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        rx,
        CancellationToken::new(),
        handle.metrics.clone(),
    ));

    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..3u64 {
        tx.send(make_snap(
            "binance_spot",
            "ETHUSDT",
            now_us + i as i64 * 1_000_000,
        ))
        .unwrap();
    }
    drop(tx);
    writer.await.unwrap();

    let label = feed_label(Feed::Snap1s);
    assert!(
        handle
            .metrics
            .parquet_bytes_written_total
            .get_or_create(&label)
            .get()
            > 0,
        "1s feed should record bytes written"
    );
    assert!(
        handle
            .metrics
            .last_flush_timestamp
            .get_or_create(&label)
            .get()
            > 0,
        "1s feed should stamp last flush timestamp"
    );
}

#[tokio::test]
async fn test_snap_writer_records_write_error_on_failure() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("1s"), b"not a dir").unwrap();

    let (tx, rx) = broadcast::channel::<Snapshot1s>(64);
    let handle = new_metrics();
    let writer = tokio::spawn(run_snap_writer(
        dir.path().to_path_buf(),
        rx,
        CancellationToken::new(),
        handle.metrics.clone(),
    ));

    tx.send(make_snap(
        "binance_spot",
        "ETHUSDT",
        chrono::Utc::now().timestamp_micros(),
    ))
    .unwrap();
    drop(tx);
    writer.await.unwrap();

    let label = feed_label(Feed::Snap1s);
    assert!(
        handle
            .metrics
            .write_errors_total
            .get_or_create(&label)
            .get()
            >= 1,
        "1s feed should count the open/create failure"
    );
}

#[tokio::test]
async fn test_deriv_writer_records_bytes_and_last_flush() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = broadcast::channel::<DerivEvent>(64);
    let handle = new_metrics();
    let writer = tokio::spawn(run_deriv_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        handle.metrics.clone(),
    ));

    let ts_us = 1_700_000_000_000_000_i64;
    tx.send(mk_funding(ts_us)).unwrap();
    tx.send(mk_funding(ts_us + 1_000_000)).unwrap();
    drop(tx);
    writer.await.unwrap();

    let label = feed_label(Feed::Deriv);
    assert!(
        handle
            .metrics
            .parquet_bytes_written_total
            .get_or_create(&label)
            .get()
            > 0,
        "deriv feed should record bytes written"
    );
    assert!(
        handle
            .metrics
            .last_flush_timestamp
            .get_or_create(&label)
            .get()
            > 0,
        "deriv feed should stamp last flush timestamp"
    );
}

#[tokio::test]
async fn test_deriv_writer_records_write_error_on_failure() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("deriv"), b"not a dir").unwrap();

    let (tx, rx) = broadcast::channel::<DerivEvent>(64);
    let handle = new_metrics();
    let writer = tokio::spawn(run_deriv_writer(
        dir.path().to_path_buf(),
        rx,
        1,
        handle.metrics.clone(),
    ));

    tx.send(mk_funding(1_700_000_000_000_000_i64)).unwrap();
    drop(tx);
    writer.await.unwrap();

    let label = feed_label(Feed::Deriv);
    assert!(
        handle
            .metrics
            .write_errors_total
            .get_or_create(&label)
            .get()
            >= 1,
        "deriv feed should count the open/create failure"
    );
}
