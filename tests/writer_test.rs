/// Writer unit tests: verify Parquet files are created and contain correct data.
///
/// Strategy: spawn writer tasks, send N records, drop channel (graceful shutdown),
/// await task completion, read Parquet back and assert.
use std::path::PathBuf;

use chrono::Timelike;
use fathom::{
    accumulator::Snapshot1s,
    writer::{
        raw::RawDiff,
        raw::{bucket_open, run_raw_writer},
        snap_1s::run_snap_writer,
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use tokio::sync::mpsc;

// ── Raw writer ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_raw_writer_creates_file() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    let (tx, rx) = mpsc::channel::<RawDiff>(64);

    // Spawn writer with very short flush interval (1s for test)
    let writer = tokio::spawn(run_raw_writer(data_dir.clone(), rx, 1, 1));

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
        .await
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
    let (tx, rx) = mpsc::channel::<RawDiff>(64);
    let writer = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), rx, 1, 1));

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
            .await
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
    let (tx, rx) = mpsc::channel::<RawDiff>(64);
    let writer = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), rx, 1, 1));

    // Close immediately without sending anything
    drop(tx);
    writer.await.unwrap();

    // No files should be created
    let files = find_parquets(&dir.path().to_path_buf());
    assert!(files.is_empty(), "empty channel → no parquet files");
}

// ── Snap 1s writer ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_snap_writer_creates_file() {
    let dir = TempDir::new().unwrap();
    let (tx, rx) = mpsc::channel::<Snapshot1s>(64);
    let writer = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), rx));

    let now_us = chrono::Utc::now().timestamp_micros();
    for i in 0..3u64 {
        tx.send(make_snap(
            "binance_spot",
            "ETHUSDT",
            now_us + i as i64 * 1_000_000,
        ))
        .await
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
    let (tx, rx) = mpsc::channel::<Snapshot1s>(64);
    let writer = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), rx));

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
    })
    .await
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

    // n_events is last column
    use arrow_array::UInt32Array;
    let n_events = batch
        .column(batch.num_columns() - 1)
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
    let (tx, rx) = mpsc::channel::<Snapshot1s>(64);
    let writer = tokio::spawn(run_snap_writer(dir.path().to_path_buf(), rx));

    let now_us = chrono::Utc::now().timestamp_micros();
    for sym in &["ETHUSDT", "BTCUSDT", "BNBUSDT"] {
        tx.send(make_snap("binance_spot", sym, now_us))
            .await
            .unwrap();
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
    let (tx, rx) = mpsc::channel::<RawDiff>(64);
    let writer = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), rx, 1, 1));

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
    .await
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
    let (tx, rx) = mpsc::channel::<RawDiff>(64);
    let writer = tokio::spawn(run_raw_writer(dir.path().to_path_buf(), rx, 1, 6));

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
    .await
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
    }
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
