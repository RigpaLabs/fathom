use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, Float32Array, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use chrono::{DateTime, Utc};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    accumulator::Snapshot1s,
    error::Result,
    metrics::{Feed, Metrics},
    schema::snap_1s_schema,
    writer::{batch_bytes, rotation::Bucket},
};

/// Derive event time (UTC) from a timestamp in microseconds.
fn datetime_from_ts_us(ts_us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(ts_us).unwrap_or_else(Utc::now)
}

/// Build an Arrow RecordBatch from a slice of Snapshot1s rows.
fn build_snap_record_batch(buffer: &[Snapshot1s]) -> Result<arrow_array::RecordBatch> {
    let schema = SchemaRef::new(snap_1s_schema().clone());

    let ts_us: Vec<i64> = buffer.iter().map(|s| s.ts_us).collect();
    let exchanges: Vec<&str> = buffer.iter().map(|s| s.exchange.as_str()).collect();
    let symbols: Vec<&str> = buffer.iter().map(|s| s.symbol.as_str()).collect();

    // Helper: extract top-N price or size from bids/asks
    let bid_px: Vec<[Option<f64>; 10]> = buffer
        .iter()
        .map(|s| {
            let mut arr = [None; 10];
            for (i, (px, _)) in s.bids.iter().enumerate().take(10) {
                arr[i] = Some(*px);
            }
            arr
        })
        .collect();
    let bid_sz: Vec<[Option<f64>; 10]> = buffer
        .iter()
        .map(|s| {
            let mut arr = [None; 10];
            for (i, (_, sz)) in s.bids.iter().enumerate().take(10) {
                arr[i] = Some(*sz);
            }
            arr
        })
        .collect();
    let ask_px: Vec<[Option<f64>; 10]> = buffer
        .iter()
        .map(|s| {
            let mut arr = [None; 10];
            for (i, (px, _)) in s.asks.iter().enumerate().take(10) {
                arr[i] = Some(*px);
            }
            arr
        })
        .collect();
    let ask_sz: Vec<[Option<f64>; 10]> = buffer
        .iter()
        .map(|s| {
            let mut arr = [None; 10];
            for (i, (_, sz)) in s.asks.iter().enumerate().take(10) {
                arr[i] = Some(*sz);
            }
            arr
        })
        .collect();

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ts_us)) as ArrayRef,
        Arc::new(StringArray::from(exchanges)) as ArrayRef,
        Arc::new(StringArray::from(symbols)) as ArrayRef,
    ];

    // bid_px_0..9
    for i in 0..10 {
        let col: Vec<Option<f64>> = bid_px.iter().map(|r| r[i]).collect();
        columns.push(Arc::new(Float64Array::from(col)) as ArrayRef);
    }
    // ask_px_0..9
    for i in 0..10 {
        let col: Vec<Option<f64>> = ask_px.iter().map(|r| r[i]).collect();
        columns.push(Arc::new(Float64Array::from(col)) as ArrayRef);
    }
    // bid_sz_0..9
    for i in 0..10 {
        let col: Vec<Option<f64>> = bid_sz.iter().map(|r| r[i]).collect();
        columns.push(Arc::new(Float64Array::from(col)) as ArrayRef);
    }
    // ask_sz_0..9
    for i in 0..10 {
        let col: Vec<Option<f64>> = ask_sz.iter().map(|r| r[i]).collect();
        columns.push(Arc::new(Float64Array::from(col)) as ArrayRef);
    }

    let mid_px: Vec<Option<f64>> = buffer.iter().map(|s| s.mid_px).collect();
    let microprice: Vec<Option<f64>> = buffer.iter().map(|s| s.microprice).collect();
    let spread_bps: Vec<Option<f32>> = buffer.iter().map(|s| s.spread_bps).collect();
    let imb_l1: Vec<Option<f32>> = buffer.iter().map(|s| s.imbalance_l1).collect();
    let imb_l5: Vec<Option<f32>> = buffer.iter().map(|s| s.imbalance_l5).collect();
    let imb_l10: Vec<Option<f32>> = buffer.iter().map(|s| s.imbalance_l10).collect();
    let bid_d5: Vec<f64> = buffer.iter().map(|s| s.bid_depth_l5).collect();
    let bid_d10: Vec<f64> = buffer.iter().map(|s| s.bid_depth_l10).collect();
    let ask_d5: Vec<f64> = buffer.iter().map(|s| s.ask_depth_l5).collect();
    let ask_d10: Vec<f64> = buffer.iter().map(|s| s.ask_depth_l10).collect();
    let ofi: Vec<f64> = buffer.iter().map(|s| s.ofi_l1).collect();
    let churn_bid: Vec<f64> = buffer.iter().map(|s| s.churn_bid).collect();
    let churn_ask: Vec<f64> = buffer.iter().map(|s| s.churn_ask).collect();
    let sigma: Vec<f32> = buffer.iter().map(|s| s.intra_sigma).collect();
    let open_px: Vec<Option<f64>> = buffer.iter().map(|s| s.open_px).collect();
    let close_px: Vec<Option<f64>> = buffer.iter().map(|s| s.close_px).collect();
    let n_events: Vec<u32> = buffer.iter().map(|s| s.n_events).collect();
    let volume_delta: Vec<f64> = buffer.iter().map(|s| s.volume_delta).collect();
    let buy_vol: Vec<f64> = buffer.iter().map(|s| s.buy_vol).collect();
    let sell_vol: Vec<f64> = buffer.iter().map(|s| s.sell_vol).collect();
    let trade_count: Vec<u32> = buffer.iter().map(|s| s.trade_count).collect();

    columns.extend([
        Arc::new(Float64Array::from(mid_px)) as ArrayRef,
        Arc::new(Float64Array::from(microprice)) as ArrayRef,
        Arc::new(Float32Array::from(spread_bps)) as ArrayRef,
        Arc::new(Float32Array::from(imb_l1)) as ArrayRef,
        Arc::new(Float32Array::from(imb_l5)) as ArrayRef,
        Arc::new(Float32Array::from(imb_l10)) as ArrayRef,
        Arc::new(Float64Array::from(bid_d5)) as ArrayRef,
        Arc::new(Float64Array::from(bid_d10)) as ArrayRef,
        Arc::new(Float64Array::from(ask_d5)) as ArrayRef,
        Arc::new(Float64Array::from(ask_d10)) as ArrayRef,
        Arc::new(Float64Array::from(ofi)) as ArrayRef,
        Arc::new(Float64Array::from(churn_bid)) as ArrayRef,
        Arc::new(Float64Array::from(churn_ask)) as ArrayRef,
        Arc::new(Float32Array::from(sigma)) as ArrayRef,
        Arc::new(Float64Array::from(open_px)) as ArrayRef,
        Arc::new(Float64Array::from(close_px)) as ArrayRef,
        Arc::new(UInt32Array::from(n_events)) as ArrayRef,
        Arc::new(Float64Array::from(volume_delta)) as ArrayRef,
        Arc::new(Float64Array::from(buy_vol)) as ArrayRef,
        Arc::new(Float64Array::from(sell_vol)) as ArrayRef,
        Arc::new(UInt32Array::from(trade_count)) as ArrayRef,
    ]);

    Ok(arrow_array::RecordBatch::try_new(schema, columns)?)
}

/// Encode a buffer of snapshots into an in-memory Parquet blob (Snappy-compressed).
///
/// Used by benchmarks to measure the full Arrow→Parquet encode path without disk I/O
/// variance. Returns the raw Parquet bytes.
#[doc(hidden)]
pub fn write_snap_to_memory(buffer: &[Snapshot1s]) -> Result<Vec<u8>> {
    use std::io::Cursor;
    let cursor = Cursor::new(Vec::new());
    let schema = SchemaRef::new(snap_1s_schema().clone());
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(4096)
        .build();
    let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))?;
    if !buffer.is_empty() {
        let batch = build_snap_record_batch(buffer)?;
        writer.write(&batch)?;
        writer.flush()?;
    }
    let cursor = writer.into_inner()?;
    Ok(cursor.into_inner())
}

/// Flush the Parquet row group to disk every this many rows.
/// At 1 row/sec per symbol this equals 5 minutes.
/// Keeps memory bounded and limits data loss on crash to ~5 min.
const DEFAULT_DISK_FLUSH_INTERVAL: usize = 300;

/// Default hourly rotation window, matching the other three writers'
/// implicit behavior before `rotate_hours` was threaded through from config.
const DEFAULT_ROTATE_HOURS: u32 = 1;

/// Generic over the underlying sink so tests can inject a failing writer to
/// exercise the `close()` error path (an already-open `std::fs::File` fd cannot
/// be made to fail its writes via filesystem tricks). Production always uses
/// `std::fs::File` (the default type parameter), so the run loop is unchanged
/// and there is no runtime dispatch cost.
struct DayWriter<W: std::io::Write + Send = std::fs::File> {
    writer: ArrowWriter<W>,
    bucket: Bucket,
    /// Timestamp of the last event written — used (never wall-clock) as the
    /// `as_of` for `close_and_rename`, so the filename's end-HHMM reflects
    /// what's actually in the data.
    last_event_time: DateTime<Utc>,
    buffer: Vec<Snapshot1s>,
    rows_since_disk_flush: usize,
    disk_flush_interval: usize,
}

impl DayWriter<std::fs::File> {
    fn open(
        dir: &Path,
        exchange: &str,
        symbol: &str,
        as_of: DateTime<Utc>,
        rotate_hours: u32,
        disk_flush_interval: usize,
    ) -> Result<Self> {
        let bucket = Bucket::open(
            &dir.join("1s"),
            exchange,
            symbol,
            "snap",
            as_of,
            rotate_hours,
        )?;

        let file = std::fs::File::create(&bucket.temp_path)?;
        let schema = SchemaRef::new(snap_1s_schema().clone());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(4096)
            .build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self {
            writer,
            bucket,
            last_event_time: as_of,
            buffer: Vec::new(),
            rows_since_disk_flush: 0,
            disk_flush_interval,
        })
    }
}

impl<W: std::io::Write + Send> DayWriter<W> {
    fn should_rotate(&self, as_of: DateTime<Utc>, rotate_hours: u32) -> bool {
        self.bucket.should_rotate(as_of, rotate_hours)
    }

    /// Write buffered rows to the Parquet writer. Returns the batch byte estimate
    /// (0 when the buffer is empty).
    fn flush(&mut self) -> Result<u64> {
        if self.buffer.is_empty() {
            return Ok(0);
        }
        let batch = build_snap_record_batch(&self.buffer)?;
        let n = batch.num_rows();
        let bytes = batch_bytes(&batch);
        self.writer.write(&batch)?;
        self.buffer.clear();

        self.rows_since_disk_flush += n;
        if self.rows_since_disk_flush >= self.disk_flush_interval {
            self.writer.flush()?;
            self.rows_since_disk_flush = 0;
            info!(path = %self.bucket.temp_path.display(), rows = self.disk_flush_interval, "flushed 1s row group");
        }
        Ok(bytes)
    }

    fn close(mut self) -> Result<u64> {
        let bytes = self.flush()?;
        self.writer.finish()?;
        let final_path = self.bucket.close_and_rename(self.last_event_time)?;
        info!(path = %final_path.display(), "closed 1s writer");
        Ok(bytes)
    }
}

/// Close a day writer (rollover or shutdown) and record write-health metrics:
/// success → `parquet_writes_total` + `record_flush`; failure → both error
/// counters. Shared by both close sites so the metric wiring has one home.
fn close_and_record<W: std::io::Write + Send>(dw: DayWriter<W>, metrics: &Metrics) {
    match dw.close() {
        Ok(bytes) => {
            metrics.parquet_writes_total.inc();
            if bytes > 0 {
                metrics.record_flush(Feed::Snap1s, bytes);
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to close snap writer");
            metrics.parquet_write_errors_total.inc();
            metrics.record_write_error(Feed::Snap1s);
        }
    }
}

/// 1s snapshot writer — hourly-rotated file per (exchange, symbol), flush on
/// each row. `rotate_hours` defaults to 1 via [`run_snap_writer`]; production
/// wires it to `cfg.raw_rotate_hours` through [`run_snap_writer_configured`].
pub async fn run_snap_writer(
    data_dir: PathBuf,
    rx: broadcast::Receiver<Snapshot1s>,
    cancel: CancellationToken,
    metrics: std::sync::Arc<Metrics>,
) {
    run_snap_writer_inner(
        data_dir,
        rx,
        DEFAULT_DISK_FLUSH_INTERVAL,
        DEFAULT_ROTATE_HOURS,
        cancel,
        metrics,
    )
    .await;
}

/// Testable entry point with configurable disk flush interval.
#[doc(hidden)]
pub async fn run_snap_writer_with_flush_interval(
    data_dir: PathBuf,
    rx: broadcast::Receiver<Snapshot1s>,
    disk_flush_interval: usize,
    cancel: CancellationToken,
    metrics: std::sync::Arc<Metrics>,
) {
    run_snap_writer_inner(
        data_dir,
        rx,
        disk_flush_interval,
        DEFAULT_ROTATE_HOURS,
        cancel,
        metrics,
    )
    .await;
}

/// Testable/production entry point with configurable rotation window. Used by
/// `main.rs` (with `cfg.raw_rotate_hours`) and by restart-safety tests.
#[doc(hidden)]
pub async fn run_snap_writer_configured(
    rotate_hours: u32,
    data_dir: PathBuf,
    rx: broadcast::Receiver<Snapshot1s>,
    cancel: CancellationToken,
    metrics: std::sync::Arc<Metrics>,
) {
    run_snap_writer_inner(
        data_dir,
        rx,
        DEFAULT_DISK_FLUSH_INTERVAL,
        rotate_hours,
        cancel,
        metrics,
    )
    .await;
}

/// Handle one snapshot: rollover check (event-time based), open-if-missing,
/// buffer + flush. Shared by the main receive loop and the drain loop below
/// so there is exactly one code path for "what happens to an event" — before
/// this was extracted, the drain loop skipped the rollover check entirely,
/// meaning an event for the next bucket landing during drain would be
/// written into the previous (wrong) bucket's writer.
fn handle_snap_event(
    writers: &mut HashMap<String, DayWriter>,
    data_dir: &Path,
    rotate_hours: u32,
    disk_flush_interval: usize,
    snap: Snapshot1s,
    metrics: &Metrics,
) {
    // Partition by event time, not wall-clock.
    let event_time = datetime_from_ts_us(snap.ts_us);
    let key = format!("{}:{}", snap.exchange, snap.symbol);
    let exchange = snap.exchange.clone();
    let symbol = snap.symbol.clone();

    // Rollover check.
    // The nested ifs cannot be collapsed into a let-chain: `writers.get(&key)`
    // borrows immutably and the inner `writers.remove(&key)` needs a mutable
    // borrow — the borrow checker would reject the flat form.
    #[allow(clippy::collapsible_if)]
    if let Some(dw) = writers.get(&key) {
        if dw.should_rotate(event_time, rotate_hours) {
            if let Some(old) = writers.remove(&key) {
                close_and_record(old, metrics);
            }
        }
    }

    // Open writer if needed.
    if !writers.contains_key(&key) {
        match DayWriter::open(
            data_dir,
            &exchange,
            &symbol,
            event_time,
            rotate_hours,
            disk_flush_interval,
        ) {
            Ok(dw) => {
                writers.insert(key.clone(), dw);
            }
            Err(e) => {
                warn!(error = %e, "failed to open snap writer");
                metrics.parquet_write_errors_total.inc();
                metrics.record_write_error(Feed::Snap1s);
                return;
            }
        }
    }

    if let Some(dw) = writers.get_mut(&key) {
        dw.last_event_time = event_time;
        dw.buffer.push(snap);
        // Flush immediately — 1 row/sec per symbol, no buffering needed
        match dw.flush() {
            Ok(bytes) => {
                metrics.parquet_writes_total.inc();
                if bytes > 0 {
                    metrics.record_flush(Feed::Snap1s, bytes);
                }
            }
            Err(e) => {
                warn!(error = %e, "snap flush error");
                metrics.parquet_write_errors_total.inc();
                metrics.record_write_error(Feed::Snap1s);
            }
        }
    }
}

async fn run_snap_writer_inner(
    data_dir: PathBuf,
    mut rx: broadcast::Receiver<Snapshot1s>,
    disk_flush_interval: usize,
    rotate_hours: u32,
    cancel: CancellationToken,
    metrics: std::sync::Arc<Metrics>,
) {
    let mut writers: HashMap<String, DayWriter> = HashMap::new();

    loop {
        let recv_result = tokio::select! {
            r = rx.recv() => r,
            _ = cancel.cancelled() => break,
        };
        match recv_result {
            Err(broadcast::error::RecvError::Closed) => break,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("snap_writer lagged by {n} messages");
                continue;
            }
            Ok(snap) => {
                handle_snap_event(
                    &mut writers,
                    &data_dir,
                    rotate_hours,
                    disk_flush_interval,
                    snap,
                    &metrics,
                );
            }
        }
    }

    // Drain in-flight messages buffered before cancellation/channel close
    while let Ok(snap) = rx.try_recv() {
        handle_snap_event(
            &mut writers,
            &data_dir,
            rotate_hours,
            disk_flush_interval,
            snap,
            &metrics,
        );
    }

    // Graceful shutdown — close all writers (writes Parquet footers)
    for (_, dw) in writers {
        close_and_record(dw, &metrics);
    }
    info!("snap_writer shutdown complete");
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::metrics::{Feed, FeedLabel, new_metrics};

    use super::*;

    /// A sink that writes fine until `armed`, then fails every write/flush.
    /// Lets a test drive `DayWriter::close()` into its error path — the footer
    /// write in `ArrowWriter::finish()` fails — which no filesystem trick can do
    /// to an already-open `std::fs::File` fd.
    #[derive(Clone)]
    struct ArmedFailWriter {
        armed: Arc<AtomicBool>,
    }

    impl Write for ArmedFailWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.armed.load(Ordering::Relaxed) {
                Err(std::io::Error::other("injected write failure"))
            } else {
                Ok(buf.len())
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if self.armed.load(Ordering::Relaxed) {
                Err(std::io::Error::other("injected flush failure"))
            } else {
                Ok(())
            }
        }
    }

    /// Regression test for the day-rollover close path: when closing the old
    /// day's writer fails, `close_and_record` (used by the rollover and shutdown
    /// sites) must bump both error counters for the 1s feed. Previously the
    /// rollover close only logged a warning, so a persistent close failure was
    /// invisible to alerting.
    #[test]
    fn close_failure_records_write_error() {
        let armed = Arc::new(AtomicBool::new(false));
        let sink = ArmedFailWriter {
            armed: armed.clone(),
        };
        let schema = SchemaRef::new(snap_1s_schema().clone());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        // try_new writes the file magic — must succeed, so arm only afterward.
        let writer = ArrowWriter::try_new(sink, schema, Some(props)).unwrap();
        let tmp = tempfile::TempDir::new().unwrap();
        #[allow(clippy::expect_used)]
        let as_of = "2025-01-15T00:00:00Z"
            .parse::<DateTime<Utc>>()
            .expect("valid fixed timestamp");
        let bucket = Bucket::open(
            &tmp.path().join("1s"),
            "binance_spot",
            "ETHUSDT",
            "snap",
            as_of,
            1,
        )
        .unwrap();
        let dw = DayWriter {
            writer,
            bucket,
            last_event_time: as_of,
            buffer: Vec::new(),
            rows_since_disk_flush: 0,
            disk_flush_interval: 300,
        };

        let handle = new_metrics();
        armed.store(true, Ordering::Relaxed); // now finish()'s footer write fails
        close_and_record(dw, &handle.metrics);

        let label = FeedLabel {
            feed: Feed::Snap1s.as_str().to_string(),
        };
        assert_eq!(
            handle
                .metrics
                .write_errors_total
                .get_or_create(&label)
                .get(),
            1,
            "close failure must increment write_errors_total{{1s}}"
        );
        assert_eq!(
            handle.metrics.parquet_write_errors_total.get(),
            1,
            "close failure must also increment the legacy error counter"
        );
        assert_eq!(
            handle
                .metrics
                .parquet_bytes_written_total
                .get_or_create(&label)
                .get(),
            0,
            "a failed close records no bytes"
        );
    }
}
