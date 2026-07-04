use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, Int64Array, ListArray, StringArray,
    builder::{Float64Builder, ListBuilder},
};
use arrow_schema::SchemaRef;
use chrono::{DateTime, Utc};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::{
    error::Result,
    metrics::{Feed, Metrics},
    schema::raw_schema,
    writer::{batch_bytes, rotation},
};

// Re-export from fathom-types crate.
pub use fathom_types::RawDiff;

/// Bucket-boundary math lives in `writer::rotation` — shared with `snap_1s.rs`
/// and `deriv.rs`. Re-exported here since `trades.rs` and existing tests
/// import it as `raw::bucket_open`.
pub use crate::writer::rotation::bucket_open;

/// Derive UTC event time from a timestamp in microseconds. Used only for the
/// `close_and_rename` marker (see `SymbolWriter::last_event_time`) — the
/// rotation *trigger* below is wall-clock, not event-time (see module doc).
fn event_time_from_ts_us(ts_us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(ts_us).unwrap_or_else(Utc::now)
}

struct SymbolWriter {
    writer: ArrowWriter<std::fs::File>,
    bucket: rotation::Bucket,
    /// Timestamp of the most recently written event — used as the `as_of`
    /// passed to `Bucket::close_and_rename` so the file's end-HHMM reflects
    /// the data, never the wall-clock time the process happened to close it.
    /// The rotation *trigger* (`should_rotate` below) stays wall-clock-based
    /// — raw diffs arrive at high frequency, so wall-clock "now" and the true
    /// last-event-time are already virtually identical in practice, and
    /// wall-clock triggering (unlike snap_1s.rs/deriv.rs's event-time
    /// triggering) is what lets this writer rotate even during a total
    /// upstream silence. Only the close-marker needs to reflect the data.
    last_event_time: DateTime<Utc>,
    buffer: Vec<RawDiff>,
}

impl SymbolWriter {
    fn open(
        dir: &Path,
        symbol: &str,
        exchange: &str,
        as_of: DateTime<Utc>,
        rotate_hours: u32,
    ) -> Result<Self> {
        let bucket = rotation::Bucket::open(dir, exchange, symbol, "depth", as_of, rotate_hours)?;

        let file = std::fs::File::create(&bucket.temp_path)?;
        let schema = SchemaRef::new(raw_schema().clone());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(8192)
            .build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self {
            writer,
            bucket,
            last_event_time: as_of,
            buffer: Vec::new(),
        })
    }

    fn should_rotate(&self, as_of: DateTime<Utc>, rotate_hours: u32) -> bool {
        self.bucket.should_rotate(as_of, rotate_hours)
    }

    /// Flush any buffered rows, finalize the file, and rename it into place
    /// using the tracked last-event-time. Returns the bytes recorded by the
    /// final buffer flush.
    fn close(mut self) -> Result<u64> {
        let bytes = self.flush_buffer()?;
        self.writer.finish()?;
        let new_path = self.bucket.close_and_rename(self.last_event_time)?;
        info!(
            from = %self.bucket.temp_path.display(),
            to = %new_path.display(),
            "rotated raw file"
        );
        Ok(bytes)
    }

    /// Write buffered rows to the Parquet writer. Returns the batch byte estimate
    /// (0 when the buffer is empty).
    fn flush_buffer(&mut self) -> Result<u64> {
        if self.buffer.is_empty() {
            return Ok(0);
        }
        let schema = SchemaRef::new(raw_schema().clone());

        let timestamps: Vec<i64> = self.buffer.iter().map(|e| e.timestamp_us).collect();
        let exchanges: Vec<&str> = self.buffer.iter().map(|e| e.exchange.as_str()).collect();
        let symbols: Vec<&str> = self.buffer.iter().map(|e| e.symbol.as_str()).collect();
        let seq_ids: Vec<i64> = self.buffer.iter().map(|e| e.seq_id).collect();
        let prev_seq_ids: Vec<i64> = self.buffer.iter().map(|e| e.prev_seq_id).collect();

        let bid_prices = build_list_f64(self.buffer.iter().map(|e| e.bids.iter().map(|(p, _)| *p)));
        let bid_qtys = build_list_f64(self.buffer.iter().map(|e| e.bids.iter().map(|(_, q)| *q)));
        let ask_prices = build_list_f64(self.buffer.iter().map(|e| e.asks.iter().map(|(p, _)| *p)));
        let ask_qtys = build_list_f64(self.buffer.iter().map(|e| e.asks.iter().map(|(_, q)| *q)));

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(StringArray::from(exchanges)) as ArrayRef,
                Arc::new(StringArray::from(symbols)) as ArrayRef,
                Arc::new(Int64Array::from(seq_ids)) as ArrayRef,
                Arc::new(Int64Array::from(prev_seq_ids)) as ArrayRef,
                Arc::new(bid_prices) as ArrayRef,
                Arc::new(bid_qtys) as ArrayRef,
                Arc::new(ask_prices) as ArrayRef,
                Arc::new(ask_qtys) as ArrayRef,
            ],
        )?;

        let bytes = batch_bytes(&batch);
        self.writer.write(&batch)?;
        self.buffer.clear();
        Ok(bytes)
    }
}

fn build_list_f64<I, J>(rows: I) -> ListArray
where
    I: Iterator<Item = J>,
    J: Iterator<Item = f64>,
{
    let mut builder = ListBuilder::new(Float64Builder::new());
    for row in rows {
        let values = builder.values();
        for v in row {
            values.append_value(v);
        }
        builder.append(true);
    }
    builder.finish()
}

/// Raw Parquet writer — receives RawDiff via broadcast channel, buffers, flushes periodically, rotates on hour boundary.
pub async fn run_raw_writer(
    data_dir: PathBuf,
    rx: broadcast::Receiver<RawDiff>,
    flush_interval_s: u64,
    rotate_hours: u32,
    metrics: std::sync::Arc<Metrics>,
) {
    run_raw_writer_with_clock(
        data_dir,
        rx,
        flush_interval_s,
        rotate_hours,
        Arc::new(rotation::SystemClock),
        metrics,
    )
    .await;
}

/// Testable entry point with an injectable `Clock` driving the wall-clock
/// rotation trigger — lets tests simulate hour/date rollovers deterministically
/// instead of racing real wall-clock time. Production always uses
/// `SystemClock` via `run_raw_writer`, so the run loop's behavior is unchanged.
#[doc(hidden)]
pub async fn run_raw_writer_with_clock(
    data_dir: PathBuf,
    mut rx: broadcast::Receiver<RawDiff>,
    flush_interval_s: u64,
    rotate_hours: u32,
    clock: Arc<dyn rotation::Clock>,
    metrics: std::sync::Arc<Metrics>,
) {
    let mut writers: HashMap<String, SymbolWriter> = HashMap::new();
    let mut last_flush = tokio::time::Instant::now();
    let flush_dur = std::time::Duration::from_secs(flush_interval_s);

    loop {
        // Try to receive with timeout so we can flush periodically
        match tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await {
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                // All senders dropped → graceful shutdown
                break;
            }
            Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                warn!("raw_writer lagged by {n} messages");
                continue;
            }
            Ok(Ok(event)) => {
                let key = format!("{}:{}", event.exchange, event.symbol);
                let now = clock.now();
                let event_time = event_time_from_ts_us(event.timestamp_us);
                let exchange = event.exchange.clone();
                let symbol = event.symbol.clone();

                // Rollover check — wall-clock triggered (see module doc).
                //
                // The nested ifs cannot be collapsed into a let-chain:
                // `writers.get(&key)` borrows immutably and the inner
                // `writers.remove(&key)` needs a mutable borrow.
                #[allow(clippy::collapsible_if)]
                if let Some(sw) = writers.get(&key) {
                    if sw.should_rotate(now, rotate_hours)
                        && let Some(old) = writers.remove(&key)
                    {
                        match old.close() {
                            Ok(bytes) => {
                                metrics.parquet_writes_total.inc();
                                if bytes > 0 {
                                    metrics.record_flush(Feed::Raw, bytes);
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to rotate raw file");
                                metrics.parquet_write_errors_total.inc();
                                metrics.record_write_error(Feed::Raw);
                            }
                        }
                    }
                }

                // Open writer if needed
                if !writers.contains_key(&key) {
                    match SymbolWriter::open(
                        &data_dir.join("raw"),
                        &symbol,
                        &exchange,
                        now,
                        rotate_hours,
                    ) {
                        Ok(sw) => {
                            writers.insert(key.clone(), sw);
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to open raw writer");
                            metrics.parquet_write_errors_total.inc();
                            metrics.record_write_error(Feed::Raw);
                            continue;
                        }
                    }
                }

                if let Some(sw) = writers.get_mut(&key) {
                    sw.last_event_time = event_time;
                    sw.buffer.push(event);
                }
            }
            Err(_timeout) => {} // just continue to check flush
        }

        // Periodic flush
        if last_flush.elapsed() >= flush_dur {
            for sw in writers.values_mut() {
                match sw.flush_buffer() {
                    Ok(bytes) => {
                        metrics.parquet_writes_total.inc();
                        if bytes > 0 {
                            metrics.record_flush(Feed::Raw, bytes);
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "raw flush error");
                        metrics.parquet_write_errors_total.inc();
                        metrics.record_write_error(Feed::Raw);
                    }
                }
            }
            last_flush = tokio::time::Instant::now();
        }
    }

    // Graceful shutdown: flush all and finalize (each writer's own tracked
    // last_event_time is used as the close marker, not wall-clock).
    for (_, sw) in writers {
        match sw.close() {
            Ok(bytes) => {
                metrics.parquet_writes_total.inc();
                if bytes > 0 {
                    metrics.record_flush(Feed::Raw, bytes);
                }
            }
            Err(e) => {
                warn!(error = %e, "shutdown: failed to finalize raw file");
                metrics.parquet_write_errors_total.inc();
                metrics.record_write_error(Feed::Raw);
            }
        }
    }
    info!("raw_writer shutdown complete");
}
