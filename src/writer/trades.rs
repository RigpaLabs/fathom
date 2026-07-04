//! Trades Parquet writer — raw trade tape (specs/trades-feed.md).
//!
//! Same hourly-rotation pattern as `raw.rs`, flat columns instead of lists:
//! `{data_dir}/trades/{exchange}/{symbol}/{date}/trades_HHMM_HHMM.parquet`.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow_schema::SchemaRef;
use chrono::{DateTime, Utc};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::{
    error::Result,
    metrics::{Feed, Metrics},
    schema::trades_schema,
    writer::{batch_bytes, rotation},
};

// Re-export from fathom-types crate.
pub use fathom_types::RawTrade;

/// Derive UTC event time from a timestamp in microseconds. Used only for the
/// `close_and_rename` marker (see `SymbolWriter::last_event_time`) — the
/// rotation *trigger* below is wall-clock, not event-time (see `raw.rs`'s
/// module doc, which this writer mirrors).
fn event_time_from_ts_us(ts_us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(ts_us).unwrap_or_else(Utc::now)
}

struct SymbolWriter {
    writer: ArrowWriter<std::fs::File>,
    bucket: rotation::Bucket,
    /// Timestamp of the most recently written event — used as the `as_of`
    /// passed to `Bucket::close_and_rename` so the file's end-HHMM reflects
    /// the data, never the wall-clock time the process happened to close it.
    /// The rotation *trigger* (`should_rotate` below) stays wall-clock-based,
    /// same rationale as `raw.rs`.
    last_event_time: DateTime<Utc>,
    buffer: Vec<RawTrade>,
}

impl SymbolWriter {
    fn open(
        dir: &Path,
        symbol: &str,
        exchange: &str,
        as_of: DateTime<Utc>,
        rotate_hours: u32,
    ) -> Result<Self> {
        let bucket = rotation::Bucket::open(dir, exchange, symbol, "trades", as_of, rotate_hours)?;

        let file = std::fs::File::create(&bucket.temp_path)?;
        let schema = SchemaRef::new(trades_schema().clone());
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
            "rotated trades file"
        );
        Ok(bytes)
    }

    /// Write buffered rows to the Parquet writer. Returns the batch byte estimate
    /// (0 when the buffer is empty).
    fn flush_buffer(&mut self) -> Result<u64> {
        if self.buffer.is_empty() {
            return Ok(0);
        }
        let schema = SchemaRef::new(trades_schema().clone());

        let timestamps: Vec<i64> = self.buffer.iter().map(|t| t.timestamp_us).collect();
        let exchanges: Vec<&str> = self.buffer.iter().map(|t| t.exchange.as_str()).collect();
        let symbols: Vec<&str> = self.buffer.iter().map(|t| t.symbol.as_str()).collect();
        let trade_ids: Vec<i64> = self.buffer.iter().map(|t| t.trade_id).collect();
        let prices: Vec<f64> = self.buffer.iter().map(|t| t.price).collect();
        let qtys: Vec<f64> = self.buffer.iter().map(|t| t.qty).collect();
        let buyer_makers: Vec<bool> = self.buffer.iter().map(|t| t.is_buyer_maker).collect();

        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(StringArray::from(exchanges)) as ArrayRef,
                Arc::new(StringArray::from(symbols)) as ArrayRef,
                Arc::new(Int64Array::from(trade_ids)) as ArrayRef,
                Arc::new(Float64Array::from(prices)) as ArrayRef,
                Arc::new(Float64Array::from(qtys)) as ArrayRef,
                Arc::new(BooleanArray::from(buyer_makers)) as ArrayRef,
            ],
        )?;

        let bytes = batch_bytes(&batch);
        self.writer.write(&batch)?;
        self.buffer.clear();
        Ok(bytes)
    }
}

/// Trades Parquet writer — receives RawTrade via broadcast channel, buffers,
/// flushes periodically, rotates on hour boundary. Mirrors `run_raw_writer`.
pub async fn run_trades_writer(
    data_dir: PathBuf,
    rx: broadcast::Receiver<RawTrade>,
    flush_interval_s: u64,
    rotate_hours: u32,
    metrics: std::sync::Arc<Metrics>,
) {
    run_trades_writer_with_clock(
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
/// rotation trigger — mirrors `raw.rs::run_raw_writer_with_clock`. Production
/// always uses `SystemClock` via `run_trades_writer`.
#[doc(hidden)]
pub async fn run_trades_writer_with_clock(
    data_dir: PathBuf,
    mut rx: broadcast::Receiver<RawTrade>,
    flush_interval_s: u64,
    rotate_hours: u32,
    clock: Arc<dyn rotation::Clock>,
    metrics: std::sync::Arc<Metrics>,
) {
    let mut writers: HashMap<String, SymbolWriter> = HashMap::new();
    let mut last_flush = tokio::time::Instant::now();
    let flush_dur = std::time::Duration::from_secs(flush_interval_s);

    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await {
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                // All senders dropped → graceful shutdown
                break;
            }
            Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                warn!("trades_writer lagged by {n} messages");
                continue;
            }
            Ok(Ok(trade)) => {
                let key = format!("{}:{}", trade.exchange, trade.symbol);
                let now = clock.now();
                let event_time = event_time_from_ts_us(trade.timestamp_us);

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
                                    metrics.record_flush(Feed::Trades, bytes);
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to rotate trades file");
                                metrics.parquet_write_errors_total.inc();
                                metrics.record_write_error(Feed::Trades);
                            }
                        }
                    }
                }

                if !writers.contains_key(&key) {
                    match SymbolWriter::open(
                        &data_dir.join("trades"),
                        &trade.symbol,
                        &trade.exchange,
                        now,
                        rotate_hours,
                    ) {
                        Ok(sw) => {
                            writers.insert(key.clone(), sw);
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to open trades writer");
                            metrics.parquet_write_errors_total.inc();
                            metrics.record_write_error(Feed::Trades);
                            continue;
                        }
                    }
                }

                if let Some(sw) = writers.get_mut(&key) {
                    sw.last_event_time = event_time;
                    sw.buffer.push(trade);
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
                            metrics.record_flush(Feed::Trades, bytes);
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "trades flush error");
                        metrics.parquet_write_errors_total.inc();
                        metrics.record_write_error(Feed::Trades);
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
                    metrics.record_flush(Feed::Trades, bytes);
                }
            }
            Err(e) => {
                warn!(error = %e, "shutdown: failed to finalize trades file");
                metrics.parquet_write_errors_total.inc();
                metrics.record_write_error(Feed::Trades);
            }
        }
    }
    info!("trades_writer shutdown complete");
}
