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
use chrono::{Timelike, Utc};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::{
    error::Result,
    metrics::{Feed, Metrics},
    schema::trades_schema,
    writer::{batch_bytes, raw::bucket_open},
};

// Re-export from fathom-types crate.
pub use fathom_types::RawTrade;

struct SymbolWriter {
    writer: ArrowWriter<std::fs::File>,
    /// Temp path: trades_HHMM_open.parquet
    temp_path: PathBuf,
    /// UTC hour when this file was opened (bucket open)
    bucket_open_hour: u32,
    /// Formatted HHMM for file renaming on close
    open_hhmm: String,
    buffer: Vec<RawTrade>,
}

impl SymbolWriter {
    fn open(
        dir: &Path,
        symbol: &str,
        exchange: &str,
        now_utc: chrono::DateTime<Utc>,
        rotate_hours: u32,
    ) -> Result<Self> {
        let date_str = now_utc.format("%Y-%m-%d").to_string();
        let bucket = bucket_open(now_utc.hour(), rotate_hours);
        let open_hhmm = format!("{bucket:02}00");

        let sym_dir = dir.join(exchange).join(symbol).join(&date_str);
        std::fs::create_dir_all(&sym_dir)?;

        let temp_path = sym_dir.join(format!("trades_{open_hhmm}_open.parquet"));

        let file = std::fs::File::create(&temp_path)?;
        let schema = SchemaRef::new(trades_schema().clone());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(8192)
            .build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self {
            writer,
            temp_path,
            bucket_open_hour: bucket,
            open_hhmm,
            buffer: Vec::new(),
        })
    }

    fn should_rotate(&self, now_utc: chrono::DateTime<Utc>, rotate_hours: u32) -> bool {
        bucket_open(now_utc.hour(), rotate_hours) != self.bucket_open_hour
    }

    /// Flush any buffered rows, finalize the file, and rename it into place.
    /// Returns the bytes recorded by the final buffer flush.
    fn close_and_rename(&mut self, end_utc: chrono::DateTime<Utc>) -> Result<u64> {
        let bytes = self.flush_buffer()?;
        self.writer.finish()?;

        let end_hhmm = format!("{:02}{:02}", end_utc.hour(), end_utc.minute());
        #[allow(clippy::unwrap_used)] // temp_path is always dir/.../file.parquet
        let new_path = self
            .temp_path
            .parent()
            .unwrap()
            .join(format!("trades_{}_{}.parquet", self.open_hhmm, end_hhmm));

        std::fs::rename(&self.temp_path, &new_path)?;
        info!(
            from = %self.temp_path.display(),
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
    mut rx: broadcast::Receiver<RawTrade>,
    flush_interval_s: u64,
    rotate_hours: u32,
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
                let now_utc = Utc::now();

                if let Some(sw) = writers.get_mut(&key)
                    && sw.should_rotate(now_utc, rotate_hours)
                {
                    match sw.close_and_rename(now_utc) {
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
                    writers.remove(&key);
                }

                if !writers.contains_key(&key) {
                    match SymbolWriter::open(
                        &data_dir.join("trades"),
                        &trade.symbol,
                        &trade.exchange,
                        now_utc,
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

    // Graceful shutdown: flush all and finalize
    let now_utc = Utc::now();
    for (_, mut sw) in writers {
        match sw.close_and_rename(now_utc) {
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
