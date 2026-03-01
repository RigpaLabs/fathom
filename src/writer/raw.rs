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
use chrono::Utc;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{error::Result, orderbook::DepthDiff, schema::raw_schema};

/// A raw diff event ready to be written to Parquet.
#[derive(Debug, Clone)]
pub struct RawDiff {
    pub timestamp_us: i64,
    pub exchange: String,
    pub symbol: String,
    pub seq_id: i64,
    pub prev_seq_id: i64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

impl From<DepthDiff> for RawDiff {
    fn from(d: DepthDiff) -> Self {
        Self {
            timestamp_us: d.timestamp_us,
            exchange: d.exchange,
            symbol: d.symbol,
            seq_id: d.seq_id,
            prev_seq_id: d.prev_seq_id,
            bids: d.bids,
            asks: d.asks,
        }
    }
}

/// Which 6-hour bucket does a UTC hour belong to?
/// Returns the bucket open hour: 0, 6, 12, or 18.
fn bucket_open(hour: u32) -> u32 {
    (hour / 6) * 6
}

struct SymbolWriter {
    writer: ArrowWriter<std::fs::File>,
    /// Temp path: depth_HHMM_open.parquet
    temp_path: PathBuf,
    /// UTC hour when this file was opened (bucket open)
    bucket_open_hour: u32,
    /// Formatted HHMM for file renaming on close
    open_hhmm: String,
    buffer: Vec<RawDiff>,
}

impl SymbolWriter {
    fn open(
        dir: &Path,
        symbol: &str,
        exchange: &str,
        now_utc: chrono::DateTime<Utc>,
    ) -> Result<Self> {
        let date_str = now_utc.format("%Y-%m-%d").to_string();
        let open_hour = now_utc.hour();
        let bucket = bucket_open(open_hour);
        let open_hhmm = format!("{:02}{:02}", bucket, 0);

        let sym_dir = dir.join(exchange).join(symbol).join(&date_str);
        std::fs::create_dir_all(&sym_dir)?;

        let temp_path = sym_dir.join(format!("depth_{open_hhmm}_open.parquet"));

        let file = std::fs::File::create(&temp_path)?;
        let schema = SchemaRef::new(raw_schema().clone());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
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

    fn should_rotate(&self, now_utc: chrono::DateTime<Utc>) -> bool {
        bucket_open(now_utc.hour()) != self.bucket_open_hour
    }

    fn close_and_rename(&mut self, end_utc: chrono::DateTime<Utc>) -> Result<()> {
        // Flush buffer first
        if !self.buffer.is_empty() {
            self.flush_buffer()?;
        }
        self.writer.finish()?;

        let end_hhmm = format!("{:02}{:02}", end_utc.hour(), end_utc.minute());
        let new_path = self
            .temp_path
            .parent()
            .unwrap()
            .join(format!("depth_{}_{}.parquet", self.open_hhmm, end_hhmm));

        std::fs::rename(&self.temp_path, &new_path)?;
        info!(
            from = %self.temp_path.display(),
            to = %new_path.display(),
            "rotated raw file"
        );
        Ok(())
    }

    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
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

        self.writer.write(&batch)?;
        self.buffer.clear();
        Ok(())
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

/// Raw Parquet writer — receives RawDiff via channel, buffers, flushes every 5 min, rotates every 6h.
pub async fn run_raw_writer(
    data_dir: PathBuf,
    mut rx: mpsc::Receiver<RawDiff>,
    flush_interval_s: u64,
) {
    let mut writers: HashMap<String, SymbolWriter> = HashMap::new();
    let mut last_flush = tokio::time::Instant::now();
    let flush_dur = std::time::Duration::from_secs(flush_interval_s);

    loop {
        // Try to receive with timeout so we can flush periodically
        match tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await {
            Ok(None) => {
                // Channel closed → graceful shutdown
                break;
            }
            Ok(Some(event)) => {
                let key = format!("{}:{}", event.exchange, event.symbol);
                let now_utc = Utc::now();
                let exchange = event.exchange.clone();
                let symbol = event.symbol.clone();

                // Check rotation
                if let Some(sw) = writers.get_mut(&key)
                    && sw.should_rotate(now_utc)
                {
                    if let Err(e) = sw.close_and_rename(now_utc) {
                        warn!(error = %e, "failed to rotate raw file");
                    }
                    writers.remove(&key);
                }

                // Open writer if needed
                if !writers.contains_key(&key) {
                    match SymbolWriter::open(&data_dir.join("raw"), &symbol, &exchange, now_utc) {
                        Ok(sw) => {
                            writers.insert(key.clone(), sw);
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to open raw writer");
                            continue;
                        }
                    }
                }

                if let Some(sw) = writers.get_mut(&key) {
                    sw.buffer.push(event);
                }
            }
            Err(_timeout) => {} // just continue to check flush
        }

        // Periodic flush
        if last_flush.elapsed() >= flush_dur {
            for sw in writers.values_mut() {
                if let Err(e) = sw.flush_buffer() {
                    warn!(error = %e, "raw flush error");
                }
            }
            last_flush = tokio::time::Instant::now();
        }
    }

    // Graceful shutdown: flush all and finalize
    let now_utc = Utc::now();
    for (_, mut sw) in writers {
        if let Err(e) = sw.close_and_rename(now_utc) {
            warn!(error = %e, "shutdown: failed to finalize raw file");
        }
    }
    info!("raw_writer shutdown complete");
}

use chrono::Timelike;
