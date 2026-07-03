//! Derivatives Parquet writer — funding/mark, open interest, liquidations
//! (specs/derivatives-feeds.md).
//!
//! Layout: `{data_dir}/deriv/{exchange}/{symbol}/{date}/{feed}.parquet` with
//! `feed` ∈ {funding, oi, liq} — DAILY files, no hourly rotation (rates are
//! ~1 row/s at most; a day of funding is single-digit MB).
//!
//! Lifecycle composition (deliberate mix of the two existing writer patterns):
//! - Daily file keyed by *event time* with rollover on date change, like
//!   `snap_1s.rs` — no `_open`/rename dance, the filename is stable from open.
//! - Row buffer drained on a periodic flush interval (default 5 s), like
//!   `raw.rs` — plus an explicit `ArrowWriter::flush()` each time so row
//!   groups hit disk. Without it, at deriv rates the 8192-row group would sit
//!   in memory for hours and a crash would lose the whole day. Tiny row groups
//!   are an acceptable trade at this volume.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow_schema::SchemaRef;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::{
    error::Result,
    metrics::Metrics,
    schema::{liquidation_schema, mark_funding_schema, open_interest_schema},
};

// Re-export from fathom-types crate.
pub use fathom_types::{Liquidation, MarkFunding, OpenInterest};

/// Internal fan-out envelope for the three derivatives structs.
///
/// One broadcast channel instead of three: producers are per-connection,
/// and both consumers (Parquet writer, NATS sink) want all three feeds.
/// The NATS wire format has no type discriminant, so the publisher routes
/// each variant to its own subject and wire-encodes the *inner* struct —
/// `DerivEvent` itself never crosses a process boundary.
#[derive(Debug, Clone)]
pub enum DerivEvent {
    MarkFunding(MarkFunding),
    OpenInterest(OpenInterest),
    Liquidation(Liquidation),
}

impl DerivEvent {
    pub fn exchange(&self) -> &str {
        match self {
            Self::MarkFunding(e) => &e.exchange,
            Self::OpenInterest(e) => &e.exchange,
            Self::Liquidation(e) => &e.exchange,
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::MarkFunding(e) => &e.symbol,
            Self::OpenInterest(e) => &e.symbol,
            Self::Liquidation(e) => &e.symbol,
        }
    }

    pub fn timestamp_us(&self) -> i64 {
        match self {
            Self::MarkFunding(e) => e.timestamp_us,
            Self::OpenInterest(e) => e.timestamp_us,
            Self::Liquidation(e) => e.timestamp_us,
        }
    }

    /// Feed name — Parquet file stem and NATS subject suffix.
    pub fn feed(&self) -> &'static str {
        match self {
            Self::MarkFunding(_) => "funding",
            Self::OpenInterest(_) => "oi",
            Self::Liquidation(_) => "liq",
        }
    }

    fn schema(&self) -> SchemaRef {
        match self {
            Self::MarkFunding(_) => SchemaRef::new(mark_funding_schema().clone()),
            Self::OpenInterest(_) => SchemaRef::new(open_interest_schema().clone()),
            Self::Liquidation(_) => SchemaRef::new(liquidation_schema().clone()),
        }
    }
}

/// Derive UTC date string from event timestamp in microseconds.
fn date_from_ts_us(ts_us: i64) -> String {
    chrono::DateTime::from_timestamp_micros(ts_us)
        .unwrap_or_else(chrono::Utc::now)
        .format("%Y-%m-%d")
        .to_string()
}

/// One open daily file for a single (exchange, symbol, feed).
/// The buffer only ever holds the variant matching `feed` — the writer map is
/// keyed by feed, so mixing is impossible by construction.
struct FeedWriter {
    writer: ArrowWriter<std::fs::File>,
    date_str: String,
    path: PathBuf,
    buffer: Vec<DerivEvent>,
}

impl FeedWriter {
    fn open(dir: &Path, first: &DerivEvent, date_str: &str) -> Result<Self> {
        let sym_dir = dir
            .join(first.exchange())
            .join(first.symbol())
            .join(date_str);
        std::fs::create_dir_all(&sym_dir)?;
        let path = sym_dir.join(format!("{}.parquet", first.feed()));

        let file = std::fs::File::create(&path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(8192)
            .build();
        let writer = ArrowWriter::try_new(file, first.schema(), Some(props))?;

        Ok(Self {
            writer,
            date_str: date_str.to_string(),
            path,
            buffer: Vec::new(),
        })
    }

    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = build_batch(&self.buffer)?;
        self.writer.write(&batch)?;
        // Force the row group to disk — see module doc for why.
        self.writer.flush()?;
        self.buffer.clear();
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.flush_buffer()?;
        self.writer.finish()?;
        info!(path = %self.path.display(), "closed deriv writer");
        Ok(())
    }
}

/// Build a RecordBatch for a homogeneous buffer (all rows one variant).
fn build_batch(buffer: &[DerivEvent]) -> Result<arrow_array::RecordBatch> {
    #[allow(clippy::unwrap_used)] // callers never pass an empty buffer
    let schema = buffer.first().unwrap().schema();

    let ts: Vec<i64> = buffer.iter().map(DerivEvent::timestamp_us).collect();
    let exchanges: Vec<&str> = buffer.iter().map(DerivEvent::exchange).collect();
    let symbols: Vec<&str> = buffer.iter().map(DerivEvent::symbol).collect();

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ts)) as ArrayRef,
        Arc::new(StringArray::from(exchanges)) as ArrayRef,
        Arc::new(StringArray::from(symbols)) as ArrayRef,
    ];

    #[allow(clippy::unwrap_used)] // homogeneity is enforced by the writer key
    match buffer.first().unwrap() {
        DerivEvent::MarkFunding(_) => {
            let rows: Vec<&MarkFunding> = buffer
                .iter()
                .filter_map(|e| match e {
                    DerivEvent::MarkFunding(m) => Some(m),
                    _ => None,
                })
                .collect();
            let mark: Vec<f64> = rows.iter().map(|m| m.mark_px).collect();
            let index: Vec<Option<f64>> = rows.iter().map(|m| m.index_px).collect();
            let rate: Vec<f64> = rows.iter().map(|m| m.funding_rate).collect();
            let next: Vec<Option<i64>> = rows.iter().map(|m| m.next_funding_ts).collect();
            columns.extend([
                Arc::new(Float64Array::from(mark)) as ArrayRef,
                Arc::new(Float64Array::from(index)) as ArrayRef,
                Arc::new(Float64Array::from(rate)) as ArrayRef,
                Arc::new(Int64Array::from(next)) as ArrayRef,
            ]);
        }
        DerivEvent::OpenInterest(_) => {
            let rows: Vec<&OpenInterest> = buffer
                .iter()
                .filter_map(|e| match e {
                    DerivEvent::OpenInterest(o) => Some(o),
                    _ => None,
                })
                .collect();
            let base: Vec<f64> = rows.iter().map(|o| o.oi_base).collect();
            let quote: Vec<Option<f64>> = rows.iter().map(|o| o.oi_quote).collect();
            columns.extend([
                Arc::new(Float64Array::from(base)) as ArrayRef,
                Arc::new(Float64Array::from(quote)) as ArrayRef,
            ]);
        }
        DerivEvent::Liquidation(_) => {
            let rows: Vec<&Liquidation> = buffer
                .iter()
                .filter_map(|e| match e {
                    DerivEvent::Liquidation(l) => Some(l),
                    _ => None,
                })
                .collect();
            let side: Vec<&str> = rows.iter().map(|l| l.side.as_str()).collect();
            let price: Vec<f64> = rows.iter().map(|l| l.price).collect();
            let qty: Vec<f64> = rows.iter().map(|l| l.qty).collect();
            columns.extend([
                Arc::new(StringArray::from(side)) as ArrayRef,
                Arc::new(Float64Array::from(price)) as ArrayRef,
                Arc::new(Float64Array::from(qty)) as ArrayRef,
            ]);
        }
    }

    Ok(arrow_array::RecordBatch::try_new(schema, columns)?)
}

/// Derivatives Parquet writer — receives DerivEvent via broadcast channel,
/// buffers per (exchange, symbol, feed), flushes every `flush_interval_s`,
/// rolls files on UTC date change (event time). Supervised as fatal in main.
pub async fn run_deriv_writer(
    data_dir: PathBuf,
    mut rx: broadcast::Receiver<DerivEvent>,
    flush_interval_s: u64,
    metrics: std::sync::Arc<Metrics>,
) {
    let mut writers: HashMap<String, FeedWriter> = HashMap::new();
    let mut last_flush = tokio::time::Instant::now();
    let flush_dur = std::time::Duration::from_secs(flush_interval_s);

    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await {
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                // All senders dropped → graceful shutdown
                break;
            }
            Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                warn!("deriv_writer lagged by {n} messages");
                continue;
            }
            Ok(Ok(event)) => {
                let key = format!("{}:{}:{}", event.exchange(), event.symbol(), event.feed());
                let date_str = date_from_ts_us(event.timestamp_us());

                // Day rollover: close the finished day's file (footer written),
                // a new one opens below.
                if writers.get(&key).is_some_and(|fw| fw.date_str != date_str)
                    && let Some(old) = writers.remove(&key)
                {
                    match old.close() {
                        Ok(()) => {
                            metrics.parquet_writes_total.inc();
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to close deriv writer on rollover");
                            metrics.parquet_write_errors_total.inc();
                        }
                    }
                }

                if !writers.contains_key(&key) {
                    match FeedWriter::open(&data_dir.join("deriv"), &event, &date_str) {
                        Ok(fw) => {
                            writers.insert(key.clone(), fw);
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to open deriv writer");
                            continue;
                        }
                    }
                }

                if let Some(fw) = writers.get_mut(&key) {
                    fw.buffer.push(event);
                }
            }
            Err(_timeout) => {} // just continue to check flush
        }

        // Periodic flush
        if last_flush.elapsed() >= flush_dur {
            for fw in writers.values_mut() {
                match fw.flush_buffer() {
                    Ok(()) => {
                        metrics.parquet_writes_total.inc();
                    }
                    Err(e) => {
                        warn!(error = %e, "deriv flush error");
                        metrics.parquet_write_errors_total.inc();
                    }
                }
            }
            last_flush = tokio::time::Instant::now();
        }
    }

    // Graceful shutdown: flush all and finalize footers
    for (_, fw) in writers {
        match fw.close() {
            Ok(()) => {
                metrics.parquet_writes_total.inc();
            }
            Err(e) => {
                warn!(error = %e, "shutdown: failed to close deriv writer");
                metrics.parquet_write_errors_total.inc();
            }
        }
    }
    info!("deriv_writer shutdown complete");
}
