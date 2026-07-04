//! Derivatives Parquet writer — funding/mark, open interest, liquidations
//! (specs/derivatives-feeds.md).
//!
//! Layout: `{data_dir}/deriv/{exchange}/{symbol}/{date}/{feed}_HHMM_HHMM.parquet`
//! with `feed` ∈ {funding, oi, liq} — hourly-rotated files (`rotate_hours`,
//! shared with raw/trades/1s), written via the temp-file-then-rename `Bucket`
//! pattern in `writer::rotation`. Bounds restart data loss to a single open
//! bucket instead of an entire day.
//!
//! Lifecycle:
//! - Bucket rollover on event-time hour/date boundary, like `raw.rs`/`trades.rs`.
//! - Additionally, a periodic tick (driven by `Clock`, real wall-clock in
//!   production) force-closes any writer whose bucket has expired even with
//!   no new events — needed because sparse feeds (e.g. `liq`) can go silent
//!   for hours and would otherwise hold an `_open.parquet` file well past its
//!   bucket boundary, an orphaned file on crash that the uploader never picks
//!   up (roadmap.md: "deriv time-based rollover: sporadic feed holds file
//!   open >2 days").
//! - Row buffer drained on a periodic flush interval (default 5 s), like
//!   `raw.rs` — plus an explicit `ArrowWriter::flush()` each time so row
//!   groups hit disk. Without it, at deriv rates the 8192-row group would sit
//!   in memory for hours and a crash would lose the whole bucket. Tiny row
//!   groups are an acceptable trade at this volume.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow_schema::SchemaRef;
use chrono::{DateTime, Utc};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::{
    error::Result,
    metrics::{Feed, Metrics},
    schema::{liquidation_schema, mark_funding_schema, open_interest_schema},
    writer::{batch_bytes, rotation},
};

// Re-export from fathom-types crate.
pub use fathom_types::{Liquidation, MarkFunding, OpenInterest};
// Rotation infrastructure shared with snap_1s.rs.
pub use rotation::{Clock, SystemClock};

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

/// Derive UTC event time from a timestamp in microseconds.
fn event_time_from_ts_us(ts_us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(ts_us).unwrap_or_else(Utc::now)
}

/// One open hourly bucket for a single (exchange, symbol, feed).
/// The buffer only ever holds the variant matching `feed` — the writer map is
/// keyed by feed, so mixing is impossible by construction.
struct FeedWriter {
    writer: ArrowWriter<std::fs::File>,
    bucket: rotation::Bucket,
    /// Timestamp of the most recently written event — used as the `as_of`
    /// passed to `close_and_rename`, never wall-clock (see module doc).
    last_event_time: DateTime<Utc>,
    buffer: Vec<DerivEvent>,
}

impl FeedWriter {
    fn open(
        dir: &Path,
        first: &DerivEvent,
        as_of: DateTime<Utc>,
        rotate_hours: u32,
    ) -> Result<Self> {
        let bucket = rotation::Bucket::open(
            dir,
            first.exchange(),
            first.symbol(),
            first.feed(),
            as_of,
            rotate_hours,
        )?;

        let file = std::fs::File::create(&bucket.temp_path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(8192)
            .build();
        let writer = ArrowWriter::try_new(file, first.schema(), Some(props))?;

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

    /// Write buffered rows to the Parquet writer. Returns the batch byte estimate
    /// (0 when the buffer is empty).
    fn flush_buffer(&mut self) -> Result<u64> {
        if self.buffer.is_empty() {
            return Ok(0);
        }
        let batch = build_batch(&self.buffer)?;
        let bytes = batch_bytes(&batch);
        self.writer.write(&batch)?;
        // Force the row group to disk — see module doc for why.
        self.writer.flush()?;
        self.buffer.clear();
        Ok(bytes)
    }

    fn close(mut self) -> Result<u64> {
        let bytes = self.flush_buffer()?;
        self.writer.finish()?;
        let final_path = self.bucket.close_and_rename(self.last_event_time)?;
        info!(path = %final_path.display(), "closed deriv writer");
        Ok(bytes)
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

/// Close a feed writer and record write-health metrics. Shared by the
/// event-driven rollover, the periodic force-rotate, and shutdown sites.
fn close_writer(fw: FeedWriter, metrics: &Metrics) {
    match fw.close() {
        Ok(bytes) => {
            metrics.parquet_writes_total.inc();
            if bytes > 0 {
                metrics.record_flush(Feed::Deriv, bytes);
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to close deriv writer");
            metrics.parquet_write_errors_total.inc();
            metrics.record_write_error(Feed::Deriv);
        }
    }
}

/// Derivatives Parquet writer — receives DerivEvent via broadcast channel,
/// buffers per (exchange, symbol, feed), flushes every `flush_interval_s`,
/// rotates hourly (event time). Supervised as fatal in main.
pub async fn run_deriv_writer(
    data_dir: PathBuf,
    rx: broadcast::Receiver<DerivEvent>,
    flush_interval_s: u64,
    metrics: std::sync::Arc<Metrics>,
) {
    run_deriv_writer_configured(
        data_dir,
        rx,
        flush_interval_s,
        1,
        Arc::new(SystemClock),
        metrics,
    )
    .await;
}

/// Testable/production entry point with configurable rotation granularity and
/// an injectable `Clock` for the periodic force-rotate check. `main.rs` calls
/// this with `cfg.raw_rotate_hours` and the real `SystemClock`.
#[doc(hidden)]
pub async fn run_deriv_writer_configured(
    data_dir: PathBuf,
    mut rx: broadcast::Receiver<DerivEvent>,
    flush_interval_s: u64,
    rotate_hours: u32,
    clock: Arc<dyn Clock>,
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
                let event_time = event_time_from_ts_us(event.timestamp_us());

                // Rollover: close the finished bucket's file (footer + rename),
                // a new one opens below.
                //
                // The nested ifs cannot be collapsed into a let-chain:
                // `writers.get(&key)` borrows immutably and the inner
                // `writers.remove(&key)` needs a mutable borrow.
                #[allow(clippy::collapsible_if)]
                if let Some(fw) = writers.get(&key) {
                    if fw.should_rotate(event_time, rotate_hours)
                        && let Some(old) = writers.remove(&key)
                    {
                        close_writer(old, &metrics);
                    }
                }

                if !writers.contains_key(&key) {
                    match FeedWriter::open(
                        &data_dir.join("deriv"),
                        &event,
                        event_time,
                        rotate_hours,
                    ) {
                        Ok(fw) => {
                            writers.insert(key.clone(), fw);
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to open deriv writer");
                            metrics.parquet_write_errors_total.inc();
                            metrics.record_write_error(Feed::Deriv);
                            continue;
                        }
                    }
                }

                if let Some(fw) = writers.get_mut(&key) {
                    fw.last_event_time = event_time;
                    fw.buffer.push(event);
                }
            }
            Err(_timeout) => {} // just continue to check flush
        }

        // Periodic flush
        if last_flush.elapsed() >= flush_dur {
            for fw in writers.values_mut() {
                match fw.flush_buffer() {
                    Ok(bytes) => {
                        metrics.parquet_writes_total.inc();
                        if bytes > 0 {
                            metrics.record_flush(Feed::Deriv, bytes);
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "deriv flush error");
                        metrics.parquet_write_errors_total.inc();
                        metrics.record_write_error(Feed::Deriv);
                    }
                }
            }
            last_flush = tokio::time::Instant::now();

            // Force-rotate any bucket whose window has elapsed even with no
            // new events (see module doc — sparse feeds like `liq`).
            let now = clock.now();
            let stale_keys: Vec<String> = writers
                .iter()
                .filter(|(_, fw)| fw.should_rotate(now, rotate_hours))
                .map(|(k, _)| k.clone())
                .collect();
            for key in stale_keys {
                if let Some(old) = writers.remove(&key) {
                    close_writer(old, &metrics);
                }
            }
        }
    }

    // Graceful shutdown: flush all and finalize footers
    for (_, fw) in writers {
        close_writer(fw, &metrics);
    }
    info!("deriv_writer shutdown complete");
}
