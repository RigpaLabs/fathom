use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, Float32Array, Float64Array, Int64Array, StringArray, UInt32Array,
};
use arrow_schema::SchemaRef;
use chrono::Utc;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::WriterProperties,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{accumulator::Snapshot1s, error::Result, schema::snap_1s_schema};

struct DayWriter {
    writer: ArrowWriter<std::fs::File>,
    date_str: String,
    path: PathBuf,
    buffer: Vec<Snapshot1s>,
}

impl DayWriter {
    fn open(dir: &Path, exchange: &str, symbol: &str, date_str: &str) -> Result<Self> {
        let sym_dir = dir.join("1s").join(exchange).join(symbol);
        std::fs::create_dir_all(&sym_dir)?;
        let path = sym_dir.join(format!("{date_str}.parquet"));

        let file = std::fs::File::create(&path)?;
        let schema = SchemaRef::new(snap_1s_schema().clone());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self {
            writer,
            date_str: date_str.to_string(),
            path,
            buffer: Vec::new(),
        })
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let schema = SchemaRef::new(snap_1s_schema().clone());
        let _n = self.buffer.len();

        let ts_us: Vec<i64> = self.buffer.iter().map(|s| s.ts_us).collect();
        let exchanges: Vec<&str> = self.buffer.iter().map(|s| s.exchange.as_str()).collect();
        let symbols: Vec<&str> = self.buffer.iter().map(|s| s.symbol.as_str()).collect();

        // Helper: extract top-N price or size from bids/asks
        let bid_px: Vec<[Option<f64>; 10]> = self.buffer.iter().map(|s| {
            let mut arr = [None; 10];
            for (i, (px, _)) in s.bids.iter().enumerate().take(10) { arr[i] = Some(*px); }
            arr
        }).collect();
        let bid_sz: Vec<[Option<f64>; 10]> = self.buffer.iter().map(|s| {
            let mut arr = [None; 10];
            for (i, (_, sz)) in s.bids.iter().enumerate().take(10) { arr[i] = Some(*sz); }
            arr
        }).collect();
        let ask_px: Vec<[Option<f64>; 10]> = self.buffer.iter().map(|s| {
            let mut arr = [None; 10];
            for (i, (px, _)) in s.asks.iter().enumerate().take(10) { arr[i] = Some(*px); }
            arr
        }).collect();
        let ask_sz: Vec<[Option<f64>; 10]> = self.buffer.iter().map(|s| {
            let mut arr = [None; 10];
            for (i, (_, sz)) in s.asks.iter().enumerate().take(10) { arr[i] = Some(*sz); }
            arr
        }).collect();

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

        let mid_px: Vec<Option<f64>> = self.buffer.iter().map(|s| s.mid_px).collect();
        let microprice: Vec<Option<f64>> = self.buffer.iter().map(|s| s.microprice).collect();
        let spread_bps: Vec<Option<f32>> = self.buffer.iter().map(|s| s.spread_bps).collect();
        let imb_l1: Vec<Option<f32>> = self.buffer.iter().map(|s| s.imbalance_l1).collect();
        let imb_l5: Vec<Option<f32>> = self.buffer.iter().map(|s| s.imbalance_l5).collect();
        let imb_l10: Vec<Option<f32>> = self.buffer.iter().map(|s| s.imbalance_l10).collect();
        let bid_d5: Vec<f64> = self.buffer.iter().map(|s| s.bid_depth_l5).collect();
        let bid_d10: Vec<f64> = self.buffer.iter().map(|s| s.bid_depth_l10).collect();
        let ask_d5: Vec<f64> = self.buffer.iter().map(|s| s.ask_depth_l5).collect();
        let ask_d10: Vec<f64> = self.buffer.iter().map(|s| s.ask_depth_l10).collect();
        let ofi: Vec<f64> = self.buffer.iter().map(|s| s.ofi_l1).collect();
        let churn_bid: Vec<f64> = self.buffer.iter().map(|s| s.churn_bid).collect();
        let churn_ask: Vec<f64> = self.buffer.iter().map(|s| s.churn_ask).collect();
        let sigma: Vec<f32> = self.buffer.iter().map(|s| s.intra_sigma).collect();
        let open_px: Vec<Option<f64>> = self.buffer.iter().map(|s| s.open_px).collect();
        let close_px: Vec<Option<f64>> = self.buffer.iter().map(|s| s.close_px).collect();
        let n_events: Vec<u32> = self.buffer.iter().map(|s| s.n_events).collect();

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
        ]);

        let batch = arrow_array::RecordBatch::try_new(schema, columns)?;
        self.writer.write(&batch)?;
        self.buffer.clear();
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.finish()?;
        info!(path = %self.path.display(), "closed 1s writer");
        Ok(())
    }
}

/// 1s snapshot writer — one daily file per (exchange, symbol), flush on each row.
pub async fn run_snap_writer(data_dir: PathBuf, mut rx: mpsc::Receiver<Snapshot1s>) {
    let mut writers: HashMap<String, DayWriter> = HashMap::new();

    loop {
        match rx.recv().await {
            None => break,
            Some(snap) => {
                let now_utc = Utc::now();
                let date_str = now_utc.format("%Y-%m-%d").to_string();
                let key = format!("{}:{}", snap.exchange, snap.symbol);
                let exchange = snap.exchange.clone();
                let symbol = snap.symbol.clone();

                // Day rollover check.
                // The nested ifs cannot be collapsed into a let-chain: `writers.get(&key)`
                // borrows immutably and the inner `writers.remove(&key)` needs a mutable
                // borrow — the borrow checker would reject the flat form.
                #[allow(clippy::collapsible_if)]
                if let Some(dw) = writers.get(&key) {
                    if dw.date_str != date_str {
                        if let Some(old) = writers.remove(&key) {
                            if let Err(e) = old.close() {
                                warn!(error = %e, "failed to close old snap writer");
                            }
                        }
                    }
                }

                // Open writer if needed
                if !writers.contains_key(&key) {
                    match DayWriter::open(&data_dir, &exchange, &symbol, &date_str) {
                        Ok(dw) => { writers.insert(key.clone(), dw); }
                        Err(e) => { warn!(error = %e, "failed to open snap writer"); continue; }
                    }
                }

                if let Some(dw) = writers.get_mut(&key) {
                    dw.buffer.push(snap);
                    // Flush immediately — 1 row/sec per symbol, no buffering needed
                    if let Err(e) = dw.flush() {
                        warn!(error = %e, "snap flush error");
                    }
                }
            }
        }
    }

    // Graceful shutdown
    for (_, dw) in writers {
        if let Err(e) = dw.close() {
            warn!(error = %e, "shutdown: failed to close snap writer");
        }
    }
    info!("snap_writer shutdown complete");
}
