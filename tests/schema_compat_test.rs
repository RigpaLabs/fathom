/// Verifies Arrow schema definitions and Parquet roundtrip compatibility.
use std::sync::Arc;

use arrow_array::{Float32Array, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use fathom::schema::{raw_schema, snap_1s_schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use tempfile::NamedTempFile;

// ── Schema field counts ──────────────────────────────────────────────────────

#[test]
fn test_raw_schema_fields() {
    let schema = raw_schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "timestamp_us",
            "exchange",
            "symbol",
            "seq_id",
            "prev_seq_id",
            "bid_prices",
            "bid_qtys",
            "ask_prices",
            "ask_qtys"
        ]
    );
    assert_eq!(schema.fields().len(), 9);
}

#[test]
fn test_snap_1s_schema_fields() {
    let schema = snap_1s_schema();
    // 3 base + 10 bid_px + 10 ask_px + 10 bid_sz + 10 ask_sz + 17 derived = 60
    assert_eq!(schema.fields().len(), 60);

    // Check first three
    assert_eq!(schema.field(0).name(), "ts_us");
    assert_eq!(schema.field(1).name(), "exchange");
    assert_eq!(schema.field(2).name(), "symbol");

    // Check price columns
    for i in 0..10 {
        let _ = schema
            .field_with_name(&format!("bid_px_{i}"))
            .expect("bid_px");
        let _ = schema
            .field_with_name(&format!("ask_px_{i}"))
            .expect("ask_px");
        let _ = schema
            .field_with_name(&format!("bid_sz_{i}"))
            .expect("bid_sz");
        let _ = schema
            .field_with_name(&format!("ask_sz_{i}"))
            .expect("ask_sz");
    }

    // Check derived fields present
    for name in &[
        "mid_px",
        "microprice",
        "spread_bps",
        "imbalance_l1",
        "imbalance_l5",
        "imbalance_l10",
        "ofi_l1",
        "churn_bid",
        "churn_ask",
        "intra_sigma",
        "open_px",
        "close_px",
        "n_events",
    ] {
        schema.field_with_name(name).expect(name);
    }
}

#[test]
fn test_schema_is_singleton() {
    // OnceLock: same pointer both calls
    let a = raw_schema() as *const _;
    let b = raw_schema() as *const _;
    assert_eq!(a, b, "raw_schema() should return same instance");

    let c = snap_1s_schema() as *const _;
    let d = snap_1s_schema() as *const _;
    assert_eq!(c, d, "snap_1s_schema() should return same instance");
}

// ── Raw schema Parquet roundtrip ─────────────────────────────────────────────

#[test]
fn test_raw_schema_parquet_roundtrip() {
    use arrow_array::{
        ArrayRef, ListArray,
        builder::{Float64Builder, ListBuilder},
    };

    let schema = SchemaRef::new(raw_schema().clone());
    let tmp = NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();

    // Write
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    fn list_f64(rows: &[&[f64]]) -> ListArray {
        let mut builder = ListBuilder::new(Float64Builder::new());
        for row in rows {
            let vals = builder.values();
            for &v in *row {
                vals.append_value(v);
            }
            builder.append(true);
        }
        builder.finish()
    }

    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_000_000_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["binance_spot"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ETHUSDT"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![101_i64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
            Arc::new(list_f64(&[&[3000.0, 2999.0]])) as ArrayRef,
            Arc::new(list_f64(&[&[5.0, 3.0]])) as ArrayRef,
            Arc::new(list_f64(&[&[3001.0]])) as ArrayRef,
            Arc::new(list_f64(&[&[4.0]])) as ArrayRef,
        ],
    )
    .unwrap();

    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    // Read back
    let read_file = tmp.reopen().unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(read_file).unwrap();
    let read_schema = builder.schema().clone();
    let mut reader = builder.build().unwrap();

    // Schema compatibility
    for field in schema.fields() {
        read_schema
            .field_with_name(field.name())
            .expect("field should exist in read schema");
    }

    let read_batch = reader.next().unwrap().unwrap();
    assert_eq!(read_batch.num_rows(), 1);

    let ts_col = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ts_col.value(0), 1_000_000);

    let sym_col = read_batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(sym_col.value(0), "ETHUSDT");
}

// ── Snap 1s schema Parquet roundtrip ────────────────────────────────────────

#[test]
fn test_snap_1s_schema_parquet_roundtrip() {
    let schema = SchemaRef::new(snap_1s_schema().clone());
    let tmp = NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    // Build a minimal batch: 3 base + 40 price/size (nullable Float64) + 17 derived
    let _n = 1_usize;
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(Int64Array::from(vec![2_000_000_i64])),
        Arc::new(StringArray::from(vec!["binance_perp"])),
        Arc::new(StringArray::from(vec!["BTCUSDT"])),
    ];
    // 10 bid_px, 10 ask_px, 10 bid_sz, 10 ask_sz — all nullable
    for _ in 0..40 {
        columns.push(Arc::new(Float64Array::from(vec![Some(1234.5_f64)])));
    }
    // Derived Float64/Float32/UInt32 columns
    columns.push(Arc::new(Float64Array::from(vec![Some(3000.5_f64)]))); // mid_px
    columns.push(Arc::new(Float64Array::from(vec![Some(3000.4_f64)]))); // microprice
    columns.push(Arc::new(Float32Array::from(vec![Some(1.5_f32)]))); // spread_bps
    columns.push(Arc::new(Float32Array::from(vec![Some(0.2_f32)]))); // imbalance_l1
    columns.push(Arc::new(Float32Array::from(vec![Some(0.1_f32)]))); // imbalance_l5
    columns.push(Arc::new(Float32Array::from(vec![Some(0.05_f32)]))); // imbalance_l10
    columns.push(Arc::new(Float64Array::from(vec![100.0_f64]))); // bid_depth_l5
    columns.push(Arc::new(Float64Array::from(vec![110.0_f64]))); // bid_depth_l10
    columns.push(Arc::new(Float64Array::from(vec![95.0_f64]))); // ask_depth_l5
    columns.push(Arc::new(Float64Array::from(vec![105.0_f64]))); // ask_depth_l10
    columns.push(Arc::new(Float64Array::from(vec![2.5_f64]))); // ofi_l1
    columns.push(Arc::new(Float64Array::from(vec![10.0_f64]))); // churn_bid
    columns.push(Arc::new(Float64Array::from(vec![8.0_f64]))); // churn_ask
    columns.push(Arc::new(Float32Array::from(vec![Some(0.01_f32)]))); // intra_sigma
    columns.push(Arc::new(Float64Array::from(vec![Some(3000.0_f64)]))); // open_px
    columns.push(Arc::new(Float64Array::from(vec![Some(3001.0_f64)]))); // close_px
    columns.push(Arc::new(UInt32Array::from(vec![42_u32]))); // n_events

    assert_eq!(
        columns.len(),
        schema.fields().len(),
        "column count must match schema field count: {} vs {}",
        columns.len(),
        schema.fields().len()
    );

    let batch = arrow_array::RecordBatch::try_new(schema.clone(), columns).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    // Read back and verify
    let read_file = tmp.reopen().unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(read_file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    // Verify n_events column (last)
    let n_events_col = batch
        .column(batch.num_columns() - 1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(n_events_col.value(0), 42);
}
