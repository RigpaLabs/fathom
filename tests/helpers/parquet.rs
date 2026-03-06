use std::path::Path;

use arrow::array::{Array, Float32Array, Float64Array, UInt32Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub fn collect_parquets(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(collect_parquets(&path));
            } else if path.extension().map_or(false, |e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out
}

pub fn count_rows(path: &Path) -> usize {
    let file = std::fs::File::open(path).expect("parquet file");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

pub fn read_f64_col(path: &Path, col: &str) -> Vec<f64> {
    let file = std::fs::File::open(path).expect("parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        if let Some(arr) = batch.column_by_name(col) {
            if let Some(fa) = arr.as_any().downcast_ref::<Float64Array>() {
                for i in 0..fa.len() {
                    if !fa.is_null(i) {
                        values.push(fa.value(i));
                    }
                }
            }
        }
    }
    values
}

pub fn read_f32_col(path: &Path, col: &str) -> Vec<f32> {
    let file = std::fs::File::open(path).expect("parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        if let Some(arr) = batch.column_by_name(col) {
            if let Some(fa) = arr.as_any().downcast_ref::<Float32Array>() {
                for i in 0..fa.len() {
                    if !fa.is_null(i) {
                        values.push(fa.value(i));
                    }
                }
            }
        }
    }
    values
}

pub fn read_u32_col(path: &Path, col: &str) -> Vec<u32> {
    let file = std::fs::File::open(path).expect("parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        if let Some(arr) = batch.column_by_name(col) {
            if let Some(ua) = arr.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..ua.len() {
                    if !ua.is_null(i) {
                        values.push(ua.value(i));
                    }
                }
            }
        }
    }
    values
}
