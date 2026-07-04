pub mod deriv;
pub mod raw;
pub mod rotation;
pub mod snap_1s;
pub mod trades;

use arrow_array::RecordBatch;

/// Byte estimate for a batch about to be written, feeding the
/// `fathom_parquet_bytes_written_total` write-health counter.
///
/// This is the in-memory size of the Arrow arrays, not the compressed on-disk
/// Parquet size. Exact on-disk bytes are awkward to pull from `ArrowWriter`
/// (row groups buffer internally and only reach disk on row-group close / flush
/// / finish), and the counter only needs a monotonic "data is leaving the
/// writer" signal (specs/observability.md). Actual disk-write failures are
/// caught separately by `fathom_write_errors_total`.
pub(crate) fn batch_bytes(batch: &RecordBatch) -> u64 {
    batch.get_array_memory_size() as u64
}
