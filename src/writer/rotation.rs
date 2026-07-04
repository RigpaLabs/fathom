//! Shared hourly-rotation infrastructure for the 1s and deriv writers.
//!
//! `raw.rs`/`trades.rs` already rotate hourly via a temp-file-then-rename
//! dance (`SymbolWriter`); `Bucket` lifts that same naming/lifecycle logic out
//! so `snap_1s.rs` and `deriv.rs` can adopt it too, bounding restart data loss
//! to a single open bucket instead of a full calendar day.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Timelike, Utc};

/// Which hourly bucket does a UTC hour belong to?
/// `interval` must divide 24 evenly (1, 2, 3, 4, 6, 8, 12, 24).
pub fn bucket_open(hour: u32, interval: u32) -> u32 {
    (hour / interval) * interval
}

/// Abstracts wall-clock time so tests can drive periodic force-rotation
/// checks (deriv.rs) without racing real time.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// One open (or closed) rotation bucket for a single (exchange, symbol, feed).
///
/// `open()` only creates the `{date}/` directory — callers build their own
/// `std::fs::File` + `ArrowWriter` against `temp_path` since each writer has
/// its own Arrow schema.
pub struct Bucket {
    dir: PathBuf,
    pub temp_path: PathBuf,
    open_hour: u32,
    open_hhmm: String,
    date_str: String,
    prefix: &'static str,
}

impl Bucket {
    pub fn open(
        base_dir: &Path,
        exchange: &str,
        symbol: &str,
        prefix: &'static str,
        as_of: DateTime<Utc>,
        rotate_hours: u32,
    ) -> std::io::Result<Self> {
        let date_str = as_of.format("%Y-%m-%d").to_string();
        let open_hour = bucket_open(as_of.hour(), rotate_hours);
        let open_hhmm = format!("{open_hour:02}00");

        let dir = base_dir.join(exchange).join(symbol).join(&date_str);
        std::fs::create_dir_all(&dir)?;

        let temp_path = dir.join(format!("{prefix}_{open_hhmm}_open.parquet"));

        Ok(Self {
            dir,
            temp_path,
            open_hour,
            open_hhmm,
            date_str,
            prefix,
        })
    }

    pub fn should_rotate(&self, as_of: DateTime<Utc>, rotate_hours: u32) -> bool {
        as_of.format("%Y-%m-%d").to_string() != self.date_str
            || bucket_open(as_of.hour(), rotate_hours) != self.open_hour
    }

    /// Rename `temp_path` to its final `{prefix}_{HHMM}_{HHMM}.parquet` name.
    /// `as_of` must be the caller's tracked last-event-time, never wall-clock
    /// — it determines the end-HHMM in the filename and must reflect what's
    /// actually in the data.
    ///
    /// If the final path already exists (e.g. two restarts within the same
    /// minute closing the "same" incomplete bucket), appends `_2`, `_3`, ...
    /// until a free path is found rather than silently overwriting.
    pub fn close_and_rename(&self, as_of: DateTime<Utc>) -> std::io::Result<PathBuf> {
        let end_hhmm = format!("{:02}{:02}", as_of.hour(), as_of.minute());
        let base_name = format!("{}_{}_{}", self.prefix, self.open_hhmm, end_hhmm);

        let mut new_path = self.dir.join(format!("{base_name}.parquet"));
        let mut suffix = 2;
        while new_path.exists() {
            new_path = self.dir.join(format!("{base_name}_{suffix}.parquet"));
            suffix += 1;
        }

        std::fs::rename(&self.temp_path, &new_path)?;
        Ok(new_path)
    }
}
