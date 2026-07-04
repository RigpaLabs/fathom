//! Rotation infrastructure shared by `snap_1s.rs` and `deriv.rs`: hourly-bucketed
//! file naming/lifecycle (`Bucket`) plus a minimal `Clock` abstraction that
//! `deriv.rs` needs for its periodic force-rotate check (see its module doc).
//!
//! `raw.rs` and `trades.rs` already rotate hourly: open a uniquely-named temp
//! file (`{prefix}_{HHMM}_open.parquet`), buffer into it, and `rename` to a
//! final `{prefix}_{HHMM}_{HHMM}.parquet` name on rotation/shutdown. `snap_1s.rs`
//! and `deriv.rs` used to write one file per calendar day instead, opened
//! lazily via a `HashMap` that starts empty every process start — a mid-day
//! restart's `File::create` truncated the existing day file (confirmed
//! production incident 2026-07-04: 3 restarts destroyed ~22h + ~3.5h of data).
//!
//! `Bucket` gives snap_1s/deriv the same hourly-bounded-loss shape raw/trades
//! already have. This bounds the loss, it does not eliminate it: an
//! `ArrowWriter` that never reaches `.finish()` (e.g. process killed mid-bucket)
//! has no Parquet footer and the file is entirely unreadable — the currently
//! open bucket at crash time is still lost, just never more than one bucket's
//! worth (sized to the configured `rotate_hours`) instead of up to a full day.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Timelike, Utc};

/// Which hourly bucket does a UTC hour belong to?
/// `interval` must divide 24 evenly (1, 2, 3, 4, 6, 8, 12, 24).
pub fn bucket_open(hour: u32, interval: u32) -> u32 {
    (hour / interval) * interval
}

/// Abstracts "now" so tests can drive rotation decisions off a controllable
/// clock instead of real wall-clock time. Production always uses `SystemClock`.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Production clock — real wall-clock time.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Hourly-bucketed file naming and lifecycle for a single (exchange, symbol,
/// feed) writer.
///
/// `Bucket` does not own the `File`/`ArrowWriter` — the caller creates
/// `std::fs::File::create(&bucket.temp_path)` and builds its own `ArrowWriter`
/// with its own schema, then calls `close_and_rename` once that writer has
/// been finalized (`.finish()` already called).
pub struct Bucket {
    dir: PathBuf,
    /// Temp path: `{prefix}_{HHMM}_open.parquet`. The caller creates the
    /// actual file here.
    pub temp_path: PathBuf,
    open_hour: u32,
    open_hhmm: String,
    date_str: String,
    prefix: &'static str,
}

impl Bucket {
    /// Creates the `{date}/` directory (not the temp file itself) and
    /// computes the temp path for a bucket opened at `as_of`.
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

    /// Should this bucket rotate given an event/tick at `as_of`? True on a
    /// UTC date change OR a bucket-boundary crossing — unifying what used to
    /// be two separately-checked conditions (date-only for snap_1s/deriv,
    /// hour-bucket-only for raw/trades) into one, so the two can't disagree.
    pub fn should_rotate(&self, as_of: DateTime<Utc>, rotate_hours: u32) -> bool {
        as_of.format("%Y-%m-%d").to_string() != self.date_str
            || bucket_open(as_of.hour(), rotate_hours) != self.open_hour
    }

    /// Rename `temp_path` to its final name. `as_of` MUST be the caller's
    /// tracked "last event time" — NEVER wall-clock — since it determines the
    /// end-HHMM in the filename and must reflect what's actually in the data.
    ///
    /// If the computed final path already exists (e.g. two graceful restarts
    /// within the same minute closing the "same" incomplete bucket), an
    /// incrementing numeric suffix (`_2`, `_3`, ...) is appended until a free
    /// path is found — never silently overwrite. Returns the actual path used.
    pub fn close_and_rename(&self, as_of: DateTime<Utc>) -> std::io::Result<PathBuf> {
        let end_hhmm = format!("{:02}{:02}", as_of.hour(), as_of.minute());

        let mut new_path = self.dir.join(format!(
            "{}_{}_{}.parquet",
            self.prefix, self.open_hhmm, end_hhmm
        ));
        let mut suffix = 2;
        while new_path.exists() {
            new_path = self.dir.join(format!(
                "{}_{}_{}_{}.parquet",
                self.prefix, self.open_hhmm, end_hhmm, suffix
            ));
            suffix += 1;
        }

        std::fs::rename(&self.temp_path, &new_path)?;
        Ok(new_path)
    }
}
