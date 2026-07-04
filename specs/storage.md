# Storage — Layout, Rotation, Retention

Status: **stable** for layout; retention/upload **planned**

## Local layout

Root: `{data_dir}` from `config.toml`; `DATA_DIR` env overrides (blue-green deploys).

```
{data_dir}/
├── raw/{exchange}/{symbol}/{date}/depth_HHMM_HHMM.parquet     # hourly-rotated raw diffs
├── 1s/{exchange}/{symbol}/{date}/snap_HHMM_HHMM.parquet       # hourly-rotated 1s snapshots
├── trades/{exchange}/{symbol}/{date}/trades_HHMM_HHMM.parquet # hourly-rotated trade tape
├── deriv/{exchange}/{symbol}/{date}/{funding|oi|liq}_HHMM_HHMM.parquet  # hourly-rotated derivatives feeds
└── metadata/status.json                                       # health snapshot, rewritten every 30s
```

All four writers rotate on the same `raw_rotate_hours` config value (default 1) via the shared
`Bucket` type in `src/writer/rotation.rs`: an open bucket writes to `{prefix}_HHMM_open.parquet`,
then gets renamed to its final `{prefix}_HHMM_HHMM.parquet` name on rotation or graceful shutdown.
If that final name is already taken (e.g. two restarts closing the "same" incomplete bucket within
the same minute), an incrementing suffix is appended instead of overwriting:
`{prefix}_HHMM_HHMM[_N].parquet`. `Bucket::should_rotate` checks both date and hour-bucket, so a
bucket correctly rotates across a date boundary even when the hour-bucket value alone is unchanged
(e.g. `raw_rotate_hours=24`, a single bucket per day: only the date distinguishes one day's bucket
from the next).

Writers: `src/writer/raw.rs` (hourly rotation via `Bucket`, wall-clock-triggered), `src/writer/snap_1s.rs`
(hourly rotation via `Bucket`, event-time-triggered, flush every row), `src/writer/trades.rs`
(hourly rotation via `Bucket`, wall-clock-triggered, same pattern as raw), `src/writer/deriv.rs`
(hourly rotation via `Bucket`, event-time-triggered, per feed, 5s flush, plus a periodic
force-rotate for sparse feeds — [derivatives-feeds.md](derivatives-feeds.md)). raw.rs/trades.rs
trigger rotation on wall-clock time (so a totally silent symbol still rotates and doesn't hold a
stale bucket open); snap_1s.rs/deriv.rs trigger on event time. All four use `Bucket`'s
`close_and_rename` with a tracked last-event-time as the close marker, never wall-clock, so the
filename's end-HHMM always reflects the data.

Restart-safety: bounding the file granularity to one hour bounds restart data loss to at most the
single bucket open at crash time, instead of an entire day (docs/adr/005). This is a bounded-loss
design, not full restart-safety — an `ArrowWriter` that never reaches `.finish()` before a crash
has no Parquet footer and that bucket's data is entirely unreadable, not partially recoverable.

## Volumes (order of magnitude, current symbol set)

Raw depth dominates: ~2 GB/day. 1s snapshots: ~100 MB/day. Adding trades + derivatives feeds is small relative to depth (~+0.3 GB/day).

## ⚠️ Known gap: no retention — disk WILL fill

There is **no rotation-by-age, no size cap, no upload**. On a small disk raw fills it in ~2 weeks. This has caused real silent outages twice (writes fail with `No space left on device` while WS connections stay up and event counters keep growing — see [observability.md](observability.md) for the alerting consequence). Until upload+retention lands, operators must offload and prune `raw/` manually.

## Planned: object-storage upload + local retention

Design (deliberately minimal — no collector/writer split, no new services beyond a sidecar):

1. **Uploader sidecar**: watches for *completed* files (any rotated `{prefix}_HHMM_HHMM[_N].parquet`, never a `_open` file), moves them to object storage (S3-compatible), deletes local copy on verified upload. `rclone move` in a loop is an acceptable v1.
2. **Partitioning**: `{bucket}/{feed}/{exchange}/{symbol}/{date}/...` — mirrors local layout.
3. **Local retention**: keep last N hours of raw as a buffer (NATS depth stream already covers short replay); 1s files keep longer locally (small).
4. **Lifecycle**: storage-class transition / expiry handled by bucket policy, not fathom.
5. **Safety rule**: fathom itself never deletes a file the uploader hasn't confirmed — deletion is the uploader's job only.

Non-goals: streaming uploads of open files, exactly-once semantics (files are idempotent by path), NATS-based collector/writer split (revisit only when adding whole new exchange collectors makes the monolith unwieldy).
