# Storage — Layout, Rotation, Retention

Status: **stable** for layout; retention/upload **planned**

## Local layout

Root: `{data_dir}` from `config.toml`; `DATA_DIR` env overrides (blue-green deploys).

```
{data_dir}/
├── raw/{exchange}/{symbol}/{date}/depth_HHMM_HHMM.parquet   # hourly-rotated raw diffs
├── 1s/{exchange}/{symbol}/{date}/snap_HHMM_HHMM.parquet     # hourly-rotated 1s snapshots
├── trades/{exchange}/{symbol}/{date}/trades_HHMM_HHMM.parquet  # hourly-rotated trade tape
├── deriv/{exchange}/{symbol}/{date}/{funding|oi|liq}_HHMM_HHMM.parquet   # hourly-rotated derivatives feeds
└── metadata/status.json                                     # health snapshot, rewritten every 30s
```

All four writers rotate on the same `rotate_hours` window (config `raw_rotate_hours`, default 1h) via the shared `Bucket` open/rename lifecycle (`src/writer/rotation.rs`): each opens a uniquely-named temp file (`{prefix}_{HHMM}_open.parquet`) and renames it to `{prefix}_{HHMM}_{HHMM}.parquet` on rotation or graceful shutdown. If the computed final name already exists (e.g. two restarts closing the "same" incomplete bucket within the same end-minute), an incrementing numeric suffix is appended instead of overwriting: `{prefix}_{HHMM}_{HHMM}[_N].parquet`.

Writers: `src/writer/raw.rs` (hourly rotation), `src/writer/snap_1s.rs` (hourly rotation, flush on each row), `src/writer/trades.rs` (hourly rotation, same pattern as raw), `src/writer/deriv.rs` (hourly rotation + periodic force-rotate for sparse feeds, 5s buffer flush — [derivatives-feeds.md](derivatives-feeds.md)).

Before this rotation scheme, `snap_1s.rs` and `deriv.rs` wrote one file per calendar day, opened lazily via a `HashMap` that started empty every process restart — a mid-day restart's `File::create` truncated the existing day's file. Hourly rotation bounds that loss to at most one open bucket (sized to `rotate_hours`) instead of up to a full day; it does not eliminate it — an `ArrowWriter` that never reaches `.finish()` has no Parquet footer and the file is entirely unreadable (see [ADR-005](../docs/adr/005-acceptable-data-loss-window.md)).

## Volumes (order of magnitude, current symbol set)

Raw depth dominates: ~2 GB/day. 1s snapshots: ~100 MB/day. Adding trades + derivatives feeds is small relative to depth (~+0.3 GB/day).

## ⚠️ Known gap: no retention — disk WILL fill

There is **no rotation-by-age, no size cap, no upload**. On a small disk raw fills it in ~2 weeks. This has caused real silent outages twice (writes fail with `No space left on device` while WS connections stay up and event counters keep growing — see [observability.md](observability.md) for the alerting consequence). Until upload+retention lands, operators must offload and prune `raw/` manually.

## Planned: object-storage upload + local retention

Design (deliberately minimal — no collector/writer split, no new services beyond a sidecar):

1. **Uploader sidecar**: watches for *completed* files (rotated raw hours, previous-day 1s), moves them to object storage (S3-compatible), deletes local copy on verified upload. `rclone move` in a loop is an acceptable v1.
2. **Partitioning**: `{bucket}/{feed}/{exchange}/{symbol}/{date}/...` — mirrors local layout.
3. **Local retention**: keep last N hours of raw as a buffer (NATS depth stream already covers short replay); 1s files keep longer locally (small).
4. **Lifecycle**: storage-class transition / expiry handled by bucket policy, not fathom.
5. **Safety rule**: fathom itself never deletes a file the uploader hasn't confirmed — deletion is the uploader's job only.

Non-goals: streaming uploads of open files, exactly-once semantics (files are idempotent by path), NATS-based collector/writer split (revisit only when adding whole new exchange collectors makes the monolith unwieldy).
