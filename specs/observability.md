# Observability — Metrics, Status, Health Semantics

Status: **stable** (write-health metrics implemented; gap-rate counter + disk alert planned)

## Current

- **Prometheus** (`src/metrics.rs`, port per deploy config): event counters per connection (`fathom_events_total`), reconnects, liveness.
- **Write-health metrics** (`src/metrics.rs`, wired into all four writers via `Metrics::record_flush` / `record_write_error`; per-feed byte estimate from `src/writer/mod.rs::batch_bytes`):

  | Metric | Type | Alert intent |
  |---|---|---|
  | `fathom_parquet_bytes_written_total{feed}` | counter | **Primary health signal.** No increase for N minutes while events flow → page |
  | `fathom_write_errors_total{feed}` | counter | Any sustained non-zero rate → page (catches ENOSPC at create/write/flush/rename) |
  | `fathom_last_flush_timestamp{feed}` | gauge | Staleness check independent of counters |

  `feed` ∈ {`raw`, `1s`, `trades`, `deriv`}. Wired at every flush, rotation, day-rollover, open failure, and graceful shutdown in `src/writer/{raw,snap_1s,trades,deriv}.rs`. `bytes_written` is the **in-memory Arrow batch size** at write time (not compressed on-disk bytes): exact on-disk bytes are awkward from `ArrowWriter` (row groups buffer internally), and the alert only needs a monotonic "data is leaving" signal — actual disk-write failures are caught by `write_errors_total`. The `exchange` label from the original plan was dropped; feed-level cardinality is enough for "is it growing".
- **`metadata/status.json`** (rewritten every 30s by `src/monitor.rs`): uptime, per-connection `connected`, per-symbol `last_event_age_s`, `gaps_today`, `reconnects_today`.
- Structured logs: hourly `periodic stats` per connection (events, events/sec), WARN on gaps/reconnects/write failures.

## ⚠️ The lesson written in blood: watch WRITES, not events

Two production incidents (disk full) where **event counters kept growing while every write failed** for weeks. WS connections were healthy, `fathom_events_total` climbed, `status.json` stayed fresh — and every Parquet file was 0 bytes. Alerting on event flow says "everything is fine" precisely while data is being lost.

**Health of a collector = data leaving it, not data entering it.**

## Planned

| Metric | Type | Alert intent |
|---|---|---|
| `fathom_gaps_total{exchange,symbol}` | counter | Gap-rate trending (today: WARN logs + daily totals only) |

Deployment-side (not fathom code, but part of the contract): disk-usage alert on the data volume must exist wherever fathom runs — fathom cannot see the disk filling until writes fail.
