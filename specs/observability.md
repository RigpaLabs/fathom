# Observability — Metrics, Status, Health Semantics

Status: **stable** (write-health **planned**)

## Current

- **Prometheus** (`src/metrics.rs`, port per deploy config): event counters per connection (`fathom_events_total`), reconnects, liveness.
- **`metadata/status.json`** (rewritten every 30s by `src/monitor.rs`): uptime, per-connection `connected`, per-symbol `last_event_age_s`, `gaps_today`, `reconnects_today`.
- Structured logs: hourly `periodic stats` per connection (events, events/sec), WARN on gaps/reconnects/write failures.

## ⚠️ The lesson written in blood: watch WRITES, not events

Two production incidents (disk full) where **event counters kept growing while every write failed** for weeks. WS connections were healthy, `fathom_events_total` climbed, `status.json` stayed fresh — and every Parquet file was 0 bytes. Alerting on event flow says "everything is fine" precisely while data is being lost.

**Health of a collector = data leaving it, not data entering it.**

## Planned

| Metric | Type | Alert intent |
|---|---|---|
| `fathom_parquet_bytes_written_total{feed,exchange}` | counter | **Primary health signal.** No increase for N minutes while events flow → page |
| `fathom_write_errors_total{feed}` | counter | Any sustained non-zero rate → page (catches ENOSPC immediately) |
| `fathom_gaps_total{exchange,symbol}` | counter | Gap-rate trending (today: WARN logs + daily totals only) |
| `fathom_last_flush_timestamp{feed}` | gauge | Staleness check independent of counters |

Deployment-side (not fathom code, but part of the contract): disk-usage alert on the data volume must exist wherever fathom runs — fathom cannot see the disk filling until writes fail.
