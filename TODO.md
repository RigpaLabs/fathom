# fathom — TODO

## Reliability

- [ ] **Per-symbol REST snapshot on gap** — instead of reconnecting the entire WS on a single symbol's sequence gap, fetch REST snapshot only for the gapped symbol and continue on the same WS. Reduces disruption to other symbols sharing the connection.

- [ ] **Per-symbol WS connections** — separate WS per symbol so a disconnect/gap on one doesn't affect others. Trade-off: 5x more connections, Binance rate limits apply (5 connections per IP for spot, separate for perp).

## Storage

- [x] **S3 upload** — `fathom-sync` sidecar binary. Scans data_dir for completed Parquet files, uploads to S3-compatible storage via `object_store`, tracks state with `.synced` markers, cleans up after configurable retention.

## Observability

- [ ] **OpenTelemetry + SigNoz** — instrument fathom and fathom-sync with OTel traces/metrics. Deploy SigNoz for visualization. Consider: spans for WS connect/reconnect, upload duration, error rates, disk usage gauge.

## Monitoring

- [ ] **Gap rate metrics** — expose per-symbol gap counts as Prometheus metrics or structured log for alerting. Currently only logged as WARN with daily totals.
