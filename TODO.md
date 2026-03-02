# fathom — TODO

## Reliability

- [ ] **Per-symbol REST snapshot on gap** — instead of reconnecting the entire WS on a single symbol's sequence gap, fetch REST snapshot only for the gapped symbol and continue on the same WS. Reduces disruption to other symbols sharing the connection.

- [ ] **Per-symbol WS connections** — separate WS per symbol so a disconnect/gap on one doesn't affect others. Trade-off: 5x more connections, Binance rate limits apply (5 connections per IP for spot, separate for perp).

## Storage

- [ ] **S3 upload** — upload completed Parquet files (rotated raw + daily 1s) to S3/R2 for long-term storage. Could run as a sidecar or post-rotation hook. Consider: lifecycle policies, compression, partitioning by date/exchange/symbol.

## Monitoring

- [ ] **Gap rate metrics** — expose per-symbol gap counts as Prometheus metrics or structured log for alerting. Currently only logged as WARN with daily totals.
