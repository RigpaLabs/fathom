# Risk Register

Known risks, their severity, and mitigations.

## 1. Binance REST snapshot rate limiting

| | |
|---|---|
| **Risk** | Binance rate-limits REST `/depth` requests. Hitting the limit during initial sync or reconnect causes snapshot failures, leaving symbols without a valid L2 book. |
| **Severity** | Medium |
| **Mitigation** | Per-symbol spacing: snapshot requests are staggered with a configurable delay between symbols. Exponential backoff on HTTP 429 responses. Forwarder task buffers WS events during the snapshot window so no diffs are lost. |
| **Status** | Mitigated |

## 2. BTreeMap memory growth on deep books

| | |
|---|---|
| **Risk** | L2 books stored as `BTreeMap<OrderedFloat<f64>, f64>` grow with the number of active price levels. Exchanges with thousands of levels could cause memory pressure on the 256 MB VPS. |
| **Severity** | Low |
| **Mitigation** | Only top-10 levels are used for snapshot columns. Parquet writers do not store the full book. BTreeMap entries at zero quantity are removed immediately on diff application. Hyperliquid sends only top-N levels by design. |
| **Status** | Mitigated |

## 3. Single VPS — single point of failure

| | |
|---|---|
| **Risk** | Fathom runs on a single EC2 instance. If the instance goes down, data collection stops for all 22 symbols across 4 exchanges. |
| **Severity** | Medium |
| **Mitigation** | Blue-green deploy enables quick recovery (< 5 min). `status.json` health check enables external monitoring (Uptime Kuma). Docker `restart: unless-stopped` handles process crashes. NATS streaming provides a secondary data path for consumers that can tolerate gaps. |
| **Status** | Known, accepted |

## 4. Exchange WebSocket API changes

| | |
|---|---|
| **Risk** | Exchanges may change their WebSocket protocol, message format, or rate limits without notice. This would cause silent data corruption or connection failures. |
| **Severity** | Medium |
| **Mitigation** | Smoke tests (`smoke_test.rs`, `smoke_hl_test.rs`, `smoke_dydx_test.rs`) run against live exchange APIs to detect protocol changes. Gap detection catches missing or out-of-order diffs. Monitor task flags stale symbols (no events for > threshold). |
| **Status** | Mitigated |

## 5. Parquet writer crash — data loss window

| | |
|---|---|
| **Risk** | If the process crashes between periodic flushes, buffered rows are lost. The 1s writer flushes every 3600 rows (~1 hour), so worst-case loss is 1 hour of 1s data. |
| **Severity** | Low |
| **Mitigation** | 1-hour flush interval balances I/O overhead vs data loss window. Raw writer rotates hourly with explicit flush. Docker graceful shutdown (30s grace period) ensures clean flush on planned stops. NATS subscribers receive data in real-time as a secondary record. |
| **Status** | Known, accepted |
