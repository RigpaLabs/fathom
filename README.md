# fathom — Multi-Exchange L2 Order Book Collector

Collects Level-2 order book data from **Binance Spot**, **Binance USDM Futures**, **Hyperliquid**, and **dYdX v4** via WebSocket. Maintains BTreeMap-based L2 books, accumulates per-second microstructure statistics (OFI, churn, microprice, imbalance, trade flow), and writes Snappy-compressed Parquet files — both raw diffs and 1-second snapshots.

**22 symbols** across 4 exchanges, running continuously on a 256 MB VPS.

## Architecture

Each exchange has its own connection strategy, but all paths converge to the same two writers:

```
Binance Spot / USDM Futures:
  WebSocket combined stream
    → forwarder task (buffers during REST snapshot fetch)
    → REST snapshot → initialize BTreeMap L2 book
    → gap detection (per-symbol sequence validation)
    → apply diff → OFI / churn / microprice accumulation

Hyperliquid:
  WebSocket (single endpoint, subscribe after connect)
    → full L2 snapshots (~500ms) + trades
    → snapshot-to-snapshot OFI / churn (no gap detection needed)

dYdX v4:
  WebSocket Indexer API (subscribe after connect)
    → initial snapshot + batched diffs (~250ms) + trades
    → local BTreeMap book (DydxBook)
    → accumulation via WindowAccumulator::on_diff_from_levels

All paths → two parallel writers:
┌─────────────────────────────────────────────────────────┐
│              raw_tx (8192)         snap_tx (8192)        │
└──────────────────┬──────────────────────┬───────────────┘
                   │                      │
            ┌──────▼──────┐       ┌───────▼───────┐
            │ raw writer  │       │ 1s writer     │
            │ hourly      │       │ hourly row    │
            │ rotation    │       │ group flush   │
            └──────┬──────┘       └───────┬───────┘
                   │                      │
                   ▼                      ▼
            raw/*.parquet          1s/*.parquet
```

**Backpressure:** broadcast channels overwrite the oldest unread messages for slow subscribers, who receive `Lagged(n)` warnings — the producer never blocks the WS event loop. See [ADR-002](docs/adr/002-drop-on-backpressure.md).

## Quick start

### Local

```bash
cp config.toml.example config.toml   # edit symbols/paths as needed
make run                              # RUST_LOG=fathom=info cargo run
```

### Docker

```bash
cp config.toml.example config.toml
docker compose up                     # dev: mounts ./data and ./config.toml
```

Production (GHCR image):

```bash
IMAGE_TAG=latest docker compose -f docker-compose.prod.yml up -d
```

## Data output

```
{data_dir}/
├── raw/{exchange}/{symbol}/{date}/
│   └── depth_HHMM_HHMM.parquet      # rotated every raw_rotate_hours (default 1h)
├── 1s/{exchange}/{symbol}/
│   └── {date}.parquet                # 1 row/second, 64 columns
└── metadata/
    └── status.json                   # health/monitoring, updated every 30s
```

When deployed with `DATA_DIR` env override, files are written under `{data_dir}/{version}/` for blue-green isolation.

### Raw diff columns

`timestamp_us`, `exchange`, `symbol`, `seq_id`, `prev_seq_id`, `bids` (list of [price, qty]), `asks` (list of [price, qty])

### 1s snapshot columns (64)

`ts_us`, `exchange`, `symbol`, `bid_px_0..9`, `ask_px_0..9`, `bid_sz_0..9`, `ask_sz_0..9`, `mid_px`, `microprice`, `spread_bps`, `imbalance_l1`, `imbalance_l5`, `imbalance_l10`, `bid_depth_l5`, `bid_depth_l10`, `ask_depth_l5`, `ask_depth_l10`, `ofi_l1`, `churn_bid`, `churn_ask`, `intra_sigma`, `open_px`, `close_px`, `n_events`, `volume_delta`, `buy_vol`, `sell_vol`, `trade_count`

## NATS streaming (optional)

When a `[nats]` section is present in config, Fathom publishes data to NATS JetStream in addition to writing Parquet files. If NATS is disabled or the connection fails, Parquet output continues unaffected.

### Topics

| Subject pattern | Example | Rate |
|-----------------|---------|------|
| `fathom.v1.{exchange}.{symbol}.snapshot` | `fathom.v1.binance_perp.ETHUSDT.snapshot` | 1/sec per symbol |
| `fathom.v1.{exchange}.{symbol}.depth` | `fathom.v1.dydx.ETH-USD.depth` | ~100ms per symbol |

Wire format: `[version: u8][bincode payload]` (version = 1). Types defined in `crates/fathom-types`.

### Streams

| Stream | Subjects | Storage | Max age | Max bytes | Durability |
|--------|----------|---------|---------|-----------|------------|
| `FATHOM_SNAPSHOTS` | `fathom.v1.*.*.snapshot` | File | 24 h | 200 MB | **Critical** — JetStream ACK confirms durable write |
| `FATHOM_DEPTH` | `fathom.v1.*.*.depth` | File | 1 h | 500 MB | **Durable** — JetStream ACK, file storage persists across restarts |

Both streams use the JetStream double-await pattern (send request → await ACK) to confirm each message is durably stored. Both use file-backed storage that survives NATS restarts. Publish failures log warnings without blocking the main pipeline.

### Timestamp semantics

- **`Snapshot1s.ts_us`** — wall-clock time of the Fathom process at the end of the 1-second accumulation window (`Utc::now()` at flush), in microseconds since Unix epoch. This is **not** exchange event time — it reflects when Fathom flushed the accumulated stats, not when the underlying events occurred. Parquet files are partitioned by the date component of this field.
- **`RawDiff.timestamp_us`** — per-exchange origin:
  - **Binance (spot + perp):** exchange event time (`E` field from the WebSocket diff message), converted to microseconds.
  - **Hyperliquid:** exchange-provided snapshot time (`time` field), converted to microseconds.
  - **dYdX:** wall-clock receive time (`Utc::now()` when the batch is processed) — the dYdX WebSocket API does not provide per-batch timestamps.

### Configuration

```toml
[nats]
url = "nats://127.0.0.1:4222"
enabled = true   # default: true when [nats] section is present
```

## Config reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `data_dir` | string | required | Root directory for Parquet output |
| `raw_rotate_hours` | integer | `1` | Raw file rotation interval in hours (must divide 24: 1,2,3,4,6,8,12,24) |
| `connections[].name` | string | required | Logical name for the connection group |
| `connections[].exchange` | string | required | `binance_spot`, `binance_perp`, `hyperliquid`, or `dydx` |
| `connections[].symbols` | string[] | required | Trading pairs (format varies by exchange, e.g. `ETHUSDT` / `ETH` / `ETH-USD`) |
| `connections[].depth_ms` | integer | required | WebSocket update speed in ms (Binance: `100`/`1000`, HL: `500`). Not used for `dydx` — the dYdX WebSocket uses a fixed update interval controlled by the exchange. |
| `nats.url` | string | — | NATS server URL (e.g. `nats://127.0.0.1:4222`). Omit `[nats]` section to disable. |
| `nats.enabled` | bool | `true` | Set `false` to disable NATS while keeping the config section. |

### Environment variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Tracing filter (e.g. `fathom=info`, `fathom=debug`) |
| `FATHOM_JSON_LOG` | If set, enables structured JSON logging |
| `DATA_DIR` | Override `data_dir` from config at runtime (used for blue-green deploy) |

## Deployment

### Blue-green deploy (zero downtime)

Triggered manually via GitHub Actions (`workflow_dispatch`). The pipeline:

1. **CI** — `cargo test` + `clippy` + `fmt` check
2. **Build** — Docker multi-stage build, pushed to `ghcr.io` with version tag `vYYYYMMDD-{sha7}`
3. **Deploy** — SSH to VPS:
   - Pull new image while old container is still running
   - Start `fathom-new` alongside `fathom` with versioned `DATA_DIR` (no file conflicts)
   - Poll `status.json` for up to 120s until healthy
   - If healthy → gracefully stop old container (`docker stop -t 30`, 30s for writers to flush) → rename new to `fathom`
   - If unhealthy → rollback (remove new container, exit 1)

Both containers run simultaneously during the health check window. Versioned data directories prevent file conflicts between old and new instances.

### Graceful shutdown

On SIGTERM/SIGINT:
1. Connection tasks abort
2. Writer channel senders drop → receivers drain remaining messages
3. Raw writer flushes current buffer and closes files
4. 1s writer flushes row group and writes Parquet footer
5. Process exits

Docker `stop_grace_period: 30s` gives writers enough time to complete.

### Health check

The monitor task writes `metadata/status.json` every 30 seconds with per-connection and per-symbol stats. Docker healthcheck verifies the file was modified within the last 3 minutes.

```json
{
  "updated_at": "2026-03-02T08:00:00Z",
  "uptime_s": 3600,
  "connections": {
    "spot": {
      "connected": true,
      "reconnects_today": 0,
      "symbols": {
        "ETHUSDT": { "last_event_age_s": 0.12, "gaps_today": 0 }
      }
    }
  }
}
```

## Testing

```bash
make test           # unit + integration + e2e (no network)
make smoke          # live Binance (manual, requires network)
make lint           # cargo clippy -- -D warnings
make fmt-check      # cargo fmt --check
make docker-smoke   # build Docker image + run vs live Binance
make cov            # coverage report (llvm-cov)

# Exchange-specific smoke tests (live network, manual only):
cargo test --test smoke_hl_test -- --include-ignored --test-threads 1 --nocapture
cargo test --test smoke_dydx_test -- --include-ignored --test-threads 1 --nocapture
```

**~117 tests** across 12 test files:

| Category | Count | Description |
|----------|-------|-------------|
| Orderbook | 39 | L2 book, apply_diff, gap detection, perp `pu` field, imbalance, OFI |
| Writers | 14 | Raw + 1s Parquet output, rotation, periodic flush, blue-green isolation |
| Exchange | 11 | BinanceSpot/BinancePerp adapters, URL construction |
| Config | 10 | TOML parsing, defaults, validation |
| Accumulator | 10 | OFI, churn, microprice, sigma, flush/reset |
| Connection | 9 | WS parsing, backoff, reconnect logic |
| Monitor | 6 | status.json, stale symbol detection, gap warnings |
| Schema | 5 | Parquet schema compatibility, roundtrip |
| E2E | 7 | Full pipeline with axum mock WS/REST server (no real network) |
| Integration | 2 | Multi-component pipeline tests |
| Smoke | 3 | Real Binance/HL/dYdX (`#[ignore]`, manual only) |

## Non-goals

What fathom deliberately does **not** do:

- **Order routing or execution** — fathom is a passive collector, never places orders
- **Real-time alerting** — use external alerting on `/metrics` or `status.json`
- **Cross-exchange arbitrage signal generation** — that's [sigil](https://github.com/RigpaLabs/sigil)'s job
- **Data normalization beyond raw L2 diffs and 1s snapshots** — consumers handle further aggregation

## Roadmap

### Near-term

- [ ] Additional venues (Bybit, OKX)
- [ ] OHLCV aggregation writer (5m, 1h)
- [ ] Schema versioning for Parquet columns (see [docs/schema-versioning.md](docs/schema-versioning.md))

### Long-term

- [ ] Sigil integration: NATS → signal engine
- [ ] Replay mode: serve historical snapshots via HTTP

## Design notes

### Binance: spot vs perp gap detection

Binance USDM Futures diffs carry a `pu` (prev_final_update_id) field absent from spot. Gap checks in `orderbook/mod.rs`:

- **Perp:** `pu == last_update_id`
- **Spot:** `first_update_id == last_update_id + 1`

Using the wrong rule causes spurious reconnects. The code branches on `Option<i64>` presence.

### Hyperliquid: snapshot-based OFI

Hyperliquid sends full L2 snapshots every ~500ms rather than incremental diffs. OFI and churn are computed snapshot-to-snapshot via `WindowAccumulator::on_diff_from_levels`, which synthesizes a synthetic diff by comparing consecutive snapshots. No gap detection needed.

### dYdX v4: batched diffs

dYdX v4 Indexer API delivers an initial snapshot followed by batched diffs. A local `DydxBook` (BTreeMap) applies each batch, and accumulation uses the same `on_diff_from_levels` path as Hyperliquid. The WebSocket guarantees ordering, so no sequence validation is required.

### Forwarder task (Binance)

Each Binance connection spawns a forwarder that owns the socket, handles Ping/Pong frames, and buffers text events in a channel. This lets the main loop fetch REST snapshots without missing WS events — critical during initial sync when snapshot requests take 200-500ms per symbol.

### 1s writer periodic flush

`ArrowWriter` buffers rows in a row group until `flush()` or `finish()`. With 1 row/sec the default 1M-row threshold would never trigger, so the writer explicitly flushes every 300 rows (~5 minutes). This limits worst-case data loss on crash to ~5 minutes of 1s data instead of an entire day.

## Architecture Decision Records

Key architectural trade-offs are documented as ADRs in [`docs/adr/`](docs/adr/):

| ADR | Decision |
|-----|----------|
| [001](docs/adr/001-broadcast-channel-over-mpsc.md) | Broadcast channel over mpsc/worker pool |
| [002](docs/adr/002-drop-on-backpressure.md) | Drop on backpressure over block/retry |
| [003](docs/adr/003-btreemap-for-order-book.md) | BTreeMap for order book |
| [004](docs/adr/004-order-book-invariants-and-gap-semantics.md) | Order book invariants and gap semantics |
| [005](docs/adr/005-acceptable-data-loss-window.md) | Acceptable data loss window by component |
| [006](docs/adr/006-completeness-vs-uptime.md) | Completeness vs uptime trade-off |

## Engineering Blog

Production insights from running fathom at scale: [`docs/engineering/`](docs/engineering/)
