# Changelog

## [Unreleased]

### Fixed
- **1s/deriv restart truncation (unbounded → ≤1 open bucket)** — `snap_1s.rs` and `deriv.rs` wrote one file per calendar day, opened lazily via a `HashMap` that started empty every process restart; a mid-day restart's `File::create` truncated the existing day's file. Confirmed in production 2026-07-04: 3 fathom restarts destroyed ~22h of one day's data and ~3.5h of another day's data. Both writers now use the same hourly `Bucket` open-temp/rename-on-close lifecycle `raw.rs`/`trades.rs` already had (`src/writer/rotation.rs`), bounding restart loss to at most one open bucket (sized to `raw_rotate_hours`). Deriv also gained a periodic force-rotate (injected `Clock`) so a sparse feed (e.g. `liq`) can't hold an `_open.parquet` file open past its bucket boundary indefinitely.

### Changed
- Layout: `1s/{exchange}/{symbol}/{date}.parquet` → `1s/{exchange}/{symbol}/{date}/snap_HHMM_HHMM.parquet`; `deriv/{exchange}/{symbol}/{date}/{funding|oi|liq}.parquet` → `.../{funding|oi|liq}_HHMM_HHMM.parquet`. Old daily-format files are not migrated; they coexist untouched.
- `raw_rotate_hours` config now governs all four writers (raw, trades, 1s, deriv), not just raw/trades.

## [0.3.0] — 2026-03-08

### Fixed
- **dYdX crossed book (94% → 0%)** — dYdX v4 diff stream doesn't always send explicit qty=0 for consumed levels; stale levels accumulated in BTreeMap causing $50-100 crossing. Added `uncross()` method that removes crossed levels after each diff application.

### Added
- 4 unit tests for `DydxBook::uncross()` (stale ask, multiple levels, auto-uncross via apply_diffs, no-op)
- Smoke test assertion: `spread_bps >= 0` regression guard for dYdX

## [0.2.2] — 2026-03-07

### Changed
- Channel buffer 2048 → 4096 (4x headroom for 22 symbols)
- Extracted `top10()` helper in HL connection (dedup flush_with_levels)
- Added `scripts/` to `.gitignore`

## [0.2.1] — 2026-03-07

### Fixed
- **Memory: ~500MB → ~50MB** — raw flush interval 300s → 5s, parquet `max_row_group_size` 1M → 8K/4K, channel buffer 8192 → 2048

### Changed
- Docker deploy: added `--memory 192m` limit

## [0.2.0] — 2026-03-06

### Added
- **Hyperliquid adapter** — L2 orderbook + trades via WS, snapshot-only protocol
- **dYdX v4 adapter** — L2 orderbook (snapshot + batched diffs) + trades via Indexer WS
- Trade stream accumulation: `volume_delta`, `buy_vol`, `sell_vol`, `trade_count` columns in 1s snapshots
- `flush_with_levels()` on WindowAccumulator for exchanges without OrderBook (HL, dYdX)
- `on_diff_from_levels()` for OFI computation from raw best bid/ask
- `accumulate_trade()` on WindowAccumulator
- Smoke tests for Hyperliquid (2 tests) and dYdX (2 tests)
- Config: HL (7 pairs), dYdX (3 pairs), Binance spot+perp (6 pairs each)

### Changed
- Exchange enum: added `Hyperliquid`, `Dydx` variants
- Parquet schema: 4 new columns (volume_delta, buy_vol, sell_vol, trade_count)
- Config: added DOGE to Binance, replaced standalone BNB-only pairs

## [0.1.0] — 2026-03-01

### Added
- Initial release: Binance Spot + USDM Futures L2 collector
- BTreeMap orderbook with gap detection and auto-resnapshot
- 1s snapshot Parquet writer (60 columns)
- Raw diff Parquet writer with configurable rotation
- OFI, churn, microprice, imbalance, intra_sigma accumulation
- Blue-green Docker deployment via GitHub Actions
- Health monitoring with status.json
