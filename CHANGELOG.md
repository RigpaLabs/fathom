# Changelog

## [Unreleased]

### Fixed
- **Order book memory leak (unbounded → capped)** — the shared `OrderBook` (Binance/Bybit) only removes a price level on an explicit 0-qty diff, which exchanges send solely for levels inside their active window. A level that drifts far from the mid is never removed, so a long-lived book — Binance perp resyncs ~once per 6 days — accumulated stale deep levels without bound (observed ~800 MB anon RSS over 6 days, approaching the 1 GiB container limit). `bids`/`asks` and their `bid_last`/`ask_last` mirrors are now bounded per side: once a side exceeds `BOOK_PRUNE_TRIGGER` (1200) it is pruned back to `BOOK_LEVEL_CAP` (1000) nearest-mid levels (the gap is hysteresis — steady state holds ≤1200, well under the old unbounded growth). Near-mid metrics stay exact — we read only the top 10 levels (snapshot columns, OFI, microprice, imbalance, depth L5/L10), the cap is ~100× deeper, and full depth is preserved in the raw-diff stream. One accepted caveat: full-book churn (`bid_abs_change`) loses its baseline for a pruned level that later re-enters (deep-book noise, dwarfed by near-mid churn). Same root cause as the 0.3.0 dYdX crossed-book fix, different symptom.
- **1s/deriv restart data loss (unbounded → ≤1 bucket)** — `snap_1s.rs` and `deriv.rs` wrote one file per calendar day via a bare `File::create`, opened lazily through a `HashMap` that started empty on every process restart. A restart mid-day truncated whatever had already been written for that day — 3 production restarts on 2026-07-04 destroyed ~22h and ~3.5h of data. Both writers now rotate hourly via the same temp-file-then-rename `Bucket` pattern `raw.rs`/`trades.rs` already used, bounding restart loss to the single bucket open at crash time (`raw_rotate_hours`, default 1h). Also fixes a related gap where a sparse deriv feed (e.g. `liq`) could hold its file open for hours past the bucket boundary — the deriv writer now force-rotates a stale bucket on its periodic tick even with no new events.

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
