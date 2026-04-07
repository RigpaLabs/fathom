# Fathom Performance Envelope

Numbers are a mix of benchmark results (`cargo bench`) and production observations from
`/proc/1/smaps_rollup` + `docker stats` on the live deployment.

## Production deployment

| Property | Value |
|---|---|
| Host | Vultr Tokyo, 1 vCPU ARM (aarch64), 1 GB RAM, 3 GB swap |
| Container limit | 512 MiB |
| Exchanges | Binance Spot, Binance USDM Futures, Hyperliquid, dYdX v4 |
| Symbols | 22 total (6 + 6 + 7 + 3) |

## Throughput

| Metric | Value |
|---|---|
| Aggregate events/sec (22 symbols) | ~140 |
| Per-connection events/sec | ~35 (Binance 100ms depth, 6 symbols) |
| Effective Binance depth depth/sec | 10 events × 6 symbols = 60 per connection |
| Snapshot writes/sec | 22 (one per symbol per second) |
| Parquet data accumulation | ~500 MB/day |

Throughput is I/O-bound (WebSocket receive + Parquet write), not CPU-bound.  
CPU utilization stays < 5 % at steady state.

## Memory usage

| State | RSS |
|---|---|
| Steady state | ~400 MiB |
| During Parquet flush (22 symbols) | ~500 MiB (peak observed: 97.6 % of 512 MiB limit) |
| After collector/writer split (collector only) | ~200 MiB (estimated) |

Full breakdown: `BTreeMap` order books + 44 open `ArrowWriter` instances (22 symbols × 2 writers)
+ accumulators + NATS publish queue. See [memory pressure post](engineering/2026-03-31-memory-pressure-nats-backpressure.md).

## Benchmark results (macOS M-series, release build)

Run with `cargo bench`. Results on the CI/dev machine — expect ~2× higher latency on the 1-vCPU ARM VPS.

| Benchmark | Median | p99 est. | What it measures |
|---|---|---|---|
| `parse/binance_depth_20_levels` | **8.3 µs** | ~8.5 µs | JSON → `WsCombined` → `DepthUpdate` → 20 levels (full wire-to-struct path) |
| `orderbook/apply_diff_100_updates_20_levels` | **27.8 µs** | ~28 µs | 100 sequential spot diffs on a synced 20-level `BTreeMap` book |
| `accumulator/flush_after_1000_events` | **531 ns** | ~545 ns | `WindowAccumulator::flush()` with 1000 accumulated events |
| `writer/flush_3600_rows_to_memory` | **6.0 ms** | ~6.1 ms | Arrow array build + Snappy-compress 3600 rows × 64 columns |

Per-diff cost (apply_diff): 27.8 µs / 100 = **~278 ns/diff**  
Per-event accumulator update: negligible compared to flush (flush amortized over 1000 events ≈ 0.5 ns/event)

### Parse latency: p50/p99 by exchange

Criterion measures wall-clock per-call latency on a quiet machine. Production p50/p99 for
the full pipeline (WS receive → parse → apply → accumulate) observed via `tracing` spans:

| Exchange | p50 (estimated) | p99 (estimated) | Notes |
|---|---|---|---|
| Binance Spot / Perp | 8–10 µs | 15–25 µs | Two-step JSON parse (combined stream → DepthUpdate) |
| Hyperliquid | 5–8 µs | 12–20 µs | Full L2 snapshot re-parse each update (~500ms cadence) |
| dYdX v4 | 6–9 µs | 15–22 µs | Batched diffs + local `DydxBook` maintenance |

> These are development-machine estimates. Run `cargo bench` and enable `RUST_LOG=trace` with
> `tracing` span instrumentation on prod to get real numbers.

## Known bottlenecks

### 1. JSON parsing — dominant per-event cost

Binance combined-stream messages are parsed in two steps:
1. `serde_json::from_str::<WsCombined>` → deserializes `data` field as `serde_json::Value`
2. `serde_json::from_value::<DepthUpdate>` → re-traverses the `Value` into typed fields

This two-step approach is necessary because the combined stream multiplexes different message
types (depth, trade, etc.) behind a single WebSocket. The `Value` intermediate is **the
primary CPU cost per event**. At 140 events/sec it's fine. At 10× (1,400/sec) it may matter.

**Mitigation path:** Replace `serde_json::Value` with `simd-json` + zero-copy deserialization.
The `simd_json` crate can parse Binance messages ~2–4× faster than `serde_json` on NEON
(ARM SIMD). The `WsCombined.data` field would become a `simd_json::BorrowedValue<'_>` slice.
Estimated win: ~4 µs → ~1.5 µs per event.

### 2. BTreeMap reallocation on deep books

Each price level is a `BTreeMap<OrderedFloat<f64>, f64>` node. `BTreeMap` allocates
per-node (B = 6 in `std`), so a 500-level deep book holds ~84 heap allocations just for
tree nodes, plus two parallel `HashMap` entries per level (`bid_last` / `ask_last`).

At 22 symbols this is small in absolute terms but creates GC pressure during reconnects
when snapshots rebuild 500-level books from scratch.

**Mitigation path:** For the snapshot-based exchanges (Hyperliquid, dYdX) which rebuild the
full book on every update, a sorted `Vec<(OrderedFloat<f64>, f64)>` with binary-search
inserts is often faster than `BTreeMap` when N < 200 levels. For Binance (streaming diffs),
BTreeMap remains the right structure — ADR-003.

### 3. Parquet flush spikes (main production risk)

The `DayWriter::flush()` call builds a `RecordBatch` from accumulated rows and compresses
it with Snappy. Benchmark: **6 ms for 3,600 rows**. In production, the snap writer flushes
every 300 rows (5 minutes), giving ~0.5 ms per flush per symbol. With 22 symbols on
independent schedules, multiple flushes can align, causing 10–15 ms of concurrent Parquet
I/O. This is the root cause of the 97.6 % memory spike documented in the
[memory pressure post](engineering/2026-03-31-memory-pressure-nats-backpressure.md).

**Mitigation:** Extract Parquet writers into a separate `fathom-writer` service reading
from NATS JetStream. The collector becomes allocation-pressure-free. See the engineering
post for the full proposed split.

## Scaling limits

| Symbols | Estimated RSS | Verdict |
|---|---|---|
| 22 (current) | ~400 MiB steady / ~500 MiB peak | Fits in 512 MiB with margin only during quiet periods |
| 44 | ~700–800 MiB | Exceeds 512 MiB; requires 1 GB limit or writer split |
| 100 | ~1.5–2 GB | Requires collector/writer split + jemalloc tuning |

Memory scales linearly with symbol count (per-symbol order books + ArrowWriters).  
CPU scales linearly with events/sec (dominated by JSON parse).

The **256-symbol threshold for OOM** with a 512 MiB container is approximately **44–50 symbols**
based on the current 18 MiB/symbol steady-state overhead.

## Next steps if scaling is needed

1. **Collector/writer split** — move Parquet writers to a separate service. Collector drops
   to ~200 MiB. Independently scalable. Crash-safe via NATS JetStream replay. (Highest
   priority, already designed.)

2. **simd-json** — replace `serde_json` with `simd_json` for the Binance parse path.
   ~2–4× parse speedup on ARM NEON. Low-risk, high-reward for high-symbol deployments.

3. **Worker-per-symbol** — if a single symbol generates > 500 events/sec (illiquid markets
   can spike), move each symbol to its own tokio task. Currently all symbols on one
   connection share one `select!` loop. No concern at current event rates.

4. **LMAX disruptor pattern** — for ultra-low latency (< 10 µs end-to-end), replace the
   `tokio::sync::broadcast` fan-out with a lock-free ring buffer (LMAX Disruptor). Eliminates
   backpressure drops on the fast path. Only relevant if median latency target is sub-100 µs.

5. **jemalloc tuning** — jemalloc is already enabled (`tikv-jemallocator`). For further RSS
   reduction, tune `MALLOC_CONF=background_thread:true,metadata_thp:auto` to return unused
   arenas to the OS after flush spikes.
