# Memory Pressure & NATS Backpressure at 22 Symbols

**Date:** 2026-03-31

Fathom collects Level-2 order book data from 4 exchanges via WebSocket. It reconstructs full order books, computes derived metrics (OFI, churn, microprice), writes Parquet files, and publishes events to NATS JetStream. All in one Rust binary, running in a Docker container on a 2 GB ARM instance.

22 symbols across Binance Spot, Binance Futures, Hyperliquid, and dYdX. About 140 events per second aggregate.

This post documents a memory pressure incident we caught through Grafana, the cascade it triggered, and why it's driving an architectural split.

## What happened

After 5 hours of runtime, Grafana showed fathom at **97.6% of its 512 MiB container limit**. At the same time, NATS started logging ACK timeouts:

```
WARN NATS depth ACK error: timed out: didn't receive ack in time
WARN NATS depth sink lagged by 697 messages
```

The system recovered on its own. No data loss, no OOM kill. But 97.6% is not a margin you want to rely on.

## Where the memory goes

```
VmPeak:   1,471,048 kB
VmRSS:     418,392 kB
Pss_Anon:  414,432 kB  (99% of RSS)
Pss_File:    6,401 kB
```

405 MB of heap. Four consumers:

**BTreeMap order books.** 22 symbols, each holding a full L2 book as a `BTreeMap<OrderedFloat<f64>, f64>`. BTreeMap chosen for sorted iteration ([ADR-003](../adr/003-btreemap-for-order-book.md)). Each book holds hundreds of price levels. HashMap would save ~30% memory but require sorting on every top-N extraction. At 22 symbols updating 6 times per second, the sorted invariant pays for itself.

**44 open ArrowWriters.** 22 symbols × 2 writers (raw diffs + 1-second snapshots). Each writer holds an open Parquet row group with Arrow column buffers. The raw writer flushes every 5 seconds. The snap writer flushes every 300 rows (5 minutes). Between flushes, accumulated data sits in memory.

**Accumulators.** Per-symbol 10-second window accumulators for OFI, churn, and microprice. Small individually, but 22 of them add up.

**NATS publish queue.** The async-nats client buffers outgoing messages. During high-throughput periods, this buffer grows.

## Why it spikes

The snap writer flush (every 5 minutes per symbol) is the trigger. Each flush:

1. Creates an Arrow RecordBatch from 300 rows × 64 columns
2. Compresses with zstd
3. Writes to disk
4. Frees the old buffer

With 22 symbols on independent 5-minute offsets, some symbol is mid-flush at any given moment. When multiple symbols align, memory jumps from 400 MB steady state to 500 MB. On a 512 MB limit, that's 97.6%.

This isn't one big flush. It's a continuous stream of overlapping flush cycles. The "spike" we caught was several symbols flushing at the same time.

## Why NATS times out

Fathom publishes to NATS JetStream with File storage. Every published message hits disk. FATHOM_DEPTH alone produces about 7,500 messages per minute.

The cascade works like this:

```
Parquet flush starts (multiple symbols aligned)
  → memory spikes from 400 to 500 MB
  → tokio runtime under allocation pressure
  → NATS publish tasks don't get scheduled
  → messages queue in the async-nats internal buffer
  → buffer grows → more memory pressure (positive feedback)
  → NATS server is also flushing File storage to the same EBS volume
  → ACK takes too long → timeout
  → fathom logs "depth sink lagged by 697 messages"
  → flush completes → pressure drops → system recovers
```

The disk I/O is low in steady state (1-2% utilization). The bottleneck is not the disk itself but tokio task scheduling under memory pressure. Parquet serialization and NATS publishing compete for the same thread pool. When one spikes, the other starves.

Fathom uses `try_send` (non-blocking) on its internal broadcast channel ([ADR-002](../adr/002-drop-on-backpressure.md)). If a writer can't keep up, events drop instead of blocking the WebSocket event loop. This keeps connections alive during pressure. NATS publishes lag but catch up once the flush completes.

## The numbers

| Metric | Steady state | During flush |
|--------|-------------|-------------|
| Fathom RSS | ~400 MB | ~500 MB |
| Container usage | 78% | 97.6% |
| NATS memory | 87 / 128 MB | ~120 MB |
| Events/sec | 140 | 140 (unchanged) |
| NATS depth lag | 0 | 697 messages |
| Disk %util | 1.3% | ~2% |
| Parquet data dir | 14 GB | growing ~500 MB/day |

Collected via `/proc/1/smaps_rollup`, `docker stats`, and `iostat`.

## Why splitting the writer fixes this

Bumping the memory limit from 512 to 768 MB buys time. It doesn't fix the problem. At 44 symbols (2× current), we'd hit 768 MB the same way.

The fix is extracting the Parquet writers into a separate service:

```
Today:
  fathom (512 MB)
    WS → books → accumulators → Parquet writers → NATS publish
    All share one tokio runtime and one memory pool

After split:
  fathom-collector (256 MB)       fathom-writer (512 MB)
    WS → books → accum → NATS      NATS → Parquet writers
    No flush spikes                 Can crash without losing data
    NATS publish never starved      Replays from last ACK on restart
```

The collector drops to ~200 MB. No ArrowWriters, no Parquet buffers, no periodic flush spikes. NATS publishing runs in a runtime that only does networking. No contention with disk I/O.

The writer reads from NATS JetStream. If it crashes or falls behind, no data is lost. JetStream tracks the last acknowledged message and replays from there on reconnect. The writer can run on a different host with different memory and disk characteristics.

## Diagnostics reference

```bash
# Memory breakdown
docker exec fathom cat /proc/1/smaps_rollup
# Rss: 421804 kB, Pss_Anon: 414432 kB

# Container stats
docker stats fathom --no-stream
# 401.4MiB / 512MiB (78.41%)

# NATS backpressure
docker logs fathom 2>&1 | grep "ACK error\|lagged"
# depth sink lagged by 697 messages

# Disk utilization
iostat -x nvme0n1 1 5
# %util: 1.3-2.2%

# Data volume
docker exec fathom du -sh /app/data/
# 14G
```
