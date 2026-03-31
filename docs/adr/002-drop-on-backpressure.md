# ADR-002: Drop on backpressure over block/retry

Date: 2026-03-31
Status: Accepted

## Context

Fathom's connection tasks sit in a tight WebSocket event loop processing market data at rates of 10–100 messages/second per symbol (22 symbols total). If the downstream broadcast channel fills up, the producer must either block (waiting for space) or accept the loss and continue.

Blocking the WS event loop has cascading consequences: Ping/Pong heartbeats stop, the exchange drops the connection after its timeout, triggering a full reconnect cycle (backoff + REST snapshot + re-sync) that loses far more data than a few dropped events.

## Decision

Use `tokio::sync::broadcast` with a fixed capacity of 4,096 messages. The broadcast channel's semantics handle backpressure by **overwriting the oldest unread messages** for slow subscribers, who then receive `RecvError::Lagged(n)`. The producer's `.send()` never blocks — it always succeeds (unless there are zero receivers).

On the consumer side, all receivers (raw writer, snap writer, NATS sink) handle `Lagged(n)` by logging a warning and continuing:

```rust
Err(broadcast::error::RecvError::Lagged(n)) => {
    warn!("snap_writer lagged by {n} messages");
    continue;
}
```

This means: if a consumer can't keep up, it skips forward and resumes from the latest available message. No retry of dropped messages.

## Consequences

**What data loss is acceptable:**

- **Raw diffs:** Losing a few diffs creates a gap in the raw Parquet file. The order book state (maintained in the connection task, before the channel) remains correct — only the written record has a hole. For microstructure research, a gap of <1 second in raw data is acceptable; the 1-second snapshots are the primary analysis artifact.
- **1s snapshots:** Losing a snapshot means a missing row in the daily file. At 1 row/sec, the 4,096-message buffer provides ~68 minutes of headroom per symbol before any loss occurs. In practice, the snap writer processes rows faster than they arrive.

**What data loss is not acceptable:**

- Order book state corruption. This never happens because the book lives in the connection task, upstream of the channel. The channel carries computed results, not state mutations.

**Benefits:**

- The WS event loop never stalls. Connection liveness is preserved.
- No complex retry logic or dead-letter queues for a real-time data stream where retries are meaningless (the market has moved on).
- Simple failure mode: warn + skip forward. Easy to monitor via log alerts.

**Costs:**

- Lagged consumers silently lose messages. Monitoring `Lagged` warnings is required to detect sustained performance issues.
- No way to reconstruct dropped raw diffs after the fact (the exchange doesn't offer replay). Acceptable for a research data pipeline.

## Alternatives considered

**Blocking send (`mpsc` with bounded channel).** Would preserve all messages at the cost of stalling the WS loop. A stalled loop means missed heartbeats → forced disconnect → reconnect cycle losing 5–60 seconds of data. Blocking to save a few milliseconds of data loses seconds.

**Retry queue / dead-letter.** Market data is ephemeral — by the time a retry runs, the data is stale. Adding retry infrastructure increases complexity without improving data quality.

**Unbounded channel.** Eliminates backpressure entirely but risks unbounded memory growth if a consumer hangs. On a 256 MB VPS, OOM is a real threat. The 4,096 bound caps memory at a predictable level.
