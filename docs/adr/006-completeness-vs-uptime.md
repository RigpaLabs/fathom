# ADR-006: Completeness vs uptime trade-off

Date: 2026-03-31
Status: Accepted

## Context

A market data collector faces a fundamental tension: should it prioritize **completeness** (capture every event, even at the cost of pausing or crashing) or **uptime** (stay connected and collecting, even if some data is lost)?

For Fathom's use case — collecting microstructure data for quantitative research — a 30-second connection outage loses more data than dropping a handful of events from a full buffer. The system must favor staying alive over capturing every last byte.

## Decision

Fathom systematically chooses **uptime over completeness** at every decision point where the two conflict:

### 1. Broadcast channels drop data rather than block producers

When a subscriber falls behind, `broadcast` overwrites old messages and the subscriber receives `Lagged(n)`. The producer (WS event loop) never blocks. This preserves connection liveness at the cost of consumer-side gaps. (See ADR-002.)

### 2. Order book gaps trigger reconnect, not pause

A sequence gap means the book state is potentially wrong. Rather than pausing output and waiting for recovery, the system tears down the entire connection and reconnects from scratch. This loses a few seconds of data for all symbols in the group but guarantees that when data resumes, it's correct. (See ADR-004.)

### 3. Writer failures are fatal — connection failures are not

| Component | Failure policy |
|-----------|---------------|
| Raw Parquet writer | **Fatal** — process exit |
| Snap Parquet writer | **Fatal** — process exit |
| Connection tasks | Restart with exponential backoff |
| NATS sink | Warn + continue |
| Metrics server | Warn + continue |
| Monitor | Warn + continue |

Writers are fatal because if we can't write data, running is pointless — we'd consume resources while silently losing everything. Connections restart because a network hiccup is transient. NATS and metrics are ancillary — their failure doesn't affect data collection.

### 4. Reconnect backoff preserves the connection long-term

Exponential backoff: 1s → 2s → 4s → ... → 60s max, with 25% jitter. Rate limit errors (Binance codes -1003, -1015) trigger an extended 300-second (5-minute) backoff. This ensures the system doesn't get banned for aggressive reconnection, which would cause a far longer outage.

### 5. State is fully cleared on reconnect

On any reconnect, all per-symbol state (order books, accumulators, previous snapshots) is cleared. The system restarts from a clean slate rather than attempting to recover partial state. This trades a few seconds of data for certainty that post-reconnect data is correct.

### 6. NATS failure doesn't affect Parquet output

If NATS is down or slow, the NATS sink logs warnings and continues. Parquet writers — the durable store — are completely independent. NATS is a real-time distribution layer, not a persistence layer.

## Consequences

**Why this is the right trade-off for microstructure research:**

- Quantitative models consume daily files with 86,400 rows. A 30-second gap (0.03% of a day) has negligible impact on signal quality.
- A stale or incorrect order book produces *wrong* data, which is far more dangerous than *missing* data. Models trained on corrupted microstructure signals produce incorrect trading decisions.
- Continuous operation over weeks/months matters more than perfect capture in any single minute. The system has run for months with <1 gap/day in production.

**Monitoring:**

- `status.json` (updated every 30s) tracks reconnect counts and per-symbol last-event age.
- `Lagged` log warnings indicate consumer backpressure.
- Gap detection log warnings (`"gap — reconnecting"`) indicate sequence breaks.
- Prometheus metrics (`/metrics` endpoint) expose write errors and reconnect counts.

**Costs:**

- Missing data cannot be recovered after the fact. Binance does not offer a historical diff replay API. Hyperliquid and dYdX similarly provide no replay.
- A sustained infrastructure issue (e.g., 10-minute network outage) creates a 10-minute hole in all data streams. No amount of buffering helps — the events simply don't arrive.

## Alternatives considered

**Completeness-first (block on backpressure, retry on gap).** Would preserve more events in theory but would cause cascading failures: blocked WS loops → missed heartbeats → exchange disconnects → longer outages → more data loss. The "safe" approach is actually less safe.

**Hybrid (local event log + async replay).** Write raw WS frames to a local append log for recovery, then process asynchronously. Adds disk I/O in the critical path, complexity for replay logic, and still doesn't help when the WS connection itself is down. The cost/benefit doesn't justify the complexity.

**Redundant collectors.** Run multiple Fathom instances in different regions and merge their output. Viable but a deployment-level solution, not an architecture decision within the collector itself. Can be layered on top of the current design without changes.
