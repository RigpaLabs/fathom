# ADR-001: Broadcast channel over mpsc/worker pool

Date: 2026-03-31
Status: Accepted

## Context

Fathom's connection tasks produce two event types — raw order book diffs and 1-second aggregated snapshots — that must reach multiple independent consumers: Parquet writers (raw + snap), NATS JetStream publisher, and potentially future sinks (e.g. real-time dashboards). The fan-out topology is 1 producer → N consumers per channel.

We needed a channel pattern that supports multiple receivers without the producer knowing how many consumers exist or what they do.

## Decision

Use `tokio::sync::broadcast` channels with a capacity of 4,096 messages. Two channels are created in `main.rs`:

```rust
let (raw_tx, _) = broadcast::channel::<RawDiff>(CHANNEL_BUFFER);    // 4096
let (snap_tx, _) = broadcast::channel::<Snapshot1s>(CHANNEL_BUFFER); // 4096
```

Each consumer calls `.subscribe()` to get its own independent receiver. Producers (connection tasks) call `.send()` on the shared sender. New sinks can be added by subscribing without modifying producer code.

## Consequences

**Benefits:**

- **Decoupled fan-out.** Connection tasks don't know about writers or NATS — they push to a channel and move on. Adding a new sink is one `.subscribe()` call in `main.rs`.
- **Independent consumer pace.** Each subscriber has its own read cursor. A slow NATS publisher doesn't block the Parquet writer (though it may lag and lose messages — see ADR-002).
- **Simple wiring.** No worker pool, no routing logic, no dispatcher task. The runtime handles delivery.

**Costs:**

- **Memory duplication.** Each message is cloned per active subscriber. With 3 subscribers on the snap channel, each `Snapshot1s` exists in memory 3 times. At 22 symbols × 1 row/sec × ~2 KB per snapshot, this is ~132 KB/sec — negligible on a 256 MB system.
- **Lagged subscribers lose data.** If a subscriber falls 4,096 messages behind, it receives `RecvError::Lagged(n)` and those messages are gone. This is an intentional trade-off (ADR-002).
- **No backpressure to producers.** A slow consumer cannot signal the producer to slow down. This is the correct behavior for a real-time market data pipeline — the WS feed is not something we can pace.

## Alternatives considered

**mpsc per consumer.** Would require the producer to maintain a list of senders and explicitly clone+send to each. Adding a new consumer means modifying connection task code. More coupling, no real benefit for our fan-out pattern.

**Dedicated worker pool / dispatcher task.** A central dispatcher that receives events and routes them to workers. Adds a hop of latency, a single point of failure, and scheduling complexity. Unnecessary when broadcast handles the topology natively.

**crossbeam or flume channels.** Third-party channels with different performance characteristics. Tokio broadcast integrates directly with the async runtime (no bridging needed), and our bottleneck is I/O (WebSocket + disk), not channel throughput.
