# ADR-005: Acceptable data loss window by component

Date: 2026-03-31
Status: Accepted

## Context

Fathom runs continuously on a 256 MB VPS collecting market data 24/7. Crashes, OOM kills, disk errors, and ungraceful Docker stops are real scenarios. Each component buffers data in memory before writing to disk — the amount of in-flight data determines how much is lost on a crash.

We need explicit bounds on data loss for each component, balancing write frequency (more I/O, more disk wear, lower throughput) against loss tolerance.

## Decision

### Raw diff writer

| Parameter | Value |
|-----------|-------|
| Flush interval | **5 seconds** (`RAW_FLUSH_INTERVAL_S = 5`) |
| Row group size | 8,192 rows |
| File rotation | Every `raw_rotate_hours` (default 1 hour) |
| Crash loss window | **≤5 seconds** of raw diff data |

The raw writer buffers diffs in memory and flushes to disk every 5 seconds. Each flush writes a Parquet row group. On crash, at most 5 seconds of buffered diffs are lost.

The 5-second interval was chosen as a balance: short enough that crash loss is negligible for research purposes, long enough to batch I/O efficiently (hundreds of diffs per flush, not one write per event).

### 1s snapshot writer

| Parameter | Value |
|-----------|-------|
| Disk flush interval | **300 rows** (~5 minutes at 1 row/sec per symbol) |
| Row group size | 4,096 rows |
| Crash loss window | **≤5 minutes** of 1s snapshot data per symbol |

The snap writer accumulates rows in memory and calls `writer.flush()` every 300 rows. This triggers a Parquet row group write and `fsync`. Without this explicit flush, the default ArrowWriter row group threshold (1M rows) would never trigger at 1 row/sec — an entire day's data would sit in memory until `finish()`.

The 300-row interval limits crash loss to ~5 minutes while keeping disk writes infrequent (once every 5 minutes per symbol, not once per second).

### Trade tape writer

| Parameter | Value |
|-----------|-------|
| Flush interval | **5 seconds** (shares `RAW_FLUSH_INTERVAL_S`) |
| File rotation | Every `raw_rotate_hours` (default 1 hour) |
| Crash loss window | **≤5 seconds** of trade data |

The trades writer mirrors the raw diff writer (same buffer/flush/rotation pattern, same rationale): trade volume is far below depth volume, so the 5-second batch is comfortably efficient while keeping crash loss negligible.

### Graceful shutdown

On SIGTERM/SIGINT:

1. `CancellationToken` fires → connection tasks stop producing.
2. Connection tasks are awaited **sequentially, 5 s timeout each**.
3. Broadcast senders drop → receivers drain remaining messages.
4. Raw writer flushes all buffered diffs and finalizes Parquet files.
5. Snap writer flushes remaining rows and writes Parquet footer.
6. Partial buckets are renamed to their final `{prefix}_HHMM_HHMM.parquet` names.
7. Process exits cleanly — **zero bucket loss** on graceful shutdown.

**This requires the deployment to grant enough time — it is a precondition, not a given.**

Only step 2 is time-bounded. Its `5s` timeout is *per connection* and the loop is sequential, so
that phase alone costs up to `5s × connections` and grows as venues and symbols are added. Steps
3-6 have **no timeout at all** (`raw_handle.await` and friends in `src/main.rs`): writer finalize
is bounded only by how long the flush, `.finish()` and rename actually take, which depends on
buffered rows and disk. Under disk pressure (see `specs/storage.md`, "no retention") that is not
hypothetical. So the honest worst case is:

```
(≤5s × connections)  +  (writer finalize + NATS drain, unbounded)
```

Observed drains are ~1.5 s total, because connections honour the cancel token immediately and the
writers finalize fast. Docker's default grace is **10 s**, already below the bounded phase alone at
today's connection count. Deployments must set `stop_grace_period: 30s` (as
`docker-compose.prod.yml` does). If the grant expires, Docker sends SIGKILL mid-drain and the open
bucket is lost whole — see the 2026-07-15 erratum, where this requirement went unmet in production
for months. Note that no grant is a *guarantee* while the finalize phase is unbounded; 30 s buys
headroom against the drain we can measure, not a proof.

### NATS sink

Both NATS streams (snapshots and depth diffs) use JetStream with file-backed storage and double-await ACK. If NATS is down or the broadcast channel lags, messages are dropped on the fathom side. The Parquet writers are the durable store, not NATS. Under memory pressure, the NATS depth sink can lag behind the broadcast channel, causing permanent message gaps for downstream consumers (see [engineering blog: memory pressure incident](../engineering/2026-03-31-memory-pressure-nats-backpressure.md)).

## Consequences

**Worst-case crash data loss:**

| Component | Loss window | Impact |
|-----------|------------|--------|
| Raw diffs | 5 seconds | Tiny gap in raw archive. 1s snapshots (primary artifact) unaffected. |
| 1s snapshots | 5 minutes | ~300 missing rows per symbol. For daily analysis, 5 min out of 86,400 sec = 0.35% loss. |
| NATS depth | Entire in-flight buffer | NATS is best-effort. Loss acceptable — consumers should tolerate gaps. |
| NATS snapshots | Last unACKed message | At most 1 message per symbol (JetStream ACK confirms durability). |

**Process crash vs OS crash:** A Rust panic triggers the abort handler (`panic = "abort"` in release profile). There is no unwinding or cleanup — loss is the same as an OOM kill. The flush intervals are the actual safety net, not graceful shutdown.

**Benefits:**

- Predictable, bounded data loss on any failure mode.
- Low disk I/O: raw flushes every 5s, snap flushes every 5 min.
- Graceful shutdown achieves zero *bucket* loss for planned restarts (deploys, config changes).
  Such a restart still costs the WebSocket gap — disconnect to resync, ~3 s measured (2026-07-15
  erratum). Requires an adequate `stop_grace_period`.

**Costs:**

- 5 minutes of snap data at risk on crash. Reducing to 60 rows (1 minute) would 5× the disk writes. Accepted because crashes are rare and 5 minutes of missing 1s data is tolerable for research.
- Raw diffs are not individually ACKed. A crash loses the entire in-flight buffer. Accepted because raw diffs are supplementary to 1s snapshots.

## Errata (2026-07-04): restart loss was unbounded, not the flush-interval window

The bounds above (≤5s raw, ≤5min snap) describe the *steady-state buffering* loss for a
single continuously-running process. They did not hold for a **restart**: before this fix,
`snap_1s.rs`'s `DayWriter` and `deriv.rs`'s `FeedWriter` each wrote one file per calendar day
via a bare `File::create`, opened lazily through a `HashMap` that starts empty on every process
start — there was no distinction between "first event of a new day" and "process restarted
mid-day", so a restart truncated whatever had already been written for that day. The actual
restart-crash worst case was **unbounded up to ~24h** (whatever fraction of the day preceded the
restart), not the 5-minute figure above. Confirmed in production 2026-07-04: 3 fathom restarts
destroyed ~22h and ~3.5h of data in the 1s/deriv feeds.

Fix: `snap_1s.rs` and `deriv.rs` now rotate on the same schedule as `raw.rs`/`trades.rs`, via the
same temp-file-then-rename rotation pattern (`snap_1s.rs`/`deriv.rs` via the shared `Bucket` in
`src/writer/rotation.rs`; `raw.rs`/`trades.rs` at the time via their own pre-existing, independent
implementation of the identical pattern — see "Restart-crash bound, all writers" below). Post-fix,
an **ungraceful** restart's worst case is **≤ the configured rotation bucket** (`raw_rotate_hours`,
currently 1h in prod/default — any divisor of 24 up to 24h is configurable), not the entire day.
This bounds the loss; it does not eliminate it — the bucket open at the moment of the crash is
still entirely lost. An `ArrowWriter` that never reaches `.finish()` has no Parquet footer, so that
bucket's data is unreadable, not "missing a few rows". This is a bounded-loss design, not full
restart-safety. See `specs/storage.md` for the naming/rotation contract.

This bound describes a **crash**. A graceful stop loses no bucket at all — see the
2026-07-15 erratum below before quoting any figure here as the cost of a deploy.

**Update (follow-up refactor, same day):** `raw.rs`/`trades.rs` have since been migrated onto the
same shared `Bucket` type too — the "own, independent implementation" described above and in
"Restart-crash bound, all writers" below is no longer accurate; all four writers now share one
mechanism. This also closed a smaller latent gap in `raw.rs`/`trades.rs`'s old separate
`should_rotate`: it compared only the hour-bucket value, not date+hour like `Bucket::should_rotate`
does, so a bucket could in theory survive a date boundary uncrossed if the hour-bucket value
happened to repeat (e.g. `raw_rotate_hours=24`). See `specs/storage.md`.

### Restart-crash bound, all writers (correction to the tables above)

The per-component "Crash loss window" figures in the Decision tables above (≤5s raw, ≤5s trades,
≤5min snap) describe **steady-state buffering loss only** — how much unflushed, in-memory data is
lost if a *continuously-running* process crashes between flushes. They were never an accurate
restart-crash bound, including for raw/trades: an `ArrowWriter` writes row groups into a temp file
across many `flush()` calls, but the Parquet footer (schema + row-group offsets) is only written at
`.finish()`. Without it, the *entire* temp file is unreadable — every row group written since the
bucket opened, not just the last 5 seconds of buffer. This was already true for raw/trades before
today's fix (their bucket was already hourly, so the practical exposure was smaller — "only" up to
one hour, not up to a full day like the pre-fix snap/deriv — but the "≤5s" figure was still wrong
for a *restart*, not just a design gap unique to snap/deriv).

**Post-fix, all four writers (raw, trades, snap, deriv) share the same restart-crash bound: ≤ the
configured `raw_rotate_hours` bucket, currently 1h in prod/default.** The 5s/5min figures in the
tables above remain correct descriptions of steady-state buffering loss for a process that keeps
running; they are not the restart bound for any writer, not just snap/deriv.

## Errata (2026-07-15): a planned deploy costs seconds, not a bucket

The bounds above are so heavily qualified with "restart" that they have been read as *"every
restart destroys the open bucket"*, and that reading was used to justify designing a two-instance
blue-green hand-off. It is wrong. Every figure in this ADR describes a **crash**. A planned deploy
is a graceful stop, and a graceful stop loses no bucket.

Measured on a production restart, 2026-07-14 18:03 UTC:

| Observation | Value |
|---|---|
| Partial bucket, old process | `depth_1800_1803.parquet` — footer intact, **2139 rows**, events 18:00:00.068 → 18:03:38.164 |
| Drain duration | ~1.5 s (last event 18:03:38.164 → rename 18:03:39.69) |
| New process resynced | `snapshot ok` at 18:03:41.413 |
| **Actual data gap** | **≈3.2 s** of depth diffs, ~3 rows/symbol of 1s snaps |

The old process closed its 3-minute bucket into a valid file; the new one opened its own for the
same hour and closed it at 18:59 (`depth_1800_1859.parquet`). The two coexist naturally: the
end-HHMM comes from each writer's own last-event time, so same-hour buckets from different
processes get different names and never collide. (`Bucket::close_and_rename`'s `_2`/`_3` suffix
branch is for the narrower case of two closes landing on an identical end-HHMM; this restart did
not exercise it.) The only real cost of a restart is the WebSocket gap: disconnect → connect →
resync, a few seconds.

**Precondition — the grant must exceed the drain.** This ADR's "Graceful shutdown" section stated
`stop_grace_period: 30s` as if it were a fact of the system. It is not; it is a *requirement on the
deployment*, and it was not met. The production Compose file never set it, so fathom ran with
Docker's 10 s default (`docker inspect` → `StopTimeout=<nil>`) for its entire life until
2026-07-15, when it was set to 30 s.

The graceful path had been working on luck. The bounded phase alone costs `5s × connections`
(connections as configured in the private deployment, 7 as of 2026-07-15 — this repo's reference
`config.toml` enables fewer; the number is a property of a deployment, not of this repo, so treat
it as a dated observation rather than a constant), and the finalize phase after it is unbounded.
Against a 10 s grant, either phase could have overrun. It never did, because connections honour
the cancel token promptly and writers finalize fast — hence the 1.5 s measured above.

"Promptly" is not "always immediately": a connection sitting in a reconnect or rate-limit backoff
sleep (e.g. `src/connection/binance.rs`'s `sleep_backoff`) does not race that sleep against
cancellation, so it only notices on wake, capped by its 5 s timeout. A deploy that lands while
several connections are backing off is the realistic bad case — likelier than the "one hung
connection" framing suggests, since backoff is normal operation, not a fault.

**Where the 30 s came from — and how it evaporated.** The figure was never arbitrary: the original
deploy procedure (a GitHub Actions blue-green pipeline, documented in README) stopped the old
container with an explicit `docker stop -t 30`. This ADR then recorded 30 s as though the system
had it. When deployment moved to an external Compose-based setup, that procedure and its `-t 30`
disappeared together, and nothing carried the grant across; the ADR kept asserting it. A number
that lives in a deploy script and is restated as a fact in a doc has no owner once the script dies.
`docker-compose.prod.yml` in this repo happened to carry `stop_grace_period: 30s` from its first
commit, so the reference config stayed correct while the actual deployment drifted — which is
exactly where a doc that states a precondition as a fact stops being checkable.

**When quoting this ADR:** the bounded-loss figures answer *"what does a crash cost?"*. They do not
answer *"what does a deploy cost?"* — that answer is ~3 s, and it is bounded by WebSocket resync,
not by the rotation bucket.

## Alternatives considered

**Write-through (every event to disk).** Eliminates buffering loss but generates thousands of small writes per second. On SSD, this accelerates wear; on HDD, it's a throughput bottleneck. Not viable on a 256 MB VPS where disk I/O competes with the OS.

**WAL (write-ahead log).** Write events to a WAL file, then batch-compact into Parquet. Adds complexity (WAL rotation, replay on recovery) for a marginal improvement — recovering 5 seconds of raw diffs is not worth a WAL implementation.

**Shorter flush intervals (1 second).** Raw writer at 1s flush is feasible but unnecessary — 5s loss is already negligible. Snap writer at 1s flush means 22 fsync calls per second (22 symbols), which is significant I/O on a small VPS.
