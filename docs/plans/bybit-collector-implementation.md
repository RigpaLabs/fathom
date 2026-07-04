# Bybit Collector — Implementation Plan

Companion to [`specs/bybit-collection.md`](../../specs/bybit-collection.md) (read that first — this
file is *how to build it*, the spec is *what it must do*). No code written yet. Structured into
work packages so pieces can be picked up by separate agents with minimal file overlap — dependency
order is noted per package.

## Ground truth already confirmed in this codebase (don't re-derive)

- **Writers are fully exchange-agnostic.** `RawDiff`/`RawTrade`/`MarkFunding`/`OpenInterest`/
  `Liquidation`/`Snapshot1s` (`crates/fathom-types`) all carry `exchange: String` — no enum, no
  per-venue branching anywhere in `src/writer/*.rs` or `src/nats_sink.rs`. **Adding Bybit touches
  zero writer code and zero NATS code.** This is the main reason the work is parallelizable: the
  entire task is new adapter + connection-task + config + tests, isolated from everything that
  landed in the recent hourly-rotation fix (fathom#52) and the raw/trades→`Bucket` migration
  (in progress as of this writing) — no file conflicts expected between that work and this.
- **Adapter pattern**: `src/exchange/mod.rs`'s `ExchangeAdapter` trait (`name`, `ws_url`,
  `snapshot_url`, `open_interest_url`), one impl file per exchange+market (`binance_spot.rs`,
  `binance_perp.rs`, `hyperliquid.rs`). Bybit needs two impls: `bybit_spot.rs`, `bybit_perp.rs`.
- **Connection-task dispatch**: `src/main.rs` matches on `conn.exchange` (the string from
  `config.toml`'s `[[connections]]` blocks) and spawns the matching `connection_task*` function
  (`src/connection/mod.rs` re-exports `connection_task` (Binance, shared by spot+perp),
  `connection_task_hl`, `connection_task_dydx`). Bybit needs its own entry, e.g.
  `connection_task_bybit`, taking a category/adapter parameter the same way `connection_task`
  already takes an `&dyn ExchangeAdapter` — check the exact current signature in
  `src/connection/binance.rs::connection_task` before assuming.
- **`OrderBook::apply_snapshot`/`apply_diff`** (`src/orderbook/mod.rs`) are already generic over
  `SnapshotMsg`/`DepthDiff` — Bybit's connection task builds these from its own wire schema and
  calls the same two methods everything else uses. No orderbook module changes needed.
- **No REST snapshot fetch, no REST OI poll for Bybit** — both are WS-native (see spec's
  "Architecturally different" and "no REST poll needed" sections). This makes the Bybit connection
  task structurally *simpler* than Binance's (no buffer-replay sync phase, no `poll_open_interest`
  background task), closer in shape to Hyperliquid's single-WS-loop pattern but with real gap
  detection unlike HL.
- **Config**: `config.toml`'s `[[connections]]` blocks (`name`, `exchange`, `symbols`, `depth_ms`)
  — Bybit ignores `depth_ms` (documented precedent: HL already does this), hardcodes depth-1000
  orderbook subscription per the spec.

## Work packages

### WP1 — Exchange adapters (`src/exchange/bybit_spot.rs`, `src/exchange/bybit_perp.rs`)

**Depends on**: nothing. **Blocks**: WP2 (connection task needs the adapter to exist, at least as
a stub, to compile against).

- Implement `ExchangeAdapter` for both. `ws_url` returns the fixed category base URL (no query
  params needed unlike Binance's combined-stream URL — Bybit subscribes via a separate `op:
  subscribe` WS message after connecting, not URL query params; check whether the existing
  `ExchangeAdapter::ws_url` signature/usage in `connection_task` assumes a Binance-style
  "URL already encodes all subscriptions" pattern, since Bybit doesn't work that way — this might
  require either a small trait extension (e.g. a `subscribe_message(&self, symbols) -> String`
  method) or handling the subscribe-after-connect step directly in the new Bybit connection task
  without going through `ws_url` for that part. Decide and document which, don't silently
  shoehorn Bybit into an ill-fitting trait shape.
- `snapshot_url`/`open_interest_url`: both `None` for Bybit (WS-native snapshot, WS-native OI via
  tickers — see spec).
- Add `mod bybit_spot; mod bybit_perp;` + re-exports to `src/exchange/mod.rs`.
- Unit tests: adapter URL construction (mirror the existing `binance_perp.rs`/`binance_spot.rs`
  test style if any exist inline, or `tests/` if that's the convention — check first).

### WP2 — Connection task + gap detection (`src/connection/bybit.rs`)

**Depends on**: WP1 (adapter). **Blocks**: WP4 (main.rs wiring), WP5 (e2e tests).
**This is the largest, most novel work package** — budget the most time/review attention here.

- New file `src/connection/bybit.rs`, exported from `src/connection/mod.rs` as
  `connection_task_bybit` (or however the dispatch match in `main.rs` ends up calling it — keep
  consistent with the existing `connection_task_hl`/`connection_task_dydx` naming).
- One task handles one category (spot or linear) for all its configured symbols — same "one WS
  connection, N symbols, one tokio task" shape as every other connection task.
- Subscribe: `{"op": "subscribe", "args": [...]}` listing `orderbook.1000.{symbol}`,
  `publicTrade.{symbol}` for every symbol, plus (linear only) `tickers.{symbol}` and
  `allLiquidation.{symbol}`. **Spot caps a `subscribe` request at 10 args (spec §Channels), and
  spot's 6 symbols × 2 topics = 12 → the spot task must send its subscription in ≥2 batched
  `subscribe` messages (≤10 args each) on the same socket.** Linear (24 args) fits one message.
  Same batching must run on every reconnect, not only first connect.
- Ping: send `{"op": "ping"}` about every 20s (spec: heartbeat keepalive) — reuse whatever
  ping/keepalive pattern an existing connection task already has (check `binance.rs`/
  `hyperliquid.rs` for the tokio interval/select pattern) rather than inventing a new one.
- Message dispatch by `topic` prefix (`orderbook.`, `publicTrade.`, `tickers.`, `allLiquidation.`)
  → parse the JSON shape from the spec → route to the relevant handler:
  - `orderbook.*`: `type: "snapshot"` → `apply_snapshot`, full reset of that symbol's local book
    (this handles both first-sync and a server-initiated resync — Bybit may push `snapshot`
    unprompted on a server-side issue); `type: "delta"` → gap-check `u` against last-seen `u` for
    that symbol, `apply_diff` if contiguous, else **treat it as a client-detected gap: log it via
    `src/monitor.rs`'s gap tracking, then break out and reconnect+resubscribe** (per spec §Gap
    detection — Bybit does NOT promise an unsolicited snapshot for a client-detected loss, so
    passively waiting can hang; the reconnect rebuilds from a fresh `snapshot`).
  - `publicTrade.*`: iterate the `data` array (can hold up to 1024 trades — **do not assume one
    trade per message**, this is a real behavioral difference from Binance's `aggTrade`), emit one
    `RawTrade` per element.
  - `tickers.*` (linear only): merge onto an in-memory per-symbol last-known-ticker-state struct
    (see spec's "Ticker" section for the exact merge design), emit `MarkFunding`/`OpenInterest`
    from the merged state on relevant field changes.
  - `allLiquidation.*` (linear only): iterate `data` array, emit one `Liquidation` per element.
- **Reconnect loop (required, not implicit)**: wrap the connect→subscribe→read-loop in an outer
  loop that, on any socket close / read error / client-detected gap, applies backoff (reuse the
  existing connection tasks' reconnect/backoff pattern — check `binance.rs`/`hyperliquid.rs`),
  **resubscribes all topics** (spot in its ≤10-arg batches), and **resets all per-symbol state —
  order books AND the in-memory ticker-merge state (WP3)** — before accepting any delta, so a stale
  pre-reconnect book/ticker can never be merged against a fresh post-reconnect stream. The first
  post-reconnect message per symbol must be a `snapshot`; drop deltas until it arrives.
- Gap/reconnect/liveness integration: register with `src/monitor.rs` the same way every other
  connection does (symbol staleness tracking, reconnect counting) — check `ConnStats`/
  `SymbolStats` usage in an existing connection task for the exact calls to make.

### WP3 — Ticker-state merge logic (could split out of WP2 if useful for parallelism)

**Depends on**: nothing structurally (pure logic, no I/O) — **could run in parallel with WP1/WP2**
if given the message schema up front (it's fully specified in `specs/bybit-collection.md`'s
"Ticker" section), since it's a self-contained data-merge problem: given a stream of
snapshot+partial-delta JSON objects, maintain and emit merged state. Only wire this into WP2's
message dispatch once both are ready. If the team executing this is small, just fold it into WP2 —
call this out as a *possible* split only if running genuinely parallel agents on this task.

- Unit tests: seed from a snapshot, apply a delta missing several fields, assert the merged state
  keeps prior values for absent fields and updates only the present ones. Test the "when does an
  emit happen" boundary — e.g. does every delta emit a new `MarkFunding`/`OpenInterest` row (even
  if only, say, `volume24h` changed and none of the fields fathom cares about did), or only when a
  field fathom persists actually changed? Prefer the latter (avoid emitting redundant/duplicate
  deriv rows for irrelevant ticker noise) — decide and document, this is a real design choice not
  fully dictated by the spec.

### WP4 — Config + `main.rs` wiring

**Depends on**: WP1 + WP2 must exist (even as stubs is enough to start, but functionally depends on
them being real for end-to-end testing). **Small, mechanical** — good candidate for whoever
finishes WP1/WP2 first to pick up immediately after, not worth a dedicated parallel agent on its
own.

- `src/main.rs`: add a `"bybit_spot" | "bybit_perp"` arm to the `match conn.exchange` block,
  spawning `connection_task_bybit` (see WP2) with the right adapter + category.
- `config.toml`: add `[[connections]]` blocks for `bybit_spot`/`bybit_perp`, same 6 symbols
  (BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, DOGEUSDT, BNBUSDT) — **confirm these exact strings against
  Bybit's live instrument list first** (spec flags this as unverified), don't copy Binance's list
  blindly if Bybit uses different symbol naming for any of the six.
- `infra/configs/fathom/config.toml` (the deployed prod config, different repo) — same addition,
  separate PR (matches existing convention: fathom repo owns the code, infra repo owns the
  deployed config; see how dYdX's collection-disabled-but-code-present split already works today
  as the precedent for keeping infra config and fathom code changes in separate PRs/repos).

### WP5 — Tests

**Depends on**: WP1-WP4 for anything beyond pure unit tests; ticker-merge unit tests (WP3) and gap-
detection unit tests can be written test-first (red, against not-yet-existing code) if following
strict TDD ordering.

- Unit: adapter URL/topic construction, gap-detection pure logic (`u`-sequence comparison
  function, extracted testable the same way `bucket_open`/`should_rotate` are pure functions
  elsewhere in this codebase — don't bury the gap check inside the connection task where it can
  only be tested via a full mock WS server), ticker-merge (WP3).
- e2e: repo already has an axum mock-server e2e harness (`tests/e2e_test.rs`, "7 scenarios" per
  `CLAUDE.md`) — add Bybit scenarios following that existing pattern: mock WS server emits a
  snapshot, some deltas, a deliberate gap, trades batch, ticker snapshot+delta, liquidation batch;
  assert the resulting Parquet/NATS output matches expectations. This is the main integration-
  level confidence check before touching a live Bybit endpoint at all.
- Smoke test (manual, `#[ignore]`, live network) — mirror `tests/smoke_hl_test.rs`/
  `smoke_dydx_test.rs`: connect to real Bybit WS for a short window, assert basic liveness (at
  least one snapshot + one delta + no panics). This is also the moment to actually verify the
  `u`-increment-per-message assumption flagged as unconfirmed in the spec — capture and eyeball a
  short real session before finalizing the gap-detection unit tests' assumptions.

### WP6 — Docs (small, do last or alongside WP4)

- `specs/collection.md`: add Bybit rows to the top-level connections table and capture matrix
  (currently only has Binance/HL/dYdX rows) — cross-reference `bybit-collection.md` rather than
  duplicating its content, same way `collection.md` cross-references `trades-feed.md`/
  `derivatives-feeds.md` for those venues instead of inlining them.
- `fathom/CLAUDE.md`: extend the architecture ASCII diagram and symbol-count line ("22 symbols
  total" → new total once Bybit's 12 — 6 spot + 6 perp — are added, unless the final symbol count
  differs after WP4's verification step).
- `infra/docs/uploader.md`, `rigpa/docs/roadmap.md`: no changes needed — both already describe
  feeds/writers generically by exchange-agnostic path patterns, nothing Bybit-specific to add
  there beyond flipping the roadmap's Phase E status once this ships.

## Suggested execution order for parallel agents

If running several agents concurrently: **WP1 and WP3 can start immediately and truly in
parallel** (adapter shells + ticker-merge logic, zero shared files, both fully spec'd already).
**WP2 depends on WP1** landing (even a minimal stub compiles against) — start it once WP1's trait
impls exist, wiring in WP3's merge logic once both are ready. **WP4 is fast, sequential, do it
right after WP2 is functionally complete.** **WP5's unit tests can be written test-first alongside
WP1-WP3** (red before the implementation, green after); **WP5's e2e/smoke tests need WP2+WP4
complete** to have anything real to exercise. **WP6 last**, or folded into whichever package
finishes last since it's small.

If running solo (one agent, sequential): WP1 → WP2 (+ WP3 inline) → WP4 → WP5 → WP6, same order,
just without the "start simultaneously" framing.

## Verification before calling this done

- `cargo test --workspace` green, including new Bybit unit + e2e tests.
- `make check` (fmt + clippy -D warnings + test).
- Smoke test run once manually against live Bybit (confirms the spec's unverified assumptions
  about symbol naming and `u`-sequence behavior — update `specs/bybit-collection.md` with findings
  if anything differs from what's documented there today).
- Manual sanity check on a running instance: `docker logs fathom | grep bybit` shows both
  connections synced, periodic stats show non-zero `events_per_sec` for `bybit_spot`/`bybit_perp`,
  Parquet files appear under `raw/bybit_spot/...`, `deriv/bybit_perp/...` etc. with the expected
  hourly-rotation naming (this part is already correct for free, once the writers receive events
  tagged with the new exchange strings — no writer-side changes needed, per the "ground truth"
  section above).
- Volume sanity check: rough estimate from `docs/roadmap.md`'s prior note ("+~1.5-2 GB/day") —
  confirm actual observed volume is in that ballpark once live, flag to Ar if it's wildly off
  (would suggest a config or depth-level mistake, e.g. accidentally subscribing to a shallower/
  more frequent depth level than intended).
