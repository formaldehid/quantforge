# Backend suggestions (not yet applied)

Review of `quantforge` at `main` (28 commits, single crate, v0.2.0). Focus: hardening
and cleanup of existing code, not new features. The code is already defensive,
commented, and well tested; the items below are the gaps that remain.

## Bugs

### 1. `new_client_order_id` length bound is only a `debug_assert`

`src/engine/live/execution.rs::new_client_order_id`:

```rust
    let id = format!("qf-{tag}-{prefix}-{ts}-{nonce}");
    debug_assert!(id.len() <= 36);
    id
```

Binance rejects a `newClientOrderId` longer than 36 chars. Today it fits
(`3 + 5 + 1 + 8 + 1 + 8 + 1 + 8 = 35`) only because three separate caps line up:
`sanitize_client_order_id_fragment` truncates the tag to 5 and `run_id` to 8, and
`% 100_000_000` keeps `ts` at 8 digits. The invariant lives nowhere except that
arithmetic, and `debug_assert!` (compiled out in release) is the only thing catching a
later edit that bumps one of those numbers: an over-length id would then ship in
release and the exchange rejects the order mid-run.

**Fix:** name the limit and clamp unconditionally; test with an oversized `run_id`.

```rust
const BINANCE_CLIENT_ORDER_ID_MAX: usize = 36;
// build the id, then:
let id: String = id.chars().take(BINANCE_CLIENT_ORDER_ID_MAX).collect();
```

### 2. Live poll loop aborts on any transient exchange/storage error

`src/engine/live/runner.rs::run_inner`, inside the main `loop { ... }`:

```rust
loop {
    let end_ms = now_utc_ms();
    let start_ms = /* ... */;

    if start_ms <= end_ms {
        sync_market_range(self.market_data, self.candle_store, &cfg.market,
            start_ms, end_ms, cfg.batch_limit).await?;   // one HTTP timeout ends the run
    }
    let new_candles = self.candle_store.load_candles(&cfg.market, CandleQuery { .. })?;
    for candle in filter_closed_candles(new_candles) {
        // on_bar / execute_target / save_run_state
    }

    loops += 1;
    if cfg.max_loops.map(|max| loops >= max).unwrap_or(false) { break; }
    if sleep_or_shutdown(cfg.poll_interval).await { break; }
}
```

A single failed poll (timeout, 5xx, brief network drop, a locked SQLite file)
propagates through `?`, ends `run_inner`, marks the run failed, and the bot stops. A
polling bot is expected to miss a tick and recover on the next one.

**Fix:** move the fallible body into one helper, tolerate failure in the loop, abort
only on a sustained streak.

```rust
loop {
    let end_ms = now_utc_ms();

    match self.poll_once(cfg, rules, run_state, strategy, summary, end_ms).await {
        Ok(()) => consecutive_failures = 0,
        Err(err) => {
            consecutive_failures += 1;
            warn!(%err, consecutive_failures, "poll iteration failed; retrying next tick");
            if consecutive_failures >= cfg.max_consecutive_failures {
                return Err(err);           // real outage still surfaces
            }
        }
    }

    loops += 1;
    if cfg.max_loops.map(|max| loops >= max).unwrap_or(false) { break; }
    if sleep_or_shutdown(cfg.poll_interval).await { break; }
}
```

`poll_once` holds the `sync_market_range` + `load_candles` + per-candle block that is
inlined today. `max_consecutive_failures` gets a CLI default (e.g. 5).

### 3. Dry-run and backtest disagree on fees and lot rounding for the same trade

- `src/engine/backtest.rs::execute_target` applies `fee_bps` and sizes the buy as
  `cash / (price * (1 + fee_rate))`, with **no** exchange lot-size rounding.
- `src/engine/live/execution.rs::synthetic_market_order` rounds to the exchange step
  (`maybe_round_qty`) but models **zero fees** (`fills: Some(Vec::new())`, comment
  notes it is "optimistic").

Same strategy, same candles, two engines, two different fills and two different P&L.
Dry-run reads rosier than the backtest it is supposed to preview.

**Fix:** pick one accounting model and share it. Either pass `SymbolRules` into the
backtest and have it round like live, and apply the same `fee_bps` assumption inside
`synthetic_market_order`; or extract a single `simulate_fill(price, qty_or_quote,
fee_rate, rules) -> Fill` used by both. Document any remaining intentional gap.

### 4. `EngineError::InvalidState(String)` is a catch-all asserted on by substring

Many distinct failures collapse into one variant carrying prose ("exchange did not
report an executed quantity", "journaling the order event failed", "position had no
recorded entry price", "sell quantity exceeds exchange maximum"). Tests then assert
with `error.to_string().contains("...")`, which breaks on any wording change and
cannot distinguish the cases.

Before:

```rust
return Err(EngineError::InvalidState(format!(
    "sell quantity {requested_qty} exceeds exchange maximum {max_qty}"
)));
```

```rust
assert!(error.to_string().contains("non-positive reference price"), "got {error}");
```

**Fix:** typed variants; tests match the variant, messages stay free to change.

```rust
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    // ...
    #[error("sell quantity {qty} exceeds exchange maximum {max}")]
    SellAboveMax { qty: Decimal, max: Decimal },
    #[error("exchange did not report an executed quantity for {order_id:?}")]
    MissingExecutedQty { order_id: Option<u64> },
    #[error("position had no recorded entry price")]
    MissingEntryPrice,
    #[error("order executed but journaling failed: {0}")]
    JournalWriteFailed(String),
}
```

```rust
assert!(matches!(error, EngineError::SellAboveMax { .. }));
```

## General improvements

### 5. Blocking SQLite called directly inside `async fn` on a multi-thread runtime

`CandleStore` / `RunJournalStore` are sync traits (rusqlite is blocking). They are
called with a bare `?` from `async fn run_inner` on the tokio multi-thread runtime,
so each query parks a worker thread for its duration.

Before:

```rust
let new_candles = self.candle_store.load_candles(&cfg.market, query)?;
```

**Fix:** wrap in `tokio::task::block_in_place` (cheapest, stays on the same runtime)
or move storage behind `spawn_blocking`.

```rust
let new_candles = tokio::task::block_in_place(|| {
    self.candle_store.load_candles(&cfg.market, query)
})?;
```

Low volume today, but it is a latent runtime-correctness issue.

### 6. Wall clock read directly throughout the engine

`now_utc_ms()` is called inline in `runner.rs` (bootstrap window math, every
`updated_at_ms`, `filter_closed_candles`), `execution.rs` (`new_client_order_id`
timestamp, position timestamps), and `state.rs`. `sdk.rs` makes determinism a
headline property of the strategy boundary, yet the engine's own time-dependent
branches (bootstrap window, closed-bar cutoff) cannot be tested deterministically.

Before:

```rust
// runner.rs
let now = now_utc_ms();
ctx.now_ms = now_utc_ms();
run_state.updated_at_ms = now_utc_ms();

fn filter_closed_candles(candles: Vec<Candle>) -> Vec<Candle> {
    let now_ms = now_utc_ms();
    candles.into_iter().filter(|c| c.close_time_ms <= now_ms).collect()
}
```

**Fix:** a `Clock` port, injected the same way `MarketDataSource` / `CandleStore` are.

```rust
// ports.rs
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> TimestampMs;
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> TimestampMs { now_utc_ms() }
}
```

```rust
// runner.rs
let now = self.clock.now_ms();
run_state.updated_at_ms = self.clock.now_ms();

fn filter_closed_candles(candles: Vec<Candle>, now_ms: TimestampMs) -> Vec<Candle> {
    candles.into_iter().filter(|c| c.close_time_ms <= now_ms).collect()
}
```

```rust
// tests
struct FixedClock(TimestampMs);
impl Clock for FixedClock {
    fn now_ms(&self) -> TimestampMs { self.0 }
}
```

### 7. `execute_target` is one ~255-line method with two large `match` arms

`src/engine/live/execution.rs::execute_target`. After the dry-run/live branch, the
`LongAllIn` and `Flat` arms each repeat the same shape: pull `executed_qty`, guard
zero, mutate `run_state.position`, set `updated_at_ms` / `status` / `last_error`,
`save_run_state`, resolve `order_event_result`, then (Flat only) build the
`ClosedTrade`.

Before:

```rust
match target {
    TargetPosition::LongAllIn => {
        let executed_qty = order.executed_qty.ok_or_else(|| /* ... */)?;
        // ~55 lines: net-of-fees qty, warnings, position write, save, journal
        Ok(())
    }
    TargetPosition::Flat => {
        let executed_qty = order.executed_qty.ok_or_else(|| /* ... */)?;
        // ~75 lines: closed_qty, dust write-off, position write, save, journal,
        //            entry/exit price checks, ClosedTrade build, append
        Ok(())
    }
}
```

**Fix:** one helper per arm; `execute_target` keeps the dry-run/live branch and the
pre-trade rule gates.

```rust
match target {
    TargetPosition::LongAllIn => self.apply_entry_fill(run_state, &order, reference_bar, order_event_result),
    TargetPosition::Flat      => self.apply_exit_fill(cfg, rules, run_state, &order, reference_bar, summary, order_event_result),
}
```

Each helper is then testable in isolation with a hand-built `ExchangeOrder`.

### 8. Backtest accounting function takes 9 `&mut` args with `#[allow(clippy::too_many_arguments)]`

`src/engine/backtest.rs::execute_target` (the free function).

Before:

```rust
#[allow(clippy::too_many_arguments)]
fn execute_target(
    market: &MarketId,
    target: TargetPosition,
    price: Decimal,
    timestamp_ms: TimestampMs,
    fee_rate: Decimal,
    cash: &mut Decimal,
    qty: &mut Decimal,
    open_trade: &mut Option<OpenTrade>,
    trades: &mut Vec<ClosedTrade>,
) { /* ... */ }
```

**Fix:** bundle the mutable state; the `#[allow]` goes away and it stops sharing a
name with `live::execution::execute_target`.

```rust
struct BacktestPortfolio {
    cash: Decimal,
    qty: Decimal,
    open_trade: Option<OpenTrade>,
    trades: Vec<ClosedTrade>,
}

impl BacktestPortfolio {
    fn enter(&mut self, market: &MarketId, price: Decimal, ts: TimestampMs, fee_rate: Decimal) { /* ... */ }
    fn exit(&mut self,  market: &MarketId, price: Decimal, ts: TimestampMs, fee_rate: Decimal) { /* ... */ }
}
```

### 9. Test fixtures duplicated across modules

`market()`, `run_state()`, `reference_bar()`, `candle()`, `dec()` are redefined in
the `tests` modules of `backtest.rs`, `execution.rs`, `state.rs`, and `sdk.rs`.

**Fix:** one `#[cfg(test)] mod test_support` (or a `tests/common/`) with the shared
builders; each module imports what it needs.

### 10. `BinanceCredentials` derives `Debug`, secret leaks plain if ever logged

`src/exchange/binance/credentials.rs`:

```rust
#[derive(Clone, Debug)]
pub struct BinanceCredentials {
    pub api_key: String,
    pub secret: String,
}
```

`BinanceClient` (`client.rs`) also derives `Debug` and holds credentials. No current
logging site prints it, but derived `Debug` is a live trap: `tracing::debug!(?client,
...)`, an `.expect()`/panic message, or a future error context that includes the
struct dumps the HMAC secret into logs or stdout.

**Fix:** hand-write `Debug` to redact, or wrap `secret` in `secrecy::SecretString`
(crate already pulls in `hmac`/`sha2`, so one more small dep is cheap):

```rust
impl std::fmt::Debug for BinanceCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceCredentials")
            .field("api_key", &self.api_key)
            .field("secret", &"<redacted>")
            .finish()
    }
}
```

`AppContext` (`cli/context.rs`) also derives `Debug` and holds `private_client: Option<BinanceSpotClient>` — the leak path runs the whole way from a `{:?}` on the top-level CLI context down to the raw secret today.

### 11. Every store call opens a brand-new SQLite connection

`src/storage/sqlite/mod.rs::SqliteStore::open` is called fresh at 11 separate call
sites (`candles.rs` x4, `journal.rs` x7) — one per query, not once per `SqliteStore`.
Each call opens a new `Connection`, re-runs `PRAGMA journal_mode=WAL`, `PRAGMA
synchronous=NORMAL`, and `busy_timeout(5s)`, then drops the connection at the end of
the method.

```rust
fn open(&self) -> Result<Connection, StorageError> {
    let connection = Connection::open(&self.path).map_err(StorageError::other)?;
    connection.pragma_update(None, "journal_mode", "WAL")...
    connection.pragma_update(None, "synchronous", "NORMAL")...
    connection.busy_timeout(Duration::from_secs(5))...
    Ok(connection)
}
```

Live trading calls this on every poll iteration (`load_candles`, `save_run_state`,
`append_order_event`, `append_closed_trade`), so a bot polling every few seconds opens
and tears down a SQLite connection, and re-applies three pragmas, on every single tick.
Correctness is unaffected (WAL tolerates this), but it is needless per-call overhead
and file-open churn.

**Fix:** hold one `Connection` behind a `Mutex` (rusqlite `Connection` is `!Sync`) for
the store's lifetime, opened once in `SqliteStore::new`/`init`, and have every method
lock and reuse it instead of calling `self.open()`.
