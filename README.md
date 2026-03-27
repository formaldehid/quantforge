# QuantForge

QuantForge is a CLI-first trading systems framework in Rust for deterministic research,
SQLite-backed market data, and controlled strategy execution.

Version `0.2.0` keeps the repository as a **single Cargo package**, following the
`0.1.0` contributor experience, while adding the first live-trading and monitoring
path for **Binance Spot**.

There is **no UI**. The CLI is the product.

## What changed in 0.2.0

- single-crate repository layout with internal Rust modules
- `data` command group for historical and incremental candle ingestion into SQLite
- `trade run` command for a polling strategy bot that reads closed bars from SQLite,
  evaluates a strategy, and can place or close spot market orders
- `trade close` command for operator-driven position exits
- `monitor` command group for balances, open orders, recent trades, manual cancel,
  and manual close
- bot run state and order or trade journal persisted in SQLite

QuantForge is engineering software. It is **not** investment advice, does not
recommend trades, and does not promise profitability.

## Why keep a single crate?

This repository stays as a single Cargo package in `0.2.0` so versioning,
publishing, installation, and contributor onboarding remain simple.

Internal boundaries still exist as Rust modules:

- `model` - normalized market types and validation
- `ports` - exchange and storage traits plus request types
- `exchange` - Binance Spot REST client
- `storage` - SQLite candle store and bot journal
- `sdk` - strategy trait, indicators, built-in strategies
- `backtest` - deterministic bar-by-bar engine
- `data_sync` - historical and incremental ingestion loop
- `live` - polling live-trade engine

## Scope and guardrails

- exchange in 0.2.0: **Binance Spot**
- storage in 0.2.0: **SQLite**
- strategy execution model: closed-bar, event-driven
- built-in strategy in 0.2.0: SMA crossover example
- default execution mode: **dry-run**
- live order placement requires explicit `--mode live` and Binance credentials from
  environment variables

## Credentials

QuantForge keeps secrets out of the repository. For live and monitor commands, use:

```bash
export QF_BINANCE_API_KEY="..."
export QF_BINANCE_API_SECRET="..."
export QF_BINANCE_BASE_URL="https://testnet.binance.vision/"
```

Use Binance Spot testnet first.

## Quickstart

Sync candles into SQLite:

```bash
cargo run -- \
  --db data/market.sqlite \
  data sync \
  --symbol BTCUSDT \
  --interval 1m
```

Validate the stored series:

```bash
cargo run -- \
  --db data/market.sqlite \
  data validate \
  --symbol BTCUSDT \
  --interval 1m
```

Backtest the SMA crossover example:

```bash
cargo run -- \
  --db data/market.sqlite \
  backtest \
  --symbol BTCUSDT \
  --interval 1m \
  --fast 20 \
  --slow 50 \
  --cash 10000 \
  --fee-bps 10
```

Run the strategy against live-updating database candles without sending real orders:

```bash
cargo run -- \
  --db data/market.sqlite \
  trade run \
  --symbol BTCUSDT \
  --interval 1m \
  --fast 20 \
  --slow 50 \
  --quote-order-qty 100 \
  --mode dry-run \
  --poll-secs 5
```

Run against Binance Spot testnet with live order placement:

```bash
export QF_BINANCE_BASE_URL="https://testnet.binance.vision/"

cargo run -- \
  --db data/market.sqlite \
  trade run \
  --symbol BTCUSDT \
  --interval 1m \
  --fast 20 \
  --slow 50 \
  --quote-order-qty 100 \
  --mode live \
  --bootstrap-enter \
  --poll-secs 5
```

Inspect balances, open orders, and recent trades:

```bash
cargo run -- \
  --db data/market.sqlite \
  monitor status \
  --symbol BTCUSDT
```

Cancel an order manually:

```bash
cargo run -- \
  --db data/market.sqlite \
  monitor cancel-order \
  --symbol BTCUSDT \
  --order-id 123456789 \
  --yes
```

Close the bot-managed position manually:

```bash
cargo run -- \
  --db data/market.sqlite \
  trade close \
  --symbol BTCUSDT \
  --yes
```

## Determinism contract

- timestamps are stored as UTC epoch milliseconds
- prices and volumes use decimal arithmetic, not floating-point math
- strategies evaluate only closed bars
- backtests execute intent on the **next bar open**
- live trading polls closed bars and records run state into SQLite

## Local development

Run the full quality gate before opening a PR:

```bash
cargo generate-lockfile
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
./scripts/release-check.sh
```

## Roadmap

- additional exchanges such as Bybit
- restart reconciliation between local bot state and exchange state
- richer indicators and more built-in strategies
- alternative storage backends such as Postgres
- research-oriented parameter sweeps and snapshot reports

## License

Licensed under MIT.
