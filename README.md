# QuantForge

QuantForge is a CLI-first, deterministic trading research toolkit written in Rust.
It focuses on market data ingestion, strict normalization, reproducible backtesting,
and a strategy SDK for building TradingView/PineScript-like workflows in pure Rust.

This initial release is intentionally small and opinionated:

- Binance Spot OHLCV download over public REST endpoints
- SQLite candle storage with idempotent upserts
- validation for gaps, duplicates, ordering, and OHLC sanity
- deterministic event-driven backtesting
- built-in SMA crossover example strategy

There is **no UI**. The CLI is the product.

## Why a single crate?

This repository is intentionally structured as a single Cargo package for the first
public release so versioning, publishing, installation, and contributor onboarding
stay simple.

Internal boundaries still exist as Rust modules:

- `model` - normalized market types and validation
- `exchange` - exchange client trait and Binance implementation
- `storage` - candle store trait and SQLite implementation
- `sdk` - strategy trait, context, indicators, example strategies
- `backtest` - deterministic bar-by-bar engine

## Determinism contract

Backtests in QuantForge follow these rules:

- timestamps are stored as UTC epoch milliseconds
- prices and volumes use decimal arithmetic, not floating-point math
- strategies observe a bar at close and submit intent
- the engine executes that intent at the **next bar open**
- data validation is explicit, not best-effort

## Install

If the `quantforge` package name is available on crates.io:

```bash
cargo install quantforge --locked
```

If you are working from source:

```bash
cargo install --path . --locked
```

## Quickstart

Download candles into SQLite:

```bash
quantforge --db data/market.sqlite download \
  --symbol BTCUSDT \
  --interval 1m \
  --start 2024-01-01T00:00:00Z \
  --end 2024-01-02T00:00:00Z
```

Validate the stored series:

```bash
quantforge --db data/market.sqlite validate \
  --symbol BTCUSDT \
  --interval 1m
```

Run the built-in SMA crossover backtest:

```bash
quantforge --db data/market.sqlite backtest \
  --symbol BTCUSDT \
  --interval 1m \
  --fast 20 \
  --slow 50 \
  --cash 10000 \
  --fee-bps 10
```

## Local development

Run the full quality gate before opening a PR:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
./scripts/release-check.sh
```

## Docker

Build the CLI image:

```bash
docker build -t quantforge:0.1.0 .
```

Run the CLI inside Docker:

```bash
docker run --rm -v "$PWD/data:/data" quantforge:0.1.0 \
  --db /data/market.sqlite \
  validate --symbol BTCUSDT --interval 1m
```

## Roadmap

- incremental download and resume support
- additional exchanges such as Bybit
- more indicators and a richer strategy SDK
- alternative storage backends such as Parquet or Postgres
- research-oriented parameter sweeps and snapshot reports

## License

Licensed under MIT license.
