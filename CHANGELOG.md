# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-03-21

### Changed

- kept the repository as a single Cargo package with internal modules
- replaced the `download` command with the `data` command group
- expanded SQLite from candle storage into candle plus bot journal storage

### Added

- incremental and follow-mode candle synchronization
- live-trading runtime with dry-run and live execution modes
- manual trade close command
- monitor command group for balances, open orders, recent trades, cancel, and manual close
- signed Binance Spot client support for account and order endpoints

## [0.1.0] - 2026-03-18

### Added

- single-package QuantForge CLI project layout
- Binance Spot OHLCV ingestion
- SQLite candle storage
- candle validation tooling
- deterministic event-driven backtest engine
- strategy SDK with SMA crossover example
- CI, packaging checks, and contributor documentation
