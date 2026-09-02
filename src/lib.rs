#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
//! Deterministic market-data ingestion, backtesting, and controlled live
//! trading for spot crypto markets.
//!
//! The `quantforge` binary in this package is a CLI over the types below and
//! reaches them through this crate root, exactly as an external caller does.
//!
//! # Module tree
//!
//! Six modules, innermost first: [`model`] names the domain, [`ports`] names
//! the seams over it, [`exchange`] and [`storage`] are the adapters behind
//! those seams, [`sdk`] is the boundary a strategy is written against, and
//! [`engine`] drives all of them.
//!
//! - [`model`] — market identity, the candle grid and its validation,
//!   exchange orders, bot-run state, and the timestamp and decimal helpers.
//! - [`ports`] — [`MarketDataSource`] and [`TradingVenue`] are what a venue
//!   adapter provides, [`CandleStore`] and [`RunJournalStore`] what a
//!   persistence backend provides, plus their request and error types.
//! - [`exchange`] — venue adapters, one namespace per venue;
//!   [`exchange::binance`] is the only one today.
//! - [`storage`] — persistence adapters, one namespace per backend;
//!   [`storage::sqlite`] is the only one today.
//! - [`sdk`] — the [`Strategy`] contract, the indicator math it is built
//!   from, and the built-in SMA crossover the CLI runs.
//! - [`engine`] — the drivers: [`engine::backtest`], [`engine::data_sync`]
//!   and [`engine::live`].
//!
//! # The command tree it answers to
//!
//! The mirror is real but partial. Four subcommands are a shell over one
//! library entry point each:
//!
//! | Command | Entry point |
//! | --- | --- |
//! | `quantforge data sync` | [`DataSyncEngine`] |
//! | `quantforge data validate` | [`validate_candles`] |
//! | `quantforge backtest` | [`BacktestEngine`] |
//! | `quantforge trade run` | [`LiveTradeEngine`] |
//!
//! `trade close` and the six `monitor` subcommands have no engine behind
//! them: they drive [`MarketDataSource`], [`TradingVenue`] and
//! [`RunJournalStore`] directly, so that orchestration lives in the binary.
//! The global `--db` opens a [`SqliteStore`] and `--binance-base-url` builds
//! a [`BinanceSpotClient`]; `--log-level` has no counterpart here, because
//! this crate emits `tracing` events and the binary installs the subscriber.
//!
//! [`model`], [`ports`] and [`sdk`] answer to no single command — every
//! command crosses them.
//!
//! # What is public API
//!
//! This root is the front door: every type an external caller needs is
//! re-exported below, grouped by the module that defines it. The module
//! paths are supported too, and are how the tree above is read —
//! `quantforge::Candle` and `quantforge::model::Candle` are one type.
//!
//! A name is public here for exactly one of three reasons:
//!
//! - the binary or the integration tests name it, and both consume this
//!   crate externally, so `pub(crate)` is not available for it;
//! - it is reachable through a public signature or field of something in
//!   the first group — [`Fill`] through [`ExchangeOrder`], [`ModelError`]
//!   through [`Symbol::new`], [`ValidationReport`] through
//!   [`validate_candles`];
//! - it is the strategy boundary, published for authors outside this
//!   repository: [`Strategy`], [`StrategyContext`], [`TargetPosition`],
//!   [`StrategyError`], [`Indicator`], [`Sma`] and [`SmaCrossStrategy`] are
//!   driven by the engines but named by neither the CLI nor the tests, and
//!   are kept reachable on purpose.
//!
//! Everything else is internal and moves without notice: the Binance wire
//! DTOs, request signing and HTTP transport; the SQLite schema and row
//! decoders; and the live engine's poll loop, order execution and run-state
//! handling.
//!
#![doc = include_str!("../README.md")]

// Declared alphabetically: rustfmt re-sorts a contiguous run of module
// declarations, and rustdoc sorts the module list it renders. The layer
// order is the one the crate docs above state and the re-exports below use.
pub mod engine;
pub mod exchange;
pub mod model;
pub mod ports;
pub mod sdk;
pub mod storage;

/// Domain vocabulary. Every layer speaks it, and every command parses its
/// arguments into these types before anything else runs. `TargetPosition` is
/// the exception to the grouping: it is defined here but belongs to the
/// `sdk` contract, as the value `Strategy::on_bar` returns.
pub use model::{
    AccountTrade, AssetBalance, BotRunState, Candle, ClosedTrade, ExchangeId, ExchangeOrder,
    ExecutionMode, Fill, Interval, MarketId, ModelError, OrderStatus, PositionState, RunStatus,
    Side, Symbol, SymbolRules, TargetPosition, TimestampMs, ValidationIssue, ValidationReport,
    ms_to_rfc3339, now_utc_ms, parse_rfc3339_to_ms, round_down_to_step, validate_candles,
};

/// The seams. An out-of-tree venue or storage backend implements these four
/// traits; the engines are written against them and never against an
/// adapter. `ExchangeError` and `StorageError` are the two failure channels
/// they report through.
pub use ports::{
    CancelOrderRequest, CandleQuery, CandleStore, ExchangeError, KlineRequest, MarketDataSource,
    MarketOrderRequest, OrderQueryRequest, RunJournalStore, StorageError, TradingVenue,
};

/// The venue adapter, named because a caller has to construct one. The wire
/// DTOs, request signing and HTTP transport behind it stay private.
pub use exchange::{BinanceCredentials, BinanceSpotClient};

/// The persistence adapter, named for the same reason. The schema and the
/// row decoders behind it stay private.
pub use storage::SqliteStore;

/// The strategy boundary, published for strategy authors outside this
/// repository. The CLI only ever sees the `Box<dyn Strategy>` that
/// `BuiltInStrategyConfig::build` hands back, so most of this group has no
/// in-repo caller by design.
pub use sdk::{
    BuiltInStrategyConfig, Indicator, Sma, SmaCrossStrategy, Strategy, StrategyContext,
    StrategyError,
};

/// The drivers the CLI subcommands wrap: a config in, a summary out, with
/// `EngineError` folding the strategy, exchange and storage failures into
/// one channel.
pub use engine::EngineError;
pub use engine::backtest::{BacktestConfig, BacktestEngine, BacktestResult};
pub use engine::data_sync::{DataSyncConfig, DataSyncEngine, DataSyncSummary};
pub use engine::live::{LiveTradeConfig, LiveTradeEngine, LiveTradeSummary};
