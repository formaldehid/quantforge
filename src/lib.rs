#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]
#![doc = include_str!("../README.md")]

pub mod backtest;
pub mod exchange;
pub mod model;
pub mod sdk;
pub mod storage;

pub use backtest::{BacktestConfig, BacktestEngine, BacktestError, BacktestResult, Trade};
pub use exchange::{BinanceSpotClient, ExchangeError, KlineRequest, MarketDataSource};
pub use model::{
    Candle, ExchangeId, Interval, MarketId, ModelError, Symbol, TimestampMs, ValidationIssue,
    ValidationReport, ms_to_rfc3339, parse_rfc3339_to_ms, validate_candles,
};
pub use sdk::{Sma, Strategy, StrategyContext, StrategyError, TargetPosition};
pub use storage::{CandleQuery, CandleStore, SqliteCandleStore, StorageError};
