#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]
#![doc = include_str!("../README.md")]

pub mod backtest;
pub mod data_sync;
pub mod engine;
pub mod exchange;
pub mod live;
pub mod model;
pub mod ports;
pub mod sdk;
pub mod storage;

pub use backtest::{BacktestConfig, BacktestEngine, BacktestResult};
pub use data_sync::{DataSyncConfig, DataSyncEngine, DataSyncSummary};
pub use engine::EngineError;
pub use exchange::{BinanceCredentials, BinanceSpotClient};
pub use live::{LiveTradeConfig, LiveTradeEngine, LiveTradeSummary};
pub use model::{
    AccountTrade, AssetBalance, BotRunState, Candle, ClosedTrade, ExchangeId, ExchangeOrder,
    ExecutionMode, Fill, Interval, MarketId, ModelError, OrderStatus, PositionState, RunStatus,
    Side, Symbol, SymbolRules, TargetPosition, TimestampMs, ValidationIssue, ValidationReport,
    ms_to_rfc3339, now_utc_ms, parse_rfc3339_to_ms, round_down_to_step, validate_candles,
};
pub use ports::{
    CancelOrderRequest, CandleQuery, CandleStore, ExchangeError, KlineRequest, MarketDataSource,
    MarketOrderRequest, OrderQueryRequest, RunJournalStore, StorageError, TradingVenue,
};
pub use sdk::{
    BuiltInStrategyConfig, Indicator, Sma, Strategy, StrategyContext, StrategyError,
    strategies::SmaCrossStrategy,
};
pub use storage::SqliteStore;
