#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]
#![doc = include_str!("../README.md")]

pub mod engine;
pub mod exchange;
pub mod model;
pub mod ports;
pub mod sdk;
pub mod storage;

pub use engine::EngineError;
pub use engine::backtest::{BacktestConfig, BacktestEngine, BacktestResult};
pub use engine::data_sync::{DataSyncConfig, DataSyncEngine, DataSyncSummary};
pub use engine::live::{LiveTradeConfig, LiveTradeEngine, LiveTradeSummary};
pub use exchange::{BinanceCredentials, BinanceSpotClient};
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
    BuiltInStrategyConfig, Indicator, Sma, SmaCrossStrategy, Strategy, StrategyContext,
    StrategyError,
};
pub use storage::SqliteStore;
