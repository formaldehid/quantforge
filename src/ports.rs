use async_trait::async_trait;
use rust_decimal::Decimal;
use std::error::Error as StdError;
use thiserror::Error;

use crate::model::{
    AccountTrade, AssetBalance, BotRunState, Candle, ClosedTrade, ExchangeId, ExchangeOrder,
    Interval, MarketId, ModelError, Side, Symbol, SymbolRules, TimestampMs,
};

#[derive(Clone, Debug, Default)]
pub struct CandleQuery {
    pub start_time_ms: Option<TimestampMs>,
    pub end_time_ms: Option<TimestampMs>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct KlineRequest {
    pub symbol: Symbol,
    pub interval: Interval,
    pub start_time_ms: Option<TimestampMs>,
    pub end_time_ms: Option<TimestampMs>,
    pub limit: Option<u16>,
}

#[derive(Clone, Debug)]
pub struct MarketOrderRequest {
    pub symbol: Symbol,
    pub side: Side,
    pub quantity: Option<Decimal>,
    pub quote_order_qty: Option<Decimal>,
    pub new_client_order_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CancelOrderRequest {
    pub symbol: Symbol,
    pub order_id: Option<i64>,
    pub client_order_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OrderQueryRequest {
    pub symbol: Symbol,
    pub order_id: Option<i64>,
    pub client_order_id: Option<String>,
}

#[derive(Error, Debug)]
pub enum ExchangeError {
    #[error("transport error")]
    Transport(#[source] Box<dyn StdError + Send + Sync>),

    #[error("api error: code={code:?} message={message}")]
    Api { code: Option<i64>, message: String },

    #[error("missing credentials")]
    MissingCredentials,

    #[error("invalid response: {message}")]
    InvalidResponse { message: String },

    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
}

impl ExchangeError {
    pub fn transport<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Transport(Box::new(err))
    }
}

impl From<ModelError> for ExchangeError {
    fn from(err: ModelError) -> Self {
        Self::InvalidResponse {
            message: err.to_string(),
        }
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("storage error")]
    Other(#[source] Box<dyn StdError + Send + Sync>),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

impl StorageError {
    pub fn other<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Other(Box::new(err))
    }
}

#[async_trait]
pub trait MarketDataSource: Send + Sync {
    fn exchange_id(&self) -> ExchangeId;
    async fn fetch_klines(&self, request: &KlineRequest) -> Result<Vec<Candle>, ExchangeError>;
    async fn fetch_symbol_rules(&self, symbol: &Symbol) -> Result<SymbolRules, ExchangeError>;
}

#[async_trait]
pub trait TradingVenue: Send + Sync {
    fn exchange_id(&self) -> ExchangeId;
    async fn account_balances(&self) -> Result<Vec<AssetBalance>, ExchangeError>;
    async fn open_orders(
        &self,
        symbol: Option<&Symbol>,
    ) -> Result<Vec<ExchangeOrder>, ExchangeError>;
    async fn recent_trades(
        &self,
        symbol: &Symbol,
        limit: usize,
    ) -> Result<Vec<AccountTrade>, ExchangeError>;
    async fn submit_market_order(
        &self,
        request: &MarketOrderRequest,
    ) -> Result<ExchangeOrder, ExchangeError>;
    async fn cancel_order(
        &self,
        request: &CancelOrderRequest,
    ) -> Result<ExchangeOrder, ExchangeError>;
    async fn query_order(
        &self,
        request: &OrderQueryRequest,
    ) -> Result<ExchangeOrder, ExchangeError>;
}

pub trait CandleStore: Send + Sync {
    fn init(&self) -> Result<(), StorageError>;
    fn upsert_candles(&self, market: &MarketId, candles: &[Candle]) -> Result<usize, StorageError>;
    fn load_candles(
        &self,
        market: &MarketId,
        query: CandleQuery,
    ) -> Result<Vec<Candle>, StorageError>;
    fn load_recent_candles(
        &self,
        market: &MarketId,
        limit: usize,
    ) -> Result<Vec<Candle>, StorageError>;
    fn max_open_time_ms(&self, market: &MarketId) -> Result<Option<TimestampMs>, StorageError>;
}

pub trait RunJournalStore: Send + Sync {
    fn init(&self) -> Result<(), StorageError>;
    fn save_run_state(&self, state: &BotRunState) -> Result<(), StorageError>;
    fn load_run_state(&self, run_id: &str) -> Result<Option<BotRunState>, StorageError>;
    fn latest_run_for_market(
        &self,
        market: &MarketId,
        strategy_name: &str,
    ) -> Result<Option<BotRunState>, StorageError>;
    fn append_order_event(&self, run_id: &str, order: &ExchangeOrder) -> Result<(), StorageError>;
    fn append_closed_trade(&self, run_id: &str, trade: &ClosedTrade) -> Result<(), StorageError>;
    fn list_order_events(
        &self,
        run_id: &str,
        limit: usize,
    ) -> Result<Vec<ExchangeOrder>, StorageError>;
    fn list_closed_trades(
        &self,
        run_id: &str,
        limit: usize,
    ) -> Result<Vec<ClosedTrade>, StorageError>;
}
