use thiserror::Error;

use crate::{ExchangeError, StorageError, StrategyError};

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("no candles provided")]
    NoCandles,

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error(transparent)]
    Strategy(#[from] StrategyError),

    #[error(transparent)]
    Exchange(#[from] ExchangeError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}
