//! Strategies that ship with the crate, and the [`BuiltInStrategyConfig`]
//! that names and constructs one. [`sma_cross`] is the only entry today.
//!
//! Self-contained on purpose: nothing outside this directory reaches into a
//! built-in strategy's internals, so the whole namespace can be extended or
//! removed as one unit.

mod sma_cross;

use super::{Strategy, StrategyError};
use serde::{Deserialize, Serialize};

pub use sma_cross::SmaCrossStrategy;

/// Serializable selection of a strategy that ships with the crate.
///
/// Its serialized form is a storage contract, not just an API one: the
/// variant name is recorded as a run's `strategy_name` and the whole value
/// as its `strategy_config` (`{"kind":"sma_cross","fast":..,"slow":..}`),
/// which a resume compares against the configuration the run started with
/// (see `crate::engine::live`). Renaming the type, the variant, or the
/// fields, or dropping the `serde` attributes, breaks every stored run —
/// and there are no migrations.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuiltInStrategyConfig {
    SmaCross { fast: usize, slow: usize },
}

impl BuiltInStrategyConfig {
    pub fn strategy_name(&self) -> &'static str {
        match self {
            Self::SmaCross { .. } => "sma_cross",
        }
    }

    pub fn build(&self) -> Result<Box<dyn Strategy>, StrategyError> {
        match self {
            Self::SmaCross { fast, slow } => Ok(Box::new(SmaCrossStrategy::new(*fast, *slow)?)),
        }
    }
}
