//! Strategy SDK: the boundary third-party strategies are written against.
//! The work is split across sibling modules — [`strategy`] owns the
//! [`Strategy`] trait and the plain-data [`StrategyContext`] snapshot it is
//! called with, [`indicators`] holds the reusable indicator math, and
//! [`builtin`] ships the SMA crossover example the CLI runs.
//! [`StrategyError`] stays here: all three report failures through it.

pub mod builtin;
pub mod indicators;
pub mod strategy;

use thiserror::Error;

pub use builtin::{BuiltInStrategyConfig, SmaCrossStrategy};
pub use indicators::{Indicator, Sma};
pub use strategy::{Strategy, StrategyContext};

/// Error reported across the strategy boundary: by a [`Strategy`] callback,
/// and by the indicator and strategy constructors a strategy is built from.
///
/// Deliberately a plain message string: a foreign strategy implementation
/// (for example a Python strategy behind an FFI boundary) can produce it
/// without constructing any Rust-only error type.
#[derive(Error, Debug)]
pub enum StrategyError {
    #[error("{0}")]
    Message(String),
}

impl StrategyError {
    pub fn msg(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}
