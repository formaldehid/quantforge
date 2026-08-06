use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

use crate::model::{Candle, MarketId, TargetPosition, TimestampMs};

/// Error reported by a strategy callback.
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

/// Plain-data snapshot of engine state passed to every [`Strategy`]
/// callback.
///
/// Every field is an owned value: nothing borrows into engine internals,
/// so a snapshot can be copied across an FFI boundary without lifetime
/// coupling to the engine that produced it.
///
/// Field provenance differs by engine and is part of the contract:
///
/// - `now_ms` is the current bar's open time in backtests and its close
///   time in live and dry runs (wall clock during `on_start` there)
/// - `cash` is the simulated quote balance in backtests and always zero in
///   live and dry runs, where order sizing comes from the configured
///   `quote_order_qty` instead
/// - `position_qty` is the engine's current base-asset position
#[derive(Clone, Debug, PartialEq)]
pub struct StrategyContext {
    pub market: MarketId,
    pub now_ms: TimestampMs,
    pub cash: Decimal,
    pub position_qty: Decimal,
}

/// A trading strategy driven bar by bar by an engine.
///
/// # Boundary contract
///
/// The trait is deliberately FFI-friendly: object-safe, no generics, no
/// lifetimes beyond transient borrows of owned data, input as a plain-data
/// snapshot ([`StrategyContext`]), and both decisions
/// (`Option<TargetPosition>`) and errors ([`StrategyError`], a message
/// string) as plain data. A foreign implementation only consumes values
/// and returns values; it never calls back into the engine.
///
/// ## Call order
///
/// 1. `on_start` — exactly once, before any bar
/// 2. `on_bar` — once per closed candle, in ascending open-time order;
///    engines never deliver a partial bar or the same bar twice
/// 3. `on_finish` — exactly once after the final bar, but not when an
///    earlier callback returned an error
///
/// ## Decision semantics
///
/// `on_bar` returns the desired position after this bar: `Some(target)`
/// requests it, `None` leaves the current position untouched. Requesting
/// the already-held target is a no-op. Backtests fill a request at the
/// next bar's open; live and dry runs execute against the signal bar's
/// close.
///
/// ## Error semantics
///
/// Returning `Err` from any callback aborts the run: engines stop
/// delivering bars, mark the run failed, and surface the message to the
/// operator.
///
/// ## Determinism
///
/// Implementations must be pure functions of the observed bar sequence and
/// their own accumulated state: no clocks, randomness, or I/O. The engines
/// rely on this to keep backtests reproducible and to warm strategies up
/// consistently when a live run restarts and replays recent bars.
pub trait Strategy: Send {
    /// Stable identifier recorded in run journals and operator output.
    ///
    /// Borrowed from `self` rather than `'static` so foreign strategies
    /// can report dynamically owned names.
    fn name(&self) -> &str;

    fn on_start(&mut self, _ctx: &StrategyContext) -> Result<(), StrategyError> {
        Ok(())
    }

    fn on_bar(
        &mut self,
        ctx: &StrategyContext,
        bar: &Candle,
    ) -> Result<Option<TargetPosition>, StrategyError>;

    fn on_finish(&mut self, _ctx: &StrategyContext) -> Result<(), StrategyError> {
        Ok(())
    }
}

pub trait Indicator {
    type Input;
    type Output;

    fn reset(&mut self);
    fn update(&mut self, input: Self::Input) -> Option<Self::Output>;
}

#[derive(Clone, Debug)]
pub struct Sma {
    window: usize,
    sum: Decimal,
    values: VecDeque<Decimal>,
}

impl Sma {
    pub fn new(window: usize) -> Result<Self, StrategyError> {
        if window == 0 {
            return Err(StrategyError::msg("SMA window must be greater than zero"));
        }
        Ok(Self {
            window,
            sum: Decimal::ZERO,
            values: VecDeque::with_capacity(window),
        })
    }
}

impl Indicator for Sma {
    type Input = Decimal;
    type Output = Decimal;

    fn reset(&mut self) {
        self.sum = Decimal::ZERO;
        self.values.clear();
    }

    fn update(&mut self, input: Self::Input) -> Option<Self::Output> {
        self.values.push_back(input);
        self.sum += input;

        if self.values.len() > self.window {
            if let Some(removed) = self.values.pop_front() {
                self.sum -= removed;
            }
        }

        if self.values.len() == self.window {
            Some(self.sum / Decimal::from(self.window as i64))
        } else {
            None
        }
    }
}

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
            Self::SmaCross { fast, slow } => {
                Ok(Box::new(strategies::SmaCrossStrategy::new(*fast, *slow)?))
            }
        }
    }
}

pub mod strategies {
    use super::*;

    #[derive(Debug)]
    pub struct SmaCrossStrategy {
        fast: Sma,
        slow: Sma,
        prev_fast: Option<Decimal>,
        prev_slow: Option<Decimal>,
    }

    impl SmaCrossStrategy {
        pub fn new(fast: usize, slow: usize) -> Result<Self, StrategyError> {
            if fast == 0 || slow == 0 {
                return Err(StrategyError::msg(
                    "fast and slow windows must be greater than zero",
                ));
            }
            if fast >= slow {
                return Err(StrategyError::msg(
                    "fast window must be smaller than slow window",
                ));
            }

            Ok(Self {
                fast: Sma::new(fast)?,
                slow: Sma::new(slow)?,
                prev_fast: None,
                prev_slow: None,
            })
        }
    }

    impl Strategy for SmaCrossStrategy {
        fn name(&self) -> &str {
            "sma_cross"
        }

        fn on_bar(
            &mut self,
            _ctx: &StrategyContext,
            bar: &Candle,
        ) -> Result<Option<TargetPosition>, StrategyError> {
            let fast_now = self.fast.update(bar.close);
            let slow_now = self.slow.update(bar.close);

            if let (Some(fast_now), Some(slow_now)) = (fast_now, slow_now) {
                self.prev_fast = Some(fast_now);
                self.prev_slow = Some(slow_now);

                if fast_now > slow_now {
                    return Ok(Some(TargetPosition::LongAllIn));
                }
                if fast_now < slow_now {
                    return Ok(Some(TargetPosition::Flat));
                }
            }

            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExchangeId, Interval, Symbol};
    use std::str::FromStr;

    #[test]
    fn sma_computes_expected_value() {
        let mut sma = Sma::new(3).expect("sma");
        assert_eq!(sma.update(Decimal::from_str("1").expect("decimal")), None);
        assert_eq!(sma.update(Decimal::from_str("2").expect("decimal")), None);
        assert_eq!(
            sma.update(Decimal::from_str("3").expect("decimal")),
            Some(Decimal::from_str("2").expect("decimal"))
        );
    }

    fn context() -> StrategyContext {
        StrategyContext {
            market: MarketId::new(
                ExchangeId::BinanceSpot,
                Symbol::new("BTCUSDT").expect("symbol"),
                Interval::M1,
            ),
            now_ms: 0,
            cash: Decimal::ZERO,
            position_qty: Decimal::ZERO,
        }
    }

    fn candle(open_time_ms: TimestampMs, close: &str) -> Candle {
        let close = Decimal::from_str(close).expect("decimal");
        Candle {
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            open: close,
            high: close,
            low: close,
            close,
            volume: Decimal::ONE,
            trades: Some(1),
        }
    }

    // The cross comparisons are strictly greater/less on purpose: equal
    // averages must emit nothing, so a holder keeps its position and a flat
    // run stays flat. The live-engine execution tests lean on this.
    #[test]
    fn sma_cross_emits_no_signal_when_fast_equals_slow() {
        let mut strategy = strategies::SmaCrossStrategy::new(1, 2).expect("strategy");
        let ctx = context();

        // Slow window still warming: no signal possible.
        assert_eq!(strategy.on_bar(&ctx, &candle(0, "100")).expect("bar"), None);

        // Fast == slow == 100: strictly-greater/less comparisons stay silent.
        assert_eq!(
            strategy.on_bar(&ctx, &candle(60_000, "100")).expect("bar"),
            None
        );

        // A rising close crosses fast above slow and finally signals.
        assert_eq!(
            strategy.on_bar(&ctx, &candle(120_000, "101")).expect("bar"),
            Some(TargetPosition::LongAllIn)
        );
    }
}
