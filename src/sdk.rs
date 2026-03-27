use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

use crate::model::{Candle, MarketId, TargetPosition, TimestampMs};

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

pub trait StrategyContext {
    fn market(&self) -> &MarketId;
    fn now_ms(&self) -> TimestampMs;
    fn cash(&self) -> Decimal;
    fn position_qty(&self) -> Decimal;
    fn set_target_position(&mut self, target: TargetPosition);
}

pub trait Strategy: Send {
    fn name(&self) -> &'static str;

    fn on_start(&mut self, _ctx: &mut dyn StrategyContext) -> Result<(), StrategyError> {
        Ok(())
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Candle) -> Result<(), StrategyError>;

    fn on_finish(&mut self, _ctx: &mut dyn StrategyContext) -> Result<(), StrategyError> {
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
        fn name(&self) -> &'static str {
            "sma_cross"
        }

        fn on_bar(
            &mut self,
            ctx: &mut dyn StrategyContext,
            bar: &Candle,
        ) -> Result<(), StrategyError> {
            let fast_now = self.fast.update(bar.close);
            let slow_now = self.slow.update(bar.close);

            if let (Some(fast_now), Some(slow_now)) = (fast_now, slow_now) {
                if fast_now > slow_now {
                    ctx.set_target_position(TargetPosition::LongAllIn);
                } else if fast_now < slow_now {
                    ctx.set_target_position(TargetPosition::Flat);
                }

                self.prev_fast = Some(fast_now);
                self.prev_slow = Some(slow_now);
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
