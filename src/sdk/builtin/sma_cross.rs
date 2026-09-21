//! The SMA crossover example: the strategy the CLI runs today, and the
//! reference for what an end-to-end [`Strategy`] implementation looks like.
//! It imports everything it needs through the crate root, exactly as a
//! third-party strategy crate would, so this file doubles as the shortest
//! answer to "what do I have to write?".

use crate::{Candle, Indicator, Sma, Strategy, StrategyContext, StrategyError, TargetPosition};

/// The SMA crossover example: long while the fast average is above the slow
/// one, flat while it is below.
///
/// It compares moving-average *levels*, not crossings: while the fast
/// average stays above the slow one it re-requests
/// [`TargetPosition::LongAllIn`] on every bar, and an engine treats a
/// re-asserted target as a no-op. Equal averages emit `None`, so a holder
/// keeps its position and a flat run stays flat; no signal is possible at
/// all until the slow window is warm.
#[derive(Debug)]
pub struct SmaCrossStrategy {
    fast: Sma,
    slow: Sma,
}

impl SmaCrossStrategy {
    /// Creates the strategy from its two window lengths.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] when either window is zero, or when `fast`
    /// is not smaller than `slow`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExchangeId, Interval, MarketId, Symbol, TimestampMs};
    use rust_decimal::Decimal;
    use std::str::FromStr;

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
        let mut strategy = SmaCrossStrategy::new(1, 2).expect("strategy");
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

    #[test]
    fn sma_cross_rejects_windows_that_cannot_cross() {
        assert_eq!(
            SmaCrossStrategy::new(0, 5)
                .expect_err("zero fast")
                .to_string(),
            "fast and slow windows must be greater than zero"
        );
        assert_eq!(
            SmaCrossStrategy::new(5, 0)
                .expect_err("zero slow")
                .to_string(),
            "fast and slow windows must be greater than zero"
        );
        assert_eq!(
            SmaCrossStrategy::new(5, 5)
                .expect_err("equal windows")
                .to_string(),
            "fast window must be smaller than slow window"
        );
        assert_eq!(
            SmaCrossStrategy::new(5, 2)
                .expect_err("inverted windows")
                .to_string(),
            "fast window must be smaller than slow window"
        );
    }
}
