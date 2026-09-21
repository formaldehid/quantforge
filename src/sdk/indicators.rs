//! Indicator math: small state machines that fold a stream of observations
//! into a running value. An indicator is a pure function of the values it
//! has been fed and the order it saw them in — no clocks, randomness, or
//! I/O — the same determinism rule the [`Strategy`](super::Strategy)
//! contract states, and what lets a live run reproduce a backtest's warmup
//! by replaying the same bars. Every indicator warms up:
//! [`Indicator::update`] returns `None` until it has seen enough input.

use super::StrategyError;
use rust_decimal::Decimal;
use std::collections::VecDeque;

/// A streaming indicator: observations arrive one at a time and the
/// indicator yields a value only once it has seen enough of them.
///
/// Implementations must be pure functions of the observations they have
/// been given and the order they arrived in — no clocks, randomness, or
/// I/O — so a strategy replayed over the same bars warms up identically.
pub trait Indicator {
    type Input;
    type Output;

    /// Discards every observation, returning the indicator to the state it
    /// had immediately after construction.
    ///
    /// Nothing in this crate calls it today; it is here so a strategy can
    /// re-warm an indicator on a different series without rebuilding it.
    fn reset(&mut self);

    /// Folds one observation into the accumulated state, returning the
    /// current output once warmed up and `None` while still warming.
    fn update(&mut self, input: Self::Input) -> Option<Self::Output>;
}

/// Rolling simple moving average over the last `window` observations.
///
/// The mean is maintained incrementally — each update adds the new value to
/// a running sum and subtracts the one leaving the window — so the cost per
/// bar is constant and [`Indicator::reset`] must clear both the window and
/// that sum. Nothing is emitted until `window` values have been observed,
/// and the division is decimal arithmetic, never floating point.
#[derive(Clone, Debug)]
pub struct Sma {
    window: usize,
    sum: Decimal,
    values: VecDeque<Decimal>,
}

impl Sma {
    /// Creates a moving average over the last `window` observations.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] when `window` is zero.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("decimal")
    }

    #[test]
    fn sma_computes_expected_value() {
        let mut sma = Sma::new(3).expect("sma");
        assert_eq!(sma.update(dec("1")), None);
        assert_eq!(sma.update(dec("2")), None);
        assert_eq!(sma.update(dec("3")), Some(dec("2")));
    }

    #[test]
    fn sma_rejects_a_zero_window() {
        let error = Sma::new(0).expect_err("zero window must be rejected");
        assert_eq!(error.to_string(), "SMA window must be greater than zero");
    }

    #[test]
    fn sma_with_a_window_of_one_emits_every_input() {
        let mut sma = Sma::new(1).expect("sma");
        assert_eq!(sma.update(dec("7")), Some(dec("7")));
        assert_eq!(sma.update(dec("9")), Some(dec("9")));
    }

    #[test]
    fn sma_drops_the_oldest_value_once_the_window_is_full() {
        let mut sma = Sma::new(2).expect("sma");
        assert_eq!(sma.update(dec("1")), None);
        assert_eq!(sma.update(dec("2")), Some(dec("1.5")));
        assert_eq!(sma.update(dec("3")), Some(dec("2.5")));
        // The 1 is gone from both the window and the running sum.
        assert_eq!(sma.update(dec("4")), Some(dec("3.5")));
    }

    #[test]
    fn sma_averages_stay_exact_where_floats_would_drift() {
        let mut sma = Sma::new(3).expect("sma");
        assert_eq!(sma.update(dec("0.1")), None);
        assert_eq!(sma.update(dec("0.2")), None);
        // The same sum in f64 lands on 0.6000000000000001.
        assert_eq!(sma.update(dec("0.3")), Some(dec("0.2")));
    }

    #[test]
    fn sma_divides_with_decimal_precision() {
        let mut sma = Sma::new(3).expect("sma");
        assert_eq!(sma.update(dec("1")), None);
        assert_eq!(sma.update(dec("1")), None);
        // 4 / 3 has no exact decimal form. Bounds rather than a literal, so
        // the test catches integer truncation without pinning
        // rust_decimal's 28-digit rounding. Float error is far too small to
        // escape a 1e-10 bracket; that regression is caught by
        // `sma_averages_stay_exact_where_floats_would_drift`.
        let average = sma.update(dec("2")).expect("warmed up");
        assert!(
            average > dec("1.3333333333") && average < dec("1.3333333334"),
            "got {average}"
        );
    }

    #[test]
    fn sma_reset_clears_the_window_and_the_running_sum() {
        let mut sma = Sma::new(2).expect("sma");
        assert_eq!(sma.update(dec("10")), None);
        assert_eq!(sma.update(dec("20")), Some(dec("15")));

        sma.reset();

        // Warmup restarts, and the discarded values do not leak into the
        // running sum: a stale sum would make the next average 20, not 5.
        assert_eq!(sma.update(dec("4")), None);
        assert_eq!(sma.update(dec("6")), Some(dec("5")));
    }

    #[test]
    fn sma_stays_exact_over_a_long_sequence() {
        let mut sma = Sma::new(10).expect("sma");
        let mut last = None;
        for value in 1..=1000i64 {
            last = sma.update(Decimal::from(value));
        }
        // The mean of 991..=1000, so the window never grew and the running
        // sum never drifted over a thousand updates.
        assert_eq!(last, Some(dec("995.5")));
    }
}
