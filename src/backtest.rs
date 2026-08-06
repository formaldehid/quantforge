use crate::{
    Candle, ClosedTrade, MarketId, Strategy, StrategyContext, TargetPosition, TimestampMs,
};
use rust_decimal::Decimal;
use tracing::info;

use crate::EngineError;

#[derive(Clone, Debug)]
pub struct BacktestConfig {
    pub initial_cash: Decimal,
    pub fee_bps: Decimal,
    pub close_out_at_end: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BacktestResult {
    pub initial_cash: Decimal,
    pub final_equity: Decimal,
    pub total_return_pct: Decimal,
    pub trade_count: usize,
    pub max_drawdown_pct: Decimal,
    pub trades: Vec<ClosedTrade>,
}

#[derive(Clone, Debug)]
pub struct BacktestEngine {
    cfg: BacktestConfig,
}

impl BacktestEngine {
    pub fn new(cfg: BacktestConfig) -> Self {
        Self { cfg }
    }

    pub fn run(
        &self,
        market: &MarketId,
        candles: &[Candle],
        strategy: &mut dyn Strategy,
    ) -> Result<BacktestResult, EngineError> {
        if candles.is_empty() {
            return Err(EngineError::NoCandles);
        }
        if self.cfg.initial_cash <= Decimal::ZERO {
            return Err(EngineError::InvalidConfig(format!(
                "initial_cash must be greater than 0, got {}",
                self.cfg.initial_cash
            )));
        }
        if self.cfg.fee_bps < Decimal::ZERO {
            return Err(EngineError::InvalidConfig(format!(
                "fee_bps must be zero or greater, got {}",
                self.cfg.fee_bps
            )));
        }

        let fee_rate = self.cfg.fee_bps / Decimal::from(10_000);
        let mut cash = self.cfg.initial_cash;
        let mut qty = Decimal::ZERO;
        let mut open_trade: Option<OpenTrade> = None;
        let mut trades = Vec::new();
        let mut pending_target: Option<TargetPosition> = None;
        let mut peak_equity = Decimal::ZERO;
        let mut max_drawdown = Decimal::ZERO;

        let mut ctx = StrategyContext {
            market: market.clone(),
            now_ms: candles[0].open_time_ms,
            cash,
            position_qty: qty,
        };

        strategy.on_start(&ctx)?;

        for (index, candle) in candles.iter().enumerate() {
            ctx.now_ms = candle.open_time_ms;

            if index > 0 {
                if let Some(target) = pending_target.take() {
                    execute_target(
                        market,
                        target,
                        candle.open,
                        candle.open_time_ms,
                        fee_rate,
                        &mut cash,
                        &mut qty,
                        &mut open_trade,
                        &mut trades,
                    );
                }
            }

            ctx.cash = cash;
            ctx.position_qty = qty;

            let equity = cash + qty * candle.close;
            if equity > peak_equity {
                peak_equity = equity;
            }
            if peak_equity > Decimal::ZERO {
                let drawdown = (peak_equity - equity) / peak_equity;
                if drawdown > max_drawdown {
                    max_drawdown = drawdown;
                }
            }

            pending_target = strategy.on_bar(&ctx, candle)?;
        }

        if self.cfg.close_out_at_end && qty > Decimal::ZERO {
            if let Some(last) = candles.last() {
                execute_target(
                    market,
                    TargetPosition::Flat,
                    last.close,
                    last.close_time_ms,
                    fee_rate,
                    &mut cash,
                    &mut qty,
                    &mut open_trade,
                    &mut trades,
                );
            }
        }

        ctx.cash = cash;
        ctx.position_qty = qty;
        strategy.on_finish(&ctx)?;

        let last_close = candles
            .last()
            .map(|c| c.close)
            .ok_or_else(|| EngineError::InvalidState("no candles at close-out".to_string()))?;
        let final_equity = cash + qty * last_close;
        let total_return_pct =
            (final_equity - self.cfg.initial_cash) / self.cfg.initial_cash * Decimal::from(100);

        info!(
            strategy = strategy.name(),
            final_equity = %final_equity,
            total_return_pct = %total_return_pct,
            trades = trades.len(),
            "backtest completed"
        );

        Ok(BacktestResult {
            initial_cash: self.cfg.initial_cash,
            final_equity,
            total_return_pct,
            trade_count: trades.len(),
            max_drawdown_pct: max_drawdown * Decimal::from(100),
            trades,
        })
    }
}

#[derive(Debug)]
struct OpenTrade {
    entry_time_ms: TimestampMs,
    entry_price: Decimal,
    qty: Decimal,
    cash_before: Decimal,
}

#[allow(clippy::too_many_arguments)]
fn execute_target(
    market: &MarketId,
    target: TargetPosition,
    price: Decimal,
    timestamp_ms: TimestampMs,
    fee_rate: Decimal,
    cash: &mut Decimal,
    qty: &mut Decimal,
    open_trade: &mut Option<OpenTrade>,
    trades: &mut Vec<ClosedTrade>,
) {
    match target {
        TargetPosition::Flat => {
            if *qty <= Decimal::ZERO {
                return;
            }

            let notional = *qty * price;
            let fee = notional * fee_rate;
            let cash_after = *cash + notional - fee;

            if let Some(open_trade) = open_trade.take() {
                trades.push(ClosedTrade {
                    symbol: market.symbol.clone(),
                    entry_time_ms: open_trade.entry_time_ms,
                    exit_time_ms: timestamp_ms,
                    entry_price: open_trade.entry_price,
                    exit_price: price,
                    qty: open_trade.qty,
                    gross_quote_pnl: cash_after - open_trade.cash_before,
                    entry_order_id: None,
                    exit_order_id: None,
                });
            }

            *cash = cash_after;
            *qty = Decimal::ZERO;
        }
        TargetPosition::LongAllIn => {
            if *qty > Decimal::ZERO || *cash <= Decimal::ZERO {
                return;
            }

            let denominator = price * (Decimal::ONE + fee_rate);
            if denominator <= Decimal::ZERO {
                return;
            }

            let cash_before = *cash;
            let buy_qty = *cash / denominator;
            let notional = buy_qty * price;
            let fee = notional * fee_rate;

            *cash = *cash - notional - fee;
            *qty = buy_qty;
            *open_trade = Some(OpenTrade {
                entry_time_ms: timestamp_ms,
                entry_price: price,
                qty: buy_qty,
                cash_before,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BuiltInStrategyConfig, ExchangeId, Interval, Symbol, validate_candles};
    use std::str::FromStr;

    #[derive(Debug)]
    struct ScriptedStrategy {
        targets: Vec<(TimestampMs, TargetPosition)>,
    }

    impl Strategy for ScriptedStrategy {
        fn name(&self) -> &str {
            "scripted"
        }

        fn on_bar(
            &mut self,
            _ctx: &StrategyContext,
            bar: &Candle,
        ) -> Result<Option<TargetPosition>, crate::StrategyError> {
            Ok(self
                .targets
                .iter()
                .find(|(ts, _)| *ts == bar.open_time_ms)
                .map(|(_, target)| *target))
        }
    }

    fn market() -> MarketId {
        MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        )
    }

    fn candle(open_time_ms: i64, open: &str, close: &str) -> Candle {
        Candle {
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            open: Decimal::from_str(open).expect("decimal"),
            high: Decimal::from_str(close).expect("decimal"),
            low: Decimal::from_str(open).expect("decimal"),
            close: Decimal::from_str(close).expect("decimal"),
            volume: Decimal::ONE,
            trades: Some(1),
        }
    }

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("decimal")
    }

    fn ohlc_candle(index: i64, open: &str, close: &str) -> Candle {
        let open = dec(open);
        let close = dec(close);
        let open_time_ms = index * 60_000;
        Candle {
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            open,
            high: open.max(close),
            low: open.min(close),
            close,
            volume: Decimal::ONE,
            trades: Some(1),
        }
    }

    // Regression fixture for the full SMA-cross pipeline (fast=2, slow=3,
    // cash=10010, fee=10 bps). Prices are chosen so every division behind the
    // pinned values is exact in `Decimal` and each entry spends cash to
    // exactly zero, keeping the pinned trades, equity, return, and drawdown
    // hand-verifiable (the few rounded divisions only feed comparisons whose
    // outcome is unambiguous):
    // - closes rise: fast crosses above slow on bar 2, buy fills at bar 3
    //   open 100 (10010 / (100 * 1.001) = qty 100), equity peaks at 11000 on
    //   bar 5, then closes fall to 9900 on bar 7 (the 10% max drawdown) where
    //   fast drops below slow;
    // - the sell fills at bar 8 open 100.1 (cash 100 * 100.1 * 0.999 =
    //   9999.99, a fee-driven losing trade);
    // - bar 10 has fast == slow, which must produce no signal;
    // - fast crosses back above slow on bar 11, the buy fills at bar 12 open
    //   99.9 (9999.99 / (99.9 * 1.001) = qty 100), and the run ends long, so
    //   close-out sells at the last close 110.11 (cash 10999.989).
    fn sma_cross_fixture() -> Vec<Candle> {
        [
            ("96", "97"),
            ("97", "99"),
            ("99", "101"),
            ("100", "103"),
            ("103", "105"),
            ("105", "110"),
            ("110", "103"),
            ("103", "99"),
            ("100.1", "98"),
            ("98", "97"),
            ("97", "99"),
            ("99", "101"),
            ("99.9", "103"),
            ("103", "105"),
            ("105", "110.11"),
        ]
        .iter()
        .enumerate()
        .map(|(index, (open, close))| ohlc_candle(index as i64, open, close))
        .collect()
    }

    #[test]
    fn backtest_executes_on_next_bar_open_and_records_trade() {
        let candles = vec![
            candle(0, "100", "100"),
            candle(60_000, "110", "110"),
            candle(120_000, "120", "120"),
        ];
        let mut strategy = ScriptedStrategy {
            targets: vec![
                (0, TargetPosition::LongAllIn),
                (60_000, TargetPosition::Flat),
            ],
        };

        let result = BacktestEngine::new(BacktestConfig {
            initial_cash: Decimal::from(1_000),
            fee_bps: Decimal::ZERO,
            close_out_at_end: true,
        })
        .run(&market(), &candles, &mut strategy)
        .expect("backtest");

        assert_eq!(result.trade_count, 1);
        assert_eq!(result.trades[0].entry_price, Decimal::from(110));
        assert_eq!(result.trades[0].exit_price, Decimal::from(120));
        assert!(result.final_equity > Decimal::from(1_000));
        assert!(result.trades[0].gross_quote_pnl > Decimal::ZERO);
    }

    #[test]
    fn backtest_rejects_non_positive_initial_cash() {
        let candles = vec![candle(0, "100", "100")];
        let mut strategy = ScriptedStrategy { targets: vec![] };

        for cash in [Decimal::ZERO, Decimal::from(-100)] {
            let error = BacktestEngine::new(BacktestConfig {
                initial_cash: cash,
                fee_bps: Decimal::ZERO,
                close_out_at_end: true,
            })
            .run(&market(), &candles, &mut strategy)
            .expect_err("config error");
            assert!(
                matches!(error, EngineError::InvalidConfig(_)),
                "for cash {cash}"
            );
            assert!(
                error.to_string().contains("initial_cash must be greater"),
                "for cash {cash}, got {error}"
            );
        }
    }

    #[test]
    fn backtest_rejects_negative_fee_bps() {
        let candles = vec![candle(0, "100", "100")];
        let mut strategy = ScriptedStrategy { targets: vec![] };

        let error = BacktestEngine::new(BacktestConfig {
            initial_cash: Decimal::from(1_000),
            fee_bps: Decimal::from(-1),
            close_out_at_end: true,
        })
        .run(&market(), &candles, &mut strategy)
        .expect_err("config error");
        assert!(matches!(error, EngineError::InvalidConfig(_)));
        assert!(
            error
                .to_string()
                .contains("fee_bps must be zero or greater")
        );
    }

    #[test]
    fn backtest_sma_cross_on_fixed_fixture_reproduces_pinned_result() {
        let market = market();
        let candles = sma_cross_fixture();
        let report = validate_candles(&market, &candles);
        assert!(
            report.is_ok(),
            "fixture must stay a valid candle series: {:?}",
            report.issues
        );

        let run = || {
            let mut strategy = BuiltInStrategyConfig::SmaCross { fast: 2, slow: 3 }
                .build()
                .expect("strategy");
            BacktestEngine::new(BacktestConfig {
                initial_cash: dec("10010"),
                fee_bps: dec("10"),
                close_out_at_end: true,
            })
            .run(&market, &candles, strategy.as_mut())
            .expect("backtest")
        };

        let result = run();
        let expected = BacktestResult {
            initial_cash: dec("10010"),
            final_equity: dec("10999.989"),
            total_return_pct: dec("9.89"),
            trade_count: 2,
            max_drawdown_pct: dec("10"),
            trades: vec![
                ClosedTrade {
                    symbol: Symbol::new("BTCUSDT").expect("symbol"),
                    entry_time_ms: 180_000,
                    exit_time_ms: 480_000,
                    entry_price: dec("100"),
                    exit_price: dec("100.1"),
                    qty: dec("100"),
                    gross_quote_pnl: dec("-10.01"),
                    entry_order_id: None,
                    exit_order_id: None,
                },
                ClosedTrade {
                    symbol: Symbol::new("BTCUSDT").expect("symbol"),
                    entry_time_ms: 720_000,
                    exit_time_ms: 899_999,
                    entry_price: dec("99.9"),
                    exit_price: dec("110.11"),
                    qty: dec("100"),
                    gross_quote_pnl: dec("999.999"),
                    entry_order_id: None,
                    exit_order_id: None,
                },
            ],
        };
        assert_eq!(result, expected);

        // A fresh strategy over the same fixture must reproduce the result
        // bit for bit: the backtest is a pure function of its inputs.
        assert_eq!(run(), result);
    }
}
