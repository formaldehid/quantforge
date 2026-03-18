use rust_decimal::Decimal;
use thiserror::Error;
use tracing::info;

use crate::{
    model::{Candle, MarketId, TimestampMs},
    sdk::{Strategy, StrategyContext, StrategyError, TargetPosition},
};

#[derive(Error, Debug)]
pub enum BacktestError {
    #[error("no candles provided")]
    NoCandles,

    #[error("strategy error: {0}")]
    Strategy(#[from] StrategyError),
}

#[derive(Clone, Debug)]
pub struct BacktestConfig {
    pub initial_cash: Decimal,
    pub fee_bps: Decimal,
    pub close_out_at_end: bool,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_cash: Decimal::from(10_000),
            fee_bps: Decimal::from(10),
            close_out_at_end: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Trade {
    pub entry_time_ms: TimestampMs,
    pub entry_price: Decimal,
    pub exit_time_ms: TimestampMs,
    pub exit_price: Decimal,
    pub qty: Decimal,
    pub pnl: Decimal,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BacktestResult {
    pub initial_cash: Decimal,
    pub final_equity: Decimal,
    pub total_return_pct: Decimal,
    pub trade_count: usize,
    pub max_drawdown_pct: Decimal,
    pub trades: Vec<Trade>,
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
    ) -> Result<BacktestResult, BacktestError> {
        if candles.is_empty() {
            return Err(BacktestError::NoCandles);
        }

        let fee_rate = self.cfg.fee_bps / Decimal::from(10_000);
        let mut cash = self.cfg.initial_cash;
        let mut qty = Decimal::ZERO;
        let mut open_trade: Option<OpenTrade> = None;
        let mut trades = Vec::new();
        let mut pending_target: Option<TargetPosition> = None;
        let mut peak_equity = Decimal::ZERO;
        let mut max_drawdown = Decimal::ZERO;

        let mut ctx = EngineContext {
            market: market.clone(),
            now_ms: candles[0].open_time_ms,
            cash,
            position_qty: qty,
            desired_next: None,
        };

        strategy.on_start(&mut ctx)?;

        for (index, candle) in candles.iter().enumerate() {
            ctx.now_ms = candle.open_time_ms;

            if index > 0 {
                if let Some(target) = pending_target.take() {
                    execute_target(
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
            ctx.desired_next = None;

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

            strategy.on_bar(&mut ctx, candle)?;
            pending_target = ctx.desired_next;
        }

        if self.cfg.close_out_at_end && qty > Decimal::ZERO {
            let last = candles.last().expect("non-empty checked above");
            execute_target(
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

        ctx.cash = cash;
        ctx.position_qty = qty;
        strategy.on_finish(&mut ctx)?;

        let last_close = candles.last().map(|c| c.close).unwrap_or(Decimal::ZERO);
        let final_equity = cash + qty * last_close;
        let total_return_pct = if self.cfg.initial_cash == Decimal::ZERO {
            Decimal::ZERO
        } else {
            (final_equity - self.cfg.initial_cash) / self.cfg.initial_cash * Decimal::from(100)
        };

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
    target: TargetPosition,
    price: Decimal,
    timestamp_ms: TimestampMs,
    fee_rate: Decimal,
    cash: &mut Decimal,
    qty: &mut Decimal,
    open_trade: &mut Option<OpenTrade>,
    trades: &mut Vec<Trade>,
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
                trades.push(Trade {
                    entry_time_ms: open_trade.entry_time_ms,
                    entry_price: open_trade.entry_price,
                    exit_time_ms: timestamp_ms,
                    exit_price: price,
                    qty: open_trade.qty,
                    pnl: cash_after - open_trade.cash_before,
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

#[derive(Debug)]
struct EngineContext {
    market: MarketId,
    now_ms: TimestampMs,
    cash: Decimal,
    position_qty: Decimal,
    desired_next: Option<TargetPosition>,
}

impl StrategyContext for EngineContext {
    fn market(&self) -> &MarketId {
        &self.market
    }

    fn now_ms(&self) -> TimestampMs {
        self.now_ms
    }

    fn cash(&self) -> Decimal {
        self.cash
    }

    fn position_qty(&self) -> Decimal {
        self.position_qty
    }

    fn set_target_position(&mut self, target: TargetPosition) {
        self.desired_next = Some(target);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::{ExchangeId, Interval, Symbol},
        sdk::strategies::SmaCrossStrategy,
    };
    use std::str::FromStr;

    fn candle(open_time_ms: i64, open: &str, close: &str) -> Candle {
        let open = Decimal::from_str(open).expect("decimal");
        let close = Decimal::from_str(close).expect("decimal");
        let high = open.max(close) + Decimal::ONE;
        let low = open.min(close) - Decimal::ONE;

        Candle {
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            open,
            high,
            low,
            close,
            volume: Decimal::from(100),
            trades: Some(1),
        }
    }

    #[test]
    fn backtest_is_deterministic() {
        let market = MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        );
        let candles = vec![
            candle(0, "100", "100"),
            candle(60_000, "100", "101"),
            candle(120_000, "101", "102"),
            candle(180_000, "102", "105"),
            candle(240_000, "105", "103"),
            candle(300_000, "103", "99"),
            candle(360_000, "99", "98"),
            candle(420_000, "98", "101"),
        ];

        let mut strategy = SmaCrossStrategy::new(2, 3).expect("strategy");
        let engine = BacktestEngine::new(BacktestConfig {
            initial_cash: Decimal::from(10_000),
            fee_bps: Decimal::from(10),
            close_out_at_end: true,
        });

        let result_a = engine
            .run(&market, &candles, &mut strategy)
            .expect("backtest");
        let mut strategy_b = SmaCrossStrategy::new(2, 3).expect("strategy");
        let result_b = engine
            .run(&market, &candles, &mut strategy_b)
            .expect("backtest");

        assert_eq!(result_a, result_b);
    }
}
