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
            desired_target: None,
        };

        strategy.on_start(&mut ctx)?;

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
            ctx.desired_target = None;

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
            pending_target = ctx.desired_target;
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

#[derive(Debug)]
struct EngineContext {
    market: MarketId,
    now_ms: TimestampMs,
    cash: Decimal,
    position_qty: Decimal,
    desired_target: Option<TargetPosition>,
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
        self.desired_target = Some(target);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExchangeId, Interval, Symbol};
    use std::str::FromStr;

    #[derive(Debug)]
    struct ScriptedStrategy {
        targets: Vec<(TimestampMs, TargetPosition)>,
    }

    impl Strategy for ScriptedStrategy {
        fn name(&self) -> &'static str {
            "scripted"
        }

        fn on_bar(
            &mut self,
            ctx: &mut dyn StrategyContext,
            bar: &Candle,
        ) -> Result<(), crate::StrategyError> {
            for (ts, target) in &self.targets {
                if *ts == bar.open_time_ms {
                    ctx.set_target_position(*target);
                }
            }
            Ok(())
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
}
