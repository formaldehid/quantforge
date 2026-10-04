//! Polling live/dry-run trading engine: public surface and the run
//! lifecycle. The work is split across private sibling modules — `runner`
//! owns the poll loop, `execution` places orders, `state` owns run-state
//! identity and resume checks. Only [`LiveTradeConfig`], [`LiveTradeEngine`]
//! and [`LiveTradeSummary`] leave this namespace.

mod execution;
mod runner;
mod state;

use crate::EngineError;
use crate::{
    BuiltInStrategyConfig, CandleStore, ExecutionMode, MarketDataSource, MarketId, RunJournalStore,
    RunStatus, TradingVenue, now_utc_ms,
};
use rust_decimal::Decimal;
use std::time::Duration;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct LiveTradeConfig {
    pub market: MarketId,
    pub strategy: BuiltInStrategyConfig,
    pub execution_mode: ExecutionMode,
    pub quote_order_qty: Decimal,
    pub poll_interval: Duration,
    pub bootstrap_bars: usize,
    pub bootstrap_enter: bool,
    pub batch_limit: u16,
    pub run_id: Option<String>,
    pub max_loops: Option<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LiveTradeSummary {
    pub run_id: String,
    pub processed_bars: usize,
    pub submitted_orders: usize,
    pub closed_trades: usize,
    pub last_processed_open_time_ms: Option<i64>,
}

pub struct LiveTradeEngine<'a> {
    market_data: &'a dyn MarketDataSource,
    candle_store: &'a dyn CandleStore,
    journal_store: &'a dyn RunJournalStore,
    trading_venue: Option<&'a dyn TradingVenue>,
}

impl<'a> std::fmt::Debug for LiveTradeEngine<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveTradeEngine").finish_non_exhaustive()
    }
}

impl<'a> LiveTradeEngine<'a> {
    pub fn new(
        market_data: &'a dyn MarketDataSource,
        candle_store: &'a dyn CandleStore,
        journal_store: &'a dyn RunJournalStore,
        trading_venue: Option<&'a dyn TradingVenue>,
    ) -> Self {
        Self {
            market_data,
            candle_store,
            journal_store,
            trading_venue,
        }
    }

    pub async fn run(&self, cfg: &LiveTradeConfig) -> Result<LiveTradeSummary, EngineError> {
        let mut summary = LiveTradeSummary::default();
        let mut run_state = self.load_or_create_run_state(cfg)?;
        let rules = self
            .market_data
            .fetch_symbol_rules(&cfg.market.symbol)
            .await?;
        if rules.effective_market_step_size().is_none() {
            warn!(
                symbol = %cfg.market.symbol,
                "exchange reported no lot-size step; quantity rounding is disabled"
            );
        }
        if rules.min_notional.is_none() {
            warn!(
                symbol = %cfg.market.symbol,
                "exchange reported no min-notional rule; the notional pre-trade check is disabled"
            );
        }
        let mut strategy = cfg.strategy.build()?;

        let result = self
            .run_inner(cfg, &rules, &mut run_state, strategy.as_mut(), &mut summary)
            .await;

        match result {
            Ok(()) => {
                run_state.status = RunStatus::Stopped;
                run_state.updated_at_ms = now_utc_ms();
                run_state.stopped_at_ms = Some(run_state.updated_at_ms);
                self.journal_store.save_run_state(&run_state)?;
                summary.run_id = run_state.run_id.clone();
                summary.last_processed_open_time_ms = run_state.last_processed_open_time_ms;
                Ok(summary)
            }
            Err(err) => {
                run_state.status = RunStatus::Failed;
                run_state.updated_at_ms = now_utc_ms();
                run_state.last_error = Some(err.to_string());
                run_state.stopped_at_ms = Some(run_state.updated_at_ms);
                self.journal_store.save_run_state(&run_state)?;
                Err(err)
            }
        }
    }
}
