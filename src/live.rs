use crate::{
    BotRunState, BuiltInStrategyConfig, Candle, CandleQuery, CandleStore, ClosedTrade,
    ExchangeOrder, ExecutionMode, MarketDataSource, MarketId, MarketOrderRequest, PositionState,
    RunJournalStore, RunStatus, Side, Strategy, StrategyContext, SymbolRules, TargetPosition,
    TimestampMs, TradingVenue, now_utc_ms, round_down_to_step,
};
use crate::{
    EngineError,
    data_sync::{sleep_or_shutdown, sync_market_range},
};
use rust_decimal::Decimal;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

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

    fn load_or_create_run_state(&self, cfg: &LiveTradeConfig) -> Result<BotRunState, EngineError> {
        if let Some(run_id) = &cfg.run_id {
            if let Some(existing) = self.journal_store.load_run_state(run_id)? {
                return Ok(existing);
            }
        }

        let now_ms = now_utc_ms();
        Ok(BotRunState {
            run_id: cfg
                .run_id
                .clone()
                .unwrap_or_else(|| format!("qf-{}", Uuid::new_v4().simple())),
            market: cfg.market.clone(),
            strategy_name: cfg.strategy.strategy_name().to_string(),
            strategy_config: serde_json::to_value(&cfg.strategy).map_err(|err| {
                EngineError::InvalidConfig(format!("failed to serialize strategy config: {err}"))
            })?,
            status: RunStatus::Starting,
            last_processed_open_time_ms: None,
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            stopped_at_ms: None,
            last_error: None,
            position: PositionState::flat(),
        })
    }

    async fn run_inner(
        &self,
        cfg: &LiveTradeConfig,
        rules: &SymbolRules,
        run_state: &mut BotRunState,
        strategy: &mut dyn Strategy,
        summary: &mut LiveTradeSummary,
    ) -> Result<(), EngineError> {
        self.journal_store.save_run_state(run_state)?;

        let now = now_utc_ms();
        let bootstrap_start = run_state
            .last_processed_open_time_ms
            .map(|value| value + cfg.market.interval.step_ms())
            .or_else(|| Some(now - (cfg.market.interval.step_ms() * cfg.bootstrap_bars as i64)));

        if let Some(start_ms) = bootstrap_start {
            sync_market_range(
                self.market_data,
                self.candle_store,
                &cfg.market,
                start_ms,
                now,
                cfg.batch_limit,
            )
            .await?;
        }

        let mut ctx = LiveStrategyContext::new(cfg.market.clone(), run_state.position.qty);
        strategy.on_start(&mut ctx)?;

        let bootstrap_candles = self
            .candle_store
            .load_recent_candles(&cfg.market, cfg.bootstrap_bars)?;
        let closed_bootstrap = filter_closed_candles(bootstrap_candles);

        let mut last_bootstrap_target = current_target(&run_state.position);
        for candle in &closed_bootstrap {
            ctx.now_ms = candle.close_time_ms;
            ctx.position_qty = run_state.position.qty;
            ctx.desired_target = None;
            strategy.on_bar(&mut ctx, candle)?;
            if let Some(target) = ctx.desired_target {
                last_bootstrap_target = target;
            }
        }

        if run_state.last_processed_open_time_ms.is_none() {
            run_state.last_processed_open_time_ms = closed_bootstrap.last().map(|c| c.open_time_ms);
            run_state.status = RunStatus::Running;
            run_state.updated_at_ms = now_utc_ms();

            if cfg.bootstrap_enter && last_bootstrap_target != current_target(&run_state.position) {
                if let Some(reference_bar) = closed_bootstrap.last() {
                    self.execute_target(
                        cfg,
                        rules,
                        run_state,
                        last_bootstrap_target,
                        reference_bar,
                        summary,
                    )
                    .await?;
                }
            }
            self.journal_store.save_run_state(run_state)?;
        }

        let mut loops = 0usize;
        loop {
            let end_ms = now_utc_ms();
            let start_ms = run_state
                .last_processed_open_time_ms
                .map(|value| value + cfg.market.interval.step_ms())
                .unwrap_or_else(|| end_ms - cfg.market.interval.step_ms());

            if start_ms <= end_ms {
                sync_market_range(
                    self.market_data,
                    self.candle_store,
                    &cfg.market,
                    start_ms,
                    end_ms,
                    cfg.batch_limit,
                )
                .await?;
            }

            let new_candles = self.candle_store.load_candles(
                &cfg.market,
                CandleQuery {
                    start_time_ms: run_state
                        .last_processed_open_time_ms
                        .map(|value| value + cfg.market.interval.step_ms()),
                    end_time_ms: None,
                    limit: None,
                },
            )?;
            let closed_new_candles = filter_closed_candles(new_candles);

            for candle in closed_new_candles {
                if run_state
                    .last_processed_open_time_ms
                    .map(|value| candle.open_time_ms <= value)
                    .unwrap_or(false)
                {
                    continue;
                }

                ctx.now_ms = candle.close_time_ms;
                ctx.position_qty = run_state.position.qty;
                ctx.desired_target = None;

                strategy.on_bar(&mut ctx, &candle)?;
                let desired = ctx
                    .desired_target
                    .unwrap_or_else(|| current_target(&run_state.position));

                if desired != current_target(&run_state.position) {
                    self.execute_target(cfg, rules, run_state, desired, &candle, summary)
                        .await?;
                }

                run_state.last_processed_open_time_ms = Some(candle.open_time_ms);
                run_state.status = RunStatus::Running;
                run_state.updated_at_ms = now_utc_ms();
                self.journal_store.save_run_state(run_state)?;
                summary.processed_bars += 1;
                summary.last_processed_open_time_ms = run_state.last_processed_open_time_ms;
            }

            loops += 1;
            if cfg.max_loops.map(|max| loops >= max).unwrap_or(false) {
                break;
            }
            if sleep_or_shutdown(cfg.poll_interval).await {
                break;
            }
        }

        strategy.on_finish(&mut ctx)?;
        Ok(())
    }

    async fn execute_target(
        &self,
        cfg: &LiveTradeConfig,
        rules: &SymbolRules,
        run_state: &mut BotRunState,
        target: TargetPosition,
        reference_bar: &Candle,
        summary: &mut LiveTradeSummary,
    ) -> Result<(), EngineError> {
        let order = match cfg.execution_mode {
            ExecutionMode::DryRun => synthetic_market_order(
                rules,
                run_state,
                target,
                cfg.quote_order_qty,
                reference_bar,
            )?,
            ExecutionMode::Live => {
                let venue = self.trading_venue.ok_or_else(|| {
                    EngineError::InvalidConfig("live mode requires a trading venue".to_string())
                })?;
                match target {
                    TargetPosition::LongAllIn => {
                        if let Some(min_notional) = rules.min_notional {
                            if cfg.quote_order_qty < min_notional {
                                return Err(EngineError::InvalidConfig(format!(
                                    "quote_order_qty {} is below exchange min_notional {}",
                                    cfg.quote_order_qty, min_notional
                                )));
                            }
                        }

                        venue
                            .submit_market_order(&MarketOrderRequest {
                                symbol: cfg.market.symbol.clone(),
                                side: Side::Buy,
                                quantity: None,
                                quote_order_qty: Some(cfg.quote_order_qty),
                                new_client_order_id: Some(new_client_order_id(
                                    "entry",
                                    &run_state.run_id,
                                )),
                            })
                            .await?
                    }
                    TargetPosition::Flat => {
                        let balances = venue.account_balances().await?;
                        let free_base_qty = balances
                            .into_iter()
                            .find(|balance| balance.asset.eq_ignore_ascii_case(&rules.base_asset))
                            .map(|balance| balance.free)
                            .unwrap_or(Decimal::ZERO);

                        let requested_qty = free_base_qty.min(run_state.position.qty);
                        let requested_qty = maybe_round_qty(requested_qty, rules);

                        if requested_qty <= Decimal::ZERO {
                            warn!(
                                requested_qty = %requested_qty,
                                run_position_qty = %run_state.position.qty,
                                "ignoring flat target because no sellable quantity remained"
                            );
                            return Ok(());
                        }

                        venue
                            .submit_market_order(&MarketOrderRequest {
                                symbol: cfg.market.symbol.clone(),
                                side: Side::Sell,
                                quantity: Some(requested_qty),
                                quote_order_qty: None,
                                new_client_order_id: Some(new_client_order_id(
                                    "exit",
                                    &run_state.run_id,
                                )),
                            })
                            .await?
                    }
                }
            }
        };

        info!(
            run_id = %run_state.run_id,
            side = %order.side,
            status = %order.status.as_str(),
            executed_qty = %order.executed_qty,
            avg_price = ?order.average_price(),
            "order submitted"
        );

        self.journal_store
            .append_order_event(&run_state.run_id, &order)?;
        summary.submitted_orders += 1;

        match target {
            TargetPosition::LongAllIn => {
                let qty = order.net_base_qty_after_base_fees(&rules.base_asset);
                if qty <= Decimal::ZERO {
                    warn!("entry order had zero executed quantity");
                    return Ok(());
                }

                run_state.position = PositionState {
                    qty,
                    entry_price: order.average_price(),
                    entry_time_ms: order.transact_time_ms.or(Some(reference_bar.close_time_ms)),
                    entry_order_id: order.order_id,
                };
            }
            TargetPosition::Flat => {
                let closed_qty = order.executed_qty.min(run_state.position.qty);
                if closed_qty <= Decimal::ZERO {
                    warn!("exit order had zero executed quantity");
                    return Ok(());
                }

                let entry_price = run_state
                    .position
                    .entry_price
                    .unwrap_or(reference_bar.close);
                let exit_price = order.average_price().unwrap_or(reference_bar.close);

                let closed_trade = ClosedTrade {
                    symbol: cfg.market.symbol.clone(),
                    entry_time_ms: run_state
                        .position
                        .entry_time_ms
                        .unwrap_or(reference_bar.open_time_ms),
                    exit_time_ms: order
                        .transact_time_ms
                        .unwrap_or(reference_bar.close_time_ms),
                    entry_price,
                    exit_price,
                    qty: closed_qty,
                    gross_quote_pnl: (exit_price - entry_price) * closed_qty,
                    entry_order_id: run_state.position.entry_order_id,
                    exit_order_id: order.order_id,
                };
                self.journal_store
                    .append_closed_trade(&run_state.run_id, &closed_trade)?;
                summary.closed_trades += 1;

                let remaining_qty = (run_state.position.qty - closed_qty).max(Decimal::ZERO);
                if remaining_qty > Decimal::ZERO {
                    run_state.position.qty = remaining_qty;
                } else {
                    run_state.position = PositionState::flat();
                }
            }
        }

        run_state.updated_at_ms = now_utc_ms();
        run_state.status = RunStatus::Running;
        run_state.last_error = None;
        self.journal_store.save_run_state(run_state)?;
        Ok(())
    }
}

fn current_target(position: &PositionState) -> TargetPosition {
    if position.is_open() {
        TargetPosition::LongAllIn
    } else {
        TargetPosition::Flat
    }
}

fn filter_closed_candles(candles: Vec<Candle>) -> Vec<Candle> {
    let now_ms = now_utc_ms();
    candles
        .into_iter()
        .filter(|candle| candle.close_time_ms <= now_ms)
        .collect()
}

fn maybe_round_qty(qty: Decimal, rules: &SymbolRules) -> Decimal {
    if let Some(step_size) = rules.effective_market_step_size() {
        round_down_to_step(qty, step_size)
    } else {
        qty
    }
}

fn synthetic_market_order(
    rules: &SymbolRules,
    run_state: &BotRunState,
    target: TargetPosition,
    quote_order_qty: Decimal,
    reference_bar: &Candle,
) -> Result<ExchangeOrder, EngineError> {
    let side = match target {
        TargetPosition::LongAllIn => Side::Buy,
        TargetPosition::Flat => Side::Sell,
    };

    let (requested_qty, requested_quote_qty, executed_qty, cumulative_quote_qty) = match target {
        TargetPosition::LongAllIn => {
            if reference_bar.close <= Decimal::ZERO {
                return Err(EngineError::InvalidState(
                    "cannot simulate market buy with non-positive reference price".to_string(),
                ));
            }
            let raw_qty = quote_order_qty / reference_bar.close;
            let qty = maybe_round_qty(raw_qty, rules);
            (None, Some(quote_order_qty), qty, qty * reference_bar.close)
        }
        TargetPosition::Flat => {
            let qty = maybe_round_qty(run_state.position.qty, rules);
            (Some(qty), None, qty, qty * reference_bar.close)
        }
    };

    Ok(ExchangeOrder {
        symbol: run_state.market.symbol.clone(),
        side,
        order_type: "MARKET".to_string(),
        status: crate::OrderStatus::Filled,
        order_id: None,
        client_order_id: Some(new_client_order_id("dry", &run_state.run_id)),
        requested_qty,
        requested_quote_qty,
        executed_qty,
        cumulative_quote_qty,
        avg_price: Some(reference_bar.close),
        transact_time_ms: Some(reference_bar.close_time_ms),
        fills: Vec::new(),
        raw: serde_json::json!({
            "execution_mode": "dry_run",
            "reference_open_time_ms": reference_bar.open_time_ms,
            "reference_close_time_ms": reference_bar.close_time_ms
        }),
    })
}

fn sanitize_client_order_id_fragment(input: &str, max_len: usize) -> String {
    let out: String = input
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(max_len)
        .collect();

    if out.is_empty() {
        "run".to_string()
    } else {
        out
    }
}

fn new_client_order_id(tag: &str, run_id: &str) -> String {
    let tag = sanitize_client_order_id_fragment(tag, 5);
    let prefix = sanitize_client_order_id_fragment(run_id, 8);

    let nonce = Uuid::new_v4().simple().to_string();
    let nonce = &nonce[..8];

    // keep timestamp short so total length stays <= 36
    let ts = (now_utc_ms() % 100_000_000).to_string();

    // qf-<tag>-<prefix>-<ts>-<nonce>
    let id = format!("qf-{tag}-{prefix}-{ts}-{nonce}");
    debug_assert!(id.len() <= 36);
    id
}

#[derive(Debug)]
struct LiveStrategyContext {
    market: MarketId,
    now_ms: TimestampMs,
    cash: Decimal,
    position_qty: Decimal,
    desired_target: Option<TargetPosition>,
}

impl LiveStrategyContext {
    fn new(market: MarketId, position_qty: Decimal) -> Self {
        Self {
            market,
            now_ms: now_utc_ms(),
            cash: Decimal::ZERO,
            position_qty,
            desired_target: None,
        }
    }
}

impl StrategyContext for LiveStrategyContext {
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

    fn market() -> MarketId {
        MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("BTCUSDT").expect("symbol"),
            Interval::M1,
        )
    }

    fn rules() -> SymbolRules {
        SymbolRules {
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            min_qty: Some(Decimal::from_str("0.001").expect("decimal")),
            max_qty: None,
            step_size: Some(Decimal::from_str("0.001").expect("decimal")),
            market_min_qty: Some(Decimal::from_str("0.001").expect("decimal")),
            market_max_qty: None,
            market_step_size: Some(Decimal::from_str("0.001").expect("decimal")),
            min_notional: Some(Decimal::from(10)),
            tick_size: Some(Decimal::from_str("0.01").expect("decimal")),
        }
    }

    fn reference_bar() -> Candle {
        Candle {
            open_time_ms: 0,
            close_time_ms: 59_999,
            open: Decimal::from(10_000),
            high: Decimal::from(10_000),
            low: Decimal::from(10_000),
            close: Decimal::from(10_000),
            volume: Decimal::ONE,
            trades: Some(1),
        }
    }

    fn run_state() -> BotRunState {
        BotRunState {
            run_id: "run-1".to_string(),
            market: market(),
            strategy_name: "sma_cross".to_string(),
            strategy_config: serde_json::json!({"kind":"sma_cross","fast":20,"slow":50}),
            status: RunStatus::Running,
            last_processed_open_time_ms: None,
            started_at_ms: 0,
            updated_at_ms: 0,
            stopped_at_ms: None,
            last_error: None,
            position: PositionState {
                qty: Decimal::from_str("0.0254").expect("decimal"),
                entry_price: Some(Decimal::from(9_900)),
                entry_time_ms: Some(0),
                entry_order_id: Some(7),
            },
        }
    }

    #[test]
    fn synthetic_buy_uses_quote_order_qty_and_rounds_down() {
        let order = synthetic_market_order(
            &rules(),
            &run_state(),
            TargetPosition::LongAllIn,
            Decimal::from(123),
            &reference_bar(),
        )
        .expect("order");

        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.requested_quote_qty, Some(Decimal::from(123)));
        assert_eq!(
            order.executed_qty,
            Decimal::from_str("0.012").expect("decimal")
        );
    }

    #[test]
    fn synthetic_sell_uses_position_qty_and_rounds_down() {
        let order = synthetic_market_order(
            &rules(),
            &run_state(),
            TargetPosition::Flat,
            Decimal::from(123),
            &reference_bar(),
        )
        .expect("order");

        assert_eq!(order.side, Side::Sell);
        assert_eq!(
            order.requested_qty,
            Some(Decimal::from_str("0.025").expect("decimal"))
        );
        assert_eq!(
            order.executed_qty,
            Decimal::from_str("0.025").expect("decimal")
        );
    }
}
