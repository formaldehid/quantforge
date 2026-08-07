use crate::{
    BotRunState, BuiltInStrategyConfig, Candle, CandleQuery, CandleStore, ClosedTrade,
    ExchangeOrder, ExecutionMode, MarketDataSource, MarketId, MarketOrderRequest, PositionState,
    RunJournalStore, RunStatus, Side, Strategy, StrategyContext, SymbolRules, TargetPosition,
    TradingVenue, now_utc_ms, round_down_to_step,
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

    fn load_or_create_run_state(&self, cfg: &LiveTradeConfig) -> Result<BotRunState, EngineError> {
        if let Some(run_id) = &cfg.run_id {
            if let Some(existing) = self.journal_store.load_run_state(run_id)? {
                apply_resume_checks(&existing, cfg)?;
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
            execution_mode: cfg.execution_mode,
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
        let bootstrap_start = match run_state.last_processed_open_time_ms {
            Some(value) => Some(value + cfg.market.interval.step_ms()),
            None => {
                let bars = i64::try_from(cfg.bootstrap_bars).map_err(|_| {
                    EngineError::InvalidConfig(format!(
                        "bootstrap_bars {} does not fit into the timestamp range",
                        cfg.bootstrap_bars
                    ))
                })?;
                let window_start = cfg
                    .market
                    .interval
                    .step_ms()
                    .checked_mul(bars)
                    .and_then(|window_ms| now.checked_sub(window_ms))
                    .ok_or_else(|| {
                        EngineError::InvalidConfig(format!(
                            "bootstrap window overflows: {} bars of {}",
                            cfg.bootstrap_bars, cfg.market.interval
                        ))
                    })?;
                Some(window_start)
            }
        };

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

        let mut ctx = StrategyContext {
            market: cfg.market.clone(),
            now_ms: now_utc_ms(),
            // Live trading does not track quote-asset cash: `ctx.cash` is
            // always zero here, while the backtest context reports real cash.
            // Cash-based position sizing is a backtest-only feature today;
            // live sizing comes from `LiveTradeConfig::quote_order_qty`.
            cash: Decimal::ZERO,
            position_qty: run_state.position.qty,
        };
        strategy.on_start(&ctx)?;

        let bootstrap_candles = self
            .candle_store
            .load_recent_candles(&cfg.market, cfg.bootstrap_bars)?;
        let closed_bootstrap = filter_closed_candles(bootstrap_candles);

        let mut last_bootstrap_target = current_target(&run_state.position);
        for candle in &closed_bootstrap {
            ctx.now_ms = candle.close_time_ms;
            ctx.position_qty = run_state.position.qty;
            if let Some(target) = strategy.on_bar(&ctx, candle)? {
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

                let desired = strategy
                    .on_bar(&ctx, &candle)?
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

        strategy.on_finish(&ctx)?;
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
        // The min-notional entry check runs for BOTH execution modes, so a
        // clean dry run implies the same notional validation a live entry
        // performs. Exit-side quantity checks (balance, min/max lot rules)
        // depend on live balances and still run only in live mode.
        if target == TargetPosition::LongAllIn {
            ensure_entry_notional(rules, cfg.quote_order_qty)?;
        }

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
                        if let Some(min_qty) = sell_qty_below_market_min(requested_qty, rules) {
                            warn!(
                                requested_qty = %requested_qty,
                                min_qty = %min_qty,
                                "ignoring flat target: quantity is below the exchange minimum \
                                 (dust); the exchange would reject the sell"
                            );
                            return Ok(());
                        }
                        if let Some(max_qty) = sell_qty_above_market_max(requested_qty, rules) {
                            return Err(EngineError::InvalidState(format!(
                                "sell quantity {requested_qty} exceeds exchange maximum {max_qty}"
                            )));
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
            executed_qty = ?order.executed_qty,
            avg_price = ?order.average_price(),
            "order submitted"
        );

        // Journal the order event, but defer any journaling failure until the
        // position mutation below is persisted: an executed order must be
        // reflected in local state even when the journal write fails,
        // otherwise a restart sees a flat position and doubles the exposure.
        let order_event_result = self
            .journal_store
            .append_order_event(&run_state.run_id, &order)
            .map_err(|err| {
                EngineError::InvalidState(format!(
                    "order executed but journaling the order event failed ({err}); \
                     reconcile manually with `monitor status`"
                ))
            });
        summary.submitted_orders += 1;

        match target {
            TargetPosition::LongAllIn => {
                let executed_qty = order.executed_qty.ok_or_else(|| {
                    EngineError::InvalidState(format!(
                        "exchange did not report an executed quantity for entry order {:?}; \
                         reconcile the position manually with `monitor status`",
                        order.order_id
                    ))
                })?;
                if executed_qty <= Decimal::ZERO {
                    warn!("entry order had zero executed quantity");
                    order_event_result?;
                    return Ok(());
                }

                // The buy has executed: the position must be recorded even
                // when parts of the response are missing. Missing fills mean
                // the gross quantity is recorded (no fee deduction can be
                // computed); a missing price is recorded as None and will
                // fail the exit explicitly instead of fabricating PnL.
                let qty = match order.net_base_qty_after_base_fees(&rules.base_asset) {
                    Some(net) => net,
                    None => {
                        warn!(
                            order_id = ?order.order_id,
                            "exchange did not report fills; recording gross executed \
                             quantity without fee deduction"
                        );
                        executed_qty
                    }
                };
                if qty <= Decimal::ZERO {
                    warn!("entry order netted zero quantity after fees");
                    order_event_result?;
                    return Ok(());
                }
                if order.average_price().is_none() {
                    warn!(
                        order_id = ?order.order_id,
                        "exchange did not report a fill price for the entry; closing \
                         this position will not produce a trade record"
                    );
                }

                run_state.position = PositionState {
                    qty,
                    entry_price: order.average_price(),
                    entry_time_ms: order.transact_time_ms.or(Some(reference_bar.close_time_ms)),
                    entry_order_id: order.order_id,
                };

                run_state.updated_at_ms = now_utc_ms();
                run_state.status = RunStatus::Running;
                run_state.last_error = None;
                self.journal_store.save_run_state(run_state)?;
                order_event_result?;
                Ok(())
            }
            TargetPosition::Flat => {
                let executed_qty = order.executed_qty.ok_or_else(|| {
                    EngineError::InvalidState(format!(
                        "exchange did not report an executed quantity for exit order {:?}; \
                         run state left unchanged — reconcile manually with `monitor status`",
                        order.order_id
                    ))
                })?;
                let closed_qty = executed_qty.min(run_state.position.qty);
                if closed_qty <= Decimal::ZERO {
                    warn!("exit order had zero executed quantity");
                    order_event_result?;
                    return Ok(());
                }

                // The sell has executed on the exchange, so the local
                // position is updated and persisted before anything below
                // can fail. A missing price then aborts with a clear error
                // instead of writing a closed-trade row with fabricated PnL.
                let position_before = run_state.position.clone();
                let remaining_qty = (position_before.qty - closed_qty).max(Decimal::ZERO);
                if remaining_qty > Decimal::ZERO && !is_dust_remnant(remaining_qty, rules) {
                    run_state.position.qty = remaining_qty;
                } else {
                    if remaining_qty > Decimal::ZERO {
                        warn!(
                            written_off_qty = %remaining_qty,
                            "position remnant after exit is below the tradeable minimum; \
                             writing it off so the run does not wedge on unsellable dust"
                        );
                    }
                    run_state.position = PositionState::flat();
                }
                run_state.updated_at_ms = now_utc_ms();
                run_state.status = RunStatus::Running;
                run_state.last_error = None;
                self.journal_store.save_run_state(run_state)?;
                order_event_result?;

                let entry_price = position_before.entry_price.ok_or_else(|| {
                    EngineError::InvalidState(
                        "position had no recorded entry price; the exit was executed and \
                         position state updated, but no closed-trade row was written \
                         because its PnL would be fabricated"
                            .to_string(),
                    )
                })?;
                let exit_price = order.average_price().ok_or_else(|| {
                    EngineError::InvalidState(format!(
                        "exchange did not report a fill price for exit order {:?}; the exit \
                         was executed and position state updated, but no closed-trade row \
                         was written because its PnL would be fabricated",
                        order.order_id
                    ))
                })?;

                let closed_trade = ClosedTrade {
                    symbol: cfg.market.symbol.clone(),
                    entry_time_ms: position_before
                        .entry_time_ms
                        .unwrap_or(reference_bar.open_time_ms),
                    exit_time_ms: order
                        .transact_time_ms
                        .unwrap_or(reference_bar.close_time_ms),
                    entry_price,
                    exit_price,
                    qty: closed_qty,
                    gross_quote_pnl: (exit_price - entry_price) * closed_qty,
                    entry_order_id: position_before.entry_order_id,
                    exit_order_id: order.order_id,
                };
                self.journal_store
                    .append_closed_trade(&run_state.run_id, &closed_trade)?;
                summary.closed_trades += 1;
                Ok(())
            }
        }
    }
}

/// Validates that a resumed run matches the current invocation's identity.
///
/// A run's market, strategy, and execution mode are part of its identity: a
/// position accumulated under one must never be silently adopted by another
/// (a dry-run position leaking into live trading being the worst case).
fn apply_resume_checks(existing: &BotRunState, cfg: &LiveTradeConfig) -> Result<(), EngineError> {
    if existing.market != cfg.market {
        return Err(EngineError::InvalidConfig(format!(
            "run {} was recorded for market {} {} {}, but this invocation targets {} {} {}; \
             refusing to resume",
            existing.run_id,
            existing.market.exchange,
            existing.market.symbol,
            existing.market.interval,
            cfg.market.exchange,
            cfg.market.symbol,
            cfg.market.interval
        )));
    }

    let strategy_name = cfg.strategy.strategy_name();
    if existing.strategy_name != strategy_name {
        return Err(EngineError::InvalidConfig(format!(
            "run {} was recorded with strategy {}, but this invocation uses {}; \
             refusing to resume",
            existing.run_id, existing.strategy_name, strategy_name
        )));
    }

    let strategy_config = serde_json::to_value(&cfg.strategy).map_err(|err| {
        EngineError::InvalidConfig(format!("failed to serialize strategy config: {err}"))
    })?;
    if existing.strategy_config != strategy_config {
        return Err(EngineError::InvalidConfig(format!(
            "run {} was recorded with strategy config {}, but this invocation uses {}; \
             refusing to resume so its position is not driven by different parameters",
            existing.run_id, existing.strategy_config, strategy_config
        )));
    }

    if existing.execution_mode != cfg.execution_mode {
        return Err(EngineError::InvalidConfig(format!(
            "run {} was recorded in {} mode, but this invocation is {} mode; refusing to \
             resume so a {} position cannot leak into {} trading",
            existing.run_id,
            existing.execution_mode.as_str(),
            cfg.execution_mode.as_str(),
            existing.execution_mode.as_str(),
            cfg.execution_mode.as_str()
        )));
    }

    Ok(())
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

fn ensure_entry_notional(rules: &SymbolRules, quote_order_qty: Decimal) -> Result<(), EngineError> {
    if let Some(min_notional) = rules.min_notional {
        if quote_order_qty < min_notional {
            return Err(EngineError::InvalidConfig(format!(
                "quote_order_qty {quote_order_qty} is below exchange min_notional {min_notional}"
            )));
        }
    }
    Ok(())
}

/// Returns the exchange market-order minimum when `qty` is below it.
fn sell_qty_below_market_min(qty: Decimal, rules: &SymbolRules) -> Option<Decimal> {
    rules
        .effective_market_min_qty()
        .filter(|min_qty| qty < *min_qty)
}

/// Returns the exchange market-order maximum when `qty` exceeds it.
fn sell_qty_above_market_max(qty: Decimal, rules: &SymbolRules) -> Option<Decimal> {
    rules
        .effective_market_max_qty()
        .filter(|max_qty| qty > *max_qty)
}

/// A position remnant is dust when it cannot be sold: it rounds to zero at
/// the exchange step size or falls below the market minimum quantity.
/// Keeping dust as an open position wedges the run — exits skip it as
/// unsellable while entries are no-ops because the position reads as open.
fn is_dust_remnant(qty: Decimal, rules: &SymbolRules) -> bool {
    if qty <= Decimal::ZERO {
        return false;
    }
    let tradeable = maybe_round_qty(qty, rules);
    tradeable <= Decimal::ZERO || sell_qty_below_market_min(tradeable, rules).is_some()
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
        executed_qty: Some(executed_qty),
        cumulative_quote_qty: Some(cumulative_quote_qty),
        avg_price: Some(reference_bar.close),
        transact_time_ms: Some(reference_bar.close_time_ms),
        // Synthetic fills model no fees: dry-run reports the gross quantity
        // as net, which is optimistic relative to a real fill.
        fills: Some(Vec::new()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountTrade, AssetBalance, CancelOrderRequest, ExchangeError, ExchangeId, Interval,
        KlineRequest, OrderQueryRequest, OrderStatus, SqliteStore, StorageError, Symbol,
        TimestampMs,
    };
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::Mutex;
    use tempfile::tempdir;

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
            execution_mode: ExecutionMode::DryRun,
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
            Some(Decimal::from_str("0.012").expect("decimal"))
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
            Some(Decimal::from_str("0.025").expect("decimal"))
        );
    }

    fn config(execution_mode: ExecutionMode) -> LiveTradeConfig {
        LiveTradeConfig {
            market: market(),
            strategy: BuiltInStrategyConfig::SmaCross { fast: 20, slow: 50 },
            execution_mode,
            quote_order_qty: Decimal::from(100),
            poll_interval: Duration::from_secs(1),
            bootstrap_bars: 10,
            bootstrap_enter: false,
            batch_limit: 1000,
            run_id: Some("run-1".to_string()),
            max_loops: Some(1),
        }
    }

    #[test]
    fn resume_refuses_execution_mode_mismatch() {
        let existing = run_state();
        let error = apply_resume_checks(&existing, &config(ExecutionMode::Live)).expect_err("mode");
        assert!(matches!(error, EngineError::InvalidConfig(_)));
        assert!(
            error.to_string().contains("recorded in dry_run mode"),
            "got {error}"
        );
        assert!(
            error.to_string().contains("cannot leak into live trading"),
            "got {error}"
        );
    }

    #[test]
    fn resume_refuses_market_mismatch() {
        let mut existing = run_state();
        existing.market = MarketId::new(
            ExchangeId::BinanceSpot,
            Symbol::new("ETHUSDT").expect("symbol"),
            Interval::M1,
        );
        let error =
            apply_resume_checks(&existing, &config(ExecutionMode::DryRun)).expect_err("market");
        assert!(matches!(error, EngineError::InvalidConfig(_)));
        assert!(error.to_string().contains("ETHUSDT"), "got {error}");
        assert!(
            error.to_string().contains("refusing to resume"),
            "got {error}"
        );
    }

    #[test]
    fn resume_refuses_strategy_mismatch() {
        let mut existing = run_state();
        existing.strategy_name = "other_strategy".to_string();
        let error =
            apply_resume_checks(&existing, &config(ExecutionMode::DryRun)).expect_err("strategy");
        assert!(matches!(error, EngineError::InvalidConfig(_)));
        assert!(error.to_string().contains("other_strategy"), "got {error}");
    }

    #[test]
    fn resume_refuses_strategy_parameter_mismatch() {
        let mut existing = run_state();
        existing.strategy_config = serde_json::json!({"kind":"sma_cross","fast":5,"slow":200});
        let error = apply_resume_checks(&existing, &config(ExecutionMode::DryRun))
            .expect_err("parameter mismatch");
        assert!(matches!(error, EngineError::InvalidConfig(_)));
        assert!(error.to_string().contains("strategy config"), "got {error}");
    }

    #[test]
    fn resume_accepts_matching_identity() {
        let existing = run_state();
        apply_resume_checks(&existing, &config(ExecutionMode::DryRun)).expect("matching");
    }

    #[test]
    fn dust_remnants_are_detected_only_below_the_tradeable_minimum() {
        // step 0.001, market min 0.001 (from rules()).
        let sub_step = Decimal::from_str("0.0004").expect("decimal");
        let at_min = Decimal::from_str("0.001").expect("decimal");
        let sellable = Decimal::from_str("0.5").expect("decimal");

        assert!(is_dust_remnant(sub_step, &rules()));
        assert!(!is_dust_remnant(at_min, &rules()));
        assert!(!is_dust_remnant(sellable, &rules()));
        assert!(!is_dust_remnant(Decimal::ZERO, &rules()));
    }

    #[test]
    fn entry_notional_below_exchange_minimum_is_rejected() {
        let error = ensure_entry_notional(&rules(), Decimal::from(9)).expect_err("notional error");
        assert!(matches!(error, EngineError::InvalidConfig(_)));
        assert!(
            error.to_string().contains("below exchange min_notional 10"),
            "got {error}"
        );
    }

    #[test]
    fn entry_notional_at_or_above_exchange_minimum_is_accepted() {
        ensure_entry_notional(&rules(), Decimal::from(10)).expect("at minimum");
        ensure_entry_notional(&rules(), Decimal::from(100)).expect("above minimum");
    }

    #[test]
    fn sell_qty_rule_helpers_flag_only_out_of_range_quantities() {
        let mut rules = rules();
        rules.market_max_qty = Some(Decimal::from(1));

        let below = Decimal::from_str("0.0001").expect("decimal");
        let within = Decimal::from_str("0.5").expect("decimal");
        let above = Decimal::from(2);

        assert_eq!(
            sell_qty_below_market_min(below, &rules),
            Some(Decimal::from_str("0.001").expect("decimal"))
        );
        assert_eq!(sell_qty_below_market_min(within, &rules), None);
        assert_eq!(
            sell_qty_above_market_max(above, &rules),
            Some(Decimal::from(1))
        );
        assert_eq!(sell_qty_above_market_max(within, &rules), None);
    }

    #[test]
    fn sell_qty_rule_helpers_pass_everything_when_rules_are_absent() {
        let mut rules = rules();
        rules.min_qty = None;
        rules.max_qty = None;
        rules.market_min_qty = None;
        rules.market_max_qty = None;

        let qty = Decimal::from_str("0.0000001").expect("decimal");
        assert_eq!(sell_qty_below_market_min(qty, &rules), None);
        assert_eq!(
            sell_qty_above_market_max(Decimal::from(1_000_000), &rules),
            None
        );
    }

    // ── Execution-mode safety: engine-level behavior ─────────────────────
    //
    // The structural dry-run has two layers. The CLI wires trading_venue:
    // None for dry-run (handle_trade_run), so a dry-run process cannot even
    // hold an order endpoint. The tests below prove the second, engine-side
    // layer: even when a venue IS present, the ExecutionMode::DryRun arm of
    // execute_target never touches it (RefusingVenue panics on any call).
    // Dry-run fills are synthesized from the reference bar close and
    // journaled like real orders ("execution_mode": "dry_run" in raw,
    // qf-dry- client ids), and dry-run exits sell the journaled position
    // quantity without reading balances. Live mode requires a venue at the
    // moment an order is due (InvalidConfig, run journaled as Failed),
    // sizes entries by quote_order_qty without a balance read, and records
    // the position from the venue-reported fill, not the reference bar.
    // The min-notional entry check runs before the mode branch, in both
    // modes. CLI-side gates (dry-run default, --yes confirmation,
    // credential requirements, PRODUCTION marking) are covered black-box in
    // tests/cli.rs.

    // Market data for engine tests: one candle batch is consumed per
    // fetch_klines call (a drained queue yields an empty batch, which ends
    // a sync loop); symbol rules are served for real because
    // LiveTradeEngine::run fetches them in both execution modes.
    struct ScriptedMarketData {
        batches: Mutex<VecDeque<Vec<Candle>>>,
    }

    impl ScriptedMarketData {
        fn new(batches: Vec<Vec<Candle>>) -> Self {
            Self {
                batches: Mutex::new(batches.into_iter().collect()),
            }
        }
    }

    #[async_trait::async_trait]
    impl MarketDataSource for ScriptedMarketData {
        fn exchange_id(&self) -> ExchangeId {
            ExchangeId::BinanceSpot
        }

        async fn fetch_klines(
            &self,
            _request: &KlineRequest,
        ) -> Result<Vec<Candle>, ExchangeError> {
            Ok(self
                .batches
                .lock()
                .expect("lock")
                .pop_front()
                .unwrap_or_default())
        }

        async fn fetch_symbol_rules(&self, _symbol: &Symbol) -> Result<SymbolRules, ExchangeError> {
            Ok(rules())
        }
    }

    // A venue whose every method panics: passed as Some(&RefusingVenue) to
    // prove the DryRun arm of execute_target structurally never reaches the
    // venue, and that live mode leaves it untouched while no order is due.
    struct RefusingVenue;

    #[async_trait::async_trait]
    impl TradingVenue for RefusingVenue {
        fn exchange_id(&self) -> ExchangeId {
            ExchangeId::BinanceSpot
        }

        async fn account_balances(&self) -> Result<Vec<AssetBalance>, ExchangeError> {
            unreachable!("dry-run must never call the trading venue")
        }

        async fn open_orders(
            &self,
            _symbol: Option<&Symbol>,
        ) -> Result<Vec<ExchangeOrder>, ExchangeError> {
            unreachable!("dry-run must never call the trading venue")
        }

        async fn recent_trades(
            &self,
            _symbol: &Symbol,
            _limit: usize,
        ) -> Result<Vec<AccountTrade>, ExchangeError> {
            unreachable!("dry-run must never call the trading venue")
        }

        async fn submit_market_order(
            &self,
            _request: &MarketOrderRequest,
        ) -> Result<ExchangeOrder, ExchangeError> {
            unreachable!("dry-run must never call the trading venue")
        }

        async fn cancel_order(
            &self,
            _request: &CancelOrderRequest,
        ) -> Result<ExchangeOrder, ExchangeError> {
            unreachable!("dry-run must never call the trading venue")
        }

        async fn query_order(
            &self,
            _request: &OrderQueryRequest,
        ) -> Result<ExchangeOrder, ExchangeError> {
            unreachable!("dry-run must never call the trading venue")
        }
    }

    // Records every submitted market order and answers with one canned
    // fill. account_balances is unreachable on purpose: live entries size
    // by quote_order_qty and must not read balances.
    struct ScriptedVenue {
        submitted: Mutex<Vec<MarketOrderRequest>>,
        fill: ExchangeOrder,
    }

    #[async_trait::async_trait]
    impl TradingVenue for ScriptedVenue {
        fn exchange_id(&self) -> ExchangeId {
            ExchangeId::BinanceSpot
        }

        async fn account_balances(&self) -> Result<Vec<AssetBalance>, ExchangeError> {
            unreachable!("live entries size by quote_order_qty and never read balances")
        }

        async fn open_orders(
            &self,
            _symbol: Option<&Symbol>,
        ) -> Result<Vec<ExchangeOrder>, ExchangeError> {
            unreachable!("the trade engine submits market orders only")
        }

        async fn recent_trades(
            &self,
            _symbol: &Symbol,
            _limit: usize,
        ) -> Result<Vec<AccountTrade>, ExchangeError> {
            unreachable!("the trade engine submits market orders only")
        }

        async fn submit_market_order(
            &self,
            request: &MarketOrderRequest,
        ) -> Result<ExchangeOrder, ExchangeError> {
            self.submitted.lock().expect("lock").push(request.clone());
            Ok(self.fill.clone())
        }

        async fn cancel_order(
            &self,
            _request: &CancelOrderRequest,
        ) -> Result<ExchangeOrder, ExchangeError> {
            unreachable!("the trade engine submits market orders only")
        }

        async fn query_order(
            &self,
            _request: &OrderQueryRequest,
        ) -> Result<ExchangeOrder, ExchangeError> {
            unreachable!("the trade engine submits market orders only")
        }
    }

    // Wraps the real store so order-event appends fail while everything
    // else delegates; every saved (status, position qty) snapshot is
    // recorded so tests can assert the position-carrying save happened
    // BEFORE the deferred journaling error propagated — the ordering that
    // keeps a restart from doubling exposure.
    struct FailingOrderJournal<'a> {
        inner: &'a SqliteStore,
        saves: Mutex<Vec<(RunStatus, Decimal)>>,
    }

    impl RunJournalStore for FailingOrderJournal<'_> {
        fn init(&self) -> Result<(), StorageError> {
            RunJournalStore::init(self.inner)
        }

        fn save_run_state(&self, state: &BotRunState) -> Result<(), StorageError> {
            self.saves
                .lock()
                .expect("lock")
                .push((state.status, state.position.qty));
            self.inner.save_run_state(state)
        }

        fn load_run_state(&self, run_id: &str) -> Result<Option<BotRunState>, StorageError> {
            self.inner.load_run_state(run_id)
        }

        fn latest_run_for_market(
            &self,
            market: &MarketId,
            strategy_name: &str,
        ) -> Result<Option<BotRunState>, StorageError> {
            self.inner.latest_run_for_market(market, strategy_name)
        }

        fn append_order_event(
            &self,
            _run_id: &str,
            _order: &ExchangeOrder,
        ) -> Result<(), StorageError> {
            Err(StorageError::InvalidArgument(
                "scripted order-journal failure".to_string(),
            ))
        }

        fn append_closed_trade(
            &self,
            run_id: &str,
            trade: &ClosedTrade,
        ) -> Result<(), StorageError> {
            self.inner.append_closed_trade(run_id, trade)
        }

        fn list_order_events(
            &self,
            run_id: &str,
            limit: usize,
        ) -> Result<Vec<ExchangeOrder>, StorageError> {
            self.inner.list_order_events(run_id, limit)
        }

        fn list_closed_trades(
            &self,
            run_id: &str,
            limit: usize,
        ) -> Result<Vec<ClosedTrade>, StorageError> {
            self.inner.list_closed_trades(run_id, limit)
        }
    }

    // The live engine reads the wall clock internally (bootstrap window,
    // poll range, closed-bar filter), so a fixed epoch anchor would leave
    // every fixture bar either unclosed or centuries stale. Fixtures anchor
    // to the current minute, offset far enough into the past that every bar
    // stays closed at every clock read for any test runtime; all offsets
    // are exact multiples of the interval step, so everything except the
    // anchor itself is deterministic.
    fn anchor_ms() -> TimestampMs {
        now_utc_ms() / 60_000 * 60_000 - 10 * 60_000
    }

    fn engine_bar(open_time_ms: TimestampMs, close: Decimal) -> Candle {
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

    // Shared engine-test choreography. Warm-up bars are pre-seeded straight
    // into the store (the bootstrap replay reads the store, not the
    // source), and the scripted source serves [empty, vec![poll_bar]]: the
    // bootstrap sync pops the empty batch and stops without touching the
    // store, the poll sync delivers the signal bar, and the then-drained
    // queue ends the poll sync. The strategy therefore sees every bar
    // exactly once. Warm-up closes are equal — signal-silent under
    // fast=1/slow=2 — so the poll bar's close relative to 10_000 is the
    // entire signal script.
    fn timeline(poll_close: Decimal) -> (TimestampMs, Vec<Candle>, Candle) {
        let anchor = anchor_ms();
        let warmup = vec![
            engine_bar(anchor, Decimal::from(10_000)),
            engine_bar(anchor + 60_000, Decimal::from(10_000)),
        ];
        let poll_bar = engine_bar(anchor + 120_000, poll_close);

        // Guard against pathological clock skew at fixture-build time
        // instead of debugging a silent zero-bar run.
        let now = now_utc_ms();
        assert!(
            poll_bar.close_time_ms + 60_000 <= now,
            "fixture bars must be closed with >= 60s margin; clock skew detected"
        );

        (anchor, warmup, poll_bar)
    }

    fn seeded_store(dir: &tempfile::TempDir, warmup: &[Candle]) -> SqliteStore {
        let store = SqliteStore::new(dir.path().join("live.sqlite"));
        CandleStore::init(&store).expect("init");
        store
            .upsert_candles(&market(), warmup)
            .expect("seed warmup");
        store
    }

    fn engine_config(execution_mode: ExecutionMode, run_id: &str) -> LiveTradeConfig {
        LiveTradeConfig {
            market: market(),
            // fast=1/slow=2 keeps the script tiny: a bar's signal is its
            // close vs the previous close, and equal closes emit nothing.
            strategy: BuiltInStrategyConfig::SmaCross { fast: 1, slow: 2 },
            execution_mode,
            quote_order_qty: Decimal::from(100),
            poll_interval: Duration::from_millis(1),
            bootstrap_bars: 10,
            bootstrap_enter: false,
            batch_limit: 1000,
            run_id: Some(run_id.to_string()),
            max_loops: Some(1),
        }
    }

    // The open position and processed-bar cursor a resumed dry-run exits
    // from. Built from the config itself so apply_resume_checks always sees
    // an identical strategy config shape.
    fn seeded_open_run_state(cfg: &LiveTradeConfig, anchor: TimestampMs) -> BotRunState {
        BotRunState {
            run_id: cfg.run_id.clone().expect("run id"),
            market: cfg.market.clone(),
            strategy_name: cfg.strategy.strategy_name().to_string(),
            strategy_config: serde_json::to_value(&cfg.strategy).expect("strategy config"),
            execution_mode: cfg.execution_mode,
            status: RunStatus::Running,
            last_processed_open_time_ms: Some(anchor + 60_000),
            started_at_ms: anchor,
            updated_at_ms: anchor + 60_000,
            stopped_at_ms: None,
            last_error: None,
            position: PositionState {
                qty: Decimal::from_str("0.010").expect("decimal"),
                entry_price: Some(Decimal::from(10_000)),
                entry_time_ms: Some(anchor + 59_999),
                entry_order_id: None,
            },
        }
    }

    #[tokio::test]
    async fn dry_run_entry_journals_synthetic_order_and_never_calls_the_trading_venue() {
        let (_anchor, warmup, poll_bar) = timeline(Decimal::from(12_500));
        let tempdir = tempdir().expect("tempdir");
        let store = seeded_store(&tempdir, &warmup);
        let source = ScriptedMarketData::new(vec![Vec::new(), vec![poll_bar.clone()]]);
        let venue = RefusingVenue;

        let engine = LiveTradeEngine::new(&source, &store, &store, Some(&venue));
        let summary = engine
            .run(&engine_config(ExecutionMode::DryRun, "run-dry-entry"))
            .await
            .expect("dry run");

        assert_eq!(
            summary,
            LiveTradeSummary {
                run_id: "run-dry-entry".to_string(),
                processed_bars: 1,
                submitted_orders: 1,
                closed_trades: 0,
                last_processed_open_time_ms: Some(poll_bar.open_time_ms),
            }
        );

        let state = store
            .load_run_state("run-dry-entry")
            .expect("load state")
            .expect("state");
        assert_eq!(state.status, RunStatus::Stopped);
        assert_eq!(
            state.position.qty,
            Decimal::from_str("0.008").expect("decimal")
        );
        assert_eq!(state.position.entry_price, Some(Decimal::from(12_500)));
        assert_eq!(state.position.entry_time_ms, Some(poll_bar.close_time_ms));
        assert_eq!(state.position.entry_order_id, None);

        let events = store
            .list_order_events("run-dry-entry", 10)
            .expect("events");
        assert_eq!(events.len(), 1);
        let order = &events[0];
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.order_id, None);
        assert_eq!(order.requested_quote_qty, Some(Decimal::from(100)));
        assert_eq!(
            order.executed_qty,
            Some(Decimal::from_str("0.008").expect("decimal"))
        );
        assert_eq!(order.fills, Some(Vec::new()));
        // Pin the reported fill price itself: average_price() would derive
        // the same value from cumulative/executed even if avg_price were
        // dropped, so downstream math alone cannot detect that regression.
        assert_eq!(order.avg_price, Some(Decimal::from(12_500)));
        assert_eq!(order.raw["execution_mode"], "dry_run");
        assert_eq!(order.raw["reference_open_time_ms"], poll_bar.open_time_ms);
        let client_id = order.client_order_id.as_deref().expect("client id");
        assert!(client_id.starts_with("qf-dry-"), "got {client_id}");
        assert!(client_id.len() <= 36, "got {client_id}");
    }

    #[tokio::test]
    async fn dry_run_exit_on_resume_sells_journaled_qty_and_writes_closed_trade_without_venue_calls()
     {
        let (anchor, warmup, poll_bar) = timeline(Decimal::from(9_000));
        let tempdir = tempdir().expect("tempdir");
        let store = seeded_store(&tempdir, &warmup);
        let cfg = engine_config(ExecutionMode::DryRun, "run-dry-exit");
        store
            .save_run_state(&seeded_open_run_state(&cfg, anchor))
            .expect("seed state");
        let source = ScriptedMarketData::new(vec![Vec::new(), vec![poll_bar.clone()]]);
        let venue = RefusingVenue;

        let engine = LiveTradeEngine::new(&source, &store, &store, Some(&venue));
        let summary = engine.run(&cfg).await.expect("dry run");

        assert_eq!(
            summary,
            LiveTradeSummary {
                run_id: "run-dry-exit".to_string(),
                processed_bars: 1,
                submitted_orders: 1,
                closed_trades: 1,
                last_processed_open_time_ms: Some(poll_bar.open_time_ms),
            }
        );

        let state = store
            .load_run_state("run-dry-exit")
            .expect("load state")
            .expect("state");
        assert_eq!(state.status, RunStatus::Stopped);
        assert_eq!(state.position, PositionState::flat());

        let events = store.list_order_events("run-dry-exit", 10).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].side, Side::Sell);
        assert_eq!(
            events[0].requested_qty,
            Some(Decimal::from_str("0.010").expect("decimal"))
        );
        assert_eq!(
            events[0].executed_qty,
            Some(Decimal::from_str("0.010").expect("decimal"))
        );
        assert_eq!(events[0].avg_price, Some(Decimal::from(9_000)));
        assert_eq!(events[0].raw["execution_mode"], "dry_run");
        let client_id = events[0].client_order_id.as_deref().expect("client id");
        assert!(client_id.starts_with("qf-dry-"), "got {client_id}");

        let trades = store
            .list_closed_trades("run-dry-exit", 10)
            .expect("trades");
        assert_eq!(
            trades,
            vec![ClosedTrade {
                symbol: Symbol::new("BTCUSDT").expect("symbol"),
                entry_time_ms: anchor + 59_999,
                exit_time_ms: poll_bar.close_time_ms,
                entry_price: Decimal::from(10_000),
                exit_price: Decimal::from(9_000),
                qty: Decimal::from_str("0.010").expect("decimal"),
                gross_quote_pnl: Decimal::from(-10),
                entry_order_id: None,
                exit_order_id: None,
            }]
        );
    }

    #[tokio::test]
    async fn live_mode_without_a_venue_fails_the_run_when_an_order_is_due() {
        let (anchor, warmup, poll_bar) = timeline(Decimal::from(12_500));
        let tempdir = tempdir().expect("tempdir");
        let store = seeded_store(&tempdir, &warmup);
        let source = ScriptedMarketData::new(vec![Vec::new(), vec![poll_bar]]);

        let engine = LiveTradeEngine::new(&source, &store, &store, None);
        let error = engine
            .run(&engine_config(ExecutionMode::Live, "run-live-none"))
            .await
            .expect_err("missing venue");

        assert!(matches!(error, EngineError::InvalidConfig(_)));
        assert!(
            error
                .to_string()
                .contains("live mode requires a trading venue"),
            "got {error}"
        );

        let state = store
            .load_run_state("run-live-none")
            .expect("load state")
            .expect("state");
        assert_eq!(state.status, RunStatus::Failed);
        assert!(
            state
                .last_error
                .as_deref()
                .expect("last error")
                .contains("live mode requires a trading venue")
        );
        assert_eq!(state.position, PositionState::flat());
        // The failing bar was never watermarked: a restart reprocesses it.
        assert_eq!(state.last_processed_open_time_ms, Some(anchor + 60_000));
        assert!(
            store
                .list_order_events("run-live-none", 10)
                .expect("events")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn live_entry_submits_one_market_order_and_records_venue_reported_quantity() {
        let (_anchor, warmup, poll_bar) = timeline(Decimal::from(12_500));
        let tempdir = tempdir().expect("tempdir");
        let store = seeded_store(&tempdir, &warmup);
        let source = ScriptedMarketData::new(vec![Vec::new(), vec![poll_bar.clone()]]);
        // Every canned-fill field differs from what a synthetic fill would
        // fabricate (qty 0.007 vs 0.008, price 88.2/0.007 = 12_600 vs the
        // 12_500 bar close, timestamp close_time + 1), so the position
        // asserts prove the venue-reported data won.
        let venue = ScriptedVenue {
            submitted: Mutex::new(Vec::new()),
            fill: ExchangeOrder {
                symbol: Symbol::new("BTCUSDT").expect("symbol"),
                side: Side::Buy,
                order_type: "MARKET".to_string(),
                status: OrderStatus::Filled,
                order_id: Some(42),
                client_order_id: Some("venue-echo".to_string()),
                requested_qty: None,
                requested_quote_qty: Some(Decimal::from(100)),
                executed_qty: Some(Decimal::from_str("0.007").expect("decimal")),
                cumulative_quote_qty: Some(Decimal::from_str("88.2").expect("decimal")),
                avg_price: None,
                transact_time_ms: Some(poll_bar.close_time_ms + 1),
                fills: Some(Vec::new()),
                raw: serde_json::json!({}),
            },
        };

        let engine = LiveTradeEngine::new(&source, &store, &store, Some(&venue));
        let summary = engine
            .run(&engine_config(ExecutionMode::Live, "run-live-1"))
            .await
            .expect("live run");
        assert_eq!(summary.submitted_orders, 1);
        assert_eq!(summary.closed_trades, 0);

        let submitted = venue.submitted.lock().expect("lock");
        assert_eq!(submitted.len(), 1);
        let request = &submitted[0];
        assert_eq!(request.side, Side::Buy);
        assert_eq!(request.quantity, None);
        assert_eq!(request.quote_order_qty, Some(Decimal::from(100)));
        assert_eq!(request.symbol.as_str(), "BTCUSDT");
        let client_id = request.new_client_order_id.as_deref().expect("client id");
        assert!(client_id.starts_with("qf-entry-"), "got {client_id}");
        assert!(client_id.len() <= 36, "got {client_id}");
        drop(submitted);

        let state = store
            .load_run_state("run-live-1")
            .expect("load state")
            .expect("state");
        assert_eq!(
            state.position.qty,
            Decimal::from_str("0.007").expect("decimal")
        );
        assert_eq!(state.position.entry_price, Some(Decimal::from(12_600)));
        assert_eq!(state.position.entry_order_id, Some(42));
        assert_eq!(
            state.position.entry_time_ms,
            Some(poll_bar.close_time_ms + 1)
        );

        let events = store.list_order_events("run-live-1", 10).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].order_id, Some(42));
    }

    #[test]
    fn synthetic_market_order_rejects_non_positive_reference_price() {
        let mut reference = reference_bar();
        reference.close = Decimal::ZERO;

        let error = synthetic_market_order(
            &rules(),
            &run_state(),
            TargetPosition::LongAllIn,
            Decimal::from(100),
            &reference,
        )
        .expect_err("non-positive price");

        assert!(matches!(error, EngineError::InvalidState(_)));
        assert!(
            error.to_string().contains("non-positive reference price"),
            "got {error}"
        );
    }

    #[tokio::test]
    async fn live_mode_with_no_target_change_never_touches_the_venue() {
        let (_anchor, warmup, poll_bar) = timeline(Decimal::from(10_000));
        let tempdir = tempdir().expect("tempdir");
        let store = seeded_store(&tempdir, &warmup);
        let source = ScriptedMarketData::new(vec![Vec::new(), vec![poll_bar.clone()]]);
        let venue = RefusingVenue;

        let engine = LiveTradeEngine::new(&source, &store, &store, Some(&venue));
        let summary = engine
            .run(&engine_config(ExecutionMode::Live, "run-live-idle"))
            .await
            .expect("live run");

        assert_eq!(
            summary,
            LiveTradeSummary {
                run_id: "run-live-idle".to_string(),
                processed_bars: 1,
                submitted_orders: 0,
                closed_trades: 0,
                last_processed_open_time_ms: Some(poll_bar.open_time_ms),
            }
        );

        let state = store
            .load_run_state("run-live-idle")
            .expect("load state")
            .expect("state");
        assert_eq!(state.status, RunStatus::Stopped);
        assert_eq!(state.position, PositionState::flat());
        assert!(
            store
                .list_order_events("run-live-idle", 10)
                .expect("events")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dry_run_entry_persists_the_position_before_surfacing_a_journal_failure() {
        let (_anchor, warmup, poll_bar) = timeline(Decimal::from(12_500));
        let tempdir = tempdir().expect("tempdir");
        let store = seeded_store(&tempdir, &warmup);
        let journal = FailingOrderJournal {
            inner: &store,
            saves: Mutex::new(Vec::new()),
        };
        let source = ScriptedMarketData::new(vec![Vec::new(), vec![poll_bar]]);
        let venue = RefusingVenue;

        let engine = LiveTradeEngine::new(&source, &store, &journal, Some(&venue));
        let error = engine
            .run(&engine_config(ExecutionMode::DryRun, "run-journal-fail"))
            .await
            .expect_err("journal failure");

        assert!(matches!(error, EngineError::InvalidState(_)));
        assert!(
            error
                .to_string()
                .contains("journaling the order event failed"),
            "got {error}"
        );

        // The executed entry reached the journal with its position and
        // Running status BEFORE the deferred journaling error propagated:
        // a restart sees the exposure instead of doubling it.
        let entry_qty = Decimal::from_str("0.008").expect("decimal");
        let saves = journal.saves.lock().expect("lock");
        assert!(
            saves.contains(&(RunStatus::Running, entry_qty)),
            "got {saves:?}"
        );
        drop(saves);

        let state = store
            .load_run_state("run-journal-fail")
            .expect("load state")
            .expect("state");
        assert_eq!(state.status, RunStatus::Failed);
        assert_eq!(state.position.qty, entry_qty);
        assert!(
            state
                .last_error
                .as_deref()
                .expect("last error")
                .contains("journaling the order event failed")
        );
    }
}
