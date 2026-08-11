//! `quantforge trade close` — manual position exit with the confirmation
//! flow, journal-deferral ordering, and dust write-off rules.

use crate::cli::common::{
    ConfirmArgs, MarketArgs, StrategyArgs, parse_market, print_order, round_quantity_for_rules,
};
use crate::cli::context::AppContext;
use anyhow::{Result, anyhow, bail};
use clap::Args;
use quantforge::{
    ClosedTrade, ExecutionMode, MarketDataSource, PositionState, RunJournalStore, Side,
    TradingVenue, now_utc_ms,
};
use rust_decimal::Decimal;
use tracing::warn;

#[derive(Args, Debug)]
pub(crate) struct TradeCloseArgs {
    #[command(flatten)]
    market: MarketArgs,
    #[command(flatten)]
    strategy: StrategyArgs,
    #[arg(long)]
    run_id: Option<String>,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

pub(crate) async fn handle_trade_close(ctx: &AppContext, args: TradeCloseArgs) -> Result<()> {
    let store = &ctx.store;
    // Credential gate before parsing, preserving the pre-split dispatch
    // ordering.
    let private_client = ctx.require_private_client(
        "trade close requires QF_BINANCE_API_KEY and QF_BINANCE_API_SECRET",
    )?;
    let market = parse_market(args.market.symbol, args.market.interval)?;
    let rules = private_client.fetch_symbol_rules(&market.symbol).await?;
    let mut run_state = if let Some(run_id) = args.run_id {
        store
            .load_run_state(&run_id)?
            .ok_or_else(|| anyhow!("no run found for run_id={run_id}"))?
    } else {
        store
            .latest_run_for_market(&market, &args.strategy.strategy_name)?
            .ok_or_else(|| {
                anyhow!(
                    "no run found for market={} interval={} strategy={}; runs are selected \
                     by --interval and --strategy-name, so pass them explicitly if the bot \
                     used non-default values",
                    market.symbol,
                    market.interval,
                    args.strategy.strategy_name
                )
            })?
    };

    if run_state.execution_mode == ExecutionMode::DryRun {
        bail!(
            "run {} was recorded in dry-run mode; its position is synthetic and there is \
             nothing to close on the exchange",
            run_state.run_id
        );
    }

    let balances = private_client.account_balances().await?;
    let free_base_qty = balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(&rules.base_asset))
        .map(|balance| balance.free)
        .unwrap_or(Decimal::ZERO);

    let qty = round_quantity_for_rules(free_base_qty.min(run_state.position.qty), &rules);
    println!("run_id: {}", run_state.run_id);
    println!("sell_qty: {}", qty);
    if !args.confirm.yes {
        println!("No order sent. Re-run with --yes to execute the market sell.");
        return Ok(());
    }
    if qty <= Decimal::ZERO {
        bail!("no sellable quantity available for {}", rules.base_asset);
    }
    if run_state.position.is_open() && run_state.position.entry_price.is_none() {
        bail!(
            "run {} has no recorded entry price; closing it here would fabricate PnL. \
             Use `monitor close-position --symbol {} --yes` to sell without writing a trade record",
            run_state.run_id,
            market.symbol
        );
    }

    let order = private_client
        .submit_market_order(&quantforge::MarketOrderRequest {
            symbol: market.symbol.clone(),
            side: Side::Sell,
            quantity: Some(qty),
            quote_order_qty: None,
            new_client_order_id: Some(format!("manual-close-{}", now_utc_ms())),
        })
        .await?;

    // Journal the order event, but defer any journaling failure until the
    // position mutation below is persisted: the executed sell must be
    // reflected in run state even when the journal write fails.
    let order_event_result = store
        .append_order_event(&run_state.run_id, &order)
        .map_err(|err| {
            anyhow!(
                "order executed but journaling the order event failed ({err}); \
                 reconcile manually with `monitor status`"
            )
        });

    let executed_qty = order.executed_qty.ok_or_else(|| {
        anyhow!(
            "exchange did not report an executed quantity for order {:?}; run state left \
             unchanged — reconcile manually with `monitor status`",
            order.order_id
        )
    })?;

    if run_state.position.is_open() && executed_qty > Decimal::ZERO {
        // The sell has executed, so position state is updated and persisted
        // before anything below can fail; a missing fill price then aborts
        // with a clear error instead of recording fabricated PnL.
        let position_before = run_state.position.clone();
        let closed_qty = executed_qty.min(position_before.qty);
        let remaining_qty = (position_before.qty - closed_qty).max(Decimal::ZERO);
        let tradeable_remnant = round_quantity_for_rules(remaining_qty, &rules);
        let remnant_is_dust = remaining_qty > Decimal::ZERO
            && (tradeable_remnant <= Decimal::ZERO
                || rules
                    .effective_market_min_qty()
                    .map(|min_qty| tradeable_remnant < min_qty)
                    .unwrap_or(false));
        run_state.updated_at_ms = now_utc_ms();
        run_state.last_error = None;

        if remaining_qty > Decimal::ZERO && !remnant_is_dust {
            run_state.position.qty = remaining_qty;
            run_state.status = quantforge::RunStatus::Running;
            run_state.stopped_at_ms = None;
        } else {
            if remnant_is_dust {
                warn!(
                    written_off_qty = %remaining_qty,
                    "position remnant after close is below the tradeable minimum; \
                     writing it off so the run does not wedge on unsellable dust"
                );
            }
            run_state.position = PositionState::flat();
            run_state.status = quantforge::RunStatus::Stopped;
            run_state.stopped_at_ms = Some(run_state.updated_at_ms);
        }
        store.save_run_state(&run_state)?;
        order_event_result?;

        let entry_price = position_before.entry_price.ok_or_else(|| {
            anyhow!(
                "run {} lost its recorded entry price; position state was updated but no \
                 closed-trade row was written",
                run_state.run_id
            )
        })?;
        let exit_price = order.average_price().ok_or_else(|| {
            anyhow!(
                "exchange did not report a fill price for order {:?}; position state was \
                 updated but no closed-trade row was written because its PnL would be fabricated",
                order.order_id
            )
        })?;

        let trade = ClosedTrade {
            symbol: market.symbol.clone(),
            entry_time_ms: position_before
                .entry_time_ms
                .or(order.transact_time_ms)
                .unwrap_or_else(now_utc_ms),
            exit_time_ms: order.transact_time_ms.unwrap_or_else(now_utc_ms),
            entry_price,
            exit_price,
            qty: closed_qty,
            gross_quote_pnl: (exit_price - entry_price) * closed_qty,
            entry_order_id: position_before.entry_order_id,
            exit_order_id: order.order_id,
        };
        store.append_closed_trade(&run_state.run_id, &trade)?;
    } else {
        order_event_result?;
    }

    print_order(&order);
    Ok(())
}
