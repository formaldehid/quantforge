//! Argument structs, handlers, and helpers for the monitor commands.
//! Temporary home until the monitor split (`cli/monitor`); data,
//! backtest, and trade already live in their own command modules.

use super::common::{ConfirmArgs, MarketArgs, PollArgs, StrategyArgs, SymbolArgs, parse_market};
use anyhow::{Result, bail};
use clap::Args;
use quantforge::SqliteStore;
use quantforge::{
    BinanceSpotClient, MarketDataSource, RunJournalStore, Side, Symbol, TradingVenue,
    ms_to_rfc3339, now_utc_ms, round_down_to_step,
};
use rust_decimal::Decimal;
use std::time::Duration;

#[derive(Args, Debug)]
pub(crate) struct MonitorStatusArgs {
    #[command(flatten)]
    market: MarketArgs,
    #[command(flatten)]
    strategy: StrategyArgs,
    #[arg(long, default_value_t = 10)]
    recent_trades: usize,
}

#[derive(Args, Debug)]
pub(crate) struct MonitorWatchArgs {
    #[command(flatten)]
    market: MarketArgs,
    #[command(flatten)]
    strategy: StrategyArgs,
    #[arg(long, default_value_t = 10)]
    recent_trades: usize,
    #[command(flatten)]
    poll: PollArgs,
}

#[derive(Args, Debug)]
pub(crate) struct MonitorOrdersArgs {
    #[command(flatten)]
    symbol: SymbolArgs,
}

#[derive(Args, Debug)]
pub(crate) struct MonitorTradesArgs {
    #[command(flatten)]
    symbol: SymbolArgs,
    #[arg(long, default_value_t = 20)]
    limit: usize,
}

#[derive(Args, Debug)]
pub(crate) struct MonitorCancelOrderArgs {
    #[command(flatten)]
    symbol: SymbolArgs,
    #[arg(long)]
    order_id: Option<i64>,
    #[arg(long)]
    client_order_id: Option<String>,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

#[derive(Args, Debug)]
pub(crate) struct MonitorClosePositionArgs {
    #[command(flatten)]
    symbol: SymbolArgs,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

pub(crate) async fn handle_monitor_status(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    args: MonitorStatusArgs,
) -> Result<()> {
    let market = parse_market(args.market.symbol, args.market.interval)?;
    let rules = private_client.fetch_symbol_rules(&market.symbol).await?;
    let balances = private_client.account_balances().await?;
    let open_orders = private_client.open_orders(Some(&market.symbol)).await?;
    let trades = private_client
        .recent_trades(&market.symbol, args.recent_trades)
        .await?;
    let run = store.latest_run_for_market(&market, &args.strategy.strategy_name)?;

    println!("symbol: {}", market.symbol);
    println!("base_asset: {}", rules.base_asset);
    println!("quote_asset: {}", rules.quote_asset);
    println!("time_utc_ms: {}", now_utc_ms());

    if let Some(run) = run {
        println!("latest_run_id: {}", run.run_id);
        println!("run_status: {}", run.status.as_str());
        println!(
            "last_processed_open_time: {}",
            run.last_processed_open_time_ms
                .map(ms_to_rfc3339)
                .unwrap_or_else(|| "none".to_string())
        );
        println!("position_qty: {}", run.position.qty);
        println!(
            "entry_price: {}",
            run.position
                .entry_price
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string())
        );
    } else {
        println!("latest_run_id: none");
        println!(
            "hint: runs are selected by --interval and --strategy-name; pass them \
             explicitly if the bot used non-default values"
        );
    }

    println!("balances:");
    for balance in balances.iter().filter(|balance| {
        balance.asset.eq_ignore_ascii_case(&rules.base_asset)
            || balance.asset.eq_ignore_ascii_case(&rules.quote_asset)
    }) {
        println!(
            "  {} free={} locked={}",
            balance.asset, balance.free, balance.locked
        );
    }

    println!("open_orders: {}", open_orders.len());
    for order in open_orders.iter().take(10) {
        println!(
            "  id={:?} side={} status={} qty={} avg_price={}",
            order.order_id,
            order.side,
            order.status.as_str(),
            order
                .executed_qty
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            order
                .average_price()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        );
    }

    println!("recent_trades: {}", trades.len());
    for trade in trades.iter().take(10) {
        println!(
            "  id={} order_id={} side={} qty={} price={} commission={} {} time={}",
            trade.trade_id,
            trade.order_id,
            trade.side,
            trade.qty,
            trade.price,
            trade.commission,
            trade.commission_asset.as_deref().unwrap_or("?"),
            ms_to_rfc3339(trade.time_ms),
        );
    }

    Ok(())
}

pub(crate) async fn handle_monitor_watch(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    args: MonitorWatchArgs,
) -> Result<()> {
    let mut loops = 0usize;
    loop {
        println!("---");
        handle_monitor_status(
            store,
            private_client,
            MonitorStatusArgs {
                market: MarketArgs {
                    symbol: args.market.symbol.clone(),
                    interval: args.market.interval.clone(),
                },
                strategy: StrategyArgs {
                    strategy_name: args.strategy.strategy_name.clone(),
                },
                recent_trades: args.recent_trades,
            },
        )
        .await?;

        loops += 1;
        if args.poll.max_loops.map(|max| loops >= max).unwrap_or(false) {
            break;
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(args.poll.poll_secs)) => {}
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    Ok(())
}

pub(crate) async fn handle_monitor_orders(
    private_client: &BinanceSpotClient,
    args: MonitorOrdersArgs,
) -> Result<()> {
    let symbol = Symbol::new(args.symbol.symbol)?;
    let orders = private_client.open_orders(Some(&symbol)).await?;
    println!("open_orders: {}", orders.len());
    for order in orders {
        print_order(&order);
    }
    Ok(())
}

pub(crate) async fn handle_monitor_trades(
    private_client: &BinanceSpotClient,
    args: MonitorTradesArgs,
) -> Result<()> {
    let symbol = Symbol::new(args.symbol.symbol)?;
    let trades = private_client.recent_trades(&symbol, args.limit).await?;
    println!("recent_trades: {}", trades.len());
    for trade in trades {
        println!(
            "trade_id={} order_id={} side={} qty={} price={} quote_qty={} commission={} {} time={}",
            trade.trade_id,
            trade.order_id,
            trade.side,
            trade.qty,
            trade.price,
            trade.quote_qty,
            trade.commission,
            trade.commission_asset.as_deref().unwrap_or("?"),
            ms_to_rfc3339(trade.time_ms),
        );
    }
    Ok(())
}

pub(crate) async fn handle_monitor_cancel_order(
    private_client: &BinanceSpotClient,
    args: MonitorCancelOrderArgs,
) -> Result<()> {
    let symbol = Symbol::new(args.symbol.symbol)?;
    if !args.confirm.yes {
        println!("No order canceled. Re-run with --yes to execute the cancel.");
        return Ok(());
    }
    let order = private_client
        .cancel_order(&quantforge::CancelOrderRequest {
            symbol,
            order_id: args.order_id,
            client_order_id: args.client_order_id,
        })
        .await?;
    print_order(&order);
    Ok(())
}

pub(crate) async fn handle_monitor_close_position(
    private_client: &BinanceSpotClient,
    args: MonitorClosePositionArgs,
) -> Result<()> {
    let symbol = Symbol::new(args.symbol.symbol)?;
    let rules = private_client.fetch_symbol_rules(&symbol).await?;
    let balances = private_client.account_balances().await?;
    let free_base_qty = balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(&rules.base_asset))
        .map(|balance| balance.free)
        .unwrap_or(Decimal::ZERO);
    let qty = round_quantity_for_rules(free_base_qty, &rules);

    println!("base_asset: {}", rules.base_asset);
    println!("sell_qty: {}", qty);
    if !args.confirm.yes {
        println!("No order sent. Re-run with --yes to execute the market sell.");
        return Ok(());
    }
    if qty <= Decimal::ZERO {
        bail!("no sellable quantity available for {}", rules.base_asset);
    }

    let order = private_client
        .submit_market_order(&quantforge::MarketOrderRequest {
            symbol,
            side: Side::Sell,
            quantity: Some(qty),
            quote_order_qty: None,
            new_client_order_id: Some(format!("manual-close-{}", now_utc_ms())),
        })
        .await?;
    print_order(&order);
    Ok(())
}

pub(crate) fn round_quantity_for_rules(qty: Decimal, rules: &quantforge::SymbolRules) -> Decimal {
    match rules.effective_market_step_size() {
        Some(step_size) => round_down_to_step(qty, step_size),
        None => qty,
    }
}

pub(crate) fn print_order(order: &quantforge::ExchangeOrder) {
    let display_decimal = |value: Option<Decimal>| {
        value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string())
    };
    println!(
        "order: id={:?} client_id={:?} symbol={} side={} status={} executed_qty={} cumulative_quote_qty={} avg_price={}",
        order.order_id,
        order.client_order_id,
        order.symbol,
        order.side,
        order.status.as_str(),
        display_decimal(order.executed_qty),
        display_decimal(order.cumulative_quote_qty),
        display_decimal(order.average_price())
    );
}
