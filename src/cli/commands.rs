//! Argument structs, handlers, and helpers for the data, backtest, and
//! monitor commands. Temporary home until their per-command splits
//! (`cli/data`, `cli/backtest`, `cli/monitor`); trade already lives in
//! `cli/trade`.

use super::common::{
    ConfirmArgs, MarketArgs, PollArgs, StrategyArgs, SymbolArgs, parse_market,
    parse_non_negative_decimal, parse_positive_decimal, strategy_config,
};
use super::context::AppContext;
use anyhow::{Context, Result, bail};
use clap::Args;
use quantforge::SqliteStore;
use quantforge::{BacktestConfig, BacktestEngine, DataSyncConfig, DataSyncEngine};
use quantforge::{
    BinanceSpotClient, CandleQuery, CandleStore, MarketDataSource, RunJournalStore, Side, Symbol,
    TradingVenue, ms_to_rfc3339, now_utc_ms, parse_rfc3339_to_ms, round_down_to_step,
    validate_candles,
};
use rust_decimal::Decimal;
use std::time::Duration;

#[derive(Args, Debug)]
pub(crate) struct DataSyncArgs {
    #[command(flatten)]
    market: MarketArgs,
    /// RFC3339 start time. Omit to begin syncing from the current time.
    #[arg(long)]
    start: Option<String>,
    /// RFC3339 end time. Omit to keep syncing indefinitely.
    #[arg(long)]
    end: Option<String>,
    #[arg(long, default_value_t = 1000)]
    limit: u16,
    /// When --end is set, keep polling until that end boundary is reached.
    #[arg(long, default_value_t = false)]
    follow: bool,
    #[command(flatten)]
    poll: PollArgs,
}

#[derive(Args, Debug)]
pub(crate) struct DataValidateArgs {
    #[command(flatten)]
    market: MarketArgs,
    #[arg(long)]
    start: Option<String>,
    #[arg(long)]
    end: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct BacktestArgs {
    #[command(flatten)]
    market: MarketArgs,
    #[arg(long)]
    start: Option<String>,
    #[arg(long)]
    end: Option<String>,
    #[arg(long, default_value_t = 20)]
    fast: usize,
    #[arg(long, default_value_t = 50)]
    slow: usize,
    #[arg(long, default_value = "10000")]
    cash: String,
    #[arg(long, default_value = "10")]
    fee_bps: String,
}

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

pub(crate) async fn handle_data_sync(ctx: &AppContext, args: DataSyncArgs) -> Result<()> {
    let store = &ctx.store;
    let client = &ctx.public_client;
    let market = parse_market(args.market.symbol, args.market.interval)?;
    let engine = DataSyncEngine::new(client, store);
    let summary = engine
        .run(&DataSyncConfig {
            market,
            start_time_ms: args
                .start
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --start")?,
            end_time_ms: args
                .end
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --end")?,
            batch_limit: args.limit,
            follow: args.follow,
            poll_interval: Duration::from_secs(args.poll.poll_secs),
            max_loops: args.poll.max_loops,
        })
        .await?;

    println!("iterations: {}", summary.iterations);
    println!("written: {}", summary.written);
    println!(
        "first_synced_open_time: {}",
        summary
            .first_synced_open_time_ms
            .map(ms_to_rfc3339)
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "last_open_time: {}",
        summary
            .last_open_time_ms
            .map(ms_to_rfc3339)
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

pub(crate) fn handle_data_validate(ctx: &AppContext, args: DataValidateArgs) -> Result<()> {
    let store = &ctx.store;
    let market = parse_market(args.market.symbol, args.market.interval)?;
    let candles = store.load_candles(
        &market,
        CandleQuery {
            start_time_ms: args
                .start
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --start")?,
            end_time_ms: args
                .end
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --end")?,
            limit: None,
        },
    )?;

    let report = validate_candles(&market, &candles);
    println!(
        "market: {} {} {}",
        report.market.exchange, report.market.symbol, report.market.interval
    );
    println!("candles: {}", report.candle_count);
    println!("issues: {}", report.issues.len());
    for (index, issue) in report.issues.iter().take(20).enumerate() {
        println!("  {:02}: {:?}", index, issue);
    }
    if report.issues.len() > 20 {
        println!("  ... ({} more)", report.issues.len() - 20);
    }
    if !report.is_ok() {
        bail!("data validate found {} issue(s)", report.issues.len());
    }
    Ok(())
}

pub(crate) fn handle_backtest(ctx: &AppContext, args: BacktestArgs) -> Result<()> {
    let store = &ctx.store;
    let market = parse_market(args.market.symbol, args.market.interval)?;
    let initial_cash = parse_positive_decimal("--cash", &args.cash)?;
    let fee_bps = parse_non_negative_decimal("--fee-bps", &args.fee_bps)?;
    let candles = store.load_candles(
        &market,
        CandleQuery {
            start_time_ms: args
                .start
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --start")?,
            end_time_ms: args
                .end
                .as_deref()
                .map(parse_rfc3339_to_ms)
                .transpose()
                .context("failed to parse --end")?,
            limit: None,
        },
    )?;

    let mut strategy = strategy_config(args.fast, args.slow)
        .build()
        .context("failed to build strategy")?;
    let engine = BacktestEngine::new(BacktestConfig {
        initial_cash,
        fee_bps,
        close_out_at_end: true,
    });

    let result = engine.run(&market, &candles, strategy.as_mut())?;
    println!("strategy: {}", strategy.name());
    println!("final_equity: {}", result.final_equity);
    println!("total_return_pct: {}", result.total_return_pct);
    println!("max_drawdown_pct: {}", result.max_drawdown_pct);
    println!("trade_count: {}", result.trade_count);
    for trade in result.trades.iter().rev().take(5).rev() {
        println!(
            "trade: entry={} @ {} exit={} @ {} qty={} gross_pnl={}",
            trade.entry_time_ms,
            trade.entry_price,
            trade.exit_time_ms,
            trade.exit_price,
            trade.qty,
            trade.gross_quote_pnl
        );
    }
    Ok(())
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
