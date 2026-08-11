//! `quantforge monitor status` — one-shot report of balances, run state,
//! open orders, and recent trades.

use crate::cli::common::{MarketArgs, StrategyArgs, parse_market};
use anyhow::Result;
use clap::Args;
use quantforge::{
    BinanceSpotClient, MarketDataSource, RunJournalStore, SqliteStore, TradingVenue, ms_to_rfc3339,
    now_utc_ms,
};

#[derive(Args, Debug)]
pub(crate) struct MonitorStatusArgs {
    #[command(flatten)]
    market: MarketArgs,
    #[command(flatten)]
    strategy: StrategyArgs,
    #[arg(long, default_value_t = 10)]
    recent_trades: usize,
}

pub(crate) async fn handle_monitor_status(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    args: MonitorStatusArgs,
) -> Result<()> {
    render_status(
        store,
        private_client,
        &args.market,
        &args.strategy.strategy_name,
        args.recent_trades,
    )
    .await
}

/// Fetches and prints the whole status report. Shared by `monitor status`
/// and every iteration of `monitor watch`.
pub(super) async fn render_status(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    market_args: &MarketArgs,
    strategy_name: &str,
    recent_trades: usize,
) -> Result<()> {
    let market = parse_market(market_args.symbol.clone(), market_args.interval.clone())?;
    let rules = private_client.fetch_symbol_rules(&market.symbol).await?;
    let balances = private_client.account_balances().await?;
    let open_orders = private_client.open_orders(Some(&market.symbol)).await?;
    let trades = private_client
        .recent_trades(&market.symbol, recent_trades)
        .await?;
    let run = store.latest_run_for_market(&market, strategy_name)?;

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
