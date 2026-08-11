//! `quantforge monitor trades` — list recent account trades for a symbol.

use crate::cli::common::SymbolArgs;
use anyhow::Result;
use clap::Args;
use quantforge::{BinanceSpotClient, Symbol, TradingVenue, ms_to_rfc3339};

#[derive(Args, Debug)]
pub(crate) struct MonitorTradesArgs {
    #[command(flatten)]
    symbol: SymbolArgs,
    #[arg(long, default_value_t = 20)]
    limit: usize,
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
