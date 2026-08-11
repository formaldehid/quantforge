//! `quantforge monitor orders` — list open orders for a symbol.

use crate::cli::common::{SymbolArgs, print_order};
use anyhow::Result;
use clap::Args;
use quantforge::{BinanceSpotClient, Symbol, TradingVenue};

#[derive(Args, Debug)]
pub(crate) struct MonitorOrdersArgs {
    #[command(flatten)]
    symbol: SymbolArgs,
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
