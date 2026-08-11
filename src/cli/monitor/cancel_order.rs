//! `quantforge monitor cancel-order` — cancel one order manually, behind
//! the `--yes` confirmation gate.

use crate::cli::common::{ConfirmArgs, SymbolArgs, print_order};
use anyhow::Result;
use clap::Args;
use quantforge::{BinanceSpotClient, Symbol, TradingVenue};

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
