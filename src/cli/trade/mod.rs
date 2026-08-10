//! `quantforge trade` — run or manually close the strategy bot.

use super::context::AppContext;
use anyhow::Result;
use clap::Subcommand;

mod close;
mod run;

use close::{TradeCloseArgs, handle_trade_close};
use run::{TradeRunArgs, handle_trade_run};

#[derive(Subcommand, Debug)]
pub(crate) enum TradeCommand {
    /// Run the polling strategy bot against SQLite-backed live candles.
    Run(TradeRunArgs),

    /// Close the bot-managed position with a market sell.
    Close(TradeCloseArgs),
}

pub(crate) async fn dispatch(ctx: &AppContext, command: TradeCommand) -> Result<()> {
    match command {
        TradeCommand::Run(args) => handle_trade_run(ctx, args).await,
        TradeCommand::Close(args) => handle_trade_close(ctx, args).await,
    }
}
