use anyhow::Result;
use clap::Parser;
use cli::{
    AppContext, Cli, Command, DataCommand, MonitorCommand, TradeCommand, handle_backtest,
    handle_data_sync, handle_data_validate, handle_monitor_cancel_order,
    handle_monitor_close_position, handle_monitor_orders, handle_monitor_status,
    handle_monitor_trades, handle_monitor_watch, handle_trade_close, handle_trade_run,
};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let ctx = AppContext::init(&cli)?;

    match cli.command {
        Command::Data { command } => match command {
            DataCommand::Sync(args) => handle_data_sync(&ctx, args).await?,
            DataCommand::Validate(args) => handle_data_validate(&ctx, args)?,
        },
        Command::Backtest(args) => handle_backtest(&ctx, args)?,
        Command::Trade { command } => match command {
            TradeCommand::Run(args) => handle_trade_run(&ctx, args).await?,
            TradeCommand::Close(args) => handle_trade_close(&ctx, args).await?,
        },
        Command::Monitor { command } => {
            let private_client = ctx.require_private_client(
                "monitor commands require QF_BINANCE_API_KEY and QF_BINANCE_API_SECRET",
            )?;
            match command {
                MonitorCommand::Status(args) => {
                    handle_monitor_status(&ctx.store, private_client, args).await?
                }
                MonitorCommand::Watch(args) => {
                    handle_monitor_watch(&ctx.store, private_client, args).await?
                }
                MonitorCommand::Orders(args) => handle_monitor_orders(private_client, args).await?,
                MonitorCommand::Trades(args) => handle_monitor_trades(private_client, args).await?,
                MonitorCommand::CancelOrder(args) => {
                    handle_monitor_cancel_order(private_client, args).await?
                }
                MonitorCommand::ClosePosition(args) => {
                    handle_monitor_close_position(private_client, args).await?
                }
            }
        }
    }

    Ok(())
}
