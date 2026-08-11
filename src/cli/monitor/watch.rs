//! `quantforge monitor watch` — repeat the status report on an interval
//! until Ctrl-C or `--max-loops`.

use super::status;
use crate::cli::common::{MarketArgs, PollArgs, StrategyArgs};
use anyhow::Result;
use clap::Args;
use quantforge::{BinanceSpotClient, SqliteStore};
use std::time::Duration;

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

pub(crate) async fn handle_monitor_watch(
    store: &SqliteStore,
    private_client: &BinanceSpotClient,
    args: MonitorWatchArgs,
) -> Result<()> {
    let mut loops = 0usize;
    loop {
        println!("---");
        status::render_status(
            store,
            private_client,
            &args.market,
            &args.strategy.strategy_name,
            args.recent_trades,
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
