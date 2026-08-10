//! `quantforge data sync` — pull klines from Binance into SQLite, bounded
//! or following, resuming from the stored high-water mark.

use crate::cli::common::{MarketArgs, PollArgs, parse_market};
use crate::cli::context::AppContext;
use anyhow::{Context, Result};
use clap::Args;
use quantforge::{DataSyncConfig, DataSyncEngine, ms_to_rfc3339, parse_rfc3339_to_ms};
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
