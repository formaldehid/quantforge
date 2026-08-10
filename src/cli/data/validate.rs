//! `quantforge data validate` — check stored candles for duplicates,
//! gaps, ordering, and OHLC sanity; exits non-zero when issues are found.

use crate::cli::common::{MarketArgs, parse_market};
use crate::cli::context::AppContext;
use anyhow::{Context, Result, bail};
use clap::Args;
use quantforge::{CandleQuery, CandleStore, parse_rfc3339_to_ms, validate_candles};

#[derive(Args, Debug)]
pub(crate) struct DataValidateArgs {
    #[command(flatten)]
    market: MarketArgs,
    #[arg(long)]
    start: Option<String>,
    #[arg(long)]
    end: Option<String>,
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
