//! `quantforge backtest` — deterministic bar-by-bar simulation over
//! locally stored candles.
//!
//! A leaf command today; the directory form leaves room for future
//! subcommands (e.g. a parameter sweep) without moving files.

use crate::cli::common::{
    MarketArgs, parse_market, parse_non_negative_decimal, parse_positive_decimal, strategy_config,
};
use crate::cli::context::AppContext;
use anyhow::{Context, Result};
use clap::Args;
use quantforge::{BacktestConfig, BacktestEngine, CandleQuery, CandleStore, parse_rfc3339_to_ms};

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
