//! `quantforge trade run` — the polling strategy bot, dry-run by default;
//! live mode sits behind the confirmation and credential gates.

use crate::cli::common::{
    CliExecutionMode, ConfirmArgs, MarketArgs, parse_market, parse_positive_decimal,
    strategy_config,
};
use crate::cli::context::{self, AppContext, display_url};
use anyhow::{Result, anyhow};
use clap::Args;
use quantforge::{LiveTradeConfig, LiveTradeEngine, TradingVenue, ms_to_rfc3339};
use std::io::{self, IsTerminal};
use std::time::Duration;
use tracing::warn;
use url::Url;

#[derive(Args, Debug)]
pub(crate) struct TradeRunArgs {
    #[command(flatten)]
    market: MarketArgs,
    #[arg(long, default_value_t = 20)]
    fast: usize,
    #[arg(long, default_value_t = 50)]
    slow: usize,
    #[arg(long, default_value = "100")]
    quote_order_qty: String,
    #[arg(long, value_enum, default_value_t = CliExecutionMode::DryRun)]
    mode: CliExecutionMode,
    #[command(flatten)]
    confirm: ConfirmArgs,
    // poll_secs/max_loops stay raw here: they are not adjacent in this
    // struct, and flattening PollArgs would reorder `trade run --help`.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    poll_secs: u64,
    #[arg(long, default_value_t = 300)]
    bootstrap_bars: usize,
    #[arg(long, default_value_t = false)]
    bootstrap_enter: bool,
    #[arg(long, default_value_t = 1000)]
    limit: u16,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    max_loops: Option<usize>,
}

pub(crate) async fn handle_trade_run(ctx: &AppContext, args: TradeRunArgs) -> Result<()> {
    let store = &ctx.store;
    let public_client = &ctx.public_client;
    let private_client = ctx.private_client.as_ref();
    let base_url = &ctx.base_url;
    let market = parse_market(args.market.symbol, args.market.interval)?;
    let quote_order_qty = parse_positive_decimal("--quote-order-qty", &args.quote_order_qty)?;

    if matches!(args.mode, CliExecutionMode::Live) {
        if !args.confirm.yes {
            let strategy_name = strategy_config(args.fast, args.slow).strategy_name();
            let url_note = if is_production_binance_url(base_url) {
                " (PRODUCTION)"
            } else {
                ""
            };
            let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
            if !interactive {
                println!("refusing to start live trading without --yes");
                println!(
                    "would run strategy {} on {} {} {}",
                    strategy_name, market.exchange, market.symbol, market.interval
                );
                println!("with REAL orders via {}{}", display_url(base_url), url_note);
                if args.bootstrap_enter {
                    println!("note: --bootstrap-enter may place an order on the first loop");
                }
                println!("re-run with --yes to confirm, or use --mode dry-run");
                return Ok(());
            }
            println!("about to start live trading:");
            println!(
                "strategy {} on {} {} {}",
                strategy_name, market.exchange, market.symbol, market.interval
            );
            println!("REAL orders via {}{}", display_url(base_url), url_note);
            if args.bootstrap_enter {
                println!("note: --bootstrap-enter may place an order on the first loop");
            }
            if !context::prompt_confirmation("type 'yes' to confirm, anything else aborts: ")? {
                println!("aborted; no orders sent");
                return Ok(());
            }
        }
        if is_production_binance_url(base_url) {
            warn!(
                base_url = %display_url(base_url),
                "live trading against PRODUCTION Binance; real funds at risk"
            );
        }
    }

    let engine = LiveTradeEngine::new(
        public_client,
        store,
        store,
        if matches!(args.mode, CliExecutionMode::Live) {
            Some(
                private_client
                    .ok_or_else(|| anyhow!("trade run --mode live requires Binance credentials"))?
                    as &dyn TradingVenue,
            )
        } else {
            None
        },
    );

    let summary = engine
        .run(&LiveTradeConfig {
            market,
            strategy: strategy_config(args.fast, args.slow),
            execution_mode: args.mode.into(),
            quote_order_qty,
            poll_interval: Duration::from_secs(args.poll_secs),
            bootstrap_bars: args.bootstrap_bars,
            bootstrap_enter: args.bootstrap_enter,
            batch_limit: args.limit,
            run_id: args.run_id,
            max_loops: args.max_loops,
        })
        .await?;

    println!("run_id: {}", summary.run_id);
    println!("processed_bars: {}", summary.processed_bars);
    println!("submitted_orders: {}", summary.submitted_orders);
    println!("closed_trades: {}", summary.closed_trades);
    println!(
        "last_processed_open_time: {}",
        summary
            .last_processed_open_time_ms
            .map(ms_to_rfc3339)
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

const PRODUCTION_BINANCE_DOMAINS: [&str; 2] = ["binance.com", "binance.us"];

fn is_production_binance_url(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    PRODUCTION_BINANCE_DOMAINS.iter().any(|domain| {
        host == *domain
            || host
                .strip_suffix(domain)
                .is_some_and(|prefix| prefix.ends_with('.'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_classifier_matches_production_hosts() {
        for raw in [
            "https://api.binance.com/",
            "https://api1.binance.com/",
            "https://api2.binance.com/",
            "https://api3.binance.com/",
            "https://api4.binance.com/",
            "https://api-gcp.binance.com/",
            "https://api5.binance.com/",
            "https://api.binance.us/",
            "https://binance.com/",
            "https://API.BINANCE.COM/",
            "https://api.binance.com:8443/",
            "https://api.binance.com./",
            "http://api1.binance.com/some/path",
        ] {
            let url = Url::parse(raw).expect("url");
            assert!(is_production_binance_url(&url), "for input {raw:?}");
        }
    }

    #[test]
    fn production_classifier_rejects_testnet_local_and_lookalike_hosts() {
        for raw in [
            "https://testnet.binance.vision/",
            "https://data-api.binance.vision/",
            "http://127.0.0.1:9/",
            "https://evil-binance.com/",
            "https://api.binance.com.evil.example/",
            "file:///tmp/no-host",
        ] {
            let url = Url::parse(raw).expect("url");
            assert!(!is_production_binance_url(&url), "for input {raw:?}");
        }
    }
}
