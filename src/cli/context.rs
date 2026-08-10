//! Shared per-invocation construction: tracing, the SQLite store, and the
//! Binance clients — built once from the parsed [`Cli`] and handed to the
//! handlers, plus the interactive confirmation helpers.

use super::Cli;
use anyhow::{Context, Result, anyhow};
use quantforge::{BinanceCredentials, BinanceSpotClient, CandleStore, SqliteStore};
use std::io::{self, Write};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

/// Everything a command handler needs, constructed once per invocation.
#[derive(Debug)]
pub(crate) struct AppContext {
    pub(crate) store: SqliteStore,
    pub(crate) base_url: Url,
    pub(crate) public_client: BinanceSpotClient,
    pub(crate) private_client: Option<BinanceSpotClient>,
}

impl AppContext {
    /// Build the context in startup order: tracing first (so the store
    /// warnings can be emitted), then the database, then the clients.
    pub(crate) fn init(cli: &Cli) -> Result<Self> {
        init_tracing(&cli.log_level)?;

        let db_existed = cli.db.exists();
        if let Some(parent) = cli.db.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create parent directory {}", parent.display())
            })?;
        }

        let store = SqliteStore::new(&cli.db);
        CandleStore::init(&store).context("failed to initialize sqlite store")?;
        if !db_existed {
            warn!(
                db_path = %cli.db.display(),
                "database did not exist and was created empty; if you expected existing data \
                 (run state, candles), check the --db path or QF_DB"
            );
        }

        let base_url =
            Url::parse(&cli.binance_base_url).context("failed to parse --binance-base-url")?;
        info!(base_url = %display_url(&base_url), "using Binance base URL");
        let public_client = BinanceSpotClient::new(base_url.clone());
        let private_client = BinanceCredentials::from_env().map(|credentials| {
            BinanceSpotClient::new(base_url.clone()).with_credentials(credentials)
        });

        Ok(Self {
            store,
            base_url,
            public_client,
            private_client,
        })
    }

    /// The authenticated client, or `error_message` naming the missing
    /// credentials — callers pass their command's exact tested wording.
    pub(crate) fn require_private_client(
        &self,
        error_message: &'static str,
    ) -> Result<&BinanceSpotClient> {
        self.private_client
            .as_ref()
            .ok_or_else(|| anyhow!(error_message))
    }
}

fn init_tracing(level: &str) -> Result<()> {
    let env_filter = EnvFilter::try_new(level)
        .with_context(|| format!("invalid log level/filter expression: {level}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
    Ok(())
}

/// Prints `prompt`, reads one stdin line, and applies the strict `yes`
/// check. The caller decides whether the session is interactive and what
/// preview text precedes the prompt.
pub(crate) fn prompt_confirmation(prompt: &str) -> Result<bool> {
    print!("{prompt}");
    io::stdout()
        .flush()
        .context("failed to flush confirmation prompt")?;
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("failed to read live-trading confirmation")?;
    Ok(confirmation_is_yes(&input))
}

pub(crate) fn display_url(url: &Url) -> Url {
    let mut redacted = url.clone();
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted
}

fn confirmation_is_yes(input: &str) -> bool {
    input.trim().eq_ignore_ascii_case("yes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_url_strips_userinfo() {
        let url = Url::parse("https://user:secret@api.binance.com/").expect("url");
        assert_eq!(display_url(&url).as_str(), "https://api.binance.com/");
    }

    #[test]
    fn live_confirmation_accepts_only_yes() {
        for input in ["yes", "YES", " yes \n", "Yes"] {
            assert!(confirmation_is_yes(input), "for input {input:?}");
        }
        for input in ["", "y", "no", "live", "yes please"] {
            assert!(!confirmation_is_yes(input), "for input {input:?}");
        }
    }
}
