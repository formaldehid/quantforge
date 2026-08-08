use anyhow::Result;
use clap::Parser;
use cli::Cli;

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    cli::run(Cli::parse()).await
}
