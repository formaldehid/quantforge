# Contributing

Thanks for contributing to QuantForge.

## Development principles

- keep the CLI deterministic and scriptable
- avoid `unwrap` in production paths
- prefer explicit data validation over silent coercion
- document public APIs and CLI changes
- add tests for every bug fix and user-facing behavior change

## Tooling

QuantForge currently targets Rust 1.85 or newer.

## Before you open a pull request

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features --locked
./scripts/release-check.sh
```

## Commit style

Small, reviewable commits are preferred.

Examples:

- `feat(cli): add download resume mode`
- `fix(storage): commit sqlite transaction after statement drop`
- `docs(readme): clarify determinism contract`

## Running the e2e tiers

Plain `cargo test` runs the offline tier (mock exchange, zero network).
Testnet-tier tests run only when `QF_BINANCE_API_KEY` and
`QF_BINANCE_API_SECRET` (Binance Spot *testnet* keys) are exported;
otherwise they skip with a `SKIP (testnet tier)` marker on stderr. Run one
tier alone with `cargo test --test e2e offline::` or
`cargo test --test e2e testnet::`.

## Security and secrets

Do not commit API keys, secrets, `.env` files, or production credentials.

Every pull request is scanned for leaked secrets with gitleaks. To run the same
scan locally over the committed history:

```bash
gitleaks git .
```

The `git` subcommand scans commits only, so untracked files such as your local
`.env` are never read.
