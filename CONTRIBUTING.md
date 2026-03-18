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

## Security and secrets

Do not commit API keys, secrets, `.env` files, or production credentials.
