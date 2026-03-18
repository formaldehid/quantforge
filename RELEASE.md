# Release Guide

## Pre-flight

- replace repository placeholders in `Cargo.toml`
- verify the package name is available on crates.io
- update `CHANGELOG.md`
- ensure CI is green

## Local release check

```bash
./scripts/release-check.sh
cargo package --list
cargo publish --dry-run --locked
```

## Publish

Either authenticate locally with `cargo login`, or set `CARGO_REGISTRY_TOKEN`.

```bash
cargo publish --locked
```

## Smoke test

After publishing:

```bash
cargo install quantforge --locked
quantforge --help
```
