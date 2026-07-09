# Contributing to Beacon

Thanks for your interest in improving Beacon! This guide covers how to get a
local build going and the checks your pull request needs to pass.

Issues and pull requests are welcome on
[GitHub](https://github.com/maris-development/beacon/issues). For larger
changes, please open an issue first so we can discuss the approach.

## Prerequisites

Beacon is a Rust workspace pinned to the toolchain in
[`rust-toolchain`](rust-toolchain) (currently **1.91**); `rustup` picks it up
automatically.

The repository uses a git submodule for the binary format crate, so clone
recursively (or initialise it after cloning):

```bash
git clone --recursive https://github.com/maris-development/beacon.git
# or, in an existing checkout:
git submodule update --init --recursive
```

Some crates link against system libraries. On Debian/Ubuntu the build
dependencies are:

```bash
sudo apt-get install -y \
  build-essential capnproto cmake curl libclang-dev libhdf5-dev \
  libnetcdf-dev libsqlite3-dev netcdf-bin protobuf-compiler sqlite3
```

## Build and test

```bash
cargo build --workspace
cargo test --workspace
```

The admin web UI lives under `clients/` and has its own build; see the
[`Makefile`](Makefile) (`make help`) for convenience targets such as
`make run` (serve the API + UI) and `make dev-ui` (Vite hot-reload).

## Code style and checks

CI runs three gates on every push and pull request
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)); run them locally
before pushing.

### Formatting

All Rust code must be formatted with `rustfmt`. Beacon uses the **default Rust
style** (pinned in [`rustfmt.toml`](rustfmt.toml)), which matches
[Google's Rust style guide](https://google.github.io/styleguide/rust/) — the
guide mandates rustfmt defaults rather than a custom profile.

```bash
cargo fmt --all           # format everything (or: make fmt)
cargo fmt --all -- --check # what CI enforces; fails on unformatted code
```

To catch formatting problems before you commit, install the pre-commit hook
once per clone:

```bash
make hooks   # sets core.hooksPath to .githooks/
```

The hook (`.githooks/pre-commit`) runs the same `cargo fmt --all -- --check`
whenever a commit touches Rust files and rejects unformatted changes. Bypass it
for a single commit with `git commit --no-verify` if you must.

### Lints and tests

```bash
cargo clippy --workspace --lib --bins --tests
cargo test --workspace --no-fail-fast --lib --bins --tests
```

## Pull requests

- Keep the branch focused; unrelated changes are easier to review separately.
- Make sure `cargo fmt --all -- --check`, `cargo clippy`, and `cargo test`
  pass — these are the same checks CI will run.
- Reference any related issue in the PR description.

## License

By contributing, you agree that your contributions are licensed under the
project's [AGPL-3.0 license](LICENSE).
