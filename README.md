# Beacon ARCO Data lake Platform (1.4.0)

![GitHub release (latest by date)](https://img.shields.io/github/v/release/maris-development/beacon)

## What is Beacon?

Beacon is a lightweight, high-performance ARCO data lake platform for discovering, reading, transforming, and serving scientific array and tabular datasets. It focuses on interoperability with Arrow and DataFusion, and supports common scientific storage formats (Parquet, NetCDF, Zarr, ODV, CSV, and others). Beacon is designed for:

- Data scientists and engineers who need fast, programmatic access to large gridded or tabular datasets stored locally or in object stores (S3-compatible systems).
- Developers building data services that require efficient columnar reads, pushdown statistics, and integration with DataFusion execution plans.

Key capabilities:

- Format adapters: read and expose data as Arrow arrays from Parquet, NetCDF, Zarr, ODV, CSV, etc.
- Pushdown & partitioning: compute lightweight statistics and partition datasets for efficient query planning.
- Object store friendly: works with local files and S3-like object stores using the `object_store` abstraction.
- HTTP API: optional Axum-based service to expose query endpoints and metadata.
- SQL support: execute SQL queries (via DataFusion) against registered formats and datasets. Beacon integrates DataFusion's SQL engine so callers can run SQL directly through the API.

The repository is organized as a Cargo workspace containing multiple crates that provide format readers, query planning, runtime services and an HTTP API.

This README gives a short overview of the repository layout, descriptions of the main crates, how to build and run tests, and suggested next steps for contributors.

## Documentation

The documentation for Beacon is hosted on GitHub Pages: https://maris-development.github.io/beacon/
It includes guides on setting up datasets, using the API, and details on the architecture and design.

## Getting started

To get started with Beacon, clone the beacon-example repository, which contains an example setup for both local and S3, along with example queries and scripts:

```powershell
git clone https://github.com/maris-development/beacon-example.git
```

Follow the instructions in the `beacon-example/README.md` to set up datasets, run the Beacon API server, and execute example queries.

## Support

For questions, issues, or feature requests, please open an issue on the GitHub repository: https://github.com/maris-development/beacon/issues
We also have a dedicated slack channel for discussions: https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg 

## Workspace overview

Location: repository root (this README)

Key workspace members (see `Cargo.toml`):

- `beacon-api` — HTTP API server exposing query endpoints and OpenAPI/Swagger UI. The API supports submitting SQL queries (DataFusion SQL) and returns Arrow/JSON results.
- `beacon-core` — Core runtime types and orchestration used by services; ties together query planning and execution helpers.
- `beacon-common` — Shared utilities and small helpers used across crates.
- `beacon-config` — Configuration and environment handling.
- `beacon-formats` — File format adapters (Parquet, CSV, Arrow, NetCDF, Zarr, GeoParquet).
- `beacon-arrow-netcdf` — Arrow/NetCDF integration (reader/writer utilities).
- `beacon-arrow-zarr` — Arrow/Zarr integration (reader/writer utilities).
- `beacon-arrow-odv` — Arrow/ODV ASCII integration.
- `beacon-data-lake` — Utilities for working with object stores, dataset discovery and table management.
- `beacon-functions` — User-defined functions and helpers used in query execution.
- `beacon-planner` — Query planner and planning utilities that build execution plans.
- `beacon-query` — Query parsing and translation to planner structures.

Note: the workspace `Cargo.toml` references `beacon-arrow-zarr` and other crates; not all referenced crates may be present locally in this checkout. If you see build errors about missing workspace members, check whether the missing crate exists in a separate repository or submodule.

## Per-crate quick descriptions

These are short summaries to help contributors quickly find where to work:

- `beacon-api/` — An Axum-based HTTP server that exposes Beacon's query interface and metadata endpoints. Integrates with `beacon-core` and registers DataFusion file formats and resolvers.

- `beacon-core/` — Core runtime crate: session/environment scaffolding, runtime utilities, and glue between the API and execution components.

- `beacon-common/` — Small helpers, error types, and utilities (serialization helpers, common types, and small abstractions used across the workspace).

- `beacon-formats/` — Implements DataFusion FileFormat adapters for a range of formats. Notable submodule: `zarr` implements async discovery of Zarr v3 groups and integrates with `zarrs` + `zarrs_object_store` to create partitioned file groups and compute pushdown statistics.

- `beacon-arrow-netcdf/`, `beacon-arrow-odv/` — Adapter crates that expose NetCDF and ODV data as Arrow arrays and schemas.

- `beacon-arrow-zarr/` — Adapter crate that exposes Zarr v3 datasets as Arrow arrays and schemas. Uses `zarrs` and `zarrs_object_store` for low-level Zarr access.

- `beacon-data-lake/` — Utilities to manage datasets on object stores and local file systems, object discovery, and helper functions for scanning.

- `beacon-query/` — Parsing and translation of text queries into planner nodes used by `beacon-planner` and `beacon-core`.

- `beacon-planner/` — Planner that converts parsed queries into DataFusion execution plans and coordinates pushdowns and function dispatch.

There are additional crates and examples in the repo for demos, python bindings (`beacon-py`), and studio tooling (`beacon-studio`). Browse the workspace directories for more details.

## Building

Requirements:

- Rust toolchain: the repository includes a `rust-toolchain` file pinning the Rust version. Use `rustup` to install the correct toolchain.
- Cargo (comes with Rust toolchain).

Build the whole workspace (from repo root):

```powershell
cargo build --workspace
```

Build just one crate (faster):

```powershell
cargo build -p beacon-formats
```

Notes:

- The first build will download and compile dependencies, including any git dependencies referenced in crate manifests (for example `nd-arrow-array`).

## Testing

Run all tests in the workspace:

```powershell
cargo test --workspace
```

Run tests for a single crate:

```powershell
cargo test -p beacon-formats
```

Some tests require access to `test_files/` directories (local object store) and may perform async IO. Use `-- --nocapture` to see printed debug output when running individual tests.

## Linting and formatting

You can run Clippy and rustfmt for code quality checks:

```powershell
cargo clippy --workspace -- -D warnings
cargo fmt --all
```

## Development tips

- Use `cargo test -p <crate> -- --nocapture` when debugging tests that print logs.
- The project uses DataFusion and Arrow heavily; when changing format adapters (e.g., Zarr), update unit tests in `beacon-formats` and consider adding small integration tests that use `object_store::local::LocalFileSystem`.

## Contributing

1. Fork the project and create a feature branch.
2. Run and add tests for any functional change.
3. Keep changes small and focused — run `cargo test -p <crate>` locally before opening a PR.

## Troubleshooting

- Long compile times: use incremental builds and build individual crates when working on a small change.

