# Beacon ARCO Data lake Platform (1.4.0)

![GitHub release (latest by date)](https://img.shields.io/github/v/release/maris-development/beacon)
[![Docker Image](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://github.com/maris-development/beacon/pkgs/container/beacon)
[![Docs](https://github.com/maris-development/beacon/actions/workflows/pages.yml/badge.svg)](https://maris-development.github.io/beacon/)
[![Chat on Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg)

## Contents

- [What is Beacon?](#what-is-beacon)
- [Platform highlights](#platform-highlights)
- [Documentation](#documentation)
- [Getting started](#getting-started)
- [Install Beacon with Docker Compose](#install-beacon-with-docker-compose)
- [Data Lake model](#data-lake-model)
- [Querying Beacon](#querying-beacon)
- [Support](#support)
- [Workspace overview](#workspace-overview)
- [Per-crate quick descriptions](#per-crate-quick-descriptions)
- [Building](#building)
- [Testing](#testing)
- [Linting and formatting](#linting-and-formatting)
- [Development tips](#development-tips)
- [Contributing](#contributing)
- [Troubleshooting](#troubleshooting)

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

## Platform highlights

- High-performance ARCO data lake built on Rust, Apache Arrow, and DataFusion so you can query millions of datasets with sub-second responsiveness.
- Efficient storage story that spans S3-compatible buckets and local disks, letting you manage large fleets of NetCDF, Zarr, Parquet, and CSV assets without format conversions.
- Flexible query interfaces: REST API with SQL and JSON DSL endpoints, plus a Python SDK for fluent query building and DataFrame integration.

## Documentation

- Landing page: https://maris-development.github.io/beacon/
- Installation (Docker-focused quick start): https://maris-development.github.io/beacon/docs/1.4.0-install/
- Query & data lake reference: https://maris-development.github.io/beacon/docs/1.4.0/query-docs/data-lake.html

## Getting started

To get started with Beacon, clone the beacon-example repository, which contains an example setup for both local and S3, along with example queries and scripts:

```powershell
git clone https://github.com/maris-development/beacon-example.git
```

Follow the instructions in the `beacon-example/README.md` to set up datasets, run the Beacon API server, and execute example queries.

## Install Beacon with Docker Compose

The official installation guide walks through a Docker-based deployment. The short version:

1. Install Docker (desktop or server) and create a `docker-compose.yml` similar to the example below.
2. Adjust the environment variables for your admin credentials, memory budget, default table, log level, host, and port.
3. Mount your dataset and table directories so the container can persist indexed data.
4. Run `docker compose up -d` and open http://localhost:8080/swagger/ to confirm the service is live.

```yaml
version: "3.8"

services:
  beacon:
    image: ghcr.io/maris-development/beacon:community-latest
    container_name: beacon
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      - BEACON_ADMIN_USERNAME=admin
      - BEACON_ADMIN_PASSWORD=securepassword
      - BEACON_VM_MEMORY_SIZE=4096
      - BEACON_DEFAULT_TABLE=default
      - BEACON_LOG_LEVEL=INFO
      - BEACON_HOST=0.0.0.0
      - BEACON_PORT=8080
    volumes:
      - ./data/datasets:/beacon/data/datasets
      - ./data/tables:/beacon/data/tables
```

See the full installation chapter for troubleshooting tips, hardware recommendations, and additional deployment patterns (single Docker container, MinIO-backed storage, etc.).

## Data Lake model

Beacon organizes storage into datasets (raw files) and data tables (named collections):

- **Datasets** — individual NetCDF, Zarr, Parquet, CSV, ODV ASCII, or Arrow IPC assets that can be queried directly via SQL or JSON APIs. You can register many files at once using glob patterns such as `*.nc` or `*/zarr.json` to sweep directories.
- **Data tables** — logical tables that group one or more datasets under a single name (e.g., `2020_2022` created from `2020.nc`, `2021.nc`, `2022.nc`). Tables provide a higher-level schema, so analysts can query an entire collection of datasets as if it were a single table.

The [data lake guide](https://maris-development.github.io/beacon/docs/1.4.0/query-docs/data-lake.html) contains a deeper architectural explanation plus links to tutorials for SQL and JSON queries, language SDKs, and the Beacon Studio web UI.

## Querying Beacon

### SQL endpoint

Use the `/api/query` REST endpoint to run ANSI SQL over registered tables or directly over datasets:

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  --output results.parquet \
  --data-binary @- <<'JSON'
{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM observations WHERE time > TIMESTAMP '2020-01-01'",
  "output": {"format": "parquet"}
}
JSON
```

- Works for both local paths and S3/object-store URIs registered in your configuration.
- Output formats include Arrow IPC, Parquet, CSV, NetCDF, GeoParquet, GeoJSON, and ODV—set via `output.format`.

### SQL dataset helpers

Read individual collections without pre-registering tables using helper functions documented in the [SQL guide](https://maris-development.github.io/beacon/docs/1.4.0/query-docs/querying/sql.html):

- `read_zarr(['dataset.zarr/zarr.json'], ['LONGITUDE','LATITUDE'])` — optional second argument pre-fetches columns to enable pushdown filtering (recommended for large cubes).
- `read_netcdf(['dataset.nc'])` — exposes NetCDF variables and attributes directly as columns.
- `read_parquet(['dataset.parquet'])` — benefits from predicate pushdown automatically.

Example with spatial pushdown:

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  --output subset.parquet \
  --data-binary @- <<'JSON'
{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM read_zarr(['datasets.zarr/zarr.json'], ['LONGITUDE', 'LATITUDE']) WHERE LONGITUDE > 10 AND LATITUDE < 50",
  "output": {"format": "parquet"}
}
JSON
```

### Python SDK

Use the official [beacon-py](https://maris-development.github.io/beacon-py/latest/) client when you prefer fluent builders and direct access to DataFrames/GeoDataFrames.

```bash
pip install beacon-api
```

```python
from beacon_api import Client

client = Client(
    "https://beacon.example.com",
    jwt_token="<optional bearer token>",
)

client.check_status()

tables = client.list_tables()
stations = tables["default"]

df = (
    stations
    .query()
    .add_select_columns([
        ("LONGITUDE", None),
        ("LATITUDE", None),
        ("JULD", None),
        ("TEMP", "temperature_c"),
    ])
    .add_range_filter("JULD", "2024-01-01T00:00:00", "2024-12-31T23:59:59")
    .to_pandas_dataframe()
)
```

- `Client.list_tables()` and `Client.list_datasets()` expose metadata/schemas before you build queries.
- The fluent builder covers selects, filters (min/max, geospatial, distinct), ordering, and export helpers such as `to_geo_pandas_dataframe()` or `to_parquet()`.
- Prefer `client.sql_query("SELECT ...")` if you already have SQL strings and want the SDK to manage authentication + retries.

### JSON query DSL

The JSON DSL is useful for UI integrations or when you want structured column definitions, filtering, and output control. The request body follows the schema documented in [Querying with JSON](https://maris-development.github.io/beacon/docs/1.4.0/query-docs/querying/json.html):

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  --data-binary @- <<'JSON'
{
  "query_parameters": [
    {"column_name": "TEMP", "alias": "temperature"},
    {"column_name": "PSAL", "alias": "salinity"},
    {"column_name": "TIME"},
    {"column_name": "LONGITUDE"},
    {"column_name": "LATITUDE"}
  ],
  "filters": [
    {"for_query_parameter": "temperature", "min": -2, "max": 35},
    {"for_query_parameter": "salinity", "min": 30, "max": 42},
    {"and": [
      {"for_query_parameter": "LONGITUDE", "min": -20, "max": 20},
      {"for_query_parameter": "LATITUDE", "min": 40, "max": 65}
    ]}
  ],
  "from": {
    "netcdf": {"paths": ["data/2020.nc", "data/2021.nc"]}
  },
  "output": {"format": "csv"}
}
JSON
```

- Discover available columns via `GET /api/query/available-columns` (requires the same auth token, if any).
- Filters support min/max ranges, equality, polygon bounds, time windows, null filtering, and logical `and`/`or` composition.
- Set `distinct` to deduplicate values or supply GeoJSON/GeoParquet metadata when you need geospatial outputs.

### Output formats

Return results in the format that best matches your downstream tooling by adjusting `output.format`:

- `parquet` / `geoparquet`
- `ipc` (Arrow IPC streaming)
- `csv`
- `netcdf`
- `geojson`
- `odv` (with support for key columns, quality flags, and metadata columns as described in the JSON guide)

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
- `beacon-binary-format` — Beacon Binary Format (BBF) for efficient storage of multi-million-dataset collections. (Will also become an exchange format in future releases.)
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
