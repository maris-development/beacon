# Beacon — ARCO Data Lakehouse Query Engine

[![Release](https://img.shields.io/github/v/release/maris-development/beacon?label=release&color=success)](https://github.com/maris-development/beacon/releases)
[![Docs](https://img.shields.io/github/actions/workflow/status/maris-development/beacon/pages.yml?label=docs)](https://maris-development.github.io/beacon/)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-2496ED?logo=docker&logoColor=white)](https://github.com/maris-development/beacon/pkgs/container/beacon)
[![License](https://img.shields.io/github/license/maris-development/beacon)](LICENSE)
[![Slack](https://img.shields.io/badge/slack-join-4A154B?logo=slack&logoColor=white)](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg)

Beacon is a lightweight, high-performance **data lakehouse query engine** for scientific data. It lets you discover, read, transform, and serve large collections of array and tabular datasets **in place** — no copying into a warehouse, no rigid ETL pipeline. Point Beacon at a directory or object-storage bucket of files and query them directly over HTTP, with results streamed back in the format you ask for.

It is built on [Apache Arrow](https://arrow.apache.org/) and [Apache DataFusion](https://datafusion.apache.org/), so queries run on a columnar, vectorized engine while reading native scientific formats such as NetCDF, Zarr, Parquet, and ODV.

## Table of contents

- [Why Beacon](#why-beacon)
- [Features](#features)
- [Concepts](#concepts)
- [Quick start (Docker)](#quick-start-docker)
- [Query examples](#query-examples)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Why Beacon

- **Query files where they live.** Read NetCDF, Zarr, Parquet, ODV, CSV and more directly from a local volume or S3-compatible object store — no ingestion step.
- **One API, many formats in and out.** Send a SQL or JSON query, choose your output format (Parquet, CSV, NetCDF, GeoParquet, Arrow IPC, ODV) and stream the result.
- **Built for scale.** Columnar execution, predicate/projection pushdown, and statistics-based pruning on top of Arrow + DataFusion.
- **Self-describing.** A built-in OpenAPI/Swagger UI documents every endpoint, and discovery endpoints expose available datasets, tables, columns, and functions.

## Features

- **Input formats:** Parquet, GeoParquet, NetCDF, Zarr, Atlas, ODV, CSV, Arrow IPC, GeoTIFF, Delta Lake, and the native Beacon Binary Format (BBF).
- **Output formats:** Parquet, GeoParquet, NetCDF, ND-NetCDF, CSV, Arrow IPC, and ODV.
- **Two query interfaces:** a structured **JSON query** API and read-only raw **SQL** (enabled by default; toggle with `BEACON_ENABLE_SQL`).
- **Arrow Flight SQL** endpoint for high-throughput clients (enabled by default).
- **Storage backends:** local filesystem and S3-compatible object storage, with optional change-event watching.
- **Interactive API docs** via Swagger UI (`/swagger`) and Scalar (`/scalar`).

## Concepts

- **Datasets** — the raw source files you make available to Beacon (e.g. `.nc`, `.zarr`, `.parquet`, `.csv`). Drop them into the mounted datasets directory and Beacon discovers them automatically.
- **Tables** — named, queryable collections defined over one or more datasets, stored in the tables directory. A configurable **default table** (`BEACON_DEFAULT_TABLE`) is queried when no source is specified.
- **Source functions** — table functions such as `read_netcdf(...)`, `read_parquet(...)`, and `read_csv(...)` let a query read specific files directly, without first defining a table.
- **Query engine** — every request is parsed into a DataFusion logical plan and executed on the Arrow columnar engine, then encoded into the requested output format and streamed back.

See the [documentation](https://maris-development.github.io/beacon/) for the full data model.

## Quick start (Docker)

The fastest way to try Beacon is a single `docker run`. Run it from the directory where you want the `datasets` and `tables` folders to live:

```bash
docker run -d \
  --name beacon \
  -p 5001:5001 \
  -p 32011:32011 \
  -e BEACON_ADMIN_USERNAME=admin \
  -e BEACON_ADMIN_PASSWORD=securepassword \
  -v ./datasets:/beacon/data/datasets \
  -v ./tables:/beacon/data/tables \
  ghcr.io/maris-development/beacon:latest
```

This maps the HTTP API (`5001`) and Arrow Flight SQL (`32011`) ports, sets the admin credentials, and mounts a local `./datasets` directory of files to query (plus an empty `./tables` directory you can omit if you won't use tables).

### Docker Compose

For a reproducible setup, use Compose instead:

```yaml
services:
  beacon:
    image: ghcr.io/maris-development/beacon:latest
    container_name: beacon
    restart: unless-stopped
    ports:
      - "5001:5001" # HTTP API
      - "32011:32011" # Arrow Flight SQL
    environment:
      - BEACON_ADMIN_USERNAME=admin
      - BEACON_ADMIN_PASSWORD=securepassword
    volumes:
      - ./datasets:/beacon/data/datasets # Mount a local directory of files to query
      - ./tables:/beacon/data/tables # Mount an empty directory for tables, or omit if you won't use them
```

Start it with `docker compose up -d`, then open the interactive API docs at <http://localhost:5001/swagger/>.

Add data by placing files (e.g. `.nc`, `.zarr`, `.parquet`, `.csv`) into `./datasets` — the container discovers them through the mounted volume.

> Prefer a native build or a non-Docker install? See the [installation guide](https://maris-development.github.io/beacon/docs/1.7.3/getting-started.html#local).

## Query examples

Both examples below post to the same endpoint and stream back a file in the requested output format.

### SQL

> SQL is read-only and enabled by default. Set `BEACON_ENABLE_SQL=false` to disable it.

```http
POST http://localhost:5001/api/query
Content-Type: application/json

{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM read_netcdf(['data/2020.nc', 'data/2021.nc']) WHERE time > '2020-01-01T00:00:00'",
  "output": { "format": "parquet" }
}
```

### JSON

The JSON query API is always available and needs no extra configuration.

```http
POST http://localhost:5001/api/query
Content-Type: application/json

{
  "query_parameters": [
    { "column_name": "TEMP", "alias": "temperature" },
    { "column_name": "PSAL", "alias": "salinity" },
    { "column_name": "TIME" },
    { "column_name": "LONGITUDE" },
    { "column_name": "LATITUDE" }
  ],
  "filters": [
    { "for_query_parameter": "temperature", "min": -2, "max": 35 },
    { "for_query_parameter": "salinity", "min": 30, "max": 42 },
    {
      "and": [
        { "for_query_parameter": "LONGITUDE", "min": -20, "max": 20 },
        { "for_query_parameter": "LATITUDE", "min": 40, "max": 65 }
      ]
    }
  ],
  "from": {
    "netcdf": { "paths": ["data/2020.nc", "data/2021.nc"] }
  },
  "output": { "format": "csv" }
}
```

The response is a streamed file in the chosen `output.format` (here, CSV). See the [query reference](https://maris-development.github.io/beacon/docs/1.7.3/api/querying/) for the full schema, all source types, and every output format.

## Configuration

Beacon is configured entirely through `BEACON_*` environment variables. The most common ones:

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_HOST` | `0.0.0.0` | Address the HTTP server binds to. |
| `BEACON_PORT` | `5001` | HTTP server port. |
| `BEACON_ADMIN_USERNAME` | `beacon-admin` | Admin username for management endpoints. |
| `BEACON_ADMIN_PASSWORD` | `beacon-password` | Admin password — **change this in production**. |
| `BEACON_LOG_LEVEL` | `info` | Log verbosity (`trace`, `debug`, `info`, `warn`, `error`). |
| `BEACON_VM_MEMORY_SIZE` | `8192` | Working memory (MB) available to the query engine. |
| `BEACON_DEFAULT_TABLE` | `default` | Table queried when a request specifies no source. |
| `BEACON_WORKER_THREADS` | `8` | Number of query worker threads. |
| `BEACON_ENABLE_SQL` | `true` | Enable the read-only raw SQL query interface. |
| `BEACON_FLIGHT_SQL_ENABLE` | `true` | Enable the Arrow Flight SQL endpoint. |
| `BEACON_FLIGHT_SQL_PORT` | `32011` | Arrow Flight SQL port. |

S3-compatible storage, CORS, NetCDF caching, the crawler, and Flight SQL authentication have their own `BEACON_*` settings — see the [configuration reference](https://maris-development.github.io/beacon/docs/1.7.3/data-lake/configuration.html) for the complete list.

## Documentation

- Docs home: <https://maris-development.github.io/beacon/>
- Getting started: <https://maris-development.github.io/beacon/docs/1.7.3/getting-started.html#local>
- Query reference: <https://maris-development.github.io/beacon/docs/1.7.3/api/querying/>
- Community Slack: [join here](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg)

## Contributing

Beacon is a Rust workspace. To build and test from source:

```bash
git clone https://github.com/maris-development/beacon.git
cd beacon
cargo build --release
cargo test
```

Issues and pull requests are welcome on [GitHub](https://github.com/maris-development/beacon/issues). For larger changes, please open an issue first to discuss the approach.

## License

Beacon is licensed under the **GNU Affero General Public License v3.0** (AGPL-3.0). See [LICENSE](LICENSE) for the full text.
