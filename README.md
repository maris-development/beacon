# Beacon: a query engine for scientific data

[![Release](https://img.shields.io/github/v/release/maris-development/beacon?label=release&color=success)](https://github.com/maris-development/beacon/releases)
[![Docs](https://img.shields.io/github/actions/workflow/status/maris-development/beacon/pages.yml?label=docs)](https://maris-development.github.io/beacon/)
[![codecov](https://codecov.io/gh/maris-development/beacon/branch/main/graph/badge.svg)](https://codecov.io/gh/maris-development/beacon)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-2496ED?logo=docker&logoColor=white)](https://github.com/maris-development/beacon/pkgs/container/beacon)
[![License](https://img.shields.io/github/license/maris-development/beacon)](LICENSE)
[![Slack](https://img.shields.io/badge/slack-join-4A154B?logo=slack&logoColor=white)](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg)

Beacon is a small and fast query engine for scientific data. It reads large collections of array datasets and tabular datasets. Beacon keeps the files in place. You do not copy the files into a warehouse. You do not build an ETL pipeline. Point Beacon to a directory or to an object storage bucket. Then query the files over HTTP. Beacon streams the results in the format that you request.

Beacon uses [Apache Arrow](https://arrow.apache.org/) and [Apache DataFusion](https://datafusion.apache.org/). These libraries give a columnar, vectorized engine. Beacon reads native scientific formats such as NetCDF, Zarr, Parquet, and ODV.

> 🚀 Read the [Quick Start guide](QUICKSTART.md). It shows how to start Beacon and send the first query in two minutes.

## Table of contents

- [Quick Start guide](QUICKSTART.md)
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

- **Query files in place.** Beacon reads NetCDF, Zarr, Parquet, ODV, CSV, and more formats. The files stay on a local volume or in an S3-compatible object store. Beacon does not copy them.
- **One API for many formats.** Send an SQL query or a JSON query. Select the output format: Parquet, CSV, NetCDF, GeoParquet, Arrow IPC, or ODV. Beacon streams the result.
- **Large data volumes.** Beacon uses columnar execution, predicate pushdown, projection pushdown, and statistics-based pruning. Arrow and DataFusion give this speed.
- **Self-describing API.** Swagger UI documents each endpoint. Discovery endpoints list the available datasets, tables, columns, and functions.

## Features

- **Input formats:** Parquet, GeoParquet, NetCDF, Zarr, Atlas, ODV, CSV, Arrow IPC, GeoTIFF, Delta Lake, and the native Beacon Binary Format (BBF).
- **Output formats:** Parquet, GeoParquet, NetCDF, ND-NetCDF, CSV, Arrow IPC, and ODV.
- **Two query interfaces:** a structured **JSON query** API and raw **SQL**. Beacon enables SQL by default. Set `BEACON_ENABLE_SQL` to `false` to disable SQL.
- **Arrow Flight SQL:** an endpoint for clients with a high throughput. Beacon enables this endpoint by default.
- **Storage backends:** local filesystem and S3-compatible object storage. Beacon can monitor change events.
- **API documentation:** Swagger UI at `/swagger` and Scalar at `/scalar`.

## Concepts

- **Datasets:** the source files that you give to Beacon. Examples are `.nc`, `.zarr`, `.parquet`, and `.csv` files. Put the files in the mounted datasets directory. Beacon finds them automatically.
- **Tables:** named collections of one or more datasets. You query a table by name. Beacon keeps the table definitions in the tables directory. `BEACON_DEFAULT_TABLE` sets the default table. Beacon queries the default table when a request gives no source.
- **Source functions:** table functions such as `read_netcdf(...)`, `read_parquet(...)`, and `read_csv(...)`. A query uses a source function to read specific files. You do not define a table first.
- **Query engine:** Beacon parses each request into a DataFusion logical plan. The Arrow columnar engine runs the plan. Beacon then encodes the result in the requested output format and streams it back.

Read the [documentation](https://maris-development.github.io/beacon/) for the full data model.

## Quick start (Docker)

Use one `docker run` command to start Beacon. Run the command in the directory for the `datasets` and `tables` folders:

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

The command maps HTTP API port `5001` and Arrow Flight SQL port `32011`. It sets the admin credentials. It mounts a local `./datasets` directory with the files to query. It also mounts an empty `./tables` directory. Omit the `./tables` directory if you do not use tables.

### Docker Compose

Use Docker Compose for a repeatable setup:

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
      - ./datasets:/beacon/data/datasets # Mount a local directory with the files to query
      - ./tables:/beacon/data/tables # Mount an empty directory for tables. Omit it if you do not use tables
```

Start Beacon with `docker compose up -d`. Then open the API documentation at <http://localhost:5001/swagger/>.

Put files in `./datasets` to add data. Examples are `.nc`, `.zarr`, `.parquet`, and `.csv` files. Beacon finds the files through the mounted volume.

> Read the [installation guide](https://maris-development.github.io/beacon/docs/1.8.0/getting-started.html#local).

## Query examples

Both examples send a request to the same endpoint. Beacon streams back a file in the requested output format.

### SQL

> Beacon enables SQL by default. Set `BEACON_ENABLE_SQL=false` to disable SQL.

```http
POST http://localhost:5001/api/query
Content-Type: application/json

{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM read_netcdf(['data/2020.nc', 'data/2021.nc']) WHERE time > '2020-01-01T00:00:00'",
  "output": { "format": "parquet" }
}
```

### JSON

The JSON query API is read-only. It is always available. It needs no extra configuration.

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

Beacon streams the response as a file in the `output.format` value. This example gives CSV. Read the [query reference](https://maris-development.github.io/beacon/docs/1.8.0/api/querying/) for the full schema, all source types, and each output format.

### CLI

[`beacon-datalake-cli`](beacon-clients/beacon-datalake-cli) is a Python client for the terminal.
It uses the same `/api/*` endpoints as the server. Run SQL, read tables, datasets,
and schemas, and export results from the shell. Install the client from a local copy
of the repository:

```bash
pip install -e beacon-clients/beacon-datalake-cli
# or, with uv:
uv pip install -e beacon-clients/beacon-datalake-cli
```

The command installs the `beacon-datalake-cli` console script:

```bash
# Run SQL and show a table
beacon-datalake-cli --url http://localhost:5001 query "SELECT * FROM default LIMIT 10"

# Export results to a file. The file extension sets the format
beacon-datalake-cli --url http://localhost:5001 export "SELECT * FROM default" -o out.parquet

# Show the catalog
beacon-datalake-cli --url http://localhost:5001 tables

# Start the interactive REPL. Give no subcommand
beacon-datalake-cli --url http://localhost:5001
```

Read the [Beacon Datalake CLI guide](https://maris-development.github.io/beacon/docs/1.8.0/connect/beacon-datalake-cli)
for the full command list, the REPL meta-commands, and the export options.

## Configuration

You configure Beacon with `BEACON_*` environment variables. The most common variables are:

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_HOST` | `0.0.0.0` | Address for the HTTP server. |
| `BEACON_PORT` | `5001` | Port for the HTTP server. |
| `BEACON_ADMIN_USERNAME` | `beacon-admin` | Admin username for the management endpoints. |
| `BEACON_ADMIN_PASSWORD` | `beacon-password` | Admin password. **Change this password in production.** |
| `RUST_LOG` | _(built-in)_ | Log filter in `tracing-subscriber` EnvFilter syntax (e.g. `info`, `beacon_core=trace`). |
| `BEACON_VM_MEMORY_SIZE` | `8192` | Working memory (MB) for the query engine. |
| `BEACON_DEFAULT_TABLE` | `default` | Table that Beacon queries when a request gives no source. |
| `BEACON_WORKER_THREADS` | `8` | Number of worker threads for the async runtime. |
| `BEACON_ENABLE_SQL` | `true` | Enable the read-only raw SQL query interface. |
| `BEACON_FLIGHT_SQL_ENABLE` | `true` | Enable the Arrow Flight SQL endpoint. |
| `BEACON_FLIGHT_SQL_PORT` | `32011` | Port for Arrow Flight SQL. |

S3-compatible storage, CORS, the NetCDF cache, the crawler, and Flight SQL authentication use more `BEACON_*` variables. Read the [configuration reference](https://maris-development.github.io/beacon/docs/2.0.0-rc2/server/configuration.html) for the complete list.

## Documentation

- Documentation home: <https://maris-development.github.io/beacon/>
- Installation and first steps: <https://maris-development.github.io/beacon/docs/1.8.0/getting-started.html#local>
- Query reference: <https://maris-development.github.io/beacon/docs/1.8.0/api/querying/>
- Community Slack: [join here](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg)

## Contributing

Beacon is a Rust workspace. Build and test the source with these commands:

```bash
git clone https://github.com/maris-development/beacon.git
cd beacon
cargo build --release
cargo test
```

Send issues and pull requests to [GitHub](https://github.com/maris-development/beacon/issues). Open an issue first for a large change. The issue lets you discuss the approach.

## License

Beacon uses the **GNU Affero General Public License v3.0** (AGPL-3.0). Read [LICENSE](LICENSE) for the full text.

The clients under `beacon-clients/` are **Apache-2.0**, so they can be embedded freely. See [LICENSING.md](LICENSING.md).
