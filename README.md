# Beacon: a query engine for scientific data

[![Release](https://img.shields.io/github/v/release/maris-development/beacon?label=release&color=success)](https://github.com/maris-development/beacon/releases)
[![Docs](https://img.shields.io/github/actions/workflow/status/maris-development/beacon/pages.yml?label=docs)](https://maris-development.github.io/beacon/)
[![codecov](https://codecov.io/gh/maris-development/beacon/branch/main/graph/badge.svg)](https://codecov.io/gh/maris-development/beacon)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-2496ED?logo=docker&logoColor=white)](https://github.com/maris-development/beacon/pkgs/container/beacon)
[![License](https://img.shields.io/github/license/maris-development/beacon)](LICENSE)
[![Slack](https://img.shields.io/badge/slack-join-4A154B?logo=slack&logoColor=white)](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg)

Beacon is a server. Point it at an archive of scientific files. Your users then query that archive
with SQL.

Beacon reads NetCDF, Zarr, Parquet, ODV and more in place. It runs no import job. It makes no second
copy. It sends back only the rows and columns of the answer.

Read [Why Beacon](https://maris-development.github.io/beacon/why-beacon) for the problems it solves.

## 1. Start a server

```bash
docker run -d --name beacon -p 5001:5001 \
  -e BEACON_ADMIN_USERNAME=admin \
  -e BEACON_ADMIN_PASSWORD=securepassword \
  -v ./datasets:/beacon/data/datasets \
  ghcr.io/maris-development/beacon:latest
```

Port `5001` serves the HTTP API and the admin UI. Add `-p 32011:32011` for Arrow Flight SQL.

## 2. Point it at your data

Copy files into `./datasets`. Beacon finds them. You register nothing first.

Beacon reads Parquet, GeoParquet, NetCDF, HDF5, Zarr, Atlas, CSV, Arrow IPC, GeoTIFF, Delta Lake,
ODV ASCII and BBF.

A path in a query is relative to the datasets root. A file at `./datasets/obs/a.parquet` is
`obs/a.parquet`. Do not repeat `datasets/`. Do not write an `s3://` scheme.

To use a bucket instead of a directory, set `BEACON_S3_DATASETS=true` and `BEACON_S3_BUCKET`. See
[Object Storage](https://maris-development.github.io/beacon/docs/2.0.0-rc2/data-sources/object-storage).

## 3. Query it from Python

```bash
pip install beacon-api
```

```python
from beacon_api import Client

client = Client("http://localhost:5001", basic_auth=("admin", "securepassword"))

df = client.sql_query("""
    SELECT time, latitude, longitude, temperature
    FROM read_netcdf(['**/*.nc'])
    WHERE temperature > 20
    LIMIT 10
""").to_pandas_dataframe()

print(df)
```

`to_parquet()`, `to_csv()` and `to_geoparquet()` write the result to a file instead.

### Query a public server

This server needs no account. It holds the World Ocean Database:

```python
from beacon_api import Client

client = Client("https://beacon-wod.maris.nl")

df = client.sql_query(
    'SELECT time, latitude, depth, temperature FROM "easy-wod" WHERE temperature > 20 LIMIT 5'
).to_pandas_dataframe()
```

### Other clients

Beacon also answers plain HTTP at `POST /api/query`. There is a
[TypeScript SDK](https://maris-development.github.io/beacon/docs/2.0.0-rc2/connect/typescript), a
[terminal client](https://maris-development.github.io/beacon/docs/2.0.0-rc2/connect/cli), and an
Arrow Flight SQL endpoint for JDBC and ADBC tools.

## Next steps

| | |
| --- | --- |
| Name your files as tables and views | [Server Setup](https://maris-development.github.io/beacon/docs/2.0.0-rc2/server/) |
| Set ports, storage and limits | [Configuration](https://maris-development.github.io/beacon/docs/2.0.0-rc2/server/configuration) |
| Decide who reads what | [Access Control](https://maris-development.github.io/beacon/docs/2.0.0-rc2/security/access-control) |
| Write queries | [SQL Reference](https://maris-development.github.io/beacon/docs/2.0.0-rc2/sql/) |
| Move from an xarray loop | [Coming from xarray](https://maris-development.github.io/beacon/docs/2.0.0-rc2/coming-from-xarray) |

Documentation home: <https://maris-development.github.io/beacon/>

## Contributing

Beacon is a Rust workspace. Build and test it:

```bash
git clone https://github.com/maris-development/beacon.git
cd beacon
cargo build --release
cargo test
```

Send issues and pull requests to [GitHub](https://github.com/maris-development/beacon/issues). Open
an issue first for a large change. The issue lets you discuss the approach.

## License

Beacon uses the **GNU Affero General Public License v3.0** (AGPL-3.0). Read [LICENSE](LICENSE).

The clients under `beacon-clients/` use **Apache-2.0**. See [LICENSING.md](LICENSING.md).
