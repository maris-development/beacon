---
description: Get Beacon running in a couple of minutes, either embedded in Python with BeaconDB or as a server with Beacon Data Lake. Both use the same SQL and read the same formats.
---

# Quick Start

Beacon runs two ways. Both share the same engine, the same SQL dialect, and the same file
formats, so pick whichever fits how you work and switch later without rewriting a query.

- **[Embed it](#embed-it-beacondb)** with **BeaconDB**: the engine in your Python process.
- **[Serve it](#serve-it-beacon-data-lake)** with **Beacon Data Lake**: the engine as a server for your team.

## Embed it: BeaconDB

Install the package and query a file in three lines. There is no server to start and no import
step: BeaconDB reads your files in place.

```bash
pip install beacondb
```

```python
import beacondb

con = beacondb.connect("beacon.db")          # or ":memory:" for a throwaway database
con.sql("SELECT * FROM read_parquet('data/*.parquet') LIMIT 10").df()
```

`connect()` returns a [PEP 249](https://peps.python.org/pep-0249/) connection. Read results as
plain rows (`fetchall()`) or as a dataframe (`.df()` for pandas, plus Polars and Arrow). The whole
query engine runs in your process, backed by one portable `beacon.db` file.

Next: [BeaconDB getting started](/docs/2.0.0-rc2/beacondb/python/getting-started) or the
[SQL reference](/docs/2.0.0-rc2/beacondb/sql/).

## Serve it: Beacon Data Lake

Run the server, drop in files, and query them over HTTP or from the bundled admin UI.

```bash
docker run -d --name beacon -p 5001:5001 \
  -e BEACON_ADMIN_USERNAME=admin \
  -e BEACON_ADMIN_PASSWORD=securepassword \
  -v ./datasets:/beacon/data/datasets \
  ghcr.io/maris-development/beacon:latest
```

Drop supported files (`.parquet`, `.nc`, `.zarr`, `.csv`, and more) into `./datasets`. Beacon
discovers them automatically. Then open the admin UI at
[http://localhost:5001/admin](http://localhost:5001/admin), or query over HTTP:

```bash
curl -X POST http://localhost:5001/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM read_parquet([\"datasets/**/*.parquet\"]) LIMIT 10",
    "output": { "format": "csv" }
  }'
```

Next: [Beacon Data Lake getting started](/docs/2.0.0-rc2/getting-started) for reproducible Docker
Compose and S3 setups, or [connect a client](/docs/2.0.0-rc2/connect/beacon-python-sdk).

## Which one should I start with?

| If you want to… | Start with |
|---|---|
| Query files from a notebook, script, or app, locally | **BeaconDB** |
| Ship a single portable `beacon.db` file | **BeaconDB** |
| Serve datasets to many users over HTTP or Flight SQL | **Beacon Data Lake** |
| Add access control, a web admin UI, and crawlers | **Beacon Data Lake** |

You are not locked in either way. A local BeaconDB can
[`ATTACH`](/docs/2.0.0-rc2/beacondb/python/remote-catalogs) a running Beacon Data Lake and query it,
joining remote tables against local files. See [Concepts](/docs/2.0.0-rc2/concepts) for how the engine,
the `beacon.db` file, and tables fit together.
