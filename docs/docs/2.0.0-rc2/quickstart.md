---
description: Start Beacon in a few minutes. Embed it in Python with BeaconDB, or run it as a server with Beacon Data Lake.
---

# Quick Start

Beacon runs in two ways. Both ways use the same engine, the same SQL dialect and the same file
formats. Choose the option that fits your work. You can change later. Your queries stay the same.

- **[Embed it](#embed-it-beacondb)** with **BeaconDB**. The engine runs in your Python process.
- **[Serve it](#serve-it-beacon-data-lake)** with **Beacon Data Lake**. The engine runs as a server for your team.

## Embed it: BeaconDB

Install the package. Then query a file in three lines. There is no server to start and no import
step. BeaconDB reads your files in place.

```bash
pip install beacondb
```

```python
import beacondb

con = beacondb.connect("beacon.db")          # or ":memory:" for a throwaway database
con.sql("SELECT * FROM read_parquet('data/*.parquet') LIMIT 10").df()
```

`connect()` returns a [PEP 249](https://peps.python.org/pep-0249/) connection. Read the results as
plain rows with `fetchall()`. Read them as a dataframe with `.df()` for pandas. Polars and Arrow
also work. The whole query engine runs in your process. One portable `beacon.db` file holds the
state.

Next: [BeaconDB getting started](/docs/2.0.0-rc2/beacondb/python/getting-started) or the
[SQL reference](/docs/2.0.0-rc2/beacondb/sql/).

## Serve it: Beacon Data Lake

Start the server. Copy your files into the dataset folder. Then query them over HTTP or from the
admin UI.

```bash
docker run -d --name beacon -p 5001:5001 \
  -e BEACON_ADMIN_USERNAME=admin \
  -e BEACON_ADMIN_PASSWORD=securepassword \
  -v ./datasets:/beacon/data/datasets \
  ghcr.io/maris-development/beacon:latest
```

Copy supported files into `./datasets`. Supported files include `.parquet`, `.nc`, `.zarr` and
`.csv`. Beacon finds them automatically. Then open the admin UI at
[http://localhost:5001/admin](http://localhost:5001/admin). You can also query over HTTP:

```bash
curl -X POST http://localhost:5001/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM read_parquet([\"datasets/**/*.parquet\"]) LIMIT 10",
    "output": { "format": "csv" }
  }'
```

Next: [Beacon Data Lake getting started](/docs/2.0.0-rc2/getting-started) for Docker Compose and S3
setups. Or [connect a client](/docs/2.0.0-rc2/connect/beacon-python-sdk).

## Which one should I start with?

| If you want to… | Start with |
|---|---|
| Query local files from a notebook, script or application | **BeaconDB** |
| Ship one portable `beacon.db` file | **BeaconDB** |
| Serve datasets to many users over HTTP or Flight SQL | **Beacon Data Lake** |
| Add access control, a web admin UI and crawlers | **Beacon Data Lake** |

You are not locked in. A local BeaconDB can
[`ATTACH`](/docs/2.0.0-rc2/beacondb/python/remote-catalogs) a Beacon Data Lake and query it. You can
then join remote tables against local files. See [Concepts](/docs/2.0.0-rc2/concepts) for the
engine, the `beacon.db` file and tables.
