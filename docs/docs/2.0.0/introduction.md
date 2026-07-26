---
description: Beacon is a query engine for scientific data — embed it in Python (beacondb) or run it as a server (beacon-datalake). Read NetCDF, Zarr, Parquet, GeoTIFF and more in place from local files or S3, and query them with SQL.
---

# Introduction

:::info Open Source (AGPL V3)
Beacon is open source under the AGPL V3 license. Source code and contributions: [github.com/maris-development/beacon](https://github.com/maris-development/beacon)
:::

**Beacon is a query engine for scientific data.** Point it at your existing files — NetCDF, Zarr,
Parquet, CSV, ODV, GeoTIFF and more, on disk or in S3 — and query them with SQL, with no data
migration or preprocessing. It is built in Rust on Apache Arrow and DataFusion, and it reads your
files *in place*.

The same engine ships **two ways**, both at version 2.0.0. Pick the one that fits how you work.

## 🧪 beacondb — embed it

An **embeddable, DuckDB-class database** as a Python package. `pip install beacondb`, `import
beacondb`, and the whole engine runs **in-process** — no server, no HTTP. One portable `beacon.db`
file holds everything it owns and references everything else (files, S3, remote Beacons).

Reach for beacondb in a **notebook, a script, or an application** that owns its data and wants a fast
local query engine over scientific formats.

```python
import beacondb
con = beacondb.connect("beacon.db")
con.sql("SELECT platform, avg(temperature) AS t "
        "FROM read_netcdf('argo/*.nc') GROUP BY platform").df()
```

→ [Get started with beacondb](/docs/2.0.0/beacondb/python/getting-started)

## 🛰️ beacon-datalake — serve it

A **server** that puts the same engine behind an HTTP + Arrow Flight SQL API, with a datasets store,
crawlers, role-based access control, a web admin UI, and client SDKs.

Reach for beacon-datalake when you need to **serve datasets to many clients** — portals, dashboards,
notebooks, BI tools — from shared or cloud storage.

```bash
docker run -d --name beacon -p 5001:5001 \
  -v ./datasets:/beacon/data/datasets \
  ghcr.io/maris-development/beacon:latest
```

→ [Get started with beacon-datalake](/docs/2.0.0/getting-started)

## One engine, one SQL

Whichever you choose, the SQL dialect, the supported formats, and the query semantics are the
**same** — beacondb and beacon-datalake are the same engine, embedded or served. Everything in the
[SQL reference](/docs/2.0.0/beacondb/sql/) works in both. See [Concepts](/docs/2.0.0/concepts) for how the
pieces fit together.

<QueryFlow />

## Which should I use?

| You want to… | Use |
|---|---|
| Query files from a notebook or script, locally | **beacondb** |
| Ship an app with an embedded query engine | **beacondb** |
| A single portable file (`beacon.db`) you can copy | **beacondb** |
| Serve datasets to many users over HTTP / Flight SQL | **beacon-datalake** |
| RBAC, a web admin UI, crawlers, a managed lakehouse | **beacon-datalake** |
| Query a remote datalake from a local beacondb | **both** — `ATTACH` the server from beacondb |

You are not locked in: a local **beacondb** can [`ATTACH`](/docs/2.0.0/beacondb/python/remote-catalogs) a
running **beacon-datalake** and query it, joining remote tables against local files.
