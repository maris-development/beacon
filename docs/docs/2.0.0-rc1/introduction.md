---
description: Beacon is a fast SQL engine for scientific data. Query NetCDF, Zarr, Parquet, GeoTIFF and more in place from local files or S3. Embed the engine in Python with BeaconDB, or run it as a server with Beacon Data Lake.
---

# Introduction

Beacon is a fast SQL engine for scientific data. Point it at your existing files, NetCDF, Zarr,
Parquet, CSV, ODV, GeoTIFF and more, on local disk or in S3, and query them with SQL. There is no
import step and no conversion into a proprietary format: Beacon reads your files *in place*. It is
built in Rust on [Apache Arrow](https://arrow.apache.org/) and
[DataFusion](https://datafusion.apache.org/).

There are **two ways to run Beacon**, both on the same engine and the same SQL dialect:

- **[BeaconDB](#beacondb-the-embeddable-engine)** is the embeddable engine. One Python package that
  runs the whole query engine in your process, backed by a single portable `beacon.db` file. An
  in-process analytical database, built for scientific data.
- **[Beacon Data Lake](#beacon-data-lake-the-server)** is the server. It runs that same engine as a
  service, adding an HTTP and Arrow Flight SQL API, a managed dataset store, crawlers, role-based
  access control, a web admin UI, and client SDKs.

**Beacon Data Lake is BeaconDB running as a service.** The query engine, the supported formats, and
the SQL dialect are identical in both. You can prototype locally with BeaconDB and deploy the exact
same queries against Beacon Data Lake without changing a line.

New here? Jump to the **[Quick Start](/docs/2.0.0-rc1/quickstart)** to get running in a couple of minutes.

## How it fits together

One engine sits between your data and the tools you query with. It reads files, object storage,
SQL databases, and other Beacons in place, and exposes them through the same SQL whether you embed
it as BeaconDB or run it as Beacon Data Lake.

<SystemDiagram />

:::info Open source (AGPL-3.0)
Beacon is open source under the AGPL-3.0 license. Source and contributions:
[github.com/maris-development/beacon](https://github.com/maris-development/beacon)
:::

## BeaconDB: the embeddable engine

An analytical database shipped as a Python package. `pip install beacondb`, `import beacondb`, and
the engine runs entirely **in-process**: no server, no HTTP. One portable `beacon.db` file holds
everything it owns and references everything else (files, S3, remote Beacons).

Reach for BeaconDB in a notebook, a script, or an application that owns its data and wants a fast
local query engine over scientific formats.

```python
import beacondb

con = beacondb.connect("beacon.db")
con.sql("SELECT platform, avg(temperature) AS t "
        "FROM read_netcdf('argo/*.nc') GROUP BY platform").df()
```

[Get started with BeaconDB &rarr;](/docs/2.0.0-rc1/beacondb/python/getting-started)

## Beacon Data Lake: the server

The same engine behind an HTTP and Arrow Flight SQL API, with a managed dataset store, crawlers,
role-based access control, a web admin UI, and client SDKs.

Reach for Beacon Data Lake when you need to serve datasets to many clients (portals, dashboards,
notebooks, BI tools) from shared or cloud storage.

```bash
docker run -d --name beacon -p 5001:5001 \
  -v ./datasets:/beacon/data/datasets \
  ghcr.io/maris-development/beacon:latest
```

[Get started with Beacon Data Lake &rarr;](/docs/2.0.0-rc1/getting-started)

## One engine, one SQL

Whichever you choose, the SQL dialect, the supported formats, and the query semantics are the same.
Everything in the [SQL reference](/docs/2.0.0-rc1/beacondb/sql/) works either way. A query travels
the same path from client to engine to storage and back, whether Beacon runs on your laptop, your
own server, or in the cloud.

<QueryFlow />

## Which should I use?

| You want to… | Use |
|---|---|
| Query files from a notebook or script, locally | **BeaconDB** |
| Ship an app with an embedded query engine | **BeaconDB** |
| A single portable file (`beacon.db`) you can copy | **BeaconDB** |
| Serve datasets to many users over HTTP or Flight SQL | **Beacon Data Lake** |
| Role-based access, a web admin UI, crawlers, a managed lakehouse | **Beacon Data Lake** |
| Query a remote lake from a local engine | **both**: `ATTACH` the server from BeaconDB |

You are not locked in: a local **BeaconDB** can
[`ATTACH`](/docs/2.0.0-rc1/beacondb/python/remote-catalogs) a running **Beacon Data Lake** and query it,
joining remote tables against local files. See [Concepts](/docs/2.0.0-rc1/concepts) for how the engine,
the `beacon.db` file, catalogs, and tables fit together.
