---
description: Beacon is a fast SQL engine for scientific data. Query NetCDF, Zarr, Parquet, GeoTIFF and more in place, on local disk or S3.
---

# Introduction

Beacon is a fast SQL engine for scientific data. Point Beacon at your existing files and query them
with SQL. Beacon reads NetCDF, Zarr, Parquet, CSV, ODV, GeoTIFF and more, on local disk or in S3.
There is no import step and no conversion into a proprietary format. Beacon reads your files *in
place*. Beacon uses Rust, [Apache Arrow](https://arrow.apache.org/) and
[DataFusion](https://datafusion.apache.org/).

You can run Beacon in **two ways**. Both ways use the same engine and the same SQL dialect:

- **[BeaconDB](#beacondb-the-embeddable-engine)** is the embeddable engine. One Python package runs
  the whole query engine in your process. A single portable `beacon.db` file holds the state. It is
  an in-process analytical database for scientific data.
- **[Beacon Data Lake](#beacon-data-lake-the-server)** is the server. It runs the same engine as a
  service. It adds an HTTP API, an Arrow Flight SQL API, a managed dataset store, crawlers,
  role-based access control, a web admin UI and client SDKs.

**Beacon Data Lake is BeaconDB as a service.** The query engine, the supported formats and the SQL
dialect are the same in both. Build a prototype locally with BeaconDB. Then run the same queries
against Beacon Data Lake. You do not change a line.

New here? Go to the **[Quick Start](/docs/2.0.0-rc2/quickstart)**. It takes a few minutes.

## How it fits together

One engine sits between your data and your tools. It reads files, object storage, SQL databases and
other Beacons in place. It exposes them through the same SQL. This is true for BeaconDB and for
Beacon Data Lake.

<SystemDiagram />

:::info Open source (AGPL-3.0)
Beacon uses the AGPL-3.0 license. Find the source and contribute here:
[github.com/maris-development/beacon](https://github.com/maris-development/beacon)
:::

## BeaconDB: the embeddable engine

BeaconDB is an analytical database in a Python package. Run `pip install beacondb`, then
`import beacondb`. The engine runs **in your process**. There is no server and no HTTP. One portable
`beacon.db` file holds everything that Beacon owns. It also references everything else: files, S3
and remote Beacons.

Use BeaconDB in a notebook, a script or an application. It gives your application a fast local query
engine for scientific formats.

```python
import beacondb

con = beacondb.connect("beacon.db")
con.sql("SELECT platform, avg(temperature) AS t "
        "FROM read_netcdf('argo/*.nc') GROUP BY platform").df()
```

[Get started with BeaconDB &rarr;](/docs/2.0.0-rc2/beacondb/python/getting-started)

## Beacon Data Lake: the server

Beacon Data Lake runs the same engine behind an HTTP API and an Arrow Flight SQL API. It adds a
managed dataset store, crawlers, role-based access control, a web admin UI and client SDKs.

Use Beacon Data Lake to serve datasets to many clients from shared or cloud storage. Clients include
portals, dashboards, notebooks and BI tools.

```bash
docker run -d --name beacon -p 5001:5001 \
  -v ./datasets:/beacon/data/datasets \
  ghcr.io/maris-development/beacon:latest
```

[Get started with Beacon Data Lake &rarr;](/docs/2.0.0-rc2/getting-started)

## One engine, one SQL

The SQL dialect, the supported formats and the query semantics are the same in both options.
Everything in the [SQL reference](/docs/2.0.0-rc2/beacondb/sql/) works in both. A query takes the
same path from client to engine to storage and back. This is true on your laptop, on your own server
and in the cloud.

<QueryFlow />

## Which should I use?

| You want to… | Use |
|---|---|
| Query local files from a notebook or script | **BeaconDB** |
| Ship an application with an embedded query engine | **BeaconDB** |
| One portable file (`beacon.db`) that you can copy | **BeaconDB** |
| Serve datasets to many users over HTTP or Flight SQL | **Beacon Data Lake** |
| Use role-based access, a web admin UI, crawlers and a managed lakehouse | **Beacon Data Lake** |
| Query a remote lake from a local engine | **both**: `ATTACH` the server from BeaconDB |

You are not locked in. A local **BeaconDB** can
[`ATTACH`](/docs/2.0.0-rc2/beacondb/python/remote-catalogs) a **Beacon Data Lake** and query it. You
can then join remote tables against local files. See [Concepts](/docs/2.0.0-rc2/concepts) for the
engine, the `beacon.db` file, catalogs and tables.
