---
description: Beacon is a query engine for scientific data. One server node lets many clients query NetCDF, Zarr, Parquet, GeoTIFF and more in place.
---

# Introduction

Beacon is a query engine for scientific data. It runs as a **server**: you stand up one node over
an archive, and everyone who needs that data queries it with SQL.

Beacon reads NetCDF, Zarr, Parquet, CSV, ODV, GeoTIFF and more. There is no import step. There is no
conversion into a proprietary format. Beacon reads the files *in place*. It uses Rust,
[Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://datafusion.apache.org/).

Many clients reach one server at the same time. A client is a notebook, a portal, a dashboard, a BI
tool or a terminal. All clients share one catalog, one set of paths and one set of
[grants](/docs/2.0.0-rc3/security/access-control). You therefore decide in one place what each user
may read. File copies no longer decide it.

Beacon solves one problem. An institution holds an archive. Many people need parts of it. Nobody
needs to download the whole archive.

New here? Go to the **[Quick Start](/docs/2.0.0-rc3/quickstart)**. It takes a few minutes.

## How it fits together

One engine sits between your data and your tools. It reads files, object storage, SQL databases and
other Beacon servers in place. It exposes all of them through the same SQL.

<SystemDiagram />

:::info Open source
Beacon uses AGPL-3.0; the clients are Apache-2.0. Find the source here:
[github.com/maris-development/beacon](https://github.com/maris-development/beacon)
:::

## One store, one namespace

A server reads its datasets from **one** store: a local directory, or a single S3-compatible
bucket. You choose which at startup.

Clients never see that choice. **Every path in a query is relative to the datasets root**, so the
same SQL runs against a test node and against the production one:

```sql
SELECT * FROM read_parquet('obs/*.parquet') LIMIT 10;
```

Where the bytes live is an operator's decision, made once in configuration. See
[Object Storage](/docs/2.0.0-rc3/data-sources/object-storage).

## Four ways to name data

Reading files by path suits an ad-hoc query. For anything you run twice, give it a name. Beacon has
four kinds, and you query all of them the same way:

| Kind | What it is | Beacon stores |
|---|---|---|
| [External table](/docs/2.0.0-rc3/data-sources/external-tables) | A name over files in the datasets store | The definition |
| [View](/docs/2.0.0-rc3/sql/create-view) | A saved query | The definition |
| [Materialized view](/docs/2.0.0-rc3/sql/create-materialized-view) | A saved query, with its result kept and refreshed | The definition and the rows |
| [Managed table](/docs/2.0.0-rc3/sql/managed-tables) | A table Beacon owns and writes | The rows |

```sql
CREATE EXTERNAL TABLE obs STORED AS PARQUET LOCATION 'obs/';
CREATE VIEW warm AS SELECT * FROM obs WHERE temperature > 20;
CREATE MATERIALIZED VIEW warm_cached AS SELECT * FROM obs WHERE temperature > 20;
CREATE TABLE curated AS SELECT * FROM obs WHERE qc_flag = 1;
```

Only a managed table accepts `INSERT`, `UPDATE` and `DELETE`. The other three read from files that
stay exactly as they are.

## What Beacon owns

Beacon reads most data in place. It does own some state, and that state lives in one `beacon.db`
file:

- **The catalog.** Every external table, view and materialized view definition above.
- **Managed table rows.** The only data Beacon holds itself.
- **Users, roles and grants.** See [Access Control](/docs/2.0.0-rc3/security/access-control).
- **Secrets.** Credentials for another Beacon server, encrypted at rest. See
  [CREATE SECRET](/docs/2.0.0-rc3/sql/secrets).

Everything else stays where it is. Beacon never copies your source files. See
[Storage internals](/docs/2.0.0-rc3/internals/storage).

## One SQL, every source

One SQL dialect covers every source. A local NetCDF file, a Parquet prefix in S3, a Postgres table
and a table on another Beacon server all read the same way. You join across them in one statement.

```sql
SELECT a.platform, a.temperature, b.station_name
FROM read_netcdf('argo/**/*.nc') AS a
JOIN remote_wod.stations AS b ON a.platform = b.platform
WHERE a.temperature > 20;
```

Read the [SQL reference](/docs/2.0.0-rc3/sql/) for the full dialect.

<QueryFlow />

## Where to go next

**Running a node**

| You want to… | Read |
|---|---|
| Deploy one with Docker | [Getting Started](/docs/2.0.0-rc3/getting-started) |
| Set ports, storage and limits | [Configuration](/docs/2.0.0-rc3/server/configuration) |
| Register your data as tables | [Server Setup](/docs/2.0.0-rc3/server/) |
| Decide who may read what | [Access Control](/docs/2.0.0-rc3/security/access-control) |
| Make a slow query fast | [Performance Tuning](/docs/2.0.0-rc3/server/performance-tuning) |

**Querying one**

| You want to… | Read |
|---|---|
| Run your first query against a live node | [Quick Start](/docs/2.0.0-rc3/quickstart) |
| Understand how a file becomes rows | [Arrays to tables](/docs/2.0.0-rc3/arrays-to-tables) |
| Replace an xarray loop with SQL | [Coming from xarray](/docs/2.0.0-rc3/coming-from-xarray) |
| See which formats support what | [File formats](/docs/2.0.0-rc3/formats/) |
| Query another institution's node | [ATTACH](/docs/2.0.0-rc3/data-sources/attach) |

See [Concepts](/docs/2.0.0-rc3/concepts) for the engine, the `beacon.db` file, catalogs and tables.
