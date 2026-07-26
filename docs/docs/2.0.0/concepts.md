---
description: The concepts shared by BeaconDB and Beacon Data Lake, the query engine, the beacon.db file, catalogs and tables, reading formats in place, and SQL vs JSON.
---

# Concepts

BeaconDB and Beacon Data Lake are the **same engine** in two runtimes. These concepts apply to both;
the chapter for each then shows how it exposes them.

## The query engine

Beacon is built in **Rust** on **Apache Arrow** (columnar in-memory data) and **DataFusion** (the SQL
planner and execution engine). Queries are filtered, projected, joined, and aggregated as Arrow
batches and streamed out. Because the engine reads columnar formats natively, it can push filters and
projections down into the files it scans.

## Reading files *in place*

Beacon does not require a load/ingest step. The **reader table functions**:
[`read_parquet`](/docs/2.0.0/beacondb/sql/table-functions), `read_netcdf`, `read_zarr`, `read_csv`,
`read_hdf5`, …, open your existing files (local or S3) and expose them as tables at query time.
Supported formats include NetCDF, Zarr, Parquet/GeoParquet, CSV, ODV ASCII, GeoTIFF, Atlas, Arrow
IPC, and Delta Lake. See [Supported formats](/docs/2.0.0/data-lake/datasets).

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/*.nc')
WHERE temperature > 20;
```

## Catalogs, schemas, and tables

Beacon exposes an information-schema-backed catalog. Tables come in a few flavours, all queryable the
same way:

- **Reader table functions**: files read in place (above).
- **External tables**: a named, reusable definition over files (`CREATE EXTERNAL TABLE … STORED AS
  PARQUET LOCATION …`), Delta tables, or a remote SQL database (Postgres/MySQL/ODBC).
- **Managed tables**: data Beacon *owns* and can write to (`CREATE TABLE … AS …`, `INSERT`,
  `UPDATE`, `DELETE`), backed by Lance.
- **Views** and **materialized views**: saved queries.
- **Remote tables / catalogs**: tables on *another* Beacon, reached over Flight SQL and federated
  (filters/joins/aggregates push down). See [`ATTACH`](/docs/2.0.0/beacondb/sql/remote-tables).

## The `beacon.db` file (BeaconDB)

When you embed the engine with **BeaconDB**, one file, `beacon.db`, holds everything Beacon *owns*
(its catalog and managed data) and *references* everything else (files on disk or S3, remote
databases and Beacons). It is a single portable container (a redb object store): copy the file and
the managed lake travels with it. Beacon Data Lake uses the same store for its managed state.

## Secrets

Credentials for object stores (S3/GCS/Azure) and for remote Beacons are stored as named, scoped
**secrets** (`CREATE SECRET`), rather than scattered environment variables. Persistent secrets are
encrypted into the `beacon.db` file. See [Secrets](/docs/2.0.0/beacondb/sql/secrets).

## SQL and JSON

Every query can be written in **SQL**. Beacon Data Lake additionally accepts an equivalent **JSON**
query form over its HTTP API (the payload the web UI and SDKs use). Both compile to the same plan, 
see the [SQL reference](/docs/2.0.0/beacondb/sql/) and the datalake [querying guide](/docs/2.0.0/api/querying/).

## Auth

Beacon has role-based access control (users, roles, grants). In **Beacon Data Lake** it governs served
access. In **BeaconDB** it is *off by default*, since opening a local file is full control, and can be
switched on (`auth=True`) for the served-boundary contract. See
[BeaconDB auth](/docs/2.0.0/beacondb/python/getting-started#authentication) and
[datalake access control](/docs/2.0.0/security/access-control).
