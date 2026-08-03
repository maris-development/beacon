---
description: The concepts that BeaconDB and Beacon Data Lake share. The query engine, the beacon.db file, catalogs, tables, formats, secrets and SQL.
---

# Concepts

BeaconDB and Beacon Data Lake are the **same engine** in two runtimes. These concepts apply to both.
The chapter for each one then shows how it exposes them.

## The query engine

Beacon uses **Rust**, **Apache Arrow** and **DataFusion**. Arrow holds columnar data in memory.
DataFusion plans and runs the SQL. The engine filters, projects, joins and aggregates Arrow batches,
then streams the result out. The engine reads columnar formats directly. It can therefore push
filters and projections down into the files that it scans.

## Read files *in place*

Beacon needs no load step. The **reader table functions** open your existing files and expose them as
tables at query time. The files can be local or on S3. The functions include
[`read_parquet`](/docs/2.0.0-rc2/beacondb/sql/table-functions), `read_netcdf`, `read_zarr`,
`read_csv` and `read_hdf5`. Supported formats include NetCDF, Zarr, Parquet, GeoParquet, CSV, ODV
ASCII, GeoTIFF, Atlas, Arrow IPC and Delta Lake. See
[Supported formats](/docs/2.0.0-rc2/data-lake/datasets).

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/*.nc')
WHERE temperature > 20;
```

## Catalogs, schemas, and tables

Beacon has a catalog with an information schema. There are several table types. You query all of
them in the same way:

- **Reader table functions**: Beacon reads the files in place. See the section above.
- **External tables**: a named definition over files, Delta tables or a remote SQL database. Use
  `CREATE EXTERNAL TABLE … STORED AS PARQUET LOCATION …`. Remote databases include Postgres, MySQL
  and ODBC sources.
- **Managed tables**: data that Beacon *owns*. Beacon can write to them with `CREATE TABLE … AS …`,
  `INSERT`, `UPDATE` and `DELETE`. Lance holds the data.
- **Views** and **materialized views**: saved queries.
- **Remote tables and catalogs**: tables on *another* Beacon. Beacon reaches them over Flight SQL and
  pushes filters, joins and aggregates down. See
  [`ATTACH`](/docs/2.0.0-rc2/beacondb/sql/remote-tables).

## The `beacon.db` file (BeaconDB)

**BeaconDB** holds its state in one file, `beacon.db`. The file holds everything that Beacon *owns*:
its catalog and its managed data. The file *references* everything else: files on disk or S3, remote
databases and remote Beacons. The file is one portable container, a redb object store. Copy the file
and the managed lake goes with it. Beacon Data Lake uses the same store for its managed state.

## Secrets

Beacon holds credentials as named, scoped **secrets**. Use `CREATE SECRET`. Secrets cover object
stores such as S3, GCS and Azure. They also cover remote Beacons. Secrets replace scattered
environment variables. Beacon encrypts persistent secrets into the `beacon.db` file. See
[Secrets](/docs/2.0.0-rc2/beacondb/sql/secrets).

## SQL and JSON

You can write every query in **SQL**. Beacon Data Lake also accepts an equal **JSON** query form over
its HTTP API. The web UI and the SDKs send this payload. Both forms compile to the same plan. See the
[SQL reference](/docs/2.0.0-rc2/beacondb/sql/) and the data lake
[querying guide](/docs/2.0.0-rc2/api/querying/).

## Auth

Beacon has role-based access control with users, roles and grants. In **Beacon Data Lake** it
controls served access. In **BeaconDB** it is *off by default*, because a local file gives the user
full control. Set `auth=True` to switch it on. See
[BeaconDB auth](/docs/2.0.0-rc2/beacondb/python/getting-started#authentication) and
[data lake access control](/docs/2.0.0-rc2/security/access-control).
