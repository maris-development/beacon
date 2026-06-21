# CREATE EXTERNAL TABLE

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

An external table is a SQL table backed by files in Beacon's storage. Once registered, you can query it with `SELECT`, `JOIN`, or reference it from a `VIEW` — Beacon reads the files on demand without copying them. Table definitions survive restarts.

## Syntax

```sql
CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] <table_name>
STORED AS <format>
LOCATION '<path>'
[PARTITIONED BY (<col>, ...)]
```

`LOCATION` is resolved relative to Beacon's storage root. It can be a folder or a glob pattern:

```sql
-- Entire folder
CREATE EXTERNAL TABLE argo STORED AS NETCDF LOCATION 'argo/'

-- Explicit glob
CREATE EXTERNAL TABLE argo STORED AS NETCDF LOCATION 'argo/**/*.nc'
```

## Formats

| `STORED AS` | File types |
| ----------- | ---------- |
| `PARQUET`   | `.parquet` |
| `GEOPARQUET` | `.geoparquet` |
| `NETCDF`    | `.nc`, `.nc4`, `.cdf` |
| `ZARR`      | Zarr v2/v3 (`zarr.json`) |
| `ATLAS`     | Atlas array store (`atlas.json`) |
| `CSV`       | `.csv` |
| `ARROW`     | Arrow IPC (`.arrow`, `.ipc`) |
| `ODV`       | ODV ASCII spreadsheet |
| `TIFF`      | GeoTIFF / Cloud-Optimized GeoTIFF |
| `BBF`       | Beacon Binary Format |
| `DELTA`     | Delta Lake table directory (`_delta_log/`) |

`DELTA` points at an existing [Delta Lake](../data-lake/delta-lake.md) table directory and additionally supports time travel and `INSERT INTO`. `REMOTE` federates a table on another Beacon instance — see [Remote Tables](../data-lake/remote-tables.md).

Zarr tables should point at `zarr.json` entry files, and Atlas tables at `atlas.json` markers:

```sql
CREATE EXTERNAL TABLE sst STORED AS ZARR LOCATION 'sst/*/zarr.json'

CREATE EXTERNAL TABLE sensor STORED AS ATLAS LOCATION 'sensor/atlas.json'
```

`GEOPARQUET` reads Parquet files whose geometry columns are decoded to native GeoArrow — see the [GeoParquet chapter](./geoparquet.md) for querying geometry and the [data-lake setup](../data-lake/geoparquet.md) for details.

## `IF NOT EXISTS`

Silently skip registration if the table name is already taken:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS argo
STORED AS NETCDF
LOCATION 'argo/**/*.nc'
```

## `OR REPLACE`

Re-register and overwrite an existing table definition:

```sql
CREATE OR REPLACE EXTERNAL TABLE argo
STORED AS NETCDF
LOCATION 'argo/**/*.nc'
```

## `PARTITIONED BY`

When files are organized in Hive-style directories (`year=2024/month=01/...`), declare the partition columns so Beacon can prune them at query time:

```sql
CREATE EXTERNAL TABLE observations
STORED AS PARQUET
LOCATION 'obs/'
PARTITIONED BY (year, month)
```

```sql
SELECT * FROM observations WHERE year = 2024 AND month = 6
```

## `DROP TABLE`

Remove a table from the catalog. The underlying files are not deleted.

```sql
DROP TABLE argo

DROP TABLE IF EXISTS argo
```

## Querying and inspecting

```sql
SHOW TABLES;

DESCRIBE ocean_profiles;
```

See the [External Tables](../data-lake/external-tables.md) setup guide for per-format examples and the HTTP API for listing tables.
