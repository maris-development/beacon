# CREATE EXTERNAL TABLE

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

An external table is a SQL table backed by files in Beacon's storage. Once registered, you can query it with `SELECT`, `JOIN`, or reference it from a `VIEW`, Beacon reads the files on demand without copying them. Table definitions survive restarts.

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
CREATE EXTERNAL TABLE argo STORED AS NC LOCATION 'argo/'

-- Explicit glob
CREATE EXTERNAL TABLE argo STORED AS NC LOCATION 'argo/**/*.nc'
```

## Formats

| `STORED AS` | File types |
| ----------- | ---------- |
| `PARQUET` | `.parquet` |
| `GEOPARQUET` | `.geoparquet` |
| `NC` | `.nc` |
| `ZARR` | Zarr v3 (`zarr.json`) |
| `ATLAS` | Atlas array store (`atlas.json`) |
| `CSV` | `.csv` |
| `ARROW` | Arrow IPC (`.arrow`, `.feather`) |
| `TIFF` | GeoTIFF / Cloud-Optimized GeoTIFF |
| `BBF` | Beacon Binary Format |
| `DELTA` | Delta Lake table directory (`_delta_log/`) |
| `POSTGRES` | External PostgreSQL table (federated) |
| `MYSQL` | External MySQL table (federated) |

`DELTA` points at an existing [Delta Lake](/docs/2.0.0-rc1/beacondb/data-sources/formats/delta-lake) table directory and additionally supports time travel and `INSERT INTO`. `REMOTE` federates a table on another Beacon instance, see [Remote Tables](/docs/2.0.0-rc1/beacondb/data-sources/remote-tables). `POSTGRES` / `MYSQL` federate a table in an external SQL database, see [SQL Databases](/docs/2.0.0-rc1/beacondb/data-sources/sql-databases); their `LOCATION` is the remote table name and connection details (including an encrypted `password`) go in `OPTIONS`.

Zarr tables should point at `zarr.json` entry files, and Atlas tables at `atlas.json` markers:

```sql
CREATE EXTERNAL TABLE sst STORED AS ZARR LOCATION 'sst/*/zarr.json'

CREATE EXTERNAL TABLE sensor STORED AS ATLAS LOCATION 'sensor/atlas.json'
```

`GEOPARQUET` reads Parquet files whose geometry columns are decoded to native GeoArrow, see [GeoParquet in File Formats](/docs/2.0.0-rc1/beacondb/data-sources/formats/geoparquet) for querying geometry and read behaviour.

## `IF NOT EXISTS`

Silently skip registration if the table name is already taken:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS argo
STORED AS NC
LOCATION 'argo/**/*.nc'
```

## `OR REPLACE`

Re-register and overwrite an existing table definition:

```sql
CREATE OR REPLACE EXTERNAL TABLE argo
STORED AS NC
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

See the [External Tables](/docs/2.0.0-rc1/beacondb/data-sources/external-tables) setup guide for per-format examples and the HTTP API for listing tables.
