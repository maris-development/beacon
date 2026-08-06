# CREATE EXTERNAL TABLE

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

An external table is a SQL table over files in the storage of Beacon. After you register it, you can
query it with `SELECT` and `JOIN`. A `VIEW` can also reference it. Beacon reads the files on demand.
It does not copy them. A table definition survives a restart.

## Syntax

```sql
CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] <table_name>
STORED AS <format>
LOCATION '<path>'
[PARTITIONED BY (<col>, ...)]
```

Beacon resolves `LOCATION` against its storage root. Give a folder or a glob pattern:

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

`DELTA` points at an existing
[Delta Lake](/docs/2.0.0-rc2/formats/delta-lake) table directory. It also
supports time travel and `INSERT INTO`. `REMOTE` federates a table on another Beacon server. See
[Remote Tables](/docs/2.0.0-rc2/data-sources/remote-tables). `POSTGRES` and `MYSQL`
federate a table in an external SQL database. See
[SQL Databases](/docs/2.0.0-rc2/data-sources/sql-databases). Their `LOCATION` is the remote
table name. The connection details go in `OPTIONS`, with an encrypted `password`.

A Zarr table must point at a `zarr.json` entry file. An Atlas table must point at an `atlas.json`
marker:

```sql
CREATE EXTERNAL TABLE sst STORED AS ZARR LOCATION 'sst/*/zarr.json'

CREATE EXTERNAL TABLE sensor STORED AS ATLAS LOCATION 'sensor/atlas.json'
```

`GEOPARQUET` reads Parquet files. Beacon decodes their geometry columns to native GeoArrow. See
[GeoParquet in File Formats](/docs/2.0.0-rc2/formats/geoparquet) for the read
behaviour and for geometry queries.

## `IF NOT EXISTS`

Beacon skips the registration if the table name already exists. Beacon returns no error:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS argo
STORED AS NC
LOCATION 'argo/**/*.nc'
```

## `OR REPLACE`

Register the table again. Beacon overwrites the existing definition:

```sql
CREATE OR REPLACE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
```

## `PARTITIONED BY`

Your files can use Hive-style directories such as `year=2024/month=01/...`. Declare the partition
columns. Beacon can then prune them at query time:

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

`DROP TABLE` removes a table from the catalog. Beacon does not delete the files.

```sql
DROP TABLE argo

DROP TABLE IF EXISTS argo
```

## Querying and inspecting

```sql
SHOW TABLES;

DESCRIBE ocean_profiles;
```

The [External Tables](/docs/2.0.0-rc2/data-sources/external-tables) setup guide gives an
example for each format. It also shows the HTTP API that lists the tables.
