# External Tables

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

An external table is a standard SQL table backed by files in Beacon's storage. Once created, you can `SELECT`, `JOIN`, and `DROP` it like any other table — Beacon reads the underlying files on demand without copying them.

Table definitions are persisted automatically and survive restarts.

Table definitions are persisted automatically and survive restarts. All DDL can be submitted through any of Beacon's SQL surfaces:

- **HTTP** — `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ..." }`
- **Arrow Flight SQL** — any Flight SQL client (DataGrip, ADBC, DBeaver, …)

:::info
SQL must be enabled (`BEACON_ENABLE_SQL=true`) to run DDL statements over the HTTP API. Arrow Flight SQL does not require this flag.
:::

## Creating an external table

The base syntax is:

```sql
CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] <table_name>
STORED AS <format>
LOCATION '<path>'
```

The `LOCATION` is resolved relative to Beacon's configured dataset storage root (`/beacon/data/datasets` in the default Docker container, or the S3 prefix when using object storage). It may be:

- A folder path — Beacon scans all matching files inside it
- A glob pattern — e.g. `argo/**/*.nc`, `data/*.parquet`

### Parquet

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

Point at a folder and Beacon will glob all `.parquet` files under it automatically. You can also be explicit:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/**/*.parquet'
```

### NetCDF

```sql
CREATE EXTERNAL TABLE argo
STORED AS NETCDF
LOCATION 'argo/**/*.nc'
```

### Zarr

Zarr tables should point at `zarr.json` entry files rather than a folder:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/zarr.json'
```

To span multiple Zarr stores with a glob:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/*/zarr.json'
```

### CSV

```sql
CREATE EXTERNAL TABLE station_metadata
STORED AS CSV
LOCATION 'metadata/stations/'
```

### Arrow IPC

```sql
CREATE EXTERNAL TABLE cruise_data
STORED AS ARROW
LOCATION 'cruises/'
```

### ODV ASCII

```sql
CREATE EXTERNAL TABLE odv_profiles
STORED AS ODV
LOCATION 'odv/'
```

### GeoTIFF / COG

```sql
CREATE EXTERNAL TABLE elevation
STORED AS TIFF
LOCATION 'rasters/elevation.tif'
```

## Partitioned tables

When your files are organized in Hive-style partitioned directories (`year=2024/month=01/...`), declare the partition columns with `PARTITIONED BY`. Beacon will use them for partition pruning:

```sql
CREATE EXTERNAL TABLE observations
STORED AS PARQUET
LOCATION 'obs/'
PARTITIONED BY (year, month)
```

Partition columns are encoded in the directory names and are available in queries:

```sql
SELECT * FROM observations WHERE year = 2024 AND month = 6
```

## `IF NOT EXISTS`

Prevent an error if the table is already registered:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS argo
STORED AS PARQUET
LOCATION 'argo/'
```

## Views

Views let you define a persistent SQL query over any external table or table function. See the [Views](./view.md) page for the full reference, including `UNION ALL BY NAME` for harmonizing datasets with different schemas.

## Atlas-backed tables

Atlas tables are Beacon's managed, ingestible table type. Use `CREATE ATLAS TABLE` to register one, then `INGEST INTO ATLAS` to load data into a partition.

```sql
CREATE ATLAS TABLE sensor_data
LOCATION '/collections/sensor'
```

Add data to a partition:

```sql
INGEST INTO ATLAS sensor_data ON PARTITION p0
FROM 'raw_sensor/**/*.nc'
WITH netcdf
```

Remove specific datasets from a partition:

```sql
DELETE ATLAS DATASETS 'bad_sensor.nc' FROM sensor_data ON PARTITION p0
```

Adjust a column type within a partition:

```sql
ALTER ATLAS TABLE sensor_data ON PARTITION p0
ALTER COLUMN temperature SET DATA TYPE FLOAT
```

## Removing tables

Remove a registered table from the catalog. The underlying files are not deleted.

```sql
DROP TABLE argo
```

Use `IF EXISTS` to suppress errors when the table may not be registered:

```sql
DROP TABLE IF EXISTS argo
```

## Listing and inspecting tables

List all registered tables:

```http
GET /api/tables
```

Inspect a table's columns and data types:

```http
GET /api/table-schema?table_name=ocean_profiles
```

Or with SQL:

```sql
SHOW TABLES;

DESCRIBE ocean_profiles;
```

## Format reference

| `STORED AS` value | File types                          |
| ----------------- | ----------------------------------- |
| `PARQUET`         | `.parquet`                          |
| `NETCDF`          | `.nc`, `.nc4`, `.cdf`               |
| `ZARR`            | Zarr v2 / v3 (`zarr.json`)          |
| `CSV`             | `.csv`                              |
| `ARROW`           | Arrow IPC stream (`.arrow`, `.ipc`) |
| `ODV`             | ODV ASCII spreadsheet               |
| `TIFF`            | GeoTIFF / Cloud-Optimized GeoTIFF   |
