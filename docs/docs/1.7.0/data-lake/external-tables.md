# External Tables

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

An external table is a standard SQL table backed by files in Beacon's storage. Once created, you can `SELECT`, `JOIN`, and `DROP` it like any other table — Beacon reads the underlying files on demand without copying them. Table definitions are persisted automatically and survive restarts.

:::tip External vs managed tables
An **external table** only points at existing files — Beacon reads them in place and never writes to them. If you want a table Beacon **owns** and can mutate with `INSERT` / `UPDATE` / `DELETE`, use a [managed table](../sql/managed-tables.md) instead.
:::

This page is a **setup guide** with per-format examples. For the full statement grammar and every clause (`OR REPLACE`, `IF NOT EXISTS`, `PARTITIONED BY`, `DROP TABLE`), see the [`CREATE EXTERNAL TABLE`](../sql/create-table.md) reference.

DDL can be submitted through any of Beacon's SQL surfaces:

- **HTTP** — `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ..." }`
- **Arrow Flight SQL** — any Flight SQL client (DataGrip, ADBC, DBeaver, …)

:::info
SQL must be enabled (`BEACON_ENABLE_SQL=true`) to run DDL statements over the HTTP API. Arrow Flight SQL does not require this flag.
:::

## Where files live

The `LOCATION` is resolved relative to Beacon's configured dataset storage root (`/beacon/data/datasets` in the default Docker container, or the S3 prefix when using object storage). It may be:

- A folder path — Beacon scans all matching files inside it
- A glob pattern — e.g. `argo/**/*.nc`, `data/*.parquet`

## Formats

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

### Atlas

Like Zarr, Atlas tables point at the store's `atlas.json` marker file rather than a folder:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/atlas.json'
```

To combine several Atlas stores under one table, use a glob over their markers:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/atlas.json'
```

See [Atlas](./datasets.md#atlas) for what the format does and how it speeds up NetCDF/Zarr workloads.

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

## Partitioned data

If your files are laid out in Hive-style partition directories (`year=2024/month=01/...`), declare the partition columns so Beacon can prune them at query time. The columns are encoded in the directory names and become queryable columns. See [`PARTITIONED BY`](../sql/create-table.md#partitioned-by) for the syntax.

## Views

Views let you define a persistent SQL query over any external table or table function. See the [Views](./view.md) page for the full reference, including `UNION ALL BY NAME` for harmonizing datasets with different schemas.

## Removing a table

Dropping an external table removes it from the catalog — the underlying files are **not** deleted. See [`DROP TABLE`](../sql/create-table.md#drop-table).

## Listing and inspecting tables

List all registered tables:

```http
GET /api/tables
```

Inspect a table's columns and data types:

```http
GET /api/table-schema?table_name=ocean_profiles
```

For the SQL equivalents (`SHOW TABLES`, `DESCRIBE`), see the [`CREATE EXTERNAL TABLE`](../sql/create-table.md#querying-and-inspecting) reference.
