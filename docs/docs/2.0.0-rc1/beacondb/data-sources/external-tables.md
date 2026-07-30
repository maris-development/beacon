# Creating External Tables

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

An external table is a standard SQL table backed by files in Beacon's storage. Once created, you can `SELECT`, `JOIN`, and `DROP` it like any other table, Beacon reads the underlying files on demand without copying them. Table definitions are persisted automatically and survive restarts.

:::tip External vs managed tables
An **external table** only points at existing files, Beacon reads them in place and never writes to them. If you want a table Beacon **owns** and can mutate with `INSERT` / `UPDATE` / `DELETE`, use a [managed table](/docs/2.0.0-rc1/beacondb/sql/managed-tables) instead.
:::

:::tip Registering many datasets at once
To auto-discover and register many datasets under a prefix, including partitioned layouts, without writing DDL for each one, use a [crawler](/docs/2.0.0-rc1/data-lake/crawlers).
:::

This page is a **setup guide** with per-format examples. For the full statement grammar and every clause (`OR REPLACE`, `IF NOT EXISTS`, `PARTITIONED BY`, `DROP TABLE`), see the [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc1/beacondb/sql/create-table) reference.

DDL can be submitted through any of Beacon's SQL surfaces:

- **HTTP**: `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ..." }`
- **Arrow Flight SQL**: any Flight SQL client (DataGrip, ADBC, DBeaver, …)

:::info
Running DDL over the HTTP API needs the SQL interface, which is enabled by default (`BEACON_ENABLE_SQL`). Arrow Flight SQL does not require this flag.
:::

## Where files live

The `LOCATION` is resolved relative to Beacon's configured dataset storage root (`/beacon/data/datasets` in the default Docker container, or the S3 prefix when using object storage). It may be:

- A folder path, Beacon scans all matching files inside it
- A glob pattern, e.g. `argo/**/*.nc`, `data/*.parquet`

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

### GeoParquet

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet'
```

Geometry columns are decoded to their native [GeoArrow](https://geoarrow.org/) representation on read. See [GeoParquet](/docs/2.0.0-rc1/beacondb/data-sources/formats/geoparquet) for details.

### NetCDF

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
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

See [Atlas](/docs/2.0.0-rc1/beacondb/data-sources/formats/atlas) for what the format does and how it speeds up NetCDF/Zarr workloads.

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

ODV ASCII is **not** an external-table format, there is no `STORED AS ODV`. Read
ODV files directly with the [`read_odv_ascii()`](/docs/2.0.0-rc1/beacondb/sql/table-functions#read-odv-ascii)
table function (or the `odv` source in the JSON query API):

```sql
SELECT * FROM read_odv_ascii('odv/*.txt') LIMIT 100;
```

### GeoTIFF / COG

```sql
CREATE EXTERNAL TABLE elevation
STORED AS TIFF
LOCATION 'rasters/elevation.tif'
```

### Delta Lake

`STORED AS DELTA` registers an existing [Delta Lake](/docs/2.0.0-rc1/beacondb/data-sources/formats/delta-lake) table. The `LOCATION` points at the Delta **table directory** (the folder containing `_delta_log/`), not a glob of files:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
```

Delta tables support snapshot-consistent reads, **time travel** (via `OPTIONS ('version' '12')` or `('timestamp' '…')`), and `INSERT INTO`, which commits a new Delta version. See [Delta Lake](/docs/2.0.0-rc1/beacondb/data-sources/formats/delta-lake) for the full reference.

## Partitioned data

If your files are laid out in Hive-style partition directories (`year=2024/month=01/...`), declare the partition columns so Beacon can prune them at query time. The columns are encoded in the directory names and become queryable columns. See [`PARTITIONED BY`](/docs/2.0.0-rc1/beacondb/sql/create-table#partitioned-by) for the syntax.

## Remote tables

`STORED AS REMOTE` registers a table that points at a table on **another Beacon instance** instead of at local files. Queries push filters, projection, limits, and whole joins/aggregates down to the remote over Arrow Flight SQL. See [Remote Tables (Federation)](/docs/2.0.0-rc1/beacondb/data-sources/remote-tables) for the full reference.

## Views

Views let you define a persistent SQL query over any external table or table function. See the [Views](/docs/2.0.0-rc1/data-lake/view) page for the full reference, including `UNION ALL BY NAME` for harmonizing datasets with different schemas.

## Removing a table

Dropping an external table removes it from the catalog, the underlying files are **not** deleted. See [`DROP TABLE`](/docs/2.0.0-rc1/beacondb/sql/create-table#drop-table).

## Listing and inspecting tables

List all registered tables:

```http
GET /api/tables
```

Inspect a table's columns and data types:

```http
GET /api/table-schema?table_name=ocean_profiles
```

For the SQL equivalents (`SHOW TABLES`, `DESCRIBE`), see the [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc1/beacondb/sql/create-table#querying-and-inspecting) reference.
