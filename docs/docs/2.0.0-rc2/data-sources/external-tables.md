# Create External Tables

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

An external table is a standard SQL table over files in the storage of Beacon. After you create it,
you can run `SELECT`, `JOIN` and `DROP` on it, like any other table. Beacon reads the files on
demand. It does not copy them. Beacon stores the table definition automatically. The definition
survives a restart.

:::tip External vs managed tables
An **external table** points at existing files. Beacon reads them in place. Beacon never writes to
them. Do you need a table that Beacon **owns** and can change with `INSERT`, `UPDATE` and `DELETE`?
Then use a [managed table](/docs/2.0.0-rc2/sql/managed-tables).
:::

:::tip Register many datasets at once
A [crawler](/docs/2.0.0-rc2/server/crawlers) finds and registers many datasets under a prefix. It
also handles partitioned layouts. You write no DDL for each dataset.
:::

This page is a **setup guide** with an example for each format. The
[`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc2/sql/create-table) reference gives the full
statement grammar and every clause: `OR REPLACE`, `IF NOT EXISTS`, `PARTITIONED BY` and
`DROP TABLE`.

You can send the DDL through any SQL interface of Beacon:

- **HTTP**: `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ..." }`
- **Arrow Flight SQL**: any Flight SQL client, such as DataGrip, ADBC or DBeaver

:::info
DDL over the HTTP API needs the SQL interface. That interface is on by default
(`BEACON_ENABLE_SQL`). Arrow Flight SQL does not need this flag.
:::

## Where files live

Beacon resolves the `LOCATION` against its dataset storage root. In the default Docker container the
root is `/beacon/data/datasets`. On object storage the root is the S3 prefix. The `LOCATION` takes
two forms:

- A folder path. Beacon scans every file in it.
- A glob pattern, for example `argo/**/*.nc` or `data/*.parquet`.

## Formats

### Parquet

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

Point at a folder. Beacon then finds every `.parquet` file under it. You can also give the glob:

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

Beacon decodes the geometry columns to native [GeoArrow](https://geoarrow.org/) on read. See
[GeoParquet](/docs/2.0.0-rc2/formats/geoparquet) for the details.

### NetCDF

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
```

### Zarr

A Zarr table must point at a `zarr.json` entry file, not at a folder:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/zarr.json'
```

Use a glob to cover several Zarr stores:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/*/zarr.json'
```

### Atlas

An Atlas table points at the `atlas.json` marker file, not at a folder. This is the same as Zarr:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/atlas.json'
```

Use a glob over the markers to put several Atlas stores in one table:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/atlas.json'
```

See [Atlas](/docs/2.0.0-rc2/formats/atlas) for the format details. That page
also explains how Atlas speeds up NetCDF and Zarr work.

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

ODV ASCII is **not** an external table format. There is no `STORED AS ODV`. Read an ODV file with
the [`read_odv_ascii()`](/docs/2.0.0-rc2/sql/table-functions#read-odv-ascii) table function.
The `odv` source in the JSON query API also works:

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

`STORED AS DELTA` registers an existing
[Delta Lake](/docs/2.0.0-rc2/formats/delta-lake) table. The `LOCATION` points
at the Delta **table directory**. That directory holds `_delta_log/`. Do not give a glob of files:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
```

A Delta table supports consistent snapshots. It supports **time travel** through
`OPTIONS ('version' '12')` or `('timestamp' '…')`. It also supports `INSERT INTO`, which commits a
new Delta version. See [Delta Lake](/docs/2.0.0-rc2/formats/delta-lake) for the
full reference.

## Partitioned data

Your files can use Hive-style partition directories such as `year=2024/month=01/...`. Declare the
partition columns. Beacon can then prune them at query time. The directory names hold the values.
The columns become normal queryable columns. See
[`PARTITIONED BY`](/docs/2.0.0-rc2/sql/create-table#partitioned-by) for the syntax.

## Remote tables

`STORED AS REMOTE` registers a table on **another Beacon server** instead of local files. A query
pushes the filters, the projection, the limits and whole joins and aggregates down to the remote
server over Arrow Flight SQL. See
[Remote Tables (Federation)](/docs/2.0.0-rc2/data-sources/remote-tables) for the full
reference.

## Views

A view holds a persistent SQL query over any external table or table function. See the
[Views](/docs/2.0.0-rc2/server/view) page for the full reference. It also covers
`UNION ALL BY NAME` for datasets with different schemas.

## Remove a table

`DROP TABLE` removes an external table from the catalog. Beacon does **not** delete the files. See
[`DROP TABLE`](/docs/2.0.0-rc2/sql/create-table#drop-table).

## List and inspect tables

List every registered table:

```http
GET /api/tables
```

Inspect the columns and data types of a table:

```http
GET /api/table-schema?table_name=ocean_profiles
```

The [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc2/sql/create-table#querying-and-inspecting)
reference gives the SQL equivalents, `SHOW TABLES` and `DESCRIBE`.
