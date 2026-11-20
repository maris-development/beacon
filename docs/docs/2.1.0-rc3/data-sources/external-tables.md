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
An **external table** points at existing files. Beacon reads them in place. Some formats also accept
writes, for example [Delta Lake](#delta-lake) and [Lance](#lance). Do you need a table that Beacon
**owns** and can change with `INSERT`, `UPDATE` and `DELETE`? Then use a
[managed table](/docs/2.1.0-rc3/sql/managed-tables).
:::

:::tip Register many datasets at once
A [crawler](/docs/2.1.0-rc3/server/crawlers) finds and registers many datasets under a prefix. It
also handles partitioned layouts. You write no DDL for each dataset.
:::

This page is a **setup guide** with an example for each format. The
[`CREATE EXTERNAL TABLE`](/docs/2.1.0-rc3/sql/create-external-table) reference gives the full
statement grammar and every clause: `OR REPLACE`, `IF NOT EXISTS`, `PARTITIONED BY`, `OPTIONS` and
`DROP TABLE`.
[Keys of each format](/docs/2.1.0-rc3/sql/create-external-table#keys-of-each-format) lists the
`OPTIONS` keys that each `STORED AS` value reads.

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
[GeoParquet](/docs/2.1.0-rc3/formats/geoparquet) for the details.

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

An Atlas table points at the `data.atlas` file itself, not at the folder around it:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/data.atlas'
```

Use a glob to put several collections in one table:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/data.atlas'
```

See [Atlas](/docs/2.1.0-rc3/formats/atlas) for the format details, its `OPTIONS` and its column
naming. That page also explains how Atlas speeds up NetCDF and Zarr work.

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
the [`read_odv_ascii()`](/docs/2.1.0-rc3/sql/table-functions#read-odv-ascii) table function.
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
[Delta Lake](/docs/2.1.0-rc3/formats/delta-lake) table. The `LOCATION` points
at the Delta **table directory**. That directory holds `_delta_log/`. Do not give a glob of files:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
```

A Delta table supports consistent snapshots. It supports **time travel** through
`OPTIONS ('version' '12')` or `('timestamp' '…')`. It also supports `INSERT INTO`, which commits a
new Delta version. See [Delta Lake](/docs/2.1.0-rc3/formats/delta-lake) for the
full reference.

### Apache Iceberg

`STORED AS ICEBERG` registers an [Apache Iceberg](/docs/2.1.0-rc3/formats/iceberg)
table that already exists. `LOCATION` gives the Iceberg **table directory**. That directory holds
`metadata/`. Give no glob of files:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS ICEBERG
LOCATION 'iceberg/ocean_profiles'
```

Beacon **reads** an Iceberg table. Another system writes the table. Beacon reads each new snapshot
and each new column. You restart nothing. `OPTIONS ('snapshot_id' '…')` pins the table to one
snapshot. Beacon writes no Iceberg table, so the table accepts no `INSERT INTO`. See
[Apache Iceberg](/docs/2.1.0-rc3/formats/iceberg) for the full reference.

### Icechunk

`STORED AS ICECHUNK` adds an [Icechunk](/docs/2.1.0-rc3/formats/icechunk) repository to the
catalog. A repository is a Zarr store with commits, branches, tags and snapshots. The repository
must exist.

Set `LOCATION` to the directory of the repository. Do not give a glob of files:

```sql
CREATE EXTERNAL TABLE sst
STORED AS ICECHUNK
LOCATION 'sst/repo'
OPTIONS ('branch' 'main')
```

`OPTIONS` selects the version. Use `branch`, `tag` or `snapshot`. Set one of the three. A tag and a
snapshot do not move. The table reads the same rows after a later commit.

The table is read-only. See [Icechunk](/docs/2.1.0-rc3/formats/icechunk) for the full reference.

### Lance

`STORED AS LANCE` registers a [Lance](https://lancedb.github.io/lance/) dataset at a location that
you select. Beacon does not own the dataset. Other tools, for example pylance, can read and write it.

```sql
CREATE EXTERNAL TABLE [IF NOT EXISTS] <name> [(<columns>)]
STORED AS LANCE
LOCATION '<location>'
[OPTIONS ('version' '<n>')]
```

A Lance external table is not read-only. It supports the same statements as a
[managed table](/docs/2.1.0-rc3/sql/managed-tables): `SELECT`, `INSERT`, `DELETE`, `UPDATE`,
`ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`, `SHOW INDEXES` and `COMPACT TABLE`.
[`INSERT INTO ... BY NAME`](/docs/2.1.0-rc3/sql/managed-tables#insert-into-by-name) also works on it.

#### Location

`LOCATION` points at the **dataset directory**. Do not give a glob of files. The location rules are
the same as for `STORED AS DELTA`:

- A relative path is in the dataset storage of Beacon.
- A path with a scheme, for example `file://` or `s3://`, uses the object store for that scheme.

Beacon uses the object store configuration of the server. A Lance table needs no other credentials.

#### Columns

The column list depends on the dataset:

| Dataset at `LOCATION` | Column list | Result |
| --- | --- | --- |
| Exists | No | Beacon reads the schema from the dataset. |
| Exists | Yes | Error. The dataset gives the columns. |
| Does not exist | Yes | Beacon creates an empty dataset with these columns. |
| Does not exist | No | Error. Beacon has no schema for a new dataset. |

Register a dataset that exists:

```sql
CREATE EXTERNAL TABLE argo_lance
STORED AS LANCE
LOCATION 's3://bucket/argo.lance'
```

Create a new empty dataset:

```sql
CREATE EXTERNAL TABLE argo_lance (platform VARCHAR, time TIMESTAMP, temp DOUBLE)
STORED AS LANCE
LOCATION 's3://bucket/argo.lance'
```

#### Pin a version

`OPTIONS ('version' '<n>')` pins the table to version `n` of the dataset. A pinned table is
**read-only**. Each write to it gives an error.

```sql
CREATE EXTERNAL TABLE argo_v1
STORED AS LANCE
LOCATION 's3://bucket/argo.lance'
OPTIONS ('version' '1')
```

`version` is the only key. An unknown key is an error. This rule is different from the file formats.

#### Remove the table

`DROP TABLE` removes only the registration. Beacon does **not** delete the dataset files. A new
`CREATE EXTERNAL TABLE` on the same location reads the old rows again.

:::warning Some statements change the dataset for all users
`DROP INDEX` and `COMPACT TABLE` change the dataset itself. `COMPACT TABLE` also deletes old
versions. Other tools that read the dataset see these changes. A table that another tool pins to an
old version can lose that version.
:::

#### Write from more than one tool

Beacon and other tools can write to the same dataset:

- In one Beacon server, a lock lets one write at a time run on a dataset. Two Beacon tables on the
  same location share that lock.
- Other writers, for example pylance or a second Beacon server, use the optimistic commit of Lance.
- Lance tries an append again after a commit conflict.
- A `DELETE` or an `UPDATE` with a commit conflict fails. Beacon returns the Lance error. Send the
  statement again.

#### Errors

| Case | Message |
| --- | --- |
| Column list on a dataset that exists | `Lance external tables take their columns from the dataset; remove the column list` |
| No dataset and no column list | `No Lance dataset at '<location>'; add a column list to create one` |
| Write to a pinned table | `Lance table '<name>' is pinned to version n and is read-only` |

## Partitioned data

Your files can use Hive-style partition directories such as `year=2024/month=01/...`. Declare the
partition columns. Beacon can then prune them at query time. The directory names hold the values.
The columns become normal queryable columns. See
[`PARTITIONED BY`](/docs/2.1.0-rc3/sql/create-external-table#partitioned-by) for the syntax.

## Remote tables

`STORED AS REMOTE` registers a table on **another Beacon server** instead of local files. A query
pushes the filters, the projection, the limits and whole joins and aggregates down to the remote
server over Arrow Flight SQL. See
[Remote Tables (Federation)](/docs/2.1.0-rc3/data-sources/remote-tables) for the full
reference.

## Views

A view holds a persistent SQL query over any external table or table function. See the
[Views](/docs/2.1.0-rc3/server/view) page for the full reference. It also covers
`UNION ALL BY NAME` for datasets with different schemas.

## Remove a table

`DROP TABLE` removes an external table from the catalog. Beacon does **not** delete the files. This
is also true for a Lance external table. See
[`DROP TABLE`](/docs/2.1.0-rc3/sql/create-external-table#drop-table).

## List and inspect tables

List every registered table:

```http
GET /api/tables
```

Inspect the columns and data types of a table:

```http
GET /api/table-schema?table_name=ocean_profiles
```

The [`CREATE EXTERNAL TABLE`](/docs/2.1.0-rc3/sql/create-external-table#querying-and-inspecting)
reference gives the SQL equivalents, `SHOW TABLES` and `DESCRIBE`.
