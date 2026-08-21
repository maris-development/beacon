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
[OPTIONS ('<key>' '<value>', ...)]
```

Beacon resolves `LOCATION` against its storage root. Give a folder or a glob pattern:

```sql
-- Entire folder
CREATE EXTERNAL TABLE argo STORED AS NC LOCATION 'argo/'

-- Explicit glob
CREATE EXTERNAL TABLE argo STORED AS NC LOCATION 'argo/**/*.nc'
```

## Formats

| `STORED AS` | File types | Format page |
| ----------- | ---------- | ----------- |
| `PARQUET` | `.parquet` | [Parquet](/docs/2.0.0-rc3/formats/parquet) |
| `GEOPARQUET` | `.geoparquet` | [GeoParquet](/docs/2.0.0-rc3/formats/geoparquet) |
| `NC` | `.nc` | [NetCDF](/docs/2.0.0-rc3/formats/netcdf) |
| `HDF5`, `H5` | `.h5`, `.hdf5` | [HDF5](/docs/2.0.0-rc3/formats/hdf5) |
| `ZARR` | Zarr v3 (`zarr.json`) | [Zarr](/docs/2.0.0-rc3/formats/zarr) |
| `ATLAS` | Atlas array store (`atlas.json`) | [Atlas](/docs/2.0.0-rc3/formats/atlas) |
| `CSV` | `.csv`, `.tsv` | [CSV](/docs/2.0.0-rc3/formats/csv) |
| `ARROW` | Arrow IPC (`.arrow`, `.feather`) | [Arrow IPC](/docs/2.0.0-rc3/formats/arrow) |
| `TIFF` | GeoTIFF / Cloud-Optimized GeoTIFF | [GeoTIFF](/docs/2.0.0-rc3/formats/geotiff) |
| `BBF` | Beacon Binary Format | [BBF](/docs/2.0.0-rc3/formats/bbf) |
| `DELTA` | Delta Lake table directory (`_delta_log/`) | [Delta Lake](/docs/2.0.0-rc3/formats/delta-lake) |
| `ICEBERG` | Apache Iceberg table directory (`metadata/`) | [Apache Iceberg](/docs/2.0.0-rc3/formats/iceberg) |
| `ICECHUNK` | Icechunk repository directory | [Icechunk](/docs/2.0.0-rc3/formats/icechunk) |
| `POSTGRES` | External PostgreSQL table (federated) | [SQL Databases](/docs/2.0.0-rc3/data-sources/sql-databases) |
| `MYSQL` | External MySQL table (federated) | [SQL Databases](/docs/2.0.0-rc3/data-sources/sql-databases) |
| `REMOTE` | A table on another Beacon server | [Remote Tables](/docs/2.0.0-rc3/data-sources/remote-tables) |

`DELTA` points at an existing
[Delta Lake](/docs/2.0.0-rc3/formats/delta-lake) table directory. It also
supports time travel and `INSERT INTO`. `ICEBERG` points to an [Apache Iceberg](/docs/2.0.0-rc3/formats/iceberg) table
directory that already exists. It supports time travel. It reads each new snapshot. It is
read-only.
`REMOTE` federates a table on another Beacon server. See
[Remote Tables](/docs/2.0.0-rc3/data-sources/remote-tables). `POSTGRES` and `MYSQL`
federate a table in an external SQL database. See
[SQL Databases](/docs/2.0.0-rc3/data-sources/sql-databases). Their `LOCATION` is the remote
table name. The connection details go in `OPTIONS`, with an encrypted `password`.

A Zarr table must point at a `zarr.json` entry file. An Atlas table must point at an `atlas.json`
marker:

```sql
CREATE EXTERNAL TABLE sst STORED AS ZARR LOCATION 'sst/*/zarr.json'

CREATE EXTERNAL TABLE sensor STORED AS ATLAS LOCATION 'sensor/atlas.json'
```

`GEOPARQUET` reads Parquet files. Beacon decodes their geometry columns to native GeoArrow. See
[GeoParquet in File Formats](/docs/2.0.0-rc3/formats/geoparquet) for the read
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

## `OPTIONS`

`OPTIONS` tunes the read of one table. Each format reads its own keys. Beacon stores the keys with
the table definition, so a restart keeps them:

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
OPTIONS ('read_dimensions' 'time,latitude,longitude')
```

The rules are the same for every format:

- Quote the key and the value. Write `'true'`, not `true`.
- A key is not case sensitive. Beacon lowercases it.
- Beacon ignores a key that the file format does not know. It reports no error.
- A key must appear one time. A repeated key is an error.
- A file-format key that takes a boolean accepts `true`, `1`, `yes` and `on`, or `false`, `0`, `no`
  and `off`. Another value is an error.
- A key with a list value takes the members separated by a comma, in one string.

Most keys have a server-wide default. The
[File formats](/docs/2.0.0-rc3/server/configuration#file-formats) settings hold those defaults. A
table option wins over the default of the server. The one exception is `enable_statistics`: Beacon
validates it and then reads the server setting alone. See the format page of each key.

### Keys of each format

| `STORED AS` | Keys | Details |
| ----------- | ---- | ------- |
| `NC` | `read_dimensions`, `use_rust_reader`, `enable_statistics` | [NetCDF](/docs/2.0.0-rc3/formats/netcdf#options) |
| `HDF5`, `H5` | `read_dimensions`, `use_rust_reader`, `enable_statistics`, `unify_phony_dimensions`, `convention` | [HDF5](/docs/2.0.0-rc3/formats/hdf5#options) |
| `ZARR` | `read_dimensions`, `enable_statistics` | [Zarr](/docs/2.0.0-rc3/formats/zarr#options) |
| `CSV` | `delimiter`, `infer_records` | [CSV](/docs/2.0.0-rc3/formats/csv#options) |
| `BBF` | `split_streams_slice` | [BBF](/docs/2.0.0-rc3/formats/bbf#options) |
| `DELTA` | `version`, `timestamp` | [Delta Lake](/docs/2.0.0-rc3/formats/delta-lake#options) |
| `ICEBERG` | `snapshot_id` | [Apache Iceberg](/docs/2.0.0-rc3/formats/iceberg#options) |
| `ICECHUNK` | `branch`, `tag`, `snapshot`, `read_dimensions` | [Icechunk](/docs/2.0.0-rc3/formats/icechunk#options) |
| `POSTGRES`, `MYSQL` | `host`, `port`, `user`, `password`, `database`, `sslmode` | [SQL Databases](/docs/2.0.0-rc3/data-sources/sql-databases#options) |
| `REMOTE` | `tls` | [Remote Tables](/docs/2.0.0-rc3/data-sources/remote-tables#options) |
| `PARQUET`, `GEOPARQUET`, `ARROW`, `TIFF` | None | |

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

The [External Tables](/docs/2.0.0-rc3/data-sources/external-tables) setup guide gives an
example for each format. It also shows the HTTP API that lists the tables.
