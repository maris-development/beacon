# CREATE EXTERNAL TABLE

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

An external table is a SQL table over files in the storage of Beacon. After you register it, you can
query it with `SELECT` and `JOIN`. A `VIEW` can also reference it. Beacon reads the files on demand.
It does not copy them. A table definition survives a restart.

For a table that Beacon owns and writes, see [`CREATE TABLE`](/docs/2.1.0-rc3/sql/managed-tables).

## Syntax

```sql
CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] <table_name>
STORED AS <format>
LOCATION '<path>'
[PARTITIONED BY (<col>, ...)]
[OPTIONS ('<key>' '<value>', ...)]
```

::: tip Names keep their case
`<table_name>` means exactly what you write. `MyTable` and `mytable` are two different tables.
See [Identifiers and case](/docs/2.1.0-rc3/sql/identifiers).
:::

Beacon resolves `LOCATION` against its storage root. Give a folder or a glob pattern:

```sql
-- Entire folder
CREATE EXTERNAL TABLE cruises STORED AS NC LOCATION 'cruises/'

-- Explicit glob
CREATE EXTERNAL TABLE cruises STORED AS NC LOCATION 'cruises/**/*.nc'
```

## Formats

| `STORED AS` | File types | Format page |
| ----------- | ---------- | ----------- |
| `PARQUET` | `.parquet` | [Parquet](/docs/2.1.0-rc3/formats/parquet) |
| `GEOPARQUET` | `.geoparquet` | [GeoParquet](/docs/2.1.0-rc3/formats/geoparquet) |
| `NC` | `.nc` | [NetCDF](/docs/2.1.0-rc3/formats/netcdf) |
| `HDF5`, `H5` | `.h5`, `.hdf5` | [HDF5](/docs/2.1.0-rc3/formats/hdf5) |
| `ZARR` | Zarr v3 (`zarr.json`) | [Zarr](/docs/2.1.0-rc3/formats/zarr) |
| `CSV` | `.csv`, `.tsv` | [CSV](/docs/2.1.0-rc3/formats/csv) |
| `ARROW` | Arrow IPC (`.arrow`, `.feather`) | [Arrow IPC](/docs/2.1.0-rc3/formats/arrow) |
| `TIFF` | GeoTIFF / Cloud-Optimized GeoTIFF | [GeoTIFF](/docs/2.1.0-rc3/formats/geotiff) |
| `DELTA` | Delta Lake table directory (`_delta_log/`) | [Delta Lake](/docs/2.1.0-rc3/formats/delta-lake) |
| `ICEBERG` | Apache Iceberg table directory (`metadata/`) | [Apache Iceberg](/docs/2.1.0-rc3/formats/iceberg) |
| `ICECHUNK` | Icechunk repository directory | [Icechunk](/docs/2.1.0-rc3/formats/icechunk) |
| `POSTGRES` | External PostgreSQL table (federated) | [SQL Databases](/docs/2.1.0-rc3/data-sources/sql-databases) |
| `MYSQL` | External MySQL table (federated) | [SQL Databases](/docs/2.1.0-rc3/data-sources/sql-databases) |
| `REMOTE` | A table on another Beacon server | [Remote Tables](/docs/2.1.0-rc3/data-sources/remote-tables) |
| `ERDDAP` | ERDDAP tabledap dataset URL | [ERDDAP](/docs/2.1.0-rc3/data-sources/erddap) |

`DELTA` points at an existing
[Delta Lake](/docs/2.1.0-rc3/formats/delta-lake) table directory. It also
supports time travel and `INSERT INTO`. `ICEBERG` points to an [Apache Iceberg](/docs/2.1.0-rc3/formats/iceberg) table
directory that already exists. It supports time travel. It reads each new snapshot. It is
read-only.
`REMOTE` federates a table on another Beacon server. See
[Remote Tables](/docs/2.1.0-rc3/data-sources/remote-tables). `POSTGRES` and `MYSQL`
federate a table in an external SQL database. See
[SQL Databases](/docs/2.1.0-rc3/data-sources/sql-databases). Their `LOCATION` is the remote
table name. The connection details go in `OPTIONS`, with an encrypted `password`.

A Zarr table must point at a `zarr.json` entry file:

```sql
CREATE EXTERNAL TABLE sst STORED AS ZARR LOCATION 'sst/*/zarr.json'
```

`GEOPARQUET` reads Parquet files. Beacon decodes their geometry columns to native GeoArrow. See
[GeoParquet in File Formats](/docs/2.1.0-rc3/formats/geoparquet) for the read
behaviour and for geometry queries.

## `IF NOT EXISTS`

Beacon skips the registration if the table name already exists. Beacon returns no error:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS cruises
STORED AS NC
LOCATION 'cruises/**/*.nc'
```

## `OR REPLACE`

Register the table again. Beacon overwrites the existing definition:

```sql
CREATE OR REPLACE EXTERNAL TABLE cruises
STORED AS NC
LOCATION 'cruises/**/*.nc'
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

The value comes from the directory name of each file.

`NC`, `HDF5`, `TIFF` and `ZARR` do not support this clause. A query on such a table gives an error. A Zarr table
holds groups inside one store, not files in directories, so a path holds no partition value.

## `OPTIONS`

`OPTIONS` tunes the read of one table. Each format reads its own keys. Beacon stores the keys with
the table definition, so a restart keeps them:

```sql
CREATE EXTERNAL TABLE cruises
STORED AS NC
LOCATION 'cruises/**/*.nc'
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
[File formats](/docs/2.1.0-rc3/server/configuration#file-formats) settings hold those defaults. A
table option wins over the default of the server. See the format page of each key.

### Keys of each table

These keys apply to a table of each file format:

| Key | Default | Description |
| --- | ------- | ----------- |
| `use_statistics_pruning` | `false` | Keep the [file statistics](/docs/2.1.0-rc3/internals/file-statistics) of this table, and prune the files of a query on them. |
| `statistics_columns` | each column with a range | The columns to keep a range for, separated by a comma. A table with more than 1,000 range columns needs this key. |
| `statistics_refresh_interval` | `900` | The seconds between two refreshes of the statistics of this table. |
| `skip_invalid_files_on_scan` | `false` | Skip a file that a query cannot open or read. Without it the query fails, and the error names the file. See [Skip invalid files](#skip-invalid-files). |
| `skip_invalid_files_on_inference` | `false` | Skip a file that schema inference cannot read. Without it `CREATE EXTERNAL TABLE` fails, and the error names the file. See [Skip invalid files](#skip-invalid-files). |
| `parallel_schema_inference` | `false` | Infer the schemas of the files on parallel tasks. See [Schema inference](#schema-inference). |
| `type_widening` | `default` | The rule that merges a column with two types across the files: `default` or `numpy`. See [Schema inference](#schema-inference). |
| `type_widening_on_conflict` | `fail` | What the merge does with two types that the rule cannot merge: `fail` or `keep_first`. See [Schema inference](#schema-inference). |

A value that a key of this table cannot read is an error. `'type_widening' 'polars'` is an example.

### Skip invalid files

A collection can hold a file that Beacon cannot read. A damaged file is an example. By default, that
file fails each query that reads it:

```
external table 'cruises' (netcdf): cannot read file 'cruises/2019/a.nc': ...
```

The error names the table, the file type and the file. It also tells if the open or the read of the
file failed. Two keys skip such a file:

```sql
CREATE EXTERNAL TABLE cruises
STORED AS NC
LOCATION 'cruises/**/*.nc'
OPTIONS (
  'skip_invalid_files_on_inference' 'true',
  'skip_invalid_files_on_scan' 'true'
)
```

| Key | Applies to | Effect |
| --- | ---------- | ------ |
| `skip_invalid_files_on_inference` | `CREATE EXTERNAL TABLE` and [`REFRESH`](#refresh) | Beacon merges the schemas of the files that it can read. The statement fails only if Beacon can read no file. |
| `skip_invalid_files_on_scan` | Each query on the table | The file ends at its error. The query continues with the next file. |

The two keys do not depend on each other. A table with only `skip_invalid_files_on_inference` still
fails a query that reads the invalid file.

Beacon writes a `warn` log line for each file that it skips. The line names the file and the error.
`EXPLAIN ANALYZE` shows the number of files that a query skipped as `skipped_file_errors`.

::: warning A skip removes rows without an error
A query on a table with `skip_invalid_files_on_scan` returns no error for an invalid file. The result
does not hold the rows of that file. A read can fail after the first rows of a file. The result then
keeps those first rows. Examine the server log to find the files that Beacon skipped.
:::

A schema inference always skips an empty file. This needs no key.

### Schema inference

Beacon infers the schema of a table from its files when you create the table. It reads the schema of
each file and merges the schemas in path order. Beacon stores the result with the table definition.
[`REFRESH`](#refresh) infers the schema again.

Two files can give one column two types. `type_widening` sets the rule that merges them:

- `default` widens inside one type family: a wider integer, a finer timestamp, a longer string.
- `numpy` promotes as `numpy.result_type` does. A number beside a string reads as text.

`type_widening_on_conflict` sets what Beacon does with two types that the rule cannot merge:

- `fail` refuses the table. The error names the column, the two types and the two files.
- `keep_first` keeps the type of the first file in path order. Beacon casts each other file to that
  type. A value that the type cannot hold reads as `NULL`.

```sql
CREATE EXTERNAL TABLE cruises
STORED AS NC
LOCATION 'cruises/**/*.nc'
OPTIONS ('type_widening' 'numpy', 'type_widening_on_conflict' 'keep_first')
```

An external table takes these two rules from its own `OPTIONS`. The server variables
`BEACON_TYPE_WIDENING_STRATEGY` and `BEACON_TYPE_WIDENING_ON_CONFLICT` do not change them.
[Configuration](/docs/2.1.0-rc3/server/configuration#query-engine) gives the full rule of each
value.

By default, one task infers the schemas of the files. The file reads overlap, but the CPU work uses
one core. `'parallel_schema_inference' 'true'` gives each file its own task, so the CPU work uses
more cores. Set it for a table with many files. The merged schema does not change.

### Keys of each format

| `STORED AS` | Keys | Details |
| ----------- | ---- | ------- |
| `NC` | `read_dimensions`, `skip_unbroadcastable`, `skip_invalid_files`, `enable_statistics` | [NetCDF](/docs/2.1.0-rc3/formats/netcdf#options) |
| `HDF5`, `H5` | `read_dimensions`, `enable_statistics`, `unify_phony_dimensions`, `convention` | [HDF5](/docs/2.1.0-rc3/formats/hdf5#options) |
| `ZARR` | `read_dimensions`, `enable_statistics` | [Zarr](/docs/2.1.0-rc3/formats/zarr#options) |
| `CSV` | `delimiter`, `infer_records` | [CSV](/docs/2.1.0-rc3/formats/csv#options) |
| `DELTA` | `version`, `timestamp` | [Delta Lake](/docs/2.1.0-rc3/formats/delta-lake#options) |
| `ICEBERG` | `snapshot_id` | [Apache Iceberg](/docs/2.1.0-rc3/formats/iceberg#options) |
| `ICECHUNK` | `branch`, `tag`, `snapshot`, `read_dimensions` | [Icechunk](/docs/2.1.0-rc3/formats/icechunk#options) |
| `POSTGRES`, `MYSQL` | `host`, `port`, `user`, `password`, `database`, `sslmode` | [SQL Databases](/docs/2.1.0-rc3/data-sources/sql-databases#options) |
| `REMOTE` | `tls` | [Remote Tables](/docs/2.1.0-rc3/data-sources/remote-tables#options) |
| `ERDDAP` | `request_timeout_secs` | [ERDDAP](/docs/2.1.0-rc3/data-sources/erddap#options) |
| `PARQUET`, `GEOPARQUET`, `ARROW`, `TIFF` | None | |

## `REFRESH`

`REFRESH` lists the files of an external table again. If Beacon inferred the schema of the table, it
infers the schema again. A new column in a new file then shows in the table. Beacon stores the new
schema.

```sql
REFRESH cruises

REFRESH TABLE cruises
```

The two statements do the same refresh. Only the admin can run `REFRESH`.

## `DROP TABLE`

`DROP TABLE` removes a table from the catalog. Beacon does not delete the files. It deletes the file
statistics of the table.

```sql
DROP TABLE cruises

DROP TABLE IF EXISTS cruises
```

## Querying and inspecting

```sql
SHOW TABLES;

DESCRIBE ocean_profiles;

SHOW CREATE TABLE ocean_profiles;
```

`SHOW CREATE TABLE` returns the statement that created the table. Beacon stores the statement
when you create the table. It masks each secret option value, such as a password or an access key,
as `'***'`. The `definition` column is null for a table that has no stored statement. A table that
a crawler creates has no stored statement. Only the admin can run `SHOW CREATE TABLE`.

The [External Tables](/docs/2.1.0-rc3/data-sources/external-tables) setup guide gives an
example for each format. It also shows the HTTP API that lists the tables.
