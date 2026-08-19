---
description: Read external files with SQL. Every supported format has a read_* table function, from Parquet and CSV to NetCDF, Zarr, Atlas and GeoTIFF.
---

# External Files

Beacon reads files directly in a `FROM` clause. Every supported format has a `read_*` table
function. The function takes a path or a glob. You register nothing first:

```sql
-- one file
SELECT * FROM read_parquet('profiles/2024.parquet') LIMIT 10;

-- a glob across many files
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 20;

-- a list of paths or globs
SELECT * FROM read_csv(['a.csv', 'b.csv']);
```

Beacon resolves a path against its storage root. That root is either a local directory or one
S3-compatible bucket, chosen at startup. See
[Object Storage](/docs/2.0.0-rc3/data-sources/object-storage).

## Formats

| Format | Function | `STORED AS` | Recognized files |
| --- | --- | --- | --- |
| [Parquet](/docs/2.0.0-rc3/formats/parquet) | `read_parquet` | `PARQUET` | `.parquet` |
| [GeoParquet](/docs/2.0.0-rc3/formats/geoparquet) | `read_geoparquet` | `GEOPARQUET` | `.geoparquet` |
| [CSV / TSV](/docs/2.0.0-rc3/formats/csv) | `read_csv` | `CSV` | `.csv`, `.tsv` |
| [Arrow IPC](/docs/2.0.0-rc3/formats/arrow) | `read_arrow` | `ARROW` | `.arrow`, `.feather` |
| [NetCDF](/docs/2.0.0-rc3/formats/netcdf) | `read_netcdf` | `NC` | `.nc` |
| [HDF5](/docs/2.0.0-rc3/formats/hdf5) | `read_hdf5` | `HDF5`, `H5` | `.h5`, `.hdf5` |
| [Zarr](/docs/2.0.0-rc3/formats/zarr) | `read_zarr` | `ZARR` | `zarr.json` marker |
| [Atlas](/docs/2.0.0-rc3/formats/atlas) | `read_atlas` | `ATLAS` | `atlas.json` marker |
| [GeoTIFF / COG](/docs/2.0.0-rc3/formats/geotiff) | `read_tiff` | `TIFF` | `.tif`, `.tiff` |
| [BBF](/docs/2.0.0-rc3/formats/bbf) | `read_bbf` | `BBF` | `.bbf` |
| [Delta Lake](/docs/2.0.0-rc3/formats/delta-lake) | `read_delta` | `DELTA` | `_delta_log/` directory |
| [Apache Iceberg](/docs/2.0.0-rc3/formats/iceberg) | `read_iceberg` | `ICEBERG` | `metadata/` directory |
| [Icechunk](/docs/2.0.0-rc3/formats/icechunk) | `read_icechunk` | `ICECHUNK` | repository directory |
| [ODV ASCII](/docs/2.0.0-rc3/formats/odv) | `read_odv_ascii` | not supported | `.txt` |

Beacon finds every format in the dataset store automatically. **Delta Lake**, **Apache Iceberg**,
**Icechunk** and **ODV ASCII** are the exception. Point a function at them. For Delta, Iceberg and
Icechunk, you can also use an external table.

## Capability matrix

The table above says how to read each format. This one says what you get.

| Format | On an S3 datasets store | Pushdown | Query output | Schema function | Array format |
| --- | --- | --- | --- | --- | --- |
| Parquet | Full | Predicate + projection, row-group pruning | Yes | `read_parquet_schema` | No |
| GeoParquet | Full | Projection; no `st_*` pushdown yet | Yes | `read_geoparquet_schema` | No |
| CSV / TSV | Full | Projection only | Yes | `read_csv_schema` | No |
| Arrow IPC | Full | Projection only | Yes | `read_arrow_schema` | No |
| NetCDF | **Anonymous only**, or full with the Rust reader | Projection + dimension selection | Yes | `read_netcdf_schema` | Yes |
| HDF5 | **Anonymous only**, or full with the Rust reader | Projection + dimension selection | Yes | `read_hdf5_schema` | Yes |
| Zarr | Full | Projection + dimension selection, chunk pruning | No | `read_zarr_schema` | Yes |
| Atlas | Full | Predicate + projection, **file-level pruning** | Yes | `read_atlas_schema` | Yes |
| GeoTIFF / COG | Full | Projection, range requests | No | `read_tiff_schema` | Yes |
| BBF | Full | Predicate + projection | No | `read_bbf_schema` | No |
| Delta Lake | Full | Predicate + projection, file skipping | No, but see below | `read_delta_schema` | No |
| Apache Iceberg | Full | Predicate + projection, file skipping | No | `read_iceberg_schema` | No |
| Icechunk | Full, see [Icechunk](/docs/2.0.0-rc3/formats/icechunk) | Projection + dimension selection, chunk pruning | No | `read_icechunk_schema` | Yes |
| ODV ASCII | Full | Projection only | Yes | `read_odv_ascii_schema` | No |

Reading the columns:

- **On an S3 datasets store** — how the reader behaves when the server's datasets store is a
  bucket rather than a local directory. NetCDF and HDF5 read through a pure-Rust reader by default.
  That reader goes through the object store and authenticates normally. A server that sets
  `BEACON_NETCDF_USE_RUST_READER=false` or `BEACON_HDF5_USE_RUST_READER=false` reads through
  netCDF-c instead. It **then needs the bucket to allow anonymous reads**: netCDF-c opens a file by
  URL and never sees the credential chain. Every other reader authenticates normally already. This is a property of the server's store, not of the
  query — paths in SQL are relative either way.
- **Pushdown** — how much of a query reaches storage instead of running after the read. *Predicate*
  means a `WHERE` clause prunes data. *Projection* means a narrow `SELECT` reads fewer columns.
  [Atlas](/docs/2.0.0-rc3/formats/atlas) is the strongest: its collection statistics drop whole
  files before any array is opened.
- **Query output** — whether a query result can be written back in that format, with `COPY TO` or
  an `output.format` on the API. Writing rows into an existing table is a different capability:
  **Delta Lake** external tables accept `INSERT INTO`, and
  [managed tables](/docs/2.0.0-rc3/sql/managed-tables) accept the full `INSERT` / `UPDATE` /
  `DELETE` set, but neither is a query output format.
- **Schema function** — the `_schema` counterpart that returns columns and types without a scan.
  See [Inspect a schema](/docs/2.0.0-rc3/formats/inspect-a-schema).
- **Array format** — whether the file holds N-dimensional arrays, which Beacon flattens into rows.
  See [Arrays to tables](/docs/2.0.0-rc3/arrays-to-tables).

## See inside a file

Check the columns and types of an unfamiliar dataset first. Every reader has a `_schema`
counterpart that does this **without a read of any data**:

```sql
SELECT * FROM read_netcdf_schema('argo/**/*.nc');
```

[`SUMMARIZE`](/docs/2.0.0-rc3/sql/summarize) also gives value ranges, distinct counts and
null shares. It profiles every column in one pass:

```sql
SUMMARIZE (SELECT * FROM read_netcdf('argo/**/*.nc'));
```

[Inspect a schema](/docs/2.0.0-rc3/formats/inspect-a-schema) compares all four ways and says what
each one costs.

## Read many files at once

A glob (`*`, `**`) expands across directories. One call can therefore cover thousands of files.
Beacon merges their schemas. It also prunes the files that cannot match your filters.

```sql
SELECT platform, avg(temperature) AS t
FROM read_netcdf('argo/**/*.nc')
WHERE depth < 100
GROUP BY platform;
```

Array formats such as [Zarr](/docs/2.0.0-rc3/formats/zarr) and
[Atlas](/docs/2.0.0-rc3/formats/atlas) use a marker file. Point at `zarr.json`
or `atlas.json`, not at the chunk files.

Some files share a schema but have different columns. Combine those files with
[`UNION BY NAME`](/docs/2.0.0-rc3/sql/union-by-name).

## Give files a table name

A `read_*` call fits an ad-hoc query. When many queries share one source, register it once as an
[external table](/docs/2.0.0-rc3/data-sources/external-tables). Then query it by name:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/';

SELECT * FROM ocean_profiles LIMIT 10;
```

## See also

- [Table Functions](/docs/2.0.0-rc3/sql/table-functions): every reader signature in one place.
- [Data Sources](/docs/2.0.0-rc3/data-sources/): the full read model.
- [Object Storage](/docs/2.0.0-rc3/data-sources/object-storage): running against a bucket.
