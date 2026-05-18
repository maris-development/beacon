# Supported Formats

Beacon discovers datasets from its configured storage root automatically — no registration step is required. Place files in the datasets folder (or S3 prefix) and they become immediately queryable via table functions or [External Tables](./external-tables.md).

Default local path inside the Docker container: `/beacon/data/datasets/`

## Parquet

Native support via DataFusion. Recommended for analytical workloads due to columnar storage and built-in predicate pushdown.

- Column pruning and predicate pushdown are fully supported.
- Hive-style directory partitioning is supported via `PARTITIONED BY` on [External Tables](./external-tables.md).
- Compatible with files produced by DuckDB, Spark, pandas, and similar tools.

## NetCDF

Streaming reads with chunk-level access — large files are read incrementally rather than loaded entirely into memory.

Supported dialects:

- **NetCDF4** (recommended)
- **NetCDF3** — `char*` arrays with a string-like dimension (e.g. `STRLEN`) are inferred as fixed-length strings

### Variable attributes

NetCDF variables carry metadata attributes (e.g. `units`, `long_name`, `valid_min`). Beacon exposes these as extra columns using dot notation: `<variable>.<attribute>`. For example, a variable `temperature` with a `units` attribute is accessible as the column `temperature.units`.

Attribute columns preserve the original type (string, integer, float, …) as stored in the file. They are available alongside the variable columns in every query.

File-level global attributes are exposed with a leading dot and no variable prefix: `.<attribute>`. For example, a global attribute `source` is accessible as the column `.source`.

```sql
SELECT temperature, "temperature.units", "temperature.long_name", ".source"
FROM read_netcdf(['argo/**/*.nc'])
LIMIT 1
```

Limitations:

- User-defined types are not supported.
- S3 / object-store backends only support anonymous access. Authenticated S3 reads are not yet supported.

:::tip
For best performance with NetCDF, convert files to [Beacon Binary Format](#beacon-binary-format) using the beacon-binary-format-toolbox. BBF supports efficient chunk-level pruning and full S3 authentication.
:::

## Zarr

Zarr datasets are queried using chunk-level predicate pushdown — Beacon reads only the chunks that can satisfy the query predicates, which makes spatial and temporal range queries fast even over large multi-dimensional stores.

- Zarr v2 and v3 are supported.
- Compressed chunks (zstd, gzip, blosc, …) are decompressed transparently.
- Multiple Zarr stores can be combined in a single external table using glob patterns (e.g. `sst/*/zarr.json`).
- S3 and other object-store backends are fully supported.

### Array attributes

Zarr arrays store per-array attributes in `.zattrs` (v2) or the `attributes` section of `zarr.json` (v3). Beacon exposes these as extra columns using dot notation: `<array>.<attribute>`. For example, an array `sst` with a `units` attribute is accessible as the column `sst.units`.

Attribute columns preserve the original type (string, integer, float, …) as stored in the file.

Root-level store attributes (not tied to a specific array) are exposed with a leading dot and no array prefix: `.<attribute>`. For example, a root attribute `Conventions` is accessible as the column `.Conventions`.

```sql
SELECT sst, "sst.units", "sst.long_name", ".Conventions"
FROM read_zarr(['sst/*/zarr.json'])
LIMIT 1
```

Limitations:

- User-defined data types are not supported.

:::tip
Declare `statistics_columns` in the `read_zarr()` table function or configure statistics on an external table to enable 1D slice pushdown for large coordinate dimensions like `time`, `latitude`, and `longitude`.
:::

## Arrow IPC

Fully supported. Arrow IPC stream files (`.arrow`, `.ipc`) are read natively with zero-copy column access.

## ODV ASCII

Fully supported. Storing ODV ASCII files with zstd compression is recommended to reduce storage and I/O:

```bash
zstd -9 < input.txt > output.txt.zst
```

Beacon detects compression automatically and decompresses on the fly. zstd-compressed ODV files work with S3-backed storage.

## CSV

- The first row must be a header row containing column names.
- Files must be UTF-8 encoded.
- Schema is inferred from the file contents.

## GeoTIFF / Cloud-Optimized GeoTIFF

Raster data in GeoTIFF and Cloud-Optimized GeoTIFF (COG) formats is supported. COG files are particularly efficient over S3 because Beacon can issue range requests to read only the required tiles.

### Tag attributes

GeoTIFF files carry TIFF tags and GeoTIFF metadata (e.g. `nodata`, `crs`, `scale`). Beacon exposes these per-band as extra columns using dot notation: `<band>.<attribute>`. For example, a band column `band_1` with a `nodata` tag is accessible as `band_1.nodata`.

Attribute columns preserve the original type (string, integer, float, …) as stored in the file.

File-level tags that are not tied to a specific band are exposed with a leading dot and no band prefix: `.<attribute>`. For example, a file-level `crs` tag is accessible as the column `.crs`.

```sql
SELECT band_1, "band_1.nodata", "band_1.scale", ".crs"
FROM read_tiff(['rasters/elevation.tif'])
LIMIT 1
```

## Beacon Binary Format (BBF)

Beacon's own columnar format, optimized for the kinds of queries common in earth-science and oceanographic workloads.

- Full S3 / object-store support with authenticated access.
- Chunk-level predicate pruning similar to Parquet row-group filtering.
- Efficient for repeated range queries over coordinate columns (time, depth, lat/lon).

Convert existing NetCDF files to BBF using the beacon-binary-format-toolbox for significant query speedups on large collections.
