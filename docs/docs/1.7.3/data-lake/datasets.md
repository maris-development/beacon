# Supported Formats

Beacon discovers datasets from its configured storage root automatically — no registration step is required. Place files in the datasets folder (or S3 prefix) and they become immediately queryable via table functions or [External Tables](./external-tables.md).

Default local path inside the Docker container: `/beacon/data/datasets/`

## Format support matrix

| Format | Recognized files | `STORED AS` | `read_*` function | Output format |
| --- | --- | --- | --- | --- |
| Parquet | `.parquet` | `PARQUET` | `read_parquet` | ✅ |
| GeoParquet | `.geoparquet` | `GEOPARQUET` | `read_geoparquet` | ✅ |
| CSV / TSV | `.csv`, `.tsv` | `CSV` | `read_csv` | ✅ |
| Arrow IPC | `.arrow`, `.feather` | `ARROW` | `read_arrow` | ✅ (`ipc`) |
| NetCDF | `.nc` | `NETCDF` | `read_netcdf` | ✅ (+ ND-NetCDF) |
| Zarr | `zarr.json` marker | `ZARR` | `read_zarr` | — |
| Atlas | `atlas.json` marker | `ATLAS` | `read_atlas` | — |
| GeoTIFF / COG | `.tiff`, `.tif` | `TIFF` | `read_tiff` | — |
| BBF | `.bbf` | `BBF` | `read_bbf` | — |
| Delta Lake | `_delta_log/` directory | `DELTA` | `read_delta` | — |
| ODV ASCII | `.txt` | — | `read_odv_ascii` | ✅ |

Every format above is **auto-discovered** from the datasets store except **Delta
Lake** and **ODV ASCII** — point a [`read_*` function](../sql/table-functions.md)
(or, for Delta, `CREATE EXTERNAL TABLE … STORED AS DELTA LOCATION …`) at those
directly. "Output format" marks formats a query result can be exported to via
[`output.format`](../api/querying/index.md#output-formats).

## Parquet

Native support via DataFusion. Recommended for analytical workloads due to columnar storage and built-in predicate pushdown.

- Column pruning and predicate pushdown are fully supported.
- Hive-style directory partitioning is supported via `PARTITIONED BY` on [External Tables](./external-tables.md).
- Compatible with files produced by DuckDB, Spark, pandas, and similar tools.

## GeoParquet

[GeoParquet](https://geoparquet.org/) files (`.geoparquet`) are Parquet files that carry geospatial geometry columns and a `geo` metadata key. Beacon reads them in addition to writing them. See the dedicated [GeoParquet chapter](./geoparquet.md) for setup details and the [GeoParquet SQL chapter](../sql/geoparquet.md) for querying geometry.

- Geometry columns described in the file's `geo` metadata are decoded to their native [GeoArrow](https://geoarrow.org/) representation on read (a non-geospatial Parquet file is read like ordinary Parquet).
- Column projection is applied — only the columns a query selects are materialized.
- Works over local disk and S3-compatible object stores.

Query a GeoParquet file with the [`read_geoparquet()`](../sql/table-functions.md#read_geoparquet) table function:

```sql
SELECT * FROM read_geoparquet(['spatial/**/*.geoparquet']) LIMIT 100
```

Or register a stable table name with an [External Table](./external-tables.md):

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet';

SELECT * FROM stations LIMIT 10;
```

:::tip
Beacon can also *write* GeoParquet: a query result with longitude/latitude columns is mapped into a geometry column on output. See [querying output formats](../api/querying/index.md).
:::

:::warning
Spatial bounding-box pruning (row-group skipping via the GeoParquet `bbox` covering) is not yet applied on read — queries perform a full scan with column projection. Geometry-aware predicate pushdown is planned.
:::

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
For best performance with large NetCDF collections, convert the files into a single [Atlas](#atlas) collection. Atlas consolidates many NetCDF files into one statistics-aware array store, so Beacon can prune whole datasets and read only the projected arrays — typically much faster than scanning the original NetCDF files.
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
Predicate pushdown is automatic — Beacon prunes chunks and slices coordinate dimensions like `time`, `latitude`, and `longitude` based on your query's filters, with nothing to configure.

For collections that are queried repeatedly, convert the Zarr stores into a single [Atlas](#atlas) collection — a statistics-aware array store that lets Beacon prune whole datasets before any chunk is read.
:::

## Arrow IPC

Fully supported. Arrow IPC stream files (`.arrow`, `.feather`) are read natively with zero-copy column access.

## ODV ASCII

Supported via the [`read_odv_ascii()`](../sql/table-functions.md#read_odv_ascii)
table function (and the `odv` source in the JSON query API). Unlike the formats
above, ODV is **not** auto-discovered as a dataset and has no `CREATE EXTERNAL
TABLE ... STORED AS ODV` form — point `read_odv_ascii()` at the files directly:

```sql
SELECT * FROM read_odv_ascii('odv/*.txt') LIMIT 100;
```

Storing ODV ASCII files with zstd compression is recommended to reduce storage and I/O:

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

## Atlas

[Atlas](https://github.com/maris-development/atlas) is a directory-based array store designed for fast analytical access to multi-dimensional scientific data. Like Parquet or Zarr, it is just another file format: place Atlas stores in the datasets folder and Beacon discovers and queries them automatically — no registration step is required. An Atlas store is a directory containing a single `atlas.json` registry that describes one or more named datasets, each holding its own set of arrays.

What it does:

- **Statistics-based dataset pruning.** Atlas keeps per-dataset, per-column statistics. When a query carries a predicate (e.g. a time or latitude range), Beacon drops whole datasets that cannot match *before reading any array data*, so range queries over large collections only touch the relevant data.
- **Column projection.** Only the arrays referenced by a query are read, keeping I/O proportional to the columns actually selected.
- **Compact, self-describing layout.** Arrays are stored compressed (zstd) and the `atlas.json` registry is opened once and cached for the lifetime of the process, avoiding repeated metadata parsing across queries.
- **Object-store friendly.** Atlas stores can live on local disk or S3-compatible object storage.

Query an Atlas store with the [`read_atlas()`](../sql/table-functions.md#read_atlas) table function, pointing at its `atlas.json` marker file — an exact path or a glob such as `**/atlas.json`. An optional second argument filters the arrays to those matching the listed dimensions.

```sql
SELECT * FROM read_atlas(['collections/sensor/atlas.json'])
```

### External tables over Atlas

For a stable, reusable table name, register the store as an [External Table](./external-tables.md#atlas). Like Zarr, point the `LOCATION` at the `atlas.json` marker (or a glob over several markers):

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/atlas.json';

SELECT time, temperature
FROM sensor_atlas
WHERE time >= '2024-01-01';
```

### Optimizing NetCDF and Zarr with Atlas

Atlas is the recommended way to speed up repeated queries over large NetCDF or Zarr collections: convert the source files into a single Atlas collection. Consolidating many NetCDF or Zarr files into one statistics-aware store lets Beacon prune whole datasets using column statistics and read only the projected arrays, so spatial and temporal range queries are typically much faster than scanning the original files directly.

See the [Atlas repository](https://github.com/maris-development/atlas) for the store format and tooling to build Atlas collections.

:::tip
Heavy, repeated aggregations over an Atlas collection — or any other table — can be cached with a [materialized view](../sql/create-materialized-view.md) and recomputed with `REFRESH` when the underlying data changes.
:::

## Beacon Binary Format (BBF)

Beacon's own columnar format, optimized for the kinds of queries common in earth-science and oceanographic workloads.

- Full S3 / object-store support with authenticated access.
- Chunk-level predicate pruning similar to Parquet row-group filtering.
- Efficient for repeated range queries over coordinate columns (time, depth, lat/lon).

Convert existing NetCDF files to BBF using the beacon-binary-format-toolbox for significant query speedups on large collections.
