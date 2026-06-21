# Reading Files

Table functions let you query files directly in a `FROM` clause without creating a persistent [External Table](../data-lake/external-tables.md) first. They are useful for ad-hoc exploration or when you want to embed the file path logic inside a [View](../data-lake/view.md).

All functions take the file path(s) to read as their first argument. This can be **either a single glob/path string** or **a list of strings**. Globs are resolved relative to Beacon's configured dataset storage root.

```sql
-- Single path
SELECT * FROM read_parquet('profiles/2024.parquet')

-- Single folder glob
SELECT * FROM read_netcdf('argo/**/*.nc')

-- A list, to combine multiple paths or globs in one call
SELECT * FROM read_netcdf(['argo/**/*.nc', 'wod/**/*.nc'])
```

In every signature below, the `glob_paths` argument accepts either form — a single string or a list of strings.

## `read_netcdf`

```text
read_netcdf(glob_paths)
read_netcdf(glob_paths, dimensions)
```

Reads NetCDF files matching one or more glob patterns.

The optional `dimensions` argument filters which variables are returned: a variable is included only if all of its dimensions are a subset of the provided list. Use it to exclude high-dimensional variables you don't need, or to resolve ambiguity when files contain variables with incompatible dimensionalities.

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')

-- With explicit dimension columns
SELECT *
FROM read_netcdf(['argo/**/*.nc'], ['time', 'pressure'])
```

### Variable attributes

NetCDF variable attributes (e.g. `units`, `long_name`) are exposed as additional columns using the pattern `<variable>.<attribute>`. Attribute columns preserve the original type (string, integer, float, …). File-level global attributes use a leading dot with no variable prefix: `.<attribute>`. Quote these column names because they contain a dot.

```sql
-- Variable attribute
SELECT temperature, "temperature.units", "temperature.long_name"
FROM read_netcdf('argo/**/*.nc')
LIMIT 1

-- Global attribute
SELECT ".source", temperature
FROM read_netcdf('argo/**/*.nc')
LIMIT 1
```

## `read_zarr`

```text
read_zarr(glob_paths)
```

Reads Zarr stores matching one or more glob patterns. Each path should point at a `zarr.json` entry file.

Predicate pushdown is automatic: Beacon prunes chunks and slices coordinate dimensions (e.g. `time`, `latitude`, `longitude`) based on the query's `WHERE` clause — no statistics columns need to be declared.

```sql
SELECT * FROM read_zarr('sst/*/zarr.json')

-- Range queries are pruned automatically
SELECT time, sst
FROM read_zarr('sst/*/zarr.json')
WHERE time >= '2024-01-01'
```

### Array attributes

Per-array attributes are exposed as additional columns using the pattern `<array>.<attribute>`. Attribute columns preserve the original type (string, integer, float, …). Root-level store attributes use a leading dot with no array prefix: `.<attribute>`. Quote these column names because they contain a dot.

```sql
-- Array attribute
SELECT sst, "sst.units", "sst.long_name"
FROM read_zarr('sst/*/zarr.json')
LIMIT 1

-- Root-level global attribute
SELECT ".Conventions", sst
FROM read_zarr('sst/*/zarr.json')
LIMIT 1
```

## `read_atlas`

```text
read_atlas(glob_paths)
read_atlas(glob_paths, dimensions)
```

Reads [Atlas](../data-lake/datasets.md#atlas) array stores matching one or more glob patterns. Each path must point at an `atlas.json` marker file — an exact path or a glob such as `**/atlas.json`.

The optional `dimensions` argument filters the arrays to those matching the listed dimension names. Atlas prunes whole datasets using per-column statistics, so range queries over large collections only read the datasets that can match the predicate.

```sql
SELECT * FROM read_atlas('collections/sensor/atlas.json')

-- Combine every Atlas store under a prefix, keeping a subset of dimensions
SELECT time, temperature
FROM read_atlas(['collections/**/atlas.json'], ['time', 'latitude', 'longitude'])
WHERE time >= '2024-01-01'
```

## `read_parquet`

```text
read_parquet(glob_paths)
```

```sql
SELECT * FROM read_parquet('obs/**/*.parquet') LIMIT 100
```

## `read_geoparquet`

```text
read_geoparquet(glob_paths)
```

Reads [GeoParquet](https://geoparquet.org/) files. Geometry columns described in the file's `geo` metadata are decoded to their native [GeoArrow](https://geoarrow.org/) representation; files without geometry are read like ordinary Parquet.

```sql
SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 100
```

## `read_arrow`

```text
read_arrow(glob_paths)
```

Reads Arrow IPC stream files (`.arrow`, `.ipc`).

```sql
SELECT * FROM read_arrow('streams/*.arrow')
```

## `read_csv`

```text
read_csv(glob_paths)
read_csv(glob_paths, delimiter)
read_csv(glob_paths, delimiter, infer_records)
```

Schema is inferred from the file contents. The first row must be a header row.

- `delimiter` — single-character field separator (default: `,`)
- `infer_records` — number of rows to sample when inferring column types (default: `100`)

```sql
SELECT * FROM read_csv('metadata/*.csv')

-- Tab-separated, sample 500 rows for type inference
SELECT * FROM read_csv(['data/*.tsv'], '\t', 500)
```

## `read_odv_ascii`

```text
read_odv_ascii(glob_paths)
```

```sql
SELECT * FROM read_odv_ascii('odv/**/*.txt')
```

## `read_bbf`

```text
read_bbf(glob_paths)
```

Reads Beacon Binary Format files.

```sql
SELECT * FROM read_bbf('bbf/**/*.bbf')
```

## `read_tiff`

```text
read_tiff(glob_paths)
```

Reads GeoTIFF and Cloud-Optimized GeoTIFF files.

```sql
SELECT * FROM read_tiff('rasters/elevation.tif')
```

### Tag attributes

Per-band TIFF tags are exposed as additional columns using the pattern `<band>.<attribute>`. Attribute columns preserve the original type (string, integer, float, …). File-level tags not tied to a specific band use a leading dot with no band prefix: `.<attribute>`. Quote these column names because they contain a dot.

```sql
-- Band attribute
SELECT band_1, "band_1.nodata", "band_1.scale"
FROM read_tiff('rasters/elevation.tif')
LIMIT 1

-- File-level global tag
SELECT ".crs", band_1
FROM read_tiff('rasters/elevation.tif')
LIMIT 1
```

## `read_delta`

```text
read_delta(location)
read_delta(location, version_or_timestamp)
```

Reads a [Delta Lake](../data-lake/delta-lake.md) table. Unlike the other functions, `location` is a **single path to the Delta table directory** (the folder containing `_delta_log/`) — not a glob or a list. The schema is read from the transaction log.

The optional second argument selects a snapshot for **time travel**:

- an integer is a Delta **version** number, e.g. `12`
- any other string is an RFC-3339 **timestamp** — the latest version at or before it

```sql
-- Latest version
SELECT * FROM read_delta('delta/ocean_profiles') LIMIT 100

-- Time travel to a specific version
SELECT count(*) FROM read_delta('delta/ocean_profiles', 12)

-- Time travel as of a timestamp
SELECT * FROM read_delta('delta/ocean_profiles', '2026-01-01T00:00:00Z')
```

To register a Delta table persistently (and to `INSERT INTO` it), use [`CREATE EXTERNAL TABLE … STORED AS DELTA`](../data-lake/delta-lake.md).
