# Reading Files

Table functions let you query files directly in a `FROM` clause without creating a persistent [External Table](../data-lake/external-tables.md) first. They are useful for ad-hoc exploration or when you want to embed the file path logic inside a [View](../data-lake/view.md).

All functions accept one or more glob paths as a list. Globs are resolved relative to Beacon's configured dataset storage root.

```sql
-- Single path
SELECT * FROM read_parquet(['profiles/2024.parquet'])

-- Folder glob
SELECT * FROM read_netcdf(['argo/**/*.nc'])

-- Multiple globs in one call
SELECT * FROM read_netcdf(['argo/**/*.nc', 'wod/**/*.nc'])
```

## `read_netcdf`

```text
read_netcdf(glob_paths)
read_netcdf(glob_paths, dimensions)
```

Reads NetCDF files matching one or more glob patterns.

The optional `dimensions` argument filters which variables are returned: a variable is included only if all of its dimensions are a subset of the provided list. Use it to exclude high-dimensional variables you don't need, or to resolve ambiguity when files contain variables with incompatible dimensionalities.

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf(['argo/**/*.nc'])

-- With explicit dimension columns
SELECT *
FROM read_netcdf(['argo/**/*.nc'], ['time', 'pressure'])
```

### Variable attributes

NetCDF variable attributes (e.g. `units`, `long_name`) are exposed as additional columns using the pattern `<variable>.<attribute>`. Attribute columns preserve the original type (string, integer, float, …). File-level global attributes use a leading dot with no variable prefix: `.<attribute>`. Quote these column names because they contain a dot.

```sql
-- Variable attribute
SELECT temperature, "temperature.units", "temperature.long_name"
FROM read_netcdf(['argo/**/*.nc'])
LIMIT 1

-- Global attribute
SELECT ".source", temperature
FROM read_netcdf(['argo/**/*.nc'])
LIMIT 1
```

## `read_zarr`

```text
read_zarr(glob_paths)
read_zarr(glob_paths, statistics_columns)
```

Reads Zarr stores matching one or more glob patterns. Each path should point at a `zarr.json` entry file.

The optional `statistics_columns` argument names coordinate columns for which Beacon has pre-computed chunk statistics. Supplying these enables 1D slice pushdown — Beacon skips chunks that cannot satisfy a `WHERE` predicate on those columns.

```sql
SELECT * FROM read_zarr(['sst/*/zarr.json'])

-- With statistics columns for faster range queries
SELECT time, sst
FROM read_zarr(['sst/*/zarr.json'], ['time', 'latitude', 'longitude'])
WHERE time >= '2024-01-01'
```

### Array attributes

Per-array attributes are exposed as additional columns using the pattern `<array>.<attribute>`. Attribute columns preserve the original type (string, integer, float, …). Root-level store attributes use a leading dot with no array prefix: `.<attribute>`. Quote these column names because they contain a dot.

```sql
-- Array attribute
SELECT sst, "sst.units", "sst.long_name"
FROM read_zarr(['sst/*/zarr.json'])
LIMIT 1

-- Root-level global attribute
SELECT ".Conventions", sst
FROM read_zarr(['sst/*/zarr.json'])
LIMIT 1
```

## `read_parquet`

```text
read_parquet(glob_paths)
```

```sql
SELECT * FROM read_parquet(['obs/**/*.parquet']) LIMIT 100
```

## `read_arrow`

```text
read_arrow(glob_paths)
```

Reads Arrow IPC stream files (`.arrow`, `.ipc`).

```sql
SELECT * FROM read_arrow(['streams/*.arrow'])
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
SELECT * FROM read_csv(['metadata/*.csv'])

-- Tab-separated, sample 500 rows for type inference
SELECT * FROM read_csv(['data/*.tsv'], '\t', 500)
```

## `read_odv_ascii`

```text
read_odv_ascii(glob_paths)
```

```sql
SELECT * FROM read_odv_ascii(['odv/**/*.txt'])
```

## `read_bbf`

```text
read_bbf(glob_paths)
```

Reads Beacon Binary Format files.

```sql
SELECT * FROM read_bbf(['bbf/**/*.bbf'])
```

## `read_tiff`

```text
read_tiff(glob_paths)
```

Reads GeoTIFF and Cloud-Optimized GeoTIFF files.

```sql
SELECT * FROM read_tiff(['rasters/elevation.tif'])
```

### Tag attributes

Per-band TIFF tags are exposed as additional columns using the pattern `<band>.<attribute>`. Attribute columns preserve the original type (string, integer, float, …). File-level tags not tied to a specific band use a leading dot with no band prefix: `.<attribute>`. Quote these column names because they contain a dot.

```sql
-- Band attribute
SELECT band_1, "band_1.nodata", "band_1.scale"
FROM read_tiff(['rasters/elevation.tif'])
LIMIT 1

-- File-level global tag
SELECT ".crs", band_1
FROM read_tiff(['rasters/elevation.tif'])
LIMIT 1
```
