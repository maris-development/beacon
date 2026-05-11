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

The optional `dimensions` argument pins which variables Beacon treats as dimension coordinates when reading N-dimensional variables. Providing it can improve correctness and performance for datasets with complex dimensionality.

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf(['argo/**/*.nc'])

-- With explicit dimension columns
SELECT *
FROM read_netcdf(['argo/**/*.nc'], ['time', 'pressure'])
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
