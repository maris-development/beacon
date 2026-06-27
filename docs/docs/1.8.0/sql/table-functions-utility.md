# Introspection

These table functions inspect schemas, file listings, and statistics without reading dataset rows. Use them to explore an unfamiliar data lake before writing queries.

## `read_schema`

```text
read_schema(glob_paths, file_format)
```

Returns the inferred schema (column names and types) for a set of files without reading any data.

`glob_paths` accepts either a single glob/path string or a list of strings. `file_format` must be one of: `parquet`, `netcdf` (or `nc`), `zarr`, `arrow`, `csv`, `bbf`, `tiff` (or `tif`).

```sql
-- Single path or glob
SELECT * FROM read_schema('argo/**/*.nc', 'netcdf')

SELECT * FROM read_schema('obs/*.parquet', 'parquet')

-- A list, to inspect the combined schema across multiple sources
SELECT * FROM read_schema(['obs/2023/*.parquet', 'obs/2024/*.parquet'], 'parquet')
```

## `list_datasets`

```text
list_datasets()
```

Lists all files currently stored in Beacon's dataset storage root. Returns one row per file.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `file_name` | `TEXT` | Path relative to the storage root |
| `file_format` | `TEXT` | Detected format |

```sql
SELECT * FROM list_datasets()

-- Find all NetCDF files
SELECT file_name FROM list_datasets() WHERE file_format = 'nc'
```

## `view_dataset_statistics`

```text
view_dataset_statistics(path)
```

Returns per-column min/max statistics for a single file. Statistics are read from the cache when available and computed on demand otherwise.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `column_name` | `TEXT` | Column name |
| `data_type` | `TEXT` | Column data type |
| `min_value` | `TEXT` | Minimum value (NULL if unknown) |
| `max_value` | `TEXT` | Maximum value (NULL if unknown) |
| `is_exact` | `BOOLEAN` | Whether the statistics are exact |

```sql
SELECT * FROM view_dataset_statistics('argo/2024/R6900001.nc')
```

## `view_external_table_statistics`

```text
view_external_table_statistics(table_name)
```

Returns per-file statistics for every file that backs an external table — useful for checking which files have cached statistics and what their value ranges are.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `path` | `TEXT` | File path |
| `file_size` | `UINT64` | File size in bytes |
| `cached` | `BOOLEAN` | Whether statistics are cached for this file |
| `column_name` | `TEXT` | Column name (NULL if not cached) |
| `data_type` | `TEXT` | Column data type |
| `min_value` | `TEXT` | Minimum value |
| `max_value` | `TEXT` | Maximum value |
| `is_exact` | `BOOLEAN` | Whether the statistics are exact |

```sql
SELECT * FROM view_external_table_statistics('ocean_profiles')

-- Find files with no cached statistics
SELECT path FROM view_external_table_statistics('ocean_profiles')
WHERE cached = false
```

## `view_statistics_cache`

```text
view_statistics_cache()
```

Streams all entries from the global file statistics cache. Each row is validated against the object store — the `is_valid` flag indicates whether the cached file still exists and its size matches.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `path` | `TEXT` | File path |
| `file_size` | `UINT64` | File size in bytes |
| `is_valid` | `BOOLEAN` | Whether the cached entry is still valid |
| `column_name` | `TEXT` | Column name |
| `data_type` | `TEXT` | Column data type |
| `min_value` | `TEXT` | Minimum value |
| `max_value` | `TEXT` | Maximum value |
| `is_exact` | `BOOLEAN` | Whether the statistics are exact |

```sql
SELECT * FROM view_statistics_cache()

-- Find stale cache entries
SELECT path FROM view_statistics_cache() WHERE is_valid = false
```
