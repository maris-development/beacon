# Introspection

These table functions inspect schemas, file lists and statistics. They read no dataset rows. Use
them to explore an unfamiliar catalog before you write a query.

## `read_<format>_schema`

```text
read_parquet_schema(glob_paths)
read_geoparquet_schema(glob_paths)
read_csv_schema(glob_paths, delimiter, infer_records)
read_arrow_schema(glob_paths)
read_netcdf_schema(glob_paths)
read_hdf5_schema(glob_paths)
read_zarr_schema(glob_paths)
read_atlas_schema(glob_paths, dimensions)
read_tiff_schema(glob_paths)
read_bbf_schema(glob_paths)
read_delta_schema(location)
read_iceberg_schema(location)
read_odv_ascii_schema(glob_paths)
```

Every `read_*` reader has a `_schema` counterpart. It returns the Arrow schema of the files that
reader would open, one row per column, **without a scan of their data**. The arguments match the
reader it wraps.

The output is always the same three columns:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `column_name` | `TEXT` | The column name |
| `data_type` | `TEXT` | The Arrow data type |
| `nullable` | `BOOLEAN` | Whether the column accepts nulls |

```sql
-- Single path or glob
SELECT * FROM read_netcdf_schema('argo/**/*.nc');

SELECT * FROM read_parquet_schema('obs/*.parquet');

-- A list, to inspect the combined schema across several sources
SELECT * FROM read_parquet_schema(['obs/2023/*.parquet', 'obs/2024/*.parquet']);

-- Compose it like any other table
SELECT column_name FROM read_netcdf_schema('argo/**/*.nc')
WHERE data_type LIKE 'Timestamp%';
```

## `list_datasets`

```text
list_datasets()
```

Lists every file in the dataset storage root of Beacon. Returns one row for each file.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `file_name` | `TEXT` | The path, relative to the storage root |
| `file_format` | `TEXT` | The format that Beacon detects |

```sql
SELECT * FROM list_datasets()

-- Find all NetCDF files
SELECT file_name FROM list_datasets() WHERE file_format = 'nc'
```

## `view_dataset_statistics`

```text
view_dataset_statistics(path)
```

Returns the minimum and maximum of each column of one file. Beacon reads the statistics from the
cache. If the cache holds nothing, Beacon computes them on demand.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `column_name` | `TEXT` | The column name |
| `data_type` | `TEXT` | The data type of the column |
| `min_value` | `TEXT` | The minimum value. `NULL` if Beacon does not know it. |
| `max_value` | `TEXT` | The maximum value. `NULL` if Beacon does not know it. |
| `is_exact` | `BOOLEAN` | `true` if the statistics are exact |

```sql
SELECT * FROM view_dataset_statistics('argo/2024/R6900001.nc')
```

## `view_external_table_statistics`

```text
view_external_table_statistics(table_name)
```

Returns the statistics of every file under an external table. Use it to see which files have cached
statistics. It also gives their value ranges.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `path` | `TEXT` | The file path |
| `file_size` | `UINT64` | The file size in bytes |
| `cached` | `BOOLEAN` | `true` if the cache holds statistics for this file |
| `column_name` | `TEXT` | The column name. `NULL` if the cache holds nothing. |
| `data_type` | `TEXT` | The data type of the column |
| `min_value` | `TEXT` | The minimum value |
| `max_value` | `TEXT` | The maximum value |
| `is_exact` | `BOOLEAN` | `true` if the statistics are exact |

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

Streams every entry of the global file statistics cache. Beacon checks each row against the object
store. The `is_valid` flag shows two things: the file still exists, and its size matches.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `path` | `TEXT` | The file path |
| `file_size` | `UINT64` | The file size in bytes |
| `is_valid` | `BOOLEAN` | `true` if the cache entry is still valid |
| `column_name` | `TEXT` | The column name |
| `data_type` | `TEXT` | The data type of the column |
| `min_value` | `TEXT` | The minimum value |
| `max_value` | `TEXT` | The maximum value |
| `is_exact` | `BOOLEAN` | `true` if the statistics are exact |

```sql
SELECT * FROM view_statistics_cache()

-- Find stale cache entries
SELECT path FROM view_statistics_cache() WHERE is_valid = false
```
