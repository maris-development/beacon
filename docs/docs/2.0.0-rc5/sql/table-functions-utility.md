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
list_datasets(pattern, offset, limit)
```

Lists every file in the dataset storage root of Beacon. Returns one row for each file. Beacon
descends the full tree below `pattern`.

All three arguments are optional and positional:

| Argument | Type | Description |
| -------- | ---- | ----------- |
| `pattern` | `TEXT` | A glob that filters the paths. The default is `**/*` |
| `offset` | `INTEGER` | The number of rows to skip. The default is `0` |
| `limit` | `INTEGER` | The maximum number of rows. The default is no limit |

The output columns are:

| Column | Type | Description |
| ------ | ---- | ----------- |
| `file_name` | `TEXT` | The path, relative to the storage root |
| `file_format` | `TEXT` | The format that Beacon detects |
| `can_inspect` | `BOOLEAN` | Whether Beacon reads a schema from this file |
| `can_partial_explore` | `BOOLEAN` | Whether Beacon reads a part of this file |
| `size` | `UBIGINT` | The size in bytes. Null when the store reports none |
| `last_modified` | `TEXT` | The modification time, in RFC 3339. Null when the store reports none |
| `is_directory` | `BOOLEAN` | Always false here. See `browse_datasets` |

```sql
SELECT * FROM list_datasets()

-- Find all NetCDF files
SELECT file_name FROM list_datasets() WHERE file_format = 'nc'

-- One page of one subtree
SELECT file_name FROM list_datasets('argo/**/*.nc', 0, 50)
```

A `LIMIT` stops the walk. Beacon reads one page of the store for `LIMIT 50`, not the full store.

## `browse_datasets`

```text
browse_datasets(prefix, offset, limit)
```

Lists one directory level of the dataset storage root. Returns one row for each file and one row
for each sub-directory. Beacon does not descend.

All three arguments are optional and positional:

| Argument | Type | Description |
| -------- | ---- | ----------- |
| `prefix` | `TEXT` | The directory to read. The default is the storage root |
| `offset` | `INTEGER` | The number of rows to skip. The default is `0` |
| `limit` | `INTEGER` | The maximum number of rows. The default is no limit |

The output columns are the columns of `list_datasets`. A sub-directory row sets `is_directory` and
holds the directory path in `file_name`.

Use this function for a folder view. One level costs one request, so the time does not increase
with the size of the store below `prefix`. `list_datasets` reads every object below its pattern.

```sql
-- The root level
SELECT * FROM browse_datasets()

-- The files of one directory
SELECT file_name FROM browse_datasets('argo') WHERE NOT is_directory

-- The sub-directories of one directory
SELECT file_name FROM browse_datasets('argo') WHERE is_directory
```

::: info A directory-shaped dataset
A Zarr store is a directory. At its own level Beacon reports it as a sub-directory. Descend into it
to see the dataset row.
:::

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
