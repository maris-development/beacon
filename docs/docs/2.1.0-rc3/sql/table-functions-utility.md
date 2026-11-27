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
read_tiff_schema(glob_paths)
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
| `can_inspect` | `BOOLEAN` | `true` if Beacon can inspect the file |
| `can_partial_explore` | `BOOLEAN` | `true` if Beacon can read a part of the file |
| `size` | `BIGINT UNSIGNED` | The size in bytes. `NULL` if Beacon does not know it. |
| `last_modified` | `TEXT` | The last change, in RFC 3339. `NULL` if Beacon does not know it. |

The function accepts three optional arguments: `list_datasets(pattern, offset, limit)`. The
`pattern` is a glob, relative to the storage root. The default is `**/*`.

A Zarr v3 store has a `zarr.json` file at its root and in each array. The store gives one row, for
the `zarr.json` file at its root.

The rows stream as the storage listing arrives. A `LIMIT` stops the listing. The rows have no
fixed order. Add `ORDER BY file_name` if you need a sorted result.

```sql
SELECT * FROM list_datasets()

-- Find all NetCDF files
SELECT file_name FROM list_datasets() WHERE file_format = 'nc'

-- The first 50 Parquet files under argo/, sorted
SELECT file_name FROM list_datasets('argo/**/*.parquet') ORDER BY file_name LIMIT 50
```

## `table_statistics`

```text
table_statistics(table_name)
```

Returns the [file statistics](/docs/2.1.0-rc3/internals/file-statistics) of an external table with
`use_statistics_pruning`. These are the statistics that a query on the table prunes on. Returns one
row for each file. Only the super-user can call it.

| Column | Type | Description |
| ------ | ---- | ----------- |
| `_stamp` | `UINT64` | A hash of the store, the path, the modification time and the size |
| `_path` | `TEXT` | The file, relative to its store |
| `_size` | `UINT64` | The file size in bytes |
| `_last_modified` | `TIMESTAMP` | The modification time |
| `_failed` | `BOOLEAN` | `true` if Beacon could not read the file |
| `_row_count` | `UINT64` | The row count. `NULL` if the format reported none. |
| one column for each statistics column | `STRUCT` | `min`, `max` and `null_count`, in the type of the table column |

```sql
SELECT _path, "TEMP"['min'], "TEMP"['max'] FROM table_statistics('ocean_profiles')

-- Find files with no range for TEMP
SELECT _path FROM table_statistics('ocean_profiles') WHERE "TEMP"['min'] IS NULL
```
