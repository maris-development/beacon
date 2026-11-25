---
description: Read Zarr v3 stores with read_zarr(). Chunk-level predicate pushdown fetches only the chunks that a query needs.
---

# Zarr

## Read the files

```text
read_zarr(glob_paths)
read_zarr(glob_paths, dimensions)
read_zarr(glob_paths, dimensions, skip_unbroadcastable)
```

Beacon reads the Zarr stores that match one or more glob patterns. Each path must point at a
`zarr.json` entry file.

The optional `dimensions` argument selects the arrays. Beacon returns an array only if the list
holds all of its dimensions. Use the argument to drop arrays with many dimensions.

The optional `skip_unbroadcastable` argument sets what Beacon does with a group that does not fit the list. Beacon fails the query by default. Set it to true to skip the group instead. Beacon writes a warning and reads the next group.

Predicate pushdown is automatic. Beacon prunes chunks and slices the coordinate dimensions such as
`time`, `latitude` and `longitude`. It uses the `WHERE` clause of your query. You declare no
statistics columns.

```sql
SELECT * FROM read_zarr('sst/*/zarr.json')

-- Range queries are pruned automatically
SELECT time, sst
FROM read_zarr('sst/*/zarr.json')
WHERE time >= '2024-01-01'
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_zarr('sst/*/zarr.json') LIMIT 0;
```

[Inspect a schema](/docs/2.1.0-rc3/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

Beacon queries a Zarr dataset with chunk-level predicate pushdown. It reads only the chunks that can
match your predicates. Spatial and time range queries are therefore fast, also on a large
multi-dimensional store.

- Beacon supports Zarr v3 stores. The `zarr.json` entry file marks such a store. Beacon does not find v2 stores, which use `.zarray`, `.zgroup` and `.zattrs` files.
- Beacon decompresses a compressed chunk automatically. It supports zstd, gzip, blosc and more.
- One external table can hold several Zarr stores. Use a glob pattern such as `sst/*/zarr.json`.
- Beacon fully supports S3 and other object stores.
- `COUNT(*)` and a row limit (`LIMIT`) also reach the scan.

### Dimension names

A Zarr v3 array names its axes in `dimension_names`. Beacon uses these names as the dimensions of
the table.

- An axis with no name gets the name `dim_{i}`. `i` is the position of the axis: `dim_0`, `dim_1`
  and so on.
- Two arrays can give `dim_{i}` two sizes. The first array in name order keeps `dim_{i}`. A later
  array with a different size gets `{array}_dim_{i}`, for example `sst_dim_0`.
- Beacon never gives an axis a name that an array of the group writes in `dimension_names`. Beacon
  then takes the next free name: `{array}_dim_{i}`, then `{array}_dim_{i}_2` and so on.
- A name in `dimension_names` has one size in a group. A name with two sizes is an error. The error
  names the dimension, the two arrays and the two sizes.
- A group in which no dimension has a name from `dimension_names` follows the rule for
  [a file that names no dimension](/docs/2.1.0-rc3/arrays-to-tables#a-file-that-names-no-dimension).

### Array attributes

A Zarr array holds its attributes in the `attributes` section of its `zarr.json`. Beacon shows these
as extra columns. It uses dot notation: `<array>.<attribute>`. The `units` attribute of the `sst`
array becomes the column `sst.units`.

The type of an attribute column comes from the JSON value:

| JSON value | Column type |
| --- | --- |
| string | `Utf8` |
| number, also an integer | `Float64` |
| `true` or `false` | `Boolean` |
| list, object or `null` | No column |

A root attribute of the store belongs to no array. Beacon shows it with a leading dot and no array
prefix: `.<attribute>`. The root attribute `Conventions` becomes the column `.Conventions`.

```sql
SELECT sst, "sst.units", "sst.long_name", ".Conventions"
FROM read_zarr(['sst/*/zarr.json'])
LIMIT 1
```

Limitations:

- Beacon reads the data types `bool`, `int8` to `int64`, `uint8` to `uint64`, `float32`,
  `float64`, `string` and `bytes`. Beacon leaves out an array of a different type, for example
  `float16` or a user-defined data type. That array gives no column and no attribute columns.
- Beacon leaves out an array that repeats a dimension name, such as `m(x, x)`. That array also
  gives no attribute columns.

:::tip
Predicate pushdown is automatic. Beacon prunes chunks and slices the coordinate dimensions such as
`time`, `latitude` and `longitude`. It uses the filters of your query. You configure nothing.

Do you query a collection often? Then convert the Zarr stores into one
[Atlas](/docs/2.1.0-rc3/formats/atlas) collection. Atlas is an array store
with statistics. Beacon can drop whole datasets before it reads a chunk.
:::

## CF decode

Beacon applies the CF conventions of an array when it reads the array. The table shows the result
for each case.

| Case | Result |
| --- | --- |
| CF time variable (`units = "<unit> since <date>"`) | `Timestamp(µs)` |
| Packed time (`scale_factor` or `add_offset`, with `units = "<unit> since <date>"`) | `Timestamp(µs)` |
| Integer variable with `units = "nanoseconds since <date>"`, without `scale_factor` and `add_offset` | `Timestamp(ns)` |
| Float variable with `units = "nanoseconds since <date>"` | Numbers |
| Calendar absent, `standard`, `gregorian`, `proleptic_gregorian` or `julian` | A timestamp |
| Calendar `noleap`, `360_day` or a different calendar | Numbers |
| NaN, infinite or out-of-range time value | Null |
| Packed values (`scale_factor`, `add_offset`) | `Float64` |
| `_FillValue` | The values equal to it become null. |
| `missing_value`, with one or more values | The values equal to one of them become null. |
| `valid_min`, `valid_max`, `valid_range` | The values outside the range become null. |
| `_FillValue` on a `string` or `bool` array | The values equal to it become null. |
| `fill_value` in the array metadata | No null. Beacon reads the value. |

Beacon's Zarr writer writes a `Timestamp(ns)` column as `int64` with the unit
`nanoseconds since 1970-01-01T00:00:00Z`. Beacon reads such a store back as `Timestamp(ns)`.

### Create a table again

An external table keeps the column types that Beacon stored when you created the table. Beacon casts
the new types to the stored types. A stored `Timestamp(ns)` column continues to work.

A column that changes from a time to a number is different. The cast reads the number as
nanoseconds since 1970 and gives wrong times. This applies to two cases:

- A time variable with a calendar that is not standard, Gregorian or Julian.
- A float variable with `nanoseconds since` units.

A packed time column also keeps its stored type, `Float64`.

Drop each such table and create it again. The table then gets the new types. If Beacon inferred the
schema of the table, [`REFRESH`](/docs/2.1.0-rc3/sql/create-external-table#refresh) also gives the
new types.

A table with `'use_statistics_pruning' 'true'` also keeps its old
[file statistics](/docs/2.1.0-rc3/internals/file-statistics). An earlier Beacon version cut a CF
time to whole nanoseconds. This version rounds a CF time to the nearest microsecond. An old maximum can
therefore be below the value that a query now reads. The query then prunes a store that holds a
match. Drop the table and create it again. `DROP TABLE` deletes the statistics of the table.

## As an external table

A Zarr table must point at a `zarr.json` entry file, not at a folder:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/zarr.json'
```

Use a glob to cover several Zarr stores:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/*/zarr.json'
```

See [Create External Tables](/docs/2.1.0-rc3/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.1.0-rc3/data-sources/) for the
full read model.

### `OPTIONS`

`STORED AS ZARR` reads three keys:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `read_dimensions` | List of dimension names | The default grid of each store | The dimensions the table reads. Beacon returns an array only if the list holds every dimension of that array. |
| `skip_unbroadcastable` | Boolean | `false` | Skip a group that does not fit `read_dimensions`. Beacon writes a warning and reads the next group. Without it the query fails. |
| `enable_statistics` | Boolean | `true` (`BEACON_ZARR_ENABLE_STATISTICS`) | Compute the column ranges of each file for the [file statistics](/docs/2.1.0-rc3/internals/file-statistics) of this table. With `false`, the table records no range and prunes no file. |

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/*/zarr.json'
OPTIONS ('read_dimensions' 'time,lat,lon')
```

See [`OPTIONS`](/docs/2.1.0-rc3/sql/create-external-table#options) for the rules that hold for every key. See
[Arrays to tables](/docs/2.1.0-rc3/arrays-to-tables#the-dimensions-argument) for the grid rule.
