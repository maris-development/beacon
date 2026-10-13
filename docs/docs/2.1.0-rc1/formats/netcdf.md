---
description: Read NetCDF files with read_netcdf(). Beacon streams chunks, shows variable attributes as columns and takes an optional dimension filter.
---

# NetCDF

## Read the files

```text
read_netcdf(glob_paths)
read_netcdf(glob_paths, dimensions)
read_netcdf(glob_paths, dimensions, skip_unbroadcastable)
```

Beacon reads the NetCDF files that match one or more glob patterns.

Beacon recognizes `.nc`, `.nc3`, `.nc4` and `.netcdf`, in upper or lower case. It finds them in the
dataset store automatically. A crawler makes one table for each extension in a folder.

The optional `dimensions` argument selects the variables. Beacon returns a variable only if the list
holds all of its dimensions. Use the argument to drop variables with many dimensions. Also use it
when the files hold variables with different dimensions.

The optional `skip_unbroadcastable` argument sets what Beacon does with a file that does not fit the list. Beacon fails the query by default. Set it to true to skip the file instead. Beacon writes a warning and reads the next file.

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')

-- With explicit dimension columns
SELECT *
FROM read_netcdf(['argo/**/*.nc'], ['time', 'pressure'])
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_netcdf('argo/**/*.nc') LIMIT 0;
```

[Inspect a schema](/docs/2.1.0-rc1/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

Beacon streams the data and reads one chunk at a time. It reads a large file step by step. It does
not load the whole file into memory.

Supported dialects:

- **NetCDF4** (recommended)
- **NetCDF3**: a `char*` array with a string dimension such as `STRLEN` becomes a fixed-length string

### Variable attributes

A NetCDF variable carries metadata attributes such as `units`, `long_name` and `valid_min`. Beacon
shows these as extra columns. It uses dot notation: `<variable>.<attribute>`. The `units` attribute
of the `temperature` variable becomes the column `temperature.units`.

An attribute column keeps the type from the file: string, integer, float and so on. Every query
returns these columns next to the variable columns.

A file attribute has no variable prefix. Beacon shows it with a leading dot: `.<attribute>`. The
global attribute `source` becomes the column `.source`.

```sql
SELECT temperature, "temperature.units", "temperature.long_name", ".source"
FROM read_netcdf(['argo/**/*.nc'])
LIMIT 1
```

Limitations:

- Beacon does not support user-defined types.
- Object storage authenticates on the default `BEACON_NETCDF_USE_RUST_READER=true`. On the netCDF-C
  library a bucket supports anonymous access only, because that library opens a file by URL and
  never sees the credential chain.

:::tip
For a large NetCDF collection, convert the files into one
[Atlas](/docs/2.1.0-rc1/formats/atlas) collection. Atlas merges many NetCDF
files into one array store with statistics. Beacon can then drop whole datasets and read only the
arrays that you select. This is much faster than a scan of the original NetCDF files.
:::

## Write a NetCDF output

### NetCDF output options

The plain name `"format": "netcdf"` uses the default options. To set an option, give the `netcdf`
format an object. In SQL, set the same keys in `COPY ... STORED AS NC OPTIONS (...)`. The SQL
format name is `NC`. `NETCDF` does not work.

| JSON key | SQL key | Default | Description |
|---|---|---|---|
| `combine_attrs` | `combine_attrs` | Not set | Write an attribute column as a NetCDF attribute. See [attributes in a NetCDF output](#attributes-in-a-netcdf-output). |
| `compression.level` | `compression.level` | Not set | Deflate level from 0 to 9. See [compression and chunks](#compression-and-chunks). |
| `compression.shuffle` | `compression.shuffle` | `true` | Apply the shuffle filter before deflate. |
| `chunk_rows` | `chunk_rows` | `65536` | Number of rows in one chunk of the `obs` dimension. |
| `time_units` | `time_units` | Not set | CF units of each timestamp column, for example `seconds since 1970-01-01`. See [time encoding](#time-encoding). |
| `global_attributes` | `global_attributes` | Not set | Global attributes, as an object of name to string or number. In SQL, give a JSON object. See [file and variable attributes](#file-and-variable-attributes). |
| `variable_attributes` | `variable_attributes` | Not set | Attributes of each column, as an object of column to attributes. In SQL, give a JSON object. |

A gridded NetCDF output, with dimension columns, does not accept these options.

### Compression and chunks

Without `compression`, Beacon writes the data with no compression. Set a deflate level to make the
file smaller:

```json
{ "output": { "format": { "netcdf": {
  "compression": { "level": 4, "shuffle": true },
  "chunk_rows": 65536
} } } }
```

```sql
COPY (SELECT * FROM argo)
TO 'result.nc'
STORED AS NC
OPTIONS ('compression.level' '4', 'compression.shuffle' 'true', 'chunk_rows' '65536')
```

- Level 1 is fast. Level 9 gives the smallest file and is slow. Level 4 is a good default.
- Each variable on `obs` is in chunks of `chunk_rows` rows. A file with fewer rows gets one chunk.
- Beacon does not compress a string variable. netCDF-C does not accept a filter on a
  variable-length string.
- xarray, netCDF4-python, Panoply and ncview read a deflate file. They need no plugin.

### Time encoding

Beacon writes a timestamp column with `standard_name = "time"` and `calendar = "standard"`. Without
`time_units`, the column keeps its Arrow unit, for example `nanoseconds since 1970-01-01T00:00:00Z`.
Some tools, such as Panoply and ncview, read `seconds since` or `days since` better. Set
`time_units` to choose the units:

```json
{ "output": { "format": { "netcdf": { "time_units": "days since 1950-01-01" } } } }
```

```sql
COPY (SELECT * FROM argo)
TO 'result.nc'
STORED AS NC
OPTIONS ('time_units' 'days since 1950-01-01')
```

- The unit is `days`, `hours`, `minutes`, `seconds`, `milliseconds`, `microseconds` or `nanoseconds`.
- The reference time is a date, or a date and a time, in UTC.
- Beacon writes `int64` when each value is a whole number in the unit. Else it writes `float64`,
  so no precision is lost.
- An invalid `time_units` is an error.

### File and variable attributes

Each NetCDF output has two global attributes:

- `Conventions = "CF-1.10"`.
- `history`, with the time of the write and the Beacon version.

Set more attributes with `global_attributes` and `variable_attributes`:

```json
{ "output": { "format": { "netcdf": {
  "global_attributes": { "title": "Argo extract", "institution": "MARIS" },
  "variable_attributes": { "temp": { "units": "degC", "long_name": "Sea temperature" } }
} } } }
```

```sql
COPY (SELECT temp FROM argo)
TO 'result.nc'
STORED AS NC
OPTIONS (
  'global_attributes' '{"title": "Argo extract"}',
  'variable_attributes' '{"temp": {"units": "degC"}}'
)
```

- A value is a string or a number. An integer is `int` in the file, or `int64` when it is too
  large. A number with a fraction is `double`.
- A value from an option wins over a value from `combine_attrs`, and over `Conventions` and
  `history`.
- `variable_attributes` must name a column of the output. Else Beacon gives an error.
- An attribute name must not be empty or start with `_`.
- On a timestamp column, `units` and `calendar` are not allowed. Use `time_units`.

### Fill values

Beacon writes a null as the `_FillValue` of the variable.

| Column type | `_FillValue` |
|---|---|
| Float | `NaN`, as in xarray |
| Integer | The maximum of the type, for example `2147483647` for `int32` |
| String | An empty string |
| Timestamp | The maximum of `int64`, or `NaN` for a `float64` time |

With `combine_attrs`, Beacon uses the source `_FillValue` of a numeric variable. The column
`<variable>._FillValue` must have one value and the type of the variable. Beacon reads a fill value
in a file as a null, so this keeps the original fill value.

### Attributes in a NetCDF output

A NetCDF output writes each column as a variable on the `obs` dimension. An attribute column such
as `temp.units` becomes a variable with one value for each row. To write such a column as a NetCDF
attribute, give the `netcdf` format an object with `combine_attrs`:

```json
{ "output": { "format": { "netcdf": { "combine_attrs": "identical" } } } }
```

The plain name `"format": "netcdf"` uses the default options. It writes each column as a variable.

In SQL, set the option in a `COPY` statement. The format name is `NC`. `NETCDF` does not work.

```sql
COPY (SELECT temp, 'degC' AS "temp.units" FROM t)
TO 'result.nc'
STORED AS NC
OPTIONS ('combine_attrs' 'identical')
```

| `combine_attrs` | One unique value | More than one value |
|---|---|---|
| not set | variable | variable |
| `identical` | attribute | variable |
| `first` | attribute | attribute with the first non-null value |
| `drop_conflicts` | attribute | Beacon drops the column |

- A column `<variable>.<attribute>` becomes an attribute of `<variable>` only when `<variable>` is
  also in the output.
- A null is a value. A column with `degC` and null has two values.
- A column with only null values, or with no rows, gives no attribute and no variable.
- A column with a type that has no NetCDF attribute type, such as a timestamp, stays a variable.
- A global attribute column `.title` that stays a variable gets the name `global.title`.
- A `global.<attribute>` name that matches a column in the output is an error.
- `first` takes the first non-null value that Beacon reads. This value can change from one run to the next.
- The Beacon option `identical` keeps a conflict as a variable. The xarray option with the same name stops
  with an error.
- Beacon never copies an attribute with a name that starts with `_`. Beacon also never copies
  `scale_factor` and `add_offset`. One exception is `_FillValue`. See [fill values](#fill-values).
- Beacon drops `valid_min`, `valid_max`, `valid_range` and `missing_value` in two cases. The first
  case is a variable with a `scale_factor` or `add_offset` column in the output. The second case is
  an attribute with a type that is not the type of the variable.
- On a timestamp column, Beacon writes its own `units` and `calendar`.

## As an external table

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
```

See [Create External Tables](/docs/2.1.0-rc1/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.1.0-rc1/data-sources/) for the
full read model.

### `OPTIONS`

`STORED AS NC` takes four keys for a read. The keys for a write are in
[NetCDF output options](#netcdf-output-options).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `read_dimensions` | List of dimension names | The default grid of each file | The dimensions the table reads. Beacon returns a variable only if the list holds every dimension of that variable. |
| `skip_unbroadcastable` | Boolean | `false` | Skip a file that does not fit `read_dimensions`. Beacon writes a warning and reads the next file. Without it the query fails. |
| `use_rust_reader` | Boolean | `true` (`BEACON_NETCDF_USE_RUST_READER`) | Read with the pure-Rust reader. Set it to `false` to read with the netCDF-C library. That library needs anonymous access to a bucket. |
| `enable_statistics` | Boolean | `true` (`BEACON_NETCDF_ENABLE_STATISTICS`) | Accepted, and without effect today. Beacon rejects a value that is not a boolean, and then reads the server setting alone: `ANALYZE FILES` resolves a format per file, not per table. Set `BEACON_NETCDF_ENABLE_STATISTICS` to turn the column ranges off. |

```sql
-- One grid, and the netCDF-C reader for this table alone
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
OPTIONS ('read_dimensions' 'time,latitude,longitude', 'use_rust_reader' 'false')
```

See [`OPTIONS`](/docs/2.1.0-rc1/sql/create-external-table#options) for the rules that hold for every key. See
[Arrays to tables](/docs/2.1.0-rc1/arrays-to-tables#the-dimensions-argument) for the grid rule.
