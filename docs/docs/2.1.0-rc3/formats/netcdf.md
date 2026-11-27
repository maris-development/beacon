---
description: Read NetCDF files with read_netcdf(). Beacon streams chunks, shows variable attributes as columns and takes an optional dimension filter.
---

# NetCDF

## Read the files

```text
read_netcdf(glob_paths)
read_netcdf(glob_paths, dimensions)
read_netcdf(glob_paths, dimensions, skip_unbroadcastable)
read_netcdf(glob_paths, dimensions, skip_unbroadcastable, skip_invalid_files)
```

Beacon reads the NetCDF files that match one or more glob patterns.

Beacon recognizes `.nc`, `.nc3`, `.nc4` and `.netcdf`, in upper or lower case. It finds them in the
dataset store automatically. A crawler makes one table for each extension in a folder.

The optional `dimensions` argument selects the variables. Beacon returns a variable only if the list
holds all of its dimensions. Use the argument to drop variables with many dimensions. Also use it
when the files hold variables with different dimensions.

The optional `skip_unbroadcastable` argument sets what Beacon does with a file that does not fit the list. Beacon fails the query by default. Set it to true to skip the file instead. Beacon writes a warning and reads the next file.

The optional `skip_invalid_files` argument sets what Beacon does with a file that the reader cannot open. An example is a damaged file. Beacon fails the query by default, and the error names the file. Set the argument to true to skip the file instead. Beacon writes a warning and reads the next file.

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')

-- With explicit dimension columns
SELECT *
FROM read_netcdf(['argo/**/*.nc'], ['time', 'pressure'])

-- Skip the files that the reader cannot open
SELECT *
FROM read_netcdf(['argo/**/*.nc'], NULL, NULL, true)
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_netcdf('argo/**/*.nc') LIMIT 0;
```

[Inspect a schema](/docs/2.1.0-rc3/formats/inspect-a-schema) compares the `_schema` functions,
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
- Beacon reads a file in object storage with the credentials of the server.

:::tip
Do you query a large NetCDF collection often? Then compute the result once in a
[materialized view](/docs/2.1.0-rc3/sql/create-materialized-view). A query on the view does not
scan the original NetCDF files.
:::

## CF decode

Beacon applies the CF conventions of a variable when it reads the variable. The table shows the
result for each case.

| Case | Result |
|---|---|
| CF time variable (`units = "<unit> since <date>"`) | `Timestamp(Microsecond)` |
| Packed values (`scale_factor`, `add_offset`) | The float type of `scale_factor` or `add_offset`, for example `Float32`. Else `Float64`. |
| `_FillValue` | The values become null. A `_FillValue` of another type gets the type of the variable first. |
| `missing_value`, with one or more values | The values become null. |
| `valid_min`, `valid_max`, `valid_range` | The values outside the range become null. |
| Packed time (`scale_factor` or `add_offset`, with `units = "<unit> since <date>"`) | A timestamp |
| NaN or infinite time value | Null |
| Time unit in singular form, as an abbreviation or with capitals (`hour`, `h`, `Days`) | A timestamp |
| A time of day or a zone in the reference date (`hours since 2000-01-01 12:00`) | Beacon applies it. |
| Calendar `standard`, `gregorian`, `proleptic_gregorian` or `julian` | A timestamp |
| Calendar `noleap`, `360_day` or a different calendar | Numbers |
| Time unit `nanoseconds` (`nanoseconds since <date>`) | Numbers in the stored type, for example `Int64` |
| A variable shorter than its unlimited dimension | The cells past the length of the variable are null. |

Beacon's NetCDF writer writes the unit `nanoseconds since 1970-01-01` for a column of type
`Timestamp(Nanosecond)`. Beacon reads such a file back as `Int64` numbers.

### Refresh a NetCDF table

An external table keeps the column types that Beacon stored when you created the table. Beacon casts
the new types to the stored types. A cast from microseconds to nanoseconds is exact for a time in
the years 1677 to 2262. Beacon gives null for a time outside these years.

A packed time column is different. A stored `Float64` column gets the microseconds since 1970, not
the original values. Beacon gives no error. Refresh each NetCDF table with a packed time column.
Use [`REFRESH`](/docs/2.1.0-rc3/sql/create-external-table#refresh), or drop the table and create it
again. The table then gets the new types. `REFRESH` changes the types only if Beacon inferred the
schema of the table.

```sql
REFRESH argo
```

## Limits

- A query must read at least one column of the file. `COUNT(*)` gives an error. Use `COUNT(column)` with a column on the full grid.
- Beacon leaves out a variable that it cannot model. An example is a variable with a repeated dimension, such as `m(x, x)`. A different example is a variable with a type that is not atomic. Beacon also leaves out the attribute columns of the variable, such as `m.units`.
- `PARTITIONED BY` is not supported. A query on a partitioned NetCDF table gives an error.
- Ragged files, for example World Ocean Database files, are not supported. Read such a file with the [HDF5 format](/docs/2.1.0-rc3/formats/hdf5): `read_hdf5` or `STORED AS HDF5`.

## Write a NetCDF output

Beacon writes NetCDF with a pure-Rust writer. Beacon writes the file to a temporary file in the
Beacon tmp folder and then uploads it to the target store.

- Beacon writes a null value of a string column as an empty string.
- An attribute above 64 KiB fails the write.
- A failed write leaves no file at the output path.

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
- Beacon does not compress a string variable.
- xarray, netCDF4-python, Panoply and ncview read a deflate file. They need no plugin.

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
  `scale_factor` and `add_offset`.
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

See [Create External Tables](/docs/2.1.0-rc3/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.1.0-rc3/data-sources/) for the
full read model.

### `OPTIONS`

`STORED AS NC` takes five keys for a read. The keys for a write are in
[NetCDF output options](#netcdf-output-options).

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `read_dimensions` | List of dimension names | The default grid of each file | The dimensions the table reads. Beacon returns a variable only if the list holds every dimension of that variable. |
| `skip_unbroadcastable` | Boolean | `false` | Skip a file that does not fit `read_dimensions`. Beacon writes a warning and reads the next file. Without it the query fails. |
| `skip_invalid_files` | Boolean | `false` | Skip a file that the reader cannot open. Beacon writes a warning and reads the next file. Without it the query fails, and the error names the file. |
| `enable_statistics` | Boolean | `true` (`BEACON_NETCDF_ENABLE_STATISTICS`) | Compute the column ranges of each file for the [file statistics](/docs/2.1.0-rc3/internals/file-statistics) of this table. With `false`, the table records no range and prunes no file. |

```sql
-- One grid
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
OPTIONS ('read_dimensions' 'time,latitude,longitude')
```

See [`OPTIONS`](/docs/2.1.0-rc3/sql/create-external-table#options) for the rules that hold for every key. See
[Arrays to tables](/docs/2.1.0-rc3/arrays-to-tables#the-dimensions-argument) for the grid rule.
