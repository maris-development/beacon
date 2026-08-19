---
description: Read NetCDF files with read_netcdf(). Beacon streams chunks, shows variable attributes as columns and takes an optional dimension filter.
---

# NetCDF

## Read the files

```text
read_netcdf(glob_paths)
read_netcdf(glob_paths, dimensions)
```

Beacon reads the NetCDF files that match one or more glob patterns.

The optional `dimensions` argument selects the variables. Beacon returns a variable only if the list
holds all of its dimensions. Use the argument to drop variables with many dimensions. Also use it
when the files hold variables with different dimensions.

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

[Inspect a schema](/docs/2.0.0-rc3/formats/inspect-a-schema) compares the `_schema` functions,
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
[Atlas](/docs/2.0.0-rc3/formats/atlas) collection. Atlas merges many NetCDF
files into one array store with statistics. Beacon can then drop whole datasets and read only the
arrays that you select. This is much faster than a scan of the original NetCDF files.
:::

## As an external table

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
```

See [Create External Tables](/docs/2.0.0-rc3/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc3/data-sources/) for the
full read model.
