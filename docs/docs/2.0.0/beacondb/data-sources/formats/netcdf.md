---
description: Read NetCDF files with read_netcdf(). Streaming chunk-level reads, variable attributes as columns, and an optional dimension filter.
---

# NetCDF

## Reading

```text
read_netcdf(glob_paths)
read_netcdf(glob_paths, dimensions)
```

Reads NetCDF files matching one or more glob patterns.

The optional `dimensions` argument filters which variables are returned: a variable is included only if all of its dimensions are a subset of the provided list. Use it to exclude high-dimensional variables you don't need, or to resolve ambiguity when files contain variables with incompatible dimensionalities.

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')

-- With explicit dimension columns
SELECT *
FROM read_netcdf(['argo/**/*.nc'], ['time', 'pressure'])
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

[`read_schema()`](/docs/2.0.0/beacondb/sql/table-functions-utility#read_schema) returns the
inferred column names and types **without reading any data**, which makes it the cheapest
option on large collections:

```sql
SELECT * FROM read_schema('argo/**/*.nc', 'netcdf');
```

Pass a list to see the combined schema across several locations, which is how you spot files
that disagree about a column:

```sql
SELECT * FROM read_schema(['argo/**/*.nc', 'other/*.nc'], 'netcdf');
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_netcdf('argo/**/*.nc'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE argo;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_netcdf('argo/**/*.nc') LIMIT 0").arrow().schema
```

## Format details

Streaming reads with chunk-level access, large files are read incrementally rather than loaded entirely into memory.

Supported dialects:

- **NetCDF4** (recommended)
- **NetCDF3**: `char*` arrays with a string-like dimension (e.g. `STRLEN`) are inferred as fixed-length strings

### Variable attributes

NetCDF variables carry metadata attributes (e.g. `units`, `long_name`, `valid_min`). Beacon exposes these as extra columns using dot notation: `<variable>.<attribute>`. For example, a variable `temperature` with a `units` attribute is accessible as the column `temperature.units`.

Attribute columns preserve the original type (string, integer, float, …) as stored in the file. They are available alongside the variable columns in every query.

File-level global attributes are exposed with a leading dot and no variable prefix: `.<attribute>`. For example, a global attribute `source` is accessible as the column `.source`.

```sql
SELECT temperature, "temperature.units", "temperature.long_name", ".source"
FROM read_netcdf(['argo/**/*.nc'])
LIMIT 1
```

Limitations:

- User-defined types are not supported.
- S3 / object-store backends only support anonymous access. Authenticated S3 reads are not yet supported.

:::tip
For best performance with large NetCDF collections, convert the files into a single [Atlas](/docs/2.0.0/beacondb/data-sources/formats/atlas) collection. Atlas consolidates many NetCDF files into one statistics-aware array store, so Beacon can prune whole datasets and read only the projected arrays, typically much faster than scanning the original NetCDF files.
:::

## As an external table

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
```

See [Creating External Tables](/docs/2.0.0/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0/beacondb/data-sources/) for the general reading model.
