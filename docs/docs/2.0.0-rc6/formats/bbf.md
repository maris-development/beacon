---
description: Read Beacon Binary Format (BBF) files with read_bbf(). BBF is the columnar format of Beacon, with chunk-level pruning.
---

# BBF

## Read the files

```text
read_bbf(glob_paths)
```

Beacon reads Beacon Binary Format files.

```sql
SELECT time, depth, temperature FROM read_bbf('bbf/**/*.bbf')
```

A BBF query must name its columns. `SELECT *` and `SELECT count(*)` fail at plan time. The reader
flattens each n-dimensional column on the dimensions of the selected columns. A scan of every
column flattens on every dimension, and a scan of no column has no dimensions, so Beacon refuses
both. Count over a named column: `SELECT count(time) FROM read_bbf('bbf/**/*.bbf')`.

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_bbf_schema('data/*.bbf');
```

[Inspect a schema](/docs/2.0.0-rc6/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

The Beacon Binary Format (BBF) is the columnar format of Beacon. It suits the queries of earth
science and oceanography.

- Full object storage support, with authenticated access.
- Chunk-level predicate pruning, like the row group filter of Parquet.
- Fast for repeated range queries over coordinate columns such as time, depth, latitude and
  longitude.

Convert your NetCDF files to BBF with the beacon-binary-format-toolbox. This makes queries over a
large collection much faster.

## As an external table

```sql
CREATE EXTERNAL TABLE my_table
STORED AS BBF
LOCATION 'path/to/files';
```

See [Create External Tables](/docs/2.0.0-rc6/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc6/data-sources/) for the
full read model.

BBF reads through the nd pipeline, like NetCDF, Zarr and Atlas. Each entry of a file is one
dataset. A `WHERE` on a coordinate column runs before Beacon broadcasts the entry, so the rows
it drops never exist in memory. A BBF table does not support `PARTITIONED BY`.

### `OPTIONS`

`STORED AS BBF` reads no keys. A file carries its own schema, and the nd pipeline decides the
shape of a batch. Beacon ignores a key you write, so a table created with the old
`split_streams_slice` key keeps working.

See [`OPTIONS`](/docs/2.0.0-rc6/sql/create-external-table#options) for the rules that hold for every key.
