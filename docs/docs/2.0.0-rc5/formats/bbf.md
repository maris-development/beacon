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
SELECT * FROM read_bbf('bbf/**/*.bbf')
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_bbf('data/*.bbf') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc5/formats/inspect-a-schema) compares the `_schema` functions,
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

See [Create External Tables](/docs/2.0.0-rc5/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc5/data-sources/) for the
full read model.

### `OPTIONS`

`STORED AS BBF` reads one key:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `split_streams_slice` | Boolean | `false` (`BEACON_ENABLE_BBF_SPLIT_STREAMS_SLICE`) | Cut each batch into 16k-row slices. This bounds the peak memory of a wide table and it raises parallelism. It changes no result. |

```sql
CREATE EXTERNAL TABLE my_table
STORED AS BBF
LOCATION 'path/to/files'
OPTIONS ('split_streams_slice' 'true')
```

See [`OPTIONS`](/docs/2.0.0-rc5/sql/create-external-table#options) for the rules that hold for every key.
