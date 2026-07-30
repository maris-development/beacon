---
description: Read Beacon Binary Format (BBF) files with read_bbf(), Beacon's own columnar format with chunk-level pruning.
---

# BBF

## Reading

```text
read_bbf(glob_paths)
```

Reads Beacon Binary Format files.

```sql
SELECT * FROM read_bbf('bbf/**/*.bbf')
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

[`read_schema()`](/docs/2.0.0-rc1/beacondb/sql/table-functions-utility#read-schema) returns the
inferred column names and types **without reading any data**, which makes it the cheapest
option on large collections:

```sql
SELECT * FROM read_schema('data/*.bbf', 'bbf');
```

Pass a list to see the combined schema across several locations, which is how you spot files
that disagree about a column:

```sql
SELECT * FROM read_schema(['data/*.bbf', 'other/*.bbf'], 'bbf');
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0-rc1/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_bbf('data/*.bbf'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE observations;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_bbf('data/*.bbf') LIMIT 0").arrow().schema
```

## Format details

The Beacon Binary Format (BBF) is Beacon's own columnar format, optimized for the kinds of queries common in earth-science and oceanographic workloads.

- Full S3 / object-store support with authenticated access.
- Chunk-level predicate pruning similar to Parquet row-group filtering.
- Efficient for repeated range queries over coordinate columns (time, depth, lat/lon).

Convert existing NetCDF files to BBF using the beacon-binary-format-toolbox for significant query speedups on large collections.

## As an external table

```sql
CREATE EXTERNAL TABLE my_table
STORED AS BBF
LOCATION 'path/to/files';
```

See [Creating External Tables](/docs/2.0.0-rc1/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0-rc1/beacondb/data-sources/) for the general reading model.
