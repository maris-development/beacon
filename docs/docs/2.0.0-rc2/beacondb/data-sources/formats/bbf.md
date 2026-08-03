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

Check the columns of a file before you write a query. Also check their types.

[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) returns the column names and types **without a read of any data**. It is
therefore the cheapest option on a large collection:

```sql
SELECT * FROM read_schema('data/*.bbf', 'bbf');
```

Pass a list to get the combined schema of several locations. This shows the files that disagree
about a column:

```sql
SELECT * FROM read_schema(['data/*.bbf', 'other/*.bbf'], 'bbf');
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) gives more than names and types. It profiles every column in
one pass. It adds the minimum, the maximum, the distinct count and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_bbf('data/*.bbf'));
```

If the files have a table name, use `DESCRIBE`:

```sql
DESCRIBE observations;
```

From Python, read the Arrow schema of a relation. Beacon collects no rows:

```python
con.sql("SELECT * FROM read_bbf('data/*.bbf') LIMIT 0").arrow().schema
```

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

See [Create External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/) for the
full read model.
