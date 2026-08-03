---
description: Read Parquet files with read_parquet(), or register them as an external table. Beacon supports column pruning and predicate pushdown in full.
---

# Parquet

## Read the files

```text
read_parquet(glob_paths)
```

```sql
SELECT * FROM read_parquet('obs/**/*.parquet') LIMIT 100
```

## Inspect the schema

Check the columns of a file before you write a query. Also check their types.

[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) returns the column names and types **without a read of any data**. It is
therefore the cheapest option on a large collection:

```sql
SELECT * FROM read_schema('obs/*.parquet', 'parquet');
```

Pass a list to get the combined schema of several locations. This shows the files that disagree
about a column:

```sql
SELECT * FROM read_schema(['obs/*.parquet', 'other/*.parquet'], 'parquet');
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) gives more than names and types. It profiles every column in
one pass. It adds the minimum, the maximum, the distinct count and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_parquet('obs/*.parquet'));
```

If the files have a table name, use `DESCRIBE`:

```sql
DESCRIBE ocean_profiles;
```

From Python, read the Arrow schema of a relation. Beacon collects no rows:

```python
con.sql("SELECT * FROM read_parquet('obs/*.parquet') LIMIT 0").arrow().schema
```

## Format details

DataFusion reads Parquet directly. Parquet suits analytical work, because it stores data by column
and supports predicate pushdown.

- Beacon supports column pruning and predicate pushdown in full.
- Beacon supports Hive-style directory partitions. Use `PARTITIONED BY` on an [external table](/docs/2.0.0-rc2/beacondb/data-sources/external-tables).
- Beacon reads files from DuckDB, Spark, pandas and similar tools.

## As an external table

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

Point at a folder. Beacon then finds every `.parquet` file under it. You can also give the glob:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/**/*.parquet'
```

See [Create External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/) for the
full read model.
