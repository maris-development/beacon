---
description: Read Parquet files with read_parquet() or register them as an external table. Column pruning and predicate pushdown are fully supported.
---

# Parquet

## Reading

```text
read_parquet(glob_paths)
```

```sql
SELECT * FROM read_parquet('obs/**/*.parquet') LIMIT 100
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

[`read_schema()`](/docs/2.0.0-rc1/beacondb/sql/table-functions-utility#read_schema) returns the
inferred column names and types **without reading any data**, which makes it the cheapest
option on large collections:

```sql
SELECT * FROM read_schema('obs/*.parquet', 'parquet');
```

Pass a list to see the combined schema across several locations, which is how you spot files
that disagree about a column:

```sql
SELECT * FROM read_schema(['obs/*.parquet', 'other/*.parquet'], 'parquet');
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0-rc1/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_parquet('obs/*.parquet'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE ocean_profiles;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_parquet('obs/*.parquet') LIMIT 0").arrow().schema
```

## Format details

Native support via DataFusion. Recommended for analytical workloads due to columnar storage and built-in predicate pushdown.

- Column pruning and predicate pushdown are fully supported.
- Hive-style directory partitioning is supported via `PARTITIONED BY` on [External Tables](/docs/2.0.0-rc1/beacondb/data-sources/external-tables).
- Compatible with files produced by DuckDB, Spark, pandas, and similar tools.

## As an external table

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/'
```

Point at a folder and Beacon will glob all `.parquet` files under it automatically. You can also be explicit:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/**/*.parquet'
```

See [Creating External Tables](/docs/2.0.0-rc1/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0-rc1/beacondb/data-sources/) for the general reading model.
