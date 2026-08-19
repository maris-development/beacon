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

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_parquet('obs/*.parquet') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc3/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

DataFusion reads Parquet directly. Parquet suits analytical work, because it stores data by column
and supports predicate pushdown.

- Beacon supports column pruning and predicate pushdown in full.
- Beacon supports Hive-style directory partitions. Use `PARTITIONED BY` on an [external table](/docs/2.0.0-rc3/data-sources/external-tables).
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

See [Create External Tables](/docs/2.0.0-rc3/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc3/data-sources/) for the
full read model.
