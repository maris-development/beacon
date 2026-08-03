---
description: Read CSV and TSV files with read_csv(). Schema is inferred from the file contents.
---

# CSV

## Reading

```text
read_csv(glob_paths)
read_csv(glob_paths, delimiter)
read_csv(glob_paths, delimiter, infer_records)
```

Schema is inferred from the file contents. The first row must be a header row.

- `delimiter`, single-character field separator (default: `,`)
- `infer_records`, number of rows to sample when inferring column types (default: `128000`)

```sql
SELECT * FROM read_csv('metadata/*.csv')

-- Tab-separated, sample 500 rows for type inference
SELECT * FROM read_csv(['data/*.tsv'], '\t', 500)
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) returns the
inferred column names and types **without reading any data**, which makes it the cheapest
option on large collections:

```sql
SELECT * FROM read_schema('stations/*.csv', 'csv');
```

Pass a list to see the combined schema across several locations, which is how you spot files
that disagree about a column:

```sql
SELECT * FROM read_schema(['stations/*.csv', 'other/*.csv'], 'csv');
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_csv('stations/*.csv'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE station_metadata;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_csv('stations/*.csv') LIMIT 0").arrow().schema
```

## Format details

- The first row must be a header row containing column names.
- Files must be UTF-8 encoded.
- Schema is inferred from the file contents.

## As an external table

```sql
CREATE EXTERNAL TABLE station_metadata
STORED AS CSV
LOCATION 'metadata/stations/'
```

See [Creating External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0-rc2/beacondb/data-sources/) for the general reading model.
