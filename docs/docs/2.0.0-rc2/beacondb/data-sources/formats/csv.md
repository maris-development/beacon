---
description: Read CSV and TSV files with read_csv(). Beacon infers the schema from the file contents.
---

# CSV

## Read the files

```text
read_csv(glob_paths)
read_csv(glob_paths, delimiter)
read_csv(glob_paths, delimiter, infer_records)
```

Beacon infers the schema from the file contents. The first row must be a header row.

- `delimiter`: the field separator, one character (default: `,`)
- `infer_records`: the number of rows that Beacon samples for the column types (default: `128000`)

```sql
SELECT * FROM read_csv('metadata/*.csv')

-- Tab-separated, sample 500 rows for type inference
SELECT * FROM read_csv(['data/*.tsv'], '\t', 500)
```

## Inspect the schema

Check the columns of a file before you write a query. Also check their types.

[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) returns the column names and types **without a read of any data**. It is
therefore the cheapest option on a large collection:

```sql
SELECT * FROM read_schema('stations/*.csv', 'csv');
```

Pass a list to get the combined schema of several locations. This shows the files that disagree
about a column:

```sql
SELECT * FROM read_schema(['stations/*.csv', 'other/*.csv'], 'csv');
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) gives more than names and types. It profiles every column in
one pass. It adds the minimum, the maximum, the distinct count and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_csv('stations/*.csv'));
```

If the files have a table name, use `DESCRIBE`:

```sql
DESCRIBE station_metadata;
```

From Python, read the Arrow schema of a relation. Beacon collects no rows:

```python
con.sql("SELECT * FROM read_csv('stations/*.csv') LIMIT 0").arrow().schema
```

## Format details

- The first row must be a header row with the column names.
- A file must use UTF-8 encoding.
- Beacon infers the schema from the file contents.

## As an external table

```sql
CREATE EXTERNAL TABLE station_metadata
STORED AS CSV
LOCATION 'metadata/stations/'
```

See [Create External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/) for the
full read model.
