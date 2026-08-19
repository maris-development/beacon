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

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_csv('stations/*.csv') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc3/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

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

See [Create External Tables](/docs/2.0.0-rc3/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc3/data-sources/) for the
full read model.
